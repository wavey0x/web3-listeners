from contextlib import closing
import json
from pathlib import Path
import sqlite3
import stat
from tempfile import TemporaryDirectory
from types import SimpleNamespace
import unittest
from unittest.mock import patch, Mock

from hexbytes import HexBytes

from data_fetchers import curve_gauge_votes as curve
import notifications
from sqlite_store import Store

IMPORT_ID = 'a' * 64
GAUGE = '0x1111'
USER = next(iter(curve.ALIASES))


def block_hash(number):
    return HexBytes(number.to_bytes(32, 'big'))


def vote(block=10,index=0,user=USER,gauge=GAUGE,weight=10000):
    return dict(blockNumber=block,blockHash=block_hash(block),transactionHash=block_hash(block+1000),
                logIndex=index,address=curve.GAUGE_CONTROLLER_ADDRESS,args=dict(user=user,gauge_addr=gauge,weight=weight))


class Chain:
    def __init__(self):
        self.eth = self
        self.chain_id = 1
        self.height = 12
        self.latest = 14
        self.logs = []
        self.balances = {}
        self.fail = False
        self.hash_changes = {}
        self.controller = SimpleNamespace(events=SimpleNamespace(VoteForGauge=self))
        self.ve = SimpleNamespace(functions=SimpleNamespace(balanceOf=self.balance_of))

    def balance_of(self,user):
        return SimpleNamespace(call=lambda **kwargs:self.balances.get(user,2_000_000*10**18))

    def get_block(self,number):
        number = self.height if number=='finalized' else self.latest if number=='latest' else number
        return dict(number=number,hash=self.hash_changes.get(number,block_hash(number)),timestamp=1700000000+number)

    def get_logs(self,fromBlock,toBlock):
        if self.fail:
            raise OSError('RPC failed')
        return [item for item in self.logs if fromBlock<=item['blockNumber']<=toBlock]

    def get_transaction_receipt(self,tx):
        logs = sorted([item for item in self.logs if item['transactionHash']==tx],key=lambda item:item['logIndex'])
        return dict(transactionHash=tx,blockHash=logs[0]['blockHash'],logs=logs)


class CurveTests(unittest.TestCase):
    def setUp(self):
        directory = TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        self.path = Path(directory.name)/'shared.sqlite3'
        with closing(sqlite3.connect(self.path)) as connection:
            connection.executescript('''PRAGMA user_version=1;
                CREATE TABLE _migration_meta (key TEXT PRIMARY KEY,value TEXT NOT NULL);
                CREATE TABLE curve_gauge_votes (id INTEGER PRIMARY KEY AUTOINCREMENT,gauge TEXT,gauge_name TEXT,
                    account TEXT,amount TEXT,weight INTEGER,account_alias TEXT,txn_hash TEXT,timestamp INTEGER,
                    date_str TEXT,block INTEGER);''')
            connection.execute('INSERT INTO _migration_meta VALUES (?,?)',('manifest',json.dumps(dict(schema_version=1,
                snapshot_sha256=IMPORT_ID,status='data_verified',application_ready=False,alerts_enabled=False))))
            connection.commit()
        self.store = Store(self.path,IMPORT_ID,rehearsal=True)
        curve.prepare(self.store)
        self.chain = Chain()
        self.gauges = {GAUGE:'Known gauge'}
        self.sent = []
        self.generation = 0

    def seed(self,start=10):
        def write(connection):
            connection.execute('INSERT INTO curve_checkpoint VALUES (?,?,?,?,?)',('curve',1,start,start,curve.hex_value(block_hash(start-1))))
            notifications.add_stream(connection,'curve',start-1,curve.hex_value(block_hash(start-1)))
        self.store.write(write)

    def activate(self,floor=9):
        with closing(sqlite3.connect(self.path)) as connection:
            connection.execute('PRAGMA journal_mode=WAL')
            manifest=json.loads(connection.execute("SELECT value FROM _migration_meta WHERE key='manifest'").fetchone()[0])
            manifest.update(status='ready',application_ready=True,alerts_enabled=True)
            connection.execute("UPDATE _migration_meta SET value=? WHERE key='manifest'",(json.dumps(manifest),))
            connection.commit()
        self.store=Store(self.path,IMPORT_ID)
        self.store.write(lambda c:notifications.enable(c,'curve',floor,curve.hex_value(block_hash(floor))))
        self.generation=notifications.start_session(self.store,'curve',floor,curve.hex_value(block_hash(floor)))

    def send(self,stream,destination,message):
        self.sent.append((destination,message))

    def scan(self,send=None):
        curve.scan_once(self.store,self.chain,self.chain.controller,self.chain.ve,self.gauges,self.generation,send or self.send)

    def count(self,table):
        return self.store.read(lambda c:c.execute(f'SELECT count(*) FROM {table}').fetchone()[0])

    def state(self):
        return self.store.read(curve.checkpoint)

    def decide(self,key='event',block=10,kind='test',message='message',per_block=False):
        return self.store.write(lambda c:notifications.decide(c,'curve',self.generation,key,block,kind,'YLOCKERS',message,once_per_block=per_block))

    def import_boundary(self,copies=1):
        self.chain.logs=[vote()]
        items,_=curve.collect(self.chain,self.chain.controller,self.chain.ve,self.gauges,10,10)
        row=dict(items[0]['row'],amount=items[0]['legacy_amount'],gauge_name='Historical name',account_alias='Historical alias')
        self.store.write(lambda c:[curve.insert_row(c,row) for _ in range(copies)])
        return row

    def test_imported_history_and_unknown_gauges_stay_silent(self):
        self.seed()
        self.chain.logs=[vote(gauge='unknown')]
        self.scan()
        self.assertEqual(self.sent,[])
        self.assertEqual(self.count('curve_gauge_votes'),1)
        statuses=self.store.read(lambda c:[r[0] for r in c.execute('SELECT status FROM notification_decisions')])
        self.assertEqual(statuses,['suppressed','suppressed'])
        self.assertEqual(self.state()['next_block'],13)

    def test_multiple_future_votes_preserve_one_large_alert_per_block(self):
        self.seed()
        self.activate()
        self.chain.logs=[vote(index=0),vote(index=1)]
        self.scan()
        self.assertEqual(self.count('curve_gauge_votes'),2)
        self.assertEqual(len(self.sent),1)
        self.assertEqual(self.sent[0][0],'YLOCKERS')
        self.assertEqual(self.count('notification_blocks'),1)
        self.scan()
        self.assertEqual(len(self.sent),1)
        self.assertIsNone(self.decide('third-vote',kind='large-vote',per_block=True))

    def test_unknown_and_large_alerts_follow_data_commit(self):
        self.seed()
        self.activate()
        self.chain.logs=[vote(gauge='unknown')]
        def send(stream,destination,message):
            self.assertEqual(self.count('curve_gauge_votes'),1)
            self.assertEqual(self.state()['next_block'],13)
            self.sent.append((destination,message))
        self.scan(send)
        self.assertEqual([item[0] for item in self.sent],['WAVEY_ALERTS','YLOCKERS'])

    def test_failed_insert_rolls_back_all_rows_decisions_and_progress(self):
        self.seed()
        self.activate()
        self.chain.logs=[vote(index=0),vote(index=1)]
        original=curve.insert_row
        calls=[]
        def insert(c,row):
            original(c,row)
            calls.append(1)
            if len(calls)==2:
                raise RuntimeError('failed insert')
        with patch.object(curve,'insert_row',side_effect=insert),self.assertRaisesRegex(RuntimeError,'failed insert'):
            self.scan()
        self.assertEqual(self.sent,[])
        for table in ('curve_gauge_votes','curve_events','notification_decisions','notification_blocks'):
            self.assertEqual(self.count(table),0)
        self.assertEqual(self.state()['next_block'],10)

    def test_failed_attempt_commit_never_sends_or_replays_queue(self):
        self.seed()
        self.activate()
        self.chain.logs=[vote()]
        original=self.store.write
        def write(callback):
            if callback.__name__=='begin':
                raise RuntimeError('attempt commit failed')
            return original(callback)
        with patch.object(self.store,'write',side_effect=write),self.assertRaisesRegex(RuntimeError,'attempt commit failed'):
            self.scan()
        self.assertEqual(self.sent,[])
        self.assertEqual(self.state()['next_block'],13)
        self.scan()
        self.assertEqual(self.sent,[])

    def test_uncertain_delivery_has_one_attempt_even_after_restart(self):
        self.seed()
        self.activate()
        self.chain.logs=[vote()]
        def timeout(stream,destination,message):
            self.sent.append((destination,message))
            raise TimeoutError('uncertain response')
        with self.assertRaisesRegex(RuntimeError,'automatic retry is disabled'):
            self.scan(timeout)
        self.assertEqual(len(self.sent),1)
        status=self.store.read(lambda c:c.execute('SELECT status FROM notification_decisions').fetchone()[0])
        self.assertEqual(status,'uncertain')
        self.generation=notifications.start_session(self.store,'curve',14,curve.hex_value(block_hash(14)))
        self.scan(timeout)
        claim=notifications.Claim('curve',f'1:{curve.GAUGE_CONTROLLER_ADDRESS.lower()}:{curve.hex_value(block_hash(1010))}:0','large-vote','YLOCKERS')
        self.assertFalse(notifications.dispatch(self.store,claim,'unused',timeout))
        self.assertEqual(len(self.sent),1)

    def test_restart_suppresses_outage_then_allows_future_events(self):
        self.seed()
        self.activate()
        self.generation=notifications.start_session(self.store,'curve',20,curve.hex_value(block_hash(20)))
        self.chain.logs=[vote(block=15,gauge='unknown')]
        self.chain.height=20
        self.scan()
        self.assertEqual(self.sent,[])
        self.chain.logs.append(vote(block=21))
        self.chain.height=21
        self.scan()
        self.assertEqual(len(self.sent),1)

    def test_restart_fences_stale_claims_and_workers(self):
        self.seed()
        self.activate()
        claim=self.decide()
        notifications.start_session(self.store,'curve',9,curve.hex_value(block_hash(9)))
        self.assertFalse(notifications.dispatch(self.store,claim,'message',self.send))
        with self.assertRaisesRegex(RuntimeError,'session changed'):
            self.decide('later')
        self.chain.logs=[vote()]
        with self.assertRaisesRegex(RuntimeError,'concurrently'):
            self.scan()
        self.assertEqual(self.sent,[])
        self.assertEqual(self.count('curve_gauge_votes'),0)

    def test_mute_and_global_hold_block_already_claimed_delivery(self):
        self.seed()
        self.activate()
        claim=self.decide()
        self.store.write(lambda c:notifications.mute(c,'curve'))
        self.assertFalse(notifications.dispatch(self.store,claim,'message',self.send))
        self.activate()
        claim=self.decide('next')
        def hold(c):
            manifest=json.loads(c.execute("SELECT value FROM _migration_meta WHERE key='manifest'").fetchone()[0])
            manifest['alerts_enabled']=False
            c.execute("UPDATE _migration_meta SET value=? WHERE key='manifest'",(json.dumps(manifest),))
        self.store.write(hold)
        self.assertFalse(notifications.dispatch(self.store,claim,'message',self.send))
        self.assertEqual(self.sent,[])

    def test_payload_changes_cannot_repurpose_existing_claim(self):
        self.seed()
        self.activate()
        claim=self.decide()
        self.assertIsNone(self.decide(message='different'))
        with self.assertRaisesRegex(RuntimeError,'content differs'):
            notifications.dispatch(self.store,claim,'different',self.send)
        self.assertEqual(self.sent,[])

    def test_boundary_preserves_historical_fields_and_fills_missing_vote(self):
        original=self.import_boundary()
        self.chain.logs.append(vote(index=1,gauge='another'))
        result=curve.adopt_boundary(self.store,self.chain,self.chain.controller,self.chain.ve,self.gauges)
        self.assertEqual(result['missing_boundary_events'],1)
        rows=self.store.read(lambda c:[dict(r) for r in c.execute('SELECT * FROM curve_gauge_votes ORDER BY id')])
        self.assertTrue(all(rows[0][key]==value for key,value in original.items()))
        self.assertEqual(len(rows),2)
        self.assertEqual(self.state()['next_block'],11)
        self.scan()
        self.assertEqual(self.sent,[])

    def test_import_duplicates_are_retained_and_unmatched_rows_are_rejected(self):
        self.import_boundary(copies=2)
        result=curve.adopt_boundary(self.store,self.chain,self.chain.controller,self.chain.ve,self.gauges)
        self.assertEqual(result['source_duplicates'],1)
        self.assertEqual(self.count('curve_gauge_votes'),2)
        self.assertEqual(self.count('curve_events'),1)

    def test_unmatched_import_does_not_adopt(self):
        self.import_boundary()
        self.store.write(lambda c:c.execute("UPDATE curve_gauge_votes SET amount='42'"))
        with self.assertRaisesRegex(RuntimeError,'does not match chain'):
            curve.adopt_boundary(self.store,self.chain,self.chain.controller,self.chain.ve,self.gauges)
        self.assertEqual(self.count('curve_checkpoint'),0)
        self.assertEqual(self.count('notification_streams'),0)

    def test_missing_checkpoint_rpc_failure_and_reorg_do_not_advance(self):
        with self.assertRaisesRegex(RuntimeError,'checkpoint missing'):
            self.scan()
        self.seed()
        self.chain.fail=True
        with self.assertRaises(OSError):
            self.scan()
        self.chain.fail=False
        self.chain.hash_changes[9]=block_hash(999)
        with self.assertRaisesRegex(RuntimeError,'checkpoint block changed'):
            self.scan()
        self.assertEqual(self.state()['next_block'],10)
        self.assertEqual(self.sent,[])

    def test_activation_requires_catchup_and_sets_latest_not_finalized_floor(self):
        self.seed()
        self.activate()
        with self.assertRaisesRegex(RuntimeError,'silent catch-up'):
            curve.enable_future_alerts(self.store,self.chain)
        self.scan()
        curve.enable_future_alerts(self.store,self.chain)
        self.assertEqual(self.store.read(lambda c:notifications.state(c,'curve'))['floor_block'],14)

    def test_prisma_alias_and_numeric_boundary_are_preserved(self):
        self.assertEqual(curve.ALIASES['0x490b8C6007fFa5d3728A49c2ee199e51f05D2F7e'],'Prisma')
        self.assertEqual(curve.amount_text(5,1000),'0.000000000000000001')
        self.assertEqual(curve.amount_text(1234567890123456789012345,9999),'1234444.433334444443333444')

    def test_event_identity_survives_changed_block_log_index(self):
        self.chain.logs=[vote(index=5)]
        first,_=curve.collect(self.chain,self.chain.controller,self.chain.ve,self.gauges,10,10)
        self.chain.logs[0]['logIndex']=15
        second,_=curve.collect(self.chain,self.chain.controller,self.chain.ve,self.gauges,10,10)
        self.assertEqual(first[0]['key'],second[0]['key'])

    def test_machine_permission_is_required_and_bound_to_import(self):
        import requests
        directory=self.path.parent/'permissions'
        directory.mkdir()
        permit=directory/'curve.allow'
        with patch.dict('os.environ',{'YEARN_NOTIFICATION_ALLOW_DIR':str(directory),'YEARN_IMPORT_SHA256':IMPORT_ID}),patch.object(requests,'Session') as session:
            with self.assertRaisesRegex(RuntimeError,'permission is missing'):
                notifications.send_telegram('curve','YLOCKERS','never sent')
            session.assert_not_called()
            permit.write_text(IMPORT_ID)
            def application_owned(path):
                return SimpleNamespace(st_uid=1000,st_mode=(stat.S_IFDIR|0o755) if path==directory else (stat.S_IFREG|0o644))
            with patch.object(Path,'lstat',application_owned),self.assertRaisesRegex(RuntimeError,'permission is invalid'):
                notifications.require_permission('curve')
            def root_owned(path):
                return SimpleNamespace(st_uid=0,st_mode=(stat.S_IFDIR|0o755) if path==directory else (stat.S_IFREG|0o644))
            with patch.object(Path,'lstat',root_owned):
                notifications.require_permission('curve')
                permit.write_text('b'*64)
                with self.assertRaisesRegex(RuntimeError,'permission is invalid'):
                    notifications.require_permission('curve')
            session.assert_not_called()

    def test_telegram_transport_has_no_retry_or_redirect_and_masks_errors(self):
        import requests
        response=Mock(status_code=200)
        response.json.return_value={'ok':True}
        session=Mock()
        session.post.return_value=response
        session.__enter__=Mock(return_value=session)
        session.__exit__=Mock(return_value=False)
        with patch.dict('os.environ',{'WAVEY_ALERTS_BOT_KEY':'synthetic-test-token'}),patch.object(requests,'Session',return_value=session),patch.object(notifications,'require_permission'):
            notifications.send_telegram('curve','YLOCKERS','synthetic')
            self.assertEqual(session.post.call_count,1)
            self.assertIs(session.post.call_args.kwargs['allow_redirects'],False)
            self.assertEqual(session.post.call_args.kwargs['timeout'],(5,20))
            self.assertEqual(session.mount.call_args.args[1].max_retries.total,0)
            session.post.side_effect=requests.Timeout('https://api.telegram.org/synthetic-test-token')
            with self.assertRaises(RuntimeError) as error:
                notifications.send_telegram('curve','YLOCKERS','synthetic')
            self.assertNotIn('synthetic-test-token',str(error.exception))
            self.assertEqual(session.post.call_count,2)


if __name__=='__main__':
    unittest.main()
