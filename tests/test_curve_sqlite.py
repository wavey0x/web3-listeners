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
        self.latest = 12
        self.logs = []
        self.balances = {}
        self.fail = False
        self.hash_changes = {}
        self.controller = SimpleNamespace(events=SimpleNamespace(VoteForGauge=self))
        self.ve = SimpleNamespace(functions=SimpleNamespace(balanceOf=self.balance_of))

    def balance_of(self,user):
        return SimpleNamespace(call=lambda **kwargs:self.balances.get(user,2_000_000*10**18))

    def get_block(self,number):
        number = self.height if number=='finalized' else max(self.height,self.latest) if number=='latest' else number
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

    def assert_no_history(self):
        self.assertEqual(self.store.read(lambda c: c.execute(
            "SELECT count(*) FROM sqlite_master WHERE name IN ('notification_decisions','notification_blocks')").fetchone()[0]), 0)

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
        return self.store.write(lambda c:notifications.pending(c,'curve',self.generation,block,'YLOCKERS'))

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
        self.assert_no_history()
        self.assertEqual(self.state()['next_block'],13)

    def test_every_eligible_vote_can_alert_without_a_delivery_ledger(self):
        self.seed()
        self.activate()
        self.chain.logs=[vote(index=0),vote(index=1)]
        self.scan()
        self.assertEqual(self.count('curve_gauge_votes'),2)
        self.assertEqual(len(self.sent),2)
        self.assert_no_history()
        self.scan()
        self.assertEqual(len(self.sent),2)


    def test_latest_vote_alert_does_not_wait_for_finality_or_repeat(self):
        self.seed()
        self.activate()
        self.chain.latest=14
        self.chain.logs=[vote(block=14)]
        self.scan()
        self.assertEqual(self.chain.height,12)
        self.assertEqual(self.state()['next_block'],15)
        self.assertEqual(len(self.sent),1)
        self.assertEqual(self.sent[0][0],'YLOCKERS')
        self.chain.height=14
        self.scan()
        self.assertEqual(len(self.sent),1)

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
        for table in ('curve_gauge_votes','curve_events'):
            self.assertEqual(self.count(table),0)
        self.assertEqual(self.state()['next_block'],10)

    def test_dispatch_needs_no_database_write(self):
        self.seed()
        self.activate()
        item=self.decide()
        with patch.object(self.store,'write',side_effect=AssertionError('unexpected delivery write')):
            self.assertTrue(notifications.dispatch(self.store,item,'message',self.send))
        self.assertEqual(len(self.sent),1)
        self.assert_no_history()


    def test_sending_failure_does_not_stop_indexing(self):
        self.seed()
        self.activate()
        self.chain.logs=[vote()]
        def timeout(*args):
            raise TimeoutError('uncertain response')
        self.scan(timeout)
        self.assertEqual(self.state()['next_block'],13)
        self.chain.latest=14
        self.chain.logs.append(vote(block=14))
        self.scan()
        self.assertEqual(len(self.sent),1)
        self.assert_no_history()


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

    def test_duplicate_alerts_are_allowed(self):
        self.seed()
        self.activate()
        item=self.decide()
        self.assertIsNotNone(self.decide())
        notifications.dispatch(self.store,item,'message',self.send)
        notifications.dispatch(self.store,item,'message',self.send)
        self.assertEqual(len(self.sent),2)
        self.assert_no_history()


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
        with self.assertRaisesRegex(RuntimeError,'[Rr]ecovery point'):
            self.scan()
        self.assertEqual(self.state()['next_block'],10)
        self.assertEqual(self.sent,[])

    def test_activation_requires_catchup_and_sets_latest_not_finalized_floor(self):
        self.chain.latest=14
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


    def test_reorg_replays_votes_and_keeps_finalized_rows(self):
        self.seed(); self.activate()
        self.chain.logs=[vote(block=10)]
        self.scan()
        original=self.store.read(lambda c:dict(c.execute('SELECT * FROM curve_gauge_votes').fetchone()))
        self.chain.latest=14
        self.chain.logs.append(vote(block=14))
        self.scan()
        self.chain.hash_changes[14]=block_hash(9999)
        replacement=vote(block=14,gauge='replacement')
        replacement['blockHash']=block_hash(9999)
        self.chain.logs=[vote(block=10),replacement]
        self.scan()  # Atomic rewind to 12.
        self.assertEqual(self.state()['next_block'],13)
        self.assertEqual(self.count('curve_gauge_votes'),1)
        self.scan()
        rows=self.store.read(lambda c:[dict(r) for r in c.execute('SELECT * FROM curve_gauge_votes ORDER BY id')])
        self.assertEqual(rows[0],original)
        self.assertEqual(rows[1]['gauge'],'replacement')
        self.assertEqual(self.state()['next_block'],15)
        self.assert_no_history()

    def test_empty_reorg_restarts_from_durable_fallback(self):
        self.seed(); self.activate(); self.scan()
        self.chain.latest=14; self.scan()
        self.chain.hash_changes[14]=block_hash(9999)
        self.store=Store(self.path,IMPORT_ID)
        self.scan(); self.scan()
        self.assertEqual(self.state()['previous_hash'],curve.hex_value(block_hash(9999)))
        self.assertEqual(self.count('curve_gauge_votes'),0)

    def test_interrupted_rollback_keeps_votes_and_checkpoint_together(self):
        self.seed(); self.activate(); self.scan()
        self.chain.latest=14; self.chain.logs=[vote(block=14)]; self.scan()
        previous=self.state()
        self.chain.hash_changes[14]=block_hash(9999)
        self.store.write(lambda c:c.execute("CREATE TRIGGER reject_rewind BEFORE DELETE ON curve_events BEGIN SELECT RAISE(ABORT,'interrupted'); END"))
        with self.assertRaises(sqlite3.IntegrityError): self.scan()
        self.assertEqual(self.state(),previous)
        self.assertEqual(self.count('curve_gauge_votes'),1)
        self.store.write(lambda c:c.execute('DROP TRIGGER reject_rewind'))
        self.scan()
        self.assertEqual(self.count('curve_gauge_votes'),0)
        self.assertEqual(self.state()['next_block'],13)

    def test_recovery_rpc_failure_and_invalid_anchor_never_delete_data(self):
        self.seed(); self.activate(); self.scan()
        self.chain.latest=14; self.chain.logs=[vote(block=14)]; self.scan()
        previous=self.state(); get_block=self.chain.get_block
        self.chain.hash_changes[14]=block_hash(9999)
        def unavailable(number):
            if number==12: raise OSError('RPC unavailable')
            return get_block(number)
        with patch.object(self.chain,'get_block',side_effect=unavailable),self.assertRaises(OSError): self.scan()
        self.assertEqual(self.state(),previous)
        self.chain.hash_changes[12]=block_hash(8888)
        with self.assertRaisesRegex(RuntimeError,'Finalized recovery point changed'): self.scan()
        self.assertEqual(self.state(),previous)
        self.assertEqual(self.count('curve_gauge_votes'),1)

    def test_explicit_upgrade_requires_mute_and_preserves_event_data(self):
        from scripts.upgrade_optimistic import upgrade
        self.seed(); self.activate(); self.chain.logs=[vote()]; self.scan()
        before=self.state()
        with self.assertRaisesRegex(RuntimeError,'Mute'): upgrade(self.store)
        self.store.write(lambda c:notifications.mute(c,'curve'))
        self.store.write(lambda c:c.execute('CREATE TABLE notification_decisions(dummy TEXT)'))
        upgrade(self.store); upgrade(self.store)
        self.assertEqual(self.state(),before)
        self.assertEqual(self.count('curve_gauge_votes'),1)
        self.assert_no_history()


    def test_invalid_fallback_position_cannot_skip_records(self):
        self.seed(); self.activate(); self.scan()
        self.store.write(lambda c:c.execute("UPDATE recovery_points SET position=99999 WHERE stream='curve'"))
        previous=self.state()
        with self.assertRaisesRegex(RuntimeError,'outside the processed import range'): self.scan()
        self.assertEqual(self.state(),previous)


if __name__=='__main__':
    unittest.main()
