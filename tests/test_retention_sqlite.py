from contextlib import closing
import json
from pathlib import Path
import sqlite3
from tempfile import TemporaryDirectory
from types import SimpleNamespace
import unittest
from unittest.mock import patch

from hexbytes import HexBytes

from data_fetchers import resupply_retention as retention
import notifications
from sqlite_store import Store

IMPORT_ID = 'b' * 64


def block_hash(number):
    return HexBytes(number.to_bytes(32, 'big'))


def weight(block=10, index=0, old=10**76+7, new=10**75+3):
    return dict(blockNumber=block, blockHash=block_hash(block), transactionHash=block_hash(block+1000),
                logIndex=index, address=retention.CONTRACT_ADDRESS,
                args=dict(user='0x1111111111111111111111111111111111111111', oldWeight=old, newWeight=new))


class Chain:
    def __init__(self):
        self.eth = self
        self.chain_id = 1
        self.height = 12
        self.latest = 12
        self.logs = []
        self.fail = False
        self.hash_changes = {}
        self.contract = SimpleNamespace(events=SimpleNamespace(WeightSet=self),
            functions=SimpleNamespace(totalSupply=lambda:SimpleNamespace(call=lambda **kwargs:10**76)))

    def get_block(self, number):
        number = self.height if number == 'finalized' else max(self.height,self.latest) if number == 'latest' else number
        return dict(number=number, hash=self.hash_changes.get(number,block_hash(number)),timestamp=1700000000+number)

    def get_logs(self, fromBlock, toBlock):
        if self.fail:
            raise OSError('RPC failed')
        return [item for item in self.logs if fromBlock <= item['blockNumber'] <= toBlock]

    def get_transaction_receipt(self, tx):
        logs = sorted([item for item in self.logs if item['transactionHash'] == tx],key=lambda item:item['logIndex'])
        return dict(transactionHash=tx,blockHash=logs[0]['blockHash'],logs=logs)


class RetentionTests(unittest.TestCase):
    def setUp(self):
        directory = TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        self.path = Path(directory.name)/'shared.sqlite3'
        with closing(sqlite3.connect(self.path)) as connection:
            connection.executescript('''PRAGMA user_version=1;
                CREATE TABLE _migration_meta (key TEXT PRIMARY KEY,value TEXT NOT NULL);
                CREATE TABLE weight_changes (id INTEGER PRIMARY KEY AUTOINCREMENT,user_address TEXT NOT NULL,
                    old_weight TEXT NOT NULL,new_weight TEXT NOT NULL,weight_diff TEXT NOT NULL,block INTEGER NOT NULL,
                    txn_hash TEXT NOT NULL,timestamp INTEGER NOT NULL,date_str TEXT NOT NULL,log_index INTEGER,
                    UNIQUE(txn_hash,log_index));''')
            connection.execute('INSERT INTO _migration_meta VALUES (?,?)',('manifest',json.dumps(dict(schema_version=1,
                snapshot_sha256=IMPORT_ID,status='data_verified',application_ready=False,alerts_enabled=False))))
            connection.commit()
        self.store = Store(self.path,IMPORT_ID,rehearsal=True)
        retention.prepare(self.store)
        self.chain = Chain()
        self.sent = []
        self.generation = 0

    def assert_no_history(self):
        self.assertEqual(self.store.read(lambda c: c.execute(
            "SELECT count(*) FROM sqlite_master WHERE name IN ('notification_decisions','notification_blocks')").fetchone()[0]), 0)

    def seed(self, start=10):
        def write(c):
            c.execute('INSERT INTO retention_checkpoint VALUES (?,?,?,?,?)',('retention',1,start,start,retention.hex_value(block_hash(start-1))))
            notifications.add_stream(c,'retention',start-1,retention.hex_value(block_hash(start-1)))
        self.store.write(write)

    def activate(self, floor=9):
        with closing(sqlite3.connect(self.path)) as c:
            c.execute('PRAGMA journal_mode=WAL')
            manifest=json.loads(c.execute("SELECT value FROM _migration_meta WHERE key='manifest'").fetchone()[0])
            manifest.update(status='ready',application_ready=True,alerts_enabled=True)
            c.execute("UPDATE _migration_meta SET value=? WHERE key='manifest'",(json.dumps(manifest),))
            c.commit()
        self.store=Store(self.path,IMPORT_ID)
        self.store.write(lambda c:notifications.enable(c,'retention',floor,retention.hex_value(block_hash(floor))))
        self.generation=notifications.start_session(self.store,'retention',floor,retention.hex_value(block_hash(floor)))

    def send(self, stream, destination, message):
        self.sent.append((stream,destination,message))

    def scan(self, send=None):
        return retention.scan_once(self.store,self.chain,self.chain.contract,2*10**76,self.generation,send or self.send)

    def rows(self, table):
        return self.store.read(lambda c:[dict(row) for row in c.execute(f'SELECT * FROM {table}')])

    def state(self):
        return self.store.read(retention.checkpoint)

    def test_exact_large_signed_weights_and_silent_history(self):
        self.seed()
        self.chain.logs=[weight()]
        self.assertTrue(self.scan())
        row=self.rows('weight_changes')[0]
        self.assertEqual(row['old_weight'],str(10**76+7))
        self.assertEqual(row['new_weight'],str(10**75+3))
        self.assertEqual(row['weight_diff'],str(-9*10**75-4))
        self.assert_no_history()
        self.assertEqual(self.sent,[])
        self.assertEqual(self.state()['next_block'],13)
        self.assertFalse(self.scan())

    def test_future_events_send_only_after_atomic_commit(self):
        self.seed()
        self.activate()
        self.chain.logs=[weight(index=0),weight(index=1)]
        def send(*args):
            self.assertEqual(len(self.rows('weight_changes')),2)
            self.assertEqual(self.state()['next_block'],13)
            self.send(*args)
        self.scan(send)
        self.assertEqual(len(self.sent),2)
        self.assertEqual(self.sent[0][:2],('retention','RESUPPLY_ALERTS'))
        self.assertIn('Retention Shares Checkpointed',self.sent[0][2])
        self.assertIn('(50.0%)',self.sent[0][2])
        self.scan()
        self.assertEqual(len(self.sent),2)

    def test_insert_failure_leaves_no_partial_data_or_alerts(self):
        self.seed()
        self.activate()
        self.chain.logs=[weight(index=0),weight(index=1)]
        insert=retention.insert_row
        def fail(c,row):
            result=insert(c,row)
            if row['log_index']==1:
                raise RuntimeError('failed insert')
            return result
        with patch.object(retention,'insert_row',side_effect=fail),self.assertRaisesRegex(RuntimeError,'failed insert'):
            self.scan()
        for table in ('weight_changes','retention_events'):
            self.assertEqual(self.rows(table),[])
        self.assertEqual(self.state()['next_block'],10)
        self.assertEqual(self.sent,[])

    def test_latest_weight_alert_does_not_wait_for_finality_or_repeat(self):
        self.seed()
        self.activate()
        self.chain.latest=14
        self.chain.logs=[weight(block=14)]
        self.scan()
        self.assertEqual(self.chain.height,12)
        self.assertEqual(self.state()['next_block'],15)
        self.assertEqual(len(self.sent),1)
        self.chain.height=14
        self.scan()
        self.assertEqual(len(self.sent),1)

    def test_duplicate_legacy_row_never_alerts(self):
        self.seed()
        self.chain.logs=[weight()]
        items,_=retention.collect(self.chain,self.chain.contract,10,10,include_messages=False)
        self.store.write(lambda c:retention.insert_row(c,items[0]['row']))
        self.activate()
        self.scan()
        self.assertEqual(len(self.rows('weight_changes')),1)
        self.assertEqual(len(self.rows('retention_events')),1)
        self.assert_no_history()
        self.assertEqual(self.sent,[])

    def test_sending_failure_does_not_discard_other_alerts(self):
        self.seed()
        self.activate()
        self.chain.logs=[weight(index=0),weight(index=1)]
        def timeout(*args):
            self.send(*args)
            raise TimeoutError('uncertain response')
        self.scan(timeout)
        self.generation=notifications.start_session(self.store,'retention',14,retention.hex_value(block_hash(14)))
        self.scan(timeout)
        self.assertEqual(len(self.sent),2)
        self.assert_no_history()

    def test_restart_suppresses_outage_and_allows_new_changes(self):
        self.seed()
        self.activate()
        self.generation=notifications.start_session(self.store,'retention',20,retention.hex_value(block_hash(20)))
        self.chain.height=21
        self.chain.logs=[weight(block=15),weight(block=21)]
        self.scan()
        self.assertEqual(len(self.sent),1)
        self.assert_no_history()

    def test_missing_checkpoint_rpc_failure_and_reorg_fail_closed(self):
        with self.assertRaisesRegex(RuntimeError,'checkpoint missing'):
            self.scan()
        self.seed()
        self.chain.fail=True
        with self.assertRaisesRegex(OSError,'RPC failed'):
            self.scan()
        self.chain.fail=False
        self.chain.hash_changes[9]=block_hash(99)
        with self.assertRaisesRegex(RuntimeError,'[Rr]ecovery point'):
            self.scan()
        self.assertEqual(self.state()['next_block'],10)
        self.assertEqual(self.rows('retention_events'),[])

    def test_boundary_preserves_legacy_null_index_and_duplicates_and_fills_missing(self):
        self.chain.logs=[weight(index=0),weight(index=1,new=0)]
        items,_=retention.collect(self.chain,self.chain.contract,10,10,include_messages=False)
        legacy=dict(items[0]['row'],log_index=None)
        self.store.write(lambda c:[retention.insert_row(c,legacy) for _ in range(2)])
        before=self.rows('weight_changes')
        result=retention.adopt_boundary(self.store,self.chain,self.chain.contract)
        self.assertEqual(result,dict(next_block=11,imported_boundary_rows=2,source_duplicates=1,missing_boundary_events=1))
        self.assertEqual(self.rows('weight_changes')[:2],before)
        self.assertEqual(len(self.rows('weight_changes')),3)
        self.assertEqual(len(self.rows('retention_events')),2)
        self.assert_no_history()

    def test_mismatched_import_never_adopts(self):
        self.chain.logs=[weight()]
        items,_=retention.collect(self.chain,self.chain.contract,10,10,include_messages=False)
        self.store.write(lambda c:retention.insert_row(c,dict(items[0]['row'],old_weight='1')))
        with self.assertRaisesRegex(RuntimeError,'does not match chain'):
            retention.adopt_boundary(self.store,self.chain,self.chain.contract)
        self.assertEqual(self.rows('retention_checkpoint'),[])
        self.assertEqual(self.rows('notification_streams'),[])

    def test_optional_display_supply_and_deployment_remain_compatible(self):
        self.chain.logs=[weight()]
        with patch.object(retention,'optional_supply',return_value=None):
            items,_=retention.collect(self.chain,self.chain.contract,10,10)
        self.assertIn('Total Remaining: Unable to fetch',items[0]['message'])
        self.chain.logs=[weight(block=retention.DEPLOYMENT_BLOCK)]
        items,_=retention.collect(self.chain,self.chain.contract,retention.DEPLOYMENT_BLOCK,retention.DEPLOYMENT_BLOCK)
        self.assertIsNone(items[0]['message'])

    def test_activation_requires_catchup_and_sets_latest_floor(self):
        self.chain.latest=14
        self.seed()
        self.activate()
        with self.assertRaisesRegex(RuntimeError,'silent catch-up'):
            retention.enable_future_alerts(self.store,self.chain)
        self.scan()
        retention.enable_future_alerts(self.store,self.chain)
        self.assertEqual(self.store.read(lambda c:notifications.state(c,'retention'))['floor_block'],14)

    def test_empty_ranges_are_bounded_and_stale_session_cannot_commit(self):
        self.seed()
        self.chain.height=10000
        self.scan()
        self.assertEqual(self.state()['next_block'],5010)
        notifications.start_session(self.store,'retention',10000,retention.hex_value(block_hash(10000)))
        with self.assertRaisesRegex(RuntimeError,'concurrently'):
            self.scan()
        self.assertEqual(self.state()['next_block'],5010)


    def test_reorg_replaces_only_unfinalized_weights(self):
        self.seed(); self.activate(); self.chain.logs=[weight(block=10)]; self.scan()
        original=self.rows('weight_changes')[0]
        self.chain.latest=14; self.chain.logs.append(weight(block=14,new=1)); self.scan()
        self.chain.hash_changes[14]=block_hash(9999)
        replacement=weight(block=14,new=2); replacement['blockHash']=block_hash(9999)
        self.chain.logs=[weight(block=10),replacement]
        self.scan(); self.scan()
        rows=self.rows('weight_changes')
        self.assertEqual(rows[0],original)
        self.assertEqual(rows[1]['new_weight'],'2')
        self.assertEqual(self.state()['next_block'],15)
        self.assert_no_history()


if __name__ == '__main__':
    unittest.main()
