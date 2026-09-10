from contextlib import closing
from datetime import datetime, timezone
from decimal import Decimal
import json
from pathlib import Path
import sqlite3
from tempfile import TemporaryDirectory
from types import SimpleNamespace
import unittest
from unittest.mock import patch

from hexbytes import HexBytes

from data_fetchers import ll_harvests as ll
from sqlite_store import Store, require_patched_runtime

ADDRESS = '0xde2bEF0A01845257b4aEf2A2EAa48f6EAeAfa8B7'
INFO = ll.CURVE_LIQUID_LOCKER_COMPOUNDERS[ADDRESS]
IMPORT_ID = 'a' * 64


def block_hash(number):
    return HexBytes(number.to_bytes(32, 'big'))


def make_log(block=10, index=0, raw=1234567890123456789):
    return dict(blockNumber=block, logIndex=index, address=ADDRESS,
                blockHash=block_hash(block), transactionHash=block_hash(1000 + block),
                args={'_value': raw})


class Chain:
    def __init__(self, logs=(), height=12):
        self.eth = self
        self.logs = list(logs)
        self.height = height
        self.chain_id = 1
        self.requests = []
        self.fail_logs = False
        self.hash_changes = {}

    def get_block(self, number):
        if number == 'finalized':
            number = self.height
        return dict(number=number, timestamp=1_700_000_000 + number,
                    hash=self.hash_changes.get(number, block_hash(number)))

    def get_logs(self, fromBlock, toBlock):
        self.requests.append((fromBlock, toBlock))
        if self.fail_logs:
            raise OSError('RPC failed')
        return [log for log in self.logs if fromBlock <= log['blockNumber'] <= toBlock]

    def get_transaction_receipt(self, tx):
        logs = [log for log in self.logs if log['transactionHash'] == tx]
        return dict(transactionHash=tx, blockHash=logs[0]['blockHash'], logs=logs)


class HarvestTests(unittest.TestCase):
    def setUp(self):
        self.directory = TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.path = Path(self.directory.name) / 'shared data.sqlite3'
        with closing(sqlite3.connect(self.path)) as connection:
            connection.executescript('''
                PRAGMA user_version=1;
                CREATE TABLE _migration_meta (key TEXT PRIMARY KEY,value TEXT NOT NULL);
                CREATE TABLE crv_ll_harvests (id INTEGER PRIMARY KEY AUTOINCREMENT,
                    profit TEXT NOT NULL,timestamp INTEGER,name TEXT,underlying TEXT,
                    compounder TEXT,block INTEGER,txn_hash TEXT,date_str TEXT,
                    UNIQUE(txn_hash,profit,compounder));
            ''')
            connection.execute('INSERT INTO _migration_meta VALUES (?,?)', ('manifest', json.dumps({
                'schema_version': 1, 'snapshot_sha256': IMPORT_ID, 'status': 'data_verified',
                'application_ready': False, 'alerts_enabled': False})))
            connection.commit()
        self.store = Store(self.path, IMPORT_ID, rehearsal=True)
        ll.prepare(self.store)
        self.chain = Chain()
        self.rpc_patch = patch.object(ll, 'contract_event', return_value=self.chain)
        self.rpc_patch.start()
        self.addCleanup(self.rpc_patch.stop)

    def seed(self, next_block=10):
        self.store.write(lambda connection: connection.execute('INSERT INTO ll_checkpoints VALUES (?,?,?,?,?)',
            (ADDRESS.lower(), 1, next_block, next_block, ll.hex_value(block_hash(next_block - 1)))))

    def scan(self, **kwargs):
        ll.scan_once(self.store, self.chain, {ADDRESS: INFO}, **kwargs)

    def state(self):
        return self.store.read(lambda connection: ll.checkpoint(connection, ADDRESS))

    def counts(self):
        return self.store.read(lambda connection: tuple(connection.execute(f'SELECT count(*) FROM {table}').fetchone()[0]
            for table in ('crv_ll_harvests', 'll_events')))

    def import_log(self, log, profit=None):
        timestamp = self.chain.get_block(log['blockNumber'])['timestamp']
        row = (profit if profit is not None else str(log['args']['_value'] / 1e18), timestamp,
               INFO['symbol'], INFO['underlying'], ADDRESS, log['blockNumber'],
               log['transactionHash'].hex(), datetime.fromtimestamp(timestamp, timezone.utc).strftime('%Y-%m-%d %H:%M:%S'))
        self.store.write(lambda connection: connection.execute('''INSERT INTO crv_ll_harvests
            (profit,timestamp,name,underlying,compounder,block,txn_hash,date_str) VALUES (?,?,?,?,?,?,?,?)''', row))

    def test_multiple_events_same_block_and_empty_ranges_survive_restart(self):
        self.seed()
        self.chain.logs = [make_log(index=0), make_log(index=1, raw=9 * 10**30 + 1)]
        self.scan(chunk_size=1)
        self.assertEqual(self.counts(), (2, 2))
        self.assertEqual(self.state()['next_block'], 11)
        self.store = Store(self.path, IMPORT_ID, rehearsal=True)
        self.scan()
        self.assertEqual(self.state()['next_block'], 13)
        self.scan()
        self.assertEqual(self.chain.requests, [(10, 10), (11, 12)])
        values = self.store.read(lambda connection: [r[0] for r in connection.execute('SELECT profit FROM crv_ll_harvests ORDER BY id')])
        self.assertEqual(values, ['1.234567890123456789', '9000000000000.000000000000000001'])

    def test_duplicate_scan_event_is_atomic_and_deduplicated(self):
        self.seed()
        self.chain.logs = [make_log()]
        items, _ = ll.collect(self.chain, ADDRESS, INFO, 10, 10, 1)
        self.store.write(lambda connection: ll.insert_event(connection, items[0], ADDRESS))
        self.scan()
        self.assertEqual(self.counts(), (1, 1))
        self.assertEqual(self.state()['next_block'], 13)

    def test_failed_second_insert_rolls_back_rows_and_progress(self):
        self.seed()
        self.chain.logs = [make_log(), make_log(index=1, raw=2 * 10**18)]
        original = ll.insert_event
        def insert(connection, item, address):
            result = original(connection, item, address)
            if item['key'].endswith(':1'):
                raise RuntimeError('simulated interruption')
            return result
        with patch.object(ll, 'insert_event', side_effect=insert), self.assertRaisesRegex(RuntimeError, 'interruption'):
            self.scan()
        self.assertEqual(self.counts(), (0, 0))
        self.assertEqual(self.state()['next_block'], 10)
        self.scan()
        self.assertEqual(self.counts(), (2, 2))

    def test_rpc_failure_never_advances_checkpoint(self):
        self.seed()
        self.chain.fail_logs = True
        with self.assertRaisesRegex(OSError, 'RPC failed'):
            self.scan()
        self.assertEqual(self.state()['next_block'], 10)
        self.assertEqual(self.counts(), (0, 0))

    def test_missing_checkpoint_fails_without_fallback(self):
        with self.assertRaisesRegex(RuntimeError, 'checkpoint missing'):
            self.scan()
        self.assertEqual(self.chain.requests, [])

    def test_changed_parent_or_wrong_chain_fails_closed(self):
        self.seed()
        self.chain.hash_changes[9] = block_hash(999)
        with self.assertRaisesRegex(RuntimeError, 'checkpoint block changed'):
            self.scan()
        self.chain.hash_changes.clear()
        self.chain.chain_id = 10
        with self.assertRaisesRegex(RuntimeError, 'chain does not match'):
            self.scan()
        self.assertEqual(self.state()['next_block'], 10)

    def test_concurrent_advance_cannot_be_overwritten(self):
        self.seed()
        original = ll.collect
        def collect(*args):
            result = original(*args)
            self.store.write(lambda connection: connection.execute(
                'UPDATE ll_checkpoints SET next_block=11,previous_hash=?', (ll.hex_value(block_hash(10)),)))
            return result
        with patch.object(ll, 'collect', side_effect=collect), self.assertRaisesRegex(RuntimeError, 'concurrently'):
            self.scan()
        self.assertEqual(self.state()['next_block'], 11)
        self.assertEqual(self.counts(), (0, 0))

    def test_boundary_preserves_legacy_amount_and_fills_missing_event(self):
        first, second = make_log(), make_log(index=1, raw=2 * 10**18 + 1)
        self.chain.logs = [first, second]
        self.import_log(first)
        summary = ll.adopt_boundary(self.store, self.chain, ADDRESS, INFO)
        self.assertEqual(summary['missing_boundary_events'], 1)
        self.assertEqual(self.counts(), (2, 2))
        self.assertEqual(self.state()['next_block'], 11)
        values = self.store.read(lambda connection: [r[0] for r in connection.execute('SELECT profit FROM crv_ll_harvests ORDER BY id')])
        self.assertEqual(values, [str(first['args']['_value'] / 1e18), '2.000000000000000001'])
        self.scan()
        self.assertEqual(self.counts(), (2, 2))

    def test_unmatched_boundary_is_never_adopted(self):
        self.chain.logs = [make_log()]
        self.import_log(self.chain.logs[0], profit='42')
        with self.assertRaisesRegex(RuntimeError, 'does not match chain'):
            ll.adopt_boundary(self.store, self.chain, ADDRESS, INFO)
        self.assertEqual(self.counts(), (1, 0))
        with self.assertRaisesRegex(RuntimeError, 'checkpoint missing'):
            self.state()

    def test_import_preparation_does_not_run_twice(self):
        with self.assertRaises(sqlite3.OperationalError):
            ll.prepare(self.store)
        self.assertEqual(self.counts(), (0, 0))

    def test_missing_wrong_and_inactive_imports_are_rejected(self):
        path = self.path.parent / 'missing.sqlite3'
        with self.assertRaises(sqlite3.OperationalError):
            Store(path, IMPORT_ID)
        self.assertFalse(path.exists())
        with self.assertRaisesRegex(RuntimeError, 'identity'):
            Store(self.path, 'b' * 64, rehearsal=True)
        with self.assertRaisesRegex(RuntimeError, 'not ready'):
            Store(self.path, IMPORT_ID)

    def test_live_wal_reopens_without_writer_and_rechecks_readiness(self):
        with closing(sqlite3.connect(self.path)) as connection:
            connection.execute('PRAGMA journal_mode=WAL')
            manifest = json.loads(connection.execute("SELECT value FROM _migration_meta WHERE key='manifest'").fetchone()[0])
            manifest.update(application_ready=True, status='ready')
            connection.execute("UPDATE _migration_meta SET value=? WHERE key='manifest'", (json.dumps(manifest),))
            connection.commit()
        store = Store(self.path, IMPORT_ID)
        with self.assertRaisesRegex(RuntimeError, 'inactive'):
            Store(self.path, IMPORT_ID, rehearsal=True)
        self.assertEqual(store.read(lambda connection: connection.execute('PRAGMA foreign_keys').fetchone()[0]), 1)
        self.assertEqual(store.read(lambda connection: connection.execute('PRAGMA synchronous').fetchone()[0]), 2)
        with self.assertRaises(sqlite3.OperationalError):
            store.read(lambda connection: connection.execute('DELETE FROM crv_ll_harvests'))
        manifest.update(application_ready=False, status='data_verified')
        with closing(sqlite3.connect(self.path)) as connection:
            connection.execute("UPDATE _migration_meta SET value=? WHERE key='manifest'", (json.dumps(manifest),))
            connection.commit()
        with self.assertRaisesRegex(RuntimeError, 'not ready'):
            store.write(lambda connection: connection.execute('DELETE FROM crv_ll_harvests'))

    def test_busy_transaction_retries_whole_database_operation(self):
        calls = []
        def operation(connection):
            connection.execute('INSERT INTO crv_ll_harvests (profit) VALUES (?)', ('1',))
            calls.append(1)
            if len(calls) == 1:
                error = sqlite3.OperationalError('database is locked')
                error.sqlite_errorcode = sqlite3.SQLITE_BUSY
                raise error
        self.store.write(operation)
        self.assertEqual(len(calls), 2)
        self.assertEqual(self.counts(), (1, 0))

    def test_unfixed_runtime_is_rejected(self):
        for version in ((3, 45, 1), (3, 50, 4), (3, 51, 0)):
            with self.assertRaisesRegex(RuntimeError, 'WAL-reset'):
                require_patched_runtime(version)
        for version in ((3, 44, 6), (3, 50, 7), (3, 51, 3), (3, 53, 1)):
            require_patched_runtime(version)


if __name__ == '__main__':
    unittest.main()
