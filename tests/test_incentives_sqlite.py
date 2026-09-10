from contextlib import closing
from datetime import datetime, timezone
import json
from pathlib import Path
import sqlite3
from tempfile import TemporaryDirectory
from types import SimpleNamespace
import unittest
from unittest.mock import Mock, patch

from hexbytes import HexBytes
from web3.datastructures import AttributeDict

from incentives import sqlite_worker as worker, rsup_incentives as rsup, yb_incentives as yb
from incentives.incentives_shared import WEEK
import notifications
from sqlite_store import Store
from scripts import backfill_rsup_incentives as corrections

IMPORT_ID='c'*64
PERIOD=100*WEEK


def block_hash(number):
    return HexBytes(number.to_bytes(32,'big'))


def transfer(token,block=1002,index=0):
    return AttributeDict.recursive(dict(blockNumber=block,blockHash=block_hash(block),
        transactionHash=block_hash(block+10000),logIndex=index,address=token,args=dict(value=100*10**18)))


class Chain:
    def __init__(self):
        self.eth=self
        self.chain_id=1
        self.height=1012
        self.latest=1014
        self.logs=[]
        self.fail=False
        self.hash_changes={}
        self.ranges=[]

    def get_block(self,number):
        number=self.height if number=='finalized' else self.latest if number=='latest' else number
        return dict(number=number,hash=self.hash_changes.get(number,block_hash(number)),timestamp=number*(WEEK//10))

    def get_transaction_receipt(self,tx):
        logs=sorted([log for log in self.logs if log['transactionHash']==tx],key=lambda log:log['logIndex'])
        return dict(transactionHash=tx,blockHash=logs[0]['blockHash'],logs=logs)

    def adapter(self,protocol='resupply'):
        adapter=SimpleNamespace(PROTOCOL=protocol,STREAM='rsup-incentives' if protocol=='resupply' else 'yb-incentives',
            TOKEN=rsup.TOKEN if protocol=='resupply' else yb.TOKEN,TRANSACTION_GROUPED=protocol=='yieldbasis')
        def logs(start,end):
            if self.fail:
                raise OSError('RPC failed')
            self.ranges.append((start,end))
            return [log for log in self.logs if log['address']==adapter.TOKEN and start<=log['blockNumber']<=end]
        def record(event,block,receipt,effective):
            period=block['timestamp']//WEEK*WEEK
            return dict(protocol=protocol,epoch=9 if protocol=='resupply' else None,total_incentives=100.,votium_amount=40.,
                votemarket_amount=60.,votium_votes_per_usd=None,votemarket_votes_per_usd=12.3,votium_votes=400.,
                votemarket_votes=600.,gauge_data={'gauge':{'value':1.25}},transaction_hash=event['transactionHash'].hex(),
                block_number=event['blockNumber'],timestamp=block['timestamp'],date_str=datetime.fromtimestamp(period,timezone.utc).strftime('%Y-%m-%d %H:%M UTC'),
                period_start=period,log_index=event['logIndex'])
        adapter.transfer_logs=logs
        adapter.build_record=Mock(side_effect=record)
        adapter.render_message=lambda row:'Incentive report '+row['transaction_hash']
        return adapter


class IncentiveTests(unittest.TestCase):
    def setUp(self):
        temporary=TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.path=Path(temporary.name)/'shared.sqlite3'
        with closing(sqlite3.connect(self.path)) as c:
            c.executescript('''PRAGMA user_version=1;
                CREATE TABLE _migration_meta(key TEXT PRIMARY KEY,value TEXT NOT NULL);
                CREATE TABLE incentives(id INTEGER PRIMARY KEY AUTOINCREMENT,protocol TEXT NOT NULL,epoch INTEGER,
                    total_incentives REAL NOT NULL,votium_amount REAL NOT NULL,votemarket_amount REAL NOT NULL,
                    votium_votes_per_usd REAL,votemarket_votes_per_usd REAL,votium_votes REAL NOT NULL,votemarket_votes REAL NOT NULL,
                    gauge_data TEXT NOT NULL,transaction_hash TEXT NOT NULL,block_number INTEGER NOT NULL,timestamp INTEGER NOT NULL,
                    date_str TEXT NOT NULL,period_start INTEGER NOT NULL,log_index INTEGER,
                    UNIQUE(protocol,transaction_hash,log_index));''')
            c.execute('INSERT INTO _migration_meta VALUES (?,?)',('manifest',json.dumps(dict(schema_version=1,snapshot_sha256=IMPORT_ID,
                status='data_verified',application_ready=False,alerts_enabled=False))))
            c.commit()
        self.store=Store(self.path,IMPORT_ID,rehearsal=True)
        worker.prepare(self.store)
        self.chain=Chain()
        self.adapter=self.chain.adapter()
        self.generation=0
        self.sent=[]

    def seed(self,adapter=None,period=PERIOD):
        adapter=adapter or self.adapter
        previous=period//(WEEK//10)-1
        def write(c):
            c.execute('INSERT INTO incentive_checkpoints VALUES (?,?,?,?,?,?)',(adapter.PROTOCOL,1,period,period,previous,worker.hex_value(block_hash(previous))))
            notifications.add_stream(c,adapter.STREAM,previous,worker.hex_value(block_hash(previous)))
        self.store.write(write)

    def activate(self,floor=999):
        with closing(sqlite3.connect(self.path)) as c:
            c.execute('PRAGMA journal_mode=WAL')
            manifest=json.loads(c.execute("SELECT value FROM _migration_meta WHERE key='manifest'").fetchone()[0])
            manifest.update(status='ready',application_ready=True,alerts_enabled=True)
            c.execute("UPDATE _migration_meta SET value=? WHERE key='manifest'",(json.dumps(manifest),))
            c.commit()
        self.store=Store(self.path,IMPORT_ID)
        self.store.write(lambda c:notifications.enable(c,self.adapter.STREAM,floor,worker.hex_value(block_hash(floor))))
        self.generation=notifications.start_session(self.store,self.adapter.STREAM,floor,worker.hex_value(block_hash(floor)))

    def rows(self,table):
        return self.store.read(lambda c:[dict(row) for row in c.execute(f'SELECT * FROM {table}')])

    def state(self,adapter=None):
        return self.store.read(lambda c:worker.checkpoint(c,(adapter or self.adapter).PROTOCOL))

    def send(self,*args):
        self.sent.append(args)

    def scan(self,send=None):
        return worker.scan_once(self.store,self.chain,self.adapter,self.generation,send or self.send)

    def imported_boundary(self):
        self.chain.logs=[transfer(self.adapter.TOKEN)]
        item=worker.collect(self.adapter,self.chain,PERIOD,self.chain.get_block('finalized'))[0][0]
        row=self.adapter.build_record(item['event'],item['block'],item['receipt'],item['effective_block'])
        row.update(total_incentives=987.123456789,gauge_data={'historical_price':3.14159},votium_votes_per_usd=None)
        self.store.write(lambda c:worker.insert_row(c,row))
        self.adapter.build_record.reset_mock()

    def test_boundary_retains_observations_without_repricing(self):
        self.imported_boundary()
        original=self.rows('incentives')
        self.adapter.build_record.side_effect=AssertionError('Historical calculations must not run')
        result=worker.adopt_boundary(self.store,self.chain,self.adapter)
        self.assertEqual(result['matched_events'],1)
        self.assertEqual(self.rows('incentives'),original)
        self.assertEqual(self.state()['next_period'],PERIOD+WEEK)
        self.assertEqual(self.rows('notification_decisions'),[])
        self.adapter.build_record.assert_not_called()

    def test_boundary_adds_missing_event_and_rejects_unmatched_import(self):
        self.imported_boundary()
        self.chain.logs.append(transfer(self.adapter.TOKEN,index=1))
        result=worker.adopt_boundary(self.store,self.chain,self.adapter)
        self.assertEqual(result['missing_boundary_events'],1)
        self.assertEqual(len(self.rows('incentives')),2)
        self.assertEqual(self.adapter.build_record.call_count,1)

    def test_invalid_import_boundary_stops_without_state(self):
        self.imported_boundary()
        self.store.write(lambda c:c.execute('UPDATE incentives SET timestamp=timestamp+1'))
        with self.assertRaisesRegex(RuntimeError,'does not match chain'):
            worker.adopt_boundary(self.store,self.chain,self.adapter)
        self.assertEqual(self.rows('incentive_checkpoints'),[])
        self.assertEqual(self.rows('notification_streams'),[])

    def test_empty_week_is_durable_and_ranges_exclude_adjacent_period(self):
        self.seed()
        self.chain.logs=[transfer(self.adapter.TOKEN,block=999),transfer(self.adapter.TOKEN,block=1010)]
        with patch.object(worker,'CHUNK_SIZE',3):
            self.assertTrue(self.scan())
        self.assertEqual(self.chain.ranges,[(1000,1002),(1003,1005),(1006,1008),(1009,1009)])
        self.assertEqual(self.rows('incentives'),[])
        self.assertEqual(self.state()['next_period'],PERIOD+WEEK)
        self.assertFalse(self.scan())

    def test_silent_catchup_and_protocol_isolation(self):
        self.seed()
        other=self.chain.adapter('yieldbasis')
        self.seed(other)
        self.chain.logs=[transfer(self.adapter.TOKEN),transfer(other.TOKEN)]
        self.scan()
        self.assertEqual([row['protocol'] for row in self.rows('incentives')],['resupply'])
        self.assertEqual(self.state(other)['next_period'],PERIOD)
        self.assertEqual(self.rows('notification_decisions')[0]['status'],'suppressed')
        self.assertEqual(self.sent,[])

    def test_yb_transaction_grouping_survives_repeated_input(self):
        self.adapter=self.chain.adapter('yieldbasis')
        self.seed()
        self.chain.logs=[transfer(self.adapter.TOKEN,index=5),transfer(self.adapter.TOKEN,index=6)]
        self.scan()
        self.scan()
        self.assertEqual(len(self.rows('incentives')),1)
        self.assertEqual(len(self.rows('incentive_events')),1)
        self.assertTrue(self.rows('incentive_events')[0]['event_key'].endswith(':transaction'))

    def test_future_report_sends_after_commit_with_configured_routing(self):
        self.seed()
        self.activate()
        self.chain.logs=[transfer(self.adapter.TOKEN)]
        def send(*args):
            self.assertEqual(self.state()['next_period'],PERIOD+WEEK)
            self.assertEqual(len(self.rows('incentives')),1)
            self.send(*args)
        with patch('incentives.config.resolve_chat_id',return_value=('synthetic','WAVEY_ALERTS')):
            self.scan(send)
        self.assertEqual(self.sent[0][:2],('rsup-incentives','WAVEY_ALERTS'))
        self.scan()
        self.assertEqual(len(self.sent),1)

    def test_insert_and_calculation_failure_leave_period_and_alerts_unchanged(self):
        self.seed()
        self.activate()
        self.chain.logs=[transfer(self.adapter.TOKEN,index=0),transfer(self.adapter.TOKEN,index=1)]
        insert=worker.insert_row
        def fail(c,row):
            insert(c,row)
            if row['log_index']==1:
                raise RuntimeError('failed insert')
        with patch.object(worker,'insert_row',side_effect=fail),self.assertRaisesRegex(RuntimeError,'failed insert'):
            self.scan()
        for table in ('incentives','incentive_events','notification_decisions'):
            self.assertEqual(self.rows(table),[])
        self.adapter.build_record.side_effect=OSError('calculation failed')
        with self.assertRaisesRegex(OSError,'calculation failed'):
            self.scan()
        self.assertEqual(self.state()['next_period'],PERIOD)
        self.assertEqual(self.sent,[])

    def test_uncertain_delivery_restart_and_outage_do_not_replay(self):
        self.seed()
        self.activate()
        self.chain.logs=[transfer(self.adapter.TOKEN)]
        def timeout(*args):
            self.send(*args)
            raise TimeoutError('uncertain')
        with self.assertRaisesRegex(RuntimeError,'automatic retry is disabled'):
            self.scan(timeout)
        self.chain.height=1022
        self.chain.logs.append(transfer(self.adapter.TOKEN,block=1015))
        self.generation=notifications.start_session(self.store,self.adapter.STREAM,1020,worker.hex_value(block_hash(1020)))
        self.scan(timeout)
        self.assertEqual(len(self.sent),1)
        self.assertEqual([row['status'] for row in self.rows('notification_decisions')],['uncertain','suppressed'])

    def test_missing_progress_rpc_reorg_and_stale_worker_fail_closed(self):
        with self.assertRaisesRegex(RuntimeError,'checkpoint missing'):
            self.scan()
        self.seed()
        self.chain.fail=True
        with self.assertRaisesRegex(OSError,'RPC failed'):
            self.scan()
        self.chain.fail=False
        self.chain.hash_changes[999]=block_hash(555)
        with self.assertRaisesRegex(RuntimeError,'checkpoint block changed'):
            self.scan()
        self.chain.hash_changes.clear()
        notifications.start_session(self.store,self.adapter.STREAM,1014,worker.hex_value(block_hash(1014)))
        with self.assertRaisesRegex(RuntimeError,'concurrently'):
            self.scan()
        self.assertEqual(self.state()['next_period'],PERIOD)

    def test_activation_requires_completed_weeks_and_uses_latest_head(self):
        self.seed()
        self.activate()
        with self.assertRaisesRegex(RuntimeError,'silent catch-up'):
            worker.enable_future_alerts(self.store,self.chain,self.adapter)
        self.scan()
        worker.enable_future_alerts(self.store,self.chain,self.adapter)
        self.assertEqual(self.store.read(lambda c:notifications.state(c,self.adapter.STREAM))['floor_block'],1014)

    def test_manual_correction_is_atomic_and_refuses_stale_inputs(self):
        self.imported_boundary()
        original=self.store.read(corrections.read_rows)
        row=original[0]
        update=dict(id=row['id'],new_total=200.,new_votium_amount=80.,new_votemarket_amount=120.,
            new_votium_votes=800.,new_votemarket_votes=1200.,new_votium_votes_per_usd=None,
            new_votemarket_votes_per_usd=20.,new_gauge_data={'corrected':{'bias':1.5}})
        corrections.apply_updates(self.store,original,[update])
        after=self.store.read(corrections.read_rows)
        self.assertEqual(after[0]['total_incentives'],200.)
        self.assertEqual(after[0]['gauge_data'],{'corrected':{'bias':1.5}})
        self.assertEqual(after[0]['transaction_hash'],row['transaction_hash'])
        with self.assertRaisesRegex(RuntimeError,'changed during calculation'):
            corrections.apply_updates(self.store,original,[update])
        with self.assertRaisesRegex(RuntimeError,'exactly once'):
            corrections.apply_updates(self.store,after,[update,update])
        self.assertEqual(self.store.read(corrections.read_rows),after)
        self.assertEqual(self.rows('notification_decisions'),[])


def raw_transfer(token,sender,receiver,value):
    return dict(address=token,topics=[HexBytes('0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef'),
        HexBytes(bytes.fromhex(sender.removeprefix('0x')).rjust(32,b'\0')),HexBytes(bytes.fromhex(receiver.removeprefix('0x')).rjust(32,b'\0'))],
        data=HexBytes((value*10**18).to_bytes(32,'big')))


class ProtocolCalculationTests(unittest.TestCase):
    def test_rsup_multisig_filters_patterns_and_prisma_edit_are_preserved(self):
        logs=[raw_transfer(rsup.RSUP,rsup.MULTISIG,rsup.VOTIUM,40),
              raw_transfer(rsup.RSUP,rsup.MULTISIG,rsup.VOTIUM_FEE,2),
              raw_transfer(rsup.RSUP,rsup.EC,rsup.VOTIUM,1000),
              raw_transfer(rsup.RSUP,rsup.MULTISIG,rsup.CONVEX_DEPLOYER,55),
              raw_transfer(rsup.RSUP,rsup.MULTISIG,rsup.VOTEMARKET_FACTORY,60)]
        ec=SimpleNamespace(functions=SimpleNamespace(getEpoch=lambda:SimpleNamespace(call=lambda **kwargs:9)))
        with patch.object(rsup,'ec',ec,create=True),patch.object(rsup,'calculate_efficiency',return_value=(None,None,4.,6.,{})):
            row=rsup.build_record(transfer(rsup.TOKEN),dict(timestamp=PERIOD+100),dict(logs=logs),1011)
        self.assertEqual((row['votium_amount'],row['votemarket_amount'],row['total_incentives']),(42.,60.,102.))
        self.assertEqual(rsup.CURVE_VOTERS['PRISMA'],'0x490b8C6007fFa5d3728A49c2ee199e51f05D2F7e')

    def test_rsup_bias_deducts_prisma_and_rpc_failure_is_not_zero_data(self):
        functions=SimpleNamespace(
            vote_user_slopes=lambda voter,gauge:SimpleNamespace(call=lambda **kwargs:(10**18*(2 if voter==rsup.CURVE_VOTERS['CONVEX'] else 1),0,PERIOD+10)),
            points_weight=lambda *args:SimpleNamespace(call=lambda **kwargs:(100*10**18,0)),
            gauge_relative_weight=lambda *args:SimpleNamespace(call=lambda **kwargs:10**17))
        with patch.object(rsup,'gauge_controller',SimpleNamespace(functions=functions),create=True),patch.object(rsup,'RESUPPLY_GAUGES',{'gauge':'Gauge'}),patch.object(rsup,'get_token_price',return_value=2.):
            result=rsup.calculate_efficiency(1000,PERIOD,100.,40.)
            self.assertEqual(result[2:4],(20.,70.))
            self.assertEqual(result[4]['Gauge']['prisma_bias'],10.)
            functions.points_weight=Mock(side_effect=OSError('RPC failed'))
            with self.assertRaisesRegex(OSError,'RPC failed'):
                rsup.calculate_efficiency(1000,PERIOD,100.,40.)

    def test_yb_helper_splits_and_optional_price_keep_existing_representation(self):
        logs=[raw_transfer(yb.YB,yb.DEPOSIT_DIVIDER,yb.VOTIUM_HELPER,40),raw_transfer(yb.YB,yb.DEPOSIT_DIVIDER,yb.VOTEMARKET_HELPER,60)]
        def decode(log):
            return dict(args=dict(sender='0x'+log['topics'][1].hex()[-40:],receiver='0x'+log['topics'][2].hex()[-40:],value=int.from_bytes(log['data'],'big')))
        token=SimpleNamespace(events=SimpleNamespace(Transfer=lambda:SimpleNamespace(process_log=decode)))
        with patch.object(yb,'yb',token,create=True),patch.object(yb,'calculate_efficiency',return_value=(None,None,400.,600.,{})):
            row=yb.build_record(transfer(yb.TOKEN),dict(timestamp=PERIOD+100),dict(logs=logs),1011)
        self.assertEqual((row['votium_amount'],row['votemarket_amount'],row['total_incentives']),(40.,60.,100.))
        self.assertIsNone(row['votium_votes_per_usd'])
        self.assertIsNone(row['epoch'])
        self.assertIn('YB Incentives Report',yb.render_message(row))


if __name__ == '__main__':
    unittest.main()
