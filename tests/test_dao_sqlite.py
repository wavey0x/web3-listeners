from contextlib import closing
import json
from pathlib import Path
import sqlite3
from tempfile import TemporaryDirectory
from types import SimpleNamespace
import unittest
from unittest.mock import patch

from hexbytes import HexBytes

from data_fetchers import resupply_dao as dao
import notifications
from sqlite_store import Store
import resupply

IMPORT_ID='d'*64
VOTER=dao.VOTER_ADDRESSES[0]
ACCOUNT='0x1111111111111111111111111111111111111111'


def block_hash(number):
    return HexBytes(number.to_bytes(32,'big'))


def event(name,block=10,index=0,proposal='1',address=VOTER,**values):
    args=(dict(account=ACCOUNT,id=int(proposal),epoch=9,quorumWeight=100) if name=='ProposalCreated' else
          dict(account=ACCOUNT,id=int(proposal),weightYes=2_000_000,weightNo=0) if name=='VoteCast' else
          dict(proposalId=int(proposal),description='Changed description'))
    args.update(values)
    return dict(event=name,blockNumber=block,blockHash=block_hash(block),transactionHash=block_hash(block+1000),
                logIndex=index,address=address,args=args)


class Chain:
    def __init__(self):
        self.eth=self
        self.chain_id=1
        self.height=12
        self.latest=14
        self.logs=[]
        self.fail_name=None
        self.hash_changes={}
        self.timestamps={}
        self.contracts={VOTER:self.contract(VOTER)}

    def contract(self,address):
        def logs(name,start,end):
            if name==self.fail_name:
                raise OSError('RPC failed')
            return [e for e in self.logs if e['event']==name and e['address']==address and start<=e['blockNumber']<=end]
        events=SimpleNamespace(**{name:SimpleNamespace(get_logs=lambda fromBlock,toBlock,name=name:logs(name,fromBlock,toBlock)) for name in dao.EVENTS})
        return SimpleNamespace(events=events,functions=SimpleNamespace(proposalDescription=lambda proposal:SimpleNamespace(call=lambda **kwargs:'Description')))

    def get_block(self,number):
        number=self.height if number=='finalized' else self.latest if number=='latest' else number
        return dict(number=number,hash=self.hash_changes.get(number,block_hash(number)),timestamp=self.timestamps.get(number,1700000000+number))

    def get_transaction_receipt(self,tx):
        logs=sorted([e for e in self.logs if e['transactionHash']==tx],key=lambda e:e['logIndex'])
        return dict(transactionHash=tx,blockHash=logs[0]['blockHash'],logs=logs)


class DaoTests(unittest.TestCase):
    def setUp(self):
        temp=TemporaryDirectory()
        self.addCleanup(temp.cleanup)
        self.path=Path(temp.name)/'shared.sqlite3'
        with closing(sqlite3.connect(self.path)) as c:
            c.executescript('''PRAGMA user_version=1;
                CREATE TABLE _migration_meta(key TEXT PRIMARY KEY,value TEXT NOT NULL);
                CREATE TABLE resupply_proposals(id INTEGER PRIMARY KEY AUTOINCREMENT,proposal_id TEXT NOT NULL,status TEXT NOT NULL,
                    description TEXT,proposer TEXT,start_time INTEGER,end_time INTEGER,yes_votes REAL,no_votes REAL,quorum INTEGER,
                    block INTEGER,txn_hash TEXT,voter_address TEXT,timestamp INTEGER,date_str TEXT,last_updated INTEGER,
                    execution_time INTEGER,ending_soon_alert_sent INTEGER DEFAULT 0,UNIQUE(proposal_id,voter_address));
                CREATE TABLE resupply_votes(id INTEGER PRIMARY KEY AUTOINCREMENT,proposal_id TEXT,voter TEXT,support INTEGER,
                    weight REAL,reason TEXT,block INTEGER,txn_hash TEXT,timestamp INTEGER,date_str TEXT,log_index INTEGER,
                    UNIQUE(txn_hash,log_index));
                CREATE TABLE resupply_scanner_progress(id INTEGER PRIMARY KEY AUTOINCREMENT,last_scanned_block INTEGER,updated_at INTEGER);''')
            c.execute('INSERT INTO _migration_meta VALUES (?,?)',('manifest',json.dumps(dict(schema_version=1,snapshot_sha256=IMPORT_ID,
                status='data_verified',application_ready=False,alerts_enabled=False))))
            c.commit()
        self.store=Store(self.path,IMPORT_ID,rehearsal=True)
        dao.prepare(self.store)
        self.chain=Chain()
        self.generation=0
        self.sent=[]

    def seed(self,start=10):
        def write(c):
            c.execute('INSERT INTO dao_checkpoint VALUES (?,?,?,?,?,?)',('dao',1,dao.contract_identity(self.chain.contracts),start,start,dao.hex_value(block_hash(start-1))))
            notifications.add_stream(c,'dao',start-1,dao.hex_value(block_hash(start-1)))
        self.store.write(write)

    def activate(self,floor=9):
        with closing(sqlite3.connect(self.path)) as c:
            c.execute('PRAGMA journal_mode=WAL')
            manifest=json.loads(c.execute("SELECT value FROM _migration_meta WHERE key='manifest'").fetchone()[0])
            manifest.update(status='ready',application_ready=True,alerts_enabled=True)
            c.execute("UPDATE _migration_meta SET value=? WHERE key='manifest'",(json.dumps(manifest),))
            c.commit()
        self.store=Store(self.path,IMPORT_ID)
        self.store.write(lambda c:notifications.enable(c,'dao',floor,dao.hex_value(block_hash(floor))))
        self.generation=notifications.start_session(self.store,'dao',floor,dao.hex_value(block_hash(floor)))

    def rows(self,table):
        return self.store.read(lambda c:[dict(r) for r in c.execute(f'SELECT * FROM {table}')])

    def state(self):
        return self.store.read(dao.checkpoint)

    def send(self,*args):
        self.sent.append(args)

    def scan(self,send=None):
        return dao.scan_once(self.store,self.chain,self.chain.contracts,self.generation,send or self.send)

    def poll(self,baseline=False,activate=False,send=None):
        return dao.poll_statuses(self.store,self.chain,self.generation,send or self.send,baseline=baseline,activate=activate)

    def proposal(self,*,end=None,status='open',yes=200,no=0,flag=0,proposal='1',address=VOTER):
        timestamp=self.chain.get_block(10)['timestamp']
        end=timestamp+dao.VOTING_PERIOD if end is None else end
        self.store.write(lambda c:c.execute('''INSERT INTO resupply_proposals
            (proposal_id,status,description,proposer,start_time,end_time,yes_votes,no_votes,quorum,block,txn_hash,voter_address,
             timestamp,date_str,last_updated,ending_soon_alert_sent) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)''',
            (proposal,status,'Historical description',ACCOUNT,timestamp,end,yes,no,100,10,block_hash(1010).hex(),address,timestamp,dao.date_string(timestamp),10,flag)))

    def advance_time(self,now):
        self.chain.height+=1
        self.chain.latest=max(self.chain.latest,self.chain.height+2)
        self.chain.timestamps[self.chain.height]=now
        self.scan()

    def test_all_event_types_are_ordered_atomic_and_silent_on_import(self):
        self.seed()
        self.chain.logs=[event('ProposalCreated',index=0),event('VoteCast',index=1),
            event('ProposalDescriptionUpdated',index=2),event('ProposalExecuted',index=3)]
        self.scan()
        row=self.rows('resupply_proposals')[0]
        self.assertEqual((row['status'],row['description'],row['yes_votes']),('executed','Changed description',2_000_000.))
        self.assertEqual(len(self.rows('resupply_votes')),1)
        self.assertEqual(len(self.rows('dao_events')),4)
        self.assertEqual(self.rows('resupply_scanner_progress')[0]['last_scanned_block'],12)
        self.assertEqual({r['status'] for r in self.rows('notification_decisions')},{'suppressed'})
        self.assertEqual(self.sent,[])
        self.assertFalse(self.scan())

    def test_future_event_claims_follow_committed_data_and_cancel_is_scoped(self):
        other='0x2222222222222222222222222222222222222222'
        self.chain.contracts[other]=self.chain.contract(other)
        self.seed()
        self.activate()
        self.chain.logs=[event('ProposalCreated',index=0),event('ProposalCreated',index=1,address=other),event('ProposalCancelled',index=2)]
        def send(*args):
            self.assertEqual(self.state()['next_block'],13)
            self.assertEqual(len(self.rows('resupply_proposals')),2)
            self.send(*args)
        self.scan(send)
        self.assertEqual([r['status'] for r in self.rows('resupply_proposals')],['cancelled','open'])
        self.assertEqual(len(self.sent),3)
        self.assertEqual(self.sent[0][:2],('dao','RESUPPLY_ALERTS'))

    def test_any_event_rpc_or_write_failure_leaves_entire_range_unchanged(self):
        self.seed()
        self.activate()
        self.chain.logs=[event('ProposalCreated')]
        self.chain.fail_name='ProposalExecuted'
        with self.assertRaisesRegex(OSError,'RPC failed'):
            self.scan()
        self.chain.fail_name=None
        original=dao.apply_event
        def fail(*args):
            original(*args)
            raise RuntimeError('failed write')
        with patch.object(dao,'apply_event',side_effect=fail),self.assertRaisesRegex(RuntimeError,'failed write'):
            self.scan()
        for table in ('resupply_proposals','resupply_votes','dao_events','notification_decisions','resupply_scanner_progress'):
            self.assertEqual(self.rows(table),[])
        self.assertEqual(self.state()['next_block'],10)
        self.assertEqual(self.sent,[])

    def test_legacy_null_vote_is_preserved_without_counting_it_twice(self):
        self.proposal(yes=2_000_000)
        self.chain.logs=[event('VoteCast')]
        timestamp=self.chain.get_block(10)['timestamp']
        self.store.write(lambda c:c.execute('''INSERT INTO resupply_votes(proposal_id,voter,support,weight,reason,block,txn_hash,timestamp,date_str,log_index)
            VALUES ('1',?,1,2000000,'',10,?,?,?,NULL)''',(ACCOUNT,block_hash(1010).hex(),timestamp,dao.date_string(timestamp))))
        original=self.rows('resupply_votes')
        self.seed()
        self.activate()
        self.scan()
        self.assertEqual(self.rows('resupply_votes'),original)
        self.assertEqual(self.rows('resupply_proposals')[0]['yes_votes'],2_000_000.)
        self.assertEqual(self.sent,[])

    def test_one_legacy_vote_cannot_consume_two_distinct_events(self):
        self.proposal(yes=2_000_000)
        self.chain.logs=[event('VoteCast',index=0),event('VoteCast',index=1)]
        timestamp=self.chain.get_block(10)['timestamp']
        self.store.write(lambda c:c.execute('''INSERT INTO resupply_votes(proposal_id,voter,support,weight,reason,block,txn_hash,timestamp,date_str,log_index)
            VALUES ('1',?,1,2000000,'',10,?,?,?,NULL)''',(ACCOUNT,block_hash(1010).hex(),timestamp,dao.date_string(timestamp))))
        self.seed()
        self.scan()
        self.assertEqual(len(self.rows('resupply_votes')),2)
        self.assertEqual(self.rows('resupply_proposals')[0]['yes_votes'],4_000_000.)

    def test_small_votes_do_not_alert_and_missing_proposal_is_an_error(self):
        self.seed()
        self.activate()
        self.chain.logs=[event('VoteCast',weightYes=5)]
        with self.assertRaisesRegex(RuntimeError,'no saved proposal'):
            self.scan()
        self.proposal(yes=0)
        self.scan()
        self.assertEqual(self.rows('resupply_proposals')[0]['yes_votes'],5.)
        self.assertEqual(self.sent,[])

    def test_adoption_uses_latest_progress_record_not_maximum(self):
        self.store.write(lambda c:c.executemany('INSERT INTO resupply_scanner_progress(last_scanned_block,updated_at) VALUES (?,?)',[(11,1),(9,2)]))
        before=self.rows('resupply_scanner_progress')
        report=dao.adopt_boundary(self.store,self.chain,self.chain.contracts)
        self.assertEqual(report['next_block'],10)
        self.assertEqual(report['boundary_source'],'latest-scanner-record')
        self.assertEqual(self.rows('resupply_scanner_progress'),before)

    def test_restart_uses_quiet_baseline_for_overdue_status_and_reminder(self):
        self.seed()
        self.activate()
        now=self.chain.get_block(12)['timestamp']
        self.proposal(end=now+100)
        self.proposal(end=now-dao.EXECUTION_DELAY-1,proposal='2',status='execution_delay')
        self.proposal(end=now-dao.EXECUTION_DEADLINE-1,proposal='3',status='executable')
        self.scan()
        with self.assertRaisesRegex(RuntimeError,'quiet baseline'):
            self.poll()
        self.poll(baseline=True)
        self.poll()
        self.assertEqual(self.sent,[])
        rows=self.rows('resupply_proposals')
        self.assertEqual([r['status'] for r in rows],['open','executable','expired'])
        self.assertTrue(rows[0]['ending_soon_alert_sent'])
        self.generation=notifications.start_session(self.store,'dao',14,dao.hex_value(block_hash(14)))
        with self.assertRaisesRegex(RuntimeError,'quiet baseline'):
            self.poll()
        self.poll(baseline=True)
        self.assertEqual(self.sent,[])

    def test_new_reminder_and_status_transition_attempt_once(self):
        self.seed()
        self.activate()
        now=self.chain.get_block(12)['timestamp']
        end=now+dao.DAY_IN_SECONDS+100
        self.proposal(end=end)
        self.scan()
        self.poll(baseline=True)
        self.chain.height=15
        self.chain.timestamps[15]=now+101
        self.scan()
        self.poll()
        self.poll()
        self.assertEqual(len(self.sent),1)
        self.assertIn('Ending Soon',self.sent[0][2])
        self.advance_time(end+1)
        self.poll()
        self.assertEqual(len(self.sent),2)
        self.assertIn('Proposal Passed',self.sent[-1][2])
        self.advance_time(end+dao.EXECUTION_DELAY+1)
        self.poll()
        self.assertEqual(len(self.sent),3)
        self.assertIn('Ready for Execution',self.sent[-1][2])

    def test_imported_reminder_flag_is_never_reset(self):
        self.seed()
        self.activate()
        now=self.chain.get_block(12)['timestamp']
        self.proposal(end=now+dao.DAY_IN_SECONDS+100,flag=1)
        self.scan()
        self.poll(baseline=True)
        self.chain.height=15
        self.chain.timestamps[15]=now+101
        self.scan()
        self.poll()
        self.assertEqual(self.sent,[])
        self.assertTrue(self.rows('resupply_proposals')[0]['ending_soon_alert_sent'])

    def test_activation_silently_adopts_polled_state_and_latest_floor(self):
        self.seed()
        self.activate()
        self.proposal(end=self.chain.get_block(12)['timestamp']+1)
        with self.assertRaisesRegex(RuntimeError,'silent chain catch-up'):
            self.poll(baseline=True,activate=True)
        self.scan()
        self.poll(baseline=True,activate=True)
        self.assertEqual(self.store.read(lambda c:notifications.state(c,'dao'))['floor_block'],14)
        self.assertEqual(self.sent,[])
        self.assertTrue(self.rows('dao_poll_state')[0]['reminder_consumed'])

    def test_uncertain_state_alert_is_not_retried_after_restart(self):
        self.seed()
        self.activate()
        now=self.chain.get_block(12)['timestamp']
        self.proposal(end=now+100,yes=0)
        self.scan()
        self.poll(baseline=True)
        self.chain.height=15
        self.chain.timestamps[15]=now+101
        self.scan()
        def timeout(*args):
            self.send(*args)
            raise TimeoutError('uncertain')
        with self.assertRaisesRegex(RuntimeError,'automatic retry is disabled'):
            self.poll(send=timeout)
        self.generation=notifications.start_session(self.store,'dao',17,dao.hex_value(block_hash(17)))
        self.poll(baseline=True,send=timeout)
        self.poll(send=timeout)
        self.assertEqual(len(self.sent),1)
        self.assertEqual(self.rows('notification_decisions')[0]['status'],'uncertain')

    def test_changed_hash_contract_set_and_missing_checkpoint_stop_indexing(self):
        with self.assertRaisesRegex(RuntimeError,'checkpoint missing'):
            self.scan()
        self.seed()
        self.chain.hash_changes[9]=block_hash(999)
        with self.assertRaisesRegex(RuntimeError,'checkpoint block changed'):
            self.scan()
        self.chain.hash_changes.clear()
        self.chain.contracts['0x2222']=self.chain.contract('0x2222')
        with self.assertRaisesRegex(RuntimeError,'voter contracts differ'):
            self.scan()
        self.assertEqual(self.state()['next_block'],10)

    def test_failed_poll_decision_rolls_back_reminder_flag_and_observation(self):
        self.seed()
        self.activate()
        now=self.chain.get_block(12)['timestamp']
        self.proposal(end=now+dao.DAY_IN_SECONDS+100)
        self.scan()
        self.poll(baseline=True)
        before=self.rows('dao_poll_checkpoint')
        self.chain.height=15
        self.chain.timestamps[15]=now+101
        self.scan()
        with patch.object(notifications,'decide',side_effect=RuntimeError('decision failed')),self.assertRaisesRegex(RuntimeError,'decision failed'):
            self.poll()
        self.assertFalse(self.rows('resupply_proposals')[0]['ending_soon_alert_sent'])
        self.assertFalse(self.rows('dao_poll_state')[0]['reminder_consumed'])
        self.assertEqual(self.rows('dao_poll_checkpoint'),before)
        self.assertEqual(self.sent,[])

    def test_bundle_validates_all_selected_workers_before_starting_any(self):
        self.seed()
        with patch.object(dao,'main') as entry,patch.object(dao,'runtime') as rpc:
            prepared=resupply.prepare_workers(self.store,['dao'])
            self.assertEqual(prepared,[('dao',entry)])
            with self.assertRaisesRegex(RuntimeError,'Retention schema'):
                resupply.prepare_workers(self.store,['dao','retention'])
            entry.assert_not_called()
            rpc.assert_not_called()


class LifecycleTests(unittest.TestCase):
    def test_bundle_requires_explicit_valid_worker_selection(self):
        for value in (None,'','dao,dao','unknown','dao,'):
            with self.subTest(value=value),self.assertRaises(ValueError):
                resupply.selected_workers(value)
        self.assertEqual(resupply.selected_workers('retention, dao'),['retention','dao'])

    def test_retired_entrypoints_fail_without_database_or_network_setup(self):
        from data_fetchers import ybs_listener
        from scripts import recreate_tables,recreate_weight_tracker_tables
        for function in (ybs_listener.main,recreate_tables.recreate_tables,recreate_weight_tracker_tables.recreate_tables):
            with self.subTest(function=function),self.assertRaisesRegex(RuntimeError,'retired'):
                function()


if __name__=='__main__':
    unittest.main()
