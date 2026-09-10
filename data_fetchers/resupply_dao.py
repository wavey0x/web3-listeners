"""SQLite DAO events, quiet state adoption, and durable public-notification decisions."""

import argparse
from datetime import datetime, timezone
import json
import math
import os
from pathlib import Path
import sys
import time

sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
from sqlite_store import Store
import notifications

POLL_INTERVAL = 10
EXECUTION_DELAY = 60 * 60 * 24
EXECUTION_DEADLINE = 21 * 24 * 60 * 60
VOTING_PERIOD = 60 * 60 * 24 * 7
DAY_IN_SECONDS = 24 * 60 * 60
VOTE_ALERT_POWER_THRESHOLD = 1000000
PERMASTAKERS = {'0x12341234B35c8a48908c716266db79CAeA0100E8': 'Yearn', '0xCCCCCccc94bFeCDd365b4Ee6B86108fC91848901': 'Convex'}
VOTER_ADDRESSES = ['0x11111111063874cE8dC6232cb5C1C849359476E6']

STREAM = 'dao'
CHUNK_SIZE = 5000
INITIAL_BLOCK = 22_200_001
REGISTRY = '0x10101010E0C3171D894B71B3400668aF311e7D94'
EVENTS = ('ProposalCreated','VoteCast','ProposalCancelled','ProposalExecuted','ProposalDescriptionUpdated')
TERMINAL = ('cancelled','executed','failed','expired')


def hex_value(value):
    return (value if isinstance(value,str) else value.hex()).lower().removeprefix('0x')


def date_string(timestamp):
    return datetime.fromtimestamp(timestamp,timezone.utc).strftime('%Y-%m-%d %H:%M UTC')


def prepare(store):
    if not store.rehearsal:
        raise RuntimeError('Prepare DAO state in an inactive import')
    def create(c):
        notifications.prepare(c)
        c.execute('''CREATE TABLE dao_checkpoint (
            stream TEXT PRIMARY KEY,chain_id INTEGER NOT NULL,contracts TEXT NOT NULL,
            initial_block INTEGER NOT NULL,next_block INTEGER NOT NULL,previous_hash TEXT NOT NULL,
            CHECK(next_block>=initial_block))''')
        c.execute('''CREATE TABLE dao_events (
            event_key TEXT PRIMARY KEY,voter_address TEXT NOT NULL,proposal_id TEXT NOT NULL,
            event_name TEXT NOT NULL,block INTEGER NOT NULL)''')
        c.execute('''CREATE TABLE dao_poll_state (
            voter_address TEXT NOT NULL,proposal_id TEXT NOT NULL,status TEXT NOT NULL,
            reminder_consumed INTEGER NOT NULL CHECK(reminder_consumed IN (0,1)),observed_at INTEGER NOT NULL,
            PRIMARY KEY(voter_address,proposal_id))''')
        c.execute('''CREATE TABLE dao_poll_checkpoint (
            stream TEXT PRIMARY KEY,generation INTEGER NOT NULL,block INTEGER NOT NULL,
            block_hash TEXT NOT NULL,observed_at INTEGER NOT NULL)''')
        c.execute("INSERT INTO _migration_meta VALUES ('dao_schema_version','1')")
    store.write(create)


def checkpoint(c):
    version=c.execute("SELECT value FROM _migration_meta WHERE key='dao_schema_version'").fetchone()
    if version is None or version[0]!='1':
        raise RuntimeError('DAO schema has not been explicitly prepared')
    row=c.execute('SELECT * FROM dao_checkpoint WHERE stream=?',(STREAM,)).fetchone()
    if row is None:
        raise RuntimeError('DAO checkpoint missing; boundary adoption is required')
    return dict(row)


def contract_identity(contracts):
    return json.dumps(sorted(address.lower() for address in contracts))


def collect(w3,contracts,start,end):
    end_hash=hex_value(w3.eth.get_block(end)['hash'])
    raw=[]
    for address,contract in contracts.items():
        for name in EVENTS:
            for event in getattr(contract.events,name).get_logs(fromBlock=start,toBlock=end):
                raw.append((event,name,address,contract))
    blocks,receipts,descriptions,items={},{},{},[]
    for event,name,address,contract in sorted(raw,key=lambda value:(value[0]['blockNumber'],value[0]['logIndex'])):
        number=event['blockNumber']
        if not start<=number<=end or event.get('removed',False) or hex_value(event['address'])!=hex_value(address):
            raise RuntimeError('RPC returned an invalid DAO event')
        if number not in blocks:
            blocks[number]=w3.eth.get_block(number)
        block=blocks[number]
        if hex_value(block['hash'])!=hex_value(event['blockHash']):
            raise RuntimeError('DAO event block changed during collection')
        tx=hex_value(event['transactionHash'])
        if tx not in receipts:
            receipts[tx]=w3.eth.get_transaction_receipt(event['transactionHash'])
        receipt=receipts[tx]
        if hex_value(receipt['blockHash'])!=hex_value(event['blockHash']) or hex_value(receipt['transactionHash'])!=tx:
            raise RuntimeError('DAO receipt changed during collection')
        positions=[i for i,log in enumerate(receipt['logs']) if log['logIndex']==event['logIndex']
                   and hex_value(log['address'])==hex_value(address)]
        if len(positions)!=1:
            raise RuntimeError('DAO event is missing or ambiguous in its receipt')
        proposal=str(event['args']['id'] if name in ('ProposalCreated','VoteCast') else event['args']['proposalId'])
        description_key=(address,proposal,number)
        if description_key not in descriptions:
            descriptions[description_key]=contract.functions.proposalDescription(int(proposal)).call(block_identifier=number)
        # A description event carries the value at its own log position, even if updated twice in a block.
        description=event['args']['description'] if name=='ProposalDescriptionUpdated' else descriptions[description_key]
        items.append(dict(key=f'1:{address.lower()}:{tx}:{positions[0]}',event=event,name=name,address=address,
                          proposal=proposal,block=block,description=description))
    if hex_value(w3.eth.get_block(end)['hash'])!=end_hash:
        raise RuntimeError('DAO range changed during collection')
    return items,end_hash


def proposal_row(c,address,proposal):
    row=c.execute('SELECT * FROM resupply_proposals WHERE lower(voter_address)=? AND proposal_id=?',
                  (address.lower(),proposal)).fetchone()
    return dict(row) if row is not None else None


def links(row):
    return (f"\n🔗 [Etherscan](https://etherscan.io/tx/{row['txn_hash']}) | "
            f"[Resupply](https://resupply.fi/governance/proposals) | "
            f"[Hippo Army](https://hippo.army/dao/proposal/{int(row['proposal_id'])+9})")


def format_address(address):
    return f'[0x{address[2:5]}...{address[-4:]}](https://etherscan.io/address/{address})'


def quorum_progress(row):
    total=row['yes_votes']+row['no_votes']
    quorum=row['quorum']
    return (100. if total>=quorum else total/quorum*100, max(quorum-total,0))


def event_message(item,row):
    name,args=item['name'],item['event']['args']
    title={'ProposalCreated':'📜 *New Resupply Proposal Created*','VoteCast':'🗳️ *New Vote Cast on Resupply Proposal*',
           'ProposalCancelled':'❌ *Resupply Proposal Cancelled*','ProposalExecuted':'🚀 *Resupply Proposal Executed*',
           'ProposalDescriptionUpdated':'📝 *Resupply Proposal Description Updated*'}[name]
    message=title+f"\n\nProposal {item['proposal']}: {item['description']}\n"
    if name=='ProposalCreated':
        message+=f"\nProposer: {format_address(args['account'])}\nEpoch: {args['epoch']}\n"
        message+=f"Quorum Required: {args['quorumWeight']:,}\nEnds: {date_string(row['end_time'])}\n"
    elif name=='VoteCast':
        voter=args['account']
        message+=f"User: {format_address(voter)}"+(f' ({PERMASTAKERS[voter]})' if voter in PERMASTAKERS else '')+'\n'
        message+=f"Vote: Yes ({args['weightYes']:,.0f})\n" if args['weightYes']>0 else f"Vote: No ({args['weightNo']:,.0f})\n"
        percentage,needed=quorum_progress(row)
        message+=f'Quorum Progress: {percentage:.2f}% | {needed:,.0f} needed\n'
    return message+links(dict(row,txn_hash=item['event']['transactionHash'].hex()))


def apply_event(c,item,used_legacy_votes):
    """Write one event. A None result means its data was already imported."""
    event,args,name=item['event'],item['event']['args'],item['name']
    address,proposal=item['address'],item['proposal']
    number,timestamp=event['blockNumber'],item['block']['timestamp']
    tx=event['transactionHash'].hex()
    row=proposal_row(c,address,proposal)
    if name=='ProposalCreated':
        if row is not None:
            if (hex_value(row['proposer'])!=hex_value(args['account']) or row['start_time']!=timestamp
                    or row['quorum']!=args['quorumWeight']):
                raise RuntimeError('Imported DAO proposal does not match its creation event')
            return None
        c.execute('''INSERT INTO resupply_proposals
            (proposal_id,voter_address,proposer,description,start_time,end_time,status,yes_votes,no_votes,quorum,
             block,txn_hash,timestamp,date_str,last_updated,ending_soon_alert_sent)
            VALUES (?,?,?,?,?,?,'open',0,0,?,?,?,?,?,?,0)''',
            (proposal,address,args['account'],item['description'],timestamp,timestamp+VOTING_PERIOD,args['quorumWeight'],
             number,tx,timestamp,date_string(timestamp),number))
    elif name=='VoteCast':
        weight=float(args['weightYes'] if args['weightYes']>0 else args['weightNo'])
        if not math.isfinite(weight):
            raise ValueError('Non-finite DAO vote weight')
        existing=c.execute('''SELECT * FROM resupply_votes WHERE lower(replace(txn_hash,'0x',''))=?
            AND (log_index=? OR log_index IS NULL) ORDER BY log_index IS NULL,id''',
            (hex_value(tx),event['logIndex'])).fetchall()
        for vote in existing:
            if vote['id'] in used_legacy_votes:
                continue
            matches=(vote['proposal_id']==proposal and hex_value(vote['voter'])==hex_value(args['account'])
                and vote['block']==number and vote['timestamp']==timestamp
                and bool(vote['support'])==(args['weightYes']>0) and vote['weight']==weight)
            if matches:
                if vote['log_index'] is None:
                    used_legacy_votes.add(vote['id'])
                return None
            if vote['log_index'] is not None:
                raise RuntimeError('Imported DAO vote conflicts with its chain event')
        if row is None:
            raise RuntimeError('DAO vote has no saved proposal; reconciliation required')
        c.execute('''INSERT INTO resupply_votes
            (proposal_id,voter,support,weight,reason,block,txn_hash,timestamp,date_str,log_index)
            VALUES (?,?,?,?,'',?,?,?,?,?)''',
            (proposal,args['account'],int(args['weightYes']>0),weight,number,tx,timestamp,date_string(timestamp),event['logIndex']))
        c.execute('UPDATE resupply_proposals SET yes_votes=yes_votes+?,no_votes=no_votes+?,last_updated=? WHERE id=?',
                  (float(args['weightYes']),float(args['weightNo']),number,row['id']))
        if args['weightYes']+args['weightNo']<VOTE_ALERT_POWER_THRESHOLD:
            return None
    else:
        if row is None:
            raise RuntimeError('DAO update has no saved proposal; reconciliation required')
        if row['block']>number:
            return None  # A partially completed source scan already recorded a later update.
        values=dict(block=number,txn_hash=tx,timestamp=timestamp,date_str=date_string(timestamp),last_updated=number)
        if name=='ProposalCancelled':
            values['status']='cancelled'
        elif name=='ProposalExecuted':
            values.update(status='executed',execution_time=timestamp)
        else:
            values['description']=item['description']
        c.execute('UPDATE resupply_proposals SET '+','.join(key+'=?' for key in values)+' WHERE id=?',(*values.values(),row['id']))
    return event_message(item,proposal_row(c,address,proposal))


def record_events(c,items,generation=None):
    claims=[]
    used_legacy_votes=set()
    for item in items:
        inserted=c.execute('INSERT INTO dao_events VALUES (?,?,?,?,?) ON CONFLICT(event_key) DO NOTHING',
            (item['key'],item['address'],item['proposal'],item['name'],item['event']['blockNumber'])).rowcount
        if not inserted:
            continue
        message=apply_event(c,item,used_legacy_votes)
        if message is not None and generation is not None:
            claim=notifications.decide(c,STREAM,generation,item['key'],item['event']['blockNumber'],item['name'],'RESUPPLY_ALERTS',message)
            if claim:
                claims.append((claim,message))
    return claims


def adopt_boundary(store,w3,contracts):
    if not store.rehearsal or w3.eth.chain_id!=1:
        raise RuntimeError('Adopt DAO state in an inactive mainnet import')
    def boundary(c):
        row=c.execute('SELECT last_scanned_block FROM resupply_scanner_progress ORDER BY id DESC LIMIT 1').fetchone()
        if row is not None:
            return row[0],'latest-scanner-record'
        bounds=[c.execute(f'SELECT max(block) FROM {table}').fetchone()[0] for table in ('resupply_proposals','resupply_votes')]
        return max([value for value in bounds if value is not None],default=INITIAL_BLOCK-1),'explicit-legacy-fallback'
    number,source=store.read(boundary)
    if number>w3.eth.get_block('finalized')['number']:
        raise RuntimeError('Wait for the DAO boundary to become finalized')
    items,block_hash=collect(w3,contracts,number,number)
    def adopt(c):
        c.execute('INSERT INTO dao_checkpoint VALUES (?,?,?,?,?,?)',
                  (STREAM,1,contract_identity(contracts),number+1,number+1,block_hash))
        notifications.add_stream(c,STREAM,number,block_hash)
        record_events(c,items)
        return dict(next_block=number+1,boundary_source=source,boundary_events=len(items))
    return store.write(adopt)


def scan_once(store,w3,contracts,generation,send):
    previous=store.read(checkpoint)
    if w3.eth.chain_id!=1 or previous['chain_id']!=1 or previous['contracts']!=contract_identity(contracts):
        raise RuntimeError('DAO chain or voter contracts differ from the adopted checkpoint')
    start=previous['next_block']
    if hex_value(w3.eth.get_block(start-1)['hash'])!=previous['previous_hash']:
        raise RuntimeError('DAO checkpoint block changed; reconciliation required')
    finalized=w3.eth.get_block('finalized')
    if start>finalized['number']:
        return False
    end=min(start+CHUNK_SIZE-1,finalized['number'])
    items,block_hash=collect(w3,contracts,start,end)
    if hex_value(w3.eth.get_block(start-1)['hash'])!=previous['previous_hash']:
        raise RuntimeError('DAO checkpoint changed during collection')
    def commit(c):
        if checkpoint(c)!=previous or notifications.state(c,STREAM)['generation']!=generation:
            raise RuntimeError('DAO checkpoint or session advanced concurrently')
        claims=record_events(c,items,generation)
        c.execute('UPDATE dao_checkpoint SET next_block=?,previous_hash=? WHERE stream=?',(end+1,block_hash,STREAM))
        # Preserve the full source history and its established meaning. New rows represent completed ranges.
        c.execute('INSERT INTO resupply_scanner_progress(last_scanned_block,updated_at) VALUES (?,?)',(end,int(time.time())))
        return claims
    for claim,message in store.write(commit):
        notifications.dispatch(store,claim,message,send)
    return True


def current_status(row,now):
    if row['status'] in TERMINAL:
        return row['status']
    if now<row['end_time']:
        return 'open'
    if row['yes_votes']+row['no_votes']<row['quorum'] or row['yes_votes']<=row['no_votes']:
        return 'failed'
    if now<row['end_time']+EXECUTION_DELAY:
        return 'execution_delay'
    if now<row['end_time']+EXECUTION_DEADLINE:
        return 'executable'
    return 'expired'


def status_message(row,kind):
    titles={'ending-soon':'⚠️ *Resupply Proposal Ending Soon*','passed':'✅ *Resupply Proposal Passed*',
            'failed':'❌ *Resupply Proposal Failed*','executable':'⚡ *Resupply Proposal Ready for Execution*',
            'expired':'⌛ *Resupply Proposal Expired*'}
    message=titles[kind]+f"\n\nProposal {row['proposal_id']}: {row['description']}\n"
    if kind in ('executable','expired'):
        message+=f"Execution Deadline: {date_string(row['end_time']+EXECUTION_DEADLINE)}\n"
    else:
        if kind=='ending-soon':
            message+=f"\nEnds: {date_string(row['end_time'])}\n"
        message+=f"Yes: {row['yes_votes']:,.0f}\nNo: {row['no_votes']:,.0f}\n"
        percentage,needed=quorum_progress(row)
        message+=f'Quorum: {percentage:.2f}%'+(f' | {needed:,.0f} needed' if kind!='passed' else '')+'\n\n'
        if kind=='passed':
            message+='Executable in 24hrs\n'
    return message+links(row)


def poll_statuses(store,w3,generation,send,*,baseline=False,activate=False):
    """Advance current state once; startup/activation adopts existing conditions silently."""
    head=w3.eth.get_block('finalized')
    latest=w3.eth.get_block('latest') if activate else None
    chain_id=w3.eth.chain_id
    now=head['timestamp']
    def commit(c):
        previous=checkpoint(c)
        if chain_id!=1 or previous['chain_id']!=1:
            raise RuntimeError('DAO polling chain does not match its checkpoint')
        if previous['next_block']<=head['number']:
            if activate:
                raise RuntimeError('DAO must finish silent chain catch-up before activation')
            return None  # A new finalized block may arrive between the idle scan and this poll.
        session=notifications.state(c,STREAM)
        if session['generation']!=generation:
            raise RuntimeError('DAO polling session changed')
        saved=c.execute('SELECT * FROM dao_poll_checkpoint WHERE stream=?',(STREAM,)).fetchone()
        if not baseline and (saved is None or saved['generation']!=generation):
            raise RuntimeError('DAO polling needs an explicit quiet baseline for this session')
        if saved is not None and (now<saved['observed_at'] or head['number']<saved['block']):
            raise RuntimeError('DAO polling head moved backwards; reconciliation required')
        if previous['next_block']==head['number']+1 and previous['previous_hash']!=hex_value(head['hash']):
            raise RuntimeError('DAO polling head differs from its indexed checkpoint')
        claims=[]
        for record in c.execute('SELECT * FROM resupply_proposals ORDER BY id').fetchall():
            row=dict(record)
            address=row['voter_address'].lower()
            old=c.execute('SELECT * FROM dao_poll_state WHERE voter_address=? AND proposal_id=?',(address,row['proposal_id'])).fetchone()
            status=current_status(row,now)
            due=status=='open' and 0<row['end_time']-now<=DAY_IN_SECONDS
            consumed=bool(row['ending_soon_alert_sent'] or (old and old['reminder_consumed']))
            kinds=[]
            if baseline:
                # The current condition becomes history, including a reminder already due during downtime.
                consumed=consumed or row['end_time']-DAY_IN_SECONDS<=now
            else:
                if due and not consumed:
                    kinds.append('ending-soon')
                    consumed=True
                old_status=old['status'] if old is not None else row['status']
                if status!=old_status:
                    if status=='execution_delay':
                        kinds.append('passed')
                    elif status in ('failed','executable','expired'):
                        kinds.append(status)
            if status!=row['status'] or (due and consumed and not row['ending_soon_alert_sent']):
                c.execute('UPDATE resupply_proposals SET status=?,ending_soon_alert_sent=?,last_updated=? WHERE id=?',
                    (status,int(bool(row['ending_soon_alert_sent']) or (due and consumed)),now,row['id']))
            c.execute('''INSERT INTO dao_poll_state VALUES (?,?,?,?,?) ON CONFLICT(voter_address,proposal_id)
                DO UPDATE SET status=excluded.status,reminder_consumed=excluded.reminder_consumed,observed_at=excluded.observed_at''',
                (address,row['proposal_id'],status,int(consumed),now))
            for kind in kinds:
                message=status_message(row,kind)
                claim=notifications.decide(c,STREAM,generation,f'proposal:{address}:{row["proposal_id"]}',head['number'],
                                           kind,'RESUPPLY_ALERTS',message)
                if claim:
                    claims.append((claim,message))
        c.execute('''INSERT INTO dao_poll_checkpoint VALUES (?,?,?,?,?) ON CONFLICT(stream)
            DO UPDATE SET generation=excluded.generation,block=excluded.block,block_hash=excluded.block_hash,observed_at=excluded.observed_at''',
            (STREAM,generation,head['number'],hex_value(head['hash']),now))
        if activate:
            if not baseline:
                raise RuntimeError('DAO activation requires quiet state adoption')
            notifications.enable(c,STREAM,latest['number'],hex_value(latest['hash']))
        return claims
    claims=store.write(commit)
    if claims is None:
        return False
    for claim,message in claims:
        notifications.dispatch(store,claim,message,send)
    return True


def runtime():
    from web3 import Web3
    w3=Web3(Web3.HTTPProvider(os.environ['WEB3_PROVIDER_URI'],request_kwargs={'timeout':60}))
    if w3.eth.chain_id!=1:
        raise RuntimeError('DAO monitoring requires Ethereum mainnet')
    root=Path(__file__).resolve().parents[1]/'abis'
    voter_abi=json.loads((root/'resupply_voter.json').read_text())
    registry=w3.eth.contract(address=REGISTRY,abi=json.loads((root/'resupply_registry.json').read_text()))
    registered=registry.functions.getAddress('VOTER').call(block_identifier='finalized')
    addresses=set(VOTER_ADDRESSES)
    if int(registered,16):
        addresses.add(registered)
    return w3,{address:w3.eth.contract(address=address,abi=voter_abi) for address in addresses}


def run(store,w3,contracts,*,once=False):
    latest=w3.eth.get_block('latest')
    generation=notifications.start_session(store,STREAM,latest['number'],hex_value(latest['hash']))
    baseline_needed=True
    while True:
        progressed=scan_once(store,w3,contracts,generation,notifications.send_telegram)
        # Poll only when all event types and contracts have reached the same finalized head.
        if not progressed:
            if poll_statuses(store,w3,generation,notifications.send_telegram,baseline=baseline_needed):
                baseline_needed=False
        if once:
            return
        if not progressed:
            time.sleep(POLL_INTERVAL)


def main():
    from dotenv import load_dotenv
    load_dotenv()
    store=Store.from_env()
    w3,contracts=runtime()
    run(store,w3,contracts)


def cli():
    parser=argparse.ArgumentParser(description=__doc__)
    group=parser.add_mutually_exclusive_group()
    group.add_argument('--prepare-import',action='store_true')
    group.add_argument('--adopt-boundary',action='store_true')
    group.add_argument('--enable-alerts',action='store_true')
    group.add_argument('--mute-alerts',action='store_true')
    parser.add_argument('--once',action='store_true')
    args=parser.parse_args()
    from dotenv import load_dotenv
    load_dotenv()
    store=Store(os.environ['YEARN_DB_PATH'],os.environ['YEARN_IMPORT_SHA256'],rehearsal=args.prepare_import or args.adopt_boundary)
    if args.prepare_import:
        prepare(store)
        return
    if args.mute_alerts:
        store.write(lambda c:notifications.mute(c,STREAM))
        return
    w3,contracts=runtime()
    if args.adopt_boundary:
        print(json.dumps(adopt_boundary(store,w3,contracts)))
    elif args.enable_alerts:
        notifications.require_permission(STREAM)
        generation=store.read(lambda c:notifications.state(c,STREAM)['generation'])
        poll_statuses(store,w3,generation,notifications.send_telegram,baseline=True,activate=True)
    else:
        run(store,w3,contracts,once=args.once)


if __name__=='__main__':
    cli()
