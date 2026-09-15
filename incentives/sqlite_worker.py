"""Completed incentive periods, exact imported observations, and silent recovery."""

import argparse
from datetime import datetime, timezone
import json
import math
import os
import time

import notifications
import recovery
from sqlite_store import Store
from incentives.incentives_shared import WEEK

CHUNK_SIZE = 5000
COLUMNS = ('protocol','epoch','total_incentives','votium_amount','votemarket_amount',
    'votium_votes_per_usd','votemarket_votes_per_usd','votium_votes','votemarket_votes',
    'gauge_data','transaction_hash','block_number','timestamp','date_str','period_start','log_index')
FLOAT_COLUMNS = COLUMNS[2:9]


def hex_value(value):
    return (value if isinstance(value,str) else value.hex()).lower().removeprefix('0x')


def prepare(store):
    if not store.rehearsal:
        raise RuntimeError('Prepare incentive state in an inactive import')
    def create(c):
        notifications.prepare(c)
        c.execute('''CREATE TABLE incentive_checkpoints (
            protocol TEXT PRIMARY KEY,chain_id INTEGER NOT NULL,initial_period INTEGER NOT NULL,
            next_period INTEGER NOT NULL,previous_block INTEGER NOT NULL,previous_hash TEXT NOT NULL,
            CHECK(next_period>=initial_period))''')
        c.execute('''CREATE TABLE incentive_events (
            protocol TEXT NOT NULL,event_key TEXT NOT NULL,period_start INTEGER NOT NULL,block INTEGER NOT NULL,
            PRIMARY KEY(protocol,event_key))''')
        c.execute('CREATE TABLE incentive_calculations (protocol TEXT PRIMARY KEY,block INTEGER NOT NULL,block_hash TEXT NOT NULL)')
        c.execute("INSERT INTO _migration_meta VALUES ('incentive_schema_version','1')")
    store.write(create)


def checkpoint(c, protocol):
    version=c.execute("SELECT value FROM _migration_meta WHERE key='incentive_schema_version'").fetchone()
    if version is None or version[0]!='1':
        raise RuntimeError('Incentive schema has not been explicitly prepared')
    row=c.execute('SELECT * FROM incentive_checkpoints WHERE protocol=?',(protocol,)).fetchone()
    if row is None:
        raise RuntimeError('Incentive checkpoint missing; boundary adoption is required')
    return dict(row)


def first_block(w3, timestamp, height, *, strictly_after=False):
    """Find a timestamp boundary within a caller's fixed chain head."""
    def before(number):
        value=w3.eth.get_block(number)['timestamp']
        return value<=timestamp if strictly_after else value<timestamp
    if before(height):
        raise RuntimeError('Incentive period boundary is not on chain yet')
    low,high=0,height
    while low<high:
        middle=(low+high)//2
        if before(middle):
            low=middle+1
        else:
            high=middle
    return low


def window(w3, period, head):
    if head['timestamp']<=period+WEEK:
        raise RuntimeError('Incentive period boundary is not on chain yet')
    start=first_block(w3,period,head['number'])
    stop=first_block(w3,period+WEEK,head['number'])
    effective=first_block(w3,period+WEEK,head['number'],strictly_after=True)
    return start,stop-1,effective


def collect(adapter,w3,period,head):
    start,end,effective=window(w3,period,head)
    end_hash=hex_value(w3.eth.get_block(end)['hash'])
    effective_hash=hex_value(w3.eth.get_block(effective)['hash'])
    logs=[]
    for cursor in range(start,end+1,CHUNK_SIZE):
        logs.extend(adapter.transfer_logs(cursor,min(cursor+CHUNK_SIZE-1,end)))
    blocks,receipts,seen,items={},{},set(),[]
    for log in sorted(logs,key=lambda value:(value['blockNumber'],value['logIndex'])):
        number=log['blockNumber']
        if (not start<=number<=end or log.get('removed',False)
                or hex_value(log['address'])!=hex_value(adapter.TOKEN)):
            raise RuntimeError('RPC returned an invalid incentive event')
        if number not in blocks:
            blocks[number]=w3.eth.get_block(number)
        block=blocks[number]
        if hex_value(block['hash'])!=hex_value(log['blockHash']) or not period<=block['timestamp']<period+WEEK:
            raise recovery.ChainChanged('Incentive event changed or falls outside its period')
        tx=hex_value(log['transactionHash'])
        if tx not in receipts:
            receipts[tx]=w3.eth.get_transaction_receipt(log['transactionHash'])
        receipt=receipts[tx]
        if hex_value(receipt['blockHash'])!=hex_value(log['blockHash']) or hex_value(receipt['transactionHash'])!=tx:
            raise recovery.ChainChanged('Incentive receipt changed during collection')
        positions=[i for i,item in enumerate(receipt['logs']) if item['logIndex']==log['logIndex']
                   and hex_value(item['address'])==hex_value(adapter.TOKEN)]
        if len(positions)!=1:
            raise RuntimeError('Incentive event is missing or ambiguous in its receipt')
        # YB's existing report covers the whole transaction, using its first matching transfer.
        identity=f'1:{adapter.TOKEN.lower()}:{tx}:' + ('transaction' if adapter.TRANSACTION_GROUPED else str(positions[0]))
        if identity in seen:
            continue
        seen.add(identity)
        items.append(dict(key=identity,event=log,block=block,receipt=receipt,effective_block=effective))
    if (hex_value(w3.eth.get_block(end)['hash'])!=end_hash
            or hex_value(w3.eth.get_block(effective)['hash'])!=effective_hash):
        raise recovery.ChainChanged('Incentive range changed during collection')
    return items,end,end_hash,effective,effective_hash


def matches_import(row,item,adapter,period):
    event,block=item['event'],item['block']
    return (row['protocol']==adapter.PROTOCOL and row['period_start']==period
            and hex_value(row['transaction_hash'])==hex_value(event['transactionHash'])
            and row['block_number']==event['blockNumber'] and row['timestamp']==block['timestamp']
            and row['date_str']==datetime.fromtimestamp(period,timezone.utc).strftime('%Y-%m-%d %H:%M UTC')
            and (adapter.TRANSACTION_GROUPED or row['log_index'] is None or row['log_index']==event['logIndex']))


def build_new(adapter,items,imported,period):
    """Existing observations are identified by chain metadata, never repriced or recalculated."""
    matched=set()
    for row in imported:
        candidates=[i for i,item in enumerate(items) if matches_import(row,item,adapter,period)]
        if not candidates:
            raise RuntimeError('Imported incentive boundary does not match chain data; reconciliation required')
        matched.add(candidates[0])
    for index,item in enumerate(items):
        item['row']=None if index in matched else adapter.build_record(item['event'],item['block'],item['receipt'],item['effective_block'])
    return len(matched)


def insert_row(c,row):
    for key in FLOAT_COLUMNS:
        if row[key] is not None and not math.isfinite(row[key]):
            raise ValueError('Non-finite incentive observation')
    encoded=dict(row,gauge_data=json.dumps(row['gauge_data'],allow_nan=False))
    # Unexpected conflicts are reconciliation errors, not permission to advance silently.
    c.execute('INSERT INTO incentives ('+','.join(COLUMNS)+') VALUES ('+','.join('?' for _ in COLUMNS)+')',
              tuple(encoded[key] for key in COLUMNS))


def adopt_boundary(store,w3,adapter):
    if not store.rehearsal or w3.eth.chain_id!=1:
        raise RuntimeError('Adopt incentives in an inactive mainnet import')
    rows=store.read(lambda c:[dict(row) for row in c.execute('''SELECT * FROM incentives WHERE protocol=?
        AND period_start=(SELECT max(period_start) FROM incentives WHERE protocol=?) ORDER BY id''',
        (adapter.PROTOCOL,adapter.PROTOCOL))])
    finalized=w3.eth.get_block('finalized')
    if rows:
        period=rows[0]['period_start']
        items,end,end_hash,effective,effective_hash=collect(adapter,w3,period,finalized)
        matched=build_new(adapter,items,rows,period)
        next_period=period+WEEK
        if (hex_value(w3.eth.get_block(end)['hash'])!=end_hash
                or hex_value(w3.eth.get_block(effective)['hash'])!=effective_hash):
            raise recovery.ChainChanged('Incentive boundary changed while calculating missing observations')
    else:
        from incentives.config import INCENTIVE_START_TIMESTAMPS
        period=INCENTIVE_START_TIMESTAMPS[adapter.PROTOCOL]//WEEK*WEEK
        next_period=period
        end=first_block(w3,period,finalized['number'])-1
        end_hash=hex_value(w3.eth.get_block(end)['hash'])
        items,matched=[],0
    def adopt(c):
        c.execute('INSERT INTO incentive_checkpoints VALUES (?,?,?,?,?,?)',
            (adapter.PROTOCOL,1,period,next_period,end,end_hash))
        notifications.add_stream(c,adapter.STREAM,end,end_hash)
        save_calculation(c, adapter.PROTOCOL, effective if rows else end, effective_hash if rows else end_hash)
        for item in items:
            c.execute('INSERT INTO incentive_events VALUES (?,?,?,?)',(adapter.PROTOCOL,item['key'],period,item['event']['blockNumber']))
            if item['row'] is not None:
                insert_row(c,item['row'])
        return dict(protocol=adapter.PROTOCOL,next_period=next_period,imported_boundary_rows=len(rows),
                    matched_events=matched,missing_boundary_events=len(items)-matched)
    return store.write(adopt)


def save_calculation(c, protocol, block, block_hash):
    c.execute('''INSERT INTO incentive_calculations VALUES (?,?,?) ON CONFLICT(protocol)
        DO UPDATE SET block=excluded.block,block_hash=excluded.block_hash''', (protocol,block,block_hash))


def reconcile(store, w3, adapter, previous, generation):
    saved = store.read(lambda c: recovery.point(c, adapter.STREAM))
    if saved is not None:
        recovery.verify(w3, saved)
    observed = store.read(lambda c: c.execute('SELECT * FROM incentive_calculations WHERE protocol=?',
                                             (adapter.PROTOCOL,)).fetchone())
    calculation = dict(observed) if observed is not None else dict(
        block=previous['previous_block'], block_hash=previous['previous_hash'])
    valid = (hex_value(w3.eth.get_block(previous['previous_block'])['hash']) == previous['previous_hash']
             and hex_value(w3.eth.get_block(calculation['block'])['hash']) == calculation['block_hash'])
    if not valid:
        recovery.verify(w3, saved)
        if not previous['initial_period'] <= saved['position'] <= previous['next_period']:
            raise recovery.FatalError('Incentive recovery point is outside processed periods')
        def rewind(c):
            if (checkpoint(c, adapter.PROTOCOL) != previous
                    or notifications.state(c, adapter.STREAM)['generation'] != generation):
                raise recovery.ChainChanged('Incentive checkpoint or session advanced during recovery')
            c.execute('DELETE FROM incentives WHERE protocol=? AND period_start>=?',
                      (adapter.PROTOCOL, saved['position']))
            c.execute('DELETE FROM incentive_events WHERE protocol=? AND period_start>=?',
                      (adapter.PROTOCOL, saved['position']))
            c.execute('UPDATE incentive_checkpoints SET next_period=?,previous_block=?,previous_hash=? WHERE protocol=?',
                      (saved['position'], saved['block'], saved['block_hash'], adapter.PROTOCOL))
            save_calculation(c, adapter.PROTOCOL, saved['block'], saved['block_hash'])
        store.write(rewind)
        return True
    finalized = w3.eth.get_block('finalized')['number']
    if observed is None and calculation['block'] > finalized:
        raise recovery.FatalError('Existing incentive baseline must be finalized before migration')
    if calculation['block'] <= finalized and (saved is None or previous['next_period'] > saved['position']):
        if (hex_value(w3.eth.get_block(calculation['block'])['hash']) != calculation['block_hash']
                or hex_value(w3.eth.get_block(previous['previous_block'])['hash']) != previous['previous_hash']):
            raise recovery.ChainChanged('Incentive chain changed while advancing recovery point')
        def promote(c):
            if checkpoint(c, adapter.PROTOCOL) != previous or recovery.point(c, adapter.STREAM) != saved:
                raise recovery.ChainChanged('Incentive checkpoint advanced concurrently')
            recovery.save(c, adapter.STREAM, calculation['block'], calculation['block_hash'], previous['next_period'])
        store.write(promote)
    elif saved is None:
        raise recovery.FatalError('Finalized incentive recovery point missing')
    return False


def scan_once(store,w3,adapter,generation,send):
    previous=store.read(lambda c:checkpoint(c,adapter.PROTOCOL))
    if w3.eth.chain_id!=1 or previous['chain_id']!=1:
        raise RuntimeError('Incentive checkpoint chain does not match RPC')
    if reconcile(store, w3, adapter, previous, generation):
        return True
    head=w3.eth.get_block('latest')
    period=previous['next_period']
    if head['timestamp']<=period+WEEK:
        return False
    items,end,end_hash,effective,effective_hash=collect(adapter,w3,period,head)
    imported=store.read(lambda c:[dict(row) for row in c.execute('SELECT * FROM incentives WHERE protocol=? AND period_start=?',
                                                               (adapter.PROTOCOL,period))])
    build_new(adapter,items,imported,period)
    if (hex_value(w3.eth.get_block(previous['previous_block'])['hash'])!=previous['previous_hash']
            or hex_value(w3.eth.get_block(end)['hash'])!=end_hash
            or hex_value(w3.eth.get_block(effective)['hash'])!=effective_hash):
        raise recovery.ChainChanged('Incentive range changed while calculating observations')
    from incentives.config import resolve_chat_id
    chat_id,destination=resolve_chat_id(adapter.PROTOCOL)
    if not chat_id:
        raise RuntimeError('Incentive notification destination is not configured')
    def commit(c):
        if (checkpoint(c,adapter.PROTOCOL)!=previous
                or notifications.state(c,adapter.STREAM)['generation']!=generation):
            raise RuntimeError('Incentive checkpoint or session advanced concurrently')
        claims=[]
        for item in items:
            inserted=c.execute('INSERT INTO incentive_events VALUES (?,?,?,?) ON CONFLICT(protocol,event_key) DO NOTHING',
                (adapter.PROTOCOL,item['key'],period,item['event']['blockNumber'])).rowcount
            if not inserted or item['row'] is None:
                continue
            insert_row(c,item['row'])
            message=adapter.render_message(item['row'])
            claim=notifications.pending(c,adapter.STREAM,generation,item['event']['blockNumber'],destination)
            if claim:
                claims.append((claim,message))
        c.execute('UPDATE incentive_checkpoints SET next_period=?,previous_block=?,previous_hash=? WHERE protocol=?',
                  (period+WEEK,end,end_hash,adapter.PROTOCOL))
        save_calculation(c, adapter.PROTOCOL, effective, effective_hash)
        return claims
    for claim,message in store.write(commit):
        notifications.dispatch(store,claim,message,send)
    return True


def enable_future_alerts(store,w3,adapter):
    previous=store.read(lambda c:checkpoint(c,adapter.PROTOCOL))
    latest=w3.eth.get_block('latest')
    if (w3.eth.chain_id!=1 or previous['chain_id']!=1 or latest['timestamp']>previous['next_period']+WEEK
            or hex_value(w3.eth.get_block(previous['previous_block'])['hash'])!=previous['previous_hash']):
        raise RuntimeError('Incentives must finish validated silent catch-up before enabling alerts')
    def enable(c):
        if checkpoint(c,adapter.PROTOCOL)!=previous:
            raise RuntimeError('Incentive catch-up changed before activation')
        notifications.enable(c,adapter.STREAM,latest['number'],hex_value(latest['hash']))
    store.write(enable)


def runtime(adapter):
    from web3 import Web3
    w3=Web3(Web3.HTTPProvider(os.environ['WEB3_PROVIDER_URI'],request_kwargs={'timeout':60}))
    if w3.eth.chain_id!=1:
        raise RuntimeError('Incentive reporting requires Ethereum mainnet')
    adapter.configure(w3)
    return w3


def poll_delay(next_period,now,max_interval):
    """Wake at week close, then poll promptly until its calculation block exists."""
    return min(max_interval,max(2,next_period+WEEK-now))


def run(store,w3,adapter,*,once=False):
    latest=w3.eth.get_block('latest')
    generation=notifications.start_session(store,adapter.STREAM,latest['number'],hex_value(latest['hash']))
    while True:
        progressed=recovery.attempt(lambda: scan_once(store,w3,adapter,generation,notifications.send_telegram))
        if once:
            return
        if not progressed:
            next_period=store.read(lambda c:checkpoint(c,adapter.PROTOCOL))['next_period']
            saved=store.read(lambda c: recovery.point(c,adapter.STREAM))
            pending=saved is None or saved['position']<next_period
            time.sleep(2 if pending else poll_delay(next_period,time.time(),adapter.POLL_INTERVAL))


def main(adapter):
    from dotenv import load_dotenv
    load_dotenv()
    store=Store.from_env()
    run(store,runtime(adapter),adapter)


def cli(adapter):
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
        store.write(lambda c:notifications.mute(c,adapter.STREAM))
        return
    w3=runtime(adapter)
    if args.adopt_boundary:
        print(json.dumps(adopt_boundary(store,w3,adapter)))
    elif args.enable_alerts:
        notifications.require_permission(adapter.STREAM)
        enable_future_alerts(store,w3,adapter)
    else:
        run(store,w3,adapter,once=args.once)
