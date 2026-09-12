"""Exact retention weights with atomic SQLite progress and controlled notifications."""

import argparse
from datetime import datetime, timezone
from decimal import Decimal, localcontext
import json
import logging
import os
from pathlib import Path
import sys
import time

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from sqlite_store import Store
import notifications

logger = logging.getLogger(__name__)
CONTRACT_ADDRESS = '0xB9415639618e70aBb71A0F4F8bbB2643Bf337892'
DEPLOYMENT_BLOCK = 22870945
POLL_INTERVAL = 2
CHUNK_SIZE = 5000
STREAM = 'retention'
COLUMNS = ('user_address','old_weight','new_weight','weight_diff','block','txn_hash','timestamp','date_str','log_index')


def hex_value(value):
    return (value if isinstance(value, str) else value.hex()).lower().removeprefix('0x')


def prepare(store):
    if not store.rehearsal:
        raise RuntimeError('Prepare retention state before activating the import')
    def create(connection):
        notifications.prepare(connection)
        connection.execute('''CREATE TABLE retention_checkpoint (
            stream TEXT PRIMARY KEY,chain_id INTEGER NOT NULL,initial_block INTEGER NOT NULL,
            next_block INTEGER NOT NULL,previous_hash TEXT NOT NULL,CHECK(next_block>=initial_block))''')
        connection.execute('CREATE TABLE retention_events (event_key TEXT PRIMARY KEY,block INTEGER NOT NULL)')
        connection.execute("INSERT INTO _migration_meta VALUES ('retention_schema_version','1')")
    store.write(create)


def checkpoint(connection):
    version = connection.execute("SELECT value FROM _migration_meta WHERE key='retention_schema_version'").fetchone()
    if version is None or version[0] != '1':
        raise RuntimeError('Retention schema has not been explicitly prepared')
    row = connection.execute('SELECT * FROM retention_checkpoint WHERE stream=?', (STREAM,)).fetchone()
    if row is None:
        raise RuntimeError('Retention checkpoint missing; boundary adoption is required')
    return dict(row)


def format_address(address):
    return f'[0x{address[2:5]}...{address[-4:]}](https://etherscan.io/address/{address})'


def message_for(row, original_supply, current_supply):
    with localcontext() as context:
        context.prec = 100
        difference = Decimal(row['weight_diff']).scaleb(-18)
        remaining = Decimal(row['new_weight']).scaleb(-18)
        message = '🔁 *Retention Shares Checkpointed*\n\n'
        message += f"User: {format_address(row['user_address'])}\nBurned: {abs(difference):,.0f}\nRemaining: {remaining:,.0f}\n"
        if current_supply is None:
            message += 'Total Remaining: Unable to fetch\n'
        else:
            current = Decimal(current_supply).scaleb(-18)
            message += f'\nTotal Remaining: {current:,.0f}'
            if original_supply and current_supply:
                original = Decimal(original_supply).scaleb(-18)
                remaining_pct = current / original * 100
                withdrawn_pct = (original - current) / original * 100
                message += f' ({remaining_pct:.1f}%)\nTotal Withdrawn: {original-current:,.0f} ({withdrawn_pct:.1f}%)\n'
            else:
                message += '\n'
        return message + f"\n🔗 [View on Etherscan](https://etherscan.io/tx/{row['txn_hash']})"


def optional_supply(contract, block):
    try:
        return contract.functions.totalSupply().call(block_identifier=block)
    except Exception:
        # Supply is an ancillary display field; the original listener also allowed it to be unavailable.
        logger.warning('Retention total supply is unavailable for block %s', block)
        return None


def collect(w3, contract, start, end, *, original_supply=None, include_messages=True):
    end_hash = hex_value(w3.eth.get_block(end)['hash'])
    logs = contract.events.WeightSet.get_logs(fromBlock=start, toBlock=end)
    blocks, receipts, supplies, items = {}, {}, {}, []
    for log in sorted(logs, key=lambda item: (item['blockNumber'], item['logIndex'])):
        number = log['blockNumber']
        if (not start <= number <= end or log.get('removed', False)
                or hex_value(log['address']) != hex_value(CONTRACT_ADDRESS)):
            raise RuntimeError('RPC returned an invalid retention event')
        if number not in blocks:
            blocks[number] = w3.eth.get_block(number)
        block = blocks[number]
        if hex_value(block['hash']) != hex_value(log['blockHash']):
            raise RuntimeError('Retention event block changed during collection')
        tx = hex_value(log['transactionHash'])
        if tx not in receipts:
            receipts[tx] = w3.eth.get_transaction_receipt(log['transactionHash'])
        receipt = receipts[tx]
        if hex_value(receipt['blockHash']) != hex_value(log['blockHash']) or hex_value(receipt['transactionHash']) != tx:
            raise RuntimeError('Retention receipt changed during collection')
        positions = [i for i, item in enumerate(receipt['logs']) if item['logIndex'] == log['logIndex']
                     and hex_value(item['address']) == hex_value(CONTRACT_ADDRESS)]
        if len(positions) != 1:
            raise RuntimeError('Retention event is missing or ambiguous in its receipt')
        old, new = int(log['args']['oldWeight']), int(log['args']['newWeight'])
        row = dict(zip(COLUMNS, (log['args']['user'],str(old),str(new),str(new-old),number,
            log['transactionHash'].hex(),block['timestamp'],
            datetime.fromtimestamp(block['timestamp'],timezone.utc).strftime('%Y-%m-%d %H:%M UTC'),log['logIndex'])))
        message = None
        if include_messages and number != DEPLOYMENT_BLOCK:
            if number not in supplies:
                supplies[number] = optional_supply(contract, number)
            message = message_for(row, original_supply, supplies[number])
        items.append(dict(key=f'1:{CONTRACT_ADDRESS.lower()}:{tx}:{positions[0]}',row=row,message=message))
    if hex_value(w3.eth.get_block(end)['hash']) != end_hash:
        raise RuntimeError('Retention range changed during collection')
    return items, end_hash


def insert_row(connection, row):
    return bool(connection.execute('INSERT INTO weight_changes (' + ','.join(COLUMNS) + ') VALUES (' +
        ','.join('?' for _ in COLUMNS) + ') ON CONFLICT(txn_hash,log_index) DO NOTHING',
        tuple(row[key] for key in COLUMNS)).rowcount)


def matches_import(row, item):
    candidate = item['row']
    return (all(hex_value(row[key]) == hex_value(candidate[key]) for key in ('user_address','txn_hash'))
            and all(int(row[key]) == int(candidate[key]) for key in ('old_weight','new_weight','weight_diff'))
            and all(row[key] == candidate[key] for key in ('block','timestamp','date_str'))
            and (row['log_index'] is None or row['log_index'] == candidate['log_index']))


def adopt_boundary(store, w3, contract):
    if not store.rehearsal or w3.eth.chain_id != 1:
        raise RuntimeError('Adopt retention state in an inactive mainnet import')
    rows = store.read(lambda connection: [dict(row) for row in connection.execute(
        'SELECT * FROM weight_changes WHERE block=(SELECT max(block) FROM weight_changes) ORDER BY id')])
    boundary = rows[0]['block'] if rows else DEPLOYMENT_BLOCK - 1
    if boundary > w3.eth.get_block('finalized')['number']:
        raise RuntimeError('Wait for the retention boundary to become finalized')
    items, block_hash = collect(w3, contract, boundary, boundary, include_messages=False)
    unmatched = set(range(len(items)))
    duplicates = 0
    for row in rows:
        candidates = [i for i, item in enumerate(items) if matches_import(row, item)]
        if not candidates:
            raise RuntimeError('Imported retention boundary does not match chain data; reconciliation required')
        available = [i for i in candidates if i in unmatched]
        if available:
            unmatched.remove(available[0])
        else:
            duplicates += 1
    def adopt(connection):
        connection.execute('INSERT INTO retention_checkpoint VALUES (?,?,?,?,?)', (STREAM,1,boundary+1,boundary+1,block_hash))
        notifications.add_stream(connection, STREAM, boundary, block_hash)
        for index, item in enumerate(items):
            connection.execute('INSERT INTO retention_events VALUES (?,?)', (item['key'], boundary))
            if index in unmatched and not insert_row(connection, item['row']):
                raise RuntimeError('A missing retention boundary event conflicts with an imported row')
        return dict(next_block=boundary+1,imported_boundary_rows=len(rows),source_duplicates=duplicates,missing_boundary_events=len(unmatched))
    return store.write(adopt)


def scan_once(store, w3, contract, original_supply, generation, send):
    previous = store.read(checkpoint)
    if previous['chain_id'] != 1 or w3.eth.chain_id != 1:
        raise RuntimeError('Retention checkpoint chain does not match RPC')
    start = previous['next_block']
    if hex_value(w3.eth.get_block(start-1)['hash']) != previous['previous_hash']:
        raise RuntimeError('Retention checkpoint block changed; reconciliation required')
    height = w3.eth.get_block('latest')['number']
    if start > height:
        return False
    end = min(start+CHUNK_SIZE-1,height)
    items, block_hash = collect(w3, contract, start, end, original_supply=original_supply)
    if hex_value(w3.eth.get_block(start-1)['hash']) != previous['previous_hash']:
        raise RuntimeError('Retention checkpoint block changed during collection')
    def commit(connection):
        if checkpoint(connection) != previous or notifications.state(connection, STREAM)['generation'] != generation:
            raise RuntimeError('Retention checkpoint or scan session advanced concurrently')
        claims = []
        for item in items:
            if not connection.execute('INSERT INTO retention_events VALUES (?,?) ON CONFLICT(event_key) DO NOTHING',
                                      (item['key'],item['row']['block'])).rowcount:
                continue
            if not insert_row(connection,item['row']):
                continue  # Preserve the existing return on a duplicate transaction/log.
            if item['message'] is not None:
                claim = notifications.decide(connection,STREAM,generation,item['key'],item['row']['block'],
                                             'weight-change','RESUPPLY_ALERTS',item['message'])
                if claim:
                    claims.append((claim,item['message']))
        connection.execute('UPDATE retention_checkpoint SET next_block=?,previous_hash=? WHERE stream=?', (end+1,block_hash,STREAM))
        return claims
    claims = store.write(commit)
    logger.info('Retention scanned blocks %s-%s: %s events, %s alerts',start,end,len(items),len(claims))
    for claim, message in claims:
        notifications.dispatch(store,claim,message,send)
    return True


def enable_future_alerts(store,w3):
    latest = w3.eth.get_block('latest')
    previous = store.read(checkpoint)
    if (w3.eth.chain_id != 1 or previous['chain_id'] != 1 or previous['next_block'] <= latest['number']
            or hex_value(w3.eth.get_block(previous['next_block']-1)['hash']) != previous['previous_hash']):
        raise RuntimeError('Retention must finish validated silent catch-up before enabling alerts')
    def enable(connection):
        if checkpoint(connection)['next_block'] <= latest['number']:
            raise RuntimeError('Retention catch-up changed before activation')
        notifications.enable(connection,STREAM,latest['number'],hex_value(latest['hash']))
    store.write(enable)


def runtime():
    from web3 import Web3
    w3 = Web3(Web3.HTTPProvider(os.environ['WEB3_PROVIDER_URI'],request_kwargs={'timeout':60}))
    abi = json.loads((Path(__file__).resolve().parents[1]/'abis'/'retention.json').read_text())
    return w3,w3.eth.contract(address=CONTRACT_ADDRESS,abi=abi)


def run(store,w3,contract,*,once=False):
    if w3.eth.chain_id != 1:
        raise RuntimeError('Retention notifications require Ethereum mainnet')
    original = optional_supply(contract,DEPLOYMENT_BLOCK+1)
    latest = w3.eth.get_block('latest')
    generation = notifications.start_session(store,STREAM,latest['number'],hex_value(latest['hash']))
    while True:
        progressed = scan_once(store,w3,contract,original,generation,notifications.send_telegram)
        if once:
            return
        if not progressed:
            time.sleep(POLL_INTERVAL)


def main():
    # The bundle calls this function directly; only standalone CLI use parses argv.
    from dotenv import load_dotenv
    load_dotenv()
    store = Store.from_env()
    w3,contract = runtime()
    run(store,w3,contract)


def cli():
    parser = argparse.ArgumentParser(description=__doc__)
    group = parser.add_mutually_exclusive_group()
    group.add_argument('--prepare-import',action='store_true')
    group.add_argument('--adopt-boundary',action='store_true')
    group.add_argument('--enable-alerts',action='store_true')
    group.add_argument('--mute-alerts',action='store_true')
    parser.add_argument('--once',action='store_true')
    args = parser.parse_args()
    from dotenv import load_dotenv
    load_dotenv()
    store = Store(os.environ['YEARN_DB_PATH'],os.environ['YEARN_IMPORT_SHA256'],rehearsal=args.prepare_import or args.adopt_boundary)
    if args.prepare_import:
        prepare(store)
        return
    if args.mute_alerts:
        store.write(lambda connection:notifications.mute(connection,STREAM))
        return
    w3,contract = runtime()
    if args.adopt_boundary:
        print(json.dumps(adopt_boundary(store,w3,contract)))
    elif args.enable_alerts:
        notifications.require_permission(STREAM)
        enable_future_alerts(store,w3)
    else:
        run(store,w3,contract,once=args.once)


if __name__ == '__main__':
    cli()
