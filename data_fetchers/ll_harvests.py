"""Harvest indexing with atomic records and scan progress; no notification path."""

import argparse
from datetime import datetime, timezone
from decimal import Decimal, localcontext
import json
import os
from pathlib import Path
import sys
import time

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from constants import CURVE_LIQUID_LOCKER_COMPOUNDERS
from sqlite_store import Store

POLL_INTERVAL = 120
CHUNK_SIZE = 5000
PROFIT_FIELDS = {
    '0x43e54c2e7b3e294de3a155785f52ab49d87b9922': 'assets',
    '0xde2bef0a01845257b4aef2a2eaa48f6eaeafa8b7': '_value',
    '0x27b5739e22ad9033bcbf192059122d163b60349d': 'gain',
}
COLUMNS = ('profit', 'timestamp', 'name', 'underlying', 'compounder', 'block', 'txn_hash', 'date_str')


def hex_value(value):
    result = value if isinstance(value, str) else value.hex()
    return result.lower().removeprefix('0x')


def decimal_amount(raw):
    with localcontext() as context:
        context.prec = 100
        return format(Decimal(raw).scaleb(-18), 'f')


def checkpoint(connection, address):
    row = connection.execute('SELECT * FROM ll_checkpoints WHERE compounder=?',
                             (address.lower(),)).fetchone()
    if row is None:
        raise RuntimeError('Harvest checkpoint missing; explicit boundary adoption is required')
    return dict(row)


def prepare(store):
    """Explicit offline migration step, never called during normal startup."""
    if not store.rehearsal:
        raise RuntimeError('Prepare harvest state before activating the shared import')

    def create(connection):
        connection.execute('''CREATE TABLE ll_checkpoints (
            compounder TEXT PRIMARY KEY, chain_id INTEGER NOT NULL,
            initial_block INTEGER NOT NULL, next_block INTEGER NOT NULL,
            previous_hash TEXT NOT NULL, CHECK(next_block >= initial_block))''')
        connection.execute('''CREATE TABLE ll_events (
            event_key TEXT PRIMARY KEY, compounder TEXT NOT NULL,
            block INTEGER NOT NULL, FOREIGN KEY(compounder) REFERENCES ll_checkpoints(compounder))''')
        connection.execute("INSERT INTO _migration_meta VALUES ('ll_schema_version','1')")
    store.write(create)


def contract_event(w3, address, info):
    symbol = info['symbol']
    filename = {'asdCRV': 'asdcrv', 'yvyCRV': 'yvycrv'}.get(symbol, 'ucvxcrv')
    abi = json.loads((Path(__file__).resolve().parents[1] / 'abis' / (filename + '.json')).read_text())
    contract = w3.eth.contract(address=address, abi=abi)
    return getattr(contract.events, 'StrategyReported' if symbol == 'yvyCRV' else 'Harvest')


def collect(w3, address, info, start, end, chain_id):
    """Complete all RPC work and establish the end-block hash before any DB write."""
    before = hex_value(w3.eth.get_block(end)['hash'])
    logs = contract_event(w3, address, info).get_logs(fromBlock=start, toBlock=end)
    blocks, receipts, result = {}, {}, []
    for log in sorted(logs, key=lambda value: (value['blockNumber'], value['logIndex'])):
        block = log['blockNumber']
        if not start <= block <= end or log.get('removed', False):
            raise RuntimeError('RPC returned an invalid harvest range')
        if hex_value(log['address']) != hex_value(address):
            raise RuntimeError('RPC returned a harvest from a different contract')
        if block not in blocks:
            blocks[block] = w3.eth.get_block(block)
        if hex_value(blocks[block]['hash']) != hex_value(log['blockHash']):
            raise RuntimeError('Harvest block changed during collection')
        txn_hash = hex_value(log['transactionHash'])
        if txn_hash not in receipts:
            receipts[txn_hash] = w3.eth.get_transaction_receipt(log['transactionHash'])
        receipt = receipts[txn_hash]
        if (hex_value(receipt['blockHash']) != hex_value(log['blockHash'])
                or hex_value(receipt['transactionHash']) != txn_hash):
            raise RuntimeError('Harvest receipt changed during collection')
        # Receipt-local position survives unrelated transactions moving in a reorg.
        positions = [i for i, item in enumerate(receipt['logs'])
                     if item['logIndex'] == log['logIndex']
                     and hex_value(item['address']) == hex_value(address)]
        if len(positions) != 1:
            raise RuntimeError('Harvest log is missing or ambiguous in its receipt')
        raw = log['args'][PROFIT_FIELDS[address.lower()]]
        timestamp = blocks[block]['timestamp']
        result.append({
            'key': f'{chain_id}:{address.lower()}:{txn_hash}:{positions[0]}',
            'raw_profit': raw,
            'row': dict(zip(COLUMNS, (decimal_amount(raw), timestamp, info['symbol'],
                info['underlying'], address, block, log['transactionHash'].hex(),
                datetime.fromtimestamp(timestamp, timezone.utc).strftime('%Y-%m-%d %H:%M:%S')))),
        })
    if hex_value(w3.eth.get_block(end)['hash']) != before:
        raise RuntimeError('Harvest range changed during collection')
    return result, before


def insert_event(connection, item, address):
    inserted = connection.execute('INSERT INTO ll_events VALUES (?,?,?) ON CONFLICT(event_key) DO NOTHING',
                                 (item['key'], address.lower(), item['row']['block'])).rowcount
    if inserted:
        # Preserve the imported application's reviewed uniqueness rule.
        connection.execute('''INSERT INTO crv_ll_harvests
            (profit,timestamp,name,underlying,compounder,block,txn_hash,date_str)
            VALUES (?,?,?,?,?,?,?,?) ON CONFLICT(txn_hash,profit,compounder) DO NOTHING''',
            tuple(item['row'][key] for key in COLUMNS))
    return inserted


def adopt_boundary(store, w3, address, info):
    """Check every imported row in its last block; fill only missing boundary events."""
    if not store.rehearsal:
        raise RuntimeError('Adopt boundaries before activating the shared import')
    chain_id = w3.eth.chain_id
    if chain_id != 1:
        raise RuntimeError('Harvest imports require Ethereum mainnet')
    rows = store.read(lambda connection: [dict(row) for row in connection.execute(
        '''SELECT * FROM crv_ll_harvests WHERE lower(compounder)=lower(?) AND block=(
           SELECT max(block) FROM crv_ll_harvests WHERE lower(compounder)=lower(?))''', (address, address))])
    boundary = rows[0]['block'] if rows else max(20_000_000, info['deploy_block']) - 1
    if boundary > w3.eth.get_block('finalized')['number']:
        raise RuntimeError('The imported harvest boundary is not finalized yet')
    items, block_hash = collect(w3, address, info, boundary, boundary, chain_id)
    matched = set()
    for row in rows:
        candidates = []
        for index, item in enumerate(items):
            candidate = item['row']
            # Validate the old float-derived PostgreSQL value without replacing it.
            legacy = Decimal(str(item['raw_profit'] / 1e18))
            if (hex_value(row['txn_hash']) == hex_value(candidate['txn_hash'])
                    and Decimal(row['profit']) in (legacy, Decimal(candidate['profit']))
                    and all(row[key] == candidate[key] for key in ('timestamp', 'name', 'underlying', 'date_str'))):
                candidates.append(index)
        if not candidates:
            raise RuntimeError('Imported boundary row does not match chain data; reconciliation required')
        matched.update(candidates)

    def adopt(connection):
        connection.execute('INSERT INTO ll_checkpoints VALUES (?,?,?,?,?)',
                           (address.lower(), chain_id, boundary + 1, boundary + 1, block_hash))
        for index, item in enumerate(items):
            if index in matched:
                connection.execute('INSERT INTO ll_events VALUES (?,?,?)',
                                   (item['key'], address.lower(), boundary))
            else:
                insert_event(connection, item, address)
        return {'compounder': address, 'next_block': boundary + 1,
                'imported_boundary_rows': len(rows), 'missing_boundary_events': len(items) - len(matched)}
    return store.write(adopt)


def scan_once(store, w3, compounders=CURVE_LIQUID_LOCKER_COMPOUNDERS, *, chunk_size=CHUNK_SIZE):
    if chunk_size < 1:
        raise ValueError('Harvest chunk size must be positive')
    version = store.read(lambda connection: connection.execute(
        "SELECT value FROM _migration_meta WHERE key='ll_schema_version'").fetchone())
    if version is None or version[0] != '1':
        raise RuntimeError('Harvest schema has not been prepared')
    height = w3.eth.get_block('finalized')['number']
    chain_id = w3.eth.chain_id
    for address, info in compounders.items():
        state = store.read(lambda connection: checkpoint(connection, address))
        if chain_id != 1 or state['chain_id'] != chain_id:
            raise RuntimeError('Harvest checkpoint chain does not match RPC')
        if hex_value(w3.eth.get_block(state['next_block'] - 1)['hash']) != state['previous_hash']:
            raise RuntimeError('Harvest checkpoint block changed; reconciliation required')
        start = state['next_block']
        if start > height:
            continue
        end = min(height, start + chunk_size - 1)
        items, block_hash = collect(w3, address, info, start, end, chain_id)
        if hex_value(w3.eth.get_block(start - 1)['hash']) != state['previous_hash']:
            raise RuntimeError('Harvest checkpoint block changed during collection')

        def commit(connection):
            if checkpoint(connection, address) != state:
                raise RuntimeError('Harvest checkpoint advanced concurrently; retry the scan')
            for item in items:
                insert_event(connection, item, address)
            connection.execute('UPDATE ll_checkpoints SET next_block=?,previous_hash=? WHERE compounder=?',
                               (end + 1, block_hash, address.lower()))
        store.write(commit)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--prepare-import', action='store_true', help='Explicitly prepare an inactive imported copy')
    parser.add_argument('--adopt-boundaries', action='store_true', help='Validate and adopt inactive import boundaries')
    parser.add_argument('--once', action='store_true')
    args = parser.parse_args()
    from dotenv import load_dotenv
    load_dotenv()
    if args.prepare_import or args.adopt_boundaries:
        store = Store(os.environ['YEARN_DB_PATH'], os.environ['YEARN_IMPORT_SHA256'], rehearsal=True)
        if args.prepare_import:
            prepare(store)
        if not args.adopt_boundaries:
            return
    else:
        store = Store.from_env()
    from web3 import Web3
    w3 = Web3(Web3.HTTPProvider(os.environ['WEB3_PROVIDER_URI'], request_kwargs={'timeout': 60}))
    if args.adopt_boundaries:
        for address, info in CURVE_LIQUID_LOCKER_COMPOUNDERS.items():
            print(json.dumps(adopt_boundary(store, w3, address, info)))
        return
    while True:
        scan_once(store, w3)
        if args.once:
            return
        time.sleep(POLL_INTERVAL)


if __name__ == '__main__':
    main()
