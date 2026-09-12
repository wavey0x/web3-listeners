"""Curve vote indexing with durable progress and suppressed recovery notifications."""

import argparse
from datetime import datetime, timezone
from decimal import Decimal, localcontext, ROUND_HALF_UP
import json
import logging
import os
from pathlib import Path
import re
import sys
import time

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from sqlite_store import Store
import notifications

logger = logging.getLogger(__name__)

GAUGE_CONTROLLER_ADDRESS = '0x2F50D538606Fa9EDD2B11E2446BEb18C9D5846bB'
VE_ADDRESS = '0x5f3b5DfEb7B28CDbD7FAba78963EE202a494e2A2'
DEPLOY_BLOCK = 10647875
CHUNK_SIZE = 5000
POLL_INTERVAL = 2
STREAM = 'curve'
COLUMNS = ('gauge','gauge_name','account','amount','weight','account_alias','txn_hash','timestamp','date_str','block')

GAUGE_NAME_EXCEPTIONS = {
    '0x6C09F6727113543Fd061a721da512B7eFCDD0267': 'xdai x3pool',
    '0xb9C05B8EE41FDCbd9956114B3aF15834FDEDCb54': 'ftm 2pool',
    '0xfE1A3dD8b169fB5BF0D5dbFe813d956F39fF6310': 'ftm g3CRV',
    '0xfDb129ea4b6f557b07BcDCedE54F665b7b6Bc281': 'ftm btcCRV',
    '0x260e4fBb13DD91e187AE992c3435D0cf97172316': 'ftm crv3crypto',
    '0xC48f4653dd6a9509De44c92beb0604BEA3AEe714': 'polygon am3pool',
    '0x060e386eCfBacf42Aa72171Af9EFe17b3993fC4F': 'polygon a3crypto',
    '0x488E6ef919C2bB9de535C634a80afb0114DA8F62': 'polygon btcCRV',
    '0xAF78381216a8eCC7Ad5957f3cD12a431500E0B0D': 'polygon crvEURTUSD',
    '0xFf17560d746F85674FE7629cE986E949602EF948': 'arbi 2pool',
    '0x9F86c5142369B1Ffd4223E5A2F2005FC66807894': 'arbi btcCRV',
    '0x9044E12fB1732f88ed0c93cfa5E9bB9bD2990cE5': 'arbi 3crypto',
    '0x56eda719d82aE45cBB87B7030D3FB485685Bea45': 'arbi crvEURSUSD',
    '0xB504b6EB06760019801a91B451d3f7BD9f027fC9': 'avax av3crv',
    '0x75D05190f35567e79012c2F0a02330D3Ed8a1F74': 'avax btcCRV',
    '0xa05E565cA0a103FcD999c7A7b8de7Bd15D5f6505': 'avax 3crypto',
    '0xf2Cde8c47C20aCbffC598217Ad5FE6DB9E00b163': 'harmony gauge',
    '0x1cEBdB0856dd985fAe9b8fEa2262469360B8a3a6': 'crvCRVETH',
    '0xbAF05d7aa4129CA14eC45cC9d4103a9aB9A9fF60': 'Vyper Fundraising Gauge',
    '0x44e528e6a1aa1A931946fa96753F7dDc8d61B489': 'scrvUSD/USDC',
    '0x12C3F630ec8f8A07C539b5F933e8E62F9b627396': 'insfrxETH/sfrxETH',
    '0xa971354DB30DF69b35cf99B434875A32AEA0718A': 'CrossCurve Stable on Fantom',
    '0x170100AeD2A922a570E5D105C29cc7158f3de359': 'CrossCurve ETH on Fantom',
    '0x8596721b74d92196E19c5Cb57cf7A46ADbf2b32a': 'CrossCurve 2 on Fantom',
    '0xD44AeeCc0928c016C29EA3E0C902bDcD0784C0FA': 'CrossCurve 2 Stable on Fantom',
    '0xe245d3264D9072937ed6cc1E2E34B946DE03cD53': 'Big Fraxtal Savings:  scrvUSD/sFRAX',
    '0x8CBbe2b27c574B2F283853E3eDC20271D100c285': 'CrossCurve CRV',
}

ALIASES = {
    '0x989AEb4d175e16225E39E87d0D97A3360524AD80': 'Convex',
    '0x7a16fF8270133F063aAb6C9977183D9e72835428': 'Mich',
    '0xF147b8125d2ef93FB6965Db97D6746952a133934': 'Yearn',
    '0x52f541764E6e90eeBc5c21Ff570De0e2D63766B6': 'Stakedao',
    '0x490b8C6007fFa5d3728A49c2ee199e51f05D2F7e': 'Prisma',
}


def hex_value(value):
    return (value if isinstance(value, str) else value.hex()).lower().removeprefix('0x')


def contracts(w3):
    directory = Path(__file__).resolve().parents[1] / 'abis'
    return (w3.eth.contract(address=GAUGE_CONTROLLER_ADDRESS, abi=json.loads((directory / 'gauge_controller.json').read_text())),
            w3.eth.contract(address=VE_ADDRESS, abi=json.loads((directory / 've.json').read_text())))


def amount_text(balance, weight, *, legacy=False):
    with localcontext() as context:
        context.prec = 100
        value = Decimal(str(balance / 1e18 * weight / 10_000)) if legacy else Decimal(balance) * Decimal(weight) / Decimal(10**22)
        return format(value.quantize(Decimal('1e-18'), rounding=ROUND_HALF_UP), 'f')


def checkpoint(connection):
    version = connection.execute("SELECT value FROM _migration_meta WHERE key='curve_schema_version'").fetchone()
    if version is None or version[0] != '1':
        raise RuntimeError('Curve schema has not been explicitly prepared')
    row = connection.execute('SELECT * FROM curve_checkpoint WHERE stream=?', (STREAM,)).fetchone()
    if row is None:
        raise RuntimeError('Curve checkpoint missing; boundary adoption is required')
    return dict(row)


def prepare(store):
    if not store.rehearsal:
        raise RuntimeError('Prepare Curve state before activating the shared import')
    def create(connection):
        notifications.prepare(connection)
        connection.execute("CREATE TABLE curve_checkpoint (stream TEXT PRIMARY KEY,chain_id INTEGER NOT NULL,initial_block INTEGER NOT NULL,next_block INTEGER NOT NULL,previous_hash TEXT NOT NULL,CHECK(next_block>=initial_block))")
        connection.execute('CREATE TABLE curve_events (event_key TEXT PRIMARY KEY,block INTEGER NOT NULL)')
        connection.execute("INSERT INTO _migration_meta VALUES ('curve_schema_version','1')")
    store.write(create)


def collect(w3, controller, ve, gauges, start, end):
    end_hash = hex_value(w3.eth.get_block(end)['hash'])
    logs = controller.events.VoteForGauge.get_logs(fromBlock=start, toBlock=end)
    blocks, receipts, balances, items = {}, {}, {}, []
    aliases = {key.lower(): value for key, value in ALIASES.items()}
    names = {key.lower(): value for key, value in {**GAUGE_NAME_EXCEPTIONS, **gauges}.items()}
    for log in sorted(logs, key=lambda item: (item['blockNumber'], item['logIndex'])):
        block = log['blockNumber']
        if (not start <= block <= end or log.get('removed', False)
                or hex_value(log['address']) != hex_value(GAUGE_CONTROLLER_ADDRESS)):
            raise RuntimeError('RPC returned an invalid Curve vote')
        if block not in blocks:
            blocks[block] = w3.eth.get_block(block)
        if hex_value(blocks[block]['hash']) != hex_value(log['blockHash']):
            raise RuntimeError('Curve vote block changed during collection')
        tx = hex_value(log['transactionHash'])
        if tx not in receipts:
            receipts[tx] = w3.eth.get_transaction_receipt(log['transactionHash'])
        receipt = receipts[tx]
        if hex_value(receipt['blockHash']) != hex_value(log['blockHash']) or hex_value(receipt['transactionHash']) != tx:
            raise RuntimeError('Curve vote receipt changed during collection')
        positions = [i for i, item in enumerate(receipt['logs']) if item['logIndex'] == log['logIndex']
                     and hex_value(item['address']) == hex_value(GAUGE_CONTROLLER_ADDRESS)]
        if len(positions) != 1:
            raise RuntimeError('Curve vote is missing or ambiguous in its receipt')
        gauge, user, weight = log['args']['gauge_addr'], log['args']['user'], log['args']['weight']
        if (user, block) not in balances:
            balances[user, block] = ve.functions.balanceOf(user).call(block_identifier=block)
        balance = balances[user, block]
        timestamp = blocks[block]['timestamp']
        row = dict(zip(COLUMNS, (gauge, names.get(gauge.lower(), 'Unknown Gauge Name'), user,
                   amount_text(balance, weight), weight, aliases.get(user.lower(), ''), log['transactionHash'].hex(),
                   timestamp, datetime.fromtimestamp(timestamp, timezone.utc).strftime('%Y-%m-%d %H:%M:%S'), block)))
        items.append(dict(key=f'1:{GAUGE_CONTROLLER_ADDRESS.lower()}:{tx}:{positions[0]}', row=row,
                          legacy_amount=amount_text(balance, weight, legacy=True), unknown=gauge.lower() not in names))
    if hex_value(w3.eth.get_block(end)['hash']) != end_hash:
        raise RuntimeError('Curve range changed during collection')
    return items, end_hash


def insert_row(connection, row):
    connection.execute('INSERT INTO curve_gauge_votes (' + ','.join(COLUMNS) + ') VALUES (' + ','.join('?' for _ in COLUMNS) + ')',
                       tuple(row[key] for key in COLUMNS))


def matches_import(row, item):
    candidate = item['row']
    return (all(hex_value(row[key]) == hex_value(candidate[key]) for key in ('gauge','account','txn_hash'))
            and all(row[key] == candidate[key] for key in ('weight','timestamp','date_str','block'))
            and Decimal(row['amount']) in (Decimal(item['legacy_amount']), Decimal(candidate['amount'])))


def adopt_boundary(store, w3, controller, ve, gauges):
    if not store.rehearsal or w3.eth.chain_id != 1:
        raise RuntimeError('Adopt Curve boundaries in an inactive mainnet import')
    rows = store.read(lambda connection: [dict(row) for row in connection.execute(
        'SELECT * FROM curve_gauge_votes WHERE block=(SELECT max(block) FROM curve_gauge_votes) ORDER BY id')])
    boundary = rows[0]['block'] if rows else DEPLOY_BLOCK - 1
    if boundary > w3.eth.get_block('finalized')['number']:
        raise RuntimeError('Wait for the imported Curve boundary to become finalized')
    items, block_hash = collect(w3, controller, ve, gauges, boundary, boundary)
    unmatched = set(range(len(items)))
    duplicates = 0
    for row in rows:
        candidates = [i for i, item in enumerate(items) if matches_import(row, item)]
        if not candidates:
            raise RuntimeError('Imported Curve boundary row does not match chain data; reconciliation required')
        available = [i for i in candidates if i in unmatched]
        if available:
            unmatched.remove(available[0])
        else:
            duplicates += 1  # Retain existing source duplicates, never create replacements.
    def adopt(connection):
        connection.execute('INSERT INTO curve_checkpoint VALUES (?,?,?,?,?)', (STREAM,1,boundary+1,boundary+1,block_hash))
        notifications.add_stream(connection, STREAM, boundary, block_hash)
        for index, item in enumerate(items):
            connection.execute('INSERT INTO curve_events VALUES (?,?)', (item['key'], boundary))
            if index in unmatched:
                insert_row(connection, item['row'])
        return dict(next_block=boundary+1, imported_boundary_rows=len(rows), source_duplicates=duplicates,
                    missing_boundary_events=len(unmatched))
    return store.write(adopt)


def alerts(item):
    row = item['row']
    if item['unknown']:
        yield ('unknown-gauge', 'WAVEY_ALERTS', "New Curve vote for a gauge that doesn't have a name!\n" + row['gauge'], False)
    if Decimal(row['amount']) > 1_000_000 and row['account_alias']:
        message = '🗳️ Curve Gauge Vote Detected'
        message += '\n\n ' + row['account_alias']
        message += '\n\n🔗 [View on Etherscan](https://etherscan.io/tx/' + row['txn_hash'] + ')'
        yield ('large-vote', 'YLOCKERS', message, True)


def scan_once(store, w3, controller, ve, gauges, generation, send):
    previous = store.read(checkpoint)
    if previous['chain_id'] != 1 or w3.eth.chain_id != 1:
        raise RuntimeError('Curve checkpoint chain does not match RPC')
    start = previous['next_block']
    if hex_value(w3.eth.get_block(start - 1)['hash']) != previous['previous_hash']:
        raise RuntimeError('Curve checkpoint block changed; reconciliation required')
    height = w3.eth.get_block('latest')['number']
    if start > height:
        return
    end = min(start + CHUNK_SIZE - 1, height)
    items, block_hash = collect(w3, controller, ve, gauges, start, end)
    if hex_value(w3.eth.get_block(start - 1)['hash']) != previous['previous_hash']:
        raise RuntimeError('Curve checkpoint block changed during collection')
    def commit(connection):
        if checkpoint(connection) != previous or notifications.state(connection, STREAM)['generation'] != generation:
            raise RuntimeError('Curve scan session or checkpoint advanced concurrently')
        claims = []
        for item in items:
            if not connection.execute('INSERT INTO curve_events VALUES (?,?) ON CONFLICT(event_key) DO NOTHING',
                                      (item['key'], item['row']['block'])).rowcount:
                continue
            insert_row(connection, item['row'])
            for kind, destination, message, per_block in alerts(item):
                claim = notifications.decide(connection, STREAM, generation, item['key'], item['row']['block'],
                                             kind, destination, message, once_per_block=per_block)
                if claim:
                    claims.append((claim, message))
        connection.execute('UPDATE curve_checkpoint SET next_block=?,previous_hash=? WHERE stream=?', (end+1,block_hash,STREAM))
        return claims
    claims = store.write(commit)
    logger.info('Curve scanned blocks %s-%s: %s events, %s alerts',start,end,len(items),len(claims))
    for claim, message in claims:
        notifications.dispatch(store, claim, message, send)


def get_gauge_list(w3):
    import requests
    response = requests.get('https://api.curve.finance/api/getAllGauges', timeout=(5,30))
    response.raise_for_status()
    data = response.json()['data']
    if not isinstance(data, dict) or not data:
        raise RuntimeError('Curve gauge metadata is unavailable; do not classify every gauge as unknown')
    return {w3.to_checksum_address(value.get('rootGauge') or value['gauge']): re.sub(r'\s*\(.*?\)', '', name)
            for name, value in data.items()}


def enable_future_alerts(store, w3):
    latest = w3.eth.get_block('latest')
    previous = store.read(checkpoint)
    if (w3.eth.chain_id != 1 or previous['chain_id'] != 1 or previous['next_block'] <= latest['number']
            or hex_value(w3.eth.get_block(previous['next_block'] - 1)['hash']) != previous['previous_hash']):
        raise RuntimeError('Curve must finish validated silent catch-up before enabling alerts')
    def enable(connection):
        if checkpoint(connection)['next_block'] <= latest['number']:
            raise RuntimeError('Curve catch-up position changed before activation')
        notifications.enable(connection, STREAM, latest['number'], hex_value(latest['hash']))
    store.write(enable)


def main():
    logging.basicConfig(level=logging.INFO,format='%(asctime)s %(levelname)s %(message)s')
    parser = argparse.ArgumentParser(description=__doc__)
    actions = parser.add_mutually_exclusive_group()
    actions.add_argument('--prepare-import', action='store_true')
    actions.add_argument('--adopt-boundary', action='store_true')
    actions.add_argument('--enable-alerts', action='store_true')
    actions.add_argument('--mute-alerts', action='store_true')
    parser.add_argument('--once', action='store_true')
    args = parser.parse_args()
    from dotenv import load_dotenv
    load_dotenv()
    store = Store(os.environ['YEARN_DB_PATH'], os.environ['YEARN_IMPORT_SHA256'],
                  rehearsal=args.prepare_import or args.adopt_boundary)
    if args.prepare_import:
        prepare(store)
        return
    if args.mute_alerts:
        store.write(lambda connection: notifications.mute(connection, STREAM))
        return
    from web3 import Web3
    w3 = Web3(Web3.HTTPProvider(os.environ['WEB3_PROVIDER_URI'], request_kwargs={'timeout':60}))
    if args.enable_alerts:
        notifications.require_permission(STREAM)
        enable_future_alerts(store, w3)
        return
    controller, ve = contracts(w3)
    gauges = get_gauge_list(w3)
    if args.adopt_boundary:
        print(json.dumps(adopt_boundary(store, w3, controller, ve, gauges)))
        return
    if w3.eth.chain_id != 1:
        raise RuntimeError('Curve notifications require Ethereum mainnet')
    latest = w3.eth.get_block('latest')
    generation = notifications.start_session(store, STREAM, latest['number'], hex_value(latest['hash']))
    while True:
        scan_once(store, w3, controller, ve, gauges, generation, notifications.send_telegram)
        if args.once:
            return
        time.sleep(POLL_INTERVAL)


if __name__ == '__main__':
    main()
