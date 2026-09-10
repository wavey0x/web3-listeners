import argparse
import json
import os
import sys
from datetime import datetime, timezone

from dotenv import load_dotenv
from web3 import Web3

ROOT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), os.pardir))
if ROOT_DIR not in sys.path:
    sys.path.append(ROOT_DIR)

import utils
from sqlite_store import Store
from constants import RESUPPLY_GAUGES
from incentives.incentives_shared import WEEK, get_bias
from utils.web3_utils import closest_block_after_timestamp


RSUP = '0x419905009e4656fdC02418C7Df35B1E61Ed5F726'
MULTISIG = '0xFE11a5009f2121622271e7dd0FD470264e076af6'
VOTIUM = '0x63942E31E98f1833A234077f47880A66136a2D1e'
VOTIUM_FEE = '0x29e3b0E8dF4Ee3f71a62C34847c34E139fC0b297'
CONVEX_DEPLOYER = '0x947B7742C403f20e5FaCcDAc5E092C943E7D0277'
VOTEMARKET_FACTORY = '0x96006425Da428E45c282008b00004a00002B345e'
GAUGE_CONTROLLER = '0x2F50D538606Fa9EDD2B11E2446BEb18C9D5846bB'
CONVEX_VOTER = '0x989AEb4d175e16225E39E87d0D97A3360524AD80'
PRISMA_VOTER = '0x490b8C6007fFa5d3728A49c2ee199e51f05D2F7e'
TRANSFER_SIG = 'ddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef'
GAUGE_TO_NAME = {addr.lower(): name for addr, name in RESUPPLY_GAUGES.items()}
NAME_TO_GAUGE = {name: addr for addr, name in RESUPPLY_GAUGES.items()}


def parse_amounts(w3: Web3, txn_hash: str) -> tuple[float, float]:
    receipt = w3.eth.get_transaction_receipt(txn_hash)
    votium_new = 0.0
    votium_old = 0.0
    votemarket = 0.0

    for log in receipt['logs']:
        topic0 = log['topics'][0].hex().replace('0x', '').lower() if log['topics'] else ''
        if log['address'].lower() != RSUP.lower() or topic0 != TRANSFER_SIG:
            continue

        from_addr = '0x' + log['topics'][1].hex()[-40:]
        to_addr = '0x' + log['topics'][2].hex()[-40:]
        value = int(log['data'].hex(), 16) / 1e18

        if (from_addr.lower() == MULTISIG.lower() and
                to_addr.lower() in (VOTIUM.lower(), VOTIUM_FEE.lower())):
            votium_new += value
        elif from_addr.lower() == MULTISIG.lower() and to_addr.lower() == CONVEX_DEPLOYER.lower():
            votium_old += value
        elif to_addr.lower() == VOTEMARKET_FACTORY.lower():
            votemarket += value

    return (votium_new if votium_new > 0 else votium_old), votemarket


def calculate_votes(gauge_controller, block_number: int, period_ts: int, existing_gauge_data: dict) -> tuple[float, float, dict]:
    convex_total = 0.0
    votemarket_total = 0.0
    gauge_data = {}

    for gauge_name, existing_data in existing_gauge_data.items():
        gauge = NAME_TO_GAUGE[gauge_name]
        prisma_slope = gauge_controller.functions.vote_user_slopes(
            PRISMA_VOTER,
            gauge,
        ).call(block_identifier=block_number)
        prisma_bias = get_bias(prisma_slope[0], prisma_slope[2], period_ts) / 1e18
        convex_bias = existing_data.get('votium_bias', 0.0)
        total_bias = existing_data.get('total_bias', 0.0)
        relative_weight = existing_data.get('relative_weight', 0.0)
        votemarket_bias = max(total_bias - convex_bias - prisma_bias, 0)

        convex_total += convex_bias
        votemarket_total += votemarket_bias
        gauge_data[gauge_name] = {
            'votium_bias': convex_bias,
            'prisma_bias': prisma_bias,
            'votemarket_bias': votemarket_bias,
            'total_bias': total_bias,
            'relative_weight': relative_weight,
        }

    return convex_total, votemarket_total, gauge_data


def infer_implied_price(row) -> float | None:
    if (row['votium_votes_per_usd'] is not None and row['votium_amount'] > 0 and
            row['votium_votes'] > 0):
        return row['votium_votes'] / ((row['votium_amount'] / 2) * row['votium_votes_per_usd'])

    if (row['votemarket_votes_per_usd'] is not None and row['votemarket_amount'] > 0 and
            row['votemarket_votes'] > 0):
        return row['votemarket_votes'] / ((row['votemarket_amount'] / 2) * row['votemarket_votes_per_usd'])

    return None


def read_rows(connection):
    rows = [dict(row) for row in connection.execute(
        "SELECT * FROM incentives WHERE protocol='resupply' ORDER BY timestamp,id")]
    for row in rows:
        row['gauge_data'] = json.loads(row['gauge_data'])
    return rows


def apply_updates(store, original_rows, updates):
    """Apply the reviewed calculation batch only if its input rows are still current."""
    fields = ('total_incentives','votium_amount','votemarket_amount','votium_votes','votemarket_votes',
              'votium_votes_per_usd','votemarket_votes_per_usd','gauge_data')
    sources = ('new_total','new_votium_amount','new_votemarket_amount','new_votium_votes','new_votemarket_votes',
               'new_votium_votes_per_usd','new_votemarket_votes_per_usd','new_gauge_data')
    if [row['id'] for row in updates] != [row['id'] for row in original_rows]:
        raise RuntimeError('Correction batch must cover the inspected rows exactly once')
    def commit(connection):
        if read_rows(connection) != original_rows:
            raise RuntimeError('Incentive data changed during calculation; rerun the dry run')
        for row in updates:
            values = [row[key] for key in sources]
            values[-1] = json.dumps(values[-1],allow_nan=False)
            connection.execute('UPDATE incentives SET '+','.join(field+'=?' for field in fields)+
                " WHERE protocol='resupply' AND id=?",(*values,row['id']))
    store.write(commit)


def main():
    parser = argparse.ArgumentParser(description='Backfill RSUP incentive rows with corrected amounts and votes')
    parser.add_argument('--apply', action='store_true', help='persist changes to the database')
    args = parser.parse_args()

    load_dotenv(os.path.join(ROOT_DIR, '.env'))
    store = Store.from_env()
    web3_provider_uri = os.getenv('WEB3_PROVIDER_URI')
    if not web3_provider_uri:
        raise RuntimeError('WEB3_PROVIDER_URI must be set')

    w3 = Web3(Web3.HTTPProvider(web3_provider_uri, request_kwargs={'timeout': 60}))
    if not w3.is_connected():
        raise RuntimeError('Failed to connect to Ethereum node')

    gauge_controller = w3.eth.contract(
        address=GAUGE_CONTROLLER,
        abi=utils.load_abi('./abis/gauge_controller.json'),
    )

    rows = store.read(read_rows)

    updates = []
    recent_nonzero_rows = []
    for row in rows:
        period_start = int(row['timestamp'] / WEEK) * WEEK
        period_ts = period_start + WEEK
        block = closest_block_after_timestamp(w3, period_ts)
        parsed_votium_amount, parsed_votemarket_amount = parse_amounts(w3, row['transaction_hash'])
        parsed_total = parsed_votium_amount + parsed_votemarket_amount
        votium_votes, votemarket_votes, gauge_data = calculate_votes(
            gauge_controller,
            block,
            period_ts,
            row['gauge_data'],
        )
        implied_price = infer_implied_price(row)
        votium_votes_per_usd = row['votium_votes_per_usd']
        votemarket_votes_per_usd = None

        if implied_price and parsed_votium_amount > 0 and votium_votes > 0:
            votium_votes_per_usd = votium_votes / ((parsed_votium_amount / 2) * implied_price)
        if implied_price and parsed_votemarket_amount > 0:
            votemarket_votes_per_usd = votemarket_votes / ((parsed_votemarket_amount / 2) * implied_price)

        update = {
            'id': row['id'],
            'epoch': row['epoch'],
            'date_str': row['date_str'],
            'transaction_hash': row['transaction_hash'],
            'old_total': row['total_incentives'],
            'new_total': parsed_total,
            'old_votium_amount': row['votium_amount'],
            'new_votium_amount': parsed_votium_amount,
            'old_votemarket_amount': row['votemarket_amount'],
            'new_votemarket_amount': parsed_votemarket_amount,
            'old_votium_votes': row['votium_votes'],
            'new_votium_votes': votium_votes,
            'old_votemarket_votes': row['votemarket_votes'],
            'new_votemarket_votes': votemarket_votes,
            'new_votium_votes_per_usd': votium_votes_per_usd,
            'new_votemarket_votes_per_usd': votemarket_votes_per_usd,
            'new_gauge_data': gauge_data,
        }
        updates.append(update)

        if row['date_str'] >= '2025-11-20 00:00 UTC' and parsed_votemarket_amount > 0:
            recent_nonzero_rows.append(update)

    changed = [
        row for row in updates
        if any([
            abs(row['old_total'] - row['new_total']) > 1e-9,
            abs(row['old_votium_amount'] - row['new_votium_amount']) > 1e-9,
            abs(row['old_votemarket_amount'] - row['new_votemarket_amount']) > 1e-9,
            abs(row['old_votium_votes'] - row['new_votium_votes']) > 1e-9,
            abs(row['old_votemarket_votes'] - row['new_votemarket_votes']) > 1e-9,
        ])
    ]

    print(f'RSUP rows scanned: {len(updates)}')
    print(f'Rows with material changes: {len(changed)}')
    for row in changed:
        print({
            'id': row['id'],
            'epoch': row['epoch'],
            'date_str': row['date_str'],
            'old_total': row['old_total'],
            'new_total': row['new_total'],
            'old_vm_amount': row['old_votemarket_amount'],
            'new_vm_amount': row['new_votemarket_amount'],
            'old_vm_votes': row['old_votemarket_votes'],
            'new_vm_votes': row['new_votemarket_votes'],
        })

    print('Recent parser-confirmed nonzero Votemarket rows:')
    for row in recent_nonzero_rows:
        old_votium_per_rsup = row['old_votium_votes'] / row['old_votium_amount'] if row['old_votium_amount'] else None
        new_votium_per_rsup = row['new_votium_votes'] / row['new_votium_amount'] if row['new_votium_amount'] else None
        old_vm_per_rsup = row['old_votemarket_votes'] / row['old_votemarket_amount'] if row['old_votemarket_amount'] else None
        new_vm_per_rsup = row['new_votemarket_votes'] / row['new_votemarket_amount'] if row['new_votemarket_amount'] else None
        print({
            'date_str': row['date_str'],
            'votium_votes_per_rsup': new_votium_per_rsup,
            'old_votemarket_votes_per_rsup': old_vm_per_rsup,
            'new_votemarket_votes_per_rsup': new_vm_per_rsup,
            'old_gap': old_votium_per_rsup - old_vm_per_rsup if old_votium_per_rsup is not None and old_vm_per_rsup is not None else None,
            'new_gap': new_votium_per_rsup - new_vm_per_rsup if new_votium_per_rsup is not None and new_vm_per_rsup is not None else None,
        })

    if not args.apply:
        print('Dry run only. Re-run with --apply to persist changes.')
        return

    apply_updates(store, rows, updates)

    print(f'Applied updates to {len(updates)} RSUP rows.')


if __name__ == '__main__':
    main()
