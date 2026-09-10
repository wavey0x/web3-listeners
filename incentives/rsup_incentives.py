"""Protocol calculations; persistence and notification policy live in sqlite_worker."""

from datetime import datetime, timezone, timedelta
import json
import logging
from pathlib import Path
import sys

sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
from constants import RESUPPLY_GAUGES
from incentives.incentives_shared import get_token_price, get_bias, WEEK
from incentives import sqlite_worker

logger=logging.getLogger(__name__)

POLL_INTERVAL = 60 * 60
PROTOCOL = 'resupply'
RSUP = '0x419905009e4656fdC02418C7Df35B1E61Ed5F726'
EC = '0x33333333df05b0D52edD13D230461E5A0f5a4706'
MULTISIG = '0xFE11a5009f2121622271e7dd0FD470264e076af6'
GAUGE_CONTROLLER = '0x2F50D538606Fa9EDD2B11E2446BEb18C9D5846bB'
VOTIUM = '0x63942E31E98f1833A234077f47880A66136a2D1e'
VOTIUM_FEE = '0x29e3b0E8dF4Ee3f71a62C34847c34E139fC0b297'
CONVEX_DEPLOYER = '0x947B7742C403f20e5FaCcDAc5E092C943E7D0277'
VOTEMARKET_FACTORY = '0x96006425Da428E45c282008b00004a00002B345e'
CURVE_VOTERS = {'CONVEX': '0x989AEb4d175e16225E39E87d0D97A3360524AD80', 'PRISMA': '0x490b8C6007fFa5d3728A49c2ee199e51f05D2F7e'}

TOKEN = RSUP
STREAM = 'rsup-incentives'
TRANSACTION_GROUPED = False

def configure(web3):
    global w3, TOKEN_CONTRACT, gauge_controller, ec
    w3=web3
    root=Path(__file__).resolve().parents[1]/'abis'
    TOKEN_CONTRACT=w3.eth.contract(address=TOKEN,abi=json.loads((root/'erc20.json').read_text()))
    gauge_controller=w3.eth.contract(address=GAUGE_CONTROLLER,abi=json.loads((root/'gauge_controller.json').read_text()))
    ec=w3.eth.contract(address=EC,abi=json.loads((root/'emissions_controller.json').read_text()))

def transfer_logs(start,end):
    return TOKEN_CONTRACT.events.Transfer.get_logs(argument_filters={'from':EC,'to':MULTISIG},fromBlock=start,toBlock=end)

def calculate_efficiency(block_number: int, period_ts: int, total_incentives: float, votium_amount: float) -> tuple:
    """Calculate efficiency metrics for a given block"""
    votium_incentives = votium_amount / 2
    votemarket_incentives = (total_incentives - votium_amount) / 2
    rsup_price = get_token_price(RSUP)
    price_available = rsup_price is not None and rsup_price > 0
    if not price_available:
        logger.warning('Unable to fetch RSUP price; efficiency metrics will be null')
    gauge_data = {}
    convex_total_bias = 0
    prisma_total_bias = 0
    votemarket_total_bias = 0
    for gauge in RESUPPLY_GAUGES:
        convex_slope = gauge_controller.functions.vote_user_slopes(CURVE_VOTERS['CONVEX'], gauge).call(block_identifier=block_number)
        prisma_slope = gauge_controller.functions.vote_user_slopes(CURVE_VOTERS['PRISMA'], gauge).call(block_identifier=block_number)
        convex_bias = get_bias(convex_slope[0], convex_slope[2], period_ts) / 1e+18
        prisma_bias = get_bias(prisma_slope[0], prisma_slope[2], period_ts) / 1e+18
        total_gauge_bias = gauge_controller.functions.points_weight(gauge, period_ts).call(block_identifier=block_number)[0] / 1e+18
        relative_weight = gauge_controller.functions.gauge_relative_weight(gauge, period_ts).call(block_identifier=block_number) / 1e+18
        convex_total_bias += convex_bias
        prisma_total_bias += prisma_bias
        votemarket_gauge_bias = max(total_gauge_bias - convex_bias - prisma_bias, 0)
        votemarket_total_bias += votemarket_gauge_bias
        gauge_data[RESUPPLY_GAUGES[gauge]] = {'votium_bias': convex_bias, 'prisma_bias': prisma_bias, 'votemarket_bias': votemarket_gauge_bias, 'total_bias': total_gauge_bias, 'relative_weight': relative_weight}
    votemarket_bias = votemarket_total_bias
    votium_votes_per_usd = None
    votemarket_votes_per_usd = None
    if price_available and convex_total_bias > 0 and (votium_incentives > 0) and (votium_incentives * rsup_price > 0):
        votium_votes_per_usd = convex_total_bias / (votium_incentives * rsup_price)
    if price_available and votemarket_bias > 0 and (votemarket_incentives > 0) and (votemarket_incentives * rsup_price > 0):
        votemarket_votes_per_usd = votemarket_bias / (votemarket_incentives * rsup_price)
    logger.info(f'Votium total bias: {convex_total_bias:,.2f}')
    logger.info(f'Prisma total bias: {prisma_total_bias:,.2f}')
    logger.info(f'Votemarket total bias: {votemarket_bias:,.2f}')
    logger.info('Votium votes per USD: %s', f'{votium_votes_per_usd:,.2f}' if votium_votes_per_usd is not None else 'n/a')
    logger.info('Votemarket votes per USD: %s', f'{votemarket_votes_per_usd:,.2f}' if votemarket_votes_per_usd is not None else 'n/a')
    return (votium_votes_per_usd, votemarket_votes_per_usd, convex_total_bias, votemarket_bias, gauge_data)

def build_record(event, block_data, receipt, effective_block):
    block = event.blockNumber
    timestamp = block_data['timestamp']
    txn_hash = event.transactionHash.hex()
    log_index = event.logIndex
    epoch = ec.functions.getEpoch().call(block_identifier=block)
    votium_amt = 0
    votemarket_amt = 0
    transfer_sig = 'ddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef'
    votium_new_pattern = 0
    votium_old_pattern = 0
    logger.info(f"[DEBUG] Processing {len(receipt['logs'])} logs from txn {txn_hash}")
    logger.info(f'[DEBUG] Looking for RSUP={RSUP.lower()}')
    logger.info(f'[DEBUG] VOTIUM={VOTIUM.lower()}, VOTIUM_FEE={VOTIUM_FEE.lower()}')
    logger.info(f'[DEBUG] VOTEMARKET_FACTORY={VOTEMARKET_FACTORY.lower()}')
    unique_addrs = list(set((log['address'].lower() for log in receipt['logs'])))[:5]
    logger.info(f'[DEBUG] Sample log addresses: {unique_addrs}')
    rsup_transfer_count = 0
    for log in receipt['logs']:
        topic0 = log['topics'][0].hex() if len(log['topics']) > 0 else ''
        topic0_normalized = topic0.replace('0x', '').lower()
        if log['address'].lower() == RSUP.lower() and topic0_normalized == transfer_sig:
            rsup_transfer_count += 1
            from_addr = '0x' + log['topics'][1].hex()[-40:]
            to_addr = '0x' + log['topics'][2].hex()[-40:]
            data_hex = log['data'].hex()
            value = int(data_hex, 16) / 1e+18
            if from_addr.lower() == MULTISIG.lower() and (to_addr.lower() == VOTIUM.lower() or to_addr.lower() == VOTIUM_FEE.lower()):
                votium_new_pattern += value
                logger.info(f'[DEBUG] VOTIUM match: {value:,.2f} to {to_addr}')
            elif from_addr.lower() == MULTISIG.lower() and to_addr.lower() == CONVEX_DEPLOYER.lower():
                votium_old_pattern += value
                logger.info(f'[DEBUG] CONVEX match: {value:,.2f} to {to_addr}')
            elif to_addr.lower() == VOTEMARKET_FACTORY.lower():
                votemarket_amt += value
                logger.info(f'[DEBUG] VOTEMARKET match: {value:,.2f} to {to_addr}')
    logger.info(f'[DEBUG] Found {rsup_transfer_count} RSUP transfers')
    logger.info(f'[DEBUG] votium_new={votium_new_pattern:,.2f}, votium_old={votium_old_pattern:,.2f}, votemarket={votemarket_amt:,.2f}')
    if votium_new_pattern > 0:
        votium_amt = votium_new_pattern
        logger.info(f'Using new Votium pattern (direct deposits): {votium_amt:,.2f} RSUP')
    else:
        votium_amt = votium_old_pattern
        logger.info(f'Using old Votium pattern (via Convex): {votium_amt:,.2f} RSUP')
    total = votium_amt + votemarket_amt
    logger.info(f'Parsed transfers - Votium: {votium_amt:,.2f}, Votemarket: {votemarket_amt:,.2f}, Total: {total:,.2f}')
    period_start = int(timestamp / WEEK) * WEEK
    next_period_start = period_start + WEEK
    next_period_block = effective_block
    date_str = datetime.fromtimestamp(period_start, timezone.utc).strftime('%Y-%m-%d %H:%M UTC')
    votium_votes_per_usd, votemarket_votes_per_usd, votium_total_bias, votemarket_bias, gauge_data = calculate_efficiency(next_period_block, next_period_start, total, votium_amt)
    return dict(protocol=PROTOCOL, epoch=epoch, total_incentives=total, votium_amount=votium_amt, votemarket_amount=votemarket_amt, votium_votes_per_usd=votium_votes_per_usd, votemarket_votes_per_usd=votemarket_votes_per_usd, votium_votes=votium_total_bias, votemarket_votes=votemarket_bias, gauge_data=gauge_data, transaction_hash=txn_hash, block_number=block, timestamp=timestamp, date_str=date_str, period_start=period_start, log_index=log_index)

def render_message(row):
    epoch = row['epoch']
    total = row['total_incentives']
    votium_amt = row['votium_amount']
    votemarket_amt = row['votemarket_amount']
    votium_votes = row['votium_votes']
    votemarket_votes = row['votemarket_votes']
    date_str = row['date_str']
    txn_hash = row['transaction_hash']
    gauge_data = row['gauge_data']
    date_obj = datetime.strptime(date_str, '%Y-%m-%d %H:%M UTC')
    mmddyy = (date_obj + timedelta(days=7)).strftime('%m/%d/%y')
    msg = f'🎯 *RSUP Incentives Report*\n\n'
    msg += f'Epoch {epoch} distributions | Effective {mmddyy}\n\n'
    msg += f'*Votium*: \n'
    votes_per_rsup = votium_votes / votium_amt if votium_amt > 0 else 0
    msg += f'- {votes_per_rsup:,.0f} votes/RSUP\n'
    msg += f'- {votium_votes:,.0f} votes for {votium_amt:,.0f} RSUP\n\n'
    msg += f'*Votemarket*: \n'
    votes_per_rsup = votemarket_votes / votemarket_amt if votemarket_amt > 0 else 0
    msg += f'- {votes_per_rsup:,.0f} votes/RSUP\n'
    msg += f'- {votemarket_votes:,.0f} votes for {votemarket_amt:,.0f} RSUP\n\n'
    msg += f'━━━━━━━━━━\n'
    for gauge_name, data in gauge_data.items():
        gauge_address = next((addr for addr, name in RESUPPLY_GAUGES.items() if name == gauge_name), None)
        if gauge_address:
            msg += f'\n[{gauge_name}](https://crv.lol/?gauge={gauge_address})\n'
            msg += f"- Votes: {data.get('votemarket_bias', data['total_bias']):,.0f} ({data['relative_weight'] * 100:.2f}%)\n"
    msg += f'\n🔗 [Distro txn](https://etherscan.io/tx/{txn_hash})'
    return msg

def main():
    sqlite_worker.main(sys.modules[__name__])


if __name__ == '__main__':
    sqlite_worker.cli(sys.modules[__name__])
