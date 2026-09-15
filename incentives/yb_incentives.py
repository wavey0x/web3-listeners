"""Protocol calculations; persistence and notification policy live in sqlite_worker."""

from datetime import datetime, timezone, timedelta
import json
import logging
from pathlib import Path
import sys

sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
from constants import YB_GAUGES, YB, DEPOSIT_DIVIDER, VOTIUM_HELPER, VOTEMARKET_HELPER
from incentives.incentives_shared import get_token_price, get_bias, WEEK
from incentives import sqlite_worker

logger=logging.getLogger(__name__)

POLL_INTERVAL = 60 * 60
PROTOCOL = 'yieldbasis'
GAUGE_CONTROLLER = '0x2F50D538606Fa9EDD2B11E2446BEb18C9D5846bB'
CURVE_VOTERS = {'CONVEX': '0x989AEb4d175e16225E39E87d0D97A3360524AD80'}

TOKEN = YB
STREAM = 'yb-incentives'
TRANSACTION_GROUPED = True

def configure(web3):
    global w3, TOKEN_CONTRACT, gauge_controller
    w3=web3
    root=Path(__file__).resolve().parents[1]/'abis'
    TOKEN_CONTRACT=w3.eth.contract(address=TOKEN,abi=json.loads((root/'erc20.json').read_text()))
    gauge_controller=w3.eth.contract(address=GAUGE_CONTROLLER,abi=json.loads((root/'gauge_controller.json').read_text()))
    global yb
    yb=TOKEN_CONTRACT

def transfer_logs(start,end):
    return TOKEN_CONTRACT.events.Transfer.get_logs(argument_filters={'from':DEPOSIT_DIVIDER},fromBlock=start,toBlock=end)

def calculate_efficiency(block_number: int, period_ts: int, total_incentives: float, votium_amount: float) -> tuple:
    """Calculate efficiency metrics for a given block"""
    votium_incentives = votium_amount / 2
    votemarket_incentives = (total_incentives - votium_amount) / 2
    yb_price = get_token_price(YB)
    price_available = yb_price is not None and yb_price > 0
    if not price_available:
        logger.warning('Unable to fetch YB price; efficiency metrics will be null')
    gauge_data = {}
    votium_total_bias = 0
    total_bias = 0
    for gauge in YB_GAUGES:
        convex_slope = gauge_controller.functions.vote_user_slopes(CURVE_VOTERS['CONVEX'], gauge).call(block_identifier=block_number)
        convex_bias = get_bias(convex_slope[0], convex_slope[2], period_ts) / 1e+18
        total_gauge_bias = gauge_controller.functions.points_weight(gauge, period_ts).call(block_identifier=block_number)[0] / 1e+18
        relative_weight = gauge_controller.functions.gauge_relative_weight(gauge, period_ts).call(block_identifier=block_number) / 1e+18
        votium_total_bias += convex_bias
        total_bias += total_gauge_bias
        gauge_data[YB_GAUGES[gauge]] = {'votium_bias': convex_bias, 'total_bias': total_gauge_bias, 'relative_weight': relative_weight}
    votemarket_bias = total_bias - votium_total_bias
    votium_votes_per_usd = None
    votemarket_votes_per_usd = None
    if price_available and votium_total_bias > 0 and (votium_incentives > 0) and (votium_incentives * yb_price > 0):
        votium_votes_per_usd = votium_total_bias / (votium_incentives * yb_price)
    if price_available and votemarket_bias > 0 and (votemarket_incentives > 0) and (votemarket_incentives * yb_price > 0):
        votemarket_votes_per_usd = votemarket_bias / (votemarket_incentives * yb_price)
    logger.info(f'Votium total bias: {votium_total_bias:,.2f}')
    logger.info(f'Votemarket total bias: {votemarket_bias:,.2f}')
    logger.info('Votium votes per USD: %s', f'{votium_votes_per_usd:,.2f}' if votium_votes_per_usd is not None else 'n/a')
    logger.info('Votemarket votes per USD: %s', f'{votemarket_votes_per_usd:,.2f}' if votemarket_votes_per_usd is not None else 'n/a')
    return (votium_votes_per_usd, votemarket_votes_per_usd, votium_total_bias, votemarket_bias, gauge_data)

def build_record(event, block_data, receipt, effective_block):
    block = event.blockNumber
    timestamp = block_data['timestamp']
    txn_hash = event.transactionHash.hex()
    log_index = event.logIndex
    total = event['args']['value'] / 1e+18
    votium_amt = 0
    votemarket_amt = 0
    transfer_topic = '0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef'
    for log in receipt['logs']:
        if log['address'].lower() == YB.lower() and len(log['topics']) > 0 and (log['topics'][0].hex() == transfer_topic):
            transfer_event = yb.events.Transfer().process_log(log)
            from_addr = transfer_event['args'].get('sender') or transfer_event['args'].get('from')
            to_addr = transfer_event['args'].get('receiver') or transfer_event['args'].get('to')
            value = transfer_event['args']['value'] / 1e+18
            if from_addr.lower() == DEPOSIT_DIVIDER.lower() and to_addr.lower() == VOTIUM_HELPER.lower():
                votium_amt += value
            elif from_addr.lower() == DEPOSIT_DIVIDER.lower() and to_addr.lower() == VOTEMARKET_HELPER.lower():
                votemarket_amt += value
    computed_total = votium_amt + votemarket_amt
    if computed_total > 0:
        total = computed_total
    else:
        logger.warning('No helper transfers detected for txn %s; using raw log value', txn_hash)
    period_start = int(timestamp / WEEK) * WEEK
    next_period_start = period_start + WEEK
    next_period_block = effective_block
    date_str = datetime.fromtimestamp(period_start, timezone.utc).strftime('%Y-%m-%d %H:%M UTC')
    votium_votes_per_usd, votemarket_votes_per_usd, votium_total_bias, votemarket_bias, gauge_data = calculate_efficiency(next_period_block, next_period_start, total, votium_amt)
    return dict(protocol=PROTOCOL, epoch=None, total_incentives=total, votium_amount=votium_amt, votemarket_amount=votemarket_amt, votium_votes_per_usd=votium_votes_per_usd, votemarket_votes_per_usd=votemarket_votes_per_usd, votium_votes=votium_total_bias, votemarket_votes=votemarket_bias, gauge_data=gauge_data, transaction_hash=txn_hash, block_number=block, timestamp=timestamp, date_str=date_str, period_start=period_start, log_index=log_index)

def render_message(row):
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
    msg = f'🎯 *YB Incentives Report*\n\n'
    msg += f'Distributions | Effective {mmddyy}\n\n'
    msg += f'*Votium*: \n'
    votes_per_yb = votium_votes / votium_amt if votium_amt > 0 else 0
    msg += f'- {votes_per_yb:,.0f} votes/YB\n'
    msg += f'- {votium_votes:,.0f} votes for {votium_amt:,.0f} YB\n\n'
    msg += f'*Votemarket*: \n'
    votes_per_yb = votemarket_votes / votemarket_amt if votemarket_amt > 0 else 0
    msg += f'- {votes_per_yb:,.0f} votes/YB\n'
    msg += f'- {votemarket_votes:,.0f} votes for {votemarket_amt:,.0f} YB\n\n'
    msg += f'━━━━━━━━━━\n'
    for gauge_name, data in gauge_data.items():
        gauge_address = next((addr for addr, name in YB_GAUGES.items() if name == gauge_name), None)
        if gauge_address:
            msg += f'\n[{gauge_name}](https://crv.lol/?gauge={gauge_address})\n'
            msg += f"- Votes: {data['total_bias']:,.0f} ({data['relative_weight'] * 100:.2f}%)\n"
    msg += f'\n🔗 [Distro txn](https://etherscan.io/tx/{txn_hash})'
    return msg

def main():
    sqlite_worker.main(sys.modules[__name__])


if __name__ == '__main__':
    sqlite_worker.recovery.entrypoint(lambda: sqlite_worker.cli(sys.modules[__name__]))
