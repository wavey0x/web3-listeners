from web3 import Web3
from sqlalchemy import create_engine, MetaData, select
from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import SQLAlchemyError, IntegrityError
import time
from datetime import datetime, timezone, timedelta
import os
import sys
import requests
from dotenv import load_dotenv
import logging
import telebot

# Ensure project root on path for shared constants/utils when run directly
ROOT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), os.pardir))
if ROOT_DIR not in sys.path:
    sys.path.append(ROOT_DIR)

from constants import CHAT_IDS, YB_GAUGES, YB, DEPOSIT_DIVIDER, VOTIUM_HELPER, VOTEMARKET_HELPER
import utils
load_dotenv()
from incentives.config import INCENTIVE_START_TIMESTAMPS, resolve_chat_id
from incentives.schema import create_tables
from utils.web3_utils import closest_block_before_timestamp, closest_block_after_timestamp
from incentives.incentives_shared import get_periods, get_token_price, get_bias, WEEK

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

# Constants
WEB3_PROVIDER_URI = os.getenv('WEB3_PROVIDER_URI')
DATABASE_URI = os.getenv('DATABASE_URI')
TELEGRAM_BOT_TOKEN = os.getenv('WAVEY_ALERTS_BOT_KEY')
POLL_INTERVAL = 60 * 60  # Check every hour
PROTOCOL = 'yieldbasis'

# Initialize Telegram bot
bot = telebot.TeleBot(TELEGRAM_BOT_TOKEN)

# Contract addresses
GAUGE_CONTROLLER = '0x2F50D538606Fa9EDD2B11E2446BEb18C9D5846bB'
CURVE_VOTERS = {
    'CONVEX': '0x989AEb4d175e16225E39E87d0D97A3360524AD80'
}

# Connect to Ethereum network
w3 = Web3(Web3.HTTPProvider(WEB3_PROVIDER_URI, request_kwargs={'timeout': 60}))
if not w3.is_connected():
    raise Exception("Failed to connect to Ethereum node")

# Set up PostgreSQL connection
engine = create_engine(DATABASE_URI)
Session = sessionmaker(bind=engine)
session = Session()
metadata = MetaData()

# Create tables
incentives_table = create_tables(metadata)
metadata.create_all(engine)

# Load ABIs
yb_abi = utils.load_abi('./abis/erc20.json')
gauge_controller_abi = utils.load_abi('./abis/gauge_controller.json')

# Initialize contracts
yb = w3.eth.contract(address=YB, abi=yb_abi)
gauge_controller = w3.eth.contract(address=GAUGE_CONTROLLER, abi=gauge_controller_abi)

def get_last_processed_period():
    """Get the last period we've processed from the database"""
    try:
        with engine.connect() as conn:
            query = select(incentives_table.c.period_start)
            query = query.where(incentives_table.c.protocol == PROTOCOL)
            query = query.order_by(incentives_table.c.period_start.desc()).limit(1)
            result = conn.execute(query).scalar()
            return result
    except SQLAlchemyError as e:
        logger.error(f"Database error in get_last_processed_period: {str(e)}")
        raise

def get_missing_periods():
    """Get list of periods that need to be processed"""
    current_time = int(time.time())
    current_period = int(current_time / WEEK) * WEEK
    last_processed = get_last_processed_period()

    if last_processed is None:
        start_ts = INCENTIVE_START_TIMESTAMPS.get(PROTOCOL)
        start_period = int(start_ts / WEEK) * WEEK if start_ts else current_period
        # Only include periods up to current time
        return list(range(start_period, current_period, WEEK))

    # Get all periods between last processed and current
    return list(range(last_processed + WEEK, current_period, WEEK))

def process_period(period_start):
    """Process all incentive transfers for a given period"""
    try:
        # Skip if period is in the future
        current_time = int(time.time())
        if period_start > current_time:
            logger.info(f"Skipping future period {period_start} (starts {datetime.fromtimestamp(period_start, timezone.utc).strftime('%Y-%m-%d %H:%M UTC')})")
            return

        # Get block range for this period
        period_end = period_start + WEEK

        # Get block numbers for period start and end using helper function
        start_block = closest_block_before_timestamp(w3, period_start)
        end_block = closest_block_before_timestamp(w3, period_end)

        logger.info(f"Processing period {period_start} to {period_end}")
        logger.info(f"Block range: {start_block} to {end_block}")

        # Get Transfer events from DEPOSIT_DIVIDER for this period
        logger.info(f"[YB] Fetching Transfer events from {DEPOSIT_DIVIDER}")
        logs = yb.events.Transfer.get_logs(
            argument_filters={'from': DEPOSIT_DIVIDER},
            fromBlock=start_block,
            toBlock=min(end_block, w3.eth.block_number)
        )

        logger.info(f"[YB] Found {len(logs)} Transfer events for period {period_start}")

        processed_transactions = set()
        for log in logs:
            txn_hash = log['transactionHash'].hex()
            if txn_hash in processed_transactions:
                continue
            processed_transactions.add(txn_hash)
            handle_incentive_transfer(log)

        logger.info(f"[YB] Completed processing period {period_start}")

    except Exception as e:
        import traceback
        logger.error(f"Error processing period {period_start}: {str(e)}")
        logger.error(traceback.format_exc())
        # Don't re-raise the error, just log it and continue
        return

def calculate_efficiency(block_number: int, period_ts: int, total_incentives: float, votium_amount: float) -> tuple:
    """Calculate efficiency metrics for a given block"""
    try:
        # Calculate incentives
        votium_incentives = votium_amount / 2  # Divide by 2 because each campaign is 2 weeks
        votemarket_incentives = (total_incentives - votium_amount) / 2

        yb_price = get_token_price(YB)
        price_available = yb_price is not None and yb_price > 0
        if not price_available:
            logger.warning("Unable to fetch YB price; efficiency metrics will be null")

        # Initialize bias counters
        gauge_data = {}
        votium_total_bias = 0
        total_bias = 0

        # Calculate biases for each gauge
        for gauge in YB_GAUGES:
            try:
                convex_slope = gauge_controller.functions.vote_user_slopes(
                    CURVE_VOTERS['CONVEX'],
                    gauge
                ).call(block_identifier=block_number)

                convex_bias = get_bias(convex_slope[0], convex_slope[2], period_ts) / 1e18
                total_gauge_bias = gauge_controller.functions.points_weight(
                    gauge,
                    period_ts
                ).call(block_identifier=block_number)[0] / 1e18

                # Get relative weight for this gauge
                relative_weight = gauge_controller.functions.gauge_relative_weight(
                    gauge,
                    period_ts
                ).call(block_identifier=block_number) / 1e18

                votium_total_bias += convex_bias
                total_bias += total_gauge_bias

                # Store gauge data
                gauge_data[YB_GAUGES[gauge]] = {
                    'votium_bias': convex_bias,
                    'total_bias': total_gauge_bias,
                    'relative_weight': relative_weight
                }

            except Exception as e:
                logger.warning(f"Failed to get gauge data for {YB_GAUGES[gauge]}: {str(e)}")
                continue

        votemarket_bias = total_bias - votium_total_bias

        # Calculate efficiency metrics - votes per USD
        votium_votes_per_usd = None
        votemarket_votes_per_usd = None
        if (price_available and votium_total_bias > 0 and
                votium_incentives > 0 and votium_incentives * yb_price > 0):
            votium_votes_per_usd = votium_total_bias / (votium_incentives * yb_price)

        if (price_available and votemarket_bias > 0 and
                votemarket_incentives > 0 and votemarket_incentives * yb_price > 0):
            votemarket_votes_per_usd = votemarket_bias / (votemarket_incentives * yb_price)

        logger.info(f'Votium total bias: {votium_total_bias:,.2f}')
        logger.info(f'Votemarket total bias: {votemarket_bias:,.2f}')
        logger.info(
            'Votium votes per USD: %s',
            f"{votium_votes_per_usd:,.2f}" if votium_votes_per_usd is not None else "n/a"
        )
        logger.info(
            'Votemarket votes per USD: %s',
            f"{votemarket_votes_per_usd:,.2f}" if votemarket_votes_per_usd is not None else "n/a"
        )

        return votium_votes_per_usd, votemarket_votes_per_usd, votium_total_bias, votemarket_bias, gauge_data

    except Exception as e:
        logger.error(f"Error calculating efficiency: {str(e)}")
        return 0, 0, 0, 0, {}

def send_telegram_alert(total: float, votium_amt: float, votemarket_amt: float,
                       votium_votes: float, votemarket_votes: float,
                       date_str: str, txn_hash: str, gauge_data: dict):
    """Send Telegram alert for incentive transfer"""
    try:
        # Convert date_str to MM/DD/YY format
        date_obj = datetime.strptime(date_str, '%Y-%m-%d %H:%M UTC')
        mmddyy = (date_obj + timedelta(days=7)).strftime('%m/%d/%y')

        msg = f"🎯 *YB Incentives Report*\n\n"
        msg += f"Distributions | Effective {mmddyy}\n\n"

        msg += f"*Votium*: \n"
        votes_per_yb = votium_votes / votium_amt if votium_amt > 0 else 0
        msg += f"- {votes_per_yb:,.0f} votes/YB\n"
        msg += f"- {votium_votes:,.0f} votes for {votium_amt:,.0f} YB\n\n"

        msg += f"*Votemarket*: \n"
        votes_per_yb = votemarket_votes / votemarket_amt if votemarket_amt > 0 else 0
        msg += f"- {votes_per_yb:,.0f} votes/YB\n"
        msg += f"- {votemarket_votes:,.0f} votes for {votemarket_amt:,.0f} YB\n\n"

        # Add gauge-specific data
        msg += f"━━━━━━━━━━\n"
        for gauge_name, data in gauge_data.items():
            # Find the gauge address for this gauge name
            gauge_address = next((addr for addr, name in YB_GAUGES.items() if name == gauge_name), None)
            if gauge_address:
                msg += f"\n[{gauge_name}](https://crv.lol/?gauge={gauge_address})\n"
                msg += f"- Votes: {data['total_bias']:,.0f} ({data['relative_weight']*100:.2f}%)\n"

        msg += f"\n🔗 [Distro txn](https://etherscan.io/tx/{txn_hash})"

        chat_id, chat_key = resolve_chat_id(PROTOCOL)
        if not chat_id:
            logger.error("Chat key %s not found in CHAT_IDS", chat_key)
            return
        bot.send_message(chat_id, msg, parse_mode="markdown", disable_web_page_preview=True)

    except Exception as e:
        logger.error(f"Error sending Telegram alert: {str(e)}")

def handle_incentive_transfer(event):
    block = event.blockNumber
    timestamp = w3.eth.get_block(block).timestamp
    txn_hash = event.transactionHash.hex()
    log_index = event.logIndex

    try:
        total = event['args']['value'] / 1e18
        receipt = w3.eth.get_transaction_receipt(txn_hash)
        votium_amt = 0
        votemarket_amt = 0

        # Track splits to votium and votemarket helpers
        # Transfer event signature: keccak256("Transfer(address,address,uint256)")
        transfer_topic = '0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef'

        for log in receipt['logs']:
            if log['address'].lower() == YB.lower() and len(log['topics']) > 0 and log['topics'][0].hex() == transfer_topic:
                transfer_event = yb.events.Transfer().process_log(log)
                # YB token uses 'sender' and 'receiver' parameter names instead of 'from' and 'to'
                from_addr = transfer_event['args'].get('sender') or transfer_event['args'].get('from')
                to_addr = transfer_event['args'].get('receiver') or transfer_event['args'].get('to')
                value = transfer_event['args']['value'] / 1e18

                if (from_addr.lower() == DEPOSIT_DIVIDER.lower() and
                    to_addr.lower() == VOTIUM_HELPER.lower()):
                    votium_amt += value
                elif (from_addr.lower() == DEPOSIT_DIVIDER.lower() and
                      to_addr.lower() == VOTEMARKET_HELPER.lower()):
                    votemarket_amt += value

        computed_total = votium_amt + votemarket_amt
        if computed_total > 0:
            total = computed_total
        else:
            logger.warning("No helper transfers detected for txn %s; using raw log value", txn_hash)

        # Calc period start and end
        period_start = int(timestamp / WEEK) * WEEK
        next_period_start = period_start + WEEK
        next_period_block = closest_block_after_timestamp(w3, next_period_start)
        date_str = datetime.fromtimestamp(period_start, timezone.utc).strftime('%Y-%m-%d %H:%M UTC')

        votium_votes_per_usd, votemarket_votes_per_usd, votium_total_bias, votemarket_bias, gauge_data = calculate_efficiency(next_period_block, next_period_start, total, votium_amt)

        # First, try to insert into database - this must succeed before sending alert
        ins = incentives_table.insert().values(
            protocol=PROTOCOL,
            epoch=None,
            total_incentives=total,
            votium_amount=votium_amt,
            votemarket_amount=votemarket_amt,
            votium_votes_per_usd=votium_votes_per_usd,
            votemarket_votes_per_usd=votemarket_votes_per_usd,
            votium_votes=votium_total_bias,
            votemarket_votes=votemarket_bias,
            gauge_data=gauge_data,
            transaction_hash=txn_hash,
            block_number=block,
            timestamp=timestamp,
            date_str=date_str,
            period_start=period_start,
            log_index=log_index
        )

        with engine.begin() as conn:
            conn.execute(ins)

    except IntegrityError as e:
        # Duplicate entry - already processed, skip alert
        logger.warning(f"Duplicate incentive transfer skipped (txn: {txn_hash}): {str(e)}")
        return
    except SQLAlchemyError as e:
        logger.error(f"Database error in handle_incentive_transfer: {str(e)}")
        raise
    except Exception as e:
        logger.error(f"Unexpected error in handle_incentive_transfer: {str(e)}")
        raise

    # Only send alert AFTER successful database commit
    send_telegram_alert(
        total=total,
        votium_amt=votium_amt,
        votemarket_amt=votemarket_amt,
        votium_votes=votium_total_bias,
        votemarket_votes=votemarket_bias,
        date_str=date_str,
        txn_hash=txn_hash,
        gauge_data=gauge_data
    )

    logger.info(f"Processed incentive transfer")
    logger.info(f"Total YB: {total:,.2f}")
    votium_pct = (votium_amt / total * 100) if total else 0
    votemarket_pct = (votemarket_amt / total * 100) if total else 0
    logger.info(f"Votium YB: {votium_amt:,.2f} ({votium_pct:.2f}%)")
    logger.info(f"Votemarket YB: {votemarket_amt:,.2f} ({votemarket_pct:.2f}%)")

def main():
    print("DEBUG: YB incentives main() function called")
    logger.info('Starting YB incentives fetcher')
    print("DEBUG: YB incentives logger initialized, entering main loop")
    while True:
        try:
            missing_periods = get_missing_periods()
            if missing_periods:
                logger.info(f"[YB] Found {len(missing_periods)} periods to process")
                for period in missing_periods:
                    process_period(period)
            else:
                current_period, next_period = get_periods()
                logger.info(f"No missing periods. Next period starts at {next_period} ({datetime.fromtimestamp(next_period, timezone.utc).strftime('%Y-%m-%d %H:%M UTC')})")
            
        except Exception as e:
            logger.error(f"Error in main loop: {str(e)}")
            # Don't re-raise, just log and continue
            time.sleep(60)  # Wait a bit before retrying
        
        time.sleep(POLL_INTERVAL)

if __name__ == '__main__':
    main()
