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

ROOT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), os.pardir))
if ROOT_DIR not in sys.path:
    sys.path.append(ROOT_DIR)

from constants import CHAT_IDS, RESUPPLY_GAUGES
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
PROTOCOL = 'resupply'

# Initialize Telegram bot
bot = telebot.TeleBot(TELEGRAM_BOT_TOKEN)

# Contract addresses
CONVEX_DEPLOYER = '0x947B7742C403f20e5FaCcDAc5E092C943E7D0277'
RSUP = '0x419905009e4656fdC02418C7Df35B1E61Ed5F726'
EC = '0x33333333df05b0D52edD13D230461E5A0f5a4706'
MULTISIG = '0xFE11a5009f2121622271e7dd0FD470264e076af6'
GAUGE_CONTROLLER = '0x2F50D538606Fa9EDD2B11E2446BEb18C9D5846bB'
CURVE_VOTERS = {
    'CONVEX': '0x989AEb4d175e16225E39E87d0D97A3360524AD80',
    'PRISMA': '0x2F50D538606Fa9EDD2B11E2446BEb18C9D5846bB'
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
rsup_abi = utils.load_abi('./abis/erc20.json')
ec_abi = utils.load_abi('./abis/emissions_controller.json')
gauge_controller_abi = utils.load_abi('./abis/gauge_controller.json')

# Initialize contracts
rsup = w3.eth.contract(address=RSUP, abi=rsup_abi)
ec = w3.eth.contract(address=EC, abi=ec_abi)
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

        # Get Transfer events from EC to multisig for this period
        logger.info(f"[RSUP] Fetching Transfer events from {EC} to {MULTISIG}")
        logs = rsup.events.Transfer.get_logs(
            argument_filters={'from': EC, 'to': MULTISIG},
            fromBlock=start_block,
            toBlock=min(end_block, w3.eth.block_number)
        )

        logger.info(f"[RSUP] Found {len(logs)} Transfer events for period {period_start}")

        for log in logs:
            handle_incentive_transfer(log)

        logger.info(f"[RSUP] Completed processing period {period_start}")
        
    except Exception as e:
        logger.error(f"Error processing period {period_start}: {str(e)}")
        # Don't re-raise the error, just log it and continue
        return

def calculate_efficiency(block_number: int, period_ts: int, total_incentives: float, votium_amount: float) -> tuple:
    """Calculate efficiency metrics for a given block"""
    try:        
        # Calculate incentives
        votium_incentives = votium_amount / 2  # Divide by 2 because each campaign is 2 weeks
        votemarket_incentives = (total_incentives - votium_amount) / 2
        
        rsup_price = get_token_price(RSUP)
        price_available = rsup_price is not None and rsup_price > 0
        if not price_available:
            logger.warning("Unable to fetch RSUP price; efficiency metrics will be null")
        
        # Initialize bias counters
        gauge_data = {}
        convex_total_bias = 0
        prisma_total_bias = 0
        total_bias = 0
        
        # Calculate biases for each gauge
        for gauge in RESUPPLY_GAUGES:
            try:
                convex_slope = gauge_controller.functions.vote_user_slopes(
                    CURVE_VOTERS['CONVEX'], 
                    gauge
                ).call(block_identifier=block_number)
                
                prisma_slope = gauge_controller.functions.vote_user_slopes(
                    CURVE_VOTERS['PRISMA'], 
                    gauge
                ).call(block_identifier=block_number)
                
                convex_bias = get_bias(convex_slope[0], convex_slope[2], period_ts) / 1e18
                prisma_bias = get_bias(prisma_slope[0], prisma_slope[2], period_ts) / 1e18
                total_gauge_bias = gauge_controller.functions.points_weight(
                    gauge, 
                    period_ts
                ).call(block_identifier=block_number)[0] / 1e18
                
                # Get relative weight for this gauge
                relative_weight = gauge_controller.functions.gauge_relative_weight(
                    gauge,
                    period_ts
                ).call(block_identifier=block_number) / 1e18
                
                convex_total_bias += convex_bias
                prisma_total_bias += prisma_bias
                total_bias += total_gauge_bias
                
                # Store gauge data
                gauge_data[RESUPPLY_GAUGES[gauge]] = {
                    'votium_bias': convex_bias,
                    'prisma_bias': prisma_bias,
                    'total_bias': total_gauge_bias,
                    'relative_weight': relative_weight
                }
                
            except Exception as e:
                logger.warning(f"Failed to get gauge data for {RESUPPLY_GAUGES[gauge]}: {str(e)}")
                continue
        
        votemarket_bias = total_bias - convex_total_bias - prisma_total_bias
        
        # Calculate efficiency metrics - votes per USD
        votium_votes_per_usd = None
        votemarket_votes_per_usd = None
        if (price_available and convex_total_bias > 0 and
                votium_incentives > 0 and votium_incentives * rsup_price > 0):
            votium_votes_per_usd = convex_total_bias / (votium_incentives * rsup_price)
            
        if (price_available and votemarket_bias > 0 and
                votemarket_incentives > 0 and votemarket_incentives * rsup_price > 0):
            votemarket_votes_per_usd = votemarket_bias / (votemarket_incentives * rsup_price)
        
        logger.info(f'Votium total bias: {convex_total_bias:,.2f}')
        logger.info(f'Prisma total bias: {prisma_total_bias:,.2f}')
        logger.info(f'Votemarket total bias: {votemarket_bias:,.2f}')
        logger.info(
            'Votium votes per USD: %s',
            f"{votium_votes_per_usd:,.2f}" if votium_votes_per_usd is not None else "n/a"
        )
        logger.info(
            'Votemarket votes per USD: %s',
            f"{votemarket_votes_per_usd:,.2f}" if votemarket_votes_per_usd is not None else "n/a"
        )
        
        return votium_votes_per_usd, votemarket_votes_per_usd, convex_total_bias, votemarket_bias, gauge_data
        
    except Exception as e:
        logger.error(f"Error calculating efficiency: {str(e)}")
        return 0, 0, 0, 0, {}

def send_telegram_alert(epoch: int, total: float, votium_amt: float, votemarket_amt: float, 
                       votium_votes: float, votemarket_votes: float, 
                       date_str: str, txn_hash: str, gauge_data: dict):
    """Send Telegram alert for incentive transfer"""
    try:
        # Convert date_str to MM/DD/YY format
        date_obj = datetime.strptime(date_str, '%Y-%m-%d %H:%M UTC')
        mmddyy = (date_obj + timedelta(days=7)).strftime('%m/%d/%y')
        
        msg = f"🎯 *RSUP Incentives Report*\n\n"
        msg += f"Epoch {epoch} distributions | Effective {mmddyy}\n\n"
        
        # votium_votes and votemarket_votes are already the correct total vote counts (bias)
        # Calculate votes per RSUP token
        msg += f"*Votium*: \n"
        votes_per_rsup = votium_votes / votium_amt if votium_amt > 0 else 0
        msg += f"- {votes_per_rsup:,.0f} votes/RSUP\n"
        msg += f"- {votium_votes:,.0f} votes for {votium_amt:,.0f} RSUP\n\n"
        
        msg += f"*Votemarket*: \n"
        votes_per_rsup = votemarket_votes / votemarket_amt if votemarket_amt > 0 else 0
        msg += f"- {votes_per_rsup:,.0f} votes/RSUP\n"
        msg += f"- {votemarket_votes:,.0f} votes for {votemarket_amt:,.0f} RSUP\n\n"
        
        # Add gauge-specific data
        msg += f"━━━━━━━━━━\n"
        for gauge_name, data in gauge_data.items():
            # Find the gauge address for this gauge name
            gauge_address = next((addr for addr, name in RESUPPLY_GAUGES.items() if name == gauge_name), None)
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
    
    epoch = ec.functions.getEpoch().call(block_identifier=block)
    total = event['args']['value'] / 1e18
    receipt = w3.eth.get_transaction_receipt(txn_hash)
    votium_amt = 0
    # Transfer event signature: keccak256("Transfer(address,address,uint256)")
    transfer_topic = '0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef'
    
    for log in receipt['logs']:
        if log['address'].lower() == RSUP.lower() and len(log['topics']) > 0 and log['topics'][0].hex() == transfer_topic:
            try:
                transfer_event = rsup.events.Transfer().process_log(log)
                # RSUP token might use 'sender' and 'receiver' parameter names instead of 'from' and 'to'
                from_addr = transfer_event['args'].get('sender') or transfer_event['args'].get('from')
                to_addr = transfer_event['args'].get('receiver') or transfer_event['args'].get('to')
                value = transfer_event['args']['value'] / 1e18
                
                if (from_addr.lower() == MULTISIG.lower() and 
                    to_addr.lower() == CONVEX_DEPLOYER.lower()):
                    votium_amt += value
            except Exception:
                # Not a Transfer event or different event signature, skip it
                continue
    
    votemarket_amt = total - votium_amt
    
    # Calc period start and end
    period_start = int(timestamp / WEEK) * WEEK
    next_period_start = period_start + WEEK
    next_period_block = closest_block_after_timestamp(w3, next_period_start)
    date_str = datetime.fromtimestamp(period_start, timezone.utc).strftime('%Y-%m-%d %H:%M UTC')

    votium_votes_per_usd, votemarket_votes_per_usd, votium_total_bias, votemarket_bias, gauge_data = calculate_efficiency(next_period_block, next_period_start, total, votium_amt)
    
    # First, try to insert into database - this must succeed before sending alert
    try:
        ins = incentives_table.insert().values(
            protocol=PROTOCOL,
            epoch=epoch,
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
    
    # Only send alert AFTER successful database commit
    send_telegram_alert(
        epoch=epoch,
        total=total,
        votium_amt=votium_amt,
        votemarket_amt=votemarket_amt,
        votium_votes=votium_total_bias,
        votemarket_votes=votemarket_bias,
        date_str=date_str,
        txn_hash=txn_hash,
        gauge_data=gauge_data
    )
    
    logger.info(f"Processed incentive transfer for epoch {epoch}")
    logger.info(f"Total RSUP: {total:,.2f}")
    votium_pct = (votium_amt / total * 100) if total else 0
    votemarket_pct = (votemarket_amt / total * 100) if total else 0
    logger.info(f"Votium RSUP: {votium_amt:,.2f} ({votium_pct:.2f}%)")
    logger.info(f"Votemarket RSUP: {votemarket_amt:,.2f} ({votemarket_pct:.2f}%)")

def main():
    logger.info('Starting rsup incentives fetcher')
    while True:
        try:
            missing_periods = get_missing_periods()
            if missing_periods:
                logger.info(f"[RSUP] Found {len(missing_periods)} periods to process")
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
