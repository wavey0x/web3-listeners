from web3 import Web3
from sqlalchemy import create_engine, MetaData, select
from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import SQLAlchemyError, IntegrityError
import time
from datetime import datetime, timezone, timedelta
import sys
import os
import requests
from dotenv import load_dotenv
import logging
import telebot

# Add the parent directory of the current file to sys.path
parent_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
sys.path.append(parent_dir)
from constants import CHAT_IDS, RESUPPLY_GAUGES
import utils
from schemas.rsup_incentives import create_tables
from utils.web3_utils import closest_block_before_timestamp, closest_block_after_timestamp

load_dotenv()

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
WEEK = 7 * 24 * 60 * 60  # 7 days in seconds
POLL_INTERVAL = 60 * 60  # Check every hour

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

def get_periods():
    """Get current and next period timestamps"""
    current_time = int(time.time())
    current_period = int(current_time / WEEK) * WEEK
    next_period = current_period + WEEK
    return current_period, next_period

def get_last_processed_period():
    """Get the last period we've processed from the database"""
    try:
        with engine.connect() as conn:
            query = select(incentives_table.c.period_start)
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
        # Start from March 19, 2025 timestamp
        start_period = int(1742428800 / WEEK) * WEEK
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

def get_token_price(token_address):
    """Get token price from DeFiLlama API"""
    try:
        response = requests.get(f"https://coins.llama.fi/prices/current/ethereum:{token_address}")
        if response.status_code == 200:
            data = response.json()
            return data['coins'][f'ethereum:{token_address}']['price']
        return None
    except Exception as e:
        logger.error(f"Error fetching token price: {str(e)}")
        return None

def get_bias(slope: int, end: int, current_period: int) -> int:
    """Calculate bias from slope and end time"""
    if end <= current_period:
        return 0
    return slope * (end - current_period)

def calculate_efficiency(block_number: int, period_ts: int, total_incentives: float, convex_amount: float) -> tuple:
    """Calculate efficiency metrics for a given block"""
    try:        
        # Calculate incentives
        votium_incentives = convex_amount / 2  # Divide by 2 because each campaign is 2 weeks
        vecrv_incentives = (total_incentives - convex_amount) / 2
        
        rsup_price = get_token_price(RSUP)
        
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
                    'convex_bias': convex_bias,
                    'prisma_bias': prisma_bias,
                    'total_bias': total_gauge_bias,
                    'relative_weight': relative_weight
                }
                
            except Exception as e:
                logger.warning(f"Failed to get gauge data for {RESUPPLY_GAUGES[gauge]}: {str(e)}")
                continue
        
        vecrv_bias = total_bias - convex_total_bias - prisma_total_bias
        
        # Calculate efficiency metrics - votes per USD
        if convex_total_bias > 0 and votium_incentives * rsup_price > 0:
            votium_votes_per_usd = convex_total_bias / (votium_incentives * rsup_price)
        else:
            votium_votes_per_usd = 0
            
        if vecrv_bias > 0 and vecrv_incentives * rsup_price > 0:
            vecrv_votes_per_usd = vecrv_bias / (vecrv_incentives * rsup_price)
        else:
            vecrv_votes_per_usd = 0
        
        logger.info(f'Convex total bias: {convex_total_bias:,.2f}')
        logger.info(f'Prisma total bias: {prisma_total_bias:,.2f}')
        logger.info(f'veCRV total bias: {vecrv_bias:,.2f}')
        logger.info(f'Votium votes per USD: {votium_votes_per_usd:,.2f}')
        logger.info(f'veCRV votes per USD: {vecrv_votes_per_usd:,.2f}')
        
        return votium_votes_per_usd, vecrv_votes_per_usd, convex_total_bias, vecrv_bias, gauge_data
        
    except Exception as e:
        logger.error(f"Error calculating efficiency: {str(e)}")
        return 0, 0, 0, 0, {}

def send_telegram_alert(epoch: int, total: float, convex_amt: float, yearn_amt: float, 
                       convex_votes_per_usd: float, yearn_votes_per_usd: float, 
                       date_str: str, txn_hash: str, gauge_data: dict):
    """Send Telegram alert for incentive transfer"""
    try:
        # Convert date_str to MM/DD/YY format
        date_obj = datetime.strptime(date_str, '%Y-%m-%d %H:%M UTC')
        mmddyy = (date_obj + timedelta(days=7)).strftime('%m/%d/%y')
        
        msg = f"🎯 *RSUP Incentives Report*\n\n"
        msg += f"Epoch {epoch} distributions | Effective {mmddyy}\n\n"
        
        # Calculate vote-to-spend ratios
        convex_votes = convex_votes_per_usd * convex_amt
        yearn_votes = yearn_votes_per_usd * yearn_amt
        
        msg += f"*Votium*: \n"
        votes_per_rsup = convex_votes / convex_amt
        msg += f"- {votes_per_rsup:,.0f} votes/RSUP\n"
        msg += f"- {convex_votes:,.0f} votes for {convex_amt:,.0f} RSUP\n\n"
        
        msg += f"*Votemarket*: \n"
        votes_per_rsup = yearn_votes / yearn_amt
        msg += f"- {votes_per_rsup:,.0f} votes/RSUP\n"
        msg += f"- {yearn_votes:,.0f} votes for {yearn_amt:,.0f} RSUP\n\n"
        
        # Add gauge-specific data
        msg += f"━━━━━━━━━━\n"
        for gauge_name, data in gauge_data.items():
            # Find the gauge address for this gauge name
            gauge_address = next((addr for addr, name in RESUPPLY_GAUGES.items() if name == gauge_name), None)
            if gauge_address:
                msg += f"\n[{gauge_name}](https://crv.lol/?gauge={gauge_address})\n"
                msg += f"- Votes: {data['total_bias']:,.0f} ({data['relative_weight']*100:.2f}%)\n"
        
        msg += f"\n🔗 [Distro txn](https://etherscan.io/tx/{txn_hash})"
        
        # Send to all configured chat IDs
        # chat_key = 'WAVEY_ALERTS'
        chat_key = 'RESUPPLY_ALERTS'
        bot.send_message(CHAT_IDS[chat_key], msg, parse_mode="markdown", disable_web_page_preview=True)
        
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
    convex_amt = 0
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
                    convex_amt += value
            except Exception:
                # Not a Transfer event or different event signature, skip it
                continue
    
    yearn_amt = total - convex_amt
    
    # Calc period start and end
    period_start = int(timestamp / WEEK) * WEEK
    next_period_start = period_start + WEEK
    next_period_block = closest_block_after_timestamp(w3, next_period_start)
    date_str = datetime.fromtimestamp(period_start, timezone.utc).strftime('%Y-%m-%d %H:%M UTC')

    convex_votes_per_usd, yearn_votes_per_usd, convex_total_bias, vecrv_bias, gauge_data = calculate_efficiency(next_period_block, next_period_start, total, convex_amt)
    
    # First, try to insert into database - this must succeed before sending alert
    try:
        ins = incentives_table.insert().values(
            epoch=epoch,
            total_incentives=total,
            convex_amount=convex_amt,
            yearn_amount=yearn_amt,
            convex_votes_per_usd=convex_votes_per_usd,
            yearn_votes_per_usd=yearn_votes_per_usd,
            convex_votes=convex_total_bias,
            yearn_votes=vecrv_bias,
            gauge_data=gauge_data,
            transaction_hash=txn_hash,
            block_number=block,
            timestamp=timestamp,
            date_str=date_str,
            period_start=period_start,
            log_index=log_index
        )
        
        conn = engine.connect()
        conn.execute(ins)
        conn.commit()
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
        convex_amt=convex_amt,
        yearn_amt=yearn_amt,
        convex_votes_per_usd=convex_votes_per_usd,
        yearn_votes_per_usd=yearn_votes_per_usd,
        date_str=date_str,
        txn_hash=txn_hash,
        gauge_data=gauge_data
    )
    
    logger.info(f"Processed incentive transfer for epoch {epoch}")
    logger.info(f"Total RSUP: {total:,.2f}")
    logger.info(f"Convex RSUP: {convex_amt:,.2f} ({convex_amt / total * 100:.2f}%)")
    logger.info(f"Yearn RSUP: {yearn_amt:,.2f} ({yearn_amt / total * 100:.2f}%)")

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