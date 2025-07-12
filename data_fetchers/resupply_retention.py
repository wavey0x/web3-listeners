from web3 import Web3
from sqlalchemy import create_engine, MetaData, select
from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import SQLAlchemyError, IntegrityError
import time
from datetime import datetime, timezone
import sys
import os
import telebot
from telebot.apihelper import ApiException
from dotenv import load_dotenv
import logging
import json

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

# Add the parent directory of the current file to sys.path
parent_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
sys.path.append(parent_dir)
import utils
from constants import CHAT_IDS
from schemas.weight_tracker import create_tables

load_dotenv()

# Constants
WEB3_PROVIDER_URI = os.getenv('WEB3_PROVIDER_URI')
DATABASE_URI = os.getenv('DATABASE_URI')
TELEGRAM_BOT_KEY = os.getenv('WAVEY_ALERTS_BOT_KEY')
POLL_INTERVAL = 10  # seconds
MAX_WIDTH = 400_000  # max blocks to scan per iteration
MAX_TELEGRAM_RETRIES = 5  # Maximum number of retries for Telegram API
INITIAL_RETRY_DELAY = 1  # Initial retry delay in seconds
CONTRACT_ADDRESS = '0xB9415639618e70aBb71A0F4F8bbB2643Bf337892'
DEPLOYMENT_BLOCK = 22870945

# Connect to Ethereum network
w3 = Web3(Web3.HTTPProvider(WEB3_PROVIDER_URI))
if not w3.is_connected():
    raise Exception("Failed to connect to Ethereum node")

# Set up PostgreSQL connection
engine = create_engine(DATABASE_URI)
Session = sessionmaker(bind=engine)
session = Session()
metadata = MetaData()

# Create tables
weight_changes_table = create_tables(metadata)
metadata.create_all(engine)

# Initialize telegram bot
bot = telebot.TeleBot(TELEGRAM_BOT_KEY)

# Load ABI - we'll need to create a minimal ABI for the WeightSet event
weight_tracker_abi = json.load(open('abis/retention.json'))

# Initialize contract
contract = w3.eth.contract(address=CONTRACT_ADDRESS, abi=weight_tracker_abi)

# Get original total supply (1 block after deployment)
def get_original_total_supply():
    """Get the total supply 1 block after contract deployment"""
    try:
        # Call totalSupply at deployment block + 1
        original_supply = contract.functions.totalSupply().call(block_identifier=DEPLOYMENT_BLOCK + 1)
        original_supply_eth = original_supply / 10**18
        logger.info(f"Original total supply: {original_supply_eth:,.2f}")
        return original_supply_eth
    except Exception as e:
        logger.error(f"Error getting original total supply: {str(e)}")
        return None

# Get original total supply
original_total_supply = get_original_total_supply()

def format_address(address):
    """Format an address as 0x123...456 with an Etherscan link."""
    return f"[0x{address[2:5]}...{address[-4:]}](https://etherscan.io/address/{address})"

def get_last_block_written():
    try:
        with engine.connect() as conn:
            # Get highest block from weight_changes table
            query = select(weight_changes_table.c.block).order_by(weight_changes_table.c.block.desc()).limit(1)
            result = conn.execute(query).scalar()
            
            # If no records exist, start from deployment block
            if result is None:
                return DEPLOYMENT_BLOCK
            
            return result + 1
    except SQLAlchemyError as e:
        logger.error(f"Database error in get_last_block_written: {str(e)}")
        raise  # Re-raise to prevent silent failures

def send_alert(chat_id, msg):
    """Send a Telegram alert with retry logic for rate limiting."""
    retry_count = 0
    retry_delay = INITIAL_RETRY_DELAY
    
    while retry_count < MAX_TELEGRAM_RETRIES:
        try:
            bot.send_message(chat_id, msg, parse_mode="markdown", disable_web_page_preview=True)
            logger.info(f"Successfully sent Telegram message to {chat_id}\n{msg}")
            return  # Success, exit the function
        except ApiException as e:
            if e.error_code == 429:  # Rate limit error
                retry_after = int(e.description.split('retry after ')[-1])
                logger.warning(f"Telegram rate limit hit. Waiting {retry_after} seconds...")
                time.sleep(retry_after)
                retry_count += 1
            else:
                logger.error(f"Telegram API error: {str(e)}")
                time.sleep(retry_delay)
                retry_delay *= 2  # Exponential backoff
                retry_count += 1
        except Exception as e:
            logger.error(f"Unexpected error sending Telegram message: {str(e)}")
            time.sleep(retry_delay)
            retry_delay *= 2  # Exponential backoff
            retry_count += 1
    
    if retry_count >= MAX_TELEGRAM_RETRIES:
        logger.error(f"Failed to send Telegram message after {MAX_TELEGRAM_RETRIES} retries")
        logger.error(f"Message was: {msg}")

def handle_weight_set(event):
    """Handle WeightSet event"""
    block = event.blockNumber
    timestamp = w3.eth.get_block(block).timestamp
    date_str = datetime.fromtimestamp(timestamp, timezone.utc).strftime('%Y-%m-%d %H:%M UTC')
    txn_hash = event.transactionHash.hex()
    
    user_address = event['args']['user']
    old_weight = event['args']['oldWeight']
    new_weight = event['args']['newWeight']
    weight_diff = new_weight - old_weight
    
    # Convert from wei to ether (divide by 1e18)
    old_weight_eth = old_weight / 10**18
    new_weight_eth = new_weight / 10**18
    weight_diff_eth = weight_diff / 10**18
    
    try:
        # Insert weight change record
        ins = weight_changes_table.insert().values(
            user_address=user_address,
            old_weight=old_weight,
            new_weight=new_weight,
            weight_diff=weight_diff,
            block=block,
            txn_hash=txn_hash,
            timestamp=timestamp,
            date_str=date_str
        )
        conn = engine.connect()
        conn.execute(ins)
        conn.commit()
        
        # Get current total supply at this block
        try:
            current_total_supply = contract.functions.totalSupply().call(block_identifier=block)
            current_total_supply_eth = current_total_supply / 10**18
        except Exception as e:
            logger.error(f"Error getting current total supply: {str(e)}")
            current_total_supply_eth = None
        
        # Calculate percentages if we have original total supply
        if original_total_supply and current_total_supply_eth:
            shares_remaining_pct = (current_total_supply_eth / original_total_supply) * 100
            shares_withdrawn_pct = ((original_total_supply - current_total_supply_eth) / original_total_supply) * 100
        else:
            shares_remaining_pct = None
            shares_withdrawn_pct = None
        
        # Format the weight values for display (in ETH)
        weight_diff_formatted = f"{abs(weight_diff_eth):,.0f}"
        new_weight_formatted = f"{new_weight_eth:,.0f}"
        
        # Only send alert if not from deployment block
        if block != DEPLOYMENT_BLOCK:
            # Send alert
            msg = f"🔁 *Retention Shares Checkpointed*\n\n"
            msg += f"User: {format_address(user_address)}\n"
            msg += f"Burned: {weight_diff_formatted}\n"
            msg += f"Remaining: {new_weight_formatted}\n"
            
            # Add total supply info with percentages
            if current_total_supply_eth is not None:
                msg += f"\nTotal Remaining: {current_total_supply_eth:,.0f}"
                if shares_remaining_pct is not None:
                    msg += f" ({shares_remaining_pct:.1f}%)\n"
                else:
                    msg += "\n"
                
                if shares_withdrawn_pct is not None:
                    msg += f"Total Withdrawn: {original_total_supply - current_total_supply_eth:,.0f} ({shares_withdrawn_pct:.1f}%)\n"
            else:
                msg += f"Total Remaining: Unable to fetch\n"
            
            msg += f"\n🔗 [View on Etherscan](https://etherscan.io/tx/{txn_hash})"
            
            send_alert(CHAT_IDS['RESUPPLY_ALERTS'], msg)
        
    except IntegrityError as e:
        logger.error(f"Integrity error in handle_weight_set: {str(e)}")
        raise
    except SQLAlchemyError as e:
        logger.error(f"Database error in handle_weight_set: {str(e)}")
        raise
    except Exception as e:
        logger.error(f"Unexpected error in handle_weight_set: {str(e)}")
        raise

def fetch_logs(contract, event_name, from_block, to_block):
    try:
        event = getattr(contract.events, event_name)
        
        logs = event.get_logs(
            fromBlock=from_block,
            toBlock=to_block
        )
        return logs
    except Exception as e:
        logger.error(f"Error fetching logs for {event_name}: {str(e)}")
        raise

def main():
    logger.info(f"Starting weight tracker for contract {CONTRACT_ADDRESS}")
    logger.info(f"Monitoring from block {DEPLOYMENT_BLOCK}")
    
    i = 0
    while True:
        try:
            i += 1            
            height = w3.eth.get_block_number()
            last_block_written = get_last_block_written()
            to_block = min(last_block_written + MAX_WIDTH, height)
            
            if i % 1000 == 0:
                logger.info(f"Loops since startup: {i}")
                logger.info(f'Listening from block {last_block_written} --> {to_block}')
            
            # Process WeightSet events
            try:
                logs = fetch_logs(contract, 'WeightSet', last_block_written, to_block)
                for log in logs:
                    handle_weight_set(log)
                    
            except Exception as e:
                logger.error(f"Error processing WeightSet events: {str(e)}")
            
        except Exception as e:
            logger.error(f"Error in main loop: {str(e)}")
        
        time.sleep(POLL_INTERVAL)

if __name__ == '__main__':
    main() 