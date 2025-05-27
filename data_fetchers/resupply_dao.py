from web3 import Web3
from sqlalchemy import create_engine, MetaData, select, and_
from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import SQLAlchemyError, IntegrityError
import time
from datetime import datetime, UTC
import sys
import os
import telebot
from telebot.apihelper import ApiException
from dotenv import load_dotenv
import logging

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
from schemas.resupply_dao import create_tables, ProposalStatus

load_dotenv()

# Constants
WEB3_PROVIDER_URI = os.getenv('WEB3_PROVIDER_URI')
DATABASE_URI = os.getenv('DATABASE_URI')
TELEGRAM_BOT_KEY = os.getenv('WAVEY_ALERTS_BOT_KEY')
POLL_INTERVAL = 10  # seconds
MAX_WIDTH = 400_000  # max blocks to scan per iteration
EXECUTION_DELAY = 24 * 60 * 60  # 24 hours in seconds
EXECUTION_DEADLINE = 21 * 24 * 60 * 60  # 3 weeks in seconds
MAX_TELEGRAM_RETRIES = 5  # Maximum number of retries for Telegram API
INITIAL_RETRY_DELAY = 1  # Initial retry delay in seconds
VOTING_PERIOD = 60 * 60 * 24 * 7  # 1 week
PERMASTAKERS = [
    '0x12341234B35c8a48908c716266db79CAeA0100E8',
    '0xCCCCCccc94bFeCDd365b4Ee6B86108fC91848901',
]
# Known voter addresses
VOTER_ADDRESSES = [
    '0x11111111084a560ea5755Ed904a57e5411888C28',
    '0x11111111408bd67B92C4f74B9D3cF96f1fa412BC'
]

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
proposals_table, votes_table = create_tables(metadata)
metadata.create_all(engine)

# Initialize telegram bot
bot = telebot.TeleBot(TELEGRAM_BOT_KEY)

# Load ABI
voter_abi = utils.load_abi('./abis/resupply_voter.json')

def format_address(address):
    """Format an address as 0x123...456 with an Etherscan link."""
    return f"[0x{address[2:5]}...{address[-4:]}](https://etherscan.io/address/{address})"

def get_last_block_written():
    try:
        with engine.connect() as conn:
            # Get highest block from proposals table
            proposals_query = select(proposals_table.c.block)
            proposals_query = proposals_query.order_by(proposals_table.c.block.desc()).limit(1)
            proposals_block = conn.execute(proposals_query).scalar()
            
            # Get highest block from votes table
            votes_query = select(votes_table.c.block)
            votes_query = votes_query.order_by(votes_query.c.block.desc()).limit(1)
            votes_block = conn.execute(votes_query).scalar()
            
            # Get the highest block between both tables
            highest_block = max(
                block for block in [proposals_block, votes_block] 
                if block is not None
            ) if any(block is not None for block in [proposals_block, votes_block]) else 22_200_000
            
            return highest_block + 1
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

def handle_proposal_created(event, voter_address):
    block = event.blockNumber
    timestamp = w3.eth.get_block(block).timestamp
    date_str = datetime.fromtimestamp(timestamp, UTC).strftime('%Y-%m-%d %H:%M UTC')
    txn_hash = event.transactionHash.hex()
    
    proposal_id = int(event['args']['id'])
    proposer = event['args']['account']
    start_time = timestamp
    end_time = timestamp + VOTING_PERIOD
    
    try:
        description = get_proposal_description(proposal_id, voter_address)
        
        ins = proposals_table.insert().values(
            proposal_id=proposal_id,
            voter_address=voter_address,
            proposer=proposer,
            description=description,
            start_time=start_time,
            end_time=end_time,
            status=ProposalStatus.OPEN.value,
            yes_votes=0,
            no_votes=0,
            quorum=event['args']['quorumWeight'],
            block=block,
            txn_hash=txn_hash,
            timestamp=timestamp,
            date_str=date_str,
            last_updated=block
        )
        conn = engine.connect()
        conn.execute(ins)
        conn.commit()
        
        msg = f"📜 *New Resupply Proposal Created*\n\n"
        msg += f"Proposal {proposal_id}: {description}\n\n"
        msg += f"Proposer: {format_address(proposer)}\n"
        msg += f"Epoch: {event['args']['epoch']}\n"
        msg += f"Quorum Required: {event['args']['quorumWeight']:,}\n"
        msg += f"Ends: {datetime.fromtimestamp(end_time, UTC).strftime('%Y-%m-%d %H:%M UTC')}\n"
        msg += f"\n🔗 [Etherscan](https://etherscan.io/tx/{txn_hash}) | [Resupply](https://resupply.fi/governance/proposals) | [Hippo Army](https://hippo.army/dao/proposal/{proposal_id})"
        send_alert(CHAT_IDS['RESUPPLY_ALERTS'], msg)
        
    except IntegrityError as e:
        logger.error(f"Integrity error in handle_proposal_created: {str(e)}")
        raise
    except SQLAlchemyError as e:
        logger.error(f"Database error in handle_proposal_created: {str(e)}")
        raise
    except Exception as e:
        logger.error(f"Unexpected error in handle_proposal_created: {str(e)}")
        raise

def get_proposal_description(proposal_id, voter_address):
    voter_contract = w3.eth.contract(address=voter_address, abi=voter_abi)
    description = voter_contract.functions.proposalDescription(int(proposal_id)).call()
    return description

def handle_vote_cast(event, voter_address):
    block = event.blockNumber
    timestamp = w3.eth.get_block(block).timestamp
    date_str = datetime.fromtimestamp(timestamp, UTC).strftime('%Y-%m-%d %H:%M UTC')
    txn_hash = event.transactionHash.hex()
    
    proposal_id = str(event['args']['id'])
    voter = event['args']['account']
    weight_yes = event['args']['weightYes']
    weight_no = event['args']['weightNo']
    description = ""
    
    try:
        description = get_proposal_description(proposal_id, voter_address)
        
        with engine.connect() as conn:
            # Insert vote
            ins = votes_table.insert().values(
                proposal_id=proposal_id,
                voter=voter,
                support=weight_yes > 0,  # If weightYes > 0, it's a yes vote
                weight=weight_yes if weight_yes > 0 else weight_no,  # Use the non-zero weight
                reason='',  # Reason not available in event
                block=block,
                txn_hash=txn_hash,
                timestamp=timestamp,
                date_str=date_str
            )
            conn.execute(ins)
            
            # Update proposal vote counts and last_updated
            update = proposals_table.update().where(
                and_(
                    proposals_table.c.proposal_id == proposal_id,
                    proposals_table.c.voter_address == voter_address
                )
            ).values(
                yes_votes=proposals_table.c.yes_votes + weight_yes,
                no_votes=proposals_table.c.no_votes + weight_no,
                last_updated=block
            )
            
            result = conn.execute(update)
            if result.rowcount == 0:
                logger.warning(f"No proposal found to update for proposal_id {proposal_id} and voter {voter_address}")
            
            conn.commit()
            
            # Send alert
            msg = f"🗳️ *New Vote Cast on Resupply Proposal*\n\n"
            msg += f"Proposal {proposal_id}: {description}\n"
            msg += f"Voter: {format_address(voter)}\n"
            if weight_yes > 0:
                msg += f"Vote: Yes\n"
                msg += f"Weight: {weight_yes:,.0f}\n"
            else:
                msg += f"Vote: No\n"
                msg += f"Weight: {weight_no:,.0f}\n"
            msg += f"\n🔗 [Etherscan](https://etherscan.io/tx/{txn_hash}) | [Resupply](https://resupply.fi/governance/proposals) | [Hippo Army](https://hippo.army/dao/proposal/{proposal_id})"
            if voter in PERMASTAKERS:
                send_alert(CHAT_IDS['RESUPPLY_ALERTS'], msg)
            
    except IntegrityError as e:
        logger.error(f"Integrity error in handle_vote_cast: {str(e)}")
        raise
    except SQLAlchemyError as e:
        logger.error(f"Database error in handle_vote_cast: {str(e)}")
        raise
    except Exception as e:
        logger.error(f"Unexpected error in handle_vote_cast: {str(e)}")
        raise

def handle_proposal_cancelled(event, voter_address):
    block = event.blockNumber
    timestamp = w3.eth.get_block(block).timestamp
    date_str = datetime.fromtimestamp(timestamp, UTC).strftime('%Y-%m-%d %H:%M UTC')
    txn_hash = event.transactionHash.hex()
    
    proposal_id = str(event['args']['proposalId'])
    description = ""
    
    try:
        description = get_proposal_description(proposal_id, voter_address)
        update = proposals_table.update().where(
            and_(
                proposals_table.c.proposal_id == proposal_id,
                proposals_table.c.voter_address == voter_address
            )
        ).values(
            status=ProposalStatus.CANCELLED.value,
            block=block,
            txn_hash=txn_hash,
            timestamp=timestamp,
            date_str=date_str,
            last_updated=block
        )
        conn = engine.connect()
        conn.execute(update)
        conn.commit()
        
        # Send alert
        msg = f"❌ *Resupply Proposal Cancelled*\n\n"
        msg += f"Proposal {proposal_id}: {description}\n"
        msg += f"\n🔗 [Etherscan](https://etherscan.io/tx/{txn_hash}) | [Resupply](https://resupply.fi/governance/proposals) | [Hippo Army](https://hippo.army/dao/proposal/{proposal_id})"
        send_alert(CHAT_IDS['RESUPPLY_ALERTS'], msg)
        
    except SQLAlchemyError as e:
        logger.error(f"Database error in handle_proposal_cancelled: {str(e)}")
        raise
    except Exception as e:
        logger.error(f"Unexpected error in handle_proposal_cancelled: {str(e)}")
        raise

def handle_proposal_executed(event, voter_address):
    block = event.blockNumber
    timestamp = w3.eth.get_block(block).timestamp
    date_str = datetime.fromtimestamp(timestamp, UTC).strftime('%Y-%m-%d %H:%M UTC')
    txn_hash = event.transactionHash.hex()
    
    proposal_id = str(event['args']['proposalId'])
    description = ""
    
    try:
        description = get_proposal_description(proposal_id, voter_address)
        
        update = proposals_table.update().where(
            and_(
                proposals_table.c.proposal_id == proposal_id,
                proposals_table.c.voter_address == voter_address
            )
        ).values(
            status=ProposalStatus.EXECUTED.value,
            execution_time=timestamp,
            block=block,
            txn_hash=txn_hash,
            timestamp=timestamp,
            date_str=date_str,
            last_updated=block
        )
        conn = engine.connect()
        conn.execute(update)
        conn.commit()
        
        # Send alert
        msg = f"✅ *Resupply Proposal Executed*\n\n"
        msg += f"Proposal {proposal_id}: {description}\n"
        msg += f"\n🔗 [Etherscan](https://etherscan.io/tx/{txn_hash}) | [Resupply](https://resupply.fi/governance/proposals) | [Hippo Army](https://hippo.army/dao/proposal/{proposal_id})"
        send_alert(CHAT_IDS['RESUPPLY_ALERTS'], msg)
        
    except SQLAlchemyError as e:
        logger.error(f"Database error in handle_proposal_executed: {str(e)}")
        raise
    except Exception as e:
        logger.error(f"Unexpected error in handle_proposal_executed: {str(e)}")
        raise

def handle_proposal_description_updated(event, voter_address):
    block = event.blockNumber
    timestamp = w3.eth.get_block(block).timestamp
    date_str = datetime.fromtimestamp(timestamp, UTC).strftime('%Y-%m-%d %H:%M UTC')
    txn_hash = event.transactionHash.hex()
    
    proposal_id = str(event['args']['proposalId'])
    description = ""
    
    try:
        description = get_proposal_description(proposal_id, voter_address)
        
        update = proposals_table.update().where(
            and_(
                proposals_table.c.proposal_id == proposal_id,
                proposals_table.c.voter_address == voter_address
            )
        ).values(
            description=description,
            block=block,
            txn_hash=txn_hash,
            timestamp=timestamp,
            date_str=date_str,
            last_updated=block
        )
        conn = engine.connect()
        conn.execute(update)
        conn.commit()
        
        # Send alert
        msg = f"📝 *Resupply Proposal Description Updated*\n\n"
        msg += f"Proposal {proposal_id}: {description}\n"
        msg += f"\n🔗 [Etherscan](https://etherscan.io/tx/{txn_hash}) | [Resupply](https://resupply.fi/governance/proposals) | [Hippo Army](https://hippo.army/dao/proposal/{proposal_id})"
        send_alert(CHAT_IDS['RESUPPLY_ALERTS'], msg)
        
    except SQLAlchemyError as e:
        logger.error("Database error occurred:", exc_info=True)
    except Exception as e:
        logger.error("An error occurred:", exc_info=True)

def check_proposal_statuses():
    current_time = int(time.time())
    
    try:
        with engine.connect() as conn:
            # Get all proposals that need status checking
            query = select(proposals_table).where(
                proposals_table.c.status.in_([
                    ProposalStatus.OPEN.value,
                    ProposalStatus.PASSED.value
                ])
            )
            proposals = conn.execute(query).fetchall()
            
            for proposal in proposals:
                # For OPEN proposals, check if they've ended
                if proposal.status == ProposalStatus.OPEN.value:
                    # Check if proposal is ending in 24 hours
                    if proposal.end_time - current_time <= 24 * 60 * 60:
                        msg = f"⚠️ *Resupply Proposal Ending Soon*\n\n"
                        msg += f"Proposal {proposal.proposal_id}: {proposal.description}\n\n"
                        msg += f"Ends: {datetime.fromtimestamp(proposal.end_time, UTC).strftime('%Y-%m-%d %H:%M UTC')}\n"
                        msg += f"Yes: {proposal.yes_votes:,.0f}\n"
                        msg += f"No: {proposal.no_votes:,.0f}\n"
                        vote_total = proposal.yes_votes + proposal.no_votes
                        quorum_pct = 100 if vote_total >= proposal.quorum else (vote_total / proposal.quorum) * 100
                        msg += f"Quorum: {quorum_pct:.2f}%\n\n"
                        msg += f"\n🔗 [Etherscan](https://etherscan.io/tx/{proposal.txn_hash}) | [Resupply](https://resupply.fi/governance/proposals) | [Hippo Army](https://hippo.army/dao/proposal/{proposal.proposal_id})"
                        send_alert(CHAT_IDS['RESUPPLY_ALERTS'], msg)
                    
                    # Check if proposal has ended
                    if current_time > proposal.end_time:
                        if proposal.yes_votes > proposal.no_votes:
                            # Proposal passed
                            update = proposals_table.update().where(
                                and_(
                                    proposals_table.c.proposal_id == proposal.proposal_id,
                                    proposals_table.c.voter_address == proposal.voter_address
                                )
                            ).values(
                                status=ProposalStatus.PASSED.value,
                                last_updated=current_time
                            )
                            result = conn.execute(update)
                            if result.rowcount == 0:
                                logger.warning(f"Failed to update status for proposal {proposal.proposal_id} with voter {proposal.voter_address}")
                            conn.commit()
                            
                            msg = f"🚀 *Resupply Proposal Passed*\n\n"
                            msg += f"Proposal {proposal.proposal_id}: {proposal.description}\n\n"
                            msg += f"Yes: {proposal.yes_votes:,.0f}\n"
                            msg += f"No: {proposal.no_votes:,.0f}\n"
                            vote_total = proposal.yes_votes + proposal.no_votes
                            quorum_pct = 100 if vote_total >= proposal.quorum else (vote_total / proposal.quorum) * 100
                            msg += f"Quorum: {quorum_pct:.2f}%\n\n"
                            msg += f"Executable in 24hrs\n"
                            msg += f"\n🔗 [Etherscan](https://etherscan.io/tx/{proposal.txn_hash}) | [Resupply](https://resupply.fi/governance/proposals) | [Hippo Army](https://hippo.army/dao/proposal/{proposal.proposal_id})"
                            send_alert(CHAT_IDS['RESUPPLY_ALERTS'], msg)
                        else:
                            # Proposal failed
                            update = proposals_table.update().where(
                                and_(
                                    proposals_table.c.proposal_id == proposal.proposal_id,
                                    proposals_table.c.voter_address == proposal.voter_address
                                )
                            ).values(
                                status=ProposalStatus.FAILED.value,
                                last_updated=current_time
                            )
                            result = conn.execute(update)
                            if result.rowcount == 0:
                                logger.warning(f"Failed to update status for proposal {proposal.proposal_id} with voter {proposal.voter_address}")
                            conn.commit()
                            
                            msg = f"❌ *Resupply Proposal Failed*\n\n"
                            msg += f"Proposal {proposal.proposal_id}: {proposal.description}\n\n"
                            msg += f"Yes: {proposal.yes_votes:,.0f}\n"
                            msg += f"No: {proposal.no_votes:,.0f}\n"
                            vote_total = proposal.yes_votes + proposal.no_votes
                            quorum_pct = 100 if vote_total >= proposal.quorum else (vote_total / proposal.quorum) * 100
                            msg += f"Quorum: {quorum_pct:.2f}%\n\n"
                            msg += f"\n🔗 [Etherscan](https://etherscan.io/tx/{proposal.txn_hash}) | [Resupply](https://resupply.fi/governance/proposals) | [Hippo Army](https://hippo.army/dao/proposal/{proposal.proposal_id})"
                            send_alert(CHAT_IDS['RESUPPLY_ALERTS'], msg)
                
                # For PASSED proposals, check execution status
                elif proposal.status == ProposalStatus.PASSED.value:
                    time_since_passed = current_time - proposal.end_time
                    
                    if time_since_passed < EXECUTION_DELAY:
                        # In execution delay period
                        update = proposals_table.update().where(
                            and_(
                                proposals_table.c.proposal_id == proposal.proposal_id,
                                proposals_table.c.voter_address == proposal.voter_address
                            )
                        ).values(
                            status=ProposalStatus.EXECUTION_DELAY.value,
                            last_updated=current_time
                        )
                        conn.execute(update)
                        conn.commit()
                    elif time_since_passed < EXECUTION_DEADLINE:
                        # Ready for execution
                        update = proposals_table.update().where(
                            and_(
                                proposals_table.c.proposal_id == proposal.proposal_id,
                                proposals_table.c.voter_address == proposal.voter_address
                            )
                        ).values(
                            status=ProposalStatus.EXECUTABLE.value,
                            last_updated=current_time
                        )
                        conn.execute(update)
                        conn.commit()
                        
                        msg = f"⏰ *Resupply Proposal Ready for Execution*\n\n"
                        msg += f"Proposal {proposal.proposal_id}: {proposal.description}\n"
                        msg += f"Execution Deadline: {datetime.fromtimestamp(proposal.end_time + EXECUTION_DEADLINE, UTC).strftime('%Y-%m-%d %H:%M UTC')}\n"
                        msg += f"\n🔗 [Etherscan](https://etherscan.io/tx/{proposal.txn_hash}) | [Resupply](https://resupply.fi/governance/proposals) | [Hippo Army](https://hippo.army/dao/proposal/{proposal.proposal_id})"
                        send_alert(CHAT_IDS['RESUPPLY_ALERTS'], msg)
                
                # For EXECUTION_DELAY proposals, check if they're ready for execution
                elif proposal.status == ProposalStatus.EXECUTION_DELAY.value:
                    time_since_passed = current_time - proposal.end_time
                    
                    if time_since_passed >= EXECUTION_DELAY and time_since_passed < EXECUTION_DEADLINE:
                        # Ready for execution
                        update = proposals_table.update().where(
                            and_(
                                proposals_table.c.proposal_id == proposal.proposal_id,
                                proposals_table.c.voter_address == proposal.voter_address
                            )
                        ).values(
                            status=ProposalStatus.EXECUTABLE.value,
                            last_updated=current_time
                        )
                        conn.execute(update)
                        conn.commit()
                        
                        msg = f"⏰ *Resupply Proposal Ready for Execution*\n\n"
                        msg += f"Proposal {proposal.proposal_id}: {proposal.description}\n"
                        msg += f"Execution Deadline: {datetime.fromtimestamp(proposal.end_time + EXECUTION_DEADLINE, UTC).strftime('%Y-%m-%d %H:%M UTC')}\n"
                        msg += f"\n🔗 [Etherscan](https://etherscan.io/tx/{proposal.txn_hash}) | [Resupply](https://resupply.fi/governance/proposals) | [Hippo Army](https://hippo.army/dao/proposal/{proposal.proposal_id})"
                        send_alert(CHAT_IDS['RESUPPLY_ALERTS'], msg)
                
                # Check for expired proposals (both PASSED and EXECUTION_DELAY)
                if proposal.status in [ProposalStatus.PASSED.value, ProposalStatus.EXECUTION_DELAY.value]:
                    time_since_passed = current_time - proposal.end_time
                    if time_since_passed >= EXECUTION_DEADLINE:
                        # Past execution deadline
                        update = proposals_table.update().where(
                            and_(
                                proposals_table.c.proposal_id == proposal.proposal_id,
                                proposals_table.c.voter_address == proposal.voter_address
                            )
                        ).values(
                            status=ProposalStatus.EXPIRED.value,
                            last_updated=current_time
                        )
                        conn.execute(update)
                        conn.commit()
                        
                        msg = f"⌛ *Resupply Proposal Expired*\n\n"
                        msg += f"Proposal {proposal.proposal_id}: {proposal.description}\n"
                        msg += f"Execution Deadline: {datetime.fromtimestamp(proposal.end_time + EXECUTION_DEADLINE, UTC).strftime('%Y-%m-%d %H:%M UTC')}\n"
                        msg += f"\n🔗 [Etherscan](https://etherscan.io/tx/{proposal.txn_hash}) | [Resupply](https://resupply.fi/governance/proposals) | [Hippo Army](https://hippo.army/dao/proposal/{proposal.proposal_id})"
                        send_alert(CHAT_IDS['RESUPPLY_ALERTS'], msg)
    
    except SQLAlchemyError as e:
        logger.error(f"Database error in check_proposal_statuses: {str(e)}")
        raise
    except Exception as e:
        logger.error(f"Unexpected error in check_proposal_statuses: {str(e)}")
        raise

def get_registry_voter():
    registry_address = '0x10101010E0C3171D894B71B3400668aF311e7D94'  # Replace with actual registry address
    registry_abi = utils.load_abi('./abis/resupply_registry.json')
    registry_contract = w3.eth.contract(address=registry_address, abi=registry_abi)
    return registry_contract.functions.getAddress('VOTER').call()

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
    # Get all voter addresses including from registry
    voter_addresses = set(VOTER_ADDRESSES)
    try:
        registry_voter = get_registry_voter()
        if registry_voter != '0x0000000000000000000000000000000000000000':
            voter_addresses.add(registry_voter)
    except Exception as e:
        logger.error(f"Error getting registry voter: {str(e)}")
        logger.info("Continuing with known voter addresses only")
    
    logger.info("\nMonitoring voter contracts:")
    for addr in voter_addresses:
        logger.info(f"- {addr}")
    
    # Initialize contracts
    voter_contracts = {
        address: w3.eth.contract(address=address, abi=voter_abi)
        for address in voter_addresses
    }
    
    i = 0
    while True:
        try:
            i += 1            
            height = w3.eth.get_block_number()
            last_block_written = get_last_block_written()
            to_block = min(last_block_written + MAX_WIDTH, height)
            if i % 100 == 0:
                logger.info(f"Loops since startup: {i}")
                logger.info(f'Listening from block {last_block_written} --> {to_block}')
            
            # Process events for each voter contract
            for voter_address, contract in voter_contracts.items():
                try:
                    # ProposalCreated
                    logs = fetch_logs(contract, 'ProposalCreated', last_block_written, to_block)
                    for log in logs:
                        handle_proposal_created(log, voter_address)
                    
                    # VoteCast
                    logs = fetch_logs(contract, 'VoteCast', last_block_written, to_block)
                    for log in logs:
                        handle_vote_cast(log, voter_address)
                    
                    # ProposalCancelled
                    logs = fetch_logs(contract, 'ProposalCancelled', last_block_written, to_block)
                    for log in logs:
                        handle_proposal_cancelled(log, voter_address)
                    
                    # ProposalExecuted
                    logs = fetch_logs(contract, 'ProposalExecuted', last_block_written, to_block)
                    for log in logs:
                        handle_proposal_executed(log, voter_address)
                    
                    # ProposalDescriptionUpdated
                    logs = fetch_logs(contract, 'ProposalDescriptionUpdated', last_block_written, to_block)
                    for log in logs:
                        handle_proposal_description_updated(log, voter_address)
                except Exception as e:
                    logger.error(f"Error processing events for voter {voter_address}: {str(e)}")
                    continue  # Continue with next voter contract
            
            # Check proposal statuses and send alerts
            check_proposal_statuses()
            
        except Exception as e:
            logger.error(f"Error in main loop: {str(e)}")
        
        time.sleep(POLL_INTERVAL)

if __name__ == '__main__':
    main()
