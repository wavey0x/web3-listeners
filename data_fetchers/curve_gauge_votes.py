from web3 import Web3
from sqlalchemy import create_engine, Table, Column, Integer, String, MetaData, select
from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import SQLAlchemyError, IntegrityError
from psycopg2 import errors
import time, os, json, sys
from datetime import datetime
# Add the parent directory of the current file to sys.path
parent_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
sys.path.append(parent_dir)

import utils
from dotenv import load_dotenv

load_dotenv()

GAUGE_CONTROLLER_ADDRESS = '0x2F50D538606Fa9EDD2B11E2446BEb18C9D5846bB'
VE_ADDRESS = '0x5f3b5DfEb7B28CDbD7FAba78963EE202a494e2A2'

# Connection URL to your Ethereum node
WEB3_PROVIDER_URI = os.getenv('WEB3_PROVIDER_URI')
DATABASE_URI = os.getenv('DATABASE_URI')
DEPLOY_BLOCK=10647875
MAX_WIDTH = 200_000
POLL_INTERVAL = 1 # seconds

# Connect to Ethereum network
w3 = Web3(Web3.HTTPProvider(WEB3_PROVIDER_URI))
# Ensure that connection is successful
if not w3.is_connected():
    raise Exception("Failed to connect to Ethereum node")

# Set up PostgreSQL connection
engine = create_engine(DATABASE_URI)
Session = sessionmaker(bind=engine)
session = Session()
metadata = MetaData()

table = Table('curve_gauge_votes', metadata, autoload_with=engine)

gauge_controller_abi = utils.load_abi('./abis/gauge_controller.json')
ve_abi = utils.load_abi('./abis/ve.json')

height = w3.eth.get_block_number()

gauge_name_dict = {}
gauge_controller_contract = w3.eth.contract(address=GAUGE_CONTROLLER_ADDRESS, abi=gauge_controller_abi)
ve_contract = w3.eth.contract(address=VE_ADDRESS, abi=ve_abi)

def main():
    global gauge_name_dict
    gauge_name_dict = get_gauge_list()
    
    log_loop()

def handle_vote_event(event):
    # Parse the event data and write to the database
    block = event.blockNumber
    timestamp = w3.eth.get_block(block).timestamp
    date_str = datetime.utcfromtimestamp(timestamp).strftime('%Y-%m-%d %H:%M:%S')
    txn_hash = event.transactionHash.hex()
    # Inserting data into the PostgreSQL database
    gauge = event['args']['gauge_addr']
    weight = event['args']['weight']
    user = event['args']['user']
    amount = ve_contract.functions.balanceOf(user).call(block_identifier=block) / 1e18 * weight / 10_000
    gauge_name = gauge_name_dict[gauge]
    try:
        ins = table.insert().values(
            gauge=gauge,
            gauge_name=gauge_name,
            account=user,
            amount=amount,
            weight=weight,
            txn_hash = txn_hash,
            timestamp = timestamp,
            date_str = date_str,
            block = event.blockNumber,
        )
        conn = engine.connect()
        conn.execute(ins)
        conn.commit()
        # conn.close()
        print(f'{gauge} {gauge_name} vote worth {amount:,.0f} veCRV written successfully | Block: {block} Txn: {txn_hash}')
    except IntegrityError as e:
        conn.rollback()
    except SQLAlchemyError as e:
        print("Database error occurred:", e)
    except Exception as e:
        print("An error occurred:", e)

def fetch_logs(contract, event_name, from_block, to_block):
    event = getattr(contract.events, event_name)
    logs = event.get_logs(
        fromBlock=from_block,
        toBlock=to_block
    )
    return logs

def log_loop():
    i = 0
    while True:
        i += 1
        if i % 100 == 0: print(f"Loops since startup: {i}")
        last_block_written = get_last_block_written()

        print(f'Listening from block {last_block_written}')

        logs = fetch_logs(
            gauge_controller_contract, 
            'VoteForGauge', 
            last_block_written, 
            min(last_block_written + 100_000, height)
        )

        for log in logs:
            handle_vote_event(log)

        time.sleep(POLL_INTERVAL)

def get_last_block_written():
    # Connecting to the database and fetching the last block
    with engine.connect() as conn:
        query = select(table.c.block)
        query = query.order_by(table.c.block.desc()).limit(1)
        result = conn.execute(query).scalar()
        # Return the result or DEPLOY_BLOCK if no entries found
        return result + 1 if result is not None else DEPLOY_BLOCK


def get_gauge_list():
    import requests, re
    url = 'https://api.curve.fi/api/getAllGauges'
    data = requests.get(url).json()['data']
    gauge_list = {}
    for gauge_name, d in data.items():
        gauge_name = re.sub(r'\s*\(.*?\)', '', gauge_name)
        gauge_list[w3.to_checksum_address(d['gauge'])] = gauge_name
    return gauge_list


if __name__ == '__main__':
    main()