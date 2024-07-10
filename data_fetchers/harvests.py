from web3 import Web3
from sqlalchemy import create_engine, Table, Column, Integer, String, MetaData, select
from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import SQLAlchemyError, IntegrityError
from psycopg2 import errors
import time
import json
from datetime import datetime
import sys
import os
# Add the parent directory of the current file to sys.path
parent_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
sys.path.append(parent_dir)
from constants import CURVE_LIQUID_LOCKER_COMPOUNDERS
import utils
from dotenv import load_dotenv

load_dotenv()

# Connection URL to your Ethereum node
WEB3_PROVIDER_URI = os.getenv('WEB3_PROVIDER_URI')
DATABASE_URI = os.getenv('DATABASE_URI')
POLL_INTERVAL = 10

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
harvest_table = Table('crv_ll_harvests', metadata, autoload_with=engine)

yvycrv_abi = utils.load_abi('./abis/yvycrv.json')
asdcrv_abi = utils.load_abi('./abis/asdcrv.json')
ucvxcrv_abi = utils.load_abi('./abis/ucvxcrv.json')

event_filters = {
    'Harvested': [],
    # etc ...
}

def get_last_block_written(contract):

    # Connecting to the database and fetching the last block
    with engine.connect() as conn:
        query = select(harvest_table.c.block).where((harvest_table.c.compounder == contract))
        query = query.order_by(harvest_table.c.block.desc()).limit(1)
        result = conn.execute(query).scalar()
        # Return the result or DEPLOY_BLOCK if no entries found
        min_block = max(20_000_000, CURVE_LIQUID_LOCKER_COMPOUNDERS[contract]['deploy_block'])
        return result + 1 if result is not None else min_block


def main():
    for compounder, info in CURVE_LIQUID_LOCKER_COMPOUNDERS.items():
        last_block_written = get_last_block_written(compounder)
        print(f'{info["symbol"]} listening from block: {last_block_written}')
        event_filters['Harvested'].append(create_filter(compounder, info, last_block_written))

    log_loop(event_filters, POLL_INTERVAL)

def log_loop(event_filters, poll_interval):
    while True:
        for type, filters in event_filters.items():
            if type == 'Harvested':
                for filter in filters:
                    address = filter.filter_params.get('address')
                    for event in filter.get_new_entries():
                        handle_harvested_event(address, event)
        time.sleep(poll_interval)

def handle_harvested_event(address, event):
    profit = 0
    block = event.blockNumber
    timestamp = w3.eth.get_block(block)['timestamp']
    name = CURVE_LIQUID_LOCKER_COMPOUNDERS[address]['symbol']
    underlying = CURVE_LIQUID_LOCKER_COMPOUNDERS[address]['underlying']
    compounder = address
    txn_hash = event.transactionHash.hex()
    if address == '0x43E54C2E7b3e294De3A155785F52AB49d87B9922':
        profit = event['args']['assets'] / 1e18
    if address == '0xde2bEF0A01845257b4aEf2A2EAa48f6EAeAfa8B7':
        profit = event['args']['_value'] / 1e18
    if address == '0x27B5739e22ad9033bcBf192059122d163b60349D':
        profit = event['args']['gain'] / 1e18
    date_str = datetime.utcfromtimestamp(timestamp).strftime('%Y-%m-%d %H:%M:%S')

    try:
        ins = harvest_table.insert().values(
            profit = profit,
            timestamp = timestamp,
            name=name,
            underlying=underlying,
            compounder=compounder,
            block=block,
            txn_hash = txn_hash,
            date_str = date_str
        )
        conn = engine.connect()
        conn.execute(ins)
        conn.commit()
        print(f'{name} {event["event"]} event written successfully. Txn: {txn_hash}')
    except IntegrityError as e:
        conn.rollback()
    except SQLAlchemyError as e:
        print("Database error occurred:", e)
    except Exception as e:
        print("An error occurred:", e)
    return


def create_filter(compounder, info, last_block_written):
    abi = ucvxcrv_abi
    event_name = 'Harvest'
    if info['symbol'] == 'asdCRV':
        abi = asdcrv_abi
    if info['symbol'] == 'yvyCRV':
        abi = yvycrv_abi
        event_name = 'StrategyReported'
    
    compounder_contract = w3.eth.contract(address=compounder, abi=abi)
    event = getattr(compounder_contract.events, event_name)
    
    return event.create_filter(fromBlock=max(info['deploy_block'], last_block_written))


if __name__ == '__main__':
    main()


# CREATE TABLE crv_ll_harvests (
#     profit NUMERIC(30, 18),
#     timestamp INTEGER,
#     name VARCHAR,
#     underlying VARCHAR,
#     compounder VARCHAR,
#     block INTEGER,
#     txn_hash VARCHAR,
#     date_str VARCHAR,
#     UNIQUE (txn_hash, profit, compounder)
# );
