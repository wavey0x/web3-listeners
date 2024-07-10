from web3 import Web3
from sqlalchemy import create_engine, Table, Column, Integer, String, MetaData, select
from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import SQLAlchemyError, IntegrityError
from psycopg2 import errors
import time, os, json
from datetime import datetime
import utils
from dotenv import load_dotenv

load_dotenv()

# Connection URL to your Ethereum node
WEB3_PROVIDER_URI = os.getenv('WEB3_PROVIDER_URI')
DATABASE_URI = os.getenv('DATABASE_URI')
DEPLOY_BLOCK=19919001

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

stakes_table = Table('stakes', metadata, autoload_with=engine)
rewards_table = Table('rewards', metadata, autoload_with=engine)

# Ethereum contract details
REGISTRY_ADDRESS = '0x262be1d31d0754399d8d5dc63B99c22146E9f738'
ybs_address = '0xF4C6e0E006F164535508787873d86b84fe901975'
ybs_abi = '[{"constant":true,"inputs":[],"name":"name","outputs":...}]'  # Simplified
ybs_abi = utils.load_abi('./abis/ybs.json')
registry_abi = utils.load_abi('./abis/registry.json')
rewards_abi = utils.load_abi('./abis/rewards.json')
erc20_abi = utils.load_abi('./abis/erc20.json')

deployments = {}
deployments_by_rewards = {}
deployments_by_ybs = {}

event_filters = {
    'Staked': [],
    'Unstaked': [],
    'RewardsClaimed': [],
    'RewardDeposited': [],
}

def main():
    registry = w3.eth.contract(address=REGISTRY_ADDRESS, abi=registry_abi)
    num_tokens = registry.functions.numTokens().call()
    for i in range(num_tokens):
        token = registry.functions.tokens(i).call()
        token_symbol = w3.eth.contract(address=token, abi=erc20_abi).functions.symbol().call()
        deployment = registry.functions.deployments(token).call()
        deployments[token] = {
            'ybs': deployment[0],
            'rewards': deployment[1],
            'utils': deployment[2],
            'decimals': 18,
            'symbol': token_symbol,
        }
        deployments_by_rewards[deployments[token]['rewards']] = {
            'ybs': deployment[0],
            'token': token,
            'utils': deployment[2],
            'decimals': 18,
            'symbol': token_symbol,
        }
        deployments_by_ybs[deployment[0]] = {
            'rewards': deployment[1],
            'token': token,
            'utils': deployment[2],
            'decimals': 18,
            'symbol': token_symbol,
        }

    for token, deployment in deployments.items():
        ybs_contract = w3.eth.contract(address=deployment['ybs'], abi=ybs_abi)
        rewards_contract = w3.eth.contract(address=deployment['rewards'], abi=rewards_abi)

        # Get the last block for Staked and Unstaked events
        last_block_stake = get_last_block_written(deployment['ybs'], 'Staked')
        last_block_unstake = get_last_block_written(deployment['ybs'], 'Unstaked')
        last_block_reward_deposit = get_last_block_written(deployment['ybs'], 'RewardDeposited')
        last_block_reward_claim = get_last_block_written(deployment['ybs'], 'RewardsClaimed')

        print(f'{deployments[token]["symbol"]} Staked listening from block {last_block_stake}')
        print(f'{deployments[token]["symbol"]} Staked listening from block {last_block_unstake}')
        print(f'{deployments[token]["symbol"]} RewardDeposited listening from block {last_block_reward_deposit}')
        print(f'{deployments[token]["symbol"]} RewardsClaimed listening from block {last_block_reward_claim}')
        print('---')
        # Create filters for the events
        filter_stake = ybs_contract.events.Staked.create_filter(fromBlock=max(DEPLOY_BLOCK, last_block_stake))
        filter_unstake = ybs_contract.events.Unstaked.create_filter(fromBlock=max(DEPLOY_BLOCK, last_block_unstake))
        filter_reward_claims = rewards_contract.events.RewardsClaimed.create_filter(fromBlock=max(DEPLOY_BLOCK, last_block_reward_claim))
        filter_reward_deposits = rewards_contract.events.RewardDeposited.create_filter(fromBlock=max(DEPLOY_BLOCK, last_block_reward_deposit))

        event_filters['Staked'].append(filter_stake)
        event_filters['Unstaked'].append(filter_unstake)
        event_filters['RewardsClaimed'].append(filter_reward_claims)
        event_filters['RewardDeposited'].append(filter_reward_deposits)

    # 10 second polling interval
    log_loop(event_filters, 10, deployments[token]['decimals'])

def handle_stake_event(event, decimals):
    # Parse the event data and write to the database
    block = w3.eth.get_block(event.blockNumber)
    staker = event.address
    account = event['args']['account']
    amount = event['args']['amount'] / 10 ** decimals
    week = event['args']['week']
    new_weight = event['args']['newUserWeight'] / 10 ** decimals
    if 'weightAdded' in event['args']:
        weight_change = event['args']['weightAdded'] / 10 ** decimals
    if 'weightRemoved' in event['args']:
        weight_change = event['args']['weightRemoved'] / 10 ** decimals
    timestamp = block.timestamp
    date_str = datetime.utcfromtimestamp(timestamp).strftime('%Y-%m-%d %H:%M:%S')
    txn_hash = event.transactionHash.hex()
    token = deployments_by_ybs[event.address]['token']
    # Inserting data into the PostgreSQL database
    try:
        ins = stakes_table.insert().values(
            ybs=staker,
            account=account,
            amount=amount,
            is_stake=event['event'] == 'Staked',
            week=week,
            new_weight=new_weight,   
            net_weight_change=weight_change,
            timestamp = timestamp,
            date_str = date_str,
            txn_hash = txn_hash,
            block = event.blockNumber,
            token = token
        )
        conn = engine.connect()
        conn.execute(ins)
        conn.commit()
        # conn.close()
        print(f'{deployments[token]["symbol"]} {event["event"]} event written successfully. Txn: {txn_hash}')
    except IntegrityError as e:
        conn.rollback()
    except SQLAlchemyError as e:
        print("Database error occurred:", e)
    except Exception as e:
        print("An error occurred:", e)
    # finally:
    #     conn.close()

def handle_reward_event(event, decimals, is_claim):
    # Parse the event data and write to the database
    block = w3.eth.get_block(event.blockNumber)
    reward_distributor = event.address
    if is_claim:
        account = event['args']['account']
    else:
        account = event['args']['depositor']
    amount = event['args']['rewardAmount'] / 10 ** decimals
    week = event['args']['week']
    ybs = deployments_by_rewards[event.address]['ybs']
    timestamp = block.timestamp
    date_str = datetime.utcfromtimestamp(timestamp).strftime('%Y-%m-%d %H:%M:%S')
    txn_hash = event.transactionHash.hex()
    token = deployments_by_rewards[event.address]['token']
    # Inserting data into the PostgreSQL database
    try:
        ins = rewards_table.insert().values(
            ybs = ybs,
            is_claim = is_claim,
            reward_distributor=reward_distributor,
            account=account,
            amount=amount,
            week=week,
            timestamp = timestamp,
            date_str = date_str,
            txn_hash = txn_hash,
            block = event.blockNumber,
            token = token
        )

        conn = engine.connect()
        conn.execute(ins)
        conn.commit()
        print(f'{deployments[token]["symbol"]} {event["event"]} event written successfully. Txn: {txn_hash}')
    except IntegrityError as e:
        conn.rollback()
    except SQLAlchemyError as e:
        print("Database error occurred:", e)
    except Exception as e:
        print("An error occurred:", e)
    # finally:
    #     conn.close()

def log_loop(event_filters, poll_interval, decimals):
    while True:
        for type, filters in event_filters.items():
            if type == 'Staked' or type == 'Unstaked':
                for filter in filters:
                    for event in filter.get_new_entries():
                        handle_stake_event(event, decimals)
            elif type == 'RewardsClaimed':
                for filter in filters:
                    for event in filter.get_new_entries():
                        handle_reward_event(event, decimals, True)
            elif type == 'RewardDeposited':
                for filter in filters:
                    for event in filter.get_new_entries():
                        handle_reward_event(event, decimals, False)

        # print(f'Polling...')
        time.sleep(poll_interval)

def get_last_block_written(ybs, event_type):
    # Verify which table corresponds to a given event type in the schema
    table = stakes_table if event_type in ['Staked', 'Unstaked'] else rewards_table
    is_stake = True
    is_claim = True
    if event_type == 'Unstaked':
        is_stake = False
    elif event_type == 'RewardDeposited':
        is_claim = False

    # Connecting to the database and fetching the last block
    with engine.connect() as conn:
        query = select(table.c.block).where((table.c.ybs == ybs))
        # Special condition for Staker contract (Stake/Unstake events)
        if table == stakes_table:
            query = query.where((table.c.is_stake == is_stake))
        if table == rewards_table:
            query = query.where((table.c.is_claim == is_claim))
        query = query.order_by(table.c.block.desc()).limit(1)
        result = conn.execute(query).scalar()
        # Return the result or DEPLOY_BLOCK if no entries found
        return result + 1 if result is not None else DEPLOY_BLOCK


# def log_loop(event_filters, poll_interval):
#     while True:
#         for event_filter in event_filters:
#             for event in event_filter.get_new_entries():
#                 if event_filter == event_filter_a:
#                     handle_event_a(event)
#                 elif event_filter == event_filter_b:
#                     handle_event_b(event)
#         time.sleep(poll_interval)

if __name__ == '__main__':
    main()


"""
CREATE TABLE stakes (
    id SERIAL primary KEY,                     -- Integer, not nullable
    ybs VARCHAR,                             -- VARCHAR type, nullable
    is_stake BOOLEAN,                        -- Boolean, nullable
    account VARCHAR,                         -- VARCHAR type, nullable
    amount NUMERIC(30, 18),                          -- Decimal, nullable
    new_weight NUMERIC(30, 18),                       -- Decimal, nullable
    net_weight_change NUMERIC(30, 18),                 -- Decimal, nullable
    week INTEGER,                            -- Integer, nullable
    txn_hash VARCHAR,                        -- VARCHAR type, transaction hash, nullable
    block INTEGER,                           -- Integer, represents the blockchain block number, nullable
    timestamp NUMERIC,                       -- Numeric, nullable (could represent UNIX timestamp)
    date_str VARCHAR,                        -- VARCHAR type that holds date, nullable
    token VARCHAR,                           -- VARCHAR type, nullable, refers typically to a contract or entity ID
    UNIQUE(account, ybs, is_stake, txn_hash, amount, new_weight)  -- Enforces uniqueness
);

CREATE TABLE rewards (
    id SERIAL primary KEY,                    -- Integer, not nullable
    reward_distributor VARCHAR,              -- VARCHAR type, nullable
    account VARCHAR,                         -- VARCHAR type, nullable
    amount NUMERIC(30, 18),                          -- Numeric type, suitable for financial data, nullable
    week INTEGER,                            -- Integer, represents a week number, nullable
    ybs VARCHAR,                             -- VARCHAR type, related contract or entity ID, nullable
    txn_hash VARCHAR,                        -- VARCHAR type, transaction hash, nullable
    block INTEGER,                           -- Integer, represents the blockchain block number, nullable
    timestamp INTEGER,                       -- Integer, UNIX timestamp of the event, nullable
    date_str VARCHAR,                        -- VARCHAR type, human-readable date, nullable
    token VARCHAR,                           -- VARCHAR type, token involved, nullable
    is_claim BOOLEAN,                        -- Boolean, nullable, marks if the reward was claimed
    UNIQUE(txn_hash, amount, account, reward_distributor)
);

🚢 STAKE:  0xe3668873D944E4A949DA05fc8bDE419eFF543882 20188078
🚢 UNSTAKE:  0xe3668873D944E4A949DA05fc8bDE419eFF543882 20179875
🚢 REWARDS:  0xe3668873D944E4A949DA05fc8bDE419eFF543882 19919001
🚢 STAKE:  0xFCc5c47bE19d06BF83eB04298b026F81069ff65b 20189532
🚢 UNSTAKE:  0xFCc5c47bE19d06BF83eB04298b026F81069ff65b 20176953
🚢 REWARDS:  0xFCc5c47bE19d06BF83eB04298b026F81069ff65b 19919001

🚢 STAKE:  0xe3668873D944E4A949DA05fc8bDE419eFF543882 20188078
🚢 UNSTAKE:  0xe3668873D944E4A949DA05fc8bDE419eFF543882 20179875
🚢 REWARDS:  0xe3668873D944E4A949DA05fc8bDE419eFF543882 19919001
🚢 STAKE:  0xFCc5c47bE19d06BF83eB04298b026F81069ff65b 20189532
🚢 UNSTAKE:  0xFCc5c47bE19d06BF83eB04298b026F81069ff65b 20187187
🚢 REWARDS:  0xFCc5c47bE19d06BF83eB04298b026F81069ff65b 19919001

🚢 STAKE:  0xe3668873D944E4A949DA05fc8bDE419eFF543882 20188078
🚢 UNSTAKE:  0xe3668873D944E4A949DA05fc8bDE419eFF543882 20179875
🚢 REWARDS:  0xe3668873D944E4A949DA05fc8bDE419eFF543882 19919001
🚢 STAKE:  0xFCc5c47bE19d06BF83eB04298b026F81069ff65b 20189532
🚢 UNSTAKE:  0xFCc5c47bE19d06BF83eB04298b026F81069ff65b 20187187
🚢 REWARDS:  0xFCc5c47bE19d06BF83eB04298b026F81069ff65b 19919001
"""