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
MAX_WIDTH = 200_000
POLL_INTERVAL = 120 # seconds

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
ybs_abi = utils.load_abi('./abis/ybs.json')
registry_abi = utils.load_abi('./abis/registry.json')
rewards_abi = utils.load_abi('./abis/rewards.json')
erc20_abi = utils.load_abi('./abis/erc20.json')

deployments = {}
deployments_by_rewards = {}
deployments_by_ybs = {}

height = w3.eth.get_block_number()

def main():
    global deployments
    global deployments_by_rewards
    global deployments_by_ybs

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

    log_loop()

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

def fetch_logs(contract, event_name, from_block, to_block):
    event = getattr(contract.events, event_name)
    logs = event.get_logs(
        fromBlock=from_block,
        toBlock=to_block
    )
    return logs

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

def log_loop():
    i = 0
    while True:
        i += 1
        if i % 100 == 0: print(f"Loops since startup: {i}")
        for token, deployment in deployments.items():
            decimals = deployments[token]['decimals']
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
            print('----')

            height = w3.eth.get_block_number()
            staked_logs = fetch_logs(
                ybs_contract, 
                'Staked', 
                last_block_stake, 
                height
            )
            for log in staked_logs:
                handle_stake_event(log, decimals)

            unstaked_logs = fetch_logs(
                ybs_contract, 
                'Unstaked', 
                last_block_unstake, 
                height
            )
            for log in unstaked_logs:
                handle_stake_event(log, decimals)

            reward_claim_logs = fetch_logs(
                rewards_contract, 
                'RewardsClaimed', 
                last_block_reward_claim, 
                height
            )
            for log in reward_claim_logs:
                handle_reward_event(log, decimals, True)

            reward_deposit_logs = fetch_logs(
                rewards_contract, 
                'RewardDeposited', 
                last_block_reward_deposit, 
                height
            )
            for log in reward_deposit_logs:
                handle_reward_event(log, decimals, False)

        time.sleep(POLL_INTERVAL)

def get_last_block_written(ybs, event_type):
    # Verify which table corresponds to a given event type in the schema
    table = stakes_table if event_type in ['Staked', 'Unstaked'] else rewards_table
    is_stake = True if event_type == 'Staked' else False
    is_claim = True if event_type == 'RewardsClaimed' else False

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

if __name__ == '__main__':
    main()