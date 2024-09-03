from web3 import Web3
from sqlalchemy import create_engine, Table, Column, Integer, String, MetaData, select
from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import SQLAlchemyError, IntegrityError
from psycopg2 import errors
import time, os, json, sys
import telebot
from datetime import datetime
from dotenv import load_dotenv

# Add the parent directory of the current file to sys.path
parent_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
sys.path.append(parent_dir)
import utils
from constants import CHAT_IDS


telegram_bot_key = os.environ.get('WAVEY_ALERTS_BOT_KEY')
bot = telebot.TeleBot(telegram_bot_key)
load_dotenv()

GAUGE_CONTROLLER_ADDRESS = '0x2F50D538606Fa9EDD2B11E2446BEb18C9D5846bB'
VE_ADDRESS = '0x5f3b5DfEb7B28CDbD7FAba78963EE202a494e2A2'

# Connection URL to your Ethereum node
WEB3_PROVIDER_URI = os.getenv('WEB3_PROVIDER_URI')
DATABASE_URI = os.getenv('DATABASE_URI')
DEPLOY_BLOCK=10647875
MAX_WIDTH = 250_000
POLL_INTERVAL = 10 # seconds

last_block_alerted = 0

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

GAUGE_NAME_EXCEPTIONS = {
    '0x6C09F6727113543Fd061a721da512B7eFCDD0267': 'xdai x3pool',
    '0xb9C05B8EE41FDCbd9956114B3aF15834FDEDCb54': 'ftm 2pool',
    '0xfE1A3dD8b169fB5BF0D5dbFe813d956F39fF6310': 'ftm g3CRV',
    '0xfDb129ea4b6f557b07BcDCedE54F665b7b6Bc281': 'ftm btcCRV',
    '0x260e4fBb13DD91e187AE992c3435D0cf97172316': 'ftm crv3crypto',
    '0xC48f4653dd6a9509De44c92beb0604BEA3AEe714': 'polygon am3pool',
    '0x060e386eCfBacf42Aa72171Af9EFe17b3993fC4F': 'polygon a3crypto',
    '0x488E6ef919C2bB9de535C634a80afb0114DA8F62': 'polygon btcCRV',
    '0xAF78381216a8eCC7Ad5957f3cD12a431500E0B0D': 'polygon crvEURTUSD',
    '0xFf17560d746F85674FE7629cE986E949602EF948': 'arbi 2pool',
    '0x9F86c5142369B1Ffd4223E5A2F2005FC66807894': 'arbi btcCRV',
    '0x9044E12fB1732f88ed0c93cfa5E9bB9bD2990cE5': 'arbi 3crypto',
    '0x56eda719d82aE45cBB87B7030D3FB485685Bea45': 'arbi crvEURSUSD',
    '0xB504b6EB06760019801a91B451d3f7BD9f027fC9': 'avax av3crv',
    '0x75D05190f35567e79012c2F0a02330D3Ed8a1F74': 'avax btcCRV',
    '0xa05E565cA0a103FcD999c7A7b8de7Bd15D5f6505': 'avax 3crypto',
    '0xf2Cde8c47C20aCbffC598217Ad5FE6DB9E00b163': 'harmony gauge',
    '0x1cEBdB0856dd985fAe9b8fEa2262469360B8a3a6': 'crvCRVETH',
    '0xbAF05d7aa4129CA14eC45cC9d4103a9aB9A9fF60': 'Vyper Fundraising Gauge',
}


ALIASES = {
    '0x989AEb4d175e16225E39E87d0D97A3360524AD80': 'Convex',
    '0x7a16fF8270133F063aAb6C9977183D9e72835428': 'Mich',
    '0xF147b8125d2ef93FB6965Db97D6746952a133934': 'Yearn',
    '0x52f541764E6e90eeBc5c21Ff570De0e2D63766B6': 'Stakedao',
}

def main():
    global gauge_name_dict
    gauge_name_dict = get_gauge_list()
    
    log_loop()

def handle_vote_event(event):
    global last_block_alerted
    # Parse the event data and write to the database
    block = event.blockNumber
    timestamp = w3.eth.get_block(block).timestamp
    date_str = datetime.utcfromtimestamp(timestamp).strftime('%Y-%m-%d %H:%M:%S')
    txn_hash = event.transactionHash.hex()
    # Inserting data into the PostgreSQL database
    gauge = event['args']['gauge_addr']
    weight = event['args']['weight']
    user = event['args']['user']
    alias = '' if user not in ALIASES else ALIASES[user]
    amount = ve_contract.functions.balanceOf(user).call(block_identifier=block) / 1e18 * weight / 10_000
    if gauge in gauge_name_dict:
        gauge_name = gauge_name_dict[gauge]
    elif gauge in GAUGE_NAME_EXCEPTIONS:
        gauge_name = GAUGE_NAME_EXCEPTIONS[gauge]
    else:
        raise Exception(f"GAUGE NAME NOT FOUND! {gauge}")

    try:
        ins = table.insert().values(
            gauge=gauge,
            gauge_name=gauge_name,
            account=user,
            amount=amount,
            weight=weight,
            account_alias=alias,
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

    if (
        amount > 1_000_000
        and user in ALIASES 
        and block > last_block_alerted
    ):
        last_block_alerted = block
        alias = ALIASES[user]
        m = f'🗳️ Curve Gauge Vote Detected'
        m += f'\n\n {alias}'
        m += f'\n\n🔗 [View on Etherscan](https://etherscan.io/tx/{txn_hash})'
        send_alert(CHAT_IDS['WAVEY_ALERTS'], m)

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
        to_block = min(last_block_written + MAX_WIDTH, height)
        logs = fetch_logs(
            gauge_controller_contract, 
            'VoteForGauge', 
            last_block_written, 
            to_block
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


def send_alert(chat_id, msg):
    bot.send_message(chat_id, msg, parse_mode="markdown", disable_web_page_preview = True)


if __name__ == '__main__':
    main()