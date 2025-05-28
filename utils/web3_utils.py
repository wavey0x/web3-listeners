from web3 import Web3
from datetime import datetime
from functools import lru_cache
import time
import os
from dotenv import load_dotenv

DAY = 60 * 60 * 24
WEEK = DAY * 7
ZERO_ADDRESS = '0x0000000000000000000000000000000000000000'

@lru_cache(maxsize=1000)
def block_to_date(web3: Web3, block_number: int) -> datetime:
    """Convert block number to datetime"""
    time = web3.eth.get_block(block_number).timestamp
    return datetime.fromtimestamp(time)

def closest_block_after_timestamp(web3: Web3, timestamp: int) -> int:
    """Find the closest block after a given timestamp"""
    return _closest_block_after_timestamp(web3, web3.eth.chain_id, timestamp)

@lru_cache(maxsize=1000)
def _closest_block_after_timestamp(web3: Web3, chain_id: int, timestamp: int) -> int:
    """Internal function to find closest block after timestamp with caching"""
    height = web3.eth.block_number
    lo, hi = 0, height

    while hi - lo > 1:
        mid = lo + (hi - lo) // 2
        if get_block_timestamp(web3, mid) > timestamp:
            hi = mid
        else:
            lo = mid

    if get_block_timestamp(web3, hi) < timestamp:
        raise Exception("timestamp is in the future")
    print(f'Chain ID: {chain_id} {hi}')
    return hi

@lru_cache(maxsize=1000)
def closest_block_before_timestamp(web3: Web3, timestamp: int) -> int:
    """Find the closest block before a given timestamp"""
    return closest_block_after_timestamp(web3, timestamp) - 1

@lru_cache(maxsize=1000)
def get_block_timestamp(web3: Web3, height: int) -> int:
    """Get timestamp for a given block number"""
    return web3.eth.get_block(height).timestamp

def timestamp_to_date_string(ts: int) -> str:
    """Convert timestamp to date string"""
    return datetime.utcfromtimestamp(ts).strftime("%m/%d/%Y, %H:%M:%S")

def timestamp_to_string(ts: int) -> str:
    """Convert timestamp to string"""
    dt = datetime.utcfromtimestamp(ts).strftime("%m/%d/%Y, %H:%M:%S")
    return dt

@lru_cache(maxsize=1000)
def contract_creation_block(web3: Web3, address: str) -> int:
    """
    Find contract creation block using binary search.
    NOTE Requires access to historical state. Doesn't account for CREATE2 or SELFDESTRUCT.
    """
    lo = 0
    hi = end = web3.eth.block_number

    while hi - lo > 1:
        mid = lo + (hi - lo) // 2
        code = web3.eth.get_code(address, block_identifier=mid)
        if code:
            hi = mid
        else:
            lo = mid
    return hi if hi != end else None

def get_logs_chunked(web3: Web3, contract, event_name: str, start_block: int = 0, end_block: int = 0, chunk_size: int = 100_000, debug: bool = False):
    """Get event logs in chunks to avoid timeout"""
    try:
        event = getattr(contract.events, event_name)
    except Exception as e:
        print(f'Contract has no event by the name {event_name}', e)
        return []

    if start_block == 0:
        start_block = contract_creation_block(web3, contract.address)
    if end_block == 0:
        end_block = web3.eth.block_number

    logs = []
    while start_block < end_block:
        if debug:
            print(f'getting logs from {start_block} to {min(end_block, start_block + chunk_size)}')
        logs += event.get_logs(fromBlock=start_block, toBlock=min(end_block, start_block + chunk_size))
        start_block += chunk_size

    return logs

def switch_rpc(web3: Web3, key: str) -> int:
    """Switch RPC endpoint"""
    load_dotenv()
    rpc = os.getenv(key)
    web3.provider = Web3.HTTPProvider(rpc, {"timeout": 600})
    time.sleep(3)
    web3.provider = Web3.HTTPProvider(rpc, {"timeout": 600})
    time.sleep(3)
    print(f"Switched RPC to {key} (Chain ID: {web3.eth.chain_id}) {rpc}")
    return web3.eth.chain_id 