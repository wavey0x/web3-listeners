"""
Utility functions for web3 interactions
"""
from .abi import load_abi
from .web3_utils import (
    block_to_date,
    closest_block_after_timestamp,
    closest_block_before_timestamp,
    get_block_timestamp,
    timestamp_to_date_string,
    timestamp_to_string,
    contract_creation_block,
    get_logs_chunked,
    switch_rpc,
    ZERO_ADDRESS,
    DAY,
    WEEK
) 