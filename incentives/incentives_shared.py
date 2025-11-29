"""Shared utilities for incentive tracking (YB and RSUP)"""
import time
import requests
import logging

logger = logging.getLogger(__name__)

WEEK = 7 * 24 * 60 * 60  # 7 days in seconds

def get_periods():
    """Get current and next period timestamps"""
    current_time = int(time.time())
    current_period = int(current_time / WEEK) * WEEK
    next_period = current_period + WEEK
    return current_period, next_period

def get_token_price(token_address):
    """Get token price from DeFiLlama API"""
    try:
        response = requests.get(
            f"https://coins.llama.fi/prices/current/ethereum:{token_address}",
            timeout=10
        )
        if response.status_code != 200:
            logger.warning(
                "Token price request failed for %s with status %s",
                token_address,
                response.status_code
            )
            return None

        data = response.json()
        return data['coins'][f'ethereum:{token_address}']['price']
    except Exception as e:
        logger.error(f"Error fetching token price for {token_address}: {str(e)}")
        return None

def get_bias(slope: int, end: int, current_period: int) -> int:
    """Calculate bias from slope and end time"""
    if end <= current_period:
        return 0
    return slope * (end - current_period)
