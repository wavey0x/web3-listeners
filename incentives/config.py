"""Configuration for incentives fetchers."""

import os
from constants import CHAT_IDS

# Start timestamps (UTC seconds) for incentives collection per protocol.
# Set to desired period start for fresh collection.
INCENTIVE_START_TIMESTAMPS = {
    "yieldbasis": 1761177600,  # 2025-10-23 00:00 UTC (about a month back from current period)
    "resupply": 1743033600,    # 2025-03-26 00:00 UTC
}

# Telegram routing
# Set INCENTIVES_ENV=dev in your environment (or .env) to route alerts to DEV_CHAT_KEY.
INCENTIVES_ENV = os.getenv("INCENTIVES_ENV", "prod").lower()
DEV_MODE = INCENTIVES_ENV == "dev"
ALERT_CHAT_KEYS = {
    "yieldbasis": os.getenv("YB_ALERTS_CHAT_KEY", "WAVEY_ALERTS"),
    "resupply": os.getenv("RSUP_ALERTS_CHAT_KEY", "WAVEY_ALERTS"),
}
DEV_CHAT_KEY = "WAVEY_ALERTS"

def resolve_chat_id(protocol: str):
    """Return chat_id and key for the given protocol, honoring dev mode overrides."""
    key = DEV_CHAT_KEY if DEV_MODE else ALERT_CHAT_KEYS.get(protocol, DEV_CHAT_KEY)
    return CHAT_IDS.get(key), key
