"""Retry transient listener failures and stop on inconsistent saved state."""

import logging
import sqlite3
import time

from requests import RequestException
from web3.exceptions import Web3Exception

logger = logging.getLogger(__name__)


class FatalError(RuntimeError):
    """Operator action is required; service managers must not retry exit 78."""


class ChainChanged(RuntimeError):
    """Discard this observation and try the next head."""


def transient(error):
    if isinstance(error, (FileNotFoundError, PermissionError)):
        return False
    if isinstance(error, sqlite3.OperationalError):
        return (getattr(error, 'sqlite_errorcode', 0) & 0xff) in (sqlite3.SQLITE_BUSY, sqlite3.SQLITE_LOCKED)
    return (isinstance(error, (ChainChanged, OSError, RequestException, Web3Exception))
            or (isinstance(error, ValueError) and error.args and isinstance(error.args[0], dict)))


def attempt(callback):
    """Only transient observations retry; broken application state stays visible."""
    try:
        return callback()
    except Exception as error:
        if not transient(error):
            raise
        # Provider exception messages can contain credentials.
        logger.warning('Scan retry: %s', type(error).__name__)
        time.sleep(2)
        return None


def entrypoint(main):
    try:
        main()
    except FatalError as error:
        logger.error('Listener stopped: %s', error)
        raise SystemExit(78) from None
    except (RuntimeError, ValueError) as error:
        if transient(error):
            logger.warning('Listener startup retry (%s)', type(error).__name__)
            raise SystemExit(1) from None
        logger.error('Listener state requires investigation (%s)', type(error).__name__)
        raise SystemExit(78) from None
