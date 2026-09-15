"""One finalized fallback per stream; callers own their ordinary SQL rollback."""

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


def hex_value(value):
    return (value if isinstance(value, str) else value.hex()).lower().removeprefix('0x')


def prepare(c):
    c.execute('''CREATE TABLE IF NOT EXISTS recovery_points (
        stream TEXT PRIMARY KEY, block INTEGER NOT NULL, block_hash TEXT NOT NULL,
        position INTEGER NOT NULL)''')


def point(c, stream):
    row = c.execute('SELECT * FROM recovery_points WHERE stream=?', (stream,)).fetchone()
    return dict(row) if row is not None else None


def save(c, stream, block, block_hash, position):
    c.execute('''INSERT INTO recovery_points VALUES (?,?,?,?) ON CONFLICT(stream)
        DO UPDATE SET block=excluded.block,block_hash=excluded.block_hash,position=excluded.position''',
        (stream, block, block_hash, position))


def verify(w3, saved):
    if saved is None:
        raise FatalError('Recovery point missing; repair and initialize the checkpoint explicitly')
    if hex_value(w3.eth.get_block(saved['block'])['hash']) != saved['block_hash']:
        raise FatalError('Finalized recovery point changed; operator reconciliation required')


def block_reorg(store, w3, stream, previous, checkpoint):
    """Return a verified rollback point on mismatch, otherwise advance the fallback."""
    saved = store.read(lambda c: point(c, stream))
    number = previous['next_block'] - 1
    if saved is not None:
        verify(w3, saved)
        if (not previous['initial_block'] - 1 <= saved['block'] <= number
                or saved['position'] != saved['block'] + 1):
            raise FatalError('Recovery point is outside the processed import range')
    if hex_value(w3.eth.get_block(number)['hash']) != previous['previous_hash']:
        verify(w3, saved)
        if saved['block'] < previous['initial_block'] - 1 or saved['block'] >= previous['next_block']:
            raise FatalError('Recovery point is outside the processed import range')
        return saved
    finalized = w3.eth.get_block('finalized')['number']
    block = min(number, finalized)
    if block < previous['initial_block'] - 1:
        raise FatalError('Imported boundary must already be finalized')
    if saved is None or block > saved['block']:
        block_hash = hex_value(w3.eth.get_block(block)['hash'])
        if hex_value(w3.eth.get_block(number)['hash']) != previous['previous_hash']:
            raise ChainChanged('Head changed while advancing recovery point')
        def commit(c):
            if checkpoint(c) != previous or point(c, stream) != saved:
                raise ChainChanged('Checkpoint advanced concurrently')
            save(c, stream, block, block_hash, block + 1)
        store.write(commit)
    return None


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
