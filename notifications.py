"""Best-effort alerts with stream mute/activation controls, without delivery history."""

from dataclasses import dataclass
import json
import logging
import os
from pathlib import Path
import re
import stat

import recovery

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class Pending:
    stream: str
    generation: int
    block: int
    destination: str


def prepare(connection):
    """Prepare an inactive import; live upgrades use the explicit migration."""
    manifest = json.loads(connection.execute("SELECT value FROM _migration_meta WHERE key='manifest'").fetchone()[0])
    if manifest.get('application_ready') or manifest.get('alerts_enabled') is not False:
        raise RuntimeError('Prepare notification state in an inactive, muted import')
    connection.execute("""CREATE TABLE IF NOT EXISTS notification_streams (
        stream TEXT PRIMARY KEY,enabled INTEGER NOT NULL CHECK(enabled IN (0,1)),
        floor_block INTEGER NOT NULL CHECK(floor_block>=0),floor_hash TEXT NOT NULL,
        generation INTEGER NOT NULL CHECK(generation>=0))""")
    connection.execute("INSERT INTO _migration_meta VALUES ('notification_schema_version','2') ON CONFLICT(key) DO UPDATE SET value='2'")
    recovery.prepare(connection)


def add_stream(connection, stream, floor_block, floor_hash):
    connection.execute('INSERT INTO notification_streams VALUES (?,0,?,?,0)', (stream, floor_block, floor_hash))


def state(connection, stream):
    version = connection.execute("SELECT value FROM _migration_meta WHERE key='notification_schema_version'").fetchone()
    if version is None or version[0] != '2':
        raise RuntimeError('Notification schema is missing or incompatible')
    row = connection.execute('SELECT * FROM notification_streams WHERE stream=?', (stream,)).fetchone()
    if row is None:
        raise RuntimeError('Notification stream has not been explicitly adopted')
    return dict(row)


def _advance_floor(connection, stream, block, block_hash):
    previous = state(connection, stream)
    if block >= previous['floor_block']:
        connection.execute('UPDATE notification_streams SET floor_block=?,floor_hash=? WHERE stream=?',
                           (block, block_hash, stream))


def start_session(store, stream, latest_block, latest_hash):
    """A restart suppresses everything already on chain, including outage events."""
    def start(connection):
        _advance_floor(connection, stream, latest_block, latest_hash)
        connection.execute('UPDATE notification_streams SET generation=generation+1 WHERE stream=?', (stream,))
        return state(connection, stream)
    current = store.write(start)
    logger.info('Notification session: stream=%s generation=%s enabled=%s floor_block=%s',
                stream,current['generation'],current['enabled'],current['floor_block'])
    return current['generation']


def enable(connection, stream, latest_block, latest_hash):
    """App-specific activation must first verify its data/state catch-up boundary."""
    if not _globally_enabled(connection):
        raise RuntimeError('Shared notification activation is still disabled')
    _advance_floor(connection, stream, latest_block, latest_hash)
    connection.execute('UPDATE notification_streams SET enabled=1 WHERE stream=?', (stream,))


def mute(connection, stream):
    state(connection, stream)
    connection.execute('UPDATE notification_streams SET enabled=0,generation=generation+1 WHERE stream=?', (stream,))


def _globally_enabled(connection):
    row = connection.execute("SELECT value FROM _migration_meta WHERE key='manifest'").fetchone()
    return bool(row and json.loads(row[0]).get('alerts_enabled') is True)


def eligible(connection, stream, generation, block):
    current = state(connection, stream)
    return (current['generation'] == generation and current['enabled']
            and block > current['floor_block'] and _globally_enabled(connection))


def pending(connection, stream, generation, block, destination):
    if generation != state(connection, stream)['generation']:
        raise RuntimeError('Notification session changed; stop the stale worker')
    if eligible(connection, stream, generation, block):
        return Pending(stream, generation, block, destination)
    return None


def dispatch(store, item, message, send):
    # Recheck controls after the data commit and immediately before transport.
    if not store.read(lambda c: eligible(c, item.stream, item.generation, item.block)):
        return False
    try:
        send(item.stream, item.destination, message)
    except Exception:
        logger.warning('Alert delivery failed: stream=%s destination=%s', item.stream, item.destination)
        return False
    return True


def require_permission(stream):
    """Machine-local root approval is deliberately absent from database backups."""
    directory = Path(os.environ.get('YEARN_NOTIFICATION_ALLOW_DIR', '/var/lib/yearn-notifications'))
    identity = os.environ.get('YEARN_IMPORT_SHA256', '')
    if not directory.is_absolute() or not re.fullmatch('[a-z0-9_-]+', stream) or not re.fullmatch('[0-9a-f]{64}', identity):
        raise RuntimeError('Notification activation configuration is invalid')
    path = directory / (stream + '.allow')
    try:
        parent, permit = directory.lstat(), path.lstat()
        if (not stat.S_ISDIR(parent.st_mode) or not stat.S_ISREG(permit.st_mode)
                or parent.st_uid != 0 or permit.st_uid != 0 or (parent.st_mode | permit.st_mode) & 0o022
                or path.read_text().strip() != identity):
            raise RuntimeError('Notification activation permission is invalid')
    except OSError:
        raise RuntimeError('Notification activation permission is missing') from None


def send_telegram(stream, destination, message):
    """One HTTP attempt, with no redirects or retries. Credentials are loaded lazily."""
    import requests
    from constants import CHAT_IDS
    require_permission(stream)
    token = os.environ.get('WAVEY_ALERTS_BOT_KEY')
    if not token or destination not in CHAT_IDS:
        raise RuntimeError('Notification credentials or logical destination are missing')
    try:
        with requests.Session() as session:
            session.mount('https://', requests.adapters.HTTPAdapter(max_retries=0))
            response = session.post('https://api.telegram.org/bot' + token + '/sendMessage',
                json=dict(chat_id=CHAT_IDS[destination], text=message, parse_mode='markdown', disable_web_page_preview=True),
                timeout=(5, 20), allow_redirects=False)
            response.raise_for_status()
            if response.status_code != 200 or response.json().get('ok') is not True:
                raise RuntimeError('Telegram did not confirm acceptance')
    except Exception:
        # HTTP exception text can contain the token-bearing URL.
        raise RuntimeError('Telegram delivery was not confirmed; retry is disabled') from None
