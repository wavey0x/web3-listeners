"""Persistent suppression and at most one public send attempt. There is no queue."""

from dataclasses import dataclass
import hashlib
import json
import logging
import os
from pathlib import Path
import re
import stat

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class Claim:
    stream: str
    event_key: str
    kind: str
    destination: str

    def key(self):
        return (self.stream, self.event_key, self.kind, self.destination)


def prepare(connection):
    """Called only by explicit inactive-import preparation, never normal startup."""
    manifest = json.loads(connection.execute("SELECT value FROM _migration_meta WHERE key='manifest'").fetchone()[0])
    if manifest.get('application_ready') or manifest.get('alerts_enabled') is not False:
        raise RuntimeError('Prepare notification state in an inactive, muted import')
    version = connection.execute("SELECT value FROM _migration_meta WHERE key='notification_schema_version'").fetchone()
    if version:
        if version[0] != '1':
            raise RuntimeError('Unsupported notification schema version')
        connection.execute('SELECT stream,enabled,floor_block,floor_hash,generation FROM notification_streams LIMIT 0')
        connection.execute('SELECT status,message_hash,generation FROM notification_decisions LIMIT 0')
        connection.execute('SELECT event_key FROM notification_blocks LIMIT 0')
        return
    connection.execute('''CREATE TABLE notification_streams (
        stream TEXT PRIMARY KEY,enabled INTEGER NOT NULL CHECK(enabled IN (0,1)),
        floor_block INTEGER NOT NULL CHECK(floor_block>=0),floor_hash TEXT NOT NULL,
        generation INTEGER NOT NULL CHECK(generation>=0))''')
    connection.execute('''CREATE TABLE notification_decisions (
        stream TEXT NOT NULL,event_key TEXT NOT NULL,kind TEXT NOT NULL,destination TEXT NOT NULL,
        block INTEGER NOT NULL,generation INTEGER NOT NULL,message_hash TEXT NOT NULL,
        status TEXT NOT NULL CHECK(status IN ('suppressed','claimed','attempted','delivered','uncertain')),
        PRIMARY KEY(stream,event_key,kind,destination),FOREIGN KEY(stream) REFERENCES notification_streams(stream))''')
    connection.execute('''CREATE TABLE notification_blocks (
        stream TEXT NOT NULL,kind TEXT NOT NULL,destination TEXT NOT NULL,block INTEGER NOT NULL,event_key TEXT NOT NULL,
        PRIMARY KEY(stream,kind,destination,block),FOREIGN KEY(stream) REFERENCES notification_streams(stream))''')
    connection.execute("INSERT INTO _migration_meta VALUES ('notification_schema_version','1')")


def add_stream(connection, stream, floor_block, floor_hash):
    connection.execute('INSERT INTO notification_streams VALUES (?,0,?,?,0)', (stream, floor_block, floor_hash))


def state(connection, stream):
    version = connection.execute("SELECT value FROM _migration_meta WHERE key='notification_schema_version'").fetchone()
    if version is None or version[0] != '1':
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


def decide(connection, stream, generation, event_key, block, kind, destination, message, *, once_per_block=False):
    """Call inside the same transaction that stores the associated application data."""
    current = state(connection, stream)
    if generation != current['generation']:
        raise RuntimeError('Notification session changed; stop the stale worker')
    claim = Claim(stream, event_key, kind, destination)
    if connection.execute('''SELECT 1 FROM notification_decisions
        WHERE stream=? AND event_key=? AND kind=? AND destination=?''', claim.key()).fetchone():
        return None
    eligible = bool(current['enabled'] and block > current['floor_block'] and _globally_enabled(connection))
    if eligible and once_per_block:
        eligible = bool(connection.execute('''INSERT INTO notification_blocks VALUES (?,?,?,?,?)
            ON CONFLICT(stream,kind,destination,block) DO NOTHING''',
            (stream, kind, destination, block, event_key)).rowcount)
    connection.execute('INSERT INTO notification_decisions VALUES (?,?,?,?,?,?,?,?)',
        (*claim.key(), block, generation, hashlib.sha256(message.encode()).hexdigest(), 'claimed' if eligible else 'suppressed'))
    return claim if eligible else None


def dispatch(store, claim, message, send):
    """Consume a committed claim before calling the transport; never retry a send."""
    def begin(connection):
        row = connection.execute('''SELECT * FROM notification_decisions
            WHERE stream=? AND event_key=? AND kind=? AND destination=?''', claim.key()).fetchone()
        if row is None or row['status'] != 'claimed':
            return False
        if row['message_hash'] != hashlib.sha256(message.encode()).hexdigest():
            raise RuntimeError('Notification content differs from its committed decision')
        current = state(connection, claim.stream)
        eligible = (current['enabled'] and row['generation'] == current['generation']
                    and row['block'] > current['floor_block'] and _globally_enabled(connection))
        connection.execute('''UPDATE notification_decisions SET status=?
            WHERE stream=? AND event_key=? AND kind=? AND destination=?''',
            ('attempted' if eligible else 'suppressed', *claim.key()))
        return eligible
    if not store.write(begin):
        return False
    try:
        send(claim.stream, claim.destination, message)
    except BaseException:
        # If this update also fails, the durable 'attempted' state still forbids replay.
        store.write(lambda connection: connection.execute('''UPDATE notification_decisions SET status='uncertain'
            WHERE stream=? AND event_key=? AND kind=? AND destination=?''', claim.key()))
        logger.error('Notification uncertain: stream=%s kind=%s event=%s',claim.stream,claim.kind,claim.event_key)
        raise RuntimeError('Notification delivery outcome is uncertain; automatic retry is disabled') from None
    store.write(lambda connection: connection.execute('''UPDATE notification_decisions SET status='delivered'
        WHERE stream=? AND event_key=? AND kind=? AND destination=?''', claim.key()))
    logger.info('Notification delivered: stream=%s kind=%s event=%s',claim.stream,claim.kind,claim.event_key)
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
