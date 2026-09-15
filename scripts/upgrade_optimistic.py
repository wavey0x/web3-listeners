"""Run once with notification workers stopped and all streams muted."""

from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from dotenv import load_dotenv
from sqlite_store import Store
import recovery


def upgrade(store):
    def migrate(c):
        version=c.execute("SELECT value FROM _migration_meta WHERE key='notification_schema_version'").fetchone()
        if version is None or version[0] not in ('1','2'):
            raise RuntimeError('Unsupported notification schema for this upgrade')
        if c.execute('SELECT count(*) FROM notification_streams WHERE enabled!=0').fetchone()[0]:
            raise RuntimeError('Mute notification streams before upgrading')
        recovery.prepare(c)
        c.execute('''CREATE TABLE IF NOT EXISTS incentive_calculations (
            protocol TEXT PRIMARY KEY,block INTEGER NOT NULL,block_hash TEXT NOT NULL)''')
        c.execute('DROP TABLE IF EXISTS notification_blocks')
        c.execute('DROP TABLE IF EXISTS notification_decisions')
        c.execute("UPDATE _migration_meta SET value='2' WHERE key='notification_schema_version'")
    store.write(migrate)


if __name__ == '__main__':
    load_dotenv()
    upgrade(Store.from_env())
