# SQLite listeners

The listeners share `/var/lib/yearn/yearn.sqlite3` with the Wavey API and Open Data
Scripts. Application records and scan progress commit together. Backups retain the
installed code, runtime, database and configuration; restored workers stay held.

## Behavior

- Curve votes, retention and DAO follow `latest`, polling every two seconds when
  idle. Alerts are optimistic: duplicates and subsequently orphaned events are
  acceptable. Telegram delivery is best effort; failures log and indexing continues.
- Curve indexes every gauge vote but emits at most one large-vote alert per block.
  Batch voting can produce many qualifying events in one transaction. This limit
  is local to the scanned batch; it needs no delivery history or schema change.
- RSUP and YieldBasis reports run as soon as a completed week and its calculation
  block exist. They retain that block's hash to detect stale calculations.
- Liquid-locker harvests index finalized blocks and have no notification path.
  The duplicate YBS listener and destructive recreate scripts remain retired.
- Alerts send after the data commit, with no delivery ledger, queue or exactly-once
  guarantee. A crash after commit can lose an alert. Stream mute controls, local
  permits and silent restart/catch-up remain in place. DAO retains only the current
  condition/reminder state needed to avoid announcing the same condition every poll.

## Chain changes

Live alert scans do not query `finalized` or maintain recovery points. They check
the saved checkpoint and validate logs, receipts and block hashes collected in one
scan. Temporary RPC errors and changing collections retry without partial writes.
If a processed block or an incentive calculation block changes, the affected worker
stops for explicit data reconciliation. It does not automatically rewind or replay.
Alerts already sent for orphaned blocks cannot be withdrawn; replacement events
may be missed until the checkpoint and affected rows are repaired. Event identity
remains for data correctness, not notification deduplication.

The Resupply bundle fails visibly if any worker cannot continue. Configure
`RestartPreventExitStatus=78` alongside `Restart=on-failure` on notification units.
Check actual checkpoints/logs, not just whether the parent process is running.
Databases upgraded by the earlier release may retain an unused `recovery_points`
table. No live schema change or table cleanup is required for this release.

## Runtime and upgrade

Use Python 3.12 and a SQLite runtime with the WAL-reset fix: 3.51.3+, 3.50.7–3.50.x,
or 3.44.6–3.44.x. Install dependencies from `requirements.lock` with hash checking.
Set `YEARN_DB_PATH`, `YEARN_IMPORT_SHA256`, `WEB3_PROVIDER_URI`, and explicitly
select `YEARN_RESUPPLY_WORKERS=rsup-incentives,yb-incentives,dao,retention`.
Existing credentials, routing and permissions remain configuration inputs.

The optimistic upgrade changes notification schema 1 to 2 and removes the old
`notification_decisions` and `notification_blocks` tables. Already upgraded schema 2
databases need no migration for removal of automatic recovery:

1. Stop Curve and the Resupply bundle. Take a consistent database backup and retain
   the previous release/unit configuration. Remove local notification permits and
   mute streams with the old release's native commands.
2. Install the new release and run `python scripts/upgrade_optimistic.py` with the
   service environment. It refuses enabled streams and is safe to repeat while muted.
3. If a checkpoint already disagrees with chain history, reconcile it before
   startup. Never overwrite its hash to skip a replacement block. Verify the exact
   checkpoint and affected data, then repair rows and progress together.
4. Catch up silently with the native `--once` commands, validate records and DAO
   polling state, then enable future alerts with the native activation commands.
   Restart the reviewed workers with exit-78 handling and check each one's progress.

Old code requires schema 1. Rollback therefore needs its compatible database copy;
never replace the whole shared database over other services' intervening writes.

## Controls and verification

`data_fetchers/curve_gauge_votes.py`, `data_fetchers/resupply_retention.py`,
`data_fetchers/resupply_dao.py`, and both `incentives/*_incentives.py` expose
`--once`, `--mute-alerts`, and `--enable-alerts`. Activation requires validated
catch-up and a matching root-owned permit at `/var/lib/yearn-notifications/<stream>.allow`.
The stream names are `curve`, `retention`, `dao`, `rsup-incentives`, `yb-incentives`.
`python resupply.py --check-config` checks selected prepared state without starting workers.

New imports use the existing explicit prepare/adopt commands while the import is
inactive and muted. They validate finalized boundaries and preserve imported values,
including legacy duplicates/null log indexes. Normal startup does not create an
empty database or guess an import boundary. Shared import identity, WAL and full
synchronous commits remain required.

Run `python -m unittest discover -s tests -v`. Coverage includes immediate alerts
without finalized RPC reads, atomic writes, RPC/send failure, visible checkpoint
and calculation-block mismatch, stream isolation, old observations and local permits.
Rehearse real-data upgrades on a private database copy with sending disabled.
