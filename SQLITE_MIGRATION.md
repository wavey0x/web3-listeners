# SQLite migration

Status: harvest, Curve, retention, and both incentive implementations, isolated tests, and actual-data rehearsals passed; production cutover remains pending. Resupply still requires its DAO port and bundle lifecycle changes. Do not deploy this branch as a complete replacement for the running listener bundle yet.

The shared [migration plan](https://gist.wavey.info/RsZwJygIE49CML9UeqHdTn0l) and `server-backup` repository own the source export, baseline verification, shared database activation, and deployment sequence. All services must move to the same final frozen import. Never run SQLite writers while the public API still reads the older PostgreSQL copy.

## Runtime and configuration

Use Python 3.12 with a loaded SQLite library containing the WAL-reset fix: 3.51.3 or newer, or the supported 3.50.7 / 3.44.6 maintenance releases. Startup checks the library loaded by Python, not the command-line SQLite tool. Tests and the real-data rehearsal ran on Python 3.12.14 / SQLite 3.53.1.

`requirements.lock` pins application dependencies to the currently deployed listener versions, including Web3 6.15.1. Install with `uv pip sync --require-hashes requirements.lock` into a fresh environment built from the approved Python runtime. PostgreSQL's driver remains in this interim lock because the other workers still use it; remove it only after those ports are complete.

Set the absolute `YEARN_DB_PATH`, the final import's `YEARN_IMPORT_SHA256`, and the existing `WEB3_PROVIDER_URI`. Harvest indexing has no `DATABASE_URI` fallback. The database must already exist, have the expected schema/import identity, and be explicitly ready. Production writers require WAL and use foreign keys, full synchronous commits, a five-second busy timeout, and at most three attempts of a database-only transaction. Reads are query-only.

## Harvest boundary adoption

Run the shared baseline verification first. While the imported copy is inactive and alerts remain disabled, explicitly prepare the harvest state and adopt its boundaries:

```sh
python data_fetchers/ll_harvests.py --prepare-import
python data_fetchers/ll_harvests.py --adopt-boundaries
```

These commands require an inactive `data_verified` import. They do not overwrite existing harvest state. Normal startup never creates tables or guesses a missing checkpoint. If an adoption is interrupted, inspect its recorded per-compounder results and adopt only the remaining boundaries through the migration procedure; do not drop already adopted state automatically.

For each compounder, adoption reads every imported row in its last stored block and checks that finalized block's events and transaction receipts. Imported float-derived amounts remain unchanged. Any missing event in that boundary block is added using the exact raw amount. Any unmatched source row stops adoption for reconciliation. An empty history explicitly starts at the previous application's configured initial block. Preserve the boundary report with the private import evidence.

Normal indexing scans finalized Ethereum mainnet blocks in bounded ranges. Event records, deduplication identities, and the next block are committed together, including empty ranges. RPC calls occur before the write transaction. A failed RPC or write leaves the checkpoint unchanged. A changed checkpoint hash pauses indexing for reconciliation; it does not silently rewind. Event identity uses chain, contract, transaction, and receipt-local log position. Existing harvest uniqueness rules are preserved.

The shared activation step is deliberately separate and remains pending. After activation, the existing service entry point runs the listener; `--once` runs one bounded pass. No historical alert can be emitted by this module: it has no notification transport or import.

## Verification

```sh
python -m unittest discover -s tests -v
```

Tests cover exact amounts, multiple events in one block, empty ranges and restart, duplicate processing, interrupted transactions, failed RPC, checkpoint races, changed block hashes, boundary reconciliation, missing/unready imports, runtime checks, and live WAL reopening without an existing writer connection.

The 10 September rehearsal adopted all three actual imported boundaries successfully, retained all 6,377 harvest rows, ran a bounded scan, and passed SQLite integrity checking. No notification module was loaded. This was an isolated copy, not the final production import; activation, production concurrency, backup/restore, and service cutover checks remain required.

## Curve votes and public notification control

Curve now copies and retains existing votes, validates the complete last stored block, and adds only confirmed missing boundary events. Historical amounts, names, and aliases remain unchanged. Existing duplicate source rows are retained and reported. New votes use exact decimal calculations with the original 18-place numeric rounding and a separate stable event identity. The local Prisma alias edit is preserved.

Run `--prepare-import` and then `--adopt-boundary` on `data_fetchers/curve_gauge_votes.py` against the inactive imported copy. Normal startup does neither. The listener scans finalized mainnet blocks and commits votes, event identities, notification decisions, and scan progress together. A failed insert or RPC cannot advance the range or send a notice. Empty ranges advance progress. A changed saved block hash pauses the listener for reconciliation.

`notifications.py` owns small persistent stream and decision tables. Both the large-vote and unknown-gauge paths pass through them. Large-vote notices retain the existing limit of one per block. Stream, stable event identity, alert kind, and logical destination define a decision; message content cannot turn an old decision into a new notification. These tables are not a delivery queue.

Imported streams start disabled. Each process startup advances its permanent notification floor to the latest observed chain head and begins a new session. Events at or below that floor remain silent, covering history and outages; stale sessions cannot commit or send. A future decision is claimed with the data, then durably marked attempted before the HTTP call. A timeout or crash never causes an automatic replay. An alert can be missed after a claim; avoiding repeated public announcements is the chosen tradeoff. Telegram uses one request with redirects and transport retries disabled, and token-bearing exception details are not logged.

Public transport additionally requires a machine-local activation file at `/var/lib/yearn-notifications/curve.allow`, or under the explicitly configured absolute `YEARN_NOTIFICATION_ALLOW_DIR`. Both that directory and file must be owned by root, not writable by group/others, and not symlinks. The file contains the current `YEARN_IMPORT_SHA256` followed by an optional newline and must be readable by the service account. Keep these activation files out of backups and ordinary configuration restore; a database backup alone cannot grant public-send permission on a replacement host.

During the coordinated cutover, keep the shared alert flag and stream disabled until validation passes. After silent catch-up, the activation procedure creates the root-controlled file, enables shared notification eligibility, and invokes `--enable-alerts`. That command requires a validated scan through the current finalized head and sets a new floor at the latest head, so events already present at activation are ineligible even if they have not finalized yet. Future eligible events may be announced after finalization. `--mute-alerts` disables the stream and invalidates its current session. A machine-level hold must also remove its activation file and stop the service and its activation sources; removing a file does not terminate an already running process or undo a request already in flight.

The 10 September Curve rehearsal retained all **49,742** imported votes exactly, adopted the checked boundary, and stored **two** newer votes with **zero** transport calls. Integrity passed, and Wavey API returned both new records with the expected amounts and metadata. The listener suite now has **33** passing tests covering the harvest and Curve paths, including notification failure/restart behavior and the machine permission guard. Linux application tests, combined workload checks, the final frozen import, and production activation still remain.

## Retention weights

Run `--prepare-import` and then `--adopt-boundary` on `data_fetchers/resupply_retention.py` against the inactive import. Adoption checks every source record in its last stored block against finalized chain events. It retains existing records, including legacy null log indexes and duplicate rows, and inserts only confirmed missing events. Unmatched source data stops adoption.

Raw old/new weights and their signed difference remain exact integer text, preserving the source's 78-digit capacity. Arithmetic uses Python integers and decimal formatting. Data, stable event identities, notification decisions, and the next block commit together. Completed empty ranges are retained; catch-up proceeds in bounded ranges without sleeping between them. The existing deployment-block notification exclusion and optional total-supply display remain.

The `retention` stream uses the same durable suppression, latest-head startup floor, and single-attempt transport as Curve, retaining the `RESUPPLY_ALERTS` destination. Its root-controlled activation file is `retention.allow`. Standalone `--enable-alerts` requires validated silent catch-up through the finalized head; `--mute-alerts` invalidates the session. The bundle's `main()` entry point performs no argument parsing, and imports perform no database, RPC, or notification work.

The 10 September rehearsal retained all **169** imported weight records exactly, validated the boundary, completed a further 5,000-block range, and passed integrity checking with **zero** transport calls. The listener suite now has **45** passing tests. Retention coverage includes 77-digit values and negative differences, atomic failure, legacy duplicates, silent outage recovery, uncertain delivery without replay, and activation boundaries. Production cutover remains pending.

## RSUP and YieldBasis incentives

`incentives/sqlite_worker.py` shares period persistence and notification control between the two existing protocol modules. The protocol modules retain their calculations and message content. Imports perform no database, RPC, or bot initialization. The local RSUP corrections are preserved: the Prisma address, multisig transfer filters, deduction of Prisma votes from Votemarket totals, and Votemarket-specific report display.

Prepare the shared incentive state **once** through either module's `--prepare-import`. Then run `--adopt-boundary` separately for `incentives/rsup_incentives.py` and `incentives/yb_incentives.py`. Each adoption checks the last imported week's chain metadata and receipts, retains all saved observations without calling the price/calculation functions for them, and inserts only missing transfers. Unmatched source rows stop adoption. Each protocol has its own completed-period checkpoint and notification stream. Empty histories explicitly use their configured start period.

Normal scans wait for the entire week and its calculation block to finalize. RPC log requests are bounded, period boundaries exclude adjacent weeks, and all calculations finish before a short write transaction. Incentive rows, stable event identities, notification decisions, and completed-week progress commit together. Empty weeks also advance. A failed gauge lookup, malformed transfer, database write, or changed block hash leaves the period unfinished; it cannot silently publish partial gauge data or move past a failed week. An unavailable optional token price still produces null efficiency values, matching the existing behavior. Existing floating-point and JSON representations are preserved.

YieldBasis retains its one-report-per-transaction rule. RSUP retains individual qualifying transfer identity. All queries and identities scope the protocol. Notifications retain the existing protocol chat-key and development overrides, use the common single-attempt transport, and require their own `rsup-incentives.allow` or `yb-incentives.allow` file. Both streams start muted; startup and activation floors suppress pre-existing transfers even when their weekly report becomes calculable later.

The existing manual `scripts/backfill_rsup_incentives.py` is ported to SQLite and remains **dry-run by default**. It contains no notification path. Explicit `--apply` recalculates the intended corrections and applies the batch atomically only if its inspected input rows are unchanged. The migration does not run `--apply` or rewrite historical observations as part of import.

The 10 September rehearsal checked both actual last-week boundaries and the following completed week. All **61** imported reports remained exactly unchanged, including prices, calculations, and JSON text; neither week contained missing transfers. Both checkpoints advanced, integrity passed, and there were **zero** transport calls. The listener suite now has **60** passing tests, including protocol calculations, historical preservation without repricing, transaction grouping, empty weeks, rollback, configured routing, silent restarts, and manual-correction concurrency checks.
