# SQLite migration

Status: harvest listener implementation and isolated rehearsal complete; production cutover pending. Curve and Resupply still require their own ports and notification safeguards. Do not deploy this branch as a complete replacement for the running listener bundle yet.

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
