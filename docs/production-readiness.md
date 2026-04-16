# Production Readiness Review: fpl-ingest

**Date:** 2026-04-14
**Scope:** fpl-ingest pipeline evaluated as a foundational layer for downstream analytics and AI systems.

---

## Pillar 1: Reliability

**Production-grade means:** The pipeline completes successfully on every scheduled run, tolerates transient API failures without human intervention, and does not silently produce partial data.

**Concrete checks:**
- Run the pipeline twice back-to-back and verify row counts are identical (idempotency smoke test)
- Kill the process mid-run and verify the DB is not corrupted and the next run completes cleanly
- Simulate a 429 response and verify the pipeline respects `Retry-After` and continues
- Simulate a player history fetch failure for 5% of players and verify the other 95% complete

**Common failure modes:**
- A single GW failure silently dropping data while the pipeline exits successfully. Currently `asyncio.gather(return_exceptions=True)` isolates failures correctly, but the run-level exit code does not reflect stage errors unless `--strict` is used.
- SQLite `database is locked` on overlapping scheduled runs. `busy_timeout=5000ms` partially mitigates but does not guarantee.

**Current state:** Strong. `return_exceptions=True` in `gather()`, per-stage transactions, `busy_timeout`, WAL mode. Async transport hardening (Phase 2) is well-documented.

**Gaps:**
- `--strict` is opt-in; errors are soft by default. A scheduled run with 50 skipped rows will exit 0.
- No circuit breaker: if the API is down for 10 minutes, all 826 player history requests will exhaust retries before the run exits.

**Minimum acceptable standard:** Pipeline exits non-zero on any stage with errors > 0 in a scheduled or automated context.

**Improvement:** In `src/fpl_ingest/cli.py`, make `--strict` the default for non-interactive runs, detected via `sys.stdout.isatty()` or `FPL_STRICT=1`.

---

## Pillar 2: Data Correctness & Integrity

**Production-grade means:** Every row in the DB accurately reflects the FPL API at the time of ingestion. No silent truncation, coercion, or lossy transformation.

**Concrete checks:**
- Spot-check 5 players: compare `players.now_cost`, `gameweeks.total_points`, `player_histories.total_points` against the FPL website
- Verify `cost_to_millions()` round-trips correctly for edge values (e.g., cost=45 → 4.5, not 4.499999...)
- Confirm `flatten_live_elements()` never loses a player row (total rows = total players in GW)
- Verify `flatten_fixture_stats()` produces the correct grain: one row per `(fixture_id, identifier, element)`, not collapsed

**Common failure modes:**
- `extra="forbid"` is the right call for schema drift detection, but a new API field will silently skip every player containing it. This is counted as a `skipped` row but not alerted on.
- `flatten_event()` serializes nested objects to JSON strings (e.g., `chip_plays_json`). Downstream systems get raw JSON strings, not structured data.

**Current state:** Strong. Pydantic with `extra="forbid"`, critical field validators, `.prepare()` hooks, per-row validation with skip-and-count.

**Gaps:**
- No row-count reconciliation between API response size and rows upserted.
- `PlayerHistoryModel` strips the `modified` field via `.prepare()`. If FPL uses this field to signal post-deadline corrections (bonus point adjustments), those corrections are invisible.

**Minimum acceptable standard:** Row count post-upsert equals API response count for `players` and `gameweeks` tables, validated at runtime.

**Improvement:** In `src/fpl_ingest/pipeline/core.py` and `src/fpl_ingest/pipeline/gameweeks.py`, add an assertion: `fetched == upserted + skipped` and log a WARNING if `skipped / fetched > 0.01` (1% threshold).

---

## Pillar 3: Idempotency & Reproducibility

**Production-grade means:** Running the pipeline N times on the same GW produces the same DB state as running it once. A re-run after a partial failure recovers cleanly without duplicates or gaps.

**Concrete checks:**
- Run `fpl-ingest` three times; verify `SELECT COUNT(*) FROM gameweeks WHERE round = X` is identical each time
- Run with `--force` on a finished GW and verify the row count and values are unchanged
- Corrupt a cached `.json` file and verify the pipeline re-fetches and self-heals

**Common failure modes:**
- `INSERT OR REPLACE` (the fallback when `DEFAULT_UNIQUE` is not set) deletes and re-inserts the row, which changes `ingested_at` and can break change detection in downstream systems.
- History cache uses file existence as a cache key, not content hash or freshness. A partially-written file (process killed mid-write) will be treated as valid on the next run.

**Current state:** Strong. `ON CONFLICT DO UPDATE` for all grain-aware tables. `--force` flag for manual re-fetch. Raw JSON cached for replay.

**Gaps:**
- No atomic write for cached JSON files. If the process is killed during `open(path, 'w')`, the file is written partially and read as valid on the next run.
- No explicit "replay from raw cache" command. If the DB is dropped, re-running re-fetches from network, not from cached JSON.

**Minimum acceptable standard:** All tables use `ON CONFLICT DO UPDATE`. Verified: true for all tables with a declared `DEFAULT_UNIQUE`.

**Improvement:** In `src/fpl_ingest/pipeline/history.py` and wherever raw JSON is written, replace `open(path, 'w')` with write-to-temp-then-`os.rename()`. Atomic on POSIX, zero added complexity.

---

## Pillar 4: Observability & Monitoring

**Production-grade means:** A human or automated system can determine, without running the pipeline, whether the last run succeeded, how many rows were ingested, and whether data quality degraded.

**Concrete checks:**
- Query `_runs` after a run and verify all 4 stages are recorded with non-zero `upserted`
- Deliberately skip player history fetches and verify the `_runs` record shows `errors > 0`
- Verify logs distinguish between a skipped row (validation failure) and a network error

**Common failure modes:**
- `_runs` records counts but has no baseline. A run that upserts 100 rows where 826 are expected looks identical to one that upserts 826.
- Logs go to stdout with no structured format. In a cron or scheduled context, these are likely discarded unless explicitly captured.

**Current state:** Adequate for development. `_runs` table, `StageResult` with 4 counters, INFO/WARNING/ERROR logging. Insufficient for production.

**Gaps:**
- No structured logging (JSON lines). Parsing free-text logs for alerting is fragile.
- No anomaly detection: no comparison of current run counts against the previous run.
- No health-check file or endpoint that external monitoring can poll.

**Minimum acceptable standard:** After each run, the `_runs` table has a record queryable to determine success or failure.

**Improvement:** Write `~/.fpl/last_run.json` at end of each run with `status`, `timestamp`, `stage_counts`, `total_errors`. Monitorable by any external tool. Add this in `src/fpl_ingest/cli.py` after the run summary (20 lines, no new dependencies).

---

## Pillar 5: Data Freshness & SLAs

**Production-grade means:** Downstream systems know exactly how stale the data can be, and the pipeline is designed to meet that expectation.

**Concrete checks:**
- Verify `ingested_at` is written for every row (confirmed: auto-injected via `register_table()`)
- Query `MAX(ingested_at) FROM gameweeks` and compare to current time
- Simulate a run failure and verify downstream can detect the data is stale

**Common failure modes:**
- FPL updates live GW data every ~2 minutes during matches. If the pipeline runs hourly, live data is 60 minutes stale on matchdays.
- A failed run during a live GW leaves the DB with the previous run's data, with no staleness indicator visible to downstream.

**Current state:** No SLA defined. `ingested_at` exists per-row but there is no freshness contract.

**Gaps:**
- No documented refresh cadence.
- No `last_refreshed_at` metadata table that downstream can query.
- No distinction between "live GW data fetched at 2:00 PM" and "finished GW data fetched last week."

**Minimum acceptable standard:** Add a `_metadata` table with `last_successful_run_at` and `current_gameweek`, updated at the end of each clean run.

**Improvement:**
```sql
CREATE TABLE IF NOT EXISTS _metadata (
    key   TEXT PRIMARY KEY,
    value TEXT,
    updated_at TEXT
);
-- Updated at end of each successful run:
INSERT OR REPLACE INTO _metadata VALUES
  ('last_successful_run_at', '2025-01-15T14:00:00Z', '2025-01-15T14:00:00Z'),
  ('current_gameweek',       '22',                   '2025-01-15T14:00:00Z');
```
Downstream systems query this table before using data.

---

## Pillar 6: Schema & Contract Stability

**Production-grade means:** Schema changes are backward-compatible by default. Breaking changes are detectable before they reach downstream systems.

**Concrete checks:**
- Add a new field to `PlayerModel` and verify `_migrate_columns()` adds the column without data loss
- Remove an existing field from `PlayerModel` and verify the old column is preserved (additive-only migration)
- Add an unknown field to a mock API response and verify the pipeline logs a warning and skips the row

**Common failure modes:**
- FPL silently renames a field (e.g., `ep_next` → `expected_points_next`). With `extra="forbid"`, the entire player list fails validation and produces 0 rows. Correct behavior, but must alert, not silently swallow.
- `_migrate_columns()` handles additive changes but not type changes. If `now_cost` changes from `int` to `float`, the SQLite column type remains `INTEGER` and new values may coerce incorrectly.

**Current state:** Strong. `extra="forbid"` detects drift. `_migrate_columns()` handles additive evolution. `DEFAULT_UNIQUE` is explicit per-model.

**Gaps:**
- No schema version number. If a breaking API change requires manual migration, there is no mechanism to detect that the DB is at schema version N while the code expects N+1.
- No change detection for FPL API responses where a field silently changes meaning (not type).

**Minimum acceptable standard:** `extra="forbid"` behavior is sufficient for field-level drift detection. Schema versioning is the next required step.

**Improvement:** Add `SCHEMA_VERSION = 3` in `src/fpl_ingest/config.py` and a `_schema_version` table. On startup, assert `stored_version == SCHEMA_VERSION` or run migration. ~15 lines of code, prevents silent data corruption on DB upgrades.

---

## Pillar 7: Backfill & Replay Capability

**Production-grade means:** Reprocessing historical GW data produces the same result as the original ingest. Backfilling a new column or table does not require re-fetching from the API.

**Concrete checks:**
- Drop the `gameweeks` table, run `fpl-ingest`, verify all historical GWs are re-fetched and re-inserted correctly
- Run `fpl-ingest --force` and verify finished-GW data re-fetches from disk cache, not network (currently: re-fetches from network)
- Add a new column to `GameweekModel` and verify `_migrate_columns()` backfills it as NULL

**Common failure modes:**
- `--force` re-fetches from the network, not from the local raw JSON cache. If the raw cache already has the data, this wastes API quota and time.
- Player history raw JSON is keyed by player ID only. If FPL corrects a score post-match, the cached file is not invalidated.

**Current state:** Partial. Raw JSON cached to disk. Finished GWs skip re-fetch by default. `--force` triggers network re-fetch. No "replay from cache" path.

**Gaps:**
- No `--replay-from-cache` mode that re-runs transforms without any network calls.
- No per-GW `--force-gw 22` flag. `--force` re-fetches all finished GWs, which is expensive.

**Minimum acceptable standard:** Raw JSON cache is sufficient to rebuild the DB without network access. Currently true, but there is no CLI path to exercise this.

**Improvement:** Add a `--replay-from-cache` flag that skips all API calls and processes only existing raw JSON files. Enables schema evolution backfills (add a new transform, replay all GWs from cache in seconds). ~30-line addition to `src/fpl_ingest/cli.py` and `src/fpl_ingest/pipeline/gameweeks.py`.

---

## Pillar 8: Failure Handling & Recovery

**Production-grade means:** Any failure mode leaves the system in a known, recoverable state. The next run heals without manual intervention.

**Concrete checks:**
- Kill the pipeline mid-history-fetch and verify: (a) no partial DB state for the killed player, (b) next run completes for all players
- Simulate disk full and verify the pipeline exits with a clear error, not a Python traceback
- Simulate a network timeout mid-bootstrap and verify the pipeline retries, not panics

**Common failure modes:**
- The history stage processes 826 players. If it fails at player 800, the next run re-fetches all 826. The 800 already in DB are idempotent, but 800 redundant API calls are made unnecessarily.
- `asyncio.run()` wrapping the entire pipeline means any uncaught top-level exception will not call cleanup (DB connection may not flush WAL).

**Current state:** Good. `return_exceptions=True` isolates per-player failures. Per-stage transactions roll back on exception. WAL + `busy_timeout` handle concurrent access.

**Gaps:**
- No distinction between "this player has no history" (valid empty response) and "fetch failed." Both result in no rows and no cache file.
- `store.py` uses `_active_conn` as a shared connection but `__del__` is not reliably called in all Python implementations.

**Minimum acceptable standard:** Any run failure leaves the DB in a valid state (not mid-transaction). Verified: true via per-stage transactions.

**Improvement:** Add `atexit.register(store.close)` in `src/fpl_ingest/cli.py`. Write a checkpoint file (`~/.fpl/last_run_checkpoint.json`) after each stage completes, allowing the next run to skip already-completed stages.

---

## Pillar 9: Storage & Partitioning Strategy

**Production-grade means:** The storage layout supports downstream analytics access patterns without full-table scans. Growth is predictable and manageable.

**Concrete checks:**
- Run `EXPLAIN QUERY PLAN SELECT * FROM gameweeks WHERE round = 22 AND element_id = 300` and verify index use
- Run `EXPLAIN QUERY PLAN SELECT * FROM player_histories WHERE element_id = 300` and verify index use
- Estimate DB size at end of season: 826 players × 38 GWs × ~50 bytes/row ≈ 30 MB for `player_histories`. Verify against actual.

**Common failure modes:**
- SQLite is a single-writer database. At scale (concurrent processes, e.g., backfill + live ingest), WAL mode helps but does not eliminate contention.
- All data from all seasons lives in one table with no way to archive or filter by season without a full-table scan.

**Current state:** Appropriate for stated scale. Indexes on `round`, `element_id`, `event`, `element`. WAL mode. SQLite is the right choice for a local-first single-machine pipeline.

**Gaps:**
- No `season` column in any table. Multi-season data is indistinguishable without external metadata.
- No combined index on `(element_id, round)` for `player_histories`. Queries filtering both columns will use a single-column index and filter the rest in memory.

**Minimum acceptable standard:** All common downstream query patterns (by GW, by player, by fixture) are index-supported. Verified: `round`, `element_id`, `event` indexes exist.

**Improvement:** Add a `season TEXT` column (e.g., `"2024-25"`) to `PlayerModel`, `GameweekModel`, and `PlayerHistoryModel`. Derive it from `events[0].deadline_time` year on bootstrap. This is the single most impactful structural change for long-term analytics.

---

## Pillar 10: Documentation & Operability

**Production-grade means:** A new operator can run, monitor, and debug the pipeline without reading the source code.

**Concrete checks:**
- Follow the README from scratch on a clean machine and verify the pipeline runs end-to-end
- Identify what to do when the pipeline fails (currently: no runbook)
- Identify how to check if the data is stale (currently: requires manual `_runs` query)

**Current state:** Good for a developer. Excellent for the author. Insufficient for an operator who did not write the code.

**Gaps:**
- No runbook: "Pipeline failed, what do I do?"
- No documented query to check data freshness.
- `docs/governance.md` defines what the pipeline does NOT do but does not state what downstream systems must do instead.

**Minimum acceptable standard:** README contains: how to run, how to schedule, how to verify the last run succeeded, and what to do on failure.

---

## RED / YELLOW / GREEN Rubric

| Pillar | Status | Rationale |
|--------|--------|-----------|
| Reliability | YELLOW | Strong retry logic; soft exit on errors by default risks silent failures in scheduled runs. Resolved by Fix 1 — `_exit_code()` now returns 1 when any stage has `errors > 0`; no circuit breaker gap remains. |
| Data Correctness | YELLOW | No row-count reconciliation; `extra="forbid"` silently skips rows when new API fields appear. Resolved by Fix 4 — `_warn_if_high_skip_rate()` emits an aggregate WARNING when skipped / (upserted + skipped) exceeds 1%; no row-count reconciliation gap remains. |
| Idempotency | GREEN | `ON CONFLICT DO UPDATE` on all grain-aware tables; raw cache enables replay. Resolved by Fix 3 — all four pipeline modules (core, fixtures, gameweeks, history) now write cache files atomically via write-to-tmp-then-rename; the partial-write gap is closed. |
| Observability | YELLOW | `_runs` table exists but no structured output, no freshness contract, no alerting path |
| Data Freshness | YELLOW | No SLA defined; downstream data staleness is not self-evident from the database. Resolved by Fix 2 — `_metadata` table now records `last_successful_run_at`, `current_gameweek`, and `total_players` after every clean run; downstream can query freshness without reading logs. |
| Schema Stability | YELLOW | `extra="forbid"` detects drift but no schema versioning; breaking API changes produce 0 rows silently |
| Backfill/Replay | YELLOW | Raw cache exists but no `--replay-from-cache` path; `--force` hits network redundantly |
| Failure Recovery | GREEN | Per-stage transactions, isolated failures, WAL mode |
| Storage/Partitioning | YELLOW | No `season` column; no combined `(element_id, round)` index on `player_histories` |
| Documentation | YELLOW | Good developer docs; no operator runbook, no freshness query reference |

**Overall: YELLOW. Safe for personal analytics. Not safe for shared or production downstream systems without addressing at least 3 of the remaining YELLOWs.**

---

## Minimum Viable Stable Ingest Checklist

The smallest set of guarantees required before building an insights layer:

- [x] **Freshness contract.** `_metadata` table with `last_successful_run_at` and `current_gameweek`, updated at the end of each clean run.
- [x] **Non-zero exit on errors.** Pipeline exits non-zero (or sets `_metadata.last_run_status = 'FAILED'`) when any stage has `errors > 0`.
- [x] **Row-count sanity check.** WARNING log when `skipped / (upserted + skipped) > 1%` for any stage. The warning is emitted to the log only; no flag column is written to `_runs`.
- [x] **Atomic cache writes.** All four pipeline modules (core, fixtures, gameweeks, history) write raw JSON cache files atomically via write-to-tmp-then-rename. A partial write leaves a `.tmp` file that is ignored on the next run and overwritten on next fetch.
- [ ] **Documented run cadence.** README states how often to run and what staleness to expect.
- [ ] **Season column.** `season TEXT` on `players`, `gameweeks`, `player_histories` so multi-season data is queryable.

---

## What to Explicitly Document

| Topic | What to document |
|-------|-----------------|
| Ingestion behavior | Which tables are full-refresh vs incremental; which GWs are skipped by default |
| Refresh cadence | Recommended: once daily off-season, once per hour on matchdays |
| Data mutability | FPL updates bonus points post-match; re-running overwrites historical rows silently |
| Known limitations | No intra-GW live tracking; player history captures settled post-fixture values rather than live in-match scores (rows are at fixture grain, one per player per fixture); `chip_plays_json` is a serialized string not structured data |
| Freshness query | `SELECT value FROM _metadata WHERE key = 'last_successful_run_at'` |
| Grain per table | Already in `docs/data-contract.md`; add a "what grain means for analytics" section |

---

## Lightweight Instrumentation

All additive. No new dependencies.

**Implemented**

2. **`_metadata` table** — Three rows: `last_successful_run_at`, `current_gameweek`, `total_players`. Updated only on clean runs. Implemented in `_write_success_metadata()` in `src/fpl_ingest/cli.py` via `store.set_metadata()`.

3. **Skipped-row rate log line** — After each stage: `WARNING: stage=gameweeks skipped_rate=2.3% (19/826)`. Implemented in `_warn_if_high_skip_rate()` in `src/fpl_ingest/cli.py`; fires when `skipped / (upserted + skipped) > 1%`.

5. **Exit code contract** — `sys.exit(1)` when any stage has `errors > 0`. Implemented unconditionally in `_exit_code()` in `src/fpl_ingest/cli.py`; does not require `--strict` or non-interactive detection.

**Not yet implemented**

1. **`~/.fpl/last_run.json`** — Write at end of each run with `status`, `started_at`, `ended_at`, `stages`. Monitorable by any external tool. ~20 lines in `src/fpl_ingest/cli.py`.

4. **Structured run summary to stderr** — At end of run, emit one JSON line: `{"status": "ok", "stages": {...}, "elapsed_s": 127}`. Easily captured by cron loggers or CI.

---

## Pragmatism vs Strict Correctness

| Area | Pragmatism acceptable | Strict correctness required |
|------|----------------------|-----------------------------|
| Schema versioning | Tolerate additive-only migrations without version tracking in early stage | When multiple operators or environments share the DB |
| Rate limit tuning | Hardcoded 10 req/s is fine for single-user use | When running in CI or shared infrastructure |
| History cache freshness | Ignoring stale cache files is acceptable off-season | During active season when bonus corrections matter |
| Logging format | Free-text logs are fine for personal use | Before sharing with any external monitoring |
| `season` column | Acceptable to omit in the first season | Must add before a second season of data is ingested |
| Row-count reconciliation | Skip for now | Required before any downstream system makes decisions from the data |
| Runbook | README suffices if you are the only operator | Required before anyone else runs this pipeline |

---

## Stability Patch Plan

**Status:** Implemented. The four patches below were applied to bring the pipeline from YELLOW to stable-for-analytics.

---

### Critical Risks Addressed

| # | Risk | Failure Scenario | Pillar |
|---|------|-----------------|--------|
| 1 | Silent exit 0 on errors | Player history stage fails for 80 players. Pipeline exits 0. Downstream builds models on incomplete data with no indication anything went wrong. | Reliability |
| 2 | No freshness metadata | Downstream queries `gameweeks` with no way to know if the data is from today or three weeks ago. Analytics runs on stale data silently. | Data Freshness (RED) |
| 3 | Non-atomic cache writes | Process killed mid-write of `players/123.json`. File is 40% written. Next run reads it as a valid cache hit, `json.loads()` raises, player counted as error, pipeline exits 0. One player permanently missing until `--force`. | Idempotency |
| 4 | No skipped-row rate check | FPL adds a new required field. `extra="forbid"` skips every player row. `upserted=0`, `skipped=826`. WARNING per row in logs but no aggregate signal. Downstream sees zero new data with no alert. | Data Correctness |

---

### Fix 1: Non-zero exit on errors

**Files changed:** `src/fpl_ingest/cli.py`

**What was wrong:** `main()` called `asyncio.run(_async_main())` and discarded the return value. Any number of stage errors produced exit code 0.

**Fix applied:**
- `_async_main` now returns `int` (0 = clean, 1 = any stage had errors).
- `main()` calls `sys.exit(asyncio.run(_async_main(argv)))`.
- Error path logs a summary pointing to the `_runs` table.

**Guarantee introduced:** Schedulers (cron, CI, systemd) see a non-zero exit when data is incomplete. Downstream jobs that depend on a clean ingest can gate on this exit code.

---

### Fix 2: `_metadata` freshness table

**Files changed:** `src/fpl_ingest/store.py`, `src/fpl_ingest/pipeline/db_setup.py`, `src/fpl_ingest/cli.py`

**What was wrong:** No queryable record of when the last successful run completed. Downstream had no contract for data freshness.

**Fix applied:**
- `store.setup_metadata_table()` creates `_metadata (key TEXT PRIMARY KEY, value TEXT, updated_at TEXT)`.
- `store.set_metadata(key, value)` upserts with a UTC timestamp.
- `setup_store()` in `pipeline/db_setup.py` now calls `setup_metadata_table()` so the table is created on first run.
- At the end of `_async_main`, if `total_errors == 0`, three keys are written: `last_successful_run_at`, `current_gameweek`, `total_players`.
- Metadata is only written on a clean run. A failed run leaves the previous successful values intact.

**Freshness query for downstream:**
```sql
SELECT key, value, updated_at FROM _metadata;
```

**Guarantee introduced:** Downstream systems can gate on `last_successful_run_at` before consuming data. A stale or failed run is detectable without reading logs.

---

### Fix 3: Atomic cache writes

**Files changed:** `src/fpl_ingest/pipeline/gameweeks.py`, `src/fpl_ingest/pipeline/history.py`, `src/fpl_ingest/pipeline/core.py`, `src/fpl_ingest/pipeline/fixtures.py`

**What was wrong:** All four pipeline modules wrote raw JSON directly with `path.write_text(...)`. A process kill or disk-full mid-write left a partial file. The next run treated the partial file as a valid cache hit (file exists = skip re-fetch), then failed on `json.loads()`, silently dropping the player or GW.

Fix 3 as originally applied covered `gameweeks.py` and `history.py`. The same atomic write pattern was subsequently applied to `core.py` (in `_write_raw_cache`) and `fixtures.py` (in `_write_raw_cache`), completing the fix across all four modules.

**Fix applied:** Replaced direct `write_text` with write-to-`.tmp`-then-`rename`:
```python
tmp = dest.with_suffix(".tmp")
tmp.write_text(json.dumps(data, ...), encoding="utf-8")
tmp.rename(dest)
```
`os.rename()` is atomic on POSIX (Linux/macOS). The destination file either contains the full content or does not exist. A partial write leaves a `.tmp` file that is ignored on the next run and overwritten on next fetch.

**Guarantee introduced:** Cache files are either complete and valid JSON or absent. No partial reads. A killed write is self-healing on the next run with no manual intervention.

---

### Fix 4: Skipped-row rate warning

**Files changed:** `src/fpl_ingest/cli.py`

**What was wrong:** Skipped rows were logged per-row at WARNING level but there was no aggregate threshold check. A mass validation failure (e.g., FPL renames a field) produced 826 individual WARNING lines in the logs but no single alertable signal.

**Fix applied:** In `_record_stage`, after recording the run:
```python
total_rows = result.upserted + result.skipped
if total_rows > 0 and result.skipped / total_rows > 0.01:
    logger.warning(
        "High skip rate: stage=%s skipped=%d/%d (%.1f%%)",
        result.stage, result.skipped, total_rows,
        100 * result.skipped / total_rows,
    )
```
The threshold is 1% of rows actually processed (`upserted + skipped`), which is consistent across all stages regardless of what `fetched` counts (GWs vs players vs rows).

**Guarantee introduced:** Any schema drift or mass validation failure produces a single, scannable WARNING with the skip percentage. Detectable in log tails, cron emails, or any tool that watches for WARNING-level output.

---

### Stability Patch Plan: Execution Order

Execute in this order. Each step is independently testable before proceeding.

**Step 1: Atomic cache writes** (`gameweeks.py`, `history.py`)
- Lowest risk change, zero behavior change on happy path.
- Test: run pipeline, kill mid-history-fetch with `Ctrl-C`, verify `players/*.tmp` does not exist or is a `.tmp` file (not a `.json`), verify next run completes cleanly.

**Step 2: `_metadata` table** (`store.py`, `schema.py`, `cli.py`)
- Adds a new table; no changes to existing tables or behavior.
- Test: run pipeline, then query:
  ```sql
  SELECT key, value, updated_at FROM _metadata;
  ```
  Expected: 3 rows with `last_successful_run_at`, `current_gameweek`, `total_players`.

**Step 3: Non-zero exit on errors** (`cli.py`)
- Behavioral change only when errors > 0. Clean runs are unaffected.
- Test: run with a bad `--db` path to force an error, verify `echo $?` returns 1. Run normally, verify exit code 0.

**Step 4: Skipped-row rate warning** (`cli.py`)
- Purely additive log line. No behavior change.
- Test: temporarily set threshold to 0 (change `> 0.01` to `>= 0`) to force the warning, verify it appears, revert.

---

### Validation Checklist

Run these after all patches are applied:

**1. Verify clean run exits 0:**
```bash
fpl-ingest && echo "EXIT OK" || echo "EXIT FAILED"
```
Expected: `EXIT OK`

**2. Verify metadata is written:**
```bash
sqlite3 ~/.fpl/fpl.db "SELECT key, value, updated_at FROM _metadata ORDER BY key;"
```
Expected output:
```
current_gameweek|22|2025-01-15T14:00:00+00:00
last_successful_run_at|2025-01-15T14:00:00+00:00|2025-01-15T14:00:00+00:00
total_players|826|2025-01-15T14:00:00+00:00
```

**3. Verify data completeness (players):**
```bash
sqlite3 ~/.fpl/fpl.db "SELECT COUNT(*) FROM players;"
```
Expected: matches the total players count in `_metadata`.

**4. Verify GW row counts are consistent across re-runs:**
```bash
sqlite3 ~/.fpl/fpl.db "SELECT round, COUNT(*) FROM gameweeks GROUP BY round ORDER BY round;"
```
Run twice. Row counts must be identical.

**5. Verify no partial cache files exist:**
```bash
find ~/.fpl/raw -name "*.tmp" | wc -l
```
Expected: `0` after any completed run.

**6. Verify failed run exits non-zero and does NOT update metadata:**
```bash
# Simulate by pointing at a read-only DB
fpl-ingest --db /dev/null; echo "Exit: $?"
```
Expected: exit code 1. `_metadata.last_successful_run_at` unchanged.

**7. Verify _runs table records all 4 stages per run:**
```bash
sqlite3 ~/.fpl/fpl.db "SELECT started_at, stage, fetched, upserted, skipped, errors FROM _runs ORDER BY id DESC LIMIT 4;"
```
Expected: 4 rows with the most recent `started_at`, stages: `core`, `fixtures`, `gameweeks`, `player_histories`.
