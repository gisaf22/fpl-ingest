# Pipeline Guarantees

Operational contract for `fpl-ingest`. Defines what downstream systems can trust, what the system does not guarantee, and how to verify state.

---

## System Guarantees

The following properties are enforced by the implementation and can be tested.

| # | Guarantee | How it is enforced |
|---|---|---|
| G0 | API request rate is safe by default and safe under every entry point. The default rate is `3.0` req/sec and `10.0` req/sec is a hard system-wide cap. | `DEFAULT_RATE` and `MAX_RATE` are defined once in `rate_config.py`; `build_parser()` uses `DEFAULT_RATE` and `_resolve_applied_rate()` clamps values above `MAX_RATE` before constructing `TokenBucketLimiter`; `AsyncFPLClient` also clamps token-bucket rates internally and logs the requested/applied values when safety clamping occurs. |
| G1 | Exit code is zero only when every stage completes with `errors == 0` and `skipped == 0`. | `_exit_code()` aggregates errors and skipped rows across all stage results and returns 0 only when both totals are zero. |
| G2 | `_metadata.last_successful_run_at` is written only when all stages complete with zero errors and zero skipped rows. | `_success_metadata()` is called only from the fully clean branch in `_exit_code()`, and `store.finalize_run()` writes status plus metadata together. |
| G3 | `gw_{n}.json` and `players/{id}.json` cache files are written atomically. A partial download does not leave a corrupt file at the final path. | Both file types are written to a `.tmp` path then renamed. |
| G4 | A warning is emitted when the skipped-row rate for any stage exceeds 1%. | `_warn_if_high_skip_rate()` computes `skipped / (upserted + skipped)` and calls `logger.warning` when above 0.01; it is called from `_record_stage()` after each stage completes. |
| G5 | Every stage that completes normally (with or without errors) is recorded in `_runs`, and each run receives one final persisted status. Stages that raise an unhandled exception are not recorded. | `_record_stage()` calls `store.record_run` for every returned `StageResult`; `store.finalize_run()` writes the terminal `status` onto all rows for the run once execution completes or fails fast. |
| G6 | Each stage's database writes are atomic. A stage that raises mid-write is fully rolled back. | Each stage runs inside `with store.transaction()`, which calls `conn.rollback()` on any exception. |
| G7 | Upsert semantics: re-ingesting the same source row updates the existing record and does not create a duplicate. | `bulk_upsert` uses `INSERT ... ON CONFLICT({grain_cols}) DO UPDATE SET ...` when a conflict target is known. |
| G8 | Every persisted row carries an `ingested_at` ISO 8601 UTC timestamp set at upsert time. | `upsert_models` injects `ingested_at = datetime.now(timezone.utc).isoformat()` into every row tuple. |
| G9 | Unknown API fields cause the affected row to be skipped and counted, not silently stored. | All models use `extra="forbid"`; `ValidationError` on any row increments the `skipped` counter. |
| G10 | `_metadata.total_players` is updated on every fully clean run. `_metadata.current_gameweek` is updated only when bootstrap contains an event with `is_current=True`; during pre-season or GW transitions it is not written and the previous value persists. | `total_players` is set unconditionally in the success branch where both aggregate errors and skipped rows are zero. `current_gameweek` is set only when `current_gw is not None`. |
| G11 | Strict mode is fail-fast at stage boundaries and cancels concurrent in-flight fetch work before any later side effects can occur. Once a stage reports errors or skipped rows, no later stages execute. | `_warn_or_raise_on_unclean_stage()` raises `StrictRunFailure`; `_run_pipeline()` catches it, logs fail-fast context, and returns exit code 1 without invoking later stages. In `gameweeks.py` and `history.py`, strict-mode concurrent fetches cancel pending tasks, await their cancellation, and suppress post-failure cache/database writes. |
| G12 | Every failed run logs that the database may be partially updated and must not be treated as a consistent snapshot. Strict failures also log `mode=fail_fast`, `failure_reason`, and `failed_stage`. | `_exit_code()` emits the partial-run warning for end-of-run failures; `_log_fail_fast_failure()` emits the fail-fast context and the same partial-run warning before exit. |
| G13 | `_runs.status` is durable, queryable without relying on logs, and classified deterministically. | `setup_runs_table()` ensures the `_runs.status` column exists; `run_status.classify_run_status()` is the single canonical classifier with precedence `FAILED > FAILED_PARTIAL > SUCCESS`; the CLI finalizes each run in a terminal transaction. |

---

## Non-Guarantees

### Schema Drift

- The system does not detect when the FPL API removes or renames a field. A removed required field will cause all rows for that model to be skipped with `ValidationError`.
- Column type changes and column removals are not handled by the automatic migration path. Only additive column additions are applied via `ALTER TABLE ADD COLUMN`.
- Unknown fields are rejected (G9). In default mode the run still completes, but any skipped rows make the final run status `FAILED_PARTIAL` and prevent freshness updates. `--strict` aborts earlier at the first unclean stage.

### Data Completeness

- Finished gameweeks and player histories are cached after the first successful fetch. Subsequent runs skip cached files without re-fetching unless `--force` is passed. Stale or corrupt cache files will be read as-is.
- Players with an empty `history[]` from the API produce zero rows in `player_histories`. This is not distinguishable from a player who was never fetched.
- `gameweeks` does not cover gameweeks that were neither `finished` nor `is_current` at the time of the run (i.e. future rounds).
- Stage isolation means that a rollback in one stage does not undo writes from earlier stages. A run that fails at `player_histories` leaves `players`, `teams`, `fixtures`, and `gameweeks` committed, and that partial state must not be treated as a full snapshot.

### Freshness

- There is no SLA on run frequency. The system is a CLI tool; freshness depends entirely on how often it is scheduled externally.
- During a live gameweek, `gameweeks` data reflects API state at the time of the last run, not real-time scores. The live endpoint is re-fetched every run (it is never cached for the current GW).
- `_metadata.current_gameweek` is derived from the `is_current` flag in bootstrap, not from the FPL scoring engine. These can diverge briefly during GW transitions.

---

## Data Freshness Contract

**Primary freshness signal:**

```sql
SELECT value AS last_run_at, updated_at
FROM _metadata
WHERE key = 'last_successful_run_at';
```

This timestamp is set only when all four stages completed with zero errors and zero skipped rows. If this row is absent or stale, the last run either failed, lost data, or has not been executed.

**Supporting signals:**

```sql
-- Current gameweek as of last clean run
SELECT value FROM _metadata WHERE key = 'current_gameweek';

-- Player count as of last clean run
SELECT value FROM _metadata WHERE key = 'total_players';
```

"Fresh enough" for the typical use case means `last_successful_run_at` is within the same calendar day (UTC) for non-live gameweeks. During a live gameweek, acceptable staleness depends on the downstream consumer's latency tolerance; the system makes no commitment beyond "current GW data is always re-fetched on each run."

---

## Failure Semantics

| Scenario | System state | Downstream action |
|---|---|---|
| Network error on one player fetch | `errors += 1` for that player; all other writes commit; `_runs` records the error count; exit code 1. | Check `_runs` for `errors > 0`; re-run to pick up missing players (player cache is not written on error, so the next run will retry). |
| Pydantic `ValidationError` on a row | Row is skipped; `skipped` counter incremented; all other rows in the stage upsert normally. | Monitor `_runs.skipped`; a sudden increase indicates API schema drift. |
| Exception mid-transaction | Transaction rolls back all writes for that stage; prior committed stages are unaffected; exception propagates; exit code 1. | Re-run. The rolled-back stage left no partial state. |
| `--strict` mode with any skipped/error rows | The run exits immediately after the affected stage boundary; remaining stages do not run; final logs include `mode=fail_fast`, `failure_reason`, and `failed_stage`; concurrent in-flight fetch work is cancelled and awaited; exit code 1. | Investigate the failing stage before re-running. |
| Validation failures only (`skipped > 0`, `errors == 0`) | All valid rows upsert normally. `_runs` records the skip count. Final run status is `FAILED_PARTIAL`. `last_successful_run_at` is not updated. Exit code 1. | Treat the run as partial data loss and investigate `_runs.skipped` before relying on the data. |
| `_metadata` not updated | Occurs whenever aggregate `errors > 0` or `skipped > 0`. The previous `last_successful_run_at` remains. | Treat as a stale or partial run; do not use `_metadata` values for freshness decisions until the next fully clean run. |

---

## Validation Queries

Run these against the SQLite database to verify system state.

**Last clean run:**

```sql
SELECT key, value, updated_at
FROM _metadata
ORDER BY updated_at DESC;
```

**Recent stage outcomes (last 10):**

```sql
SELECT started_at, stage, fetched, upserted, skipped, errors, status
FROM _runs
ORDER BY id DESC
LIMIT 10;
```

**Any runs with errors or high skip rates:**

```sql
SELECT started_at, stage, skipped, errors,
       ROUND(100.0 * skipped / NULLIF(upserted + skipped, 0), 1) AS skip_pct
FROM _runs
WHERE errors > 0 OR (skipped * 1.0 / NULLIF(upserted + skipped, 0)) > 0.01
ORDER BY id DESC;
```

**Row count sanity across domain tables:**

```sql
SELECT 'players'          AS tbl, COUNT(*) AS rows FROM players
UNION ALL
SELECT 'teams',                    COUNT(*)         FROM teams
UNION ALL
SELECT 'events',                   COUNT(*)         FROM events
UNION ALL
SELECT 'fixtures',                 COUNT(*)         FROM fixtures
UNION ALL
SELECT 'fixture_stats',            COUNT(*)         FROM fixture_stats
UNION ALL
SELECT 'gameweeks',                COUNT(*)         FROM gameweeks
UNION ALL
SELECT 'player_histories',         COUNT(*)         FROM player_histories;
```

**Gameweek coverage (spot-check for gaps):**

```sql
SELECT round, COUNT(DISTINCT element_id) AS players
FROM gameweeks
GROUP BY round
ORDER BY round;
```
