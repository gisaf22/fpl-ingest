# Pipeline Guarantees

Operational contract for `fpl-ingest`. Defines what downstream systems can trust, what the system does not guarantee, and how to verify state.

---

## System Guarantees

The following properties are enforced by the implementation and can be tested.

| # | Guarantee | How it is enforced |
|---|---|---|
| G1 | Exit code is non-zero if any stage records a non-zero error count. Skipped rows (validation failures) do not affect the exit code in the default mode. | `_exit_code()` computes `sum(r.errors for r in stage_results)` and returns 1 when the total is non-zero; `_run_pipeline()` calls `_exit_code()` and returns its result; `main()` passes this to `sys.exit`. |
| G2 | `_metadata.last_successful_run_at` is written only when all stages complete with zero errors. Runs with skipped rows but zero errors are treated as clean and will update this value. | Set inside the `total_errors == 0` branch after all stages complete. |
| G3 | `gw_{n}.json` and `players/{id}.json` cache files are written atomically. A partial download does not leave a corrupt file at the final path. | Both file types are written to a `.tmp` path then renamed. |
| G4 | A warning is emitted when the skipped-row rate for any stage exceeds 1%. | `_warn_if_high_skip_rate()` computes `skipped / (upserted + skipped)` and calls `logger.warning` when above 0.01; it is called from `_record_stage()` after each stage completes. |
| G5 | Every stage that completes normally (with or without errors) is recorded in `_runs`. Stages that raise an unhandled exception are not recorded. | `_record_stage()` calls `store.record_run` unconditionally on every stage that returns a `StageResult`. |
| G6 | Each stage's database writes are atomic. A stage that raises mid-write is fully rolled back. | Each stage runs inside `with store.transaction()`, which calls `conn.rollback()` on any exception. |
| G7 | Upsert semantics: re-ingesting the same source row updates the existing record and does not create a duplicate. | `bulk_upsert` uses `INSERT ... ON CONFLICT({grain_cols}) DO UPDATE SET ...` when a conflict target is known. |
| G8 | Every persisted row carries an `ingested_at` ISO 8601 UTC timestamp set at upsert time. | `upsert_models` injects `ingested_at = datetime.now(timezone.utc).isoformat()` into every row tuple. |
| G9 | Unknown API fields cause the affected row to be skipped and counted, not silently stored. | All models use `extra="forbid"`; `ValidationError` on any row increments the `skipped` counter. |
| G10 | `_metadata.total_players` is updated on every clean run. `_metadata.current_gameweek` is updated only when bootstrap contains an event with `is_current=True`; during pre-season or GW transitions it is not written and the previous value persists. | `total_players` is set unconditionally in the `total_errors == 0` branch. `current_gameweek` is set only when `current_gw is not None`. |

---

## Non-Guarantees

### Schema Drift

- The system does not detect when the FPL API removes or renames a field. A removed required field will cause all rows for that model to be skipped with `ValidationError`.
- Column type changes and column removals are not handled by the automatic migration path. Only additive column additions are applied via `ALTER TABLE ADD COLUMN`.
- Unknown fields are rejected (G9), but the run continues unless `--strict` is set. A high skip rate does not abort the run by default.

### Data Completeness

- Finished gameweeks and player histories are cached after the first successful fetch. Subsequent runs skip cached files without re-fetching unless `--force` is passed. Stale or corrupt cache files will be read as-is.
- Players with an empty `history[]` from the API produce zero rows in `player_histories`. This is not distinguishable from a player who was never fetched.
- `gameweeks` does not cover gameweeks that were neither `finished` nor `is_current` at the time of the run (i.e. future rounds).
- Stage isolation means that a rollback in one stage does not undo writes from earlier stages. A run that fails at `player_histories` leaves `players`, `teams`, `fixtures`, and `gameweeks` committed.

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

This timestamp is set only when all four stages completed with zero errors. If this row is absent or stale, the last run either failed or has not been executed.

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
| `--strict` mode with any skipped/error rows | `RuntimeError` raised at end of affected stage; remaining stages do not run; exit code 1. | Investigate the cause before re-running. |
| Validation failures only (`skipped > 0`, `errors == 0`) | All valid rows upsert normally. `_runs` records the skip count. `last_successful_run_at` is updated. Exit code 0. | Monitor `_runs.skipped`. A non-zero skip count on a run that exited 0 indicates partial data loss. Use `--strict` if skipped rows should be treated as failures. |
| `_metadata` not updated | Only occurs when `total_errors > 0`. The previous `last_successful_run_at` remains. | Treat as a stale run; do not use `_metadata` values for freshness decisions until the next clean run. |

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
SELECT started_at, stage, fetched, upserted, skipped, errors
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
