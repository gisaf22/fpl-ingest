# Architecture

`fpl-ingest` is organized as a small set of layers with clear ownership.

For system-level purpose, non-goals, operational assumptions, and major tradeoffs, read [System Purpose](system-purpose.md). This page focuses on execution flow and package ownership.

## Execution Flow

```text
CLI router -> orchestration runner -> extract stages -> transform models -> load store -> finalize run
```

At a high level:

1. `cli.py` parses arguments, resolves command handlers, and delegates run execution to `orchestration/runner.py`.
2. `orchestration/runner.py` resolves runtime dependencies and orchestrates stage order.
3. `extract/stages/` endpoint stages fetch, transform, and persist data one stage at a time.
4. Each stage returns a `StageResult` with stage-level counts.
5. `load/store.py` persists rows, `_runs` audit state, and `_metadata` freshness information.
6. Run finalization persists terminal status at the end of the run.

## Package Layout

### `extract/`

Owns API access, request pacing, raw cache writes, and endpoint-oriented extract stages:

- `extract/http/client.py`
- `extract/http/sync_client.py`
- `extract/http/sync_http.py`
- `extract/http/rate_limiter.py`
- `extract/http/rate_config.py`
- `extract/stages/bootstrap.py`
- `extract/stages/fixtures.py`
- `extract/stages/gameweeks.py`
- `extract/stages/element_summary.py`

### `transform/`

Owns typed models and pure structural transforms:

- `models.py`
- `transforms.py`
- `types.py`

### `orchestration/`

Owns run lifecycle, stage accounting, replay, and status classification:

- `runner.py`
- `replay.py`
- `stage_result.py`
- `run_status.py`
- `execution_state.py`

### `load/`

Owns SQLite persistence, run finalization, and post-run integrity checks:

- `store.py`
- `integrity.py`
- `db_setup.py`

### `schema/`

Owns the public table contract, compiled artifacts, DDL, database validation, and smoke tests:

- `definition.py`
- `compiler.py`
- `ddl.py`
- `validation.py`
- `test_data.py`

## Boundary Intent

The package boundaries are:

- cli routes commands but does not own orchestration
- extract fetches and caches raw API data but does not own run policy
- transform defines row shape and structural flattening but does not perform I/O
- orchestration coordinates stage work but does not own SQLite internals
- load persists canonical results but does not infer business meaning
- schema owns contract artifacts and validation metadata
