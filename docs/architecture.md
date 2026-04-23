# Architecture

`fpl-ingest` is organized as a small set of layers with clear ownership.

## Execution Flow

```text
CLI router -> runner -> Pipeline stages -> StageResult -> SQLite store -> Finalize run
```

At a high level:

1. `cli.py` parses arguments, routes commands, and delegates run execution to `pipeline/runner.py`.
2. `pipeline/runner.py` resolves runtime dependencies and orchestrates stage order.
3. `pipeline/` stages fetch, validate, transform, and persist data one stage at a time.
4. Each stage returns a canonical `StageResult` with fetched, validated, written, skipped, and error counts.
5. `storage/store.py` persists rows, `_runs` audit state, and `_metadata` freshness information.
6. Terminal run status is derived from canonical stage results and persisted at the end of the run.

## Package Layout

### `transport/`

Owns API access and request pacing:

- `async_client.py`
- `sync_client.py`
- `sync_http.py`
- `rate_limiter.py`
- `rate_config.py`

### `domain/`

Owns typed models, schema metadata, transforms, and run semantics:

- `models.py`
- `schema.py`
- `transforms.py`
- `run_status.py`
- `execution_state.py`
- `types.py`

### `pipeline/`

Owns ingestion orchestration and per-stage execution:

- `runner.py`
- `core.py`
- `fixtures.py`
- `gameweeks.py`
- `history.py`
- `db_setup.py`
- `shared.py`
- `stage_result.py`

### `storage/`

Owns SQLite persistence and run finalization:

- `store.py`

### `validation/`

Owns upstream structural drift checks:

- `smoke_test.py`

### `contract/`

Owns compiled schema outputs derived from the canonical domain contract:

- compiler
- DDL generation
- validation rules
- test-facing artifacts

## Design Intent

The important boundaries are:

- cli routes commands but does not own pipeline orchestration
- transport fetches data but does not know pipeline policy
- domain defines structure and rules but does not perform I/O
- pipeline orchestrates stage work but does not own storage internals
- storage persists canonical results but does not infer business meaning
- contract artifacts are generated from canonical schema metadata, not hand-maintained in parallel

This keeps schema ownership, execution flow, and observability easier to reason about.
