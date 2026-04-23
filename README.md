# fpl-ingest

`fpl-ingest` is an FPL ingestion pipeline that stores structured data in SQLite.

It fetches season metadata, fixtures, live gameweek data, and per-player history, validates those payloads against typed models, and persists a reproducible SQLite snapshot plus raw API payloads.

It is designed to make ingestion failure modes explicit and enforceable: upstream shape drift, partial fetches, silent row drops, and runs that appear to succeed without producing a complete snapshot.

## Who This Is For

- engineers who want a local, reproducible FPL snapshot with clear success semantics
- projects that treat ingestion correctness as a contract problem, not a best-effort scrape
- engineers who want a predictable ingestion workflow and a local SQLite snapshot

## Not For

- teams looking for a distributed ingestion platform or scheduler
- users who only need ad hoc FPL notebooks without a maintained local SQLite snapshot
- projects focused on downstream analytics modeling rather than ingestion correctness

## Why This Exists

Most API pipelines break long before they throw an obvious exception.

- Upstream providers change payload shape without notice.
- Validation is often advisory instead of enforced.
- Pipelines frequently log row skips but still report success.
- Schema expectations drift across application code, database DDL, and tests.
- `fpl-ingest` focuses on reliable ingestion into SQLite rather than "fetch some JSON and hope for the best."

## Key Properties

- Schema is compiled into the database, validator, and tests from a single source of truth.
- A run is only successful if zero rows are skipped and zero errors occur.
- Stage metrics are invariant-checked (`fetched >= validated >= written`).
- The pipeline is idempotent and safe to rerun without duplicating data.
- Live API drift is checked before ingestion via a smoke test.

## Quick Start

### Requirements

- Python 3.10+
- [`uv`](https://docs.astral.sh/uv/)

### Install

```bash
git clone https://github.com/gisaf22/fpl-ingest.git
cd fpl-ingest
uv sync
```

### Run

```bash
uv run fpl-ingest run
```

Useful commands:

```bash
uv run fpl-ingest run
uv run fpl-ingest status
uv run fpl-ingest schema validate
uv run fpl-ingest smoke-test
```

Common flags:

```bash
uv run fpl-ingest run --db ~/.fpl/fpl.db --raw-dir ~/.fpl/raw --rate 3.0 --strict
```

Path resolution order for `db` and `raw-dir` is:

1. CLI flag
2. environment variable
3. `~/.fpl/config.yaml`
4. default path

## System Architecture

```text
Fantasy Premier League API
            |
            v
   Async ingestion client
            |
            v
  Contract enforcement layer
  (typed models + schema compiler)
            |
            v
    Transformation pipeline
            |
            v
       SQLite storage
            |
            v
 Validation, run audit,
 freshness metadata, smoke tests
```

### Layer Breakdown

- CLI boundary: [`src/fpl_ingest/cli.py`](src/fpl_ingest/cli.py) parses arguments, routes commands, and emits final command output.
- CLI output formatting: [`src/fpl_ingest/cli_formatters.py`](src/fpl_ingest/cli_formatters.py) owns command-facing text rendering for status, schema, and smoke-test output.
- Pipeline runner: [`src/fpl_ingest/pipeline/runner.py`](src/fpl_ingest/pipeline/runner.py) owns execution orchestration, stage ordering, and run finalization.
- API client: [`src/fpl_ingest/transport/async_client.py`](src/fpl_ingest/transport/async_client.py) fetches `bootstrap-static`, `fixtures`, `event/{id}/live`, and `element-summary/{id}` with rate limiting.
- Ingestion stages: [`src/fpl_ingest/pipeline/`](src/fpl_ingest/pipeline/) orchestrate fetch -> validate -> persist for core data, fixtures, gameweeks, and player histories.
- Contract enforcement: [`src/fpl_ingest/domain/schema.py`](src/fpl_ingest/domain/schema.py) defines the public table contract, and [`src/fpl_ingest/contract/compiler.py`](src/fpl_ingest/contract/compiler.py) compiles it into SQLite DDL, validation rules, and test-facing artifacts used by [`src/fpl_ingest/pipeline/db_setup.py`](src/fpl_ingest/pipeline/db_setup.py).
- Storage: [`src/fpl_ingest/storage/store.py`](src/fpl_ingest/storage/store.py) owns SQLite pragmas, transactions, upserts, schema registration, audit logging, and metadata finalization.
- Runtime validation and drift checks: typed models, schema validation commands, `_runs`, `_metadata`, and [`src/fpl_ingest/validation/smoke_test.py`](src/fpl_ingest/validation/smoke_test.py) surface upstream drift and partial-run risk early.

## Operational Guarantees

- Truthful success semantics: each stage returns an immutable [`StageResult`](src/fpl_ingest/pipeline/stage_result.py) with invariant-checked metrics, run status is classified deterministically in [`src/fpl_ingest/domain/run_status.py`](src/fpl_ingest/domain/run_status.py), and [`src/fpl_ingest/cli.py`](src/fpl_ingest/cli.py) returns exit code `0` only for a fully clean run.
- Failure containment: `--strict` aborts on the first unclean stage boundary, [`src/fpl_ingest/domain/execution_state.py`](src/fpl_ingest/domain/execution_state.py) propagates shared failed state, and post-failure cache or database writes are blocked.
- Idempotent and reproducible execution: writes are upserts through [`src/fpl_ingest/storage/store.py`](src/fpl_ingest/storage/store.py), conflict targets are inferred from compiled keys, finished gameweeks and player histories can reuse raw JSON cache unless `--force` is supplied, and gameweek rows are written in ascending order.
- Observability: every stage emits canonical metrics, completed stages are recorded in `_runs`, final run status is persisted for all stage rows, and `_metadata` stores freshness information separately from domain tables.

For the full semantics and deeper implementation details, see the documentation in [`docs/`](docs/).

## Repository Structure

```text
src/fpl_ingest/
  cli.py                 # thin CLI router
  cli_formatters.py      # command output formatting
  contract/              # schema compiler, DDL generation, validation artifacts
  domain/                # models, schema contract, transforms, run semantics
  pipeline/              # runner plus stage modules for core, fixtures, gameweeks, history
  storage/               # SQLite persistence layer
  transport/             # API clients, sync HTTP, rate limiting
  validation/            # smoke tests and runtime drift validation

tests/
  contract/              # contract alignment and schema drift tests
  domain/                # transform and domain behavior
  integration/           # broader API and behavior tests
  pipeline/              # CLI and stage orchestration tests
  smoke/                 # smoke test behavior
  storage/               # SQLite persistence tests
  transport/             # client and rate limiter tests

```

## Example Behavior

Representative stage logs:

```text
12:00:01 INFO     fpl_ingest — [stage=core] fetched=650 validated=650 written=650 skipped=0
12:00:03 INFO     fpl_ingest — [stage=fixtures] fetched=380 validated=380 written=380 skipped=0
12:00:05 INFO     fpl_ingest — [stage=gameweeks] fetched=11400 validated=11400 written=11400 skipped=0
12:00:19 INFO     fpl_ingest — [stage=player_histories] fetched=22800 validated=22800 written=22800 skipped=0
```

Representative run summary:

```text
12:00:19 INFO     fpl_ingest — [run] status=SUCCESS total_fetched=35230 total_validated=35230 total_written=35230 total_skipped=0 total_errors=0
```

Representative smoke-test output:

```text
Smoke test passed.
Checked endpoints: bootstrap-static, fixtures, element-summary
Sample size: 5
```

Representative drift failure:

```text
Smoke test failed: Missing field: elements[].now_cost
```

## Documentation

- [docs/architecture.md](docs/architecture.md)
- [docs/data-contract.md](docs/data-contract.md)
- [docs/schema-contract.md](docs/schema-contract.md)
- [docs/guarantees.md](docs/guarantees.md)

## Limitations

- SQLite is the storage engine, so this is not a distributed ingestion platform.
- There is no scheduler or orchestration layer built in; execution is CLI-driven.
- The project focuses on ingestion correctness and contract enforcement, not downstream analytics modeling.
- The smoke test checks structural drift, not semantic correctness of every upstream value.

Those constraints are deliberate. The point of the repository is to demonstrate strong ingestion system design, not to impersonate a full data platform.

## Future Extensions

- publish into DuckDB, Postgres, or a warehouse while preserving the same compiled contract model
- emit run metrics to an external observability backend
- add snapshot/versioned exports for downstream model training or feature generation
- add orchestration only if operational scale justifies it

## License

[MIT](LICENSE)
