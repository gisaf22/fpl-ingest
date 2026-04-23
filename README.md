# fpl‑ingest

`fpl‑ingest` is an ingestion pipeline for current-season Fantasy Premier League data stored in SQLite.

It fetches season metadata, fixtures, live gameweek data, and player history, validates them with typed models, and writes a reproducible snapshot plus raw API payloads.


## Quick Start (90 seconds)

```bash
uv run fpl-ingest run
uv run fpl-ingest status
uv run fpl-ingest schema validate
uv run fpl-ingest smoke-test
```

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

Flags:

```bash
uv run fpl-ingest run --db ~/.fpl/fpl.db --raw-dir ~/.fpl/raw --rate 3.0 --strict
```

The system uses the first available value in this order:

CLI flag → env var → config file → default

## Key Guarantees

- Single source of truth schema (DB + validation + tests)
- Success only if no errors or skipped rows
- Metrics invariant: fetched ≥ validated ≥ written
- Idempotent runs (no duplication)
- Strict mode blocks partial writes
- Smoke test detects API drift early

## System Overview

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

- **CLI** (`cli.py`) → command parsing + routing  
- **CLI output** (`cli_formatters.py`) → user-facing rendering  
- **Runner** (`pipeline/runner.py`) → pipeline orchestration  
- **API client** (`transport/async_client.py`) → FPL API + rate limiting  
- **Pipeline** (`pipeline/`) → fetch → validate → persist stages  
- **Contract** (`domain/schema.py`, `contract/compiler.py`) → schema + validation rules  
- **Storage** (`storage/store.py`) → SQLite writes + transactions  
- **Validation** (`validation/`, smoke tests) → drift + runtime checks  

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

Representative smoke‑test output:

```text
Smoke test passed.
Checked endpoints: bootstrap‑static, fixtures, element‑summary
Sample size: 5
```

Representative drift failure:

```text
Smoke test failed: Missing field: elements[].now_cost
```

## Limitations

- SQLite is the storage engine, so this is not a distributed ingestion platform.
- There is no scheduler or orchestration layer built in; execution is CLI‑driven.
- The project focuses on ingestion correctness and contract enforcement, not downstream analytics modeling.
- The smoke test checks structural drift, not semantic correctness of every upstream value.

Those constraints are deliberate. The point of the repository is to demonstrate strong ingestion system design, not to impersonate a full data platform.

## Documentation

- [Architecture Overview](docs/architecture/architecture.md)
- [Architecture Contract](docs/architecture/contract.md)
- [Data Contract](docs/data‑contract.md)
- [Schema Contract](docs/schema‑contract.md)
- [Guarantees](docs/guarantees.md)

## License

[MIT](LICENSE)
