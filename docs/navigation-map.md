# Navigation Map

This map is for a reviewer or maintainer who wants to find the right document or code entry point quickly.

## Fast Reading Path

Read these files in this order:

1. `README.md` - repository entry point.
2. `docs/architecture/system-purpose.md` - system identity, guarantees, replay philosophy, trust boundary, and non-goals.
3. `docs/architecture/architecture.md` - execution flow, orchestration structure, and package ownership.
4. `docs/data-contract.md` - table grain, schema definitions, constraints, and field meaning.
5. `docs/adr/` - historical decisions. `docs/architecture/performance-assessment.md` - player-history stage timing analysis and optimization tradeoffs.
6. `artifacts/contract/schema_contract.json` - machine-readable table contract.
7. `artifacts/contract/validation_contract.json` and `artifacts/contract/ddl_contract.sql` - validation and SQL contract artifacts.
8. `.github/workflows/scheduled_run.yml` - example scheduled entry point.

## Command Entry Points

| Command | Purpose | Main code path |
|---|---|---|
| `uv run fpl-ingest run` | Run ingestion. | `cli.py` -> `orchestration/runner.py` |
| `uv run fpl-ingest status` | Show recent run and freshness metadata. | `cli.py` -> `load/store.py` |
| `uv run fpl-ingest replay` | Reprocess the raw JSON cache. | `cli.py` -> `orchestration/replay.py` |
| `uv run fpl-ingest schema export` | Generate schema artifacts. | `cli.py` -> `schema/definition.py` -> `schema/compiler.py` |
| `uv run fpl-ingest schema validate` | Compare a SQLite database against the schema contract. | `cli.py` -> `schema/validation.py` |
| `uv run fpl-ingest smoke-test` | Check upstream API shape. | `cli.py` -> `schema/validation.py` |

## Package Map

| Area | Files | What to look for |
|---|---|---|
| CLI | `src/fpl_ingest/cli.py`, `src/fpl_ingest/cli_formatters.py` | Command routing and user-facing output. |
| Config | `src/fpl_ingest/config.py` | Path and setting resolution. |
| Extract | `src/fpl_ingest/extract/stages/` | Endpoint-oriented extract stages. |
| HTTP | `src/fpl_ingest/extract/http/` | API clients, rate limiting, and request settings. |
| Transform | `src/fpl_ingest/transform/models.py`, `src/fpl_ingest/transform/transforms.py` | Pydantic models and structural transforms. |
| Schema | `src/fpl_ingest/schema/` | Contract definitions, compiler, DDL, validation, and schema tests. |
| Load | `src/fpl_ingest/load/` | SQLite setup, persistence, metadata, and integrity checks. |
| Orchestration | `src/fpl_ingest/orchestration/` | Stage ordering, replay entry point, run status, and stage accounting. |
| Tests | `tests/` | Tests by package area. |
| Contracts | `artifacts/contract/` | Generated schema artifacts. |

## Data Flow By Stage

| Stage | Source endpoint | Main file | Persisted tables |
|---|---|---|---|
| Core data | `bootstrap-static` | `extract/stages/bootstrap.py` | `players`, `teams`, `events`, `element_types` |
| Fixtures | `fixtures` | `extract/stages/fixtures.py` | `fixtures`, `fixture_stats` |
| Gameweeks | `event/{gw}/live` | `extract/stages/gameweeks.py` | `gameweeks` |
| Player histories | `element-summary/{player_id}` | `extract/stages/element_summary.py` | `player_histories` |

## Where To Go Next

| Need | Go to |
|---|---|
| Understand why the system exists | `docs/architecture/system-purpose.md` |
| Understand execution flow | `docs/architecture/architecture.md` |
| Understand table grain or field meaning | `docs/data-contract.md` |
| Inspect generated schema artifacts | `artifacts/contract/` |
| Review historical decisions | `docs/adr/` |
| Trace command handling | `src/fpl_ingest/cli.py` |
| Trace orchestration | `src/fpl_ingest/orchestration/runner.py` |
