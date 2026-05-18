# ADR 002 — Contract-as-Code Schema System

Date: 2026-05-15
Status: Accepted

## Context

Ingestion systems have three places where a schema can drift independently:
the upstream API response, the validation layer, and the storage DDL. When
these are defined separately, adding or renaming a field requires coordinated
changes across multiple files, and the gap between them is a silent failure
mode — rows silently drop, columns silently go NULL, or tests pass while
production data is corrupt.

fpl-ingest fetches live JSON from the FPL API and stores it in SQLite tables
consumed by external analytics tooling. Both the API shape and the consumer
contract need to be knowable and verifiable without running the pipeline.

## Decision

Maintain one authoritative public table definition in `schema/definition.py`,
backed by Pydantic row models in `transform/models.py`. A compiler
(`schema/compiler.py`) derives every downstream artifact from that source:

- **DDL** — `CREATE TABLE` statements with column types, nullability, primary
  keys, and unique constraints, applied by `SQLiteStore.register_contract_table`.
- **Validation rules** — Pydantic field definitions are the validation layer;
  no separate validation schema exists.
- **Checked-in artifacts** — the compiled contract is written to
  `artifacts/contract/` and checked into the repository. The CI job
  `test_checked_in_contract_artifacts_match_compiled_outputs` asserts that the
  checked-in JSON and DDL artifacts match what the compiler produces from the current code,
  making contract drift a CI failure rather than a runtime surprise.
- **Consumer-facing JSON contract** — `fpl-ingest schema export` writes a
  machine-readable JSON artefact documenting every public table, column type,
  and constraint for downstream consumers.

## Alternatives Considered

| Alternative | Reason rejected |
|---|---|
| **Separate schema files** (e.g. JSON Schema, YAML) | Two sources of truth create a synchronisation obligation on every field change. Drift between the Pydantic model and the schema file is the exact failure mode this design prevents. |
| **ORM** (SQLAlchemy, Tortoise) | Hides SQL semantics behind an abstraction layer, couples persistence to a framework, and makes the DDL non-obvious to read. The project has no need for query composition beyond simple `SELECT *` and audit queries. |
| **Hand-maintained DDL** | Every field change requires a corresponding DDL edit and a manual migration, with no automated check that they stay in sync. |

## Consequences

- **Positive:** Adding a field to a public table is a one-line change in
  `schema/definition.py` or its backing model in `transform/models.py`. The compiler, DDL, validation, and test artifact all
  update automatically.
- **Positive:** Schema drift is caught in CI before it reaches production.
  The checked-in artefact makes the schema reviewable in a pull request diff.
- **Positive:** `fpl-ingest schema validate` can check a live database against
  the compiled contract at any time, giving operators a live drift check
  without reading source code.
- **Negative:** The compiler adds a layer of indirection between the Pydantic
  model and the SQL. Engineers unfamiliar with the pattern must read
  `schema/compiler.py` to understand how field types map to SQLite column
  types.
- **Negative:** Non-nullable or primary-key column additions require a manual
  migration on existing databases (enforced by `_migrate_contract_columns` in
  `load/store.py`). Nullable additions are applied automatically.

## Migration Path

Schema versioning policy is defined in `docs/data-contract.md`. The compiled
artifact carries a `SCHEMA_VERSION` constant from `schema/definition.py`.
Consumers can pin to a version and detect breaking changes by comparing the
version field in the exported JSON contract.
