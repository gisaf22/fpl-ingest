# ADR 001 — SQLite as the Storage Backend

Date: 2026-05-15
Status: Accepted

## Context

fpl-ingest is a personal ingestion project with a single writer process that
runs on a daily schedule. The primary consumers are local analytics queries
(read-only, ad hoc). Operational overhead must be near zero: no server to
provision, no credentials to rotate, no service to keep alive.

The on-disk format must remain queryable for post-run inspection and audit —
flat files alone are insufficient.

## Decision

Use SQLite with the following connection settings, applied in
`load/store.py:_get_connection`:

- `PRAGMA journal_mode=WAL` — readers never block writers and writers never
  block readers. Supports the common pattern of running `fpl-ingest status`
  or ad-hoc SELECT queries while an ingest run is in progress.
- `PRAGMA synchronous=NORMAL` — safe with WAL mode; skips redundant full-sync
  calls that are only meaningful in rollback-journal mode.
- `PRAGMA busy_timeout=5000` — retry for up to 5 seconds on a locked
  database rather than raising immediately. Handles the rare overlap of two
  concurrent processes (e.g. a manual run while the scheduled job is still
  finishing).

## Alternatives Considered

| Alternative | Reason rejected |
|---|---|
| **DuckDB** | Better OLAP query performance, but no concurrent-writer advantage for a single-writer workload. Adds a native dependency; SQLite ships with CPython. |
| **PostgreSQL** | Operational overhead (server, credentials, network) is unjustified for a personal single-node project. |
| **Flat Parquet files** | No queryable audit surface. The `_runs` and `_metadata` tables require structured, in-process querying that a flat file layout cannot provide without an additional query engine. |

## Consequences

- **Positive:** Zero infrastructure cost. Trivial deployment — the database is
  a single file. Compatible with any machine that has Python installed.
- **Positive:** The `_runs` audit table and `_metadata` freshness table are
  immediately queryable with standard SQL tooling (sqlite3 CLI, DBeaver,
  datasette, etc.).
- **Negative:** No horizontal scaling. A second concurrent writer on the same
  file will contend on WAL locks; the `busy_timeout` buys time but does not
  eliminate the constraint.
- **Negative:** The database does not replicate or distribute. Backups are a
  manual `cp` or scheduled `rsync`.

## Migration Path

The `SQLiteStore` class encapsulates all persistence logic behind the
`bulk_upsert`, `upsert_models`, `query`, and audit-table methods.
Orchestration, extract, transform, and schema modules have no direct SQLite
imports. Switching to DuckDB or PostgreSQL requires replacing
`load/store.py` — and updating `load/integrity.py` if the new backend uses a
different connection API — without touching `orchestration/`, `extract/`,
`transform/`, or `schema/`.

The schema compiler in `schema/compiler.py` emits DDL from the public table
definition in `schema/definition.py`. A new backend would consume the same compiled
`CompiledTable` objects; only the DDL dialect may need adaptation.
