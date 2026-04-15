# Data Contract

This document defines the persisted table grain for `fpl-ingest` and the intended boundary between ingestion and downstream modeling.

## Scope

`fpl-ingest` is responsible for:

- fetching raw data from the FPL API
- validating it against local models
- persisting it into stable SQLite tables

`fpl-ingest` only persists values provided by the upstream API, plus minimal structural flattening needed to store nested payloads in relational tables. It does not derive analytics fields, aggregate records across rows, or invent new metrics.

`fpl-ingest` is not responsible for collapsing fixture-grain history into a single canonical player-gameweek fact. This project persists both contracts explicitly: live gameweek rows in `gameweeks`, and per-fixture history rows in `player_histories`.

## Source To Table Mapping

| Source endpoint | Table | Grain |
|---|---|---|
| `bootstrap-static` → `elements` | `players` | one row per player |
| `bootstrap-static` → `teams` | `teams` | one row per team |
| `bootstrap-static` → `events` | `events` | one row per event/gameweek |
| `bootstrap-static` → `element_types` | `element_types` | one row per element type |
| `fixtures` | `fixtures` | one row per fixture |
| `fixtures` → nested `stats` | `fixture_stats` | one row per `(fixture_id, identifier, element)` |
| `event/{gw}/live` | `gameweeks` | one row per `(element_id, round)` |
| `element-summary/{player_id}` → `history[]` | `player_histories` | one row per `(element_id, round, fixture)` |

## Structural Flattening Only

The ingest layer may flatten nested API payloads into a tabular shape, for example:

- unpacking `event/{gw}/live` element stats into `gameweeks`
- unpacking fixture `stats` arrays into `fixture_stats`
- extracting nested event fields into a flat event row

This is structural normalization only. It must not:

- aggregate multiple source rows into one analytical record
- compute new metrics that the API does not provide
- enrich the payload with external business logic

## Table Grain

### `gameweeks`

- Source: live endpoint `/event/{gw}/live/`
- Grain: one row per player per round
- Uniqueness: `(element_id, round)`
- Purpose: store live gameweek-level player stats exactly as exposed by the live endpoint

### `player_histories`

- Source: player endpoint `/element-summary/{player_id}/`
- Source sub-object: `history[]`
- Grain: one row per player per fixture within a round
- Uniqueness: `(element_id, round, fixture)`
- Purpose: preserve source fidelity for player history, including double gameweeks

This table intentionally does not collapse multiple fixtures from the same round into a single row.

## Grain Policy

The key policy is:

- ingest preserves source fidelity where the upstream source is fixture-grain
- ingest stores live round data at its native round grain
- ingest persists API-provided values only, aside from minimal structural flattening
- downstream systems may aggregate fixture-grain history into canonical gameweek facts

This avoids silent data loss during double gameweeks and keeps the ingest layer focused on collection and persistence rather than business-level aggregation semantics.

## Idempotency

Each persisted table uses a uniqueness constraint aligned to its grain. Re-ingesting the same source rows should update the existing row for that grain rather than duplicate it.

## Notable Column Encodings

### `events.chip_plays_json`

The FPL API returns `chip_plays` as a nested list of objects (chip name and number of plays). SQLite has no native array type, so this field is serialized to a JSON string before storage:

```
chip_plays_json TEXT  -- e.g. '[{"chip_name": "wildcard", "num_played": 1234}, ...]'
```

Consumers must parse this column with a JSON function or application-side deserialisation. It is not directly filterable as a scalar. This is intentional structural flattening under the data contract.

## Schema Validation Behaviour

All models use `extra="forbid"`. If the FPL API adds a new field to any response, the model will raise a `ValidationError` and the affected rows will be skipped and counted as `skipped` in the stage summary and `_runs` table.

This is the correct defensive posture: unknown fields are rejected rather than silently ignored, preventing silent schema drift. Downstream consumers should monitor the `skipped` count in `_runs` as a leading indicator of upstream API schema changes.

## System Columns

Every persisted table receives the following column, injected by the storage layer and not present in any Pydantic model:

| Column | Type | Description |
|---|---|---|
| `ingested_at` | `TEXT` (ISO 8601 UTC) | Timestamp of the ingest run that wrote or last updated the row. |

Downstream consumers may use `ingested_at` for freshness checks or incremental filtering. It is set once per `upsert_models` call and is overwritten on each subsequent upsert of the same row.

The `_runs` table records one row per completed pipeline stage and is not part of the domain schema:

| Column | Type | Description |
|---|---|---|
| `id` | `INTEGER` | Auto-increment primary key. |
| `started_at` | `TEXT` (ISO 8601 UTC) | Timestamp when the containing pipeline run started. |
| `stage` | `TEXT` | Stage name (e.g. `core`, `fixtures`, `gameweeks`, `player_histories`). |
| `fetched` | `INTEGER` | Rows fetched from the API. |
| `upserted` | `INTEGER` | Rows written to SQLite. |
| `skipped` | `INTEGER` | Rows that failed Pydantic validation and were not persisted. |
| `errors` | `INTEGER` | Network or processing errors during the stage. |

## Downstream Responsibility

If a consumer needs one canonical player-gameweek fact table, it should build that from `player_histories` according to its own business rules. `fpl-ingest` intentionally keeps `gameweeks` and `player_histories` separate rather than imposing one aggregation policy on all downstream consumers.
