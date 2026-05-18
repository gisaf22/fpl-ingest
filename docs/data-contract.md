# Data Contract

This document defines the persisted table grain, constraints, and field meaning for `fpl-ingest`.

## Scope

`fpl-ingest` persists values provided by the upstream API, plus minimal structural flattening needed to store nested payloads in relational tables. It does not derive analytics fields, aggregate records across rows, or invent new metrics.

`fpl-ingest` is not responsible for collapsing fixture-grain history into a single canonical player-gameweek fact. This project persists both contracts explicitly: live gameweek rows in `gameweeks`, and per-fixture history rows in `player_histories`.

## Source To Table Mapping

| Source endpoint | Table | Grain |
|---|---|---|
| `bootstrap-static` -> `elements` | `players` | one row per player |
| `bootstrap-static` -> `teams` | `teams` | one row per team |
| `bootstrap-static` -> `events` | `events` | one row per event/gameweek |
| `bootstrap-static` -> `element_types` | `element_types` | one row per element type |
| `fixtures` | `fixtures` | one row per fixture |
| `fixtures` -> nested `stats` | `fixture_stats` | one row per `(fixture_id, identifier, element)` |
| `event/{gw}/live` | `gameweeks` | one row per `(element_id, round)` |
| `element-summary/{player_id}` -> `history[]` | `player_histories` | one row per `(element_id, round, fixture)` |

## Structural Flattening Only

The ingest layer may flatten nested API payloads into a tabular shape, for example:

- unpacking `event/{gw}/live` element stats into `gameweeks`;
- unpacking fixture `stats` arrays into `fixture_stats`;
- extracting nested event fields into a flat event row.

This is structural normalization only. It must not:

- aggregate multiple source rows into one analytical record;
- compute new metrics that the API does not provide;
- enrich the payload with external business logic.

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

- ingest preserves source fidelity where the upstream source is fixture-grain;
- ingest stores live round data at its native round grain;
- ingest persists API-provided values only, aside from minimal structural flattening;
- downstream systems may aggregate fixture-grain history into canonical gameweek facts.

This avoids silent data loss during double gameweeks and keeps the ingest layer focused on collection and persistence rather than business-level aggregation semantics.

## Constraints

Each persisted table uses a uniqueness constraint aligned to its grain.

Unknown upstream fields are rejected by the model contract. This prevents silent contract drift when the FPL API adds fields that are not represented in the local schema.

## Notable Column Encodings

### `events.chip_plays_json`

The FPL API returns `chip_plays` as a nested list of objects with chip name and number of plays. SQLite has no native array type, so this field is serialized to a JSON string before storage:

```sql
chip_plays_json TEXT
```

Consumers must parse this column with a JSON function or application-side deserialization. It is not directly filterable as a scalar. This is intentional structural flattening under the data contract.

## System Columns

Every persisted public table receives the following column, injected by the load layer (`load/store.py`) and not present in any Pydantic model:

| Column | Type | Description |
|---|---|---|
| `ingested_at` | `TEXT` (ISO 8601 UTC) | Timestamp of the ingest run that wrote or last updated the row. |

## Schema Versioning

The schema contract version is declared as `SCHEMA_VERSION` in `src/fpl_ingest/schema/definition.py` and embedded in the compiled artifact at `artifacts/contract/schema_contract.json`.

### Semantic Versioning Rules

| Increment | Change type | Examples |
|-----------|-------------|---------|
| **Major** | Breaking structural change | Column removed, column renamed, column type changed, table grain changed |
| **Minor** | Additive non-breaking change | New nullable column added, new table added |
| **Patch** | Documentation or metadata only | Field notes updated, description reworded with no structural change |

### Human-Controlled Versioning

`fpl-ingest` never auto-increments the version. Versioning is a human decision that requires understanding the downstream impact of a change.

## Downstream Responsibility

If a consumer needs one canonical player-gameweek fact table, it should build that from `player_histories` according to its own business rules. `fpl-ingest` intentionally keeps `gameweeks` and `player_histories` separate rather than imposing one aggregation policy on all downstream consumers.
