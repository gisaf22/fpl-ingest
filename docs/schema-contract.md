# Schema Contract

`fpl-ingest` publishes a public schema contract for the SQLite tables that downstream consumers are expected to use. This is the single canonical schema-contract document for the repo.

## Who This Is For

- analysts building queries on top of the SQLite dataset
- downstream applications consuming exported table metadata
- operators validating that a local DB still matches the expected contract

## What Is Included

The public contract covers the domain tables only:

- `players`
- `teams`
- `fixtures`
- `fixture_stats`
- `gameweeks`
- `player_histories`
- `events`
- `element_types`

The contract excludes internal operational tables such as `_runs` and `_metadata`.

## Canonical Source And Artifact

- Canonical source: `src/fpl_ingest/domain/schema.py`
- Exported artifact: `artifacts/contract/schema_contract.json`

The artifact is generated from the canonical source. Consumers should rely on the artifact or CLI export output, not on SQLite introspection alone.

## What The Contract Represents

The contract is the reproducible, consumer-facing definition of the public SQLite schema:

- which public tables exist
- the grain and description of each table
- which columns are part of each table
- the SQLite type, nullability, and primary-key status of each column

It intentionally excludes internal operational tables such as `_runs` and `_metadata`.

## DB Path Resolution

Schema commands operate on the resolved active database path. They do not assume the README default location.

Resolution order:

1. `--db`
2. `FPL_DB_PATH`
3. `~/.fpl/config.yaml` key `db_path`
4. `~/.fpl/fpl.db`

This matters because users may ingest data to custom locations.

## Export The Contract

Write the default artifact:

```bash
uv run fpl-ingest schema export
```

Write to a custom location:

```bash
uv run fpl-ingest schema export --out /tmp/schema_contract.json
```

The export is generated from code, not from live database introspection.

## Regenerate The Checked-In Artifact

Regenerate `artifacts/contract/schema_contract.json` from the canonical generator:

```bash
uv run fpl-ingest schema export
```

Before a release or contract change:

1. regenerate the artifact
2. review the diff in `artifacts/contract/schema_contract.json`
3. run the schema contract tests
4. commit the regenerated artifact with the code change

## Validate A Live Database

Validate the resolved active DB:

```bash
uv run fpl-ingest schema validate
```

Validate a specific DB:

```bash
uv run fpl-ingest schema --db /path/to/custom.db validate
```

Validation outcomes:

- `valid`: the live DB matches the public contract
- `drift`: the live DB has extra columns not present in the contract
- `invalid`: required tables or columns are missing, or supported type checks fail

Exit codes:

- `0` valid
- `2` valid with drift
- `1` invalid

## Intentional Schema Changes

When the persisted schema changes intentionally:

1. update the canonical contract metadata in `src/fpl_ingest/domain/schema.py`
2. export the contract artifact again
3. run the schema contract tests
4. commit the regenerated artifact

Normal runtime validation and export commands do not rewrite source files.

## Compatibility Notes

The current contract version is tracked in the exported artifact. The project does not yet define full semantic versioning rules for schema evolution, so consumers should review contract diffs when upgrading.
