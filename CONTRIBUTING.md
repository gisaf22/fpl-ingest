# Contributing

Thanks for contributing to `fpl-ingest`.

## Workflow

1. Install dependencies:

```bash
uv sync
```

2. Run the full test suite:

```bash
uv run pytest -q
```

3. Run the upstream smoke test when touching API-facing code:

```bash
uv run fpl-ingest smoke-test
```

## Project Expectations

- There is no README.md in this repo yet (known gap).
- Put deeper implementation or contract details under `docs/`.
- Preserve source fidelity in ingest whenever possible.
- Keep ingestion concerns separate from downstream analytics modeling.
- Add or update tests with behavior changes.
- Update docs when CLI flags, environment variables, or the raw-capture contract (S3 key layout, manifest/metadata schema, or validation shape checks) change.

## Pull Request Guidance

- Keep changes focused and easy to review.
- Include a short summary of the user-visible or contract-visible impact.
- Call out breaking changes explicitly.
- If a change affects downstream consumers of the raw captures or CLI, note that clearly in the PR description.

## Data Contract Changes

Treat these as higher-risk changes:

- changing the S3 key layout (`raw/{source}/{endpoint}/{extraction_date}/{run_id}/payload.*`)
- changing the manifest or per-object `metadata.json` schema
- changing the shape checks in `src/fpl_ingest/schema/validation.py`
- removing or renaming CLI flags or environment variables

When making those changes:

1. Update tests in the same change.
2. Update `docs/architecture/fpl-ingest-strategy.md` and any related docs.
3. Describe migration or downstream impact explicitly (this repo's raw captures feed the `fpl-warehouse` build).
