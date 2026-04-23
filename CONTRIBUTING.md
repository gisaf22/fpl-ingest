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

- Keep the README concise and operator-facing.
- Put deeper implementation or contract details under `docs/`.
- Preserve source fidelity in ingest whenever possible.
- Keep ingestion concerns separate from downstream analytics modeling.
- Add or update tests with behavior changes.
- Update docs when CLI flags, environment variables, table grain, or persisted contracts change.

## Pull Request Guidance

- Keep changes focused and easy to review.
- Include a short summary of the user-visible or contract-visible impact.
- Call out breaking changes explicitly.
- If a change affects downstream consumers of the persisted tables or CLI, note that clearly in the PR description.

## Data Contract Changes

Treat these as higher-risk changes:

- renaming a persisted table
- changing table grain or uniqueness rules
- removing or renaming CLI flags or environment variables
- changing the meaning of persisted columns

When making those changes:

1. Update tests in the same change.
2. Update `docs/data-contract.md` and any related docs.
3. Describe migration or downstream impact explicitly.
