# fpl-ingest — agent rules

Repo-level rules. These sit on top of the global engineering policy in `~/.claude/CLAUDE.md`.

---

## What this repo is

Raw-capture-only ingestion of Fantasy Premier League API data. Each stage fetches an FPL
endpoint and writes the response **verbatim** to S3 (bucket `fpl-data-safari`) — payload
bytes, a per-object metadata sidecar, and the run manifest.

It does not flatten, normalise, or build structured tables. Flatten-and-upsert is
fpl-warehouse's job downstream; the SQLite write paths were deliberately removed rather
than dual-written during the migration (`src/fpl_ingest/extract/stages/fixtures.py`
module docstring; `PUBLIC_TABLES` is now empty — `src/fpl_ingest/schema/__init__.py`).

Stages live in `src/fpl_ingest/extract/stages/`: `bootstrap.py`, `fixtures.py`,
`event_status.py`, `gameweeks.py` (the `event-live/{gw:02d}` endpoint), `element_summary.py`.

---

## Tests

```bash
uv run pytest -m unit          # fast default
uv run pytest -m integration   # real internal components, FPL API faked
uv run pytest tests/e2e        # opt-in only — hits the real FPL API
```

Tiers are `unit` / `integration` / `e2e`, one per directory under
`tests/{unit,integration,e2e}/`, with the marker applied automatically by each tier
directory's conftest. `testpaths` is `["tests/unit", "tests/integration"]` — e2e is
excluded from the default run because it calls the real API.

---

## Architecture invariants

**`RawStorageBackend` is write-only by design.** Ingestion never reads back what it wrote.
The only read the protocol offers is `exists_prefix`, an existence check
(`src/fpl_ingest/extract/http/local_writer.py`). Skip and finality logic must be expressed
as existence checks against the active backend, never as content reads — checking a
hardcoded local path instead of the active backend was a real bug (commit `8f6b7bb`).

**Endpoint refetch policy.**

| Endpoint | Policy |
|---|---|
| `event-live/{gw:02d}` | Skip once the gameweek is settled (bonus added) **and** already captured |
| `element-summary/{player}` | Skip once settled and captured; the settlement transition forces one full refetch (commit `412516c`) |
| `bootstrap-static` | Always refetch every run |
| `fixtures` | Always refetch every run |
| `event-status` | Always refetch every run — it is the finality signal |

---

## Tooling

Use `/opt/homebrew/bin/gh` explicitly for GitHub CLI commands. Bare `gh` on PATH resolves
to a pyenv shim (`~/.pyenv/shims/gh`), not the real GitHub CLI.

---

## Git safety

This repo has suffered a real data-loss incident: `git checkout` on a path that had been
`git mv`'d while carrying unstaged changes. **Commit before any multi-file structural
operation** — renames, moves, or deletions spanning multiple files.
