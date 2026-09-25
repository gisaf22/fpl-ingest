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
| `event-live/{gw:02d}` | Fetch once per gameweek, after it is ratified (bonus added) and only while its `_settlement/event-live/{gw}` marker is absent; the marker is written only after a clean, shape-valid capture whose played rows carry published ICT (`readiness.ict_ready`). Provisional gameweeks are not fetched; unknown finality fetches nothing |
| `element-summary/{player}` | Skip once settled and captured; the settlement transition forces one full refetch, gated by its own `_settlement/element-summary/{gw}` marker (commit `412516c`), written only once that gameweek's re-fetched rows carry published ICT (`readiness.ict_ready`) |
| `bootstrap-static` | Always refetch every run |
| `fixtures` | Always refetch every run |
| `event-status` | Always refetch every run — it is the finality signal |

The two `_settlement` markers are separate on purpose: different keys, written and checked
independently. Never merge them into one shared flag — each must only ever mean "this
stage's own capture succeeded," or one stage's success could vouch for the other's failure.

---

## Tooling

Bare `gh` is the real GitHub CLI (`/opt/homebrew/bin/gh`). The pyenv shim that used to
shadow it (`~/.pyenv/shims/gh`) was removed on 2026-09-24.

---

## Reading captures

- **Effective capture time is `received_at` minus the CDN age.** Both are in the per-object
  sidecar `metadata.json`: `received_at` (top level, ISO-8601 UTC) and
  `response_headers.age` (header names are lowercased; the value is a string of seconds).
  FPL's CDN sends `cache-control: max-age=300, stale-while-revalidate=3600`, so a response
  is usually at most 5 minutes old but can be older. Across 178 bootstrap-static captures
  (2026-08-29 to 2026-09-24) the median age was 101s, the maximum 426s, and 5 were over 300s.
- **Select pre-deadline snapshots by the run manifest's `trigger == "pre_deadline"`**, not
  by "latest capture before the deadline". A capture with `trigger: "manual"` or
  `"scheduled"` can also precede a deadline.
- **Manifests written before 2026-09-24 17:16 UTC (PR #15, `ccca196`) have no `trigger`
  key**, including that morning's 07:20 daily run, and local runs without
  `--trigger` record `null`. Treat both as unknown.

---

## Git safety

This repo has suffered a real data-loss incident: `git checkout` on a path that had been
`git mv`'d while carrying unstaged changes. **Commit before any multi-file structural
operation** — renames, moves, or deletions spanning multiple files.

---

## Working on board items

Items on the [FPL Platform board](https://github.com/users/gisaf22/projects/3) follow
[AGENT_WORKFLOW.md](https://github.com/gisaf22/.github/blob/main/AGENT_WORKFLOW.md) in
`gisaf22/.github`. "Pick up #N" means: run that procedure for #N — pick up → tests →
implement → PR → close. Read it before starting. This section is identical in fpl-ingest,
fpl-warehouse and fpl-intelligence; change all three together.

Must-follow rules:

- **No acceptance criteria table → stop.** Add the `needs-spec` label and ask.
- **Move the item to In Progress** when you pick it up. Bigger than its Size → stop and
  propose a split.
- **Tests come from the acceptance criteria only**, one or more per AC at its test tier,
  each marked `covers("#<issue> AC<n>")`, with plain-English names. Don't invent tests;
  flag any you think are missing. Manual/e2e tiers: record the result on the PR or issue.
- **Commit the tests first (failing), then stop and report for approval** before implementing.
- **Stay inside Out of scope.** Spec wrong or ambiguous → stop and ask; don't improvise.
- **PR body says `Closes #N`; post AC results (pass/fail per AC, with evidence) as a PR
  comment; tick the Definition of Done** (N/A with reason where it doesn't apply).
- **Never merge.** The human merges.
- **After merge:** confirm the item is Done, then move every item it was blocking that has
  no other open blocker from Blocked to Todo.
- **New items:** use the `gisaf22/.github` templates, create with `--body-file`, and set
  Work Item Type, Epic, Size, Status and parent.
- **Public board:** no account IDs, ARNs or secrets in any issue, PR or comment.
