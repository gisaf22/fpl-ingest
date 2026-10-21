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
`event_status.py`, `gameweeks.py` (the `event-live/{gw:02d}` endpoint), `element_summary.py`,
`manager_panel.py` (the seeded manager panel and each kept manager's `entry-history`, #102;
`fpl-ingest manager-panel`), `entry_picks.py` (each kept manager's picks per settled gameweek,
#116; `fpl-ingest entry-picks`).

Before design work, read [`docs/adr/`](docs/adr/README.md): decisions recorded there are
settled unless the work says why to reopen them.

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

(Why these hold: [ADR 005](docs/adr/005-write-only-raw-storage.md).)

**`RawStorageBackend` is write-only by design.** Ingestion never reads back what it wrote.
The only read the protocol offers is `exists_prefix`, an existence check
(`src/fpl_ingest/extract/http/local_writer.py`). Skip and finality logic must be expressed
as existence checks against the active backend, never as content reads — checking a
hardcoded local path instead of the active backend was a real bug (commit `8f6b7bb`).

**The one exception is the manager panel file** (#100 D5, #102): `read_panel_bytes` reads back
`fpl/_panels/{season}/manager-panel.json`, the job's own panel, and refuses any other key
(`raw_keys.validate_panel_key`). Only `stages/manager_panel.py` calls it. Tests fail if any
other module reads through the backend, or if the ingestion package reads S3 outside
`s3_backend.py`. (`backfill.py` is a separate catalog tool with its own client, outside this rule.)

**Storage calls run concurrently; only the writer's state is locked** (#123, [ADR
006](docs/adr/006-picks-resume-and-settlement.md)). The panel and picks stages run every
backend and writer call through `OffLoop`, a pool of `MAX_CONCURRENT_REQUESTS` threads with
no lock of its own. `LocalRawWriter` guards its in-memory state and the manifest flush with
an internal `threading.Lock`, and runs payload and sidecar PUTs outside it. Code called
through `OffLoop` must be safe across threads. Never serialize storage calls again: one
lock around them capped the first picks run at 1.8 requests/s.

**Endpoint refetch policy.**

| Endpoint | Policy |
|---|---|
| `event-live/{gw:02d}` | Fetch once per gameweek, after it is ratified (bonus added) and only while its `_settlement/event-live/{gw}` marker is absent; the marker is written only after a clean, shape-valid capture whose played rows carry published ICT (`readiness.ict_ready`). Provisional gameweeks are not fetched; unknown finality fetches nothing |
| `element-summary/{player}` | Skip once settled and captured; the settlement transition forces one full refetch, gated by its own `_settlement/element-summary/{gw}` marker (commit `412516c`), written only once that gameweek's re-fetched rows carry published ICT (`readiness.ict_ready`) |
| `bootstrap-static` | Always refetch every run |
| `fixtures` | Always refetch every run |
| `event-status` | Always refetch every run — it is the finality signal |
| `entry-history/{season}/{entry}` | Once per kept manager, at panel draw. The panel is drawn once per season; a run that finds the panel file fetches nothing. A drawn id with an existing capture counts as kept and is not refetched, so a rerun after a failed draw resumes (#102 P6) |
| `entry-picks/{season}/{gw}/{entry}` | Once per kept manager per gameweek that is finished and data-checked, oldest first, while the gameweek's `_settlement/entry-picks/{gw}` marker is absent. An entry is skipped when `exists_prefix` finds its payload tree or, for the random cohort only, its `_invalid/not_found` tree; `shape_invalid` and `http_error` trees are never checked, so those entries retry. The marker is written only when every kept entry of the gameweek is captured, and the next gameweek never starts before it. A wall-clock budget (`--max-seconds` / `FPL_ENTRY_PICKS_MAX_SECONDS`) ends the run cleanly (#116, [ADR 006](docs/adr/006-picks-resume-and-settlement.md)) |

The three `_settlement` markers are separate on purpose: different keys, written and checked
independently. Never merge them into one shared flag — each must only ever mean "this
stage's own capture succeeded," or one stage's success could vouch for the other's failure.

---

## Payload baselines

`schemas/payload-baseline/<endpoint>.json` lists every field path each endpoint's payload
carries and its JSON type(s), plus the samples it was built from (#81; decisions on #80).
Regenerate from live fetches, never from S3:
`uv run fpl-ingest baseline <endpoint> [--players 1,2] [--gameweeks 5,6] [--entries 8450051,4685107]`
(`entry-history` needs `--entries`; `entry-picks` needs `--entries` and `--gameweeks`). By default the
fetches are unioned with the committed file, so a rarely seen field never drops out;
`--replace` builds from the fresh fetches only, and is the only way to remove a field.
Accepting a drift means merging the regenerated file. Integer and decimal are one type,
`number`.

---

## Tooling

Bare `gh` is the real GitHub CLI (`/opt/homebrew/bin/gh`). The pyenv shim that used to
shadow it (`~/.pyenv/shims/gh`) was removed on 2026-09-24.

The local shell is **zsh**, not bash. An unquoted `$var` is not word-split, so
`fpl-ingest baseline $a` passes `event-live --gameweeks 1,2` as one argument and argparse
rejects it (#86). An unmatched glob such as `docs/*.md` or `--include=*` is an error, not a
literal. Use arrays or `${=var}`, quote globs, and never filter `error:` out of a command's
output.

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
- **A pre-deadline run captures bootstrap-static and fixtures** in one run under one
  manifest (#46), and so does a `--force` run (`trigger: "manual"`). Pre-deadline manifests
  from before #46 merged list `bootstrap-static` only; a consumer must treat a missing
  `fixtures` entry as "not captured", not as an error. If one endpoint fails, the other is
  still written, the manifest lists the failed one under `failures`, and the run is PARTIAL
  and exits non-zero.
- **Judge each endpoint of a run by the manifest's `endpoints` block** (#49, contract
  1.1.0), keyed by endpoint (`element-summary`, not `element-summary/115`), each with
  `attempted`, `usable`, `failed`, `outcome` and `failures` (every one with a `reason`).
  - **written vs usable:** `objects[...].written` counts payloads stored; `endpoints[...].usable`
    counts payloads stored *and* passing their shape check, so a shape-invalid capture is
    written (under `_invalid`, since #54/#55) but not usable.
  - **SUCCESS:** everything attempted is usable.
  - **PARTIAL:** some usable, some failed.
  - **FAILED:** nothing usable — including an endpoint not attempted because an earlier stage
    failed (`attempted: 0`, reason names that stage).
  - An endpoint the refetch policy deliberately didn't fetch is absent, not FAILED. Manifests
    before 1.1.0 have `objects` counts but no `endpoints` block.
- **Find a run's captures from its manifest's `captures[]`** (#62, contract 2.1.0), not by
  reading sidecars. There is one entry per body written, with its full bucket key
  (`raw/fpl/...`), `shape_ok`, `usable` and `season`. It appears only in the finalized
  manifest, so ignore `IN_PROGRESS` manifests. Fetch failures stay in `failures` and have no
  entry. Manifests before 2.1.0 have no `captures[]`.
- **An invalid response is not at `payload.json`** (#54, contract 2.5.0, [ADR
  004](docs/adr/004-failure-classification.md)). A response that is not a valid capture
  (a non-2xx, or a 2xx that fails its shape check) is stored at
  `raw/fpl/_invalid/{class}/{endpoint}/{date}/{run_id}/response.body`, with a
  `metadata.json` beside it. `class` is `not_found`, `shape_invalid` or `http_error`,
  and the sidecar records `class` and `reason`. Its `captures[]` entry has `usable:
  false` and the `class`, and its endpoint counts as failed. A glob for `payload.json`
  cannot match it. Every stage does this (fixtures from #54, bootstrap-static,
  event-status, event-live and element-summary from #55, the manager panel from #115,
  entry-picks from #116); captures
  from before then may still hold a failed response at `payload.json` with `usable: false`.
  `fetch_failed` (no response, or 429/5xx after retries) stores nothing.
- **An expected invalid capture is not a failure** (#115, contract 2.7.0, [ADR
  004](docs/adr/004-failure-classification.md)). When the caller declared the outcome
  legitimate for its request (the manager panel's random-cohort 404 and empty `current`,
  and entry-picks' random-cohort 404, a manager's pre-join week; a self-cohort 404 is not),
  the sidecar and the `captures[]` entry carry `expected: true`, and the endpoint's
  `expected` count replaces a `failed`. Both are absent, never `false`, otherwise, and a
  reader treats a missing count as zero and manifests before 2.7.0 as having none. The
  capture is still `usable: false` and still in `invalid_counts`. An endpoint is SUCCESS
  when nothing failed, so judge by `failed`, not by `usable == attempted`.
- **Invalid counts and aborts are on the finalized manifest** (#109, contract 2.6.0,
  [ADR 004](docs/adr/004-failure-classification.md)): `invalid_counts`
  `{not_found, shape_invalid, http_error}`, zero-filled, and `abort_reason`
  (`aborted_http_error_streak`) when the run stopped itself after N consecutive
  `http_error` responses (default 10; `--http-error-streak` / `FPL_HTTP_ERROR_STREAK`).
  `abort_reason` is absent otherwise, and manifests before 2.6.0 have neither key.
- **A picks run's budget, throughput and spike count are on the finalized manifest** (#116,
  contract 2.8.0, [ADR 006](docs/adr/006-picks-resume-and-settlement.md)). `stop_reason`
  (`stopped_at_budget`) is top-level and absent unless the run stopped at its wall-clock
  budget; such a stop is not a failure unless it captured nothing new, which also marks
  `entry-picks` FAILED. `endpoints["entry-picks"].throughput` is `{requests, total_seconds,
  requests_per_second, gameweeks_completed}` and `.non_2xx_by_gameweek` maps a gameweek to
  its stored `not_found` plus `http_error` picks responses this run; both appear only when the
  run made at least one picks request. An absent `_settlement/entry-picks/{gw}` marker means
  that gameweek still has entries not captured. Manifests before 2.8.0 have none of these.
  A picks run rewrites its `IN_PROGRESS` manifest every 50 objects or 10 s rather than
  after each object (#123). A stage that raises still flushes first, so only a run killed
  outright (a job timeout) leaves a trace lagging by up to that. Every other run still
  rewrites after each object.
- **`season` is on every 2.1.0 sidecar and capture entry**, derived from the same run's
  bootstrap-static (the deadline year of its lowest-id event). It is `null`, logged at ERROR
  as `season_source=none`, when the run had no usable bootstrap; there is no fallback. The
  field list for both files is `schemas/raw-contract/2.2.0/`, which replaces strategy doc
  §A.5's tables.
- **Payload drift is in each FPL sidecar's `drift` block** (#82, contract 2.3.0):
  `{status, reason, entries}`, status `ok` / `drift` / `unavailable`. Each entry has
  `endpoint` (baseline family), `path`, `kind` (`added` / `removed` / `type_changed`),
  `baseline_types`, `observed_types` and `count`. It is warn-only: it never changes
  `usable`, run status, the exit code or a `_settlement` marker. `unavailable` (missing or
  corrupt baseline, non-JSON payload, internal error) carries a `reason` and is logged at
  WARNING. Sidecars from other sources, and sidecars before 2.3.0, have no `drift` key.
- **Per-endpoint drift is in the finalized manifest's `endpoints[...].drift`** (#85,
  contract 2.4.0): `{status, checked, reasons, entries}`, with each entry `{path, kind,
  baseline_types, observed_types, payloads}` grouped across the endpoint's payloads.
  `status` is the worst case (`unavailable` > `drift` > `ok`), and an `unavailable`
  endpoint can still carry entries, so act on `entries` whatever the status. There's no
  block on `IN_PROGRESS` manifests, on endpoints whose captures all failed to fetch, or on
  manifests before 2.4.0.
- **Tell a production run from a laptop run by the manifest's `origin`** (#75, contract
  2.2.0), not by `git_sha` or `trigger`: a laptop run from a checkout records a real SHA, and
  `--trigger` takes any value. `origin.kind` is `ci` or `local`; for `ci`, `workflow`, `ref`
  and `github_run_id` link to the Actions run. These are self-reported from `GITHUB_*`
  variables, so read them as audit, not proof. `aws_principal` is the role or IAM user name
  STS resolved the run's credentials to (`"root"` for the root user, `null` if the lookup
  failed). Production is the reader's call, e.g. `kind == "ci"` and
  `ref == "refs/heads/main"`. Present on every 2.2.0 manifest, `IN_PROGRESS` included;
  manifests before 2.2.0 have no `origin`, and their origin comes from Actions run history.
- **Run `status` is SUCCESS / PARTIAL / FAILED by the same rule, over the whole run** (#48,
  contract 2.0.0): SUCCESS when every endpoint is SUCCESS, PARTIAL when something is usable
  and something failed, FAILED when nothing is usable (a run with no endpoints included). A
  policy skip never makes a run PARTIAL. Status says what is usable; the exit code (non-zero
  for anything but SUCCESS) says whether to alert, so a strict-mode abort is still recorded
  by what it left usable.
- **Records before 2.0.0 use a different status vocabulary. Do not filter them on status
  alone:**
  - old `FAILED` covers any fetch error, so such a run may still have usable captures. Use
    each endpoint's `usable` count or `outcome` (1.1.0 records). Records before 1.1.0 have
    neither, only `objects` counts.
  - old `FAILED_PARTIAL` means payloads were written but some failed their shape check.
- **Manifests written before 2026-09-24 17:16 UTC (PR #15, `ccca196`) have no `trigger`
  key**, including that morning's 07:20 daily run, and local runs without
  `--trigger` record `null`. Treat both as unknown.

---

## Monitoring

The daily and pre-deadline workflows ping a [healthchecks.io](https://healthchecks.io) check
at the end of every scheduled run (#57; design decisions on #34); the picks workflow has no
check (below). A check alerts by email when a run
reports failure, and also when no ping arrives within the check's schedule plus grace. That
second case covers runs that never start and GitHub Actions outages.

| Check | Workflow | Schedule | Timezone | Grace | Ping URL secret |
|---|---|---|---|---|---|
| `fpl-ingest pre-deadline` | `scheduled_run_pre_deadline.yml` | `7,22,37,52 8-19 * * *` | UTC | 1 h | `HEALTHCHECKS_PING_URL_PRE_DEADLINE` |
| `fpl-ingest daily` | `scheduled_run_daily.yml` | `0 7,19 * * *` | UTC | 1 h | `HEALTHCHECKS_PING_URL_DAILY` |

fpl-warehouse's scheduled build has its own check, documented in that repo.

- **Outcome comes from the job's exit code**, via `job.status`. `success` pings the plain
  URL, and `failure` or `cancelled` pings `<url>/fail`. The command exits 1 for PARTIAL and
  FAILED, so those report `/fail`. So does any failure before the command runs, such as
  `uv sync` or OIDC. A pre-deadline run outside the window succeeds and pings success.
- **Scheduled runs only.** A manual dispatch, including `force=true`, does not ping, so it
  can neither raise an alert nor clear one while the schedule is broken.
- **The ping can never fail the job.** `.github/scripts/healthchecks_ping.sh` always exits
  0: it logs a notice and sends nothing when the secret is empty (a fork or fresh clone), and
  it logs a warning when the request fails. The step is also `continue-on-error` with a
  2-minute timeout. A lost ping reads as absence and alerts after the grace, which is the
  accepted false positive. The same holds for a failed checkout, which leaves no script to
  run.
- **The ping URLs are secrets, not variables.** The repo is public, and anyone holding a URL
  could send a false success ping. Never paste one into an issue, PR or log.
- **Grace sizing**, measured over scheduled runs to 2026-09-28. Pre-deadline runs start 8–22
  minutes after their cron time and take at most 37 s. Daily runs finish at most 35 minutes
  after cron. The daily job has no `timeout-minutes`, so a hung run alerts through absence
  after 1 h.
- **Drift issues** (#83). Each scheduled workflow has a `report-drift` job. It reads
  `drift-report.json`, which the capture writes from its finalized manifest, and adds the
  drift to the job summary, raises one `::warning::` per drifted endpoint, and opens one
  `schema-drift` issue per new drift. Its dedup key is in the issue body. An issue with that
  key, open or closed, suppresses a new one, and a repeat sighting adds no comment. To
  accept a drift, regenerate the baseline and open a PR that closes the issue; to dismiss it,
  close the issue as not planned. **Close drift issues; never remove their label.** Lookup
  is by label, so an unlabelled issue is opened again on the next run. The job alone holds
  `issues: write` and is `continue-on-error`, so drift never turns a run red or changes its
  ping.
- **healthchecks.io is the only failure alert for daily and pre-deadline.** The SMTP "Email
  on failure" steps were removed once its alerts had been seen working (#58), so each failure
  raises one alert.
- **The picks workflow, `scheduled_run_entry_picks.yml`, has no healthchecks check** (#104).
  It runs `fpl-ingest entry-picks --trigger scheduled` daily at `25 17 * * *` UTC, and a
  failure shows only as a red run and GitHub's failed-run email. That email goes to whoever
  last changed the workflow's cron, if their Actions failure-email setting is on. A late or
  dropped run alerts nobody; the next day's run resumes. The job's `timeout-minutes: 55`
  covers the 45 min budget plus the tail, inside the 1-hour OIDC credentials. It has no
  `workflow_dispatch`, because a dispatch must not write (R6).

---

## Git safety

This repo has suffered a real data-loss incident: `git checkout` on a path that had been
`git mv`'d while carrying unstaged changes. **Commit before any multi-file structural
operation** — renames, moves, or deletions spanning multiple files.

---

## Working on board items

Items on the [FPL Platform board](https://github.com/users/gisaf22/projects/3) follow
[AGENT_WORKFLOW.md](https://github.com/gisaf22/.github/blob/main/AGENT_WORKFLOW.md) in
`gisaf22/.github`, the one place its rules are written. "Pick up #N" means: run that
procedure for #N. Read it before starting, then apply the domain playbook from
[`playbooks/`](https://github.com/gisaf22/.github/tree/main/playbooks) that fits the item.
This section is identical in fpl-ingest, fpl-warehouse and fpl-intelligence.

Fetch the workflow, and list the playbooks, with:

```sh
gh api repos/gisaf22/.github/contents/AGENT_WORKFLOW.md -H "Accept: application/vnd.github.raw"
gh api repos/gisaf22/.github/contents/playbooks --jq '.[].path'
```
