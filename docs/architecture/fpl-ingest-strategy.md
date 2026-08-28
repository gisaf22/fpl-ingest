# Ingestion Strategy — fpl-ingest and understat-ingest

**Deliverable 4.** Canonical location: `fpl-ingest/docs/architecture/fpl-ingest-strategy.md`.
Referenced, not copied, from `understat-ingest`.

**Date:** 2026-08-24
**Scope:** `fpl-ingest` and `understat-ingest`. `fpl-warehouse` is read-only context.
**Built on:** `fpl-warehouse/docs/architecture/current-state.md` (2026-08-24 audit) and
`fpl-warehouse/docs/architecture/target-state.md` (Deliverable 2).

This document fixes the **target contract** and the **disposition of existing code**. It does
not write migration phases with exit criteria (Deliverable 7) and does not design warehouse
staging models (Deliverable 5).

## Tag legend

| Tag | Meaning |
|---|---|
| **FACT** | Verified during this pass against code, a live payload, a stored payload, a database, or a config file. |
| **INFERENCE** | Plausible reading of the evidence, not directly confirmed. |
| **OPEN QUESTION** | Needs a decision from Fred; cannot be resolved from the repos. |

---

# 0. Settled inputs

Treated as decided. Restated only so the rest of the document is readable standalone.

1. Raw storage is S3, source/endpoint-keyed:
   `s3://<bucket>/raw/{source}/{endpoint}/{extraction_date}/{run_id}/payload.json`
2. Ingestion does **raw capture only**. No flattening, no structured tables, no fact/dim
   modelling, no analytical logic, no grain concepts.
3. The contract is "raw object per source/endpoint/run."
4. `understat-ingest` adopts the identical pattern.

Section 12 records where I think one of these is under-specified rather than wrong. Nothing
in this document silently designs around a settled input.

---

# 2. FPL endpoint inventory

The cheatsheet (cheatography.com/sertalpbilal/cheat-sheets/fpl-api-endpoints, last updated
Oct 2021) was used as a starting list only. Every row below was probed live on 2026-08-24/25
or read from a stored payload under `~/.fpl/raw/`. The live season is **2026/27, GW1, in
progress and not yet final** — which turned out to be the most informative possible moment to
probe.

**FACT — cheatsheet corrections found:**
- `leagues-classical/{id}/standings/` → **404**. The working path is
  `leagues-classic/{id}/standings/` (200).
- `leagues-h2h/{id}/standings/` → 404 for the id tried; the cheatsheet omits h2h entirely.
- `bootstrap-dynamic/` and `game-settings/` → 404. Both are gone; `game_settings` and
  `game_config` are now keys inside `bootstrap-static`.
- **Undocumented endpoint found:** `GET /api/elements/` returns 200 — the bare `elements`
  array from bootstrap-static (1.57 MB, 610 records at probe time) without the surrounding
  `events`/`teams`/`phases`. Not in the cheatsheet, not called by fpl-ingest.
- `fixtures/` accepts `?event={n}` and `?future=1` query parameters (both 200). Neither is in
  the cheatsheet's description.
- `me/` returns 200 unauthenticated with `{"player": null, "watched": []}` — it does not 401.
  A naive auth check against status code would pass while returning nothing.

## 2.1 The four in-scope endpoints

| Endpoint | Called today | Actual response shape (verified) | Update frequency | Historically recoverable? |
|---|---|---|---|---|
| `bootstrap-static/` | **Yes** — `extract/stages/bootstrap.py` via `client.get_bootstrap()` | Object, 10 top-level keys: `chips`(8), `events`(38), `game_settings`(35 keys), `game_config`(3), `phases`(11), `teams`(20), `total_players`(int), `element_stats`(26), `element_types`(4), `elements`(610–841). `elements[i]` has **105 fields**. | Continuous. Prices, `status`, `news`, `chance_of_playing_*`, ownership, `form`, `ep_next` all move daily or faster. | **NO.** Latest-state only; there is no as-of query. Every unobserved moment is lost permanently. |
| `fixtures/` | **Yes** — `extract/stages/fixtures.py` | List of 380 objects, keys `code, event, finished, finished_provisional, id, kickoff_time, minutes, provisional_start_time, started, team_a, team_a_score, team_h, team_h_score, stats, team_h_difficulty, team_a_difficulty, pulse_id`. `stats` is a nested list of `{identifier, h:[{value,element}], a:[...]}`. | Fixture metadata is near-static; `stats`, scores, `finished*` mutate during and after a match. Difficulty ratings are set pre-season. | **Partly.** Final scores/stats settle. Provisional in-play states and any mid-season fixture reschedule are not recoverable. |
| `event/{GW}/live/` | **Yes** — `extract/stages/gameweeks.py` via `client.get_gw()` | Object with a single key `elements`; each element is `{id, stats, explain, modified}`. `stats` has 29 fields; `explain` is a per-fixture point breakdown. 690 elements in the stored GW1 payload. | Live during matches, then settles as bonus and BPS finalise. | **Mostly, but not fully.** The final state is refetchable; in-play intermediate states are not. `explain` is provisional until bonus is applied. |
| `element-summary/{EID}/` | **Yes** — `extract/stages/element_summary.py`, one request per player | Object, 3 keys: `fixtures` (upcoming, per-player, with `difficulty` and `is_home`), `history` (per-fixture rows, 41 fields), `history_past` (per-prior-season aggregates). | `history` grows one row per fixture played; existing rows mutate until finalised. `fixtures` shrinks as the season progresses. | **`history` mostly; `fixtures` NOT AT ALL.** `fixtures` is a forward-looking view of the schedule as of the observation instant. Once a fixture is played it disappears from the array. It is only observable live. |

**FACT — direct evidence that "final" values mutate after first observation.** At probe time
`event-status/` reported GW1 as `points: "p"` (provisional) with `bonus_added: false` on all
four listed dates. Yet `element-summary/115/` already returned `"bonus": 2` and `"bps": 46`
for that same GW1 fixture, while `"influence": "0.0"`, `"creativity": "0.0"`, `"threat":
"0.0"`, `"ict_index": "0.0"` — all still zero. So a payload captured now contains a *partly*
settled row: bonus present, ICT components not yet computed. A capture taken tomorrow will
differ. This is not hypothetical drift; it is happening in the live payload today.

**FACT — `element-summary/{EID}.fixtures` is silently discarded today.**
`element_summary.py::raw_history_rows()` reads only `data["history"]`. `history_past` and
`fixtures` are parsed by nothing. The raw JSON on disk does retain all three keys, so nothing
is lost *at the file level* — but nothing downstream can see them, and the file is
overwritten every run (§4.1).

## 2.2 The two EVALUATE endpoints

### `event-status/` — **capture now**

**FACT — verified live.** Returns `{"status": [...], "leagues": ""}`. Each `status` entry is
`{bonus_added: bool, date: "YYYY-MM-DD", event: int, points: "p"|"r"}`. 285 bytes at probe
time. `"p"` = provisional, `"r"` = ready/final.

**Recommendation: capture it, every run, before anything else.** Reasons, in order:

1. It is the **only endpoint that tells you whether the other payloads are final.** Without it,
   "GW1 data" is an undated claim. With it, every captured object can be labelled provisional
   or settled.
2. It is the correct replacement for the current incremental heuristic. `gameweeks.py:139`
   decides what to re-fetch by checking whether `gw_{n}.json` *exists on disk* — a proxy that
   answers "have I fetched this before," not "is this finished." `event-status` answers the
   real question. (§4.1.)
3. It costs 285 bytes and one request. There is no cost argument against it.
4. It labels every other capture as provisional or settled at the moment it was taken — a
   payload captured while `bonus_added: false` is *known* to be provisional, not silently
   presented as truth, by any downstream reader of that run's captures.

**INFERENCE** — `status` appears to carry one entry per match-date within the current event
window, not one per event. It should not be assumed to be a single-row response.

### `team/set-piece-notes/` — **capture now, cheaply; do not depend on it yet**

**FACT — verified live.** Returns `{"last_updated": "2026-08-24T12:19:41Z", "teams": [...]}`,
2,320 bytes, one entry per team with a `notes` list. At probe time (pre-season week 1) every
sampled note was the placeholder `"Check back for additional notes soon"` with an empty
`source_link`.

**Recommendation: capture, but classify as low-value-for-now.** It carries a
`last_updated` timestamp, which makes it self-dating and cheap to diff. It is genuinely
non-recoverable — notes are edited in place with no history. But at 2 KB per run its cost is
nil, and the argument for capturing it is precisely that its value is unknown and it cannot be
backfilled later. Capture is the reversible choice; skipping is not.

**OPEN QUESTION (Q-B)** — Is penalty/set-piece-taker order a signal fpl-intelligence intends
to model? If yes, this endpoint becomes load-bearing and needs a per-run diff. If no, it is a
2 KB insurance policy. Either way capture it now.

### `dream-team/{GW}/` — **capture is optional; recommend never**

**FACT — verified live.** `dream-team/1/` returns `{"top_player": {id, points}, "team": [{element, points, position} × 11]}`, 498 bytes.

**Recommendation: never.** It is fully derivable from `event/{GW}/live/`, which is already
captured — the dream team is just the top-scoring XI. Capturing it stores a downstream
computation as if it were a source. That is exactly the kind of thing settled input 2 exists
to prevent. If a warehouse model ever wants it, it should compute it.

## 2.3 Out of scope, and why the deferral is deliberate

**FACT — verified live, all returning 200 unauthenticated:** `entry/{TID}/`,
`entry/{TID}/history/`, `entry/{TID}/event/{GW}/picks/`, `leagues-classic/{id}/standings/`,
`stats/most-valuable-teams/`.

These are **not captured**, per the scope decision. Recording the consequence explicitly, so it
is a deferral and not an oversight:

**FACT — `entry/{TID}/event/{GW}/picks/` is the only source of real squad composition and bench
order.** Verified: it returns `active_chip`, `automatic_subs`, `entry_history`, and the picks
list. Nothing in bootstrap-static, fixtures, live, or element-summary contains a manager's
actual XI, captain, vice-captain, bench ordering, or chip usage.

**Consequence:** for as long as this is not captured, any downstream "Starting XI" or
"bench decision" work in fpl-intelligence is operating on a *modelled* squad, not an observed
one, and no backtest against actual past squads is possible. It is also **strictly
non-recoverable**: `picks` for a past gameweek reflect that gameweek's locked squad, but you
can only retrieve it for a `TID` you know, and no historical archive exists if you never asked.

**INFERENCE** — this is the highest-value deferred endpoint by a wide margin, and its cost is
tiny (one request per gameweek for one team ID). The scope decision is respected here, but the
deferral should be reviewed before GW3, not at season end.

`leagues-*` and authenticated endpoints stay out with no reservation — they serve
competitive/social features with no modelling value.

## 2.4 Summary table

| Endpoint | Now | Later | Never | Reason |
|---|---|---|---|---|
| `bootstrap-static/` | ● | | | Already ingested; only source of prices, availability, ownership. Non-recoverable. |
| `fixtures/` | ● | | | Already ingested; schedule and match stats. |
| `event/{GW}/live/` | ● | | | Already ingested; per-GW scoring detail with `explain`. |
| `element-summary/{EID}/` | ● | | | Already ingested; the only source with fixture grain (distinguishes double-gameweeks) and the only source for `history_past`. Also carries `fixtures`, which is non-recoverable once a fixture is played and observable only live — both `fixtures` and `history_past` are currently discarded on write. |
| `event-status/` | ● | | | Finality signal. Labels every other capture provisional or settled. 285 bytes. |
| `team/set-piece-notes/` | ● | | | Non-recoverable, self-dating, 2 KB. Cheap insurance. |
| `elements/` | | | ● | Strict subset of `bootstrap-static.elements`. Pure duplication. |
| `dream-team/{GW}/` | | | ● | Derivable from `event/{GW}/live/`. Storing it stores a computation. |
| `entry/{TID}/event/{GW}/picks/` | | ● | | Deliberate deferral. Only source of real squad/bench data; non-recoverable; review before GW3. |
| `entry/{TID}/`, `entry/{TID}/history/` | | ● | | Follows the picks decision. |
| `leagues-classic/*`, `leagues-h2h/*` | | | ● | Competitive/social. No modelling value. |
| `me/`, authenticated endpoints | | | ● | Out of scope; also returns a misleading 200 when unauthenticated. |

---

# 3. FPL API limits, failure modes, retry behaviour

**FACT — no documented rate limit, and no rate-limit headers returned.** Response headers on
`bootstrap-static/` (probed live): `cache-control: max-age=300, stale-while-revalidate=3600,
stale-if-error=3600`, `edge-control: max-age=300`, served through Fastly (`via: varnish`,
`x-cache: MISS, HIT, HIT`). No `X-RateLimit-*` of any kind. The API is CDN-fronted with a
5-minute edge TTL.

**INFERENCE — the 5-minute edge TTL bounds useful capture frequency.** Polling faster than
every 5 minutes largely returns cached bytes. It also means two "simultaneous" captures from
different network paths can legitimately return payloads minutes apart in freshness. There is
no request-time field in the payload to detect this — which is an argument for the capture
metadata in §8 to record the response `Date` and `Age` headers.

**FACT — current client behaviour** (`extract/http/client.py`, `extract/http/sync_http.py`):
- Async `aiohttp` client, `TokenBucketLimiter` (`rate_limiter.py`), default concurrency 10
  (`_DEFAULT_MAX_CONCURRENT`), connector limit matched to it.
- Requested rate is clamped to `MAX_RATE` via `normalize_rate()` at two independent boundaries
  (`runner.py::_resolve_applied_rate` and `AsyncFPLClient._resolve_rate_limiter`), with a
  warning on clamp. Belt and braces, deliberately.
- Retry: `_fetch_with_retries()` runs `max_retries` attempts. `429` reads `Retry-After` via
  `parse_retry_after()`; retryable 5xx and JSON-decode failures use `compute_retry_delay()`
  exponential backoff; **4xx other than 429 is terminal, no retry** (`_classify_response`).
- The backoff sleep happens *outside* the rate-limiter context manager, so a sleeping retry
  does not hold a concurrency slot. This is a genuinely good design detail and is documented in
  the module docstring.

**FACT — the significant failure-mode weakness: exhausted retries return `None`, not an
exception.** `_fetch_with_retries` logs `"All %d attempts exhausted"` and returns `None`. The
public methods then convert `None` into `FPLClientError` — *except* that in the batch stages,
`asyncio.gather(..., return_exceptions=True)` catches it as one error among many and the run
continues in non-strict mode. In `element_summary.py`, a player whose fetch failed increments
`error_count` and is simply absent from the output. `--strict` exists and does abort
(`_StrictFetchFailure`), but it is **not the default** and the scheduled workflow does not
pass it.

**INFERENCE** — under partial API failure, a default (non-strict) run produces a complete-looking
result missing an arbitrary subset of players, exits 1, and the warehouse — which reads the
database, not the exit code — cannot tell. In the S3 model this becomes the manifest's job (§8).

**FACT — request volume per full run:** 1 (`bootstrap`) + 1 (`fixtures`) + N (`live`, one per
finished-or-current GW) + P (`element-summary`, one per player, **P = 610–841**). So a full
uncached run is roughly 650–890 requests, dominated entirely by `element-summary`. Adding
`event-status` and `set-piece-notes` adds 2. That is a rounding error.

---

# 4. Is current ingestion genuinely idempotent?

**Yes.** Raw capture is idempotent by construction, and that is the only idempotency the
target contract depends on.

- **Immutable, run-scoped writes.** `extract/http/local_writer.py` writes every object under
  an immutable, `run_id`-keyed path (§A.1/A.4) — atomic per file (`.tmp` + `os.replace`), and
  a second write to an existing payload path raises rather than silently replacing it, since a
  reused `run_id` is a caller bug, not a legitimate overwrite. Every run is its own generation;
  nothing captured is ever destroyed by a later run. `element_summary.py` writes through this
  path (`raw_writer.write_object`).
- **Fetch selection is finality-driven, not existence-driven.**
  `gameweeks.py::_select_gameweeks_to_fetch` is driven by the `event-status` finality map
  captured first in every run (`extract/stages/event_status.py`, §A.5) — a finished gameweek is
  re-fetched unless `event-status` reports it settled *and* it already has a live capture.
  `--force` forces a re-fetch of a gameweek `event-status` already reports settled; there is no
  `force` parameter on `element_summary` — every player is fetched unconditionally every run.
- **`SQLiteStore` writes audit-trail rows only** (`_runs`, `_stage_lineage`, `_metadata`) — it
  is not part of the data contract, so its convergence behaviour is not a question that bears
  on ingestion idempotency.

---

# 6. Are raw payloads preserved sufficiently to rebuild downstream without re-calling the API?

**Mostly yes now; one deferred gap remains.**

**Resolved — single generation, no history.** Every capture is written under an immutable,
`run_id`-keyed path (§A.1/A.4) rather than overwritten in place, so the price/availability/
ownership time series is preserved across runs instead of collapsing to one latest-state row.

**Resolved — `element-summary`'s `fixtures` and `history_past` are captured.**
`element_summary.py` writes the endpoint's full response verbatim
(`_REQUIRED_TOP_LEVEL_KEYS = ("history", "fixtures", "history_past")`), not just `history` as
before. `fixtures` (the forward schedule with per-player difficulty) is the one field that was
**structurally impossible to backfill** if missed — a played fixture leaves the array
permanently and it is only observable live — so capturing it now, rather than after the fact,
closes what was the most severe of the original three gaps.

**Still open — `set-piece-notes` is not captured.** `event-status` now has its own stage
(`extract/stages/event_status.py`) and is captured every run — the finality signal everything
else needs. `team/set-piece-notes/` has no corresponding stage yet; it remains deferred per
§2.2/§2.3, not because it's low-value but because it hasn't been built. Same non-recoverability
caveat as before: every day it isn't captured is a day of that content permanently lost.

**What was already preserved well, and still is:** captured endpoints are written
**unmodified** — the raw writer (`extract/http/local_writer.py`) persists the response bytes
as received, before any validation or shape-checking runs. No lossy pre-processing on the FPL
side.

(Understat is materially worse on this point — see §7.3.)

---

# 7. understat-ingest — what it actually is

No prior audit covered this repo. Establishing it from scratch.

## 7.1 Shape

**FACT — HEAD `368004d`**, 6 commits total, first commit `a419ed8` ("initial release").
Six modules, 1,730 lines including tests:

| Module | Lines | Role |
|---|---|---|
| `client.py` | 188 | HTTP client — stdlib `urllib` only, no `requests`/`aiohttp` |
| `cli.py` | 236 | **Entire pipeline**: fetch, transform, store, coverage check |
| `models.py` | 199 | Four Pydantic models + a Pydantic→SQLite DDL generator |
| `store.py` | 237 | `SQLiteStore` with `register_table` / `upsert_models` / `create_index` |
| `transforms.py` | 189 | `flatten_shot(s)`, `flatten_roster(_entry)`, `flatten_match`, `flatten_match_info` |
| `tests/` | 636 | Two files, both `test_unit_*` — models and transforms only |

**FACT — sole dependency is `pydantic>=2.0`.** `pytest` in a dev group. No HTTP library, no
`mypy`.

**FACT — it does not resemble fpl-ingest's structure.** There is no `extract/`/`transform/`/
`load/`/`schema/`/`orchestration/` split; no packages at all, six flat modules. There is no
schema contract, no compiler, no contract artifact, no `_runs` table, no `_stage_lineage`, no
run status classification, no integrity checks, no strict mode, no `--help` subcommands.
`main()` in `cli.py` *is* the orchestrator, inline.

**FACT — the shared concepts are convergent, not shared code.** Both repos independently have
a `SQLiteStore` with `bulk_upsert` + `upsert_models` + `DEFAULT_UNIQUE`/`unique_key` conflict
targets, both write an `ingested_at` system column, both keep a raw JSON cache keyed by entity
id and skip already-cached entities unless `--force`. Two implementations of the same idea,
neither importing the other.

## 7.2 Sources

**FACT — three source shapes, verified live on 2026-08-24:**

| Source | Call | Live result | Shape |
|---|---|---|---|
| League index | `GET https://understat.com/main/getLeagueData/EPL/{season}` (XHR headers) | 200, 195 KB | `{teams: {20 objects, each with a per-match history array}, players: [523 season aggregates], dates: [380 fixtures]}` |
| Match data | `GET https://understat.com/main/getMatchData/{mid}` | 200, 40 KB | `{rosters: {h:{...}, a:{...}}, shots: {h:[...], a:[...]}, tmpl: ...}` |
| Match info | `GET https://understat.com/match/{mid}` — **HTML page** | 200, 31 KB | JSON extracted from `var match_info = JSON.parse('...')` via regex + `unicode_escape` decode |

**FACT — `match_info` is scraped, not an API.** `client.py::_extract_match_info` regexes the
HTML for `var\s+match_info\s*=\s*JSON\.parse\('(.+?)'\)`, then `encode("utf-8").decode("unicode_escape")`.
It returns `None` on any regex miss, and `get_match_info` propagates the `None` silently.
This is the single most fragile thing in either repo: a whitespace change in Understat's
template breaks it with no exception, no non-zero exit from the fetch, and no distinguishable
signal from "this match genuinely has no info."

**FACT — the coverage check is the only guard against that.**
`cli.py::_check_match_info_coverage` compares `COUNT(match_info)` to `COUNT(matches)` and
exits 1 below 95%. Added in the most recent commit (`368004d`). It catches wholesale scraper
breakage but not a partial one, and it can only detect the failure *after* a full run.

**FACT — `league_data.teams` and `league_data.players` are fetched and entirely discarded.**
`cli.py` reads only `league_data["dates"]`. `teams` carries per-team, per-match xG/xGA/npxG/PPDA
histories; `players` carries 523 season-level aggregates. Both are in the saved
`league_index.json` — but that file is overwritten every run, and `players` is a *running
season aggregate*, so its historical values are non-recoverable.

## 7.3 Evaluated against fpl-ingest's checklist

| Dimension | Finding |
|---|---|
| **Incremental extraction** | **FACT** — same file-existence heuristic as fpl-ingest, and the same flaw: `to_fetch = [m for m in finished if not (raw_dir/f"{m['id']}.json").exists()]`. A match captured minutes after full time — before Understat finalises xG — is never refreshed. There is no Understat equivalent of `event-status`; **INFERENCE**, `isResult` in the league index is the closest available finality signal, and it is set at full time, not at data-finalisation time. |
| **Rate limits / failures** | **FACT** — `_request()` sleeps `request_delay + jitter` (default 0.5s + up to 30%) *before every attempt*, halves-toward-baseline on success, doubles to a 60s ceiling on HTTP 429. Adaptive and reasonable. But: **on non-429 HTTP errors it does not sleep between attempts at all** (the `backoff` sleep is only in the generic `except Exception` branch), and after `max_retries` it returns `None` — indistinguishable from a legitimately empty response. |
| **Concurrency** | **FACT** — `ThreadPoolExecutor(max_workers=10)` in `cli.py`, with the 0.5s delay applied *per thread*, not globally. Effective request rate is therefore ~20/s, not ~2/s. The delay does not do what its name implies. **INFERENCE** — Understat is a small independent site with no CDN in front of it (unlike the FPL API); 20 req/s against it is impolite and a plausible cause of the 429s the backoff exists to handle. |
| **Idempotent?** | **Partly.** DB writes use `ON CONFLICT DO UPDATE` with per-model `DEFAULT_UNIQUE` (`UNIQUE(id)`, `UNIQUE(player_id, match_id)`, `UNIQUE(match_id)`) — convergent. Raw files are single-generation and `league_index.json` is overwritten every run. Same verdict as fpl-ingest. |
| **Shape validation?** | **FACT — Pydantic models only, and they run *after* flattening.** There is no check that `getMatchData` returned `rosters`/`shots` at all, or that `getLeagueData` returned `dates`. A `None` from `_extract_match_info` is handled; a structurally changed payload is not. |
| **Run metadata / lineage?** | **FACT — none whatsoever.** No `_runs`, no `_metadata`, no `_stage_lineage`, no run id, no timestamps beyond the per-row `ingested_at` column. The only run-level output is log lines and the exit code. |
| **Season handling** | **FACT — `--season` defaults to the string `"2025"`** (`cli.py:74`, `client.py:31`), i.e. 2025/26, which is now a *past* season. It is a CLI flag with a hardcoded default and no link to a calendar. |
| **Scheduling** | **FACT — none.** `.github/workflows/` contains only `ci.yml`, which runs `uv sync` and `pytest -m unit`. No scheduled workflow, no cron, no `mypy`, no integration step. Every run to date has been manual. |
| **State on disk** | **FACT** — `understat.db` last written 2026-04-13: 318 matches, 318 match_info, 7,916 shots, 9,619 rosters. 319 raw files. The 2025/26 season had 380 fixtures, so the database is **62 matches short of a complete season** and has been stale for four months. |

**FACT — the most important structural finding: understat-ingest's "raw" cache is not raw.**
Every per-match file is a **synthesised wrapper**:

```python
blob = {"match_data": match_data,      # verbatim
        "match_info": match_info,      # verbatim (or null)
        "_meta": flatten_match(match)} # TRANSFORMED
```

Verified against a stored file: `_meta` contains `home_team`, `away_team`, `home_team_id`,
`away_xg`, `is_result` — the *output* of `transforms.flatten_match()`, with renamed keys, not
the source's own `{h: {id, title, short_title}, goals: {h, a}, xG: {...}}` structure. Two
consequences: the file is one object combining three responses from two protocols, and it
already contains ingestion-side derived data. Under settled input 2 this cannot survive as-is
— it must be split into per-endpoint objects with no `_meta`.

## 7.4 Where the reep CSV belongs

**FACT — current behaviour** (`fpl-warehouse/src/fpl_warehouse/integration/matching.py:385-430`):
`load_reep_map()` downloads `https://raw.githubusercontent.com/withqwerty/reep/main/data/people.csv`
if `~/.cache/fpl_warehouse/reep_people.csv` is absent, then parses `key_opta_numeric` →
`key_understat`. No version pin, no checksum, no vendored fallback, never refreshed once
cached.

**FACT — measured on this machine:** the cached file is **65.4 MB / 444,708 rows**, downloaded
2026-04-13, with **50 columns** — a global cross-sport-identifier registry covering all of
football, of which the warehouse uses exactly two columns.

**Recommendation: move it to the ingestion boundary, as `source=reep`.**

It satisfies every property the raw contract is for, and violates every property of a build
step:

- It is a **third-party source fetched over the network on the critical build path.** That is
  the definition of what ingestion owns. The warehouse doing it directly is the same
  architectural error as `fpl-warehouse/src/fpl_warehouse/sources/fpl.py` — which already
  carries a `# WARNING: architectural debt` comment for calling the FPL API from the warehouse
  (audit §1.1). The reep download is the identical mistake, still live.
- It is **mutable upstream and unversioned.** Tracking `main` of someone else's repo means the
  player map can change under a rebuild with no diff, no notice, and no way to reproduce
  yesterday's matching. Capturing it per-run to
  `raw/reep/people/{extraction_date}/{run_id}/payload.csv` makes every warehouse build
  reproducible against a specific, immutable map.
- **The current cache is unfalsifiable.** `if not path.exists()` means the four-month-old
  65 MB file is used indefinitely and refreshes only on manual deletion. Nothing records
  which version any past build used.
- **Do not vendor it into a repo.** 65 MB of mostly-irrelevant rows in git is the wrong answer
  to open question Q9 in the audit. S3 is the right place for a large immutable third-party
  blob; that is what raw storage is.

**Two qualifications.** First, `source=reep` is a *file*, not an API, so the endpoint segment
is a filename and the payload is `payload.csv`, not `payload.json` — see §8.3. Second, it does
not change on the FPL cadence; capturing it daily is waste. Recommend weekly, or on
content-hash change, recorded in the manifest either way.

**Which repo?** **understat-ingest.** It exists only to join FPL identities to Understat
identities; understat-ingest is the repo whose entire purpose is the Understat side of that
join. Putting it in fpl-ingest would make fpl-ingest know about Understat, which it otherwise
does not. **INFERENCE** — this is a judgement call, not forced by the evidence.

**OPEN QUESTION (Q-C)** — Should the reep capture reduce the CSV to the two used columns before
writing to S3? That would cut 65 MB to well under 1 MB. But it is a transformation, and settled
input 2 says ingestion does not transform. My recommendation is **capture whole, unmodified** —
the storage cost is trivial, and column selection is a staging concern. Flagging it because it
is the one place where "capture raw" has a visible cost and someone will be tempted.

---

# A. TARGET INGESTION CONTRACT

## A.1 Key layout

Per settled input 1:

```
s3://<bucket>/raw/{source}/{endpoint}/{extraction_date}/{run_id}/payload.json
```

- `{source}` — `fpl` | `understat` | `reep`
- `{endpoint}` — the source's own endpoint identity, path-separator-safe (§A.2)
- `{extraction_date}` — `YYYY-MM-DD`, **UTC**, derived from run start, not per-object write
  time (so a run spanning midnight lands under one date)
- `{run_id}` — see §A.4
- `payload.json` — the response body, byte-for-byte as received, with **no** re-serialisation,
  no re-indenting, no key reordering, no wrapper object

**Byte-for-byte matters and is a change from today.** `write_json_cache()` currently does
`json.dumps(data, ensure_ascii=False, indent=2)` on the *decoded* object — it round-trips
through Python, losing original key order in edge cases and reformatting throughout. The
redirect must write `await resp.read()`, not `json.dumps(await resp.json())`. This also lets
the manifest record a checksum of what the source actually sent.

## A.2 Endpoint segment, per source

| Source | Endpoint segment | Objects per run | Notes |
|---|---|---|---|
| `fpl` | `bootstrap-static` | 1 | |
| `fpl` | `fixtures` | 1 | |
| `fpl` | `event-status` | 1 | Written **first**; §A.5 |
| `fpl` | `set-piece-notes` | 1 | |
| `fpl` | `event-live/{gw}` | 1 per GW fetched | `gw` zero-padded: `event-live/01` … `event-live/38`, so lexicographic S3 listing equals numeric order |
| `fpl` | `element-summary/{player_id}` | 1 per player (610–841) | §A.3 |
| `understat` | `league-data/{league}/{season}` | 1 | e.g. `league-data/EPL/2026` |
| `understat` | `match-data/{match_id}` | 1 per match fetched | Verbatim `getMatchData` JSON |
| `understat` | `match-info/{match_id}` | 1 per match fetched | §A.6 — the HTML case |
| `reep` | `people` | 1 (weekly) | `payload.csv`, §A.7 |

**Understat's `_meta` wrapper is deleted, not ported.** `match_data` and `match_info` are two
responses from two protocols and become two objects. `flatten_match()`'s output has no place in
raw storage.

## A.3 Keying the per-player endpoint

`element-summary/{EID}` is the only high-cardinality endpoint, and the only one where the
layout decision is not obvious. **Recommendation: one object per player per run**, at
`raw/fpl/element-summary/{player_id}/{extraction_date}/{run_id}/payload.json`.

**Note the segment order:** `{player_id}` is part of `{endpoint}`, so it sits *before*
`{extraction_date}`. That follows the settled template literally
(`{source}/{endpoint}/{extraction_date}/{run_id}/`) and matches target-state §3's own example,
which names `endpoint=element-summary/{player_id}`.

Considered and rejected: **one concatenated object per run** (all 841 players in a single
JSON array). It would cut object count 841→1 and simplify listing. Rejected because
concatenation is a transformation — it invents a container the source never returned, and
loses the ability to record per-player fetch failure. Under settled input 2, the object must
be what the endpoint returned.

**The cost is real and should be stated plainly.** At ~841 players × 1 run/day × 38 gameweeks
this is roughly 300,000 objects per season for this endpoint alone, versus about 1,500 for
everything else combined. **INFERENCE** — at S3 standard pricing the storage and PUT cost is
still single-digit dollars per season; the practical cost is listing latency, which the
`{player_id}` prefix ordering mitigates (a warehouse reading one player's history lists one
narrow prefix). This is the right trade, but it should be a decision, not a surprise.

**OPEN QUESTION (Q-D)** — Does `element-summary` need to be captured daily, or only after each
gameweek settles? Once `event-status` reports `points: "r"` and `bonus_added: true` for a
gameweek, that gameweek's `history` rows stop moving, and daily capture thereafter stores
duplicates. Gating on `event-status` would cut the object count severalfold. The counter-argument
is that `element-summary.fixtures` (forward schedule + difficulty) *does* change daily and is
non-recoverable (§6.2). Recommend daily until the volume is measured against real numbers
rather than estimated.

## A.4 What constitutes a "run"

**A run is one invocation of the ingest CLI for one source.** One `run_id`, one
`extraction_date`, spanning every endpoint that invocation captures.

**`run_id` format: `{utc_start}Z-{short_uuid}`**, e.g. `20260824T080012Z-a3f19c`.

Rationale for each half:

- **Timestamp prefix** — sorts chronologically as a plain string, so `ls`-style prefix listing
  is time-ordered with no metadata read. Preserves the useful property of today's
  `run_started_at` (an ISO-8601 UTC timestamp used as the `_runs` correlation key throughout
  `runner.py`).
- **Random suffix** — because a bare timestamp is not collision-safe. Two runs can start in the
  same second (a `workflow_dispatch` racing the cron; a retried job). Today that collides
  silently in `_runs.started_at`; in S3 it would silently interleave two runs' objects under one
  prefix, which is far worse. Six hex characters is enough.
- **No colons or dots** — `:` in S3 keys is legal but breaks a surprising number of CLI tools
  and URL parsers. Today's `datetime.now(timezone.utc).isoformat()` produces
  `2026-08-24T08:00:12.345678+00:00`, which must not be used as a key segment verbatim.

**Runs are per-source, not global.** fpl-ingest and understat-ingest are separate processes on
separate schedules; a shared run id would be a coordination mechanism neither needs. Correlating
them is the warehouse's job, via `extraction_date`.

**A run is never rewritten.** Objects are written once. A failed or partial run leaves its
partial objects in place, marked incomplete by its manifest (§A.5). Cleanup is a lifecycle-policy
concern, not the writer's.

## A.5 Metadata accompanying each object

Two levels: a per-object sidecar, and a per-run manifest.

### Per-object sidecar — `metadata.json`, beside `payload.json`

```
raw/fpl/bootstrap-static/2026-08-24/20260824T080012Z-a3f19c/
    payload.json
    metadata.json
```

Sidecar object rather than S3 user metadata: user metadata is capped at 2 KB, is invisible to a
`GET` of the payload, and cannot be read by a dbt external-table scan. A sidecar is just
another readable object.

Contents:

| Field | Example | Why |
|---|---|---|
| `source` | `"fpl"` | Self-describing without parsing the key |
| `endpoint` | `"bootstrap-static"` | ditto |
| `run_id` | `"20260824T080012Z-a3f19c"` | ditto |
| `request_url` | `"https://fantasy.premierleague.com/api/bootstrap-static/"` | Exact URL including any query string |
| `requested_at` / `received_at` | ISO-8601 UTC | Bounds the observation instant |
| `http_status` | `200` | Only 2xx should ever be written, but record it |
| `response_headers` | `{"date": ..., "age": "41", "cache-control": "max-age=300", "etag": ...}` | **Load-bearing.** §3 established the API is Fastly-cached with a 5-min TTL; `Age` is the only way to know how stale a 200 was. `ETag`/`Last-Modified` enable conditional requests later. |
| `content_length` | `1616455` | Cheap corruption check |
| `content_sha256` | hex digest | Identity of the payload. Enables no-op detection and cross-run dedupe without re-reading bytes. |
| `attempt_count` | `1` | Non-1 means the retry path was exercised |
| `ingest_version` | `"fpl-ingest/1.0.0"` | Which code produced this |
| `shape_validation` | `{"status": "pass", "checks": [...]}` | §B validation result for this payload |

### Per-run manifest — `raw/{source}/_manifests/{extraction_date}/{run_id}/manifest.json`

The `_manifests` prefix is a sibling of the endpoint prefixes, so a warehouse scanning
`raw/fpl/bootstrap-static/**` never accidentally reads manifests as payloads.

| Field | Why |
|---|---|
| `run_id`, `source`, `extraction_date` | Identity |
| `started_at`, `ended_at`, `duration_seconds` | Direct successor to today's per-stage timing in `runner.py::_measure_stage` |
| `status` | `SUCCESS` / `FAILED_PARTIAL` / `FAILED` — **reuse `orchestration/run_status.py` verbatim.** Its precedence rules are already the shared vocabulary of runner and store. |
| `objects` | Per-endpoint: attempted, written, failed, total bytes |
| `failures` | Per-failed-endpoint: URL, final status, attempt count, error class |
| `finality` | **FPL only, new.** The captured `event-status` payload's essentials: per-event `points` (`p`/`r`) and `bonus_added`. Lets a warehouse decide whether a run's data is settled **without opening any payload.** This is the single most valuable new field. |
| `git_sha`, `ingest_version` | Which code produced this run |
| `config` | Effective rate limit, concurrency, strict mode, `--force` |

**The manifest is the successor to `_runs` and `_stage_lineage`.** `_runs`
(`store.py::_RUNS_DDL`) carries `started_at, stage, fetched, validated, written, skipped,
errors, status`; `_stage_lineage` carries `started_at, stage, artifact_path, output_table`. The
run-level parts map onto the manifest cleanly. The parts that do **not** carry over are exactly
the ones settled input 2 removes: `validated`, `skipped`, and `output_table` are all
transform/load concepts. A raw-capture run has objects written and objects failed — no rows,
no tables, no grain.

**`event-status` is captured first, before everything else.** It is the cheapest request (285
bytes) and it determines how the rest of the run should be interpreted. Capturing it last
would mean the finality signal describes a moment *after* the payloads it labels.

## A.6 The Understat HTML case

`understat/match-info/{mid}` is scraped from an HTML page (§7.2), which creates a genuine
tension with "the object is what the endpoint returned."

**Recommendation: store the extracted JSON as `payload.json`, and the source HTML as
`source.html` alongside it.**

```
raw/understat/match-info/28778/2026-08-24/20260824T081500Z-b71e04/
    payload.json     # the parsed match_info object
    source.html      # the 31 KB page it came from
    metadata.json    # includes the extraction regex + its version
```

**Why keep the HTML.** The regex is the fragile point in either repo (§7.2). If Understat
changes its template, keeping the HTML means the extraction can be fixed and **re-run against
stored pages** — the entire point of raw capture. Discarding it means a template change
silently loses data permanently. 31 KB × ~380 matches/season ≈ 12 MB. Trivial.

**Why not store only the HTML.** The extraction is a *parse*, not an analytical transform, and
the warehouse should not be running `unicode_escape` regexes in dbt. Storing both keeps the
boundary honest in both directions.

**The metadata sidecar must record the extraction pattern used**, so a payload extracted by a
later regex version is distinguishable from one extracted by today's.

This is the one place where the contract needs a documented exception. Recording it as an
exception rather than quietly generalising the contract to "payloads may be derived."

## A.7 The reep file case

```
raw/reep/people/2026-08-24/20260824T090000Z-c02d11/
    payload.csv
    metadata.json
```

`payload.csv`, not `payload.json` — it is a CSV and converting it would be a transformation.
The metadata sidecar records `source_url`, `content_sha256`, `content_length`, and the
upstream commit SHA if the GitHub API can supply it cheaply (**INFERENCE** — the raw
`raw.githubusercontent.com` response does not carry it; a separate API call would).

Captured weekly, or on `content_sha256` change. §7.4.

---

# B. CODE DISPOSITION

Legend: **KEEP** (survives as-is) · **REDIRECT** (same logic, writes S3) · **MOVE** (belongs
in fpl-warehouse staging) · **DELETE**.

## B.1 fpl-ingest

### `extract/` — KEEP, almost entirely

| Module | Lines | Disposition |
|---|---|---|
| `extract/http/client.py` | 356 | **KEEP.** Session lifecycle, retry classification, rate-limiter integration, the sleep-outside-the-limiter design. All of it survives. Two edits: return raw bytes alongside decoded JSON (§A.1); expose response headers for the sidecar (§A.5). |
| `extract/http/rate_limiter.py`, `rate_config.py` | 160 | **KEEP verbatim.** Nothing about token-bucket rate limiting changes when the sink changes. |
| `extract/http/sync_http.py` | 232 | **KEEP.** Holds the shared retry primitives (`compute_retry_delay`, `parse_retry_after`, `RETRYABLE_STATUS_CODES`, `FPLClientError`) that `client.py` imports. Genuinely shared despite the name. |
| `extract/http/sync_client.py` | 194 | **DELETE. FACT — it has no production caller.** `FPLClient` is imported only by `extract/http/__init__.py` (a re-export, docstring: *"kept for backwards-compatible callers"*) and by its own test, `tests/extract/http/test_sync_client.py`. Every stage and the smoke test use `AsyncFPLClient`. It is 194 lines of duplicate HTTP surface; do not port it through the redirect. |
| `extract/stages/bootstrap.py` | 173 | **SPLIT.** The fetch half (`client.get_bootstrap()` + write) is **REDIRECT**. `process_core_payload`, `ingest_players/teams/events/element_types`, and `_assert_store_validation_consistency` are flatten-and-upsert — **MOVE** to warehouse staging. Each stage collapses from ~170 lines to roughly 15. |
| `extract/stages/fixtures.py` | 118 | **SPLIT**, same way. `process_fixtures_payload`, `upsert_fixtures`, `flatten_fixture_stat_rows`, `upsert_fixture_stats` → **MOVE**. |
| `extract/stages/gameweeks.py` | 251 | **SPLIT.** `_collect_gameweeks` (strict-mode concurrent fetch with cancellation) is real, well-tested machinery — **KEEP**. `_select_gameweeks_to_fetch` is **REWRITE**: its file-existence heuristic (line 139) must become an `event-status` finality check (§4.2). `upsert_gameweek_rows` and `process_gameweek_payloads` → **MOVE**. |
| `extract/stages/element_summary.py` | 243 | **SPLIT.** `_fetch_player_histories` (concurrent fetch + strict cancellation) — **KEEP**. `raw_history_rows`, `upsert_history_rows` → **MOVE**. Delete the dead `force` parameter (§4.2). The redirect fixes the `fixtures`/`history_past` loss for free: writing the payload whole means nothing is discarded. |
| **New:** `extract/stages/event_status.py`, `set_piece_notes.py` | ~30 each | **NEW.** Trivial single-request stages. `event_status` additionally populates the manifest's `finality` block. |
| **New:** `extract/http/s3_writer.py` | ~120 | **NEW.** The only genuinely new component. Writes payload + sidecar, computes checksums, assembles and writes the manifest. |

### `transform/` — MOVE entirely

| Module | Lines | Disposition |
|---|---|---|
| `transform/transforms.py` | 173 | **MOVE.** `flatten_event`, `flatten_fixture_stats`, `flatten_live_elements` are precisely "flattening payloads into structured tables," which target-state §2 names as moved out. This logic is correct and tested (`tests/transform/`, part of the 413 unit tests) — it should be *ported*, not rewritten from scratch, even though its new form is SQL. |
| `transform/models.py` | 401 | **MOVE** as a specification. Eight Pydantic models encoding field names, types, nullability, and `prepare()` normalisations, refined over the project's life. In the target they become dbt staging column definitions and `not_null`/`accepted_values` tests. **Do not delete before the warehouse staging models exist** — this file is the highest-density documentation of FPL's actual field semantics anywhere in the platform. |
| `transform/types.py` | 12 | **KEEP.** `JSON` type alias, imported by `client.py`. Relocate under `extract/`. |

**INFERENCE** — `transform/` is where the migration's real risk sits. It is 574 lines encoding
hard-won knowledge about a messy API (nullable `ep_next` early in the season, `chip_plays`
hoisting, `singular_name_short` normalisation). Re-deriving it in SQL from scratch will
reintroduce bugs this repo already fixed.

### `load/` — DELETE, except the audit machinery

| Module | Lines | Disposition |
|---|---|---|
| `load/store.py` | 483 | **DELETE the SQLite writer** (`bulk_upsert`, `upsert_models`, `register_contract_table`, `_migrate_contract_columns`, `query`). **PORT the audit half** — `_RUNS_DDL`, `_STAGE_LINEAGE_DDL`, `record_stage_result`, `record_stage_lineage`, `finalize_run`, `set_metadata` — into the manifest writer (§A.5). The *concepts* survive; the SQLite implementation does not. |
| `load/db_setup.py` | 22 | **DELETE.** |
| `load/integrity.py` | 142 | **MOVE.** `check_player_histories_elements_exist`, `check_player_histories_fixtures_exist` and friends are cross-table referential checks over `PUBLIC_TABLES` — textbook dbt `relationships` tests. Ingestion has no tables to check. |

### `schema/` — split three ways

| Module | Lines | Disposition |
|---|---|---|
| `schema/definition.py` | 227 | **DELETE `PUBLIC_TABLES`** (settled input 3). **MOVE** the eight `TableContract` declarations — `grain`, `unique_key`, `indexes`, `description`, `field_notes` — into warehouse staging as model config and tests. The grain declarations in particular are the resolution of audit open question Q1 written down in code; do not lose them. |
| `schema/compiler.py` | 226 | **DELETE.** It compiles Pydantic models into SQLite DDL. With no SQLite there is nothing to compile. §B.3 covers the CI artifact check. |
| `schema/ddl.py` | 28 | **DELETE.** |
| `schema/test_data.py` | 54 | **DELETE** with the compiler (it generates fixtures from compiled tables). |
| `schema/validation.py` | 386 | **SPLIT — and this is the important one.** The file merges two unrelated concerns, as its own docstring says. **Section 1** (`validate_contract`, PRAGMA introspection, `TypeMismatch`, `ConstraintMismatch`) — **DELETE**, it validates a SQLite database. **Section 2** (`run_smoke_test`, `_check_bootstrap`, `_check_fixtures`, `_check_player_history`, `_check_record_list`, `_require_key/_require_mapping/_require_list`, `SmokeTestFailure`) — **KEEP and PROMOTE**. This *is* the source-shape validation the target contract calls for. See §B.2. |

### `orchestration/` — KEEP, redirected

| Module | Lines | Disposition |
|---|---|---|
| `orchestration/runner.py` | 453 | **REDIRECT.** Stage sequencing, timing, strict-mode propagation, `StrictRunFailure`, exit-code derivation all survive. Removed: `setup_store`, the `store.transaction()` wrappers (S3 has no transactions — a partial run leaves partial objects and an honest manifest), `_check_stale_freshness` in its current form (it reads `_metadata` from SQLite; it should read the previous manifest), and the `run_integrity_checks()` call in `_exit_code`. |
| `orchestration/run_status.py` | 38 | **KEEP verbatim.** `classify_run` and its `FAILED > FAILED_PARTIAL > SUCCESS` precedence are the shared vocabulary across runner and store. It becomes the manifest's `status` field. Zero changes. |
| `orchestration/stage_result.py` | 139 | **KEEP, trimmed.** `StageResult`, `StageOutcome`, `StageLineage`, `StageMetadata`, `totals()`, `summary_line()` all survive. Drop `validated`/`skipped` (transform concepts) and `output_tables` from `StageMetadata` (load concept). `raw_artifacts` becomes a list of S3 keys instead of local paths — which is arguably what it always wanted to be. |
| `orchestration/execution_state.py` | 27 | **KEEP verbatim.** The RUNNING/FAILED fail-fast sentinel is storage-agnostic. |
| `orchestration/replay.py` | — | **DELETE — decided against, not just deferred.** Replay's original conception ("re-run warehouse staging against a specific captured run") would have made fpl-ingest a participant in structural transformation, which is exactly the boundary the raw-capture-only contract (§0) exists to hold. Ingestion touches the API and stores objects; interpreting those objects into rows is fpl-warehouse's job, entirely outside this repo. The module, its CLI command, and its test file have been removed. |

### Top level

| Module | Lines | Disposition |
|---|---|---|
| `cli.py` | 238 | **REDIRECT.** `run` survives with new flags (`--bucket`, `--run-id`). `smoke-test` **KEEPs** and gains prominence (§B.2). `status` **REDIRECTs** to read manifests instead of `_runs`. `replay`, `schema export` / `schema validate` — **DELETE** (§B.3). |
| `cli_formatters.py` | 209 | **KEEP**, minus the `format_schema_output` path. |
| `config.py` | 130 | **REDIRECT.** The flag → env → `~/.fpl/config.yaml` → default chain is good and should be preserved exactly. `db_path` → `bucket` + `prefix`; `raw_dir` survives as an optional local mirror (§C). `resolve_db_path_with_source` — the *source*-reporting idea is genuinely useful for debugging and should be kept for the bucket. |

### fpl-ingest disposition summary

| Disposition | Approx. lines | Share |
|---|---|---|
| KEEP (verbatim or near) | ~1,600 | 29% |
| REDIRECT | ~1,100 | 20% |
| MOVE to warehouse | ~1,500 | 27% |
| DELETE | ~1,300 | 24% |
| NEW | ~200 | — |

**INFERENCE** — roughly half the repo survives in place, a quarter relocates with its logic
intact, and a quarter is SQLite-specific machinery that the storage decision retires. That is a
smaller demolition than "remove SQLite as the ingestion boundary" sounds like, because the
`extract`/`transform`/`load` split was already drawn along nearly the right line. The 2026-05-18
restructure (`1134a88`) turns out to have been good preparation for this.

## B.2 What SHOULD be validated at a raw-capture boundary

**FACT — what is validated today, at three distinct layers:**

1. **Source shape** — `schema/validation.py::run_smoke_test()` fetches bootstrap-static,
   fixtures, and a sample of element-summary, then asserts top-level keys exist
   (`elements`, `teams`, `events`, `element_types`; `history`, `history_past`) and that sampled
   records carry required fields (`elements[].{id,team,now_cost}`,
   `fixtures[].{id,team_h,team_a,event}`, `history[].{element,round,fixture,minutes,total_points}`).
   Raises `SmokeTestFailure`. **This is exactly right for the target boundary** — it is
   structural, samples rather than scans, and asserts nothing about values.
   **FACT — it is a manual `fpl-ingest smoke-test` command and runs in no workflow.**
2. **Business/type validation** — Pydantic models in `transform/models.py`, applied per row via
   `validate_models()`. Field types, ranges, nullability. **Not** a raw-capture concern; MOVEs.
3. **Contract validation** — `validate_contract()` against a live SQLite database. Disappears
   with SQLite.

**What should be validated at the raw-capture boundary — and nothing more:**

| Check | Rationale |
|---|---|
| HTTP status is 2xx | Never write a payload for a non-2xx response |
| Body parses as JSON (or is non-empty for CSV/HTML) | A truncated or HTML-error-page response must not be stored as a JSON payload |
| Top-level type is as expected | `bootstrap-static` object, `fixtures` list, `event/{gw}/live` object, `element-summary` object |
| Required top-level keys present | Exactly `run_smoke_test`'s existing assertions |
| A sampled record carries its identifying fields | `elements[0].id`, `fixtures[0].id`, `history[0].{element,round,fixture}` — sample, do not scan |
| Non-degenerate size | `len(elements) > 0`; body length within an order of magnitude of the previous run's, from the manifest |

**What must NOT be validated here:** field types beyond structural presence; value ranges;
cross-record consistency; cross-endpoint referential integrity (`load/integrity.py`'s job,
which MOVEs); grain or uniqueness. All of it is warehouse work. The test is: *if the check
would fail because the football did something unusual rather than because the API changed
shape, it does not belong here.*

**Recommendation on failure handling: write the payload, record the failure, do not discard.**
A shape-check failure means the source changed, which is precisely the moment the payload is
most valuable. `metadata.json.shape_validation` records the failure; the manifest marks the run
`FAILED_PARTIAL`; the warehouse can refuse to build on it. Discarding a payload because it
surprised us is the one unrecoverable mistake available at this boundary.

**Recommendation: promote `smoke-test` into the scheduled workflow**, as a pre-flight step
before the capture run and as its own daily job. It costs about four requests. It is currently
excellent code that nothing runs.

## B.3 The schema contract compiler and its CI artifact check

**FACT — what exists.** `schema/compiler.py` walks each `TableContract`, derives
`CREATE TABLE`/index SQL and column metadata, and emits three checked-in artifacts to
`artifacts/contract/`: `schema_contract.json`, `ddl_contract.sql`, `validation_contract.json`.
`SCHEMA_VERSION = "1.0.0"` (`definition.py:30`). CI enforces that the checked-in artifacts match
freshly compiled output via
`tests/schema/test_compiler.py -k test_checked_in_contract_artifacts_match_compiled_outputs`.
`fpl-ingest schema export` regenerates them; `fpl-ingest schema validate` checks a live database
against them, exiting 0/1/2 for valid/invalid/drift.

**Disposition: DELETE the compiler, the three artifacts, the two `schema` subcommands, and the
CI step.**

All four exist to guarantee one thing: *the SQLite database fpl-ingest produces has the schema
it promised.* Settled inputs 3 and 4 remove both the promise and the database. Keeping the
machinery would mean maintaining a compiler for tables nothing writes.

**Do not delete the idea.** Two of its properties should be rebuilt on the S3 side, and one
should move:

1. **The CI-enforced artifact check is a good pattern** — a machine-checkable, version-controlled,
   diff-visible statement of what this repo emits. Its successor is a checked-in
   `artifacts/contract/raw_contract.json` declaring the S3 key template, the endpoint inventory
   (§2.4), the sidecar and manifest field lists (§A.5), and a `RAW_CONTRACT_VERSION`. A CI test
   asserts the code's actual key-building and metadata-writing match it. Cheaper than the SQL
   compiler and enforces the boundary that now matters.
2. **`SCHEMA_VERSION` becomes `RAW_CONTRACT_VERSION`**, recorded in every manifest, so a
   warehouse reading a two-season-old object knows which layout produced it.
3. **The eight `TableContract` declarations MOVE** to warehouse staging (§B.1). They are the
   warehouse's contract now.

**FACT — the CI step must be deleted in the same commit that deletes the compiler**, or CI
breaks. Named explicitly because it is the kind of coupling that gets missed.

**OPEN QUESTION (Q-F)** — Should `raw_contract.json` be published somewhere fpl-warehouse can
consume programmatically (a release artifact, a shared S3 key), or is a checked-in file in this
repo enough? Deliberately not answered here — it is the ingestion↔warehouse interface question
and belongs with Deliverable 5.

## B.4 understat-ingest

Its lack of structure makes this shorter, and the target is best reached by *adding* structure
rather than redirecting it.

| Module | Lines | Disposition |
|---|---|---|
| `client.py` | 188 | **KEEP, with three fixes.** The adaptive-delay/429-backoff design is sound. Fixes: (a) the per-thread delay must become a **shared** limiter — 10 threads × 0.5s is ~20 req/s, not ~2 (§7.3); (b) add a real backoff on non-429 HTTP errors, which currently retry with no wait; (c) return raw bytes and headers for the sidecar. Consider adopting fpl-ingest's `TokenBucketLimiter` — the same class, imported or vendored, is a *proven* shared abstraction rather than a speculative one. |
| `client.py::_extract_match_info` | ~15 | **KEEP, relocated.** Under §A.6 it runs at capture time to produce `payload.json`, with `source.html` stored alongside. The regex version goes in the sidecar. |
| `cli.py` | 236 | **SPLIT — the biggest change in this repo.** Inline `main()` becomes: fetch stages (**REDIRECT** to S3), an orchestrator mirroring fpl-ingest's `runner.py`, and the transform/upsert calls (**MOVE**). `_load_fpl_env()` — **DELETE**; reading `~/Documents/FPL/.env` from library code is the untracked machine-local coupling of audit Q8, and S3 credentials must not come from there (§D). `_check_match_info_coverage` — **MOVE**; it is a completeness assertion over stored tables, i.e. a dbt test. |
| `transforms.py` | 189 | **MOVE** entirely. `flatten_shot(s)`, `flatten_roster(_entry)`, `flatten_match`, `flatten_match_info` and the `_safe_float`/`_safe_int`/`_calc_ppda` helpers are staging logic. `_calc_ppda` in particular computes a *derived metric* — clearly warehouse work. |
| `models.py` | 199 | **MOVE** as specification, like fpl-ingest's. Also **DELETE** `pydantic_to_sqlite_column` and `schema_to_create_table` — a miniature parallel to fpl-ingest's compiler, retired for the same reason. |
| `store.py` | 237 | **DELETE.** Second SQLite implementation; no audit half worth porting (there is none). |
| `tests/test_unit_models.py`, `test_unit_transforms.py` | 636 | **MOVE with the code they test.** They are the only tests here; if `transforms.py` moves and the tests do not, understat-ingest ships with zero test coverage. |
| **New:** `extract/`, `orchestration/` | ~250 | **NEW**, mirroring fpl-ingest: stages for `league-data`, `match-data`, `match-info`, `reep/people`; an orchestrator; shape validation. |
| **New:** shape validation | ~60 | **NEW.** Currently nonexistent (§7.3): assert `getLeagueData` returned `dates`/`teams`/`players`; assert `getMatchData` returned `rosters`/`shots`; assert the `match_info` regex matched. That last one alone would have caught a scraper break before the coverage check did. |

**The `_meta` wrapper is deleted, not ported** (§A.2, §7.3).

**Adopt fpl-ingest's `run_status.py` and `stage_result.py` directly** — 177 lines that give
understat-ingest run classification and stage results it has never had. **INFERENCE** — this is
the one place a shared package might later be justified; see §E for why not yet.

---

# C. TRANSITION

**Resolved — hard cutover, done.** This section originally weighed dual-write against hard
cutover under the assumption that fpl-warehouse was a live consumer reading `fpl.db` and could
not be broken mid-migration. That assumption did not hold: **fpl-warehouse is not
operational.** There was no running downstream consumer to protect, so the constraint that
ruled out hard cutover never applied.

Hard cutover is what actually happened: `transform/` and `load/`'s fact/dim, flatten, and
upsert machinery, the schema compiler/DDL infrastructure, and `PUBLIC_TABLES` were deleted
outright. `SQLiteStore` was kept, trimmed to audit-trail duties only (`_runs`,
`_stage_lineage`, `_metadata`). Ingestion now writes raw JSON captures to a local S3-shaped
layout (`extract/http/raw_keys.py`, `extract/http/local_writer.py`) implementing the key
template in §A.1/A.2/A.4 against a local root; moving to real S3 later replaces the storage
backend only, not the key, sidecar, or manifest shapes. Test suite passing (313/313), mypy
clean.

The alternatives originally weighed here (dual-write; S3-only plus a rebuild-from-S3 script)
are kept below as a record of what was considered and why they were unnecessary, not as live
options — there is nothing left to migrate away from.

**Why hard cutover was safe here, for the record:** with no operational warehouse depending on
`fpl.db`, there was no "breaks the warehouse the same day" cost to weigh against dual-write's
real costs — two representations of the same run that can disagree, 1,500-odd lines of
`transform/`/`load/` kept alive and maintained for an indefinite migration window, and a
compatibility shim someone has to remember to delete. None of that cost was worth paying for a
consumer that doesn't exist yet. fpl-warehouse's staging layer (Deliverable 5) will be built
against the raw S3-shaped captures directly, once it exists.

**Originally rejected: dual-write** (S3 authoritative, SQLite kept alive as a compatibility
shim until warehouse staging could read S3). Would have kept `transform/`/`load/` alive and
maintained for the duration, and required resolving the pre-migration `fpl.db` schema split
(two divergent local database files) as a precondition. Unnecessary once it was clear nothing
downstream was reading `fpl.db` in production.

**Originally rejected: S3-only plus a rebuild-SQLite-from-S3 tool.** Architecturally the most
attractive — one write path, with a throwaway `s3 → fpl.db` script for the warehouse. Would
have kept `transform/` alive in exactly one place. Rejected because that script would have
been the warehouse's staging layer, written twice: once as a temporary Python script here,
then again as dbt models there — Deliverable 5's job, done in the wrong repo, with a name that
implies it is temporary. Moot for the same reason as dual-write: no live warehouse to bridge
to.

## understat.db

**Same recommendation, weaker constraint.** `understat.db` is already four months stale and 62
matches short of a complete 2025/26 season (§7.3), and its consumer reads only `shots`,
`match_info`, and `rosters`. It is a smaller, better-understood surface.

**FACT — but the same file is also the input to a *new* season.** The `--season` default is
still `"2025"` (§7.3) while the live Understat season is 2026. A dual-write transition here
must resolve season handling (§C.1) or it will write two seasons into tables with no season
discriminator.

## C.1 Season handling — unresolved in both repos, and now urgent

**FACT — neither repo has a season concept in its storage.**
- fpl-ingest: no `season` column in any of the eight `PUBLIC_TABLES`. Verified against
  `~/.fpl/fpl.db`: `players`, `gameweeks`, `player_histories`, `fixtures`, `events` all lack
  one. Season is implicit in "whatever the API returned today."
- understat-ingest: `matches`, `rosters`, `match_info` have no season column. Only `shots` has
  one, because Understat's own shot records carry `season` and `flatten_shot` passes it through
  (`transforms.py:36`).

**FACT — the failure mode is concrete and imminent.** A `fpl-ingest run` today (2026-08-24,
2026/27 GW1 in progress) against the existing `~/.fpl/fpl.db` would upsert 2026/27 players over
2025/26 players on `id` — and FPL element ids are reassigned every season. The 2025/26 data
would be silently corrupted, not appended to. The same applies to `gameweeks` on
`(element_id, round)` and `player_histories` on `(element_id, round, fixture)`.

**The S3 layout does not fix this, and it is worth being precise about why.** The key template
has `{extraction_date}`, not `{season}`. Objects from different seasons land in different date
prefixes, so nothing is *overwritten* — the raw layer is safe. But nothing in the key states
which season a payload belongs to, and `extraction_date` is not a reliable proxy: a July
capture could be either season's pre-season, and a backfill run captured in 2027 for the 2025/26
season would be filed under a 2027 date.

**Recommendation: derive season at capture time and record it in metadata — not in the key.**

- For FPL: derive from `bootstrap-static.events[0].deadline_time` (**FACT** — the stored payload
  gives `"2025-08-15T17:30:00Z"` for the 2025/26 season's GW1), yielding a canonical
  `"2025/26"`. Write it to the sidecar and the manifest.
- For Understat: the `--season` argument is already explicit; record it, and **remove the
  hardcoded default** so a run must state its season or derive it from the calendar. A silently
  defaulted season is how the 2025/26 data got frozen.
- Not in the key, because the settled key template is fixed (settled input 1) and because season
  is a property of the *data*, not of the capture — putting it in the path would make it a
  capture-time decision that cannot be corrected without moving objects.

**OPEN QUESTION (Q-G)** — Should the season live in the S3 key after all? It would make
season-scoped listing trivial for the warehouse and remove any need to open a manifest. It
conflicts with the settled layout, so it is raised, not assumed. This is the one place where
the settled key template has a genuine gap (§12).

**Regardless of the answer, the SQLite dual-write shim needs a season discriminator before the
first 2026/27 run lands in it**, or the migration will destroy the 2025/26 season it is meant to
preserve. This is the most time-sensitive item in this document.

---

# D. SCHEDULING

## D.1 Does S3 persistence fully resolve the fpl-ingest scheduling gap?

**Yes for durability. No for correctness — two other things must land with it.**

**FACT — the gap, re-verified.** `scheduled_run.yml` runs daily at 08:00 UTC, executes
`uv run fpl-ingest run` on an ephemeral runner, and the database dies with the runner. Only
`_runs_audit.json` survives, as a 90-day Actions artifact. The workflow's own header comments
identify the problem and propose exactly this fix.

**What S3 resolves completely:**
- Durability. Every captured object outlives the runner. This is the whole gap.
- The restore-at-start problem *disappears rather than being solved*. The workflow header
  proposes download-db-at-start / upload-at-end, which is a fragile pattern (a failed upload
  loses a day; two runs racing corrupt the file). Raw capture needs no restore at all: each run
  writes new immutable objects under its own `run_id` and reads nothing. **This is a strictly
  better answer than the one the workflow comments propose**, and it is worth saying so
  explicitly, because the comments have been the plan of record.
- Ephemerality becomes a feature. A stateless runner is exactly right for a stateless writer.

**What S3 does not resolve, and must ship alongside:**

1. **Finality-aware fetching (§4.2) — SHIPPED.** `_select_gameweeks_to_fetch` no longer keys on
   local file existence; it fetches every finished gameweek unless this run's `event-status`
   capture reports it settled (`points == "r"` and `bonus_added`) *and* that gameweek's live
   endpoint has been captured at least once. `event-status` is now captured first in every run
   (§A.5), and its parsed finality map rides in the run manifest so a warehouse can read
   settlement without opening any payload. The `gw_{n}.json` marker files the old heuristic
   depended on are retired along with it. Net effect for the ephemeral-runner problem this item
   originally described: an ephemeral runner with no local state now fetches only the gameweeks
   `event-status` says are still unsettled, not all 38 — the daily-re-fetch failure mode this
   item warned about no longer applies.
2. **`--strict`, or a defensible reason not to.** The workflow runs plain
   `uv run fpl-ingest run`. Non-strict means a partial capture exits 1 but still writes a
   partial object set (§3). With a manifest recording `FAILED_PARTIAL` and a per-object failure
   list, non-strict is *defensible* — the warehouse can see it. Without the manifest, it is not.
   Ship them together.
3. **Idempotency under re-runs.** GitHub Actions retries and `workflow_dispatch` can produce two
   runs in the same second; §A.4's `run_id` suffix handles it. A bare timestamp would not.

## D.2 Credentials and secrets

**FACT — the current workflow needs no secrets at all.** Its `env` block references only
`vars.FPL_DB_PATH` / `vars.FPL_RAW_DIR`, both optional. The FPL API is unauthenticated for
every in-scope endpoint (verified: all probes in §2 were unauthenticated). S3 is the first
credential this repo has ever needed.

**Recommendation: GitHub OIDC federation to an AWS IAM role. No long-lived access keys.**

- Add `permissions: {id-token: write, contents: read}` to the job and use
  `aws-actions/configure-aws-credentials@v4` with `role-to-assume`. GitHub mints a short-lived
  OIDC token; AWS exchanges it for temporary credentials scoped to that repo and branch.
- No `AWS_SECRET_ACCESS_KEY` in repository secrets. Nothing to rotate, nothing to leak, nothing
  to accidentally print. Given that this repo's own `SECURITY.md` exists and the global policy
  is explicit about not exposing credentials, storing a static key pair when federation is
  available would be a deliberate downgrade.
- The IAM role gets **`s3:PutObject` only**, on `arn:aws:s3:::<bucket>/raw/fpl/*`. No delete, no
  list, no read, no access to other sources' prefixes. Ingestion never needs to read what it
  wrote — a genuine benefit of the immutable append-only design.
- Non-secret configuration (`FPL_S3_BUCKET`, `FPL_S3_PREFIX`, `AWS_REGION`) goes in
  repository **variables**, not secrets. They are not sensitive and putting them in secrets
  makes debugging harder for no gain.
- **DELETE `_load_fpl_env()` from understat-ingest** before it runs anywhere near credentials.
  Reading `~/Documents/FPL/.env` from library import code (`cli.py:20`, executed at module
  import, before the CLI even parses arguments) is the untracked machine-local coupling from
  audit Q8. It must not become the path by which AWS credentials reach the process.

**Local development:** standard AWS credential chain (`~/.aws/credentials`, `AWS_PROFILE`),
no repo-specific mechanism. `config.py`'s existing flag → env → config-file → default chain
covers bucket and prefix; credentials are the SDK's problem, not this repo's.

**Bucket configuration** (**INFERENCE** — recommended, not verified against any existing
bucket): versioning **on** (defence against an accidental overwrite of an immutable object);
a lifecycle policy transitioning objects older than 90 days to Infrequent Access; **no
expiration rule** — the entire premise of raw capture is that these objects are permanent;
default SSE-S3 encryption; public access blocked.

**OPEN QUESTION (Q-H)** — Does an AWS account and bucket exist? Everything above assumes AWS
because settled input 1 says S3. If the intended target is an S3-*compatible* store (R2, MinIO,
Backblaze), OIDC federation is unavailable and the credential recommendation changes to
scoped static keys in repository secrets. This is the only blocking unknown in this section.

## D.3 Should the scheduled run stay disabled until then?

**FACT — it is not disabled.** It is running daily right now, producing nothing durable. That
is the audit's open question 5, still open.

**Recommendation: disable it now — comment out the `schedule:` trigger, keep
`workflow_dispatch` — and re-enable when S3 writing plus finality-aware fetching are both in.**

Reasons: it burns Actions minutes and makes 650–890 unauthenticated requests daily to a
third party's API for zero durable output; a red run tells nobody anything useful, so it is
training everyone to ignore this workflow's status; and re-enabling is a one-line diff.
Keeping `workflow_dispatch` preserves manual testing.

**The counter-argument, which I do not find persuasive:** the daily run currently proves the
API is reachable and the pipeline executes — a crude uptime check. But `smoke-test` does that
in four requests instead of ~800, and §B.2 already recommends scheduling it. **If a daily
signal is wanted during the migration, schedule `fpl-ingest smoke-test` and disable
`fpl-ingest run`.** That keeps the canary and drops the waste.

## D.4 understat-ingest scheduling

**FACT — there is nothing to disable.** `.github/workflows/` contains only `ci.yml`
(`uv sync` + `pytest -m unit`). No scheduled workflow has ever existed. Every run has been
manual, and the four-month-stale database (§7.3) is the direct consequence.

**Recommendation: add a scheduled workflow only after the S3 redirect, not before.** Adding a
cron now would automate writing to a local SQLite file on an ephemeral runner — reproducing
fpl-ingest's exact mistake in a second repo, four months after diagnosing it there.

When it is added:
- **Weekly, not daily**, for `league-data` + `match-data` + `match-info`. Understat publishes
  after matches; there is no in-play feed. **INFERENCE** — twice weekly (Monday and Thursday)
  would cover both a weekend round and a midweek round, but weekly is the right starting point
  and the cadence should be tuned from observed `isResult` transitions.
- **Reep separately, weekly or on content-hash change** (§7.4). Different source, different
  cadence, different failure mode — a separate job, not a step in the match run.
- **Its own IAM role**, `s3:PutObject` on `raw/understat/*` and `raw/reep/*` only. No
  cross-source write access.
- **Politeness first.** The thread-pool rate bug (§7.3) must be fixed before anything scheduled
  points at Understat. An unattended cron making ~20 req/s at a small independent site is how a
  source gets lost. This is a prerequisite, not a nice-to-have.
- **Add `mypy` and an integration step to its CI** while the workflow file is open. fpl-ingest
  enforces both; understat-ingest enforces neither, and the gap will only widen once it grows
  an `extract/`/`orchestration/` structure.

---

# E. WHAT IS DELIBERATELY NOT BUILT

Abstractions considered during this pass and rejected as premature. The governing test is
target-state §8's own: *the multi-source pattern is proven by two sources, not generalised for
hypothetical future ones.*

**1. A shared `ingest-core` package.** Both repos will have a rate limiter, a retry policy, an
S3 writer, a manifest writer, a run-status classifier, and a stage orchestrator. Extracting
them into a common library is the obvious move, and it is wrong now. Two sources is exactly one
data point about what "shared" means, and the two are genuinely different — FPL is a
JSON API behind a CDN with an unauthenticated, well-behaved contract; Understat is a small site
where one of three sources is a regex against an HTML template. A shared abstraction built
against those two would encode the *coincidences* of this pair. **Copy `run_status.py` and
`stage_result.py` into understat-ingest** (177 lines) and revisit at source three.

**2. A source-plugin/registry framework.** A `Source` interface, an endpoint registry, plugin
discovery, so a new source is "just configuration." Rejected: target-state §3 already states
that adding a source needs a new client, a new `source=` prefix, and a new warehouse staging
model. That is three concrete edits. A framework to avoid three edits, before the third source
has been named, is machinery in place of work.

**3. A generic incremental-extraction abstraction.** Tempting because both repos have the same
file-existence bug. But the *fixes* differ fundamentally: FPL has `event-status`, a real
finality signal from the source; Understat has `isResult`, which means "the match ended," not
"the data is final" (§7.3). A shared `IncrementalStrategy` would have to model both, and would
end up as an interface with two unrelated implementations — a taxonomy, not a reduction.

**4. A schema registry / evolution framework for raw payloads.** Versioned JSON Schemas per
endpoint, automatic compatibility checking, migration on read. Rejected: §B.2's rule is that
raw-boundary validation is structural and sampled. A registry would pull business validation
back into ingestion, which settled input 2 explicitly removes. The lightweight
`raw_contract.json` (§B.3) covers the real need — a machine-checkable statement of what this
repo emits.

**5. An orchestration framework (Airflow / Dagster / Prefect).** Rejected on
"complexity proportional to value," which target-state §3 already invokes. The actual
requirement is *two cron schedules writing immutable objects to S3*. GitHub Actions does that
with a workflow file and an OIDC role. An orchestrator would add a service to operate, a
database to maintain, and a deployment story, to schedule two jobs. Revisit when there is a
dependency graph worth expressing — which will be a *warehouse* need, not an ingestion one.

**6. A streaming or event-driven capture path.** Kafka, Kinesis, S3-event-triggered Lambda
chains. Rejected, and target-state §3 rules it out by name. The FPL API is a 5-minute-TTL
CDN-cached REST API (§3); there is nothing to stream. Daily batch is not a compromise here, it
is a match to the source's actual update semantics.

**7. A shared credential/config service.** Two repos will each need a bucket name, a prefix, and
an IAM role. A shared secrets layer to manage six values is more moving parts than the values.
Repository variables plus OIDC (§D.2) is the whole solution.

**8. Backfill tooling for the current season.** A tool to reconstruct per-day raw objects for
2025/26 from the existing SQLite databases. Rejected because it would be fiction: the databases
hold latest-state rows, not the daily observations that never existed (§6). Target-state §7
already says the historical season is "explicitly NOT raw" and enters via a legacy path. Writing
synthesised objects into `raw/` would destroy the one property raw storage has — that everything
in it was actually observed at the time its key claims.

**9. A read path back from S3 in ingestion.** A `fpl-ingest fetch --run-id` that pulls objects
back down. Rejected: nothing in ingestion needs to read what it wrote, and §D.2's IAM role
being `PutObject`-only is a real security benefit that a read path would give up. There is no
replay command in this repo at all (§B.1) — reconstructing rows from a captured run is
fpl-warehouse's job, using its own reader, entirely outside ingestion's scope.

---

# 12. Where I think a settled input is under-specified

Per the brief: flagged, not designed around. Nothing above quietly assumes a different answer.

**None of the six settled inputs is wrong.** One is incomplete, and one has a consequence worth
naming.

## 12.1 The S3 key template has no season dimension (settled input 1)

`raw/{source}/{endpoint}/{extraction_date}/{run_id}/payload.json` identifies *when a payload
was captured*, never *which season it describes*. For a live daily capture those coincide well
enough. They come apart in three real cases:

- **August.** A capture on 2026-08-01 could be 2025/26 post-season or 2026/27 pre-season. Only
  the payload's contents disambiguate.
- **Backfill.** A 2027 capture of 2025/26 archived data files under a 2027 date.
- **Season-scoped reads.** A warehouse model wanting "all of 2026/27's bootstrap captures" must
  either open manifests or hardcode a date range. Neither is a prefix listing.

§C.1 recommends carrying season in the sidecar and manifest, which respects the settled layout
and works. But it makes season a *metadata* property rather than an *addressing* property, and
addressing is what S3 prefixes are for. **Q-G** is the decision.

I am not recommending a change — the settled template is workable, the metadata approach is
sound, and re-litigating a fixed layout for a case that is inconvenient rather than broken
would be poor value. Recording it because §C.1 also establishes that the season problem is
*imminent* — a 2026/27 run against the existing SQLite would corrupt 2025/26 data — and this is
where anyone hitting that will look.

## 12.2 "Raw capture only" is unambiguous for FPL and needs one stated exception for Understat

Settled input 2 is clean for a JSON API. For Understat's `match_info`, "the raw payload" is a
31 KB HTML page, and the useful content is inside a `JSON.parse('...')` string literal. Either
choice loses something: storing only HTML pushes a `unicode_escape` regex into dbt; storing only
extracted JSON means a template change silently loses data with no way to re-extract.

§A.6 resolves it by storing **both** — `payload.json` and `source.html` in the same prefix, with
the regex version in the sidecar. That is a deliberate, documented exception to "one payload per
object," not a general loosening of the contract. Naming it here so it is a recorded exception
rather than something discovered later in the code.

---

# 13. Consolidated open questions

Ordered by consequence.

| # | Question | Blocks | §|
|---|---|---|---|
| **Q-H** | Does an AWS account and S3 bucket exist, and is the target real S3 or an S3-compatible store? | Everything in D; the credential design changes entirely for non-AWS | D.2 |
| **Q-G** | Should season be part of the S3 key, or metadata only? | The settled key template | C.1, 12.1 |
| **Q-D** | Must `element-summary` be captured daily, or only until `event-status` reports a gameweek settled? ~300k objects/season vs. severalfold fewer. | S3 volume and cost | A.3 |
| **Q-B** | Is set-piece/penalty-taker order a signal fpl-intelligence intends to model? | Whether `set-piece-notes` needs per-run diffing or is 2 KB insurance | 2.2 |
| **Q-C** | Should the 65 MB reep CSV be column-reduced before writing to S3? Cheaper, but it is a transformation. | Whether "capture raw" holds when raw is expensive | 7.4 |
| **Q-F** | Should `raw_contract.json` be published for fpl-warehouse to consume programmatically? | The ingestion↔warehouse interface; belongs with Deliverable 5 | B.3 |
| **Q-I** | Should `entry/{TID}/event/{GW}/picks/` be un-deferred before GW3? It is the only source of real squad/bench data and is strictly non-recoverable. | fpl-intelligence's ability to backtest against actual squads | 2.3 |

---

# 14. Coverage and limits

**Verified by execution:** fpl-ingest test collection (476 total; 413/248/4/0 by marker);
live HTTP probes of 22 FPL API paths and 3 Understat paths on 2026-08-24/25; response-header
inspection of `bootstrap-static/`; SQLite table and column inventory of `~/.fpl/fpl.db`,
`~/Documents/FPL/data/fpl/fpl.db`, and `~/Documents/FPL/data/understat/understat.db`; file
listing and JSON structure inspection of `~/.fpl/raw/` (841 player files + 40 others) and
`~/Documents/FPL/data/understat/raw/` (319 files); size and row count of the cached reep CSV.

**Verified by reading:** all 36 modules under `fpl-ingest/src/fpl_ingest/`; all 6 modules under
`understat-ingest/src/understat_ingest/`; both repos' `pyproject.toml`, `.gitignore`, and
`.github/workflows/`; `fpl-warehouse/src/fpl_warehouse/integration/matching.py:385-430`;
`fpl-warehouse/docs/architecture/current-state.md` and `target-state.md`.

**Not verified:**
- **Neither test suite was executed.** fpl-ingest counts come from `pytest --collect-only`; no
  pass/fail claim is made for either repo. Same limitation the audit recorded.
- **mypy was not run.** The fpl-ingest mypy-clean claim is reported from `ci.yml`, not from a run
  performed here.
- **No S3 bucket, IAM policy, or OIDC configuration was inspected** — none is known to exist
  (Q-H). All of §D is a recommendation against an unverified target.
- **Object-count and cost estimates in §A.3 are arithmetic, not measured.**
- **The Understat probes were three single requests.** No sustained-load behaviour, no observed
  429, no confirmation of an actual rate limit. The politeness argument in §7.3/§D.4 is
  inference from the site's size and the presence of 429-handling code, not from an observed
  block.
- **Only two `element-summary` payloads were inspected live** (ids 1 and 115) plus one stored.
  The `fixtures`-array behaviour is asserted from those.
- **fpl-warehouse's builders were not re-read** beyond the reep loader; §C's statement of what
  the warehouse reads is cited from the audit, not re-derived.
