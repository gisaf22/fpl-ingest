# ADR 006 — Picks resume, settlement and time budget

Date: 2026-10-20
Status: Accepted (#116). Amended 2026-10-21 (#123): concurrency model, measured CI cost.

## Context

The entry-picks stage requests up to ~2,000 managers' picks for each finished,
data-checked gameweek, about 24,000 requests for a six-gameweek first backfill. A
scheduled run's credentials last one hour, so one run cannot be assumed to finish, and
a stopped run must resume without redoing finished work or trusting a bad response. The
decisions are #100 D4 and D6 and #116 Q4, Q9 and Q10.

## Decision

**Resume is per manager, by existence.** An entry's picks for a gameweek are captured
when `exists_prefix` finds `fpl/entry-picks/{season}/{gw}/{entry}/` (a valid capture).
Only where a 404 is legitimate (the random cohort, `absence_ok=True`), a miss there
is followed by one more check, `fpl/_invalid/not_found/entry-picks/{season}/{gw}/{entry}/`:
a stored pre-join 404 counts as captured. The `shape_invalid` and `http_error` trees are
never checked (ADR 004), so those entries retry. This is option A below.

**Settlement is a marker, separate from the others.** `_settlement/entry-picks/{gw}`
is written only when every kept entry of that gameweek is captured (a payload or a
stored legitimate 404). It is a different key from the `event-live` and `element-summary`
markers and is written and checked independently: it vouches for the picks capture only.
A gameweek whose marker exists is skipped without a request.

**Gameweeks run oldest first and the next never starts before the earlier is settled.**
A gameweek that finishes unsettled (a failed entry, a stop) ends the run, so a later
gameweek is never captured around a gap. Eligible means finished and data-checked in
bootstrap-static; a gameweek that is only finished waits.

**The time budget is wall-clock seconds, checked before each request.** `--max-seconds`
or `FPL_ENTRY_PICKS_MAX_SECONDS`; the default, 2,700 s, is provisional and below the
one-hour credential window until the first run's throughput record sizes it. When it is
spent the stage sends nothing more, requests already in flight finish and are stored, and
the manifest records `stop_reason: stopped_at_budget`. Unreached entries are absent, not
failed, and the gameweek's marker is withheld, so the next run resumes there. A stop that
captured at least one new entry exits 0, so routine backfill days raise no alert. A stop
that captured nothing new also marks `entry-picks` FAILED, so the run exits non-zero: a
budget too small to make progress must be seen, and no schedule alert exists for it (#104).
An `http_error` streak (ADR 004) is a different stop, `abort_reason`, and is a failure.

**Calls run in three layers, and only in-memory state is serialized (#123).**
- **API calls are async.** The token bucket (10/s) and `dispatch_bounded`'s limit
  (`MAX_CONCURRENT_REQUESTS`, 10) bound them, and nothing else does.
- **S3 calls run on threads, concurrently.** `OffLoop` runs every backend and writer call
  on its own pool of `MAX_CONCURRENT_REQUESTS` threads. That keeps boto3's blocking calls
  off the event loop (#115) and lets calls for different entries overlap. Boto3 clients
  and both backends are safe across threads. The pool is not the loop's default executor,
  which sizes itself from the CPU count (8 threads on a 4-vCPU runner).
- **The writer's in-memory state is locked.** A `threading.Lock` inside `LocalRawWriter`
  guards its tallies, capture index, failures, drift totals and error streak, and the
  manifest flush. The flush holds the lock through its PUT, so an older snapshot never
  lands over a newer one. Payload and sidecar PUTs run outside it.

The resume checks therefore run up to 10 at once, as the cost estimate below assumes.

**A picks run rewrites its IN_PROGRESS manifest in batches, not after every object.**
- It flushes every 50 objects or 10 s, whichever comes first, checked when an object is
  recorded. The rewrite is a whole-manifest PUT whose `objects` map grows by one entry per
  object.
- An abort, a budget stop, a withheld marker, an endpoint detail and an endpoint not
  attempted flush at once.
- The stage flushes when it ends, after waiting for storage calls in flight, so a stage
  that raises still leaves a trace counting everything it stored. `finalize` writes the
  complete manifest as before.
- Every other stage still flushes after every object.

**New keys are written with a conditional PUT (`If-None-Match: *`)**, so S3 refuses an
existing key with a 412 in the same round trip. This replaces a HEAD before every PUT.

**The run records what it cost.** `endpoints["entry-picks"].throughput` holds the
requests sent, total seconds, requests per second and gameweeks completed, and
`non_2xx_by_gameweek` the stored non-2xx responses per gameweek, so a spike is visible
and the budget can be sized (raw contract 2.8.0).

### Resume cost, measured (option A)

Read-only `aws s3api list-objects-v2 --max-keys 1` against the capture bucket, 10 calls per
case, timed client-side from a laptop (the CI runner's region was not measured):

| Case | Prefix kind | Median | Spikes to |
|---|---|---|---|
| payload-tree hit | `raw/fpl/element-summary/1/` | ~337 ms | 853 ms |
| payload-tree miss | `raw/fpl/entry-picks/2026-27/1/9999999/` | ~320 ms | 643 ms |
| `_invalid` tree hit | `raw/fpl/_invalid/shape_invalid/entry-history/2026-27/11110620/` | ~404 ms | 535 ms |
| `_invalid` tree miss | `raw/fpl/_invalid/not_found/entry-picks/2026-27/1/9999999/` | ~314 ms | 378 ms |

At worst two calls for each of ~2,000 entries is 4,000 calls per gameweek. They run
through the bounded dispatch loop (up to 10 at once), not one after another, so the cost is
small against both the credential window and the budget.

**Measured on CI (amended, #123).**
- The first production run, [39552536884](https://github.com/gisaf22/fpl-ingest/actions/runs/39552536884),
  made 4,874 requests in 2,705.7 s, **1.8/s** against a 10/s ceiling.
- Each gameweek took 0.54–0.57 s per entry. Per random-cohort entry that is seven
  S3 calls (2 resume checks, HEAD+PUT for the payload, HEAD+PUT for the sidecar, the
  manifest PUT), so about **77 ms per call**. The laptop medians above are about 4×
  that and do not apply to the runner.
- Those calls ran one at a time: `OffLoop` then held a single `asyncio.Lock` around
  every backend and writer call, so all 10 workers queued for it. The log shows it as
  bursts of 10 completions about every 6 s. The statement above, "up to 10 at once", was
  untrue until #123.
- **The single lock was the cause.** It is rejected in favour of the layered model
  above. The rate limiter, the dispatch limit and the connection pools were not binding.
- After #123 a picks entry makes 4 S3 calls (or 3 for the self cohort) plus a
  manifest PUT shared across 50 entries, and calls for different entries overlap.
- The expected rate is **6–9 requests/s**, bounded by the API rate limit, not S3.
  #123 AC10 checks the first run after merge, and below 5/s reopens this diagnosis.

**Revisit trigger:** if a production run's throughput record shows resume checks
dominating the budget once they run concurrently (#123), reopen option B then, not before.

## Alternatives Considered

- **B. One key listing per gameweek and tree, answered from memory.** Two listings per
  gameweek instead of up to 4,000 calls. Needs a listing method on `RawStorageBackend`;
  ADR 005 allows only `exists_prefix`, so it needs an ADR change even though no content
  is read. Not pursued: the cost it saves measures as small.
- **C. Skip the `not_found` check by `joined_gw`.** It saves the second call for pre-join
  weeks, but `joined_gw` is the constant `INITIAL_JOINED_GW` for every panel entry today,
  so it saves nothing until panel data carries real values (#106).
- **A resume cursor file.** A second read exception to the write-only rule (ADR 005).
- **Per-gameweek redo.** A stop mid-gameweek would redo up to ~2,000 requests.
- **Re-requesting pre-join 404s every run.** Breaks idempotence (AC3) and the marker rule.
- **Exiting non-zero on every budget stop.** Noise on routine backfill days; counting
  requests instead of seconds does not guard against a slow API.
- **Sharing one settlement flag across stages.** One stage's success could vouch for
  another's failure.
- **One lock around every storage call (`OffLoop` before #123).** It kept a writer that
  was not thread-safe single-threaded, but it also serialized every S3 round trip across
  all workers, which capped the first run at 1.8 requests/s. Rejected for the layered
  model.
- **Unlocking only the resume checks.** About 17% of the serialized time; the payload
  and sidecar writes stay queued.
- **An async S3 client (aioboto3).** A new dependency and a rewritten backend, for what a
  thread pool already gives.

## Consequences

- A rerun costs one or two `exists_prefix` calls per entry per unsettled gameweek, and
  none for a settled one.
- A permanently failing entry (for instance a self id that always 404s) blocks its
  gameweek and every later one until fixed, by design: a gap in oldest-first order is
  never papered over.
- The marker proves every kept entry of the gameweek is captured. Modelling derives "no
  picks that week" from `joined_gw` and the absence of a `payload.json`, not from the marker.
