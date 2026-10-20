# ADR 004 — Failure classification and invalid-response storage

Date: 2026-10-19
Status: Accepted (#54). Extended by #55 (the other four stages); streak abort and per-class counts added by #109.

## Context

Every stage used to write a response that failed its shape check to `payload.json`
anyway, with the failure recorded only in the sidecar. A 4xx body was written the
same way. A JSON error object such as `{"detail": "Not found."}` then staged
downstream as one row of nulls, and an HTML body failed the read; either fails
`dbt build`, and because raw objects persist, every later build failed too
(gisaf22/fpl-warehouse#59).

## Decision

Raw stores what it is given; modelling derives meaning. But a response that is
not a valid capture is never stored where a reader expects one.

**One shared classifier and router** (`extract/http/response_router.py`) takes a
response and the verdict of its stage's own shape validator, and returns one class:

| Class | When | Stored | Resume |
|---|---|---|---|
| `valid` | 2xx and the shape check passes | `payload.json`, as before | existence of the payload tree means captured |
| `shape_invalid` | 2xx and the shape check fails | `fpl/_invalid/shape_invalid/{endpoint}/{date}/{run_id}/` | never checked: retried, blocks the settlement marker |
| `not_found` | 404 and the caller declared absence legitimate | `fpl/_invalid/not_found/...` | checked, only for requests with `absence_ok=True`: counts as captured |
| `http_error` | any other non-2xx under 500, except 429 (a 400, 401, 403, or a 404 the caller did not declare legitimate) | `fpl/_invalid/http_error/...` | never checked: retried, blocks the marker |
| `fetch_failed` | no response, or 429 or 5xx after the client's retries | nothing | retried |

`absence_ok` is a parameter of the call, not a property of the endpoint: the same
endpoint can declare a 404 legitimate for one request (a random-cohort manager
before they joined) and not for another (a configured id that must exist).
Fixtures never declares it, so its 404 is an `http_error`.

**Where.** Invalid responses live in a reserved sibling tree, `fpl/_invalid/{class}/`,
mirroring the endpoint's own key tail. The body is `response.body` (its content type
is in the sidecar, and it need not be JSON), beside a `metadata.json`. No endpoint
segment can start with an underscore and the body is never `payload.*`, so a glob for
payloads cannot match anything there.

**What is recorded.** The invalid sidecar has the valid sidecar's fields plus
`class` and `reason` (and the shape failures in `shape_validation` for
`shape_invalid`). It has no `drift` block, and invalid captures are excluded from
`endpoints[...].drift`: a body already known to be wrong is not a sample of the
endpoint's shape. Every invalid capture has a `captures[]` entry with its
`_invalid` key, `usable: false` and `class`, and its endpoint counts as failed, so
a run is never SUCCESS when it withheld a payload, unless the caller declared the
outcome expected (below). Raw contract 2.5.0.

**Expected outcomes (#115).** A caller may declare, per request, that an invalid outcome
is legitimate: `not_found` (with `absence_ok`) or an outcome its shape verdict names,
which for now is `no_picks`, a random-cohort `entry-history` with an empty `current`.
A declared response is stored under `fpl/_invalid/{class}/` and counted in
`invalid_counts` and `captures[]` (`usable: false`) as above, but carries
`expected: true` on its sidecar and entry and is tallied in its endpoint's `expected`
count instead of `failed`. An endpoint is SUCCESS when nothing failed, so a run whose
only invalid captures are expected stays SUCCESS with exit 0. Only declared outcomes
qualify: a 403 or any other 4xx, a 5xx, a malformed body and a self-cohort 404 still
fail the endpoint. Expected outcomes neither count toward nor reset the streak.
Raw contract 2.7.0.

**Streak abort (#109).** A run stops itself after N consecutive `http_error` responses
(default 10; `--http-error-streak` or `FPL_HTTP_ERROR_STREAK`; at least 1). Only a
`valid` response resets the streak; `not_found`, `shape_invalid` and `fetch_failed`
neither count nor reset it, because they say nothing about whether the API is refusing
us. The router feeds each response to the run's writer, which sets `aborted` on the
Nth. Dispatchers stop sending when it is set; requests already in flight finish and are
stored. The abort logs `streak`, `threshold` and `last_status`, the manifest records
`abort_reason: aborted_http_error_streak`, and the run exits non-zero as any run with
failed endpoints does. Items that got `http_error` are never marked captured, so the
next run retries them. A budget stop (#103) is a different reason. The bounded dispatch
loop (`http/dispatch.py`, #115) checks `aborted` before each dispatch; the manager panel
draw uses it, with the client's concurrency limit. event-live and element-summary fetch
everything with `asyncio.gather` before routing and do not.

**Per-class counts (#109).** A finalized manifest carries `invalid_counts`:
`{not_found, shape_invalid, http_error}`, zero-filled, counted from the run's invalid
`captures[]` entries, so `fetch_failed` is not counted and a spike shows without
reading sidecars. Raw contract 2.6.0.

The per-stage shape validators are unchanged. Fixtures (#54) and bootstrap-static,
event-status, event-live and element-summary (#55) use the router through one shared
tail, `route_and_record` and `fetch_and_route` (#115). None declares `absence_ok`, so a
404 from any of them is `http_error`. An invalid event-live response
earns no `_settlement/event-live` marker, and an invalid element-summary player is
retried next run: neither lands in the payload tree the resume checks read. The manager
panel (#115) uses it too: a random-cohort 404 or empty `current` is declared expected, a
self-cohort 404 is an `http_error`.

## Alternatives Considered

- **A sibling file in the same run directory** (for example `payload.invalid.json`
  beside `metadata.json`). `exists_prefix` on the run directory would then treat a
  failed fetch as captured, breaking resume and finality checks.
- **Checking all of `_invalid` as one tree on resume.** A transient `shape_invalid`
  response would never be retried.
- **Reading the sidecar to classify on resume.** A read-back; see ADR 005.
- **Folding other 4xx into `shape_invalid`.** It blurs "the body was wrong" with
  "the server refused us".
- **Consolidating the six shape validators now.** It touches the other stages,
  widens the change and risks changing what a stage accepts.

## Consequences

- A consumer of `payload.json` objects only ever sees valid captures. A run with an
  invalid fixtures response has no fixtures `payload.json`; the consumer must treat
  that as "not captured".
- The strategy doc's earlier rule, "write the payload, record the failure, do not
  discard" (B.2), no longer holds for the stages on the router. The response is still
  kept and the failure still recorded, but not as a payload.
- Count-only with no abort was rejected (#109): it hammers a blocked API and surfaces late.
