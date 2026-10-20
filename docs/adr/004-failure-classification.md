# ADR 004 — Failure classification and invalid-response storage

Date: 2026-10-19
Status: Accepted (#54). Extended by #55 (the other stages) and #109 (streak abort, per-class counts).

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
a run is never SUCCESS when it withheld a payload. Raw contract 2.5.0.

The per-stage shape validators are unchanged; only the fixtures stage uses the
router so far.

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
- Remaining stages move onto the router in #55; the streak abort and per-class
  manifest counts arrive with #109, which extends this ADR.
