# ADR 003 — Async Pipeline with Token-Bucket Rate Limiting

Date: 2026-05-15
Status: Accepted

## Context

The FPL API is a public API operated by the Premier League. It has no
published rate limit, no authentication requirement, and no SLA. Aggressive
or unthrottled polling risks triggering an IP ban that would break the entire
ingestion pipeline for the season, and degrades the service for other users.

The player history endpoint (`/api/element-summary/{id}/`) must be called
once per player — typically 600–700 calls per run. Sequential HTTP requests
at this scale would take several minutes; the latency is dominated by network
round-trips rather than server processing time.

## Decision

Use `aiohttp` for async API extraction with two complementary controls:

**Token-bucket rate limiter** (`extract/http/rate_limiter.py:TokenBucketLimiter`):
Regulates the inter-request interval to stay at or below the configured rate.
The rate is:
- User-configurable via `--rate` (CLI flag), defaulting to `DEFAULT_RATE = 10.0`
  requests/second (`extract/http/rate_config.py`).
- Hard-capped at `MAX_RATE = 10.0` at both CLI argument parse time
  (`cli.py:build_parser`) and inside `AsyncFPLClient.__init__` via
  `normalize_rate`, so no code path can exceed the cap regardless of how the
  client is constructed.

**Semaphore-bounded concurrency** (`orchestration/runner.py:_MAX_CONCURRENT_REQUESTS = 10`):
Limits the number of in-flight player history requests at any moment. This
prevents the event loop from queueing hundreds of coroutines simultaneously
and bounds peak memory usage.

Together, the token bucket controls throughput (requests per second) and the
semaphore controls width (simultaneous open connections).

## Alternatives Considered

| Alternative | Reason rejected |
|---|---|
| **httpx** | Comparable async capability. `aiohttp` was already chosen for the project; introducing a second HTTP library would add a dependency for no functional gain. |
| **Synchronous `requests`** | Simpler control flow, but sequential player history fetches at ~700 requests/run would add 1–3 minutes of wall time per run even at 10 req/s. |
| **No rate limiting** | Irresponsible for a shared public API. An unbounded burst would trigger the FPL server's implicit rate controls, likely resulting in 429 or connection resets. A season-long service ban would make the project non-functional until the IP is unblocked. |

## Consequences

- **Positive:** Predictable, polite API load. The 10 req/s cap is conservative
  enough to run continuously through the season without triggering server-side
  limits.
- **Positive:** The rate is tunable at runtime via `--rate` without code
  changes — useful for testing or if the FPL API's implicit tolerance changes.
- **Positive:** Async concurrency makes the bulk player history fetch
  significantly faster than a synchronous loop: 700 requests at 10 req/s
  with 10-wide concurrency completes in ~70 seconds rather than 700+ seconds
  sequentially.
- **Negative:** `asyncio` and `aiohttp` are harder to debug than synchronous
  code. Stack traces crossing `await` boundaries require understanding the
  event loop execution model.
- **Negative:** The token bucket is a best-effort client-side control. It does
  not protect against server-side rate limiting if the FPL API changes its
  enforcement policy.

## Future

The `TokenBucketLimiter` implements a `RateLimiter` protocol
(`extract/http/rate_limiter.py`). A leaky-bucket or adaptive implementation
(e.g. one that backs off on 429 responses and recovers gradually) can be
substituted by passing a different `RateLimiter` instance to `AsyncFPLClient`
without touching orchestration or CLI routing.
