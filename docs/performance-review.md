# Performance Review: fpl-ingest HTTP Transport

## Original Architecture (pre-async)

Each pipeline stage used `requests.Session` wrapped in a `ThreadPoolExecutor`. A shared
`RequestGate` — a lock plus a monotonic timestamp — controlled when the next request
could start. All threads competed for the same gate.

```
CLI
 └── ThreadPoolExecutor(max_workers=N)
       ├── thread-1  →  RequestGate.acquire()  →  requests.get(url)
       ├── thread-2  →  RequestGate.acquire()  →  (queued, sleeping)
       └── thread-N  →  RequestGate.acquire()  →  (queued, sleeping)
```

### Original bottlenecks

**Gate-serialised throughput.** `RequestGate` advanced `next_request_at` by
`request_delay` on every acquisition regardless of worker count. Throughput was exactly
`1 / request_delay` req/s. 20 workers at 0.25s = 4 req/s. Workers added thread overhead
without adding throughput.

**Network latency stacked on gate delay.** Each request waited `request_delay` before
dispatch, then ~150ms for a response.

```
826 requests × (0.25s gate + 0.15s latency) = 330s ≈ 5.5 minutes
```

**GIL re-acquisition contention.** `ThreadPoolExecutor` released the GIL during socket
I/O but re-acquired it for JSON decoding and store writes. Under high concurrency this
created bookkeeping contention with no benefit.

### Original measured performance (cold run, GW32, 826 players)

| Stage | Observed time |
|---|---|
| bootstrap + fixtures | ~2s |
| gameweeks (32 GWs) | ~8s |
| player histories (826 players) | ~5.5 min |
| **Total cold run** | **~6 min** |

---

## Phase 1: Async Transport

Replaced `requests` + `ThreadPoolExecutor` + `RequestGate` with
`aiohttp` + `asyncio.gather` + `TokenBucketLimiter`.

```
CLI (asyncio.run)
 └── asyncio.gather(*[fetch(pid) for pid in player_ids])
       ├── coro-1  →  TokenBucketLimiter.request()  →  aiohttp.get(url)
       ├── coro-2  →  TokenBucketLimiter.request()  →  aiohttp.get(url)
       └── coro-N  →  TokenBucketLimiter.request()  →  (awaiting token)
```

The gate model conflated two separate concerns: **rate** (requests per second, a policy
decision) and **concurrency** (in-flight requests, a resource limit). Separating them
with a token bucket and a semaphore allows both to be tuned independently and injected
at construction time.

### TokenBucketLimiter parameters

| Parameter | Role | Default |
|---|---|---|
| `rate` | Tokens added per second (= max req/s) | 10.0 |
| `capacity` | Burst tokens (initial bucket fill) | `max_concurrent` |
| `max_concurrent` | Semaphore: max simultaneous in-flight requests | 10 |

### Performance after Phase 1

| Stage | Before | After |
|---|---|---|
| gameweeks (32 GWs) | ~8s | ~3s |
| player histories (826) | ~5.5 min | ~90s |
| **Total cold run** | **~6 min** | **~2 min** |

826 requests / 10 req/s = 82.6s for player histories.

A warm re-run (finished gameweeks and player histories already cached) completes in roughly 5 seconds. Only the current gameweek is re-fetched from the API; everything else is served from the local JSON cache in `FPL_RAW_DIR`. Finished gameweek data is stable once FPL has settled bonus points and score corrections, typically within 24-48 hours of the final whistle. Use `--force` if running shortly after a gameweek closes or if a late correction is suspected.

---

## Phase 2: Production-Grade Hardening

Three correctness bugs in the Phase 1 implementation were identified and fixed.

### Bug 1: Semaphore held during retry backoff

**Problem.** The original retry loop wrapped all attempts inside a single
`async with limiter.request()` context:

```python
# BROKEN: slot held for the entire retry sequence including sleeps
async with self._rate_limiter.request():
    for attempt in range(1, max_retries + 1):
        response = await session.get(url)
        if status == 429:
            await asyncio.sleep(retry_after)  # slot still held here
            continue
```

During a 429 backoff sleep, the semaphore slot was occupied. With `max_concurrent=10`,
all 10 slots could be simultaneously parked in backoff, blocking every other request
from starting.

**Fix.** The loop is inverted so sleep happens before slot acquisition:

```python
# CORRECT: slot released before every sleep
for attempt in range(1, max_retries + 1):
    if sleep_for > 0:
        await asyncio.sleep(sleep_for)   # outside rate limiter
        sleep_for = 0.0
    async with self._rate_limiter.request():
        response = await session.get(url)
        if status == 429:
            sleep_for = retry_after
            continue                     # __aexit__ fires, slot released
```

`continue` inside `async with` correctly triggers `__aexit__`, releasing the slot
before the next sleep. Verified by `test_concurrency_slot_released_before_retry_sleep`.

### Bug 2: Retries consumed no rate-limit tokens

**Problem.** One token was consumed per logical request, not per HTTP dispatch. Under
sustained 5xx errors with `max_retries=5`, a coroutine could send 5 HTTP requests while
consuming only 1 token — the effective ceiling became `declared_rate × max_retries`.

**Fix.** Each loop iteration enters `limiter.request()` independently, consuming a fresh
token per dispatch. The ceiling is enforced regardless of retry behaviour. Verified by
`test_token_consumed_per_dispatch`, which uses a counting spy limiter to assert exactly
one acquisition per attempt.

### Bug 3: TCP connector pool size disconnected from concurrency cap

**Problem.** `TCPConnector(limit=10)` was hardcoded in `AsyncFPLClient`. Passing a
`TokenBucketLimiter(max_concurrent=20)` would create a semaphore of 20 but a connection
pool of 10. The pool would silently become the bottleneck while the semaphore appeared
to allow more concurrency.

**Fix.** `AsyncFPLClient` now accepts an explicit `connector_limit` parameter. The CLI
passes it from the same constant used for `max_concurrent`, keeping them aligned:

```python
_max_concurrent = 10
rate_limiter = TokenBucketLimiter(rate=args.rate, max_concurrent=_max_concurrent)
client = AsyncFPLClient(rate_limiter=rate_limiter, connector_limit=_max_concurrent)
```

### SQLite hardening

| Setting | Reason |
|---|---|
| `PRAGMA journal_mode=WAL` | Readers never block writers; writers never block readers. |
| `PRAGMA synchronous=NORMAL` | Safe with WAL. Removes redundant full-sync calls per commit. |
| `PRAGMA busy_timeout=5000` | Two overlapping processes (scheduled + manual) retry for 5s instead of failing immediately with `database is locked`. |

`_migrate_columns` was also fixed to read `PRAGMA table_info` through `_active_conn`
when inside a transaction, rather than opening a second connection that could observe
stale schema state.

### Burst capacity default

`TokenBucketLimiter(rate=10, max_concurrent=10)` previously defaulted `capacity=1`,
meaning the initial token bucket held only 1 token. The first 10 concurrent requests
trickled out one at a time at 10/s rather than starting immediately.

Changed: `capacity` now defaults to `max_concurrent`. Initial bucket holds 10 tokens,
so the first batch of 10 dispatches without queuing. Sustained throughput is identical
(10/s); cold-start latency for the first batch is reduced from ~0.9s to ~0s.

---

## Current Architecture

```
CLI (asyncio.run)
 └── AsyncFPLClient(rate_limiter, connector_limit)
       └── asyncio.gather(*coroutines, return_exceptions=True)
             ├── coro  →  sleep(backoff)              # outside rate limiter
             │         →  limiter.request()           # acquire slot + token
             │         →  aiohttp.get(url)
             │         →  __aexit__                   # release slot + token
             └── ...
```

### Plug-and-play design

`RateLimiter` is a Protocol. Swap strategies by injecting at construction time:

```python
# Production: 10 req/s, burst 10, max 10 in-flight
client = AsyncFPLClient(rate_limiter=TokenBucketLimiter(rate=10.0, max_concurrent=10))

# Conservative: 2 req/s, max 4 in-flight
client = AsyncFPLClient(rate_limiter=TokenBucketLimiter(rate=2.0, max_concurrent=4))

# Tests: no sleeping, instant dispatch
client = AsyncFPLClient(rate_limiter=NoopRateLimiter())
```

Pipeline stages and the CLI are unaware of the limiter implementation.

### Why this is safe for the FPL API

- 10 req/s sustained is below a typical browser session hitting the FPL web app.
- The semaphore hard-caps in-flight requests even if the token bucket misfires.
- 429 responses trigger exponential backoff with respect for the `Retry-After` header.
- `--rate` on the CLI lets callers reduce the ceiling without touching any other code:

```bash
fpl-ingest --rate 4   # more conservative: ~4 req/s
fpl-ingest --rate 10  # default
```

---

## Future Work

- **Adaptive rate.** Back off on 429, ramp up on sustained success — without changing
  the `TokenBucketLimiter` interface.
- **History invalidation.** Skip re-fetching player histories whose cache file is newer
  than the last finished-gameweek deadline. Would eliminate most history fetches on
  regular re-runs.
