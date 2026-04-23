"""Rate limiting strategies for the async FPL client.

`RateLimiter` is the single extension point controlling how fast and how
concurrently requests are dispatched. Inject a different implementation
into AsyncFPLClient to change pacing behaviour:

    # Production: use the shared safe default rate
    client = AsyncFPLClient(rate_limiter=TokenBucketLimiter(rate=DEFAULT_RATE, max_concurrent=10))

    # Conservative: 2 req/s, burst up to 4, max 4 in-flight
    client = AsyncFPLClient(rate_limiter=TokenBucketLimiter(rate=2.0, max_concurrent=4))

    # Tests: no sleeping, instant dispatch
    client = AsyncFPLClient(rate_limiter=NoopRateLimiter())

Design invariant: `request()` is entered and exited once per HTTP dispatch
attempt. The caller (AsyncFPLClient._dispatch_with_retry) sleeps between
retries *outside* this context so the concurrency slot is never held during
backoff.
"""

from __future__ import annotations

import asyncio
import time
from contextlib import asynccontextmanager
from typing import AsyncGenerator, Protocol, runtime_checkable

from fpl_ingest.transport.rate_config import DEFAULT_RATE


@runtime_checkable
class RateLimiter(Protocol):
    """Gates outbound HTTP requests by rate and concurrency.

    Implementations must provide a single async context manager, `request()`,
    which is entered before each HTTP call and exited after the response is
    fully consumed. The context manager is responsible for both acquiring a
    rate-limit token and a concurrency slot.
    """

    @asynccontextmanager
    async def request(self) -> AsyncGenerator[None, None]:
        """Enter before dispatching a request; exit after the response is read."""
        yield  # pragma: no cover


class NoopRateLimiter:
    """No-op limiter that never sleeps and imposes no concurrency limit.

    Intended for unit tests and local scripts where rate limiting
    would add noise without value.
    """

    @asynccontextmanager
    async def request(self) -> AsyncGenerator[None, None]:
        yield


class TokenBucketLimiter:
    """Token bucket rate limiter with a hard concurrency cap.

    Tokens refill at `rate` per second up to `capacity`. Each request
    consumes one token; the caller waits asynchronously until a token is
    available. An asyncio Semaphore independently caps the number of
    requests that can be in-flight at the same time.

    The two controls are complementary:
    - `rate` governs sustained throughput (requests per second).
    - `max_concurrent` prevents unbounded in-flight requests even if tokens
      are available (e.g. during a burst after a quiet period).

    Args:
        rate: Maximum sustained requests per second.
        capacity: Burst capacity in tokens. Defaults to max_concurrent so the
            first batch of concurrent requests dispatches immediately without
            queuing. Pass an explicit value to restrict burst size.
        max_concurrent: Hard cap on simultaneous in-flight requests.
    """

    def __init__(
        self,
        rate: float,
        capacity: int | None = None,
        max_concurrent: int = 10,
    ) -> None:
        if rate <= 0:
            raise ValueError(f"rate must be positive, got {rate!r}")
        if max_concurrent < 1:
            raise ValueError(f"max_concurrent must be at least 1, got {max_concurrent!r}")
        resolved_capacity = capacity if capacity is not None else max_concurrent
        if resolved_capacity < 1:
            raise ValueError(f"capacity must be at least 1, got {resolved_capacity!r}")

        self._requested_rate = rate
        self._rate = rate
        self._max_concurrent = max_concurrent
        self._capacity = float(resolved_capacity)
        self._tokens = float(resolved_capacity)
        self._last_refill = time.monotonic()
        self._lock = asyncio.Lock()
        self._semaphore = asyncio.Semaphore(max_concurrent)

    @property
    def rate(self) -> float:
        """Return the configured sustained request rate."""
        return self._rate

    @property
    def requested_rate(self) -> float:
        """Return the caller-requested rate before safety clamping."""
        return self._requested_rate

    @property
    def max_concurrent(self) -> int:
        """Return the configured concurrency cap."""
        return self._max_concurrent

    @property
    def capacity(self) -> int:
        """Return the configured token-bucket capacity."""
        return int(self._capacity)

    @asynccontextmanager
    async def request(self) -> AsyncGenerator[None, None]:
        """Acquire a concurrency slot and a rate-limit token, then yield."""
        async with self._semaphore:
            await self._acquire_token()
            yield

    async def _acquire_token(self) -> None:
        """Wait until a token is available, then consume one."""
        while True:
            async with self._lock:
                now = time.monotonic()
                elapsed = now - self._last_refill
                self._tokens = min(self._capacity, self._tokens + elapsed * self._rate)
                self._last_refill = now
                if self._tokens >= 1.0:
                    self._tokens -= 1.0
                    return
                # Release the lock before sleeping so other coroutines can
                # check for tokens without blocking behind this waiter.
                wait = (1.0 - self._tokens) / self._rate
            await asyncio.sleep(wait)
