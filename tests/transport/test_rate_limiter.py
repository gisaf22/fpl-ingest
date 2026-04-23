"""Unit tests for rate_limiter.py.

Covers:
  - NoopRateLimiter: passes through immediately
  - TokenBucketLimiter: validation, token consumption, concurrency cap
  - TokenBucketLimiter: sustained throughput does not exceed configured rate
"""

from __future__ import annotations

import asyncio
import time

import pytest

from fpl_ingest.transport.rate_config import MAX_RATE
from fpl_ingest.transport.rate_limiter import NoopRateLimiter, RateLimiter, TokenBucketLimiter

pytestmark = pytest.mark.unit


# ---------------------------------------------------------------------------
# Protocol conformance
# ---------------------------------------------------------------------------


def test_noop_satisfies_protocol():
    assert isinstance(NoopRateLimiter(), RateLimiter)


def test_token_bucket_satisfies_protocol():
    assert isinstance(TokenBucketLimiter(rate=MAX_RATE), RateLimiter)


# ---------------------------------------------------------------------------
# NoopRateLimiter
# ---------------------------------------------------------------------------


def test_noop_does_not_sleep():
    start = time.perf_counter()
    asyncio.run(_use_noop(iterations=20))
    elapsed = time.perf_counter() - start
    assert elapsed < 0.1, f"NoopRateLimiter took {elapsed:.3f}s — should be instant"


async def _use_noop(iterations: int) -> None:
    limiter = NoopRateLimiter()
    for _ in range(iterations):
        async with limiter.request():
            pass


# ---------------------------------------------------------------------------
# TokenBucketLimiter — construction validation
# ---------------------------------------------------------------------------


def test_rejects_non_positive_rate():
    with pytest.raises(ValueError, match="rate must be positive"):
        TokenBucketLimiter(rate=0)


def test_rejects_negative_rate():
    with pytest.raises(ValueError, match="rate must be positive"):
        TokenBucketLimiter(rate=-1.0)


def test_rejects_zero_capacity():
    with pytest.raises(ValueError, match="capacity must be at least 1"):
        TokenBucketLimiter(rate=1.0, capacity=0)


def test_rejects_zero_max_concurrent():
    with pytest.raises(ValueError, match="max_concurrent must be at least 1"):
        TokenBucketLimiter(rate=1.0, max_concurrent=0)


# ---------------------------------------------------------------------------
# TokenBucketLimiter — throughput
# ---------------------------------------------------------------------------


def test_token_bucket_rate_is_respected():
    """5 sequential requests at 10 req/s should complete in ~0.4s (4 waits of 100ms)."""
    asyncio.run(_sequential_requests(n=5, rate=MAX_RATE))


async def _sequential_requests(n: int, rate: float) -> None:
    limiter = TokenBucketLimiter(rate=rate, capacity=1, max_concurrent=n)
    start = time.perf_counter()
    for _ in range(n):
        async with limiter.request():
            pass
    elapsed = time.perf_counter() - start
    # n requests at rate req/s: first is free, remaining n-1 each wait ~1/rate.
    min_expected = (n - 1) / rate * 0.8   # 20% tolerance under
    max_expected = (n - 1) / rate * 2.5   # generous upper bound for slow CI
    assert elapsed >= min_expected, (
        f"{n} requests at {rate}/s took {elapsed:.3f}s — expected >= {min_expected:.3f}s"
    )
    assert elapsed <= max_expected, (
        f"{n} requests at {rate}/s took {elapsed:.3f}s — expected <= {max_expected:.3f}s"
    )


# ---------------------------------------------------------------------------
# TokenBucketLimiter — concurrency cap
# ---------------------------------------------------------------------------


def test_max_concurrent_limits_parallelism():
    """With max_concurrent=2, only 2 tasks may be inside request() at once."""
    asyncio.run(_concurrency_check(max_concurrent=2, tasks=10))


async def _concurrency_check(max_concurrent: int, tasks: int) -> None:
    limiter = TokenBucketLimiter(rate=100.0, capacity=tasks, max_concurrent=max_concurrent)
    peak = 0
    active = 0

    async def _task() -> None:
        nonlocal peak, active
        async with limiter.request():
            active += 1
            peak = max(peak, active)
            await asyncio.sleep(0.01)  # hold slot briefly
            active -= 1

    await asyncio.gather(*[_task() for _ in range(tasks)])
    assert peak <= max_concurrent, (
        f"Peak concurrency {peak} exceeded max_concurrent={max_concurrent}"
    )


# ---------------------------------------------------------------------------
# TokenBucketLimiter — burst capacity
# ---------------------------------------------------------------------------


def test_burst_capacity_allows_immediate_dispatch():
    """capacity=5 means 5 requests can start without waiting."""
    asyncio.run(_burst_check(capacity=5))


async def _burst_check(capacity: int) -> None:
    limiter = TokenBucketLimiter(rate=1.0, capacity=capacity, max_concurrent=capacity)
    start = time.perf_counter()
    for _ in range(capacity):
        async with limiter.request():
            pass
    elapsed = time.perf_counter() - start
    # All capacity tokens are available immediately; no waiting expected.
    assert elapsed < 0.2, (
        f"Burst of {capacity} requests took {elapsed:.3f}s — expected < 0.2s"
    )
