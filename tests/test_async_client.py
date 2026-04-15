"""Unit tests for AsyncFPLClient.

Covers:
  - Successful JSON fetch
  - 429 rate-limit handling with Retry-After
  - Retryable 5xx status codes
  - Non-retryable 4xx status codes
  - Network exception retry and exhaustion
  - bootstrap caching and force-refresh
  - Context manager lifecycle (session opened/closed)
"""

from __future__ import annotations

import asyncio
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from fpl_ingest.async_client import AsyncFPLClient
from fpl_ingest.rate_limiter import NoopRateLimiter
from fpl_ingest.transport import FPLClientError

pytestmark = pytest.mark.unit

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

BOOTSTRAP_DATA = {"events": [], "elements": [], "teams": []}
HISTORY_DATA = {"history": [{"element": 1, "round": 1, "fixture": 10}], "history_past": []}


def _make_client() -> AsyncFPLClient:
    """Client with NoopRateLimiter and 2 retries for fast tests."""
    return AsyncFPLClient(rate_limiter=NoopRateLimiter(), max_retries=2, timeout=5.0)


def _mock_response(status: int, json_data: object = None, headers: dict | None = None):
    """Build a mock aiohttp response usable as an async context manager."""
    resp = AsyncMock()
    resp.status = status
    resp.headers = headers or {}
    resp.json = AsyncMock(return_value=json_data)
    resp.raise_for_status = MagicMock()
    resp.__aenter__ = AsyncMock(return_value=resp)
    resp.__aexit__ = AsyncMock(return_value=False)
    return resp


# ---------------------------------------------------------------------------
# Context manager
# ---------------------------------------------------------------------------


def test_context_manager_opens_and_closes_session():
    async def _run():
        async with _make_client() as client:
            assert client._session is not None
            assert not client._session.closed
        assert client._session.closed

    asyncio.run(_run())


# ---------------------------------------------------------------------------
# Successful fetch
# ---------------------------------------------------------------------------


def test_get_returns_json_on_200():
    async def _run():
        client = _make_client()
        resp = _mock_response(200, json_data={"key": "value"})
        with patch("aiohttp.ClientSession.get", return_value=resp):
            async with client:
                result = await client._get("https://example.com/api/")
        assert result == {"key": "value"}

    asyncio.run(_run())


# ---------------------------------------------------------------------------
# bootstrap caching
# ---------------------------------------------------------------------------


def test_get_bootstrap_caches_result():
    async def _run():
        client = _make_client()
        resp = _mock_response(200, json_data=BOOTSTRAP_DATA)
        with patch("aiohttp.ClientSession.get", return_value=resp) as mock_get:
            async with client:
                first = await client.get_bootstrap()
                second = await client.get_bootstrap()
        # Should only have fetched once despite two calls.
        assert mock_get.call_count == 1
        assert first is second

    asyncio.run(_run())


def test_get_bootstrap_force_refetches():
    async def _run():
        client = _make_client()
        resp = _mock_response(200, json_data=BOOTSTRAP_DATA)
        with patch("aiohttp.ClientSession.get", return_value=resp) as mock_get:
            async with client:
                await client.get_bootstrap()
                await client.get_bootstrap(force=True)
        assert mock_get.call_count == 2

    asyncio.run(_run())


def test_get_bootstrap_raises_on_failure():
    async def _run():
        client = _make_client()
        resp = _mock_response(500)
        with patch("aiohttp.ClientSession.get", return_value=resp):
            async with client:
                with pytest.raises(FPLClientError, match="bootstrap"):
                    await client.get_bootstrap()

    asyncio.run(_run())


# ---------------------------------------------------------------------------
# 429 rate limiting
# ---------------------------------------------------------------------------


def test_retries_on_429_then_succeeds():
    async def _run():
        client = _make_client()
        rate_limited = _mock_response(429, headers={"Retry-After": "0"})
        ok = _mock_response(200, json_data={"ok": True})

        call_count = 0

        def _fake_get(*args, **kwargs):
            nonlocal call_count
            call_count += 1
            return rate_limited if call_count == 1 else ok

        with patch("aiohttp.ClientSession.get", side_effect=_fake_get):
            async with client:
                result = await client._get("https://example.com/")
        assert result == {"ok": True}
        assert call_count == 2

    asyncio.run(_run())


def test_returns_none_after_exhausting_429_retries():
    async def _run():
        client = _make_client()
        resp = _mock_response(429, headers={"Retry-After": "0"})
        with patch("aiohttp.ClientSession.get", return_value=resp):
            async with client:
                result = await client._get("https://example.com/")
        assert result is None

    asyncio.run(_run())


# ---------------------------------------------------------------------------
# 5xx retryable errors
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("status", [500, 502, 503, 504])
def test_retries_on_5xx(status: int):
    async def _run():
        client = _make_client()
        bad = _mock_response(status)
        ok = _mock_response(200, json_data={"recovered": True})

        call_count = 0

        def _fake_get(*args, **kwargs):
            nonlocal call_count
            call_count += 1
            return bad if call_count == 1 else ok

        with patch("aiohttp.ClientSession.get", side_effect=_fake_get):
            async with client:
                result = await client._get("https://example.com/")
        assert result == {"recovered": True}

    asyncio.run(_run())


# ---------------------------------------------------------------------------
# Non-retryable 4xx
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("status", [400, 403, 404])
def test_returns_none_on_non_retryable_4xx(status: int):
    async def _run():
        client = _make_client()
        resp = _mock_response(status)
        with patch("aiohttp.ClientSession.get", return_value=resp) as mock_get:
            async with client:
                result = await client._get("https://example.com/")
        assert result is None
        assert mock_get.call_count == 1  # no retry

    asyncio.run(_run())


# ---------------------------------------------------------------------------
# Network exceptions
# ---------------------------------------------------------------------------


def test_retries_on_client_error_then_succeeds():
    import aiohttp as _aiohttp

    async def _run():
        client = _make_client()
        ok = _mock_response(200, json_data={"data": 1})
        call_count = 0

        def _fake_get(*args, **kwargs):
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                raise _aiohttp.ClientConnectionError("connection refused")
            return ok

        with patch("aiohttp.ClientSession.get", side_effect=_fake_get):
            async with client:
                result = await client._get("https://example.com/")
        assert result == {"data": 1}

    asyncio.run(_run())


def test_returns_none_after_exhausting_network_retries():
    import aiohttp as _aiohttp

    async def _run():
        client = _make_client()
        with patch(
            "aiohttp.ClientSession.get",
            side_effect=_aiohttp.ClientConnectionError("timeout"),
        ):
            async with client:
                result = await client._get("https://example.com/")
        assert result is None

    asyncio.run(_run())


# ---------------------------------------------------------------------------
# Retry design invariants
# ---------------------------------------------------------------------------


def test_token_consumed_per_dispatch():
    """Each retry attempt must acquire a fresh rate-limit token.

    If only the first attempt consumed a token (old broken design), acquire
    count would be 1. Correct design: one acquisition per HTTP dispatch.
    With max_retries=3 and all attempts returning 500, we expect 3 acquisitions.
    """
    from contextlib import asynccontextmanager

    acquire_count = 0

    class CountingLimiter:
        @asynccontextmanager
        async def request(self):
            nonlocal acquire_count
            acquire_count += 1
            yield

    async def _run():
        client = AsyncFPLClient(rate_limiter=CountingLimiter(), max_retries=3, timeout=5.0)
        bad = _mock_response(500)
        with patch("aiohttp.ClientSession.get", return_value=bad):
            async with client:
                result = await client._get("https://example.com/")
        assert result is None
        assert acquire_count == 3, (
            f"Expected 3 token acquisitions (one per dispatch), got {acquire_count}. "
            "All retries must consume their own token to honour the declared rate."
        )

    asyncio.run(_run())


def test_concurrency_slot_released_before_retry_sleep():
    """The rate-limiter context must exit before backoff sleep fires.

    A spy wraps asyncio.sleep (as used in async_client) and records whether
    the rate-limiter slot was held at the moment of each sleep call. After
    the run, no sleep should have occurred while the slot was held.
    """
    from contextlib import asynccontextmanager

    slot_held = False
    slots_held_during_sleep: list[bool] = []

    class SlotTrackingLimiter:
        @asynccontextmanager
        async def request(self):
            nonlocal slot_held
            slot_held = True
            try:
                yield
            finally:
                slot_held = False

    _real_sleep = asyncio.sleep

    async def _tracking_sleep(seconds: float) -> None:
        slots_held_during_sleep.append(slot_held)
        await _real_sleep(0)  # don't actually wait in tests

    async def _run():
        client = AsyncFPLClient(
            rate_limiter=SlotTrackingLimiter(), max_retries=2, timeout=5.0
        )
        rate_limited = _mock_response(429, headers={"Retry-After": "0.01"})
        ok = _mock_response(200, json_data={"ok": True})

        call_count = 0

        def _fake_get(*args, **kwargs):
            nonlocal call_count
            call_count += 1
            return rate_limited if call_count == 1 else ok

        with (
            patch("aiohttp.ClientSession.get", side_effect=_fake_get),
            patch("fpl_ingest.async_client.asyncio.sleep", side_effect=_tracking_sleep),
        ):
            async with client:
                result = await client._get("https://example.com/")

        assert result == {"ok": True}
        assert slots_held_during_sleep, "No backoff sleep occurred — test may be misconfigured"
        assert not any(slots_held_during_sleep), (
            "Concurrency slot was held during retry sleep. "
            "The rate-limiter context must exit before asyncio.sleep is called."
        )

    asyncio.run(_run())
