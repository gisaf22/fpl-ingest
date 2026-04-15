"""Async FPL API client backed by aiohttp.

Drop-in replacement for FPLClient in async pipeline stages. The rate
limiting strategy is injected so it can be swapped without touching
client or pipeline code.

    from fpl_ingest.async_client import AsyncFPLClient
    from fpl_ingest.rate_limiter import TokenBucketLimiter

    async with AsyncFPLClient(rate_limiter=TokenBucketLimiter(rate=10.0)) as client:
        bootstrap = await client.get_bootstrap()
        history   = await client.get_player_history(123)

Retry design
------------
Each retry attempt is a fully independent dispatch:

    for attempt in 1..max_retries:
        sleep(backoff)              # outside rate limiter — slot not held
        async with limiter.request():
            response = await session.get(url)
            if transient_error:
                backoff = computed_delay
                continue            # __aexit__ fires, slot + token released

This guarantees two properties:
  1. Semaphore slot is never held during backoff sleep. Other concurrent
     requests can proceed while one waiter is backing off.
  2. Each dispatch consumes its own rate-limit token, so the effective
     request rate never exceeds the declared ceiling even under retries.
"""

from __future__ import annotations

import asyncio
import logging
from typing import Optional

import aiohttp

from fpl_ingest.rate_limiter import RateLimiter, TokenBucketLimiter
from fpl_ingest.transport import (
    DEFAULT_MAX_RETRIES,
    DEFAULT_TIMEOUT,
    FPLClientError,
    RETRYABLE_STATUS_CODES,
    compute_retry_delay,
    parse_retry_after,
)
from fpl_ingest.types import JSON

logger = logging.getLogger(__name__)

_FPL_BASE = "https://fantasy.premierleague.com/api"

_ENDPOINTS = {
    "bootstrap": f"{_FPL_BASE}/bootstrap-static/",
    "fixtures":  f"{_FPL_BASE}/fixtures/",
    "live":      f"{_FPL_BASE}/event/{{}}/live/",
    "player":    f"{_FPL_BASE}/element-summary/{{}}/",
}

_DEFAULT_RATE = 10.0
_DEFAULT_MAX_CONCURRENT = 10

# 5xx codes that warrant a retry; 429 handled separately via Retry-After.
_RETRYABLE_5XX = RETRYABLE_STATUS_CODES - {429}


class AsyncFPLClient:
    """Async HTTP client for the FPL API.

    Manages a single aiohttp.ClientSession for connection pooling. Use as an
    async context manager, or call close() explicitly in a finally block.

    Args:
        rate_limiter: Controls dispatch rate and concurrency. Defaults to
            TokenBucketLimiter(rate=10.0, max_concurrent=10). Pass
            NoopRateLimiter() in tests to skip all sleeping.
        max_retries: Retry attempts per request on transient failures.
        timeout: Per-request timeout in seconds.
        connector_limit: Maximum open TCP connections. Should match the
            rate_limiter's max_concurrent so the connection pool is never
            smaller than the concurrency cap.
    """

    def __init__(
        self,
        rate_limiter: RateLimiter | None = None,
        max_retries: int = DEFAULT_MAX_RETRIES,
        timeout: float = DEFAULT_TIMEOUT,
        connector_limit: int = _DEFAULT_MAX_CONCURRENT,
    ) -> None:
        self._rate_limiter: RateLimiter = rate_limiter or TokenBucketLimiter(
            rate=_DEFAULT_RATE,
            max_concurrent=_DEFAULT_MAX_CONCURRENT,
        )
        self._max_retries = max_retries
        self._timeout = aiohttp.ClientTimeout(total=timeout)
        self._connector_limit = connector_limit
        self._session: Optional[aiohttp.ClientSession] = None
        self._bootstrap_cache: Optional[JSON] = None

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    async def _ensure_session(self) -> aiohttp.ClientSession:
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession(
                headers={"User-Agent": "fpl-ingest/1.0.0 (github.com/gisaf22/fpl-ingest)"},
                connector=aiohttp.TCPConnector(limit=self._connector_limit),
            )
        return self._session

    async def close(self) -> None:
        """Close the underlying aiohttp session."""
        if self._session and not self._session.closed:
            await self._session.close()

    async def __aenter__(self) -> AsyncFPLClient:
        await self._ensure_session()
        return self

    async def __aexit__(self, *_: object) -> None:
        await self.close()

    # ------------------------------------------------------------------
    # Internal fetch
    # ------------------------------------------------------------------

    async def _get(self, url: str) -> JSON | None:
        """GET url with per-dispatch rate limiting and exponential backoff.

        Each attempt is fully independent:
          - acquires a fresh semaphore slot and rate-limit token
          - releases both before any backoff sleep
        Returns None when all attempts are exhausted or a non-retryable
        error is encountered.
        """
        session = await self._ensure_session()
        sleep_for = 0.0

        for attempt in range(1, self._max_retries + 1):
            # Sleep BEFORE re-entering the rate limiter so the concurrency
            # slot is never held during backoff.
            if sleep_for > 0:
                await asyncio.sleep(sleep_for)
                sleep_for = 0.0

            async with self._rate_limiter.request():
                try:
                    async with session.get(url, timeout=self._timeout) as resp:
                        if resp.status == 429:
                            sleep_for = parse_retry_after(resp.headers.get("Retry-After"))
                            logger.warning(
                                "Rate limited (429) on %s attempt %d/%d; backing off %.1fs",
                                url, attempt, self._max_retries, sleep_for,
                            )
                            if attempt < self._max_retries:
                                # Exit context (release slot + token) before sleeping.
                                continue
                            return None

                        if resp.status in _RETRYABLE_5XX:
                            sleep_for = compute_retry_delay(0, attempt)
                            logger.warning(
                                "Retryable %d on %s attempt %d/%d; backing off %.1fs",
                                resp.status, url, attempt, self._max_retries, sleep_for,
                            )
                            if attempt < self._max_retries:
                                continue
                            return None

                        if 400 <= resp.status < 500:
                            logger.error(
                                "Non-retryable %d on %s", resp.status, url
                            )
                            return None

                        try:
                            return await resp.json(content_type=None)
                        except Exception as exc:
                            sleep_for = compute_retry_delay(0, attempt)
                            logger.warning(
                                "Invalid JSON from %s attempt %d/%d: %s",
                                url, attempt, self._max_retries, exc,
                            )
                            if attempt < self._max_retries:
                                continue
                            return None

                except aiohttp.ClientError as exc:
                    sleep_for = compute_retry_delay(0, attempt)
                    logger.warning(
                        "Request error on %s attempt %d/%d: %s",
                        url, attempt, self._max_retries, exc,
                    )
                    if attempt < self._max_retries:
                        continue

        logger.error("All %d attempts exhausted for %s", self._max_retries, url)
        return None

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    async def get_bootstrap(self, force: bool = False) -> JSON:
        """Fetch bootstrap-static data (cached for the client lifetime).

        Raises:
            FPLClientError: If the bootstrap endpoint cannot be reached.
        """
        if self._bootstrap_cache is None or force:
            logger.info("Fetching bootstrap-static data...")
            self._bootstrap_cache = await self._get(_ENDPOINTS["bootstrap"])
        if self._bootstrap_cache is None:
            raise FPLClientError("Failed to fetch bootstrap data from FPL API")
        return self._bootstrap_cache

    async def get_fixtures(self) -> JSON | None:
        """Fetch all fixtures for the current season."""
        logger.info("Fetching fixtures...")
        return await self._get(_ENDPOINTS["fixtures"])

    async def get_gw(self, gw: int) -> JSON | None:
        """Fetch live player stats for a gameweek."""
        logger.info("Fetching GW%d data...", gw)
        return await self._get(_ENDPOINTS["live"].format(gw))

    async def get_player_history(self, player_id: int) -> JSON | None:
        """Fetch element-summary history for one player."""
        logger.debug("Fetching player %d history...", player_id)
        return await self._get(_ENDPOINTS["player"].format(player_id))
