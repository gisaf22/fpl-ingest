"""Async FPL API client backed by aiohttp.

Responsible for: HTTP lifecycle (session open/close), per-request retry
with exponential backoff, rate limiting via an injected RateLimiter, and
in-memory bootstrap caching. This module has no knowledge of FPL domain
models or pipeline stages.

Retry design: each attempt is a fully independent dispatch so that the
rate-limiter slot is never held during backoff sleep.

    async with AsyncFPLClient(rate_limiter=TokenBucketLimiter(rate=10.0)) as client:
        bootstrap = await client.get_bootstrap()
        history   = await client.get_player_history(123)
"""

from __future__ import annotations

import asyncio
import logging
from typing import NamedTuple, Optional

import aiohttp

from fpl_ingest.rate_limiter import RateLimiter, TokenBucketLimiter
from fpl_ingest.sync_http import (
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
    "live":      f"{_FPL_BASE}/event/{{gw}}/live/",
    "player":    f"{_FPL_BASE}/element-summary/{{player_id}}/",
}

_DEFAULT_RATE = 10.0
_DEFAULT_MAX_CONCURRENT = 10

# 5xx codes that warrant a retry; 429 is handled separately via Retry-After.
_RETRYABLE_5XX = RETRYABLE_STATUS_CODES - {429}


class RequestOutcome(NamedTuple):
    """Outcome of a single HTTP attempt, consumed by _fetch_with_retries."""

    data: JSON | None
    should_retry: bool = False
    backoff_seconds: float = 0.0


class AsyncFPLClient:
    """Async HTTP client for the FPL API.

    Manages a single aiohttp.ClientSession for connection pooling. Use as
    an async context manager or call close() explicitly in a finally block.

    Args:
        rate_limiter: Controls dispatch rate and concurrency. Defaults to
            TokenBucketLimiter(rate=10.0, max_concurrent=10). Pass
            NoopRateLimiter() in tests to skip all sleeping.
        max_retries: Retry attempts per request on transient failures.
        timeout: Per-request timeout in seconds.
        connector_limit: Maximum open TCP connections. Must match the
            rate_limiter's max_concurrent — if the connector limit is smaller
            than max_concurrent, coroutines will stall waiting for a free
            connection; if larger, the pool allows more connections than the
            rate limiter permits concurrently, wasting resources.
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
        """Return the active session, creating it if necessary.

        Session cannot be created in __init__ because aiohttp.ClientSession
        must be instantiated inside a running event loop. Lazy initialization
        here ensures the session is always created in the correct async context.
        """
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

    async def _fetch_with_retries(self, url: str) -> JSON | None:
        """GET url with per-dispatch rate limiting and exponential backoff.

        Each attempt acquires a fresh semaphore slot and rate-limit token,
        and releases both before any backoff sleep. Returns None when all
        attempts are exhausted or a non-retryable error is encountered.

        Args:
            url: Fully-qualified URL to fetch.

        Returns:
            Decoded JSON, or None on failure.
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
                outcome = await self._attempt_request(session, url, attempt)
                if not outcome.should_retry:
                    return outcome.data
                sleep_for = outcome.backoff_seconds

        logger.error("All %d attempts exhausted for %s", self._max_retries, url)
        return None

    async def _attempt_request(
        self,
        session: aiohttp.ClientSession,
        url: str,
        attempt: int,
    ) -> RequestOutcome:
        """Perform one HTTP GET and classify the outcome.

        Returns a RequestOutcome describing whether to retry and how long to wait.
        asyncio.TimeoutError is a subclass of aiohttp.ClientError and is treated
        as retryable — transient network timeouts are worth retrying, whereas
        hard failures (e.g. DNS resolution) are also caught here and retried up
        to max_retries before giving up.
        """
        try:
            async with session.get(url, timeout=self._timeout) as resp:
                return await self._classify_response(resp, url, attempt)
        except aiohttp.ClientError as exc:
            logger.warning(
                "Request error on %s attempt %d/%d: %s",
                url, attempt, self._max_retries, exc,
            )
            return self._retry_decision(attempt)

    async def _classify_response(
        self,
        resp: aiohttp.ClientResponse,
        url: str,
        attempt: int,
    ) -> RequestOutcome:
        """Route an HTTP response to success, retry, or terminal failure."""
        if resp.status == 429:
            backoff = parse_retry_after(resp.headers.get("Retry-After"))
            logger.warning(
                "Rate limited (429) on %s attempt %d/%d; backing off %.1fs",
                url, attempt, self._max_retries, backoff,
            )
            return self._retry_decision(attempt, backoff)

        if resp.status in _RETRYABLE_5XX:
            backoff = compute_retry_delay(0, attempt)
            logger.warning(
                "Retryable %d on %s attempt %d/%d; backing off %.1fs",
                resp.status, url, attempt, self._max_retries, backoff,
            )
            return self._retry_decision(attempt, backoff)

        if 400 <= resp.status < 500:
            logger.error("Non-retryable %d on %s", resp.status, url)
            return RequestOutcome(data=None)

        try:
            return RequestOutcome(data=await resp.json(content_type=None))
        except (aiohttp.ContentTypeError, ValueError) as exc:
            logger.warning(
                "Could not decode JSON from %s attempt %d/%d: %s",
                url, attempt, self._max_retries, exc,
            )
            return self._retry_decision(attempt)

    def _retry_decision(self, attempt: int, backoff: float | None = None) -> RequestOutcome:
        """Return the appropriate RequestOutcome based on whether retries remain."""
        is_last = attempt >= self._max_retries
        if is_last:
            return RequestOutcome(data=None)
        delay = backoff if backoff is not None else compute_retry_delay(0, attempt)
        return RequestOutcome(data=None, should_retry=True, backoff_seconds=delay)

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    async def get_bootstrap(self, force: bool = False) -> JSON:
        """Fetch bootstrap-static data, caching the result for the client lifetime.

        Args:
            force: If True, bypass the cache and fetch fresh data.

        Returns:
            Bootstrap data dict (events, elements, teams, element_types, ...).

        Raises:
            FPLClientError: If the bootstrap endpoint cannot be reached.
        """
        if self._bootstrap_cache is None or force:
            logger.info("Fetching bootstrap-static data...")
            self._bootstrap_cache = await self._fetch_with_retries(_ENDPOINTS["bootstrap"])
        if self._bootstrap_cache is None:
            raise FPLClientError("Failed to fetch bootstrap data from FPL API")
        return self._bootstrap_cache

    async def get_fixtures(self) -> JSON:
        """Fetch all fixtures for the current season.

        Returns:
            List of fixture dicts.

        Raises:
            FPLClientError: If the fixtures endpoint cannot be reached.
        """
        logger.info("Fetching fixtures...")
        result = await self._fetch_with_retries(_ENDPOINTS["fixtures"])
        if result is None:
            raise FPLClientError("Failed to fetch fixtures data from FPL API")
        return result

    async def get_gw(self, gameweek: int) -> JSON:
        """Fetch live player stats for one gameweek.

        Args:
            gameweek: Gameweek number (1–38).

        Returns:
            Dict with an 'elements' list.

        Raises:
            FPLClientError: If the live gameweek endpoint cannot be reached.
        """
        logger.info("Fetching gameweek %d data...", gameweek)
        result = await self._fetch_with_retries(_ENDPOINTS["live"].format(gw=gameweek))
        if result is None:
            raise FPLClientError(f"Failed to fetch gameweek {gameweek} data from FPL API")
        return result

    async def get_player_history(self, player_id: int) -> JSON:
        """Fetch element-summary history for one player.

        Args:
            player_id: FPL element ID.

        Returns:
            Dict with 'history' and 'history_past' lists.

        Raises:
            FPLClientError: If the element-summary endpoint cannot be reached.
        """
        logger.debug("Fetching player %d history...", player_id)
        result = await self._fetch_with_retries(_ENDPOINTS["player"].format(player_id=player_id))
        if result is None:
            raise FPLClientError(f"Failed to fetch history for player {player_id} from FPL API")
        return result
