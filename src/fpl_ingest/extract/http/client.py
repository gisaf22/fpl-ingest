"""Async FPL API client backed by aiohttp.

Responsible for: HTTP lifecycle (session open/close), per-request retry
with exponential backoff, rate limiting via an injected RateLimiter, and
in-memory bootstrap caching. This module has no knowledge of transform models
or orchestration policy.

Retry design: each attempt is a fully independent dispatch so that the
rate-limiter slot is never held during backoff sleep.

    async with AsyncFPLClient(rate_limiter=TokenBucketLimiter(rate=DEFAULT_RATE)) as client:
        bootstrap = await client.get_bootstrap()
        history   = await client.get_player_history(123)

Also exports ``cancel_pending_tasks`` — a shared utility used by the entity
fetch stages.
"""

from __future__ import annotations

import asyncio
import json
import logging
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, List, NamedTuple, Optional, Tuple, cast

import aiohttp

from fpl_ingest.extract.http.rate_config import DEFAULT_RATE, normalize_rate
from fpl_ingest.extract.http.rate_limiter import RateLimiter, TokenBucketLimiter
from fpl_ingest.extract.http.sync_http import (
    DEFAULT_MAX_RETRIES,
    DEFAULT_TIMEOUT,
    FPLClientError,
    RETRYABLE_STATUS_CODES,
    compute_retry_delay,
    parse_retry_after,
)
from fpl_ingest.extract.http.types import JSON

logger = logging.getLogger(__name__)

_FPL_BASE = "https://fantasy.premierleague.com/api"

_ENDPOINTS = {
    "bootstrap":    f"{_FPL_BASE}/bootstrap-static/",
    "fixtures":     f"{_FPL_BASE}/fixtures/",
    "live":         f"{_FPL_BASE}/event/{{gw}}/live/",
    "player":       f"{_FPL_BASE}/element-summary/{{player_id}}/",
    "event_status": f"{_FPL_BASE}/event-status/",
}

_DEFAULT_MAX_CONCURRENT = 10

# 5xx codes that warrant a retry; 429 is handled separately via Retry-After.
_RETRYABLE_5XX = RETRYABLE_STATUS_CODES - {429}


@dataclass(frozen=True)
class RawResponse:
    """One HTTP response captured verbatim, for the raw-capture boundary.

    Carries everything the raw-capture sidecar (strategy doc A.5) needs that
    decoded JSON throws away: the exact bytes the source sent, the status, the
    response headers, the request timings, and how many attempts it took.

    ``body`` is the response body as received — never re-serialised — so a
    checksum over it is a checksum of what the API actually returned.
    """

    url: str
    status: int
    headers: Dict[str, str]
    body: bytes
    requested_at: datetime
    received_at: datetime
    attempt_count: int = 1

    def json(self) -> JSON | None:
        """Decode the body as JSON, or return None if it does not parse."""
        try:
            return cast(JSON, json.loads(self.body.decode("utf-8")))
        except (UnicodeDecodeError, ValueError):
            return None


class RequestOutcome(NamedTuple):
    """Outcome of a single HTTP attempt, consumed by _fetch_with_retries."""

    data: JSON | None
    should_retry: bool = False
    backoff_seconds: float = 0.0
    raw: RawResponse | None = None


class AsyncFPLClient:
    """Async HTTP client for the FPL API.

    Manages a single aiohttp.ClientSession for connection pooling. Use as
    an async context manager or call close() explicitly in a finally block.

    Args:
        rate_limiter: Controls dispatch rate and concurrency. Defaults to
            TokenBucketLimiter(rate=DEFAULT_RATE, max_concurrent=10). Pass
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
        self._rate_limiter = self._resolve_rate_limiter(rate_limiter)
        self._max_retries = max_retries
        self._timeout = aiohttp.ClientTimeout(total=timeout)
        self._connector_limit = connector_limit
        self._session: Optional[aiohttp.ClientSession] = None
        self._bootstrap_cache: Optional[Dict[str, Any]] = None

    def _resolve_rate_limiter(self, rate_limiter: RateLimiter | None) -> RateLimiter:
        """Normalize rate configuration at the async-client boundary."""
        if rate_limiter is None:
            return TokenBucketLimiter(
                rate=normalize_rate(DEFAULT_RATE),
                max_concurrent=_DEFAULT_MAX_CONCURRENT,
            )
        if isinstance(rate_limiter, TokenBucketLimiter):
            applied_rate = normalize_rate(rate_limiter.requested_rate)
            if applied_rate == rate_limiter.requested_rate:
                return rate_limiter
            logger.warning(
                "Async client rate limited to safe maximum: requested_rate=%.1f applied_rate=%.1f",
                rate_limiter.requested_rate,
                applied_rate,
            )
            return TokenBucketLimiter(
                rate=applied_rate,
                capacity=rate_limiter.capacity,
                max_concurrent=rate_limiter.max_concurrent,
            )
        return rate_limiter

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
        data, _ = await self._fetch_with_retries_raw(url)
        return data

    async def _fetch_with_retries_raw(self, url: str) -> Tuple[JSON | None, RawResponse | None]:
        """GET url and return both the decoded JSON and the verbatim response.

        Identical dispatch, retry, and rate-limiting behaviour to
        ``_fetch_with_retries`` — this is the same loop, and the decoded-JSON
        path delegates to it. The second element is the last response actually
        received from the wire, present even when the decoded value is None
        (a non-2xx status, or a body that did not parse as JSON). The raw
        boundary needs that: a surprising payload is the one most worth
        keeping (strategy doc B.2). It is None only when no response was ever
        received — a transport failure, or retries exhausted on 429/5xx.

        Args:
            url: Fully-qualified URL to fetch.

        Returns:
            ``(decoded_json_or_None, raw_response_or_None)``.
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
                    return outcome.data, outcome.raw
                sleep_for = outcome.backoff_seconds

        logger.error("All %d attempts exhausted for %s", self._max_retries, url)
        return None, None

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
        requested_at = datetime.now(timezone.utc)
        try:
            async with session.get(url, timeout=self._timeout) as resp:
                return await self._classify_response(resp, url, attempt, requested_at)
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
        requested_at: datetime | None = None,
    ) -> RequestOutcome:
        """Route an HTTP response to success, retry, or terminal failure.

        The body is read once, up front, as bytes. Everything downstream —
        the retry classification and the decoded JSON — works from those same
        bytes, so the raw capture and the decoded value can never disagree
        about what the source sent.
        """
        requested_at = requested_at or datetime.now(timezone.utc)
        body = await self._read_body(resp)
        raw = self._raw_response(resp, url, body, requested_at, attempt)

        if resp.status == 429:
            backoff = parse_retry_after(resp.headers.get("Retry-After"))
            logger.warning(
                "Rate limited (429) on %s attempt %d/%d; backing off %.1fs",
                url, attempt, self._max_retries, backoff,
            )
            return self._retry_decision(attempt, backoff, raw=raw)

        if resp.status in _RETRYABLE_5XX:
            backoff = compute_retry_delay(0, attempt)
            logger.warning(
                "Retryable %d on %s attempt %d/%d; backing off %.1fs",
                resp.status, url, attempt, self._max_retries, backoff,
            )
            return self._retry_decision(attempt, backoff, raw=raw)

        if 400 <= resp.status < 500:
            logger.error("Non-retryable %d on %s", resp.status, url)
            return RequestOutcome(data=None, raw=raw)

        decoded = raw.json() if raw is not None else None
        if decoded is None:
            logger.warning(
                "Could not decode JSON from %s attempt %d/%d",
                url, attempt, self._max_retries,
            )
            return self._retry_decision(attempt, raw=raw)
        return RequestOutcome(data=decoded, raw=raw)

    async def _read_body(self, resp: aiohttp.ClientResponse) -> bytes | None:
        """Read the response body as bytes, or None if the read itself failed."""
        try:
            return await resp.read()
        except (aiohttp.ClientError, AttributeError, TypeError):
            return None

    def _raw_response(
        self,
        resp: aiohttp.ClientResponse,
        url: str,
        body: bytes | None,
        requested_at: datetime,
        attempt: int,
    ) -> RawResponse | None:
        """Build a RawResponse from a live response, or None if bytes are absent."""
        if body is None:
            return None
        return RawResponse(
            url=url,
            status=resp.status,
            headers={str(k).lower(): str(v) for k, v in dict(resp.headers).items()},
            body=body,
            requested_at=requested_at,
            received_at=datetime.now(timezone.utc),
            attempt_count=attempt,
        )

    def _retry_decision(
        self,
        attempt: int,
        backoff: float | None = None,
        *,
        raw: RawResponse | None = None,
    ) -> RequestOutcome:
        """Return the appropriate RequestOutcome based on whether retries remain."""
        is_last = attempt >= self._max_retries
        if is_last:
            return RequestOutcome(data=None, raw=raw)
        delay = backoff if backoff is not None else compute_retry_delay(0, attempt)
        return RequestOutcome(data=None, should_retry=True, backoff_seconds=delay, raw=raw)

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    async def get_bootstrap(self, force: bool = False) -> Dict[str, Any]:
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
            self._bootstrap_cache = cast(Optional[Dict[str, Any]], await self._fetch_with_retries(_ENDPOINTS["bootstrap"]))
        if self._bootstrap_cache is None:
            raise FPLClientError("Failed to fetch bootstrap data from FPL API")
        return self._bootstrap_cache

    async def get_bootstrap_raw(self) -> RawResponse:
        """Fetch bootstrap-static and return the verbatim response.

        The raw-capture counterpart of ``get_bootstrap``: same URL, same retry
        and rate-limiting path, but the caller gets the bytes, status, headers,
        timings, and attempt count instead of decoded JSON. It deliberately
        does not populate (or read) the decoded bootstrap cache — the raw
        boundary stores what the API sent, not a reused decode. A non-2xx
        status or an unparseable body is returned rather than raised; judging
        the shape is the caller's job at the raw boundary.

        Returns:
            RawResponse for the bootstrap-static endpoint.

        Raises:
            FPLClientError: If no response was received at all.
        """
        logger.info("Fetching bootstrap-static (raw)...")
        _, raw = await self._fetch_with_retries_raw(_ENDPOINTS["bootstrap"])
        if raw is None:
            raise FPLClientError("Failed to fetch bootstrap data from FPL API")
        return raw

    async def get_fixtures(self) -> List[Any]:
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
        return cast(List[Any], result)

    async def get_fixtures_raw(self) -> RawResponse:
        """Fetch all fixtures and return the verbatim response.

        The raw-capture counterpart of ``get_fixtures``: same URL, same retry
        and rate-limiting path, but the caller gets the bytes, status, headers,
        timings, and attempt count instead of decoded JSON. A non-2xx status or
        an unparseable body is returned rather than raised — judging the shape
        is the caller's job at the raw boundary.

        Returns:
            RawResponse for the fixtures endpoint.

        Raises:
            FPLClientError: If no response was received at all.
        """
        logger.info("Fetching fixtures (raw)...")
        _, raw = await self._fetch_with_retries_raw(_ENDPOINTS["fixtures"])
        if raw is None:
            raise FPLClientError("Failed to fetch fixtures data from FPL API")
        return raw

    async def get_gw(self, gameweek: int) -> Dict[str, Any]:
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
        return cast(Dict[str, Any], result)

    async def get_gameweek_live_raw(self, gameweek: int) -> RawResponse:
        """Fetch one gameweek's live data and return the verbatim response.

        The raw-capture counterpart of ``get_gw``: same URL, same retry and
        rate-limiting path, but the caller gets the bytes, status, headers,
        timings, and attempt count instead of decoded JSON. One call per
        gameweek, matching the per-gameweek concurrency the gameweeks stage
        already uses. A non-2xx status or an unparseable body is returned
        rather than raised — judging the shape is the caller's job at the raw
        boundary.

        Args:
            gameweek: Gameweek number (1-38).

        Returns:
            RawResponse for the ``event/{gw}/live`` endpoint.

        Raises:
            FPLClientError: If no response was received at all.
        """
        logger.info("Fetching gameweek %d (raw)...", gameweek)
        _, raw = await self._fetch_with_retries_raw(_ENDPOINTS["live"].format(gw=gameweek))
        if raw is None:
            raise FPLClientError(f"Failed to fetch gameweek {gameweek} data from FPL API")
        return raw

    async def get_event_status_raw(self) -> RawResponse:
        """Fetch event-status and return the verbatim response.

        Single request, no gameweek parameter — event-status returns finality
        for every event in the current window in one call. A non-2xx status
        or an unparseable body is returned rather than raised — judging the
        shape is the caller's job at the raw boundary.

        Returns:
            RawResponse for the ``event-status`` endpoint.

        Raises:
            FPLClientError: If no response was received at all.
        """
        logger.info("Fetching event-status (raw)...")
        _, raw = await self._fetch_with_retries_raw(_ENDPOINTS["event_status"])
        if raw is None:
            raise FPLClientError("Failed to fetch event-status data from FPL API")
        return raw

    async def get_element_summary_raw(self, player_id: int) -> RawResponse:
        """Fetch one player's element-summary and return the verbatim response.

        The raw-capture counterpart of ``get_player_history``: same URL, same
        retry and rate-limiting path, but the caller gets the bytes, status,
        headers, timings, and attempt count instead of decoded JSON. One call
        per player, matching the per-player concurrency the element-summary
        stage already uses. A non-2xx status or an unparseable body is
        returned rather than raised — judging the shape is the caller's job at
        the raw boundary.

        Args:
            player_id: FPL element ID.

        Returns:
            RawResponse for the ``element-summary/{player_id}`` endpoint.

        Raises:
            FPLClientError: If no response was received at all.
        """
        logger.debug("Fetching player %d element-summary (raw)...", player_id)
        _, raw = await self._fetch_with_retries_raw(_ENDPOINTS["player"].format(player_id=player_id))
        if raw is None:
            raise FPLClientError(f"Failed to fetch history for player {player_id} from FPL API")
        return raw

    async def get_player_history(self, player_id: int) -> Dict[str, Any]:
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
        return cast(Dict[str, Any], result)


# ---------------------------------------------------------------------------
# Shared async utility for task cancellation.
# ---------------------------------------------------------------------------


async def cancel_pending_tasks(tasks: set[asyncio.Task[Any]]) -> None:
    """Cancel pending tasks and await their completion."""
    if not tasks:
        return
    for task in tasks:
        task.cancel()
    await asyncio.gather(*tasks, return_exceptions=True)
