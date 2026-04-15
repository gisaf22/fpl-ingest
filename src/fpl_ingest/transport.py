"""HTTP transport helpers for the FPL client."""

from __future__ import annotations

import logging
import random
import time
from dataclasses import dataclass, field
from datetime import datetime
from email.utils import parsedate_to_datetime
from threading import Lock
from typing import Any

import requests

logger = logging.getLogger(__name__)

DEFAULT_REQUEST_DELAY = 1.0
DEFAULT_PLAYER_HISTORY_REQUEST_DELAY = 0.25
DEFAULT_MAX_RETRIES = 5
DEFAULT_TIMEOUT = 30
MAX_DELAY = 60
RATE_LIMIT_STATUS = 429
RETRYABLE_STATUS_CODES = {429, 500, 502, 503, 504}


class FPLClientError(RuntimeError):
    """Raised when the client cannot obtain a valid response from the API."""


class _RetryRequest(Exception):
    """Internal signal indicating the current request should be retried."""


@dataclass
class RequestGate:
    """Shared pacing gate to avoid bursty parallel request starts."""

    lock: Lock = field(default_factory=Lock)
    next_request_at: float = 0.0


def compute_retry_delay(request_delay: float, attempt: int) -> float:
    """Compute backoff delay for a retry attempt."""
    base_delay = max(request_delay, 0)
    return min(base_delay + (2 ** (attempt - 1)) + random.uniform(0, 1), MAX_DELAY)


def sleep_with_jitter(request_delay: float, request_gate: RequestGate | None = None) -> None:
    """Sleep before a request to smooth out bursts against the upstream API."""
    if request_delay <= 0:
        return
    jitter = random.uniform(0, 0.3 * request_delay)
    if request_gate is None:
        time.sleep(request_delay + jitter)
        return

    with request_gate.lock:
        now = time.monotonic()
        start_at = max(now, request_gate.next_request_at)
        request_gate.next_request_at = start_at + request_delay + jitter
        sleep_for = max(start_at - now, 0.0)

    if sleep_for > 0:
        time.sleep(sleep_for)


def parse_retry_after(value: str | None) -> float:
    """Parse Retry-After as seconds or HTTP-date, falling back safely."""
    if not value:
        return 30.0
    try:
        return max(float(value), 0.0)
    except ValueError:
        pass

    try:
        retry_at = parsedate_to_datetime(value)
        delay = (retry_at - datetime.now(retry_at.tzinfo)).total_seconds()
        return max(delay, 0.0)
    except (TypeError, ValueError, OverflowError):
        logger.warning("Invalid Retry-After header %r; using 30s fallback", value)
        return 30.0


def _handle_rate_limit(
    resp: requests.Response,
    url: str,
    attempt: int,
    max_retries: int,
) -> None:
    """Handle a 429 response, sleeping before a retry when possible."""
    retry_after = parse_retry_after(resp.headers.get("Retry-After"))
    logger.warning(
        "Rate limited (429) for %s on attempt %d/%d; waiting %.1fs",
        url, attempt, max_retries, retry_after,
    )
    if attempt < max_retries:
        time.sleep(retry_after)
        raise _RetryRequest


def _decode_json(
    resp: requests.Response,
    url: str,
    attempt: int,
    request_delay: float,
    max_retries: int,
) -> Any:
    """Decode a successful JSON response, retrying invalid payloads when allowed."""
    try:
        return resp.json()
    except ValueError as exc:
        logger.warning(
            "Invalid JSON from %s on attempt %d/%d: %s",
            url, attempt, max_retries, exc,
        )
        if attempt < max_retries:
            time.sleep(compute_retry_delay(request_delay, attempt))
            raise _RetryRequest
    return None


def _handle_response(
    resp: requests.Response,
    url: str,
    attempt: int,
    request_delay: float,
    max_retries: int,
) -> Any | None:
    """Classify and handle an HTTP response."""
    if resp.status_code == RATE_LIMIT_STATUS:
        _handle_rate_limit(resp, url, attempt, max_retries)
        return None

    if resp.status_code in RETRYABLE_STATUS_CODES:
        logger.warning(
            "Request to %s returned retryable status %s on attempt %d/%d",
            url, resp.status_code, attempt, max_retries,
        )
        if attempt < max_retries:
            time.sleep(compute_retry_delay(request_delay, attempt))
            raise _RetryRequest
        return None

    if 400 <= resp.status_code < 500:
        logger.error(
            "Request to %s failed with non-retryable status %s",
            url, resp.status_code,
        )
        return None

    resp.raise_for_status()
    return _decode_json(resp, url, attempt, request_delay, max_retries)


def execute_json_request(
    session: requests.Session,
    url: str,
    *,
    timeout: float,
    request_delay: float,
    max_retries: int,
    request_gate: RequestGate | None = None,
) -> Any | None:
    """Execute a GET request with retry logic for transient failures."""
    for attempt in range(1, max_retries + 1):
        sleep_with_jitter(request_delay, request_gate=request_gate)

        try:
            resp = session.get(url, timeout=timeout)
            return _handle_response(resp, url, attempt, request_delay, max_retries)
        except _RetryRequest:
            continue
        except requests.RequestException as exc:
            logger.warning(
                "Request failed for %s on attempt %d/%d: %s",
                url, attempt, max_retries, exc,
            )
            if attempt < max_retries:
                time.sleep(compute_retry_delay(request_delay, attempt))
                continue

    logger.error("All %d attempts failed for %s", max_retries, url)
    return None
