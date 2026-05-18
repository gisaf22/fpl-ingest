"""Async and sync HTTP clients, rate limiting, and cache writing.

Exports the async client (``AsyncFPLClient``) used by extract stages,
the synchronous client (``FPLClient``) kept for backwards-compatible callers,
the token-bucket rate limiter and its ``NoopRateLimiter`` test double, and the
shared rate configuration constants. Also exports ``write_json_cache`` and
``cancel_pending_tasks`` shared by the entity fetch routines.
"""

from fpl_ingest.extract.http.client import AsyncFPLClient, cancel_pending_tasks, write_json_cache
from fpl_ingest.extract.http.rate_config import DEFAULT_RATE, MAX_RATE, normalize_rate
from fpl_ingest.extract.http.rate_limiter import NoopRateLimiter, RateLimiter, TokenBucketLimiter
from fpl_ingest.extract.http.sync_client import ENDPOINTS, FPLClient
from fpl_ingest.extract.http.sync_http import FPLClientError

__all__ = [
    "AsyncFPLClient",
    "cancel_pending_tasks",
    "write_json_cache",
    "DEFAULT_RATE",
    "MAX_RATE",
    "normalize_rate",
    "NoopRateLimiter",
    "RateLimiter",
    "TokenBucketLimiter",
    "ENDPOINTS",
    "FPLClient",
    "FPLClientError",
]
