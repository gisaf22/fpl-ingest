"""fpl-ingest — Reusable FPL API ingestion library.

Public API:
    AsyncFPLClient         — Async HTTP client (aiohttp) with pluggable rate limiting
    FPLClient              — Sync HTTP client (requests); kept for backwards compatibility
    TokenBucketLimiter     — Production rate limiter: token bucket + concurrency cap
    NoopRateLimiter        — No-op rate limiter for tests
    SQLiteStore            — Generic SQLite storage for Pydantic models
    PlayerModel            — Pydantic model for player data
    TeamModel              — Pydantic model for team data
    FixtureModel           — Pydantic model for fixture data
    FixtureStatModel       — Pydantic model for per-player fixture stats
    GameweekModel          — Pydantic model for live gameweek performance
    PlayerHistoryModel     — Pydantic model for per-fixture player history
    EventModel             — Pydantic model for gameweek metadata
    ElementTypeModel       — Pydantic model for position types
ELEMENT_TYPE_TO_POS, POS_TO_ELEMENT_TYPE — position mappings
    cost_to_millions       — convert now_cost to £m
    pydantic_to_sqlite_column, schema_to_create_table — SQLite helpers
"""

from fpl_ingest.async_client import AsyncFPLClient
from fpl_ingest.client import FPLClient
from fpl_ingest.rate_limiter import NoopRateLimiter, RateLimiter, TokenBucketLimiter
from fpl_ingest.models import (
    PlayerModel,
    TeamModel,
    FixtureModel,
    FixtureStatModel,
    GameweekModel,
    PlayerHistoryModel,
    EventModel,
    ElementTypeModel,
    pydantic_to_sqlite_column,
    schema_to_create_table,
)
from fpl_ingest.store import SQLiteStore
from fpl_ingest.transport import FPLClientError
from fpl_ingest.transforms import (
    ELEMENT_TYPE_TO_POS,
    POS_TO_ELEMENT_TYPE,
    cost_to_millions,
    flatten_live_element,
    flatten_live_elements,
    flatten_fixture_stats,
    flatten_event,
)

__all__ = [
    "AsyncFPLClient",
    "FPLClient",
    "RateLimiter",
    "TokenBucketLimiter",
    "NoopRateLimiter",
    "FPLClientError",
    "SQLiteStore",
    "PlayerModel",
    "TeamModel",
    "FixtureModel",
    "FixtureStatModel",
    "GameweekModel",
    "PlayerHistoryModel",
    "EventModel",
    "ElementTypeModel",
    "ELEMENT_TYPE_TO_POS",
    "POS_TO_ELEMENT_TYPE",
    "cost_to_millions",
    "flatten_live_element",
    "flatten_live_elements",
    "flatten_fixture_stats",
    "flatten_event",
    "pydantic_to_sqlite_column",
    "schema_to_create_table",
]
