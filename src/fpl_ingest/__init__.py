"""fpl-ingest — FPL API ingestion library.

Public API for consumers (e.g. fpl-warehouse):

    Clients
        AsyncFPLClient          Async HTTP client (aiohttp) with pluggable rate limiting.
        FPLClient               Sync HTTP client (requests); kept for backwards compatibility.

    Rate limiters
        TokenBucketLimiter      Production rate limiter: token bucket + concurrency cap.
        NoopRateLimiter         No-op rate limiter for tests.

    Storage
        SQLiteStore             Generic SQLite storage for Pydantic models.

    Domain models
        PlayerModel             Player (element) data from bootstrap-static.
        TeamModel               Team data from bootstrap-static.
        FixtureModel            Fixture data from the fixtures endpoint.
        FixtureStatModel        Per-player stat entry within a fixture.
        GameweekModel           Live gameweek player performance.
        PlayerHistoryModel      Per-fixture player history from element-summary.
        EventModel              Gameweek metadata from bootstrap-static.
        ElementTypeModel        Position type definitions (GKP, DEF, MID, FWD).

    Transforms
        ELEMENT_TYPE_TO_POS     Map from element_type int to position code string.
        POS_TO_ELEMENT_TYPE     Reverse mapping from position code to element_type int.
        cost_to_millions        Convert now_cost (tenths) to float in millions.
        flatten_live_element    Flatten one live-endpoint element for GameweekModel.
        flatten_live_elements   Flatten a list of live-endpoint elements.
        flatten_fixture_stats   Flatten fixture stats into FixtureStatModel rows.
        flatten_event           Flatten a bootstrap event for EventModel.

    Errors
        FPLClientError          Raised when the client cannot reach the API.

    SQLite helpers
        pydantic_to_sqlite_column   Convert a Pydantic field to a column definition.
        schema_to_create_table      Generate CREATE TABLE SQL from a Pydantic schema.
"""

from fpl_ingest.async_client import AsyncFPLClient
from fpl_ingest.sync_client import FPLClient
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
from fpl_ingest.sync_http import FPLClientError
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
