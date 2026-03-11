"""fpl-ingest — Reusable FPL API ingestion library.

Public API:
    FPLClient         — HTTP client with rate limiting and retry
    SQLiteStore       — Generic SQLite storage for Pydantic models
    PlayerModel       — Pydantic model for player data
    TeamModel         — Pydantic model for team data
    FixtureModel      — Pydantic model for fixture data
    GameweekModel     — Pydantic model for gameweek performance
    ELEMENT_TYPE_TO_POS, POS_TO_ELEMENT_TYPE — position mappings
    cost_to_millions  — convert now_cost to £m
    get_season_id     — calculate FPL season ID from a date
    pydantic_to_sqlite_column, schema_to_create_table — SQLite helpers
"""

from fpl_ingest.client import FPLClient
from fpl_ingest.models import (
    PlayerModel,
    TeamModel,
    FixtureModel,
    GameweekModel,
    pydantic_to_sqlite_column,
    schema_to_create_table,
)
from fpl_ingest.store import SQLiteStore
from fpl_ingest.transforms import (
    ELEMENT_TYPE_TO_POS,
    POS_TO_ELEMENT_TYPE,
    cost_to_millions,
    get_season_id,
    flatten_live_element,
    flatten_live_elements,
)

__all__ = [
    "FPLClient",
    "SQLiteStore",
    "PlayerModel",
    "TeamModel",
    "FixtureModel",
    "GameweekModel",
    "ELEMENT_TYPE_TO_POS",
    "POS_TO_ELEMENT_TYPE",
    "cost_to_millions",
    "get_season_id",
    "flatten_live_element",
    "flatten_live_elements",
    "pydantic_to_sqlite_column",
    "schema_to_create_table",
]
