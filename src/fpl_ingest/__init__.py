"""fpl-ingest — Reusable FPL API ingestion library.

Public API:
    FPLClient              — HTTP client with rate limiting and retry
    SQLiteStore            — Generic SQLite storage for Pydantic models
    PlayerModel            — Pydantic model for player data
    TeamModel              — Pydantic model for team data
    FixtureModel           — Pydantic model for fixture data
    FixtureStatModel       — Pydantic model for per-player fixture stats
    GameweekModel          — Pydantic model for gameweek performance
    EventModel             — Pydantic model for gameweek metadata
    ElementTypeModel       — Pydantic model for position types
    PhaseModel             — Pydantic model for season phases
    ExplainStatModel       — Pydantic model for GW points breakdown
    PlayerHistoryModel     — Pydantic model for past-season player history
    ELEMENT_TYPE_TO_POS, POS_TO_ELEMENT_TYPE — position mappings
    cost_to_millions       — convert now_cost to £m
    get_season_id          — calculate FPL season ID from a date
    pydantic_to_sqlite_column, schema_to_create_table — SQLite helpers
"""

from fpl_ingest.client import FPLClient
from fpl_ingest.models import (
    PlayerModel,
    TeamModel,
    FixtureModel,
    FixtureStatModel,
    GameweekModel,
    EventModel,
    ElementTypeModel,
    PhaseModel,
    ExplainStatModel,
    PlayerHistoryModel,
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
    flatten_fixture_stats,
    flatten_explain,
    flatten_event,
    flatten_player_history_past,
)

__all__ = [
    "FPLClient",
    "SQLiteStore",
    "PlayerModel",
    "TeamModel",
    "FixtureModel",
    "FixtureStatModel",
    "GameweekModel",
    "EventModel",
    "ElementTypeModel",
    "PhaseModel",
    "ExplainStatModel",
    "PlayerHistoryModel",
    "ELEMENT_TYPE_TO_POS",
    "POS_TO_ELEMENT_TYPE",
    "cost_to_millions",
    "get_season_id",
    "flatten_live_element",
    "flatten_live_elements",
    "flatten_fixture_stats",
    "flatten_explain",
    "flatten_event",
    "flatten_player_history_past",
    "pydantic_to_sqlite_column",
    "schema_to_create_table",
]
