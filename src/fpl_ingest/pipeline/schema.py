"""Pipeline schema registration — owns the full SQLite table layout."""

from __future__ import annotations

from fpl_ingest.models import (
    ElementTypeModel,
    EventModel,
    FixtureModel,
    FixtureStatModel,
    GameweekModel,
    PlayerHistoryModel,
    PlayerModel,
    TeamModel,
)
from fpl_ingest.store import SQLiteStore


def setup_store(store: SQLiteStore) -> None:
    """Register all pipeline tables and indexes."""
    store.register_table("players", PlayerModel)
    store.register_table("teams", TeamModel)
    store.register_table("fixtures", FixtureModel)
    store.register_table(
        "fixture_stats", FixtureStatModel,
        unique_constraint=FixtureStatModel.DEFAULT_UNIQUE,
    )
    store.register_table(
        "gameweeks", GameweekModel,
        unique_constraint=GameweekModel.DEFAULT_UNIQUE,
    )
    store.register_table(
        "player_histories", PlayerHistoryModel,
        unique_constraint=PlayerHistoryModel.DEFAULT_UNIQUE,
    )
    store.register_table("events", EventModel)
    store.register_table("element_types", ElementTypeModel)

    store.create_index("gameweeks", ["round"])
    store.create_index("player_histories", ["round"])
    store.create_index("player_histories", ["element_id"])
    store.create_index("fixtures", ["event"])
    store.create_index("fixture_stats", ["element"])
    store.setup_runs_table()
    store.setup_metadata_table()
