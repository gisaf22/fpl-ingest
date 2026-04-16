"""Database setup — registers all pipeline tables, indexes, and constraints.

This is the single place that decides which tables exist, what constraints
they carry, and which indexes are created. It does not fetch data or
perform any I/O beyond issuing DDL statements to the SQLiteStore.

To add a new table to the pipeline, register it here.
"""

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
    """Register all pipeline tables, constraints, and indexes against the store.

    Args:
        store: Active SQLiteStore. Must be called within a store.transaction()
            block so DDL and subsequent DML share the same connection.
    """
    store.register_table("players", PlayerModel)
    store.register_table("teams", TeamModel)
    store.register_table("fixtures", FixtureModel)
    store.register_table(
        "fixture_stats", FixtureStatModel,
        unique_constraint=FixtureStatModel.GRAIN_CONSTRAINT,
    )
    store.register_table(
        "gameweeks", GameweekModel,
        unique_constraint=GameweekModel.GRAIN_CONSTRAINT,
    )
    store.register_table(
        "player_histories", PlayerHistoryModel,
        unique_constraint=PlayerHistoryModel.GRAIN_CONSTRAINT,
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
