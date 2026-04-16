"""Ingest pipeline stages for fpl-ingest."""

from fpl_ingest.pipeline.core import CoreData, ingest_core_data
from fpl_ingest.pipeline.db_setup import setup_store
from fpl_ingest.pipeline.fixtures import ingest_fixtures
from fpl_ingest.pipeline.gameweeks import ingest_gameweeks
from fpl_ingest.pipeline.history import ingest_player_histories
from fpl_ingest.pipeline.stage_result import StageResult

__all__ = [
    "CoreData",
    "setup_store",
    "ingest_core_data",
    "ingest_fixtures",
    "ingest_gameweeks",
    "ingest_player_histories",
    "StageResult",
]
