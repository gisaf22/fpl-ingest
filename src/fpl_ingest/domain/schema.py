"""Public schema contract source of truth for downstream SQLite consumers."""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Type

from pydantic import BaseModel

from fpl_ingest.domain.models import (
    ElementTypeModel,
    EventModel,
    FixtureModel,
    FixtureStatModel,
    GameweekModel,
    PlayerHistoryModel,
    PlayerModel,
    TeamModel,
)

SCHEMA_VERSION = "1.0.0"
CONTRACT_ARTIFACT_PATH = Path(__file__).resolve().parents[2] / "artifacts" / "contract" / "schema_contract.json"
SYSTEM_COLUMNS: tuple[str, ...] = ("ingested_at",)


@dataclass(frozen=True)
class ColumnContract:
    name: str
    sqlite_type: str
    nullable: bool
    primary_key: bool = False
    source: str = "model"


@dataclass(frozen=True)
class TableContract:
    name: str
    model: Type[BaseModel]
    description: str
    grain: str
    field_notes: dict[str, str] = field(default_factory=dict)
    unique_key: tuple[str, ...] = ()
    indexes: tuple[tuple[str, ...], ...] = ()
    system_columns: tuple[str, ...] = SYSTEM_COLUMNS


@dataclass(frozen=True)
class TypeMismatch:
    column: str
    expected: str
    actual: str


@dataclass(frozen=True)
class ConstraintMismatch:
    name: str
    expected: str
    actual: str


@dataclass
class ValidationResult:
    status: str
    schema_version: str
    db_path: str
    checked_at: str
    missing_tables: list[str] = field(default_factory=list)
    missing_columns: dict[str, list[str]] = field(default_factory=dict)
    extra_columns: dict[str, list[str]] = field(default_factory=dict)
    type_mismatches: dict[str, list[TypeMismatch]] = field(default_factory=dict)
    nullability_mismatches: dict[str, list[ConstraintMismatch]] = field(default_factory=dict)
    primary_key_mismatches: dict[str, ConstraintMismatch] = field(default_factory=dict)
    unique_constraint_mismatches: dict[str, ConstraintMismatch] = field(default_factory=dict)
    index_mismatches: dict[str, ConstraintMismatch] = field(default_factory=dict)

    @property
    def is_valid(self) -> bool:
        return self.status != "invalid"


PUBLIC_TABLES: tuple[TableContract, ...] = (
    TableContract(
        name="players",
        model=PlayerModel,
        description="Current-season player snapshot; one row per player.",
        grain="one row per player",
        field_notes={
            "element_type": "1=GK, 2=DEF, 3=MID, 4=FWD.",
            "selected_by_percent": "Snapshot ownership percentage, not historical ownership.",
            "now_cost": "Current price multiplied by 10. Example: 65 = £6.5m.",
            "status": "a=available, i=injured, d=doubtful, s=suspended, n=unavailable.",
        },
    ),
    TableContract(
        name="teams",
        model=TeamModel,
        description="Premier League team metadata and current-season standings.",
        grain="one row per team",
    ),
    TableContract(
        name="fixtures",
        model=FixtureModel,
        description="All fixtures for the season with scores and difficulty ratings.",
        grain="one row per fixture",
        field_notes={
            "event": "Gameweek number.",
            "team_h_difficulty": "FPL difficulty rating 1-5 from the home team's perspective.",
            "team_a_difficulty": "FPL difficulty rating 1-5 from the away team's perspective.",
        },
        indexes=(("event",),),
    ),
    TableContract(
        name="fixture_stats",
        model=FixtureStatModel,
        description="Individual stat contributions per player per fixture.",
        grain="one row per (fixture_id, identifier, element)",
        field_notes={
            "identifier": "Stat type key such as goals_scored or assists.",
            "side": "h for home team contribution, a for away team contribution.",
            "value": "Count of the stat for the player in that fixture.",
        },
        unique_key=("fixture_id", "identifier", "element"),
        indexes=(("element",),),
    ),
    TableContract(
        name="gameweeks",
        model=GameweekModel,
        description="Per-player per-round live aggregates from the event live endpoint.",
        grain="one row per (element_id, round)",
        unique_key=("element_id", "round"),
        indexes=(("round",),),
    ),
    TableContract(
        name="player_histories",
        model=PlayerHistoryModel,
        description="Per-player per-fixture history from the element-summary endpoint.",
        grain="one row per (element_id, round, fixture)",
        field_notes={
            "value": "Price multiplied by 10. Example: 65 = £6.5m.",
            "selected": "Absolute owner count at that moment.",
            "fixture": "Fixture id for the specific match in that round.",
        },
        unique_key=("element_id", "round", "fixture"),
        indexes=(("round",), ("element_id",)),
    ),
    TableContract(
        name="events",
        model=EventModel,
        description="FPL gameweek metadata including deadlines and top scorer.",
        grain="one row per event/gameweek",
        field_notes={
            "chip_plays_json": "JSON array of {chip_name, num_played} entries for that gameweek.",
            "is_current": "1 for the currently active gameweek.",
            "top_element": "Player id of the highest-scoring player that gameweek.",
        },
    ),
    TableContract(
        name="element_types",
        model=ElementTypeModel,
        description="Position definitions and squad constraints.",
        grain="one row per element type",
        field_notes={
            "id": "1=GK, 2=DEF, 3=MID, 4=FWD.",
        },
    ),
)

PUBLIC_TABLES_BY_NAME = {table.name: table for table in PUBLIC_TABLES}

def contract_tables() -> dict[str, dict[str, Any]]:
    """Return the canonical public schema contract as a stable dict."""
    from fpl_ingest.contract import compile_contract

    return compile_contract().schema_contract["tables"]


def export_contract() -> dict[str, Any]:
    """Return the exported public schema contract payload."""
    from fpl_ingest.contract import compile_contract

    return compile_contract().schema_contract


def write_contract_artifact(out_path: Path | None = None) -> Path:
    """Write the public contract artifact to disk and return the output path."""
    from fpl_ingest.contract.compiler import write_contract_artifact as _write_contract_artifact

    return _write_contract_artifact(export_contract(), out_path or CONTRACT_ARTIFACT_PATH)


def validate_contract(db_path: str | Path | None = None) -> ValidationResult:
    """Validate a live database against the canonical public schema contract."""
    from fpl_ingest.contract import compile_contract
    from fpl_ingest.contract.validation_rules import validate_contract_db

    return validate_contract_db(compile_contract(), db_path)
