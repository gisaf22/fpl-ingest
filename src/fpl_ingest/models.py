"""Pydantic models for FPL API data.

Typed representations of FPL entities (players, teams, fixtures, gameweeks).
Each model validates raw API JSON and exposes computed properties for
common transformations (position names, cost in millions).

Usage:
    from fpl_ingest import PlayerModel

    player = PlayerModel.model_validate(api_dict)
    print(player.position, player.cost_millions)
"""

from __future__ import annotations

from typing import Any, ClassVar, Dict, List, Optional, Type, get_args, get_origin

from pydantic import BaseModel, ConfigDict, Field

from fpl_ingest.transforms import ELEMENT_TYPE_TO_POS

# ---------------------------------------------------------------------------
# SQLite helpers — kept with models since they operate on Pydantic schemas
# ---------------------------------------------------------------------------

PYTHON_TO_SQLITE: Dict[Type, str] = {
    int: "INTEGER",
    float: "REAL",
    str: "TEXT",
    bool: "INTEGER",
}


def pydantic_to_sqlite_column(field_name: str, field_info: Any) -> str:
    """Convert a Pydantic field to a SQLite column definition."""
    annotation = field_info.annotation

    origin = get_origin(annotation)
    if origin is type(None) or annotation is type(None):
        return f"{field_name} TEXT"

    if hasattr(annotation, "__origin__"):
        args = get_args(annotation)
        if type(None) in args:
            annotation = next(a for a in args if a is not type(None))

    sqlite_type = PYTHON_TO_SQLITE.get(annotation, "TEXT")

    if field_name == "id":
        return f"{field_name} {sqlite_type} PRIMARY KEY"

    return f"{field_name} {sqlite_type}"


def schema_to_create_table(
    table_name: str,
    schema: Type[BaseModel],
    extra_columns: Optional[List[str]] = None,
    unique_constraint: Optional[str] = None,
) -> str:
    """Generate CREATE TABLE SQL from a Pydantic schema."""
    columns = []

    for field_name, field_info in schema.model_fields.items():
        columns.append(pydantic_to_sqlite_column(field_name, field_info))

    if extra_columns:
        columns.extend(extra_columns)

    if unique_constraint:
        columns.append(unique_constraint)

    columns_sql = ",\n                ".join(columns)
    return f"""
            CREATE TABLE IF NOT EXISTS {table_name} (
                {columns_sql}
            )
        """


# ---------------------------------------------------------------------------
# Domain models
# ---------------------------------------------------------------------------


class PlayerModel(BaseModel):
    """FPL player data."""

    id: int
    first_name: Optional[str] = None
    second_name: Optional[str] = None
    web_name: Optional[str] = None
    team: Optional[int] = None
    element_type: Optional[int] = None
    now_cost: Optional[int] = None
    status: Optional[str] = None
    chance_of_playing_next_round: Optional[int] = None
    total_points: Optional[int] = None
    form: Optional[float] = None
    points_per_game: Optional[float] = None
    selected_by_percent: Optional[float] = None

    @property
    def position(self) -> str:
        """Position string (GKP, DEF, MID, FWD)."""
        return ELEMENT_TYPE_TO_POS.get(self.element_type, "UNK")

    @property
    def cost_millions(self) -> float:
        """Cost in millions (e.g., 10.5)."""
        from fpl_ingest.transforms import cost_to_millions

        return cost_to_millions(self.now_cost or 0)

    @property
    def display_name(self) -> str:
        """Best available display name."""
        return self.web_name or f"{self.first_name} {self.second_name}"


class TeamModel(BaseModel):
    """FPL team data."""

    id: int
    name: Optional[str] = None
    short_name: Optional[str] = None
    strength: Optional[int] = None
    strength_overall_home: Optional[int] = None
    strength_overall_away: Optional[int] = None
    strength_attack_home: Optional[int] = None
    strength_attack_away: Optional[int] = None
    strength_defence_home: Optional[int] = None
    strength_defence_away: Optional[int] = None


class FixtureModel(BaseModel):
    """FPL fixture data."""

    model_config = ConfigDict(populate_by_name=True)

    id: int
    event: Optional[int] = Field(None, alias="gameweek")
    team_h: Optional[int] = None
    team_a: Optional[int] = None
    team_h_score: Optional[int] = None
    team_a_score: Optional[int] = None
    kickoff_time: Optional[str] = None
    finished: Optional[bool] = None


class GameweekModel(BaseModel):
    """Player performance for a single gameweek."""

    element_id: int
    round: int
    minutes: int = 0
    goals_scored: int = 0
    assists: int = 0
    clean_sheets: int = 0
    goals_conceded: int = 0
    own_goals: int = 0
    penalties_saved: int = 0
    penalties_missed: int = 0
    yellow_cards: int = 0
    red_cards: int = 0
    saves: int = 0
    bonus: int = 0
    bps: int = 0
    total_points: int = 0
    influence: float = 0.0
    creativity: float = 0.0
    threat: float = 0.0
    ict_index: float = 0.0
    expected_goals: float = 0.0
    expected_assists: float = 0.0
    expected_goal_involvements: float = 0.0
    expected_goals_conceded: float = 0.0
    starts: int = 0

    # Default uniqueness: one row per player per gameweek
    DEFAULT_UNIQUE: ClassVar[str] = "UNIQUE(element_id, round)"
