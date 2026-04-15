"""Pydantic models for FPL API data.

Typed representations of FPL entities (players, teams, fixtures, events,
element types) and fact records (gameweek performance, fixture stats). Each
model validates raw API JSON against a strict schema and rejects unknown fields.

A small number of models expose convenience properties (e.g. position name,
cost in millions). These are non-persisted, read-only helpers for display and
testing only. They do not derive analytics fields and are not stored in SQLite.
Adding new derived or aggregated properties to these models is out of scope per
docs/governance.md.

Usage:
    from fpl_ingest import PlayerModel

    player = PlayerModel.model_validate(api_dict)
    print(player.position, player.cost_millions)
"""

from __future__ import annotations

from typing import Any, ClassVar, Dict, List, Optional, Type, get_args, get_origin

from pydantic import BaseModel, ConfigDict, Field, model_validator

from fpl_ingest.transforms import ELEMENT_TYPE_TO_POS, cost_to_millions

# ---------------------------------------------------------------------------
# SQLite helpers — kept with models since they operate on Pydantic schemas
# ---------------------------------------------------------------------------

PYTHON_TO_SQLITE: Dict[Type, str] = {
    int: "INTEGER",
    float: "REAL",
    str: "TEXT",
    bool: "INTEGER",
}

STRICT_MODEL_CONFIG = ConfigDict(extra="forbid")
ALIASED_STRICT_MODEL_CONFIG = ConfigDict(populate_by_name=True, extra="forbid")


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
    """FPL player data — all fields from bootstrap-static elements."""

    model_config = STRICT_MODEL_CONFIG

    id: int
    first_name: Optional[str] = None
    second_name: Optional[str] = None
    web_name: Optional[str] = None
    known_name: Optional[str] = None
    team: Optional[int] = None
    team_code: Optional[int] = None
    element_type: Optional[int] = None
    now_cost: Optional[int] = None
    price_change_percent: Optional[int] = None
    status: Optional[str] = None
    code: Optional[int] = None
    opta_code: Optional[str] = None
    photo: Optional[str] = None
    birth_date: Optional[str] = None
    team_join_date: Optional[str] = None
    region: Optional[int] = None
    squad_number: Optional[int] = None
    special: Optional[bool] = None
    removed: Optional[bool] = None
    can_transact: Optional[bool] = None
    can_select: Optional[bool] = None
    has_temporary_code: Optional[bool] = None

    # Season totals
    total_points: Optional[int] = None
    event_points: Optional[int] = None
    minutes: Optional[int] = None
    goals_scored: Optional[int] = None
    assists: Optional[int] = None
    clean_sheets: Optional[int] = None
    goals_conceded: Optional[int] = None
    own_goals: Optional[int] = None
    penalties_saved: Optional[int] = None
    penalties_missed: Optional[int] = None
    yellow_cards: Optional[int] = None
    red_cards: Optional[int] = None
    saves: Optional[int] = None
    bonus: Optional[int] = None
    bps: Optional[int] = None
    starts: Optional[int] = None
    tackles: Optional[int] = None
    recoveries: Optional[int] = None
    clearances_blocks_interceptions: Optional[int] = None
    defensive_contribution: Optional[int] = None
    dreamteam_count: Optional[int] = None
    in_dreamteam: Optional[bool] = None

    # ICT
    influence: Optional[float] = None
    creativity: Optional[float] = None
    threat: Optional[float] = None
    ict_index: Optional[float] = None

    # xG
    expected_goals: Optional[float] = None
    expected_assists: Optional[float] = None
    expected_goal_involvements: Optional[float] = None
    expected_goals_conceded: Optional[float] = None

    # Per-90 stats
    clean_sheets_per_90: Optional[float] = None
    goals_conceded_per_90: Optional[float] = None
    saves_per_90: Optional[float] = None
    expected_goals_per_90: Optional[float] = None
    expected_assists_per_90: Optional[float] = None
    expected_goal_involvements_per_90: Optional[float] = None
    expected_goals_conceded_per_90: Optional[float] = None
    defensive_contribution_per_90: Optional[float] = None
    starts_per_90: Optional[float] = None

    # Form and value
    form: Optional[float] = None
    points_per_game: Optional[float] = None
    selected_by_percent: Optional[float] = None
    value_form: Optional[float] = None
    value_season: Optional[float] = None
    ep_next: Optional[float] = None
    ep_this: Optional[float] = None

    # Ranks
    form_rank: Optional[int] = None
    form_rank_type: Optional[int] = None
    points_per_game_rank: Optional[int] = None
    points_per_game_rank_type: Optional[int] = None
    now_cost_rank: Optional[int] = None
    now_cost_rank_type: Optional[int] = None
    selected_rank: Optional[int] = None
    selected_rank_type: Optional[int] = None
    influence_rank: Optional[int] = None
    influence_rank_type: Optional[int] = None
    creativity_rank: Optional[int] = None
    creativity_rank_type: Optional[int] = None
    threat_rank: Optional[int] = None
    threat_rank_type: Optional[int] = None
    ict_index_rank: Optional[int] = None
    ict_index_rank_type: Optional[int] = None

    # Ownership and transfers
    chance_of_playing_next_round: Optional[int] = None
    chance_of_playing_this_round: Optional[int] = None
    transfers_in: Optional[int] = None
    transfers_out: Optional[int] = None
    transfers_in_event: Optional[int] = None
    transfers_out_event: Optional[int] = None
    cost_change_event: Optional[int] = None
    cost_change_event_fall: Optional[int] = None
    cost_change_start: Optional[int] = None
    cost_change_start_fall: Optional[int] = None

    # Set pieces
    penalties_order: Optional[int] = None
    penalties_text: Optional[str] = None
    corners_and_indirect_freekicks_order: Optional[int] = None
    corners_and_indirect_freekicks_text: Optional[str] = None
    direct_freekicks_order: Optional[int] = None
    direct_freekicks_text: Optional[str] = None

    # News
    news: Optional[str] = None
    news_added: Optional[str] = None

    @classmethod
    def prepare(cls, raw: Dict[str, Any]) -> Dict[str, Any]:
        """Strip scout_* fields the API sends that are not part of this schema."""
        return {k: v for k, v in raw.items() if not k.startswith("scout_")}

    @model_validator(mode="after")
    def validate_critical_fields(self) -> "PlayerModel":
        """Require identity fields that downstream logic depends on."""
        missing = [
            field_name for field_name in ("team", "element_type", "now_cost")
            if getattr(self, field_name) is None
        ]
        if missing:
            missing_str = ", ".join(missing)
            raise ValueError(f"PlayerModel missing critical fields: {missing_str}")
        return self

    @property
    def position(self) -> str:
        """Position string (GKP, DEF, MID, FWD)."""
        return ELEMENT_TYPE_TO_POS.get(self.element_type, "UNK")

    @property
    def cost_millions(self) -> float:
        """Cost in millions (e.g., 10.5)."""
        return cost_to_millions(self.now_cost or 0)

    @property
    def display_name(self) -> str:
        """Best available display name."""
        return self.web_name or f"{self.first_name} {self.second_name}"


class TeamModel(BaseModel):
    """FPL team data — all fields from bootstrap-static teams."""

    model_config = STRICT_MODEL_CONFIG

    id: int
    name: Optional[str] = None
    short_name: Optional[str] = None
    code: Optional[int] = None
    pulse_id: Optional[int] = None
    strength: Optional[int] = None
    strength_overall_home: Optional[int] = None
    strength_overall_away: Optional[int] = None
    strength_attack_home: Optional[int] = None
    strength_attack_away: Optional[int] = None
    strength_defence_home: Optional[int] = None
    strength_defence_away: Optional[int] = None
    played: Optional[int] = None
    win: Optional[int] = None
    draw: Optional[int] = None
    loss: Optional[int] = None
    points: Optional[int] = None
    position: Optional[int] = None
    form: Optional[float] = None
    team_division: Optional[str] = None
    unavailable: Optional[bool] = None

    @model_validator(mode="after")
    def validate_critical_fields(self) -> "TeamModel":
        """Require display fields needed to identify teams downstream."""
        missing = [
            field_name for field_name in ("name", "short_name")
            if not getattr(self, field_name)
        ]
        if missing:
            missing_str = ", ".join(missing)
            raise ValueError(f"TeamModel missing critical fields: {missing_str}")
        return self


class FixtureModel(BaseModel):
    """FPL fixture data — all fields from fixtures endpoint."""

    model_config = STRICT_MODEL_CONFIG

    id: int
    code: Optional[int] = None
    event: Optional[int] = None
    team_h: Optional[int] = None
    team_a: Optional[int] = None
    team_h_score: Optional[int] = None
    team_a_score: Optional[int] = None
    team_h_difficulty: Optional[int] = None
    team_a_difficulty: Optional[int] = None
    kickoff_time: Optional[str] = None
    minutes: Optional[int] = None
    started: Optional[bool] = None
    finished: Optional[bool] = None
    finished_provisional: Optional[bool] = None
    provisional_start_time: Optional[bool] = None
    pulse_id: Optional[int] = None

    @classmethod
    def prepare(cls, raw: Dict[str, Any]) -> Dict[str, Any]:
        """Strip stats field — processed separately as FixtureStatModel rows."""
        return {k: v for k, v in raw.items() if k != "stats"}

    @model_validator(mode="after")
    def validate_critical_fields(self) -> "FixtureModel":
        """Require team identity fields for fixture-level joins."""
        missing = [
            field_name for field_name in ("team_h", "team_a")
            if getattr(self, field_name) is None
        ]
        if missing:
            missing_str = ", ".join(missing)
            raise ValueError(f"FixtureModel missing critical fields: {missing_str}")
        return self


class FixtureStatModel(BaseModel):
    """Individual player stat within a fixture (goals, assists, etc.)."""

    model_config = STRICT_MODEL_CONFIG

    fixture_id: int
    identifier: str
    element: int
    value: int
    side: str  # 'h' or 'a'

    DEFAULT_UNIQUE: ClassVar[str] = "UNIQUE(fixture_id, identifier, element)"


class EventModel(BaseModel):
    """Gameweek (event) metadata from bootstrap-static."""

    model_config = STRICT_MODEL_CONFIG

    id: int
    name: Optional[str] = None
    deadline_time: Optional[str] = None
    deadline_time_epoch: Optional[int] = None
    deadline_time_game_offset: Optional[int] = None
    release_time: Optional[str] = None
    released: Optional[bool] = None
    average_entry_score: Optional[int] = None
    highest_score: Optional[int] = None
    highest_scoring_entry: Optional[int] = None
    ranked_count: Optional[int] = None
    finished: Optional[bool] = None
    data_checked: Optional[bool] = None
    is_previous: Optional[bool] = None
    is_current: Optional[bool] = None
    is_next: Optional[bool] = None
    can_enter: Optional[bool] = None
    can_manage: Optional[bool] = None
    cup_leagues_created: Optional[bool] = None
    h2h_ko_matches_created: Optional[bool] = None
    most_selected: Optional[int] = None
    most_transferred_in: Optional[int] = None
    most_captained: Optional[int] = None
    most_vice_captained: Optional[int] = None
    top_element: Optional[int] = None
    top_element_points: Optional[int] = None
    transfers_made: Optional[int] = None
    chip_plays_json: Optional[str] = None

    @model_validator(mode="after")
    def validate_critical_fields(self) -> "EventModel":
        """Require event state flags used to decide what to ingest."""
        missing = [
            field_name for field_name in ("name", "finished", "is_current", "is_next")
            if getattr(self, field_name) is None
        ]
        if missing:
            missing_str = ", ".join(missing)
            raise ValueError(f"EventModel missing critical fields: {missing_str}")
        return self


class ElementTypeModel(BaseModel):
    """Position type definition from bootstrap-static."""

    model_config = STRICT_MODEL_CONFIG

    id: int
    singular_name: Optional[str] = None
    singular_name_short: Optional[str] = None
    plural_name: Optional[str] = None
    plural_name_short: Optional[str] = None
    squad_select: Optional[int] = None
    squad_min_select: Optional[int] = None
    squad_max_select: Optional[int] = None
    squad_min_play: Optional[int] = None
    squad_max_play: Optional[int] = None
    ui_shirt_specific: Optional[bool] = None
    element_count: Optional[int] = None

    @classmethod
    def prepare(cls, raw: Dict[str, Any]) -> Dict[str, Any]:
        """Strip sub_positions_locked — list type with no SQLite column equivalent."""
        return {k: v for k, v in raw.items() if k != "sub_positions_locked"}


class GameweekModel(BaseModel):
    """Player performance for a single gameweek from the live endpoint."""

    model_config = ALIASED_STRICT_MODEL_CONFIG

    element_id: int = Field(alias="element")
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
    in_dreamteam: Optional[bool] = None
    tackles: Optional[int] = None
    clearances_blocks_interceptions: Optional[int] = None
    recoveries: Optional[int] = None
    defensive_contribution: Optional[int] = None

    fixture: Optional[int] = None
    opponent_team: Optional[int] = None
    was_home: Optional[bool] = None
    kickoff_time: Optional[str] = None
    team_h_score: Optional[int] = None
    team_a_score: Optional[int] = None
    value: Optional[int] = None
    selected: Optional[int] = None
    transfers_in: Optional[int] = None
    transfers_out: Optional[int] = None
    transfers_balance: Optional[int] = None

    # Default uniqueness: one row per player per gameweek
    DEFAULT_UNIQUE: ClassVar[str] = "UNIQUE(element_id, round)"


class PlayerHistoryModel(BaseModel):
    """Per-fixture player history row from ``element-summary/{id}/history[]``.

    Source is different from GameweekModel (element-summary vs live endpoint),
    different grain, and different uniqueness key. Fields overlap by coincidence
    of the upstream API shape, not by IS-A relationship.
    """

    model_config = ALIASED_STRICT_MODEL_CONFIG

    element_id: int = Field(alias="element")
    round: int
    fixture: int
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
    in_dreamteam: Optional[bool] = None
    tackles: Optional[int] = None
    clearances_blocks_interceptions: Optional[int] = None
    recoveries: Optional[int] = None
    defensive_contribution: Optional[int] = None
    opponent_team: Optional[int] = None
    was_home: Optional[bool] = None
    kickoff_time: Optional[str] = None
    team_h_score: Optional[int] = None
    team_a_score: Optional[int] = None
    value: Optional[int] = None
    selected: Optional[int] = None
    transfers_in: Optional[int] = None
    transfers_out: Optional[int] = None
    transfers_balance: Optional[int] = None

    @classmethod
    def prepare(cls, raw: Dict[str, Any]) -> Dict[str, Any]:
        """Strip modified field the API sends that is not part of this schema."""
        return {k: v for k, v in raw.items() if k != "modified"}

    # Default uniqueness: one row per player, gameweek, and fixture.
    DEFAULT_UNIQUE: ClassVar[str] = "UNIQUE(element_id, round, fixture)"
