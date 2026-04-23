"""Pydantic models for FPL API data.

Defines typed representations of FPL entities (players, teams, fixtures,
events, element types) and fact records (gameweek performance, fixture stats,
player history). Each model validates raw API JSON against a strict schema
and rejects unknown fields.

Convenience properties on domain models (position name, cost in millions)
are non-persisted read-only helpers for display and testing only. They do
not add derived or aggregated analytics fields.

This module does not perform I/O, transformation, pipeline logic, or SQL DDL
generation. Public schema enforcement is compiled from `schema.py`.
"""

from __future__ import annotations

from typing import Any, Dict, Optional, Type

from pydantic import BaseModel, ConfigDict, Field

from fpl_ingest.domain.transforms import ELEMENT_TYPE_TO_POS, cost_to_millions

PYTHON_TO_SQLITE: Dict[Type, str] = {
    int: "INTEGER",
    float: "REAL",
    str: "TEXT",
    bool: "INTEGER",
}

STRICT_MODEL_CONFIG = ConfigDict(extra="forbid")
ALIASED_STRICT_MODEL_CONFIG = ConfigDict(populate_by_name=True, extra="forbid")


# ---------------------------------------------------------------------------
# Domain models
# ---------------------------------------------------------------------------


class PlayerModel(BaseModel):
    """FPL player (element) — all fields from bootstrap-static elements."""

    model_config = STRICT_MODEL_CONFIG

    id: int
    first_name: str
    second_name: str
    web_name: str
    known_name: Optional[str] = None        # newer field, not always present
    team: int
    team_code: int
    element_type: int                        # 1=GKP, 2=DEF, 3=MID, 4=FWD
    now_cost: int                            # price in tenths of millions (e.g. 130 = £13.0m)
    price_change_percent: Optional[int] = None
    status: str                              # 'a'=available, 'i'=injured, 'd'=doubtful, etc.
    code: int
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
    total_points: int = 0
    event_points: int = 0                    # points in most recent gameweek
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
    bps: int = 0                             # bonus points system raw score
    starts: int = 0
    tackles: Optional[int] = None           # newer stat, not always present
    recoveries: Optional[int] = None
    clearances_blocks_interceptions: Optional[int] = None
    defensive_contribution: Optional[int] = None
    dreamteam_count: int = 0
    in_dreamteam: bool = False

    # ICT index components
    influence: float = 0.0
    creativity: float = 0.0
    threat: float = 0.0
    ict_index: float = 0.0

    # Expected stats
    expected_goals: float = 0.0
    expected_assists: float = 0.0
    expected_goal_involvements: float = 0.0
    expected_goals_conceded: float = 0.0

    # Per-90-minute stats — null when player has zero minutes
    clean_sheets_per_90: Optional[float] = None
    goals_conceded_per_90: Optional[float] = None
    saves_per_90: Optional[float] = None
    expected_goals_per_90: Optional[float] = None
    expected_assists_per_90: Optional[float] = None
    expected_goal_involvements_per_90: Optional[float] = None
    expected_goals_conceded_per_90: Optional[float] = None
    defensive_contribution_per_90: Optional[float] = None
    starts_per_90: Optional[float] = None

    # Form and value metrics
    form: float = 0.0
    points_per_game: float = 0.0
    selected_by_percent: float = 0.0
    value_form: float = 0.0
    value_season: float = 0.0
    ep_next: Optional[float] = None          # null early in season
    ep_this: Optional[float] = None

    # Relative rank fields — always populated for all players
    form_rank: int
    form_rank_type: int
    points_per_game_rank: int
    points_per_game_rank_type: int
    now_cost_rank: int
    now_cost_rank_type: int
    selected_rank: int
    selected_rank_type: int
    influence_rank: int
    influence_rank_type: int
    creativity_rank: int
    creativity_rank_type: int
    threat_rank: int
    threat_rank_type: int
    ict_index_rank: int
    ict_index_rank_type: int

    # Ownership and transfer data
    chance_of_playing_next_round: Optional[int] = None  # null for fully fit players
    chance_of_playing_this_round: Optional[int] = None
    transfers_in: int = 0
    transfers_out: int = 0
    transfers_in_event: int = 0
    transfers_out_event: int = 0
    cost_change_event: int = 0
    cost_change_event_fall: int = 0
    cost_change_start: int = 0
    cost_change_start_fall: int = 0

    # Set-piece order — null for most players
    penalties_order: Optional[int] = None
    penalties_text: Optional[str] = None
    corners_and_indirect_freekicks_order: Optional[int] = None
    corners_and_indirect_freekicks_text: Optional[str] = None
    direct_freekicks_order: Optional[int] = None
    direct_freekicks_text: Optional[str] = None

    # Injury/availability news — null when fit
    news: Optional[str] = None
    news_added: Optional[str] = None

    @classmethod
    def prepare(cls, raw: Dict[str, Any]) -> Dict[str, Any]:
        """Strip scout_* fields the API sends that are not part of this schema."""
        return {k: v for k, v in raw.items() if not k.startswith("scout_")}

    @property
    def position(self) -> str:
        """Position code string: GKP, DEF, MID, or FWD."""
        return ELEMENT_TYPE_TO_POS.get(self.element_type, "UNK")

    @property
    def cost_millions(self) -> float:
        """Cost in millions (e.g., now_cost=130 → 13.0)."""
        return cost_to_millions(self.now_cost)

    @property
    def display_name(self) -> str:
        """Best available display name for logs and UI."""
        return self.web_name or f"{self.first_name} {self.second_name}"


class TeamModel(BaseModel):
    """FPL team — all fields from bootstrap-static teams."""

    model_config = STRICT_MODEL_CONFIG

    id: int
    name: str
    short_name: str
    code: int
    pulse_id: Optional[int] = None
    strength: int                            # overall FPL strength rating (1–5)
    strength_overall_home: int
    strength_overall_away: int
    strength_attack_home: int
    strength_attack_away: int
    strength_defence_home: int
    strength_defence_away: int
    played: int = 0
    win: int = 0
    draw: int = 0
    loss: int = 0
    points: int = 0
    position: int                            # league table position
    form: Optional[float] = None            # null early in season
    team_division: Optional[str] = None
    unavailable: bool


class FixtureModel(BaseModel):
    """FPL fixture — all fields from the fixtures endpoint."""

    model_config = STRICT_MODEL_CONFIG

    id: int
    code: int
    event: Optional[int] = None             # null for unscheduled fixtures
    team_h: int                              # home team id
    team_a: int                              # away team id
    team_h_score: Optional[int] = None      # null until played
    team_a_score: Optional[int] = None
    team_h_difficulty: int
    team_a_difficulty: int
    kickoff_time: Optional[str] = None      # null for postponed fixtures
    minutes: int = 0
    started: Optional[bool] = None
    finished: bool
    finished_provisional: bool
    provisional_start_time: Optional[bool] = None
    pulse_id: Optional[int] = None

    @classmethod
    def prepare(cls, raw: Dict[str, Any]) -> Dict[str, Any]:
        """Strip stats field — processed separately as FixtureStatModel rows."""
        return {k: v for k, v in raw.items() if k != "stats"}


class FixtureStatModel(BaseModel):
    """Individual player stat entry within a fixture (goals, assists, etc.)."""

    model_config = STRICT_MODEL_CONFIG

    fixture_id: int
    identifier: str    # stat type name, e.g. 'goals_scored', 'assists'
    element: int       # player id
    value: int
    side: str          # 'h' (home) or 'a' (away)


class EventModel(BaseModel):
    """Gameweek (event) metadata from bootstrap-static."""

    model_config = STRICT_MODEL_CONFIG

    id: int
    name: str
    deadline_time: str
    deadline_time_epoch: int
    deadline_time_game_offset: int
    release_time: Optional[str] = None
    released: Optional[bool] = None
    average_entry_score: Optional[int] = None    # null for upcoming gameweeks
    highest_score: Optional[int] = None
    highest_scoring_entry: Optional[int] = None
    ranked_count: Optional[int] = None
    finished: bool
    data_checked: bool
    is_previous: bool
    is_current: bool
    is_next: bool
    can_enter: bool
    can_manage: bool
    cup_leagues_created: bool
    h2h_ko_matches_created: bool
    most_selected: Optional[int] = None          # null for upcoming gameweeks
    most_transferred_in: Optional[int] = None
    most_captained: Optional[int] = None
    most_vice_captained: Optional[int] = None
    top_element: Optional[int] = None
    top_element_points: Optional[int] = None
    transfers_made: int = 0
    chip_plays_json: Optional[str] = None        # JSON-serialised chip_plays list


class ElementTypeModel(BaseModel):
    """Position type definition from bootstrap-static (GKP, DEF, MID, FWD)."""

    model_config = STRICT_MODEL_CONFIG

    id: int
    singular_name: str
    singular_name_short: str
    plural_name: str
    plural_name_short: str
    squad_select: int
    squad_min_select: Optional[int]
    squad_max_select: Optional[int]
    squad_min_play: int
    squad_max_play: int
    ui_shirt_specific: bool
    element_count: int

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
    in_dreamteam: bool = False
    tackles: Optional[int] = None
    clearances_blocks_interceptions: Optional[int] = None
    recoveries: Optional[int] = None
    defensive_contribution: Optional[int] = None


class PlayerHistoryModel(BaseModel):
    """Per-fixture player history row from element-summary/{id}/history[].

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
    in_dreamteam: bool = False
    tackles: Optional[int] = None
    clearances_blocks_interceptions: Optional[int] = None
    recoveries: Optional[int] = None
    defensive_contribution: Optional[int] = None
    opponent_team: int
    was_home: bool
    kickoff_time: str
    team_h_score: Optional[int] = None      # null for unfinished fixtures
    team_a_score: Optional[int] = None
    value: int
    selected: int
    transfers_in: int
    transfers_out: int
    transfers_balance: int

    @classmethod
    def prepare(cls, raw: Dict[str, Any]) -> Dict[str, Any]:
        """Strip modified field the API sends that is not part of this schema."""
        return {k: v for k, v in raw.items() if k != "modified"}
