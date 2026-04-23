"""Shared row-builder factories for test payloads.

Each factory returns a dict that matches the real FPL API shape for the
corresponding model type. Override any field with keyword arguments.
"""
from __future__ import annotations

from typing import Any


def player_row(**overrides: Any) -> dict:
    """Realistic bootstrap-static element row with all required fields."""
    base: dict = {
        "id": 1,
        "first_name": "Mohamed",
        "second_name": "Salah",
        "web_name": "Salah",
        "team": 11,
        "team_code": 14,
        "element_type": 3,
        "now_cost": 130,
        "status": "a",
        "code": 118748,
        "total_points": 42,
        "event_points": 12,
        "minutes": 810,
        "goals_scored": 5,
        "assists": 3,
        "clean_sheets": 2,
        "goals_conceded": 8,
        "own_goals": 0,
        "penalties_saved": 0,
        "penalties_missed": 0,
        "yellow_cards": 1,
        "red_cards": 0,
        "saves": 0,
        "bonus": 9,
        "bps": 120,
        "starts": 9,
        "dreamteam_count": 2,
        "in_dreamteam": False,
        "influence": "420.0",
        "creativity": "388.4",
        "threat": "620.0",
        "ict_index": "143.6",
        "expected_goals": "4.82",
        "expected_assists": "2.14",
        "expected_goal_involvements": "6.96",
        "expected_goals_conceded": "7.50",
        "form": "8.5",
        "points_per_game": "7.0",
        "selected_by_percent": "55.2",
        "value_form": "0.7",
        "value_season": "3.2",
        "form_rank": 1,
        "form_rank_type": 1,
        "points_per_game_rank": 1,
        "points_per_game_rank_type": 1,
        "now_cost_rank": 480,
        "now_cost_rank_type": 120,
        "selected_rank": 2,
        "selected_rank_type": 1,
        "influence_rank": 3,
        "influence_rank_type": 1,
        "creativity_rank": 5,
        "creativity_rank_type": 2,
        "threat_rank": 2,
        "threat_rank_type": 1,
        "ict_index_rank": 2,
        "ict_index_rank_type": 1,
        "transfers_in": 850000,
        "transfers_out": 320000,
        "transfers_in_event": 85000,
        "transfers_out_event": 32000,
        "cost_change_event": 1,
        "cost_change_event_fall": 0,
        "cost_change_start": 2,
        "cost_change_start_fall": 0,
        "chance_of_playing_next_round": None,
        "chance_of_playing_this_round": None,
    }
    base.update(overrides)
    return base


def team_row(**overrides: Any) -> dict:
    """Realistic bootstrap-static team row with all required fields."""
    base: dict = {
        "id": 11,
        "name": "Liverpool",
        "short_name": "LIV",
        "code": 14,
        "strength": 5,
        "strength_overall_home": 1350,
        "strength_overall_away": 1340,
        "strength_attack_home": 1370,
        "strength_attack_away": 1360,
        "strength_defence_home": 1330,
        "strength_defence_away": 1320,
        "played": 9,
        "win": 7,
        "draw": 1,
        "loss": 1,
        "points": 22,
        "position": 1,
        "unavailable": False,
    }
    base.update(overrides)
    return base


def event_row(**overrides: Any) -> dict:
    """Realistic bootstrap-static event row with all required fields."""
    base: dict = {
        "id": 1,
        "name": "Gameweek 1",
        "deadline_time": "2025-08-16T10:00:00Z",
        "deadline_time_epoch": 1755338400,
        "deadline_time_game_offset": 0,
        "finished": True,
        "data_checked": True,
        "is_previous": True,
        "is_current": False,
        "is_next": False,
        "can_enter": False,
        "can_manage": False,
        "cup_leagues_created": False,
        "h2h_ko_matches_created": False,
        "transfers_made": 4200000,
    }
    base.update(overrides)
    return base


def fixture_row(**overrides: Any) -> dict:
    """Realistic fixture row with all required fields."""
    base: dict = {
        "id": 1,
        "code": 2500001,
        "event": 1,
        "team_h": 11,
        "team_a": 7,
        "team_h_score": 2,
        "team_a_score": 0,
        "team_h_difficulty": 3,
        "team_a_difficulty": 4,
        "kickoff_time": "2025-08-16T14:00:00Z",
        "minutes": 90,
        "started": True,
        "finished": True,
        "finished_provisional": True,
    }
    base.update(overrides)
    return base


def history_row(**overrides: Any) -> dict:
    """Realistic element-summary history row with all required fields."""
    base: dict = {
        "element": 1,
        "fixture": 101,
        "opponent_team": 7,
        "total_points": 8,
        "was_home": True,
        "team_h_score": 2,
        "team_a_score": 1,
        "round": 1,
        "minutes": 90,
        "goals_scored": 1,
        "assists": 0,
        "clean_sheets": 0,
        "goals_conceded": 1,
        "own_goals": 0,
        "penalties_saved": 0,
        "penalties_missed": 0,
        "yellow_cards": 0,
        "red_cards": 0,
        "saves": 0,
        "bonus": 2,
        "bps": 28,
        "influence": "42.0",
        "creativity": "18.4",
        "threat": "55.0",
        "ict_index": "11.5",
        "starts": 1,
        "expected_goals": "0.62",
        "expected_assists": "0.14",
        "expected_goal_involvements": "0.76",
        "expected_goals_conceded": "1.10",
        "value": 130,
        "selected": 4200000,
        "transfers_in": 85000,
        "transfers_out": 32000,
        "transfers_balance": 53000,
        "kickoff_time": "2025-08-16T14:00:00Z",
        "in_dreamteam": False,
    }
    base.update(overrides)
    return base
