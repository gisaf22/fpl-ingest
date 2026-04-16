"""Tests for transforms.py.

Covers cost conversion, position mappings, and all flatten functions:
flatten_live_element, flatten_live_elements, flatten_fixture_stats, flatten_event.
"""

import pytest

pytestmark = pytest.mark.unit

from fpl_ingest.models import GameweekModel
from fpl_ingest.transforms import (
    ELEMENT_TYPE_TO_POS,
    POS_TO_ELEMENT_TYPE,
    cost_to_millions,
    flatten_event,
    flatten_fixture_stats,
    flatten_live_element,
    flatten_live_elements,
)


# ---------------------------------------------------------------------------
# Transforms: cost_to_millions
# ---------------------------------------------------------------------------


class TestCostToMillions:
    def test_standard_conversion(self):
        assert cost_to_millions(100) == 10.0

    def test_fractional(self):
        assert cost_to_millions(45) == 4.5

    def test_premium(self):
        assert cost_to_millions(145) == 14.5

    def test_zero(self):
        assert cost_to_millions(0) == 0.0

    def test_budget(self):
        assert cost_to_millions(39) == 3.9


# ---------------------------------------------------------------------------
# Transforms: position mappings (bidirectional consistency)
# ---------------------------------------------------------------------------


class TestPositionMappings:
    def test_forward_mapping_complete(self):
        assert set(ELEMENT_TYPE_TO_POS.keys()) == {1, 2, 3, 4}
        assert set(ELEMENT_TYPE_TO_POS.values()) == {"GKP", "DEF", "MID", "FWD"}

    def test_reverse_mapping_complete(self):
        assert set(POS_TO_ELEMENT_TYPE.keys()) == {"GKP", "DEF", "MID", "FWD"}

    def test_roundtrip(self):
        for code, pos in ELEMENT_TYPE_TO_POS.items():
            assert POS_TO_ELEMENT_TYPE[pos] == code


# ---------------------------------------------------------------------------
# Transforms: flatten_live_element
# ---------------------------------------------------------------------------


class TestFlattenLiveElement:
    ELEMENT = {
        "id": 42,
        "stats": {
            "minutes": 90,
            "goals_scored": 2,
            "assists": 1,
            "clean_sheets": 0,
            "goals_conceded": 1,
            "expected_goals": "1.20",
            "expected_assists": "0.55",
            "total_points": 15,
            "bonus": 3,
            "starts": 1,
        },
    }

    def test_adds_element_id_and_round(self):
        flat = flatten_live_element(self.ELEMENT, gameweek=5)
        assert flat["element_id"] == 42
        assert flat["round"] == 5

    def test_preserves_all_stats(self):
        flat = flatten_live_element(self.ELEMENT, gameweek=5)
        assert flat["goals_scored"] == 2
        assert flat["assists"] == 1
        assert flat["expected_goals"] == "1.20"

    def test_validates_into_gameweek_model(self):
        flat = flatten_live_element(self.ELEMENT, gameweek=5)
        gw = GameweekModel.model_validate(flat)
        assert gw.element_id == 42
        assert gw.goals_scored == 2
        assert gw.expected_goals == pytest.approx(1.20)

    def test_missing_id_raises(self):
        with pytest.raises(ValueError, match="missing 'id'"):
            flatten_live_element({"stats": {}}, gameweek=1)


class TestFlattenLiveElements:
    def test_skips_elements_without_id(self):
        elements = [
            {"id": 1, "stats": {"minutes": 90}},
            {"stats": {"minutes": 45}},  # no id — should be skipped
            {"id": 3, "stats": {"minutes": 60}},
        ]
        result = flatten_live_elements(elements, gameweek=10)
        assert len(result) == 2
        assert result[0]["element_id"] == 1
        assert result[1]["element_id"] == 3

    def test_empty_list(self):
        assert flatten_live_elements([], gameweek=1) == []


# ---------------------------------------------------------------------------
# Transforms: flatten_fixture_stats
# ---------------------------------------------------------------------------


class TestFlattenFixtureStats:
    FIXTURE = {
        "id": 301,
        "stats": [
            {
                "identifier": "goals_scored",
                "h": [{"element": 10, "value": 2}],
                "a": [{"element": 20, "value": 1}],
            },
            {
                "identifier": "assists",
                "h": [{"element": 11, "value": 1}],
                "a": [],
            },
        ],
    }

    def test_returns_one_row_per_stat_entry(self):
        rows = flatten_fixture_stats(self.FIXTURE)
        # 1 home goals + 1 away goals + 1 home assists = 3
        assert len(rows) == 3

    def test_row_contains_all_expected_fields(self):
        rows = flatten_fixture_stats(self.FIXTURE)
        first = rows[0]
        assert first["fixture_id"] == 301
        assert first["identifier"] == "goals_scored"
        assert first["element"] == 10
        assert first["value"] == 2
        assert first["side"] == "h"

    def test_side_field_distinguishes_home_away(self):
        rows = flatten_fixture_stats(self.FIXTURE)
        sides = {r["side"] for r in rows}
        assert sides == {"h", "a"}

    def test_missing_id_returns_empty(self):
        assert flatten_fixture_stats({"stats": []}) == []

    def test_empty_stats_returns_empty(self):
        assert flatten_fixture_stats({"id": 1, "stats": []}) == []


# ---------------------------------------------------------------------------
# Transforms: flatten_event
# ---------------------------------------------------------------------------


class TestFlattenEvent:
    EVENT = {
        "id": 24,
        "name": "Gameweek 24",
        "deadline_time": "2026-02-15T10:00:00Z",
        "top_element_info": {"element": 316, "points": 20},
        "chip_plays": [{"chip_name": "bboost", "num_played": 50000}],
        "overrides": {},
        "some_other_field": "value",
    }

    def test_excludes_nested_keys(self):
        flat = flatten_event(self.EVENT)
        assert "chip_plays" not in flat
        assert "top_element_info" not in flat
        assert "overrides" not in flat

    def test_top_element_points_extracted(self):
        flat = flatten_event(self.EVENT)
        assert flat["top_element_points"] == 20

    def test_chip_plays_json_serialised(self):
        import json
        flat = flatten_event(self.EVENT)
        parsed = json.loads(flat["chip_plays_json"])
        assert parsed[0]["chip_name"] == "bboost"

    def test_none_top_element_info(self):
        event = dict(self.EVENT, top_element_info=None)
        flat = flatten_event(event)
        assert flat["top_element_points"] is None

    def test_empty_chip_plays_gives_none(self):
        event = dict(self.EVENT, chip_plays=[])
        flat = flatten_event(event)
        assert flat["chip_plays_json"] is None

    def test_other_fields_preserved(self):
        flat = flatten_event(self.EVENT)
        assert flat["some_other_field"] == "value"
        assert flat["id"] == 24
