"""Contract tests — ensure FPL API responses validate against our Pydantic schemas.

These use `responses` to mock HTTP calls with realistic payloads.
If the FPL API renames a field (e.g. ``assists`` → ``fpl_assists``),
these tests will fail BEFORE bad data reaches the modelling stage.
"""

import responses
import pytest

pytestmark = pytest.mark.unit

from fpl_ingest import (
    FPLClient,
    PlayerModel,
    TeamModel,
    FixtureModel,
    GameweekModel,
    flatten_live_elements,
)
from fpl_ingest.client import ENDPOINTS

# ---------------------------------------------------------------------------
# Realistic fixtures — minimal but complete payloads that mirror the real API
# ---------------------------------------------------------------------------

BOOTSTRAP_PAYLOAD = {
    "events": [
        {
            "id": 1,
            "name": "Gameweek 1",
            "deadline_time": "2025-08-16T10:00:00Z",
            "finished": True,
            "is_current": False,
            "is_next": False,
        },
        {
            "id": 2,
            "name": "Gameweek 2",
            "deadline_time": "2025-08-23T10:00:00Z",
            "finished": False,
            "is_current": True,
            "is_next": False,
        },
        {
            "id": 3,
            "name": "Gameweek 3",
            "deadline_time": "2025-08-30T10:00:00Z",
            "finished": False,
            "is_current": False,
            "is_next": True,
        },
    ],
    "elements": [
        {
            "id": 1,
            "first_name": "Mohamed",
            "second_name": "Salah",
            "web_name": "Salah",
            "team": 11,
            "element_type": 3,
            "now_cost": 130,
            "status": "a",
            "chance_of_playing_next_round": 100,
            "total_points": 42,
            "form": "8.5",
            "points_per_game": "7.0",
            "selected_by_percent": "55.2",
        },
        {
            "id": 2,
            "first_name": "Erling",
            "second_name": "Haaland",
            "web_name": "Haaland",
            "team": 13,
            "element_type": 4,
            "now_cost": 145,
            "status": "a",
            "chance_of_playing_next_round": 100,
            "total_points": 38,
            "form": "7.2",
            "points_per_game": "6.3",
            "selected_by_percent": "62.1",
        },
    ],
    "teams": [
        {
            "id": 11,
            "name": "Liverpool",
            "short_name": "LIV",
            "strength": 5,
            "strength_overall_home": 1350,
            "strength_overall_away": 1340,
            "strength_attack_home": 1370,
            "strength_attack_away": 1360,
            "strength_defence_home": 1330,
            "strength_defence_away": 1320,
        },
    ],
}

FIXTURES_PAYLOAD = [
    {
        "id": 1,
        "event": 1,
        "team_h": 11,
        "team_a": 7,
        "team_h_score": 2,
        "team_a_score": 0,
        "kickoff_time": "2025-08-16T14:00:00Z",
        "finished": True,
    },
    {
        "id": 2,
        "event": 2,
        "team_h": 13,
        "team_a": 11,
        "team_h_score": None,
        "team_a_score": None,
        "kickoff_time": "2025-08-23T16:30:00Z",
        "finished": False,
    },
]

LIVE_GW_PAYLOAD = {
    "elements": [
        {
            "id": 1,
            "stats": {
                "minutes": 90,
                "goals_scored": 1,
                "assists": 1,
                "clean_sheets": 0,
                "goals_conceded": 2,
                "own_goals": 0,
                "penalties_saved": 0,
                "penalties_missed": 0,
                "yellow_cards": 0,
                "red_cards": 0,
                "saves": 0,
                "bonus": 3,
                "bps": 42,
                "total_points": 12,
                "influence": "55.2",
                "creativity": "48.7",
                "threat": "62.0",
                "ict_index": "16.6",
                "expected_goals": "0.85",
                "expected_assists": "0.42",
                "expected_goal_involvements": "1.27",
                "expected_goals_conceded": "1.50",
                "starts": 1,
            },
        },
        {
            "id": 2,
            "stats": {
                "minutes": 90,
                "goals_scored": 2,
                "assists": 0,
                "clean_sheets": 0,
                "goals_conceded": 1,
                "own_goals": 0,
                "penalties_saved": 0,
                "penalties_missed": 0,
                "yellow_cards": 1,
                "red_cards": 0,
                "saves": 0,
                "bonus": 3,
                "bps": 50,
                "total_points": 13,
                "influence": "72.0",
                "creativity": "12.3",
                "threat": "88.0",
                "ict_index": "17.2",
                "expected_goals": "1.62",
                "expected_assists": "0.10",
                "expected_goal_involvements": "1.72",
                "expected_goals_conceded": "0.90",
                "starts": 1,
            },
        },
    ]
}


# ---------------------------------------------------------------------------
# Contract: PlayerModel
# ---------------------------------------------------------------------------


class TestPlayerContract:
    """PlayerModel must accept every field the bootstrap API returns."""

    def test_validates_from_bootstrap(self):
        for raw in BOOTSTRAP_PAYLOAD["elements"]:
            player = PlayerModel.model_validate(raw)
            assert player.id > 0
            assert player.web_name is not None

    def test_required_fields_present(self):
        """The fields our downstream models depend on must exist."""
        raw = BOOTSTRAP_PAYLOAD["elements"][0]
        player = PlayerModel.model_validate(raw)
        assert player.element_type is not None
        assert player.now_cost is not None
        assert player.team is not None

    def test_position_property(self):
        player = PlayerModel.model_validate(BOOTSTRAP_PAYLOAD["elements"][0])
        assert player.position == "MID"

    def test_cost_millions_property(self):
        player = PlayerModel.model_validate(BOOTSTRAP_PAYLOAD["elements"][0])
        assert player.cost_millions == 13.0

    def test_rejects_missing_id(self):
        """A player without an id must fail validation."""
        bad = {"first_name": "Ghost", "second_name": "Player"}
        with pytest.raises(Exception):
            PlayerModel.model_validate(bad)


# ---------------------------------------------------------------------------
# Contract: TeamModel
# ---------------------------------------------------------------------------


class TestTeamContract:
    def test_validates_from_bootstrap(self):
        for raw in BOOTSTRAP_PAYLOAD["teams"]:
            team = TeamModel.model_validate(raw)
            assert team.id > 0

    def test_strength_fields_present(self):
        team = TeamModel.model_validate(BOOTSTRAP_PAYLOAD["teams"][0])
        assert team.strength_attack_home is not None
        assert team.strength_defence_away is not None


# ---------------------------------------------------------------------------
# Contract: FixtureModel
# ---------------------------------------------------------------------------


class TestFixtureContract:
    def test_validates_fixtures(self):
        for raw in FIXTURES_PAYLOAD:
            fixture = FixtureModel.model_validate(raw)
            assert fixture.id > 0

    def test_unfinished_fixture_has_null_scores(self):
        fixture = FixtureModel.model_validate(FIXTURES_PAYLOAD[1])
        assert fixture.team_h_score is None
        assert fixture.team_a_score is None

    def test_finished_fixture_has_scores(self):
        fixture = FixtureModel.model_validate(FIXTURES_PAYLOAD[0])
        assert fixture.team_h_score == 2
        assert fixture.team_a_score == 0


# ---------------------------------------------------------------------------
# Contract: GameweekModel (via live endpoint)
# ---------------------------------------------------------------------------


class TestGameweekContract:
    """GameweekModel must accept flattened live-endpoint data."""

    def test_validates_from_live(self):
        flat = flatten_live_elements(LIVE_GW_PAYLOAD["elements"], gw=2)
        for row in flat:
            gw = GameweekModel.model_validate(row)
            assert gw.element_id > 0
            assert gw.round == 2

    def test_critical_stat_fields(self):
        """Fields used by downstream Δ-calculations must be present."""
        flat = flatten_live_elements(LIVE_GW_PAYLOAD["elements"], gw=2)
        gw = GameweekModel.model_validate(flat[0])
        # These are the fields that feed assists-delta, xG, xA, etc.
        assert gw.minutes == 90
        assert gw.goals_scored == 1
        assert gw.assists == 1
        assert gw.expected_goals == pytest.approx(0.85)
        assert gw.expected_assists == pytest.approx(0.42)
        assert gw.expected_goal_involvements == pytest.approx(1.27)
        assert gw.total_points == 12
        assert gw.bonus == 3
        assert gw.starts == 1

    def test_rejects_missing_element_id(self):
        with pytest.raises(Exception):
            GameweekModel.model_validate({"round": 1, "minutes": 90})


# ---------------------------------------------------------------------------
# Contract: FPLClient (mocked HTTP)
# ---------------------------------------------------------------------------


class TestClientContract:
    """FPLClient must parse API responses and expose correct helpers."""

    @responses.activate
    def test_get_bootstrap(self):
        responses.add(
            responses.GET,
            ENDPOINTS["bootstrap"],
            json=BOOTSTRAP_PAYLOAD,
            status=200,
        )
        client = FPLClient(request_delay=0)
        data = client.get_bootstrap()
        assert "elements" in data
        assert "teams" in data
        assert "events" in data

    @responses.activate
    def test_get_current_gw(self):
        responses.add(
            responses.GET,
            ENDPOINTS["bootstrap"],
            json=BOOTSTRAP_PAYLOAD,
            status=200,
        )
        client = FPLClient(request_delay=0)
        assert client.get_current_gw() == 2

    @responses.activate
    def test_get_gw_deadline(self):
        responses.add(
            responses.GET,
            ENDPOINTS["bootstrap"],
            json=BOOTSTRAP_PAYLOAD,
            status=200,
        )
        client = FPLClient(request_delay=0)
        deadline = client.get_gw_deadline(2)
        assert deadline is not None
        assert deadline.year == 2025
        assert deadline.month == 8

    @responses.activate
    def test_get_fixtures(self):
        responses.add(
            responses.GET,
            ENDPOINTS["fixtures"],
            json=FIXTURES_PAYLOAD,
            status=200,
        )
        client = FPLClient(request_delay=0)
        fixtures = client.get_fixtures()
        assert len(fixtures) == 2

    @responses.activate
    def test_get_live_gw(self):
        url = ENDPOINTS["live"].format(gw=2)
        responses.add(responses.GET, url, json=LIVE_GW_PAYLOAD, status=200)
        client = FPLClient(request_delay=0)
        data = client.get_gw(2)
        assert "elements" in data
        assert len(data["elements"]) == 2

    @responses.activate
    def test_returns_none_on_failure(self):
        responses.add(
            responses.GET,
            ENDPOINTS["bootstrap"],
            status=500,
        )
        client = FPLClient(request_delay=0, max_retries=1)
        with pytest.raises(RuntimeError):
            client.get_bootstrap(force=True)
