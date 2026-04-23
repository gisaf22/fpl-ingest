"""Contract tests — ensure FPL API responses validate against our Pydantic schemas.

These use `responses` to mock HTTP calls with realistic payloads.
If the FPL API renames a field (e.g. ``assists`` → ``fpl_assists``),
these tests will fail BEFORE bad data reaches the modelling stage.
"""

import responses
import pytest
from unittest.mock import patch

from fpl_ingest.transport.sync_client import FPLClient, ENDPOINTS
from fpl_ingest.domain.models import (
    ElementTypeModel, EventModel, FixtureModel, GameweekModel,
    PlayerHistoryModel, PlayerModel, TeamModel,
)
from fpl_ingest.domain.transforms import flatten_live_elements
from tests.factories import (
    event_row as _event_row,
    fixture_row as _fixture_row,
    history_row as _history_row,
    player_row as _player_row,
    team_row as _team_row,
)

pytestmark = pytest.mark.unit


def _element_type_row(**overrides) -> dict:
    base = {
        "id": 3,
        "singular_name": "Midfielder",
        "singular_name_short": "MID",
        "plural_name": "Midfielders",
        "plural_name_short": "MIDs",
        "squad_select": 5,
        "squad_min_select": 2,
        "squad_max_select": 5,
        "squad_min_play": 2,
        "squad_max_play": 5,
        "ui_shirt_specific": False,
        "element_count": 250,
    }
    base.update(overrides)
    return base


BOOTSTRAP_PAYLOAD = {
    "events": [
        _event_row(id=1, name="Gameweek 1", finished=True, is_previous=True, is_current=False, is_next=False),
        _event_row(id=2, name="Gameweek 2", deadline_time="2025-08-23T10:00:00Z",
                   deadline_time_epoch=1755943200, finished=False, data_checked=False,
                   is_previous=False, is_current=True, is_next=False, transfers_made=0),
        _event_row(id=3, name="Gameweek 3", deadline_time="2025-08-30T10:00:00Z",
                   deadline_time_epoch=1756548000, finished=False, data_checked=False,
                   is_previous=False, is_current=False, is_next=True, transfers_made=0),
    ],
    "elements": [
        _player_row(id=1, web_name="Salah", team=11, element_type=3, now_cost=130),
        _player_row(id=2, first_name="Erling", second_name="Haaland", web_name="Haaland",
                    team=13, team_code=43, element_type=4, now_cost=145, code=223094,
                    total_points=38, form="7.2", points_per_game="6.3",
                    selected_by_percent="62.1", form_rank=2, form_rank_type=1,
                    points_per_game_rank=2, points_per_game_rank_type=1,
                    influence_rank=4, influence_rank_type=2,
                    creativity_rank=80, creativity_rank_type=20,
                    threat_rank=1, threat_rank_type=1,
                    ict_index_rank=3, ict_index_rank_type=2),
    ],
    "teams": [
        _team_row(id=11, name="Liverpool", short_name="LIV", code=14),
    ],
}

FIXTURES_PAYLOAD = [
    _fixture_row(id=1, event=1, team_h=11, team_a=7, team_h_score=2, team_a_score=0,
                 finished=True, finished_provisional=True),
    _fixture_row(id=2, event=2, team_h=13, team_a=11, team_h_score=None, team_a_score=None,
                 kickoff_time="2025-08-23T16:30:00Z", started=False, finished=False,
                 finished_provisional=False),
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
                "in_dreamteam": True,
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
                "in_dreamteam": False,
            },
        },
    ]
}

PLAYER_HISTORY_PAYLOAD = {
    "history": [
        _history_row(round=2, fixture=101, opponent_team=7, was_home=True),
        _history_row(round=2, fixture=102, opponent_team=13, was_home=False, minutes=25, total_points=3),
    ]
}

ELEMENT_TYPES_PAYLOAD = [
    _element_type_row(id=1, singular_name="Goalkeeper", singular_name_short="GKP",
                      plural_name="Goalkeepers", plural_name_short="GKPs",
                      squad_select=2, squad_min_select=1, squad_max_select=1,
                      squad_min_play=1, squad_max_play=1, element_count=80),
    _element_type_row(id=3, singular_name="Midfielder", singular_name_short="MID",
                      plural_name="Midfielders", plural_name_short="MIDs",
                      squad_select=5, squad_min_select=2, squad_max_select=5,
                      squad_min_play=2, squad_max_play=5, element_count=250),
]


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

    def test_required_identity_fields_present(self):
        player = PlayerModel.model_validate(BOOTSTRAP_PAYLOAD["elements"][0])
        assert player.team == 11
        assert player.element_type == 3
        assert player.now_cost == 130
        assert player.team_code == 14
        assert player.code == 118748
        assert player.status == "a"
        assert player.first_name == "Mohamed"
        assert player.second_name == "Salah"
        assert player.web_name == "Salah"

    def test_required_rank_fields_present(self):
        player = PlayerModel.model_validate(BOOTSTRAP_PAYLOAD["elements"][0])
        assert player.form_rank == 1
        assert player.form_rank_type == 1
        assert player.ict_index_rank == 2
        assert player.ict_index_rank_type == 1

    def test_required_transfer_fields_present(self):
        player = PlayerModel.model_validate(BOOTSTRAP_PAYLOAD["elements"][0])
        assert player.transfers_in == 850000
        assert player.transfers_out == 320000
        assert player.transfers_in_event == 85000
        assert player.transfers_out_event == 32000

    def test_required_stat_fields_default_to_zero(self):
        player = PlayerModel.model_validate(_player_row(goals_scored=0, assists=0, dreamteam_count=0))
        assert player.goals_scored == 0
        assert player.assists == 0
        assert player.dreamteam_count == 0
        assert player.in_dreamteam is False

    def test_position_property(self):
        player = PlayerModel.model_validate(BOOTSTRAP_PAYLOAD["elements"][0])
        assert player.position == "MID"

    def test_cost_millions_property(self):
        player = PlayerModel.model_validate(BOOTSTRAP_PAYLOAD["elements"][0])
        assert player.cost_millions == 13.0

    def test_per_90_stats_null_when_no_minutes(self):
        player = PlayerModel.model_validate(_player_row(minutes=0))
        assert player.clean_sheets_per_90 is None
        assert player.expected_goals_per_90 is None
        assert player.starts_per_90 is None

    def test_chance_of_playing_null_for_fit_player(self):
        player = PlayerModel.model_validate(_player_row(
            chance_of_playing_next_round=None,
            chance_of_playing_this_round=None,
        ))
        assert player.chance_of_playing_next_round is None
        assert player.chance_of_playing_this_round is None

    def test_rejects_missing_team(self):
        bad = _player_row()
        del bad["team"]
        with pytest.raises(Exception):
            PlayerModel.model_validate(bad)

    def test_rejects_missing_element_type(self):
        bad = _player_row()
        del bad["element_type"]
        with pytest.raises(Exception):
            PlayerModel.model_validate(bad)

    def test_rejects_missing_rank_fields(self):
        bad = _player_row()
        del bad["form_rank"]
        with pytest.raises(Exception):
            PlayerModel.model_validate(bad)

    def test_rejects_missing_id(self):
        bad = {"first_name": "Ghost", "second_name": "Player"}
        with pytest.raises(Exception):
            PlayerModel.model_validate(bad)

    def test_rejects_unknown_fields(self):
        bad = dict(BOOTSTRAP_PAYLOAD["elements"][0], invented_metric=123)
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

    def test_required_identity_fields_present(self):
        team = TeamModel.model_validate(BOOTSTRAP_PAYLOAD["teams"][0])
        assert team.name == "Liverpool"
        assert team.short_name == "LIV"
        assert team.code == 14

    def test_required_strength_fields_present(self):
        team = TeamModel.model_validate(BOOTSTRAP_PAYLOAD["teams"][0])
        assert team.strength == 5
        assert team.strength_attack_home == 1370
        assert team.strength_defence_away == 1320

    def test_required_league_table_fields_present(self):
        team = TeamModel.model_validate(BOOTSTRAP_PAYLOAD["teams"][0])
        assert team.played == 9
        assert team.win == 7
        assert team.position == 1
        assert team.unavailable is False

    def test_form_null_early_in_season(self):
        team = TeamModel.model_validate(_team_row(form=None))
        assert team.form is None

    def test_rejects_missing_name(self):
        bad = _team_row()
        del bad["name"]
        with pytest.raises(Exception):
            TeamModel.model_validate(bad)

    def test_rejects_missing_short_name(self):
        bad = _team_row()
        del bad["short_name"]
        with pytest.raises(Exception):
            TeamModel.model_validate(bad)

    def test_rejects_missing_strength(self):
        bad = _team_row()
        del bad["strength"]
        with pytest.raises(Exception):
            TeamModel.model_validate(bad)

    def test_rejects_missing_position(self):
        bad = _team_row()
        del bad["position"]
        with pytest.raises(Exception):
            TeamModel.model_validate(bad)

    def test_rejects_missing_unavailable(self):
        bad = _team_row()
        del bad["unavailable"]
        with pytest.raises(Exception):
            TeamModel.model_validate(bad)


# ---------------------------------------------------------------------------
# Contract: FixtureModel
# ---------------------------------------------------------------------------


class TestFixtureContract:
    def test_validates_fixtures(self):
        for raw in FIXTURES_PAYLOAD:
            fixture = FixtureModel.model_validate(raw)
            assert fixture.id > 0

    def test_required_fields_present(self):
        fixture = FixtureModel.model_validate(FIXTURES_PAYLOAD[0])
        assert fixture.code == 2500001
        assert fixture.team_h == 11
        assert fixture.team_a == 7
        assert fixture.team_h_difficulty == 3
        assert fixture.team_a_difficulty == 4
        assert fixture.finished is True
        assert fixture.finished_provisional is True

    def test_unfinished_fixture_has_null_scores(self):
        fixture = FixtureModel.model_validate(FIXTURES_PAYLOAD[1])
        assert fixture.team_h_score is None
        assert fixture.team_a_score is None

    def test_finished_fixture_has_scores(self):
        fixture = FixtureModel.model_validate(FIXTURES_PAYLOAD[0])
        assert fixture.team_h_score == 2
        assert fixture.team_a_score == 0

    def test_rejects_missing_team_h(self):
        bad = _fixture_row()
        del bad["team_h"]
        with pytest.raises(Exception):
            FixtureModel.model_validate(bad)

    def test_rejects_missing_code(self):
        bad = _fixture_row()
        del bad["code"]
        with pytest.raises(Exception):
            FixtureModel.model_validate(bad)

    def test_rejects_missing_difficulty(self):
        bad = _fixture_row()
        del bad["team_h_difficulty"]
        with pytest.raises(Exception):
            FixtureModel.model_validate(bad)

    def test_rejects_missing_finished(self):
        bad = _fixture_row()
        del bad["finished"]
        with pytest.raises(Exception):
            FixtureModel.model_validate(bad)


# ---------------------------------------------------------------------------
# Contract: EventModel
# ---------------------------------------------------------------------------


class TestEventContract:
    def test_validates_from_bootstrap(self):
        for raw in BOOTSTRAP_PAYLOAD["events"]:
            event = EventModel.model_validate(raw)
            assert event.id > 0

    def test_required_deadline_fields_present(self):
        event = EventModel.model_validate(BOOTSTRAP_PAYLOAD["events"][0])
        assert event.name == "Gameweek 1"
        assert event.deadline_time == "2025-08-16T10:00:00Z"
        assert event.deadline_time_epoch == 1755338400
        assert event.deadline_time_game_offset == 0

    def test_required_state_flags_present(self):
        event = EventModel.model_validate(BOOTSTRAP_PAYLOAD["events"][0])
        assert event.finished is True
        assert event.data_checked is True
        assert event.is_previous is True
        assert event.is_current is False
        assert event.is_next is False
        assert event.can_enter is False
        assert event.can_manage is False
        assert event.cup_leagues_created is False
        assert event.h2h_ko_matches_created is False

    def test_aggregates_null_for_upcoming_gameweek(self):
        event = EventModel.model_validate(BOOTSTRAP_PAYLOAD["events"][1])
        assert event.average_entry_score is None
        assert event.highest_score is None
        assert event.most_captained is None
        assert event.top_element is None

    def test_rejects_missing_name(self):
        bad = _event_row()
        del bad["name"]
        with pytest.raises(Exception):
            EventModel.model_validate(bad)

    def test_rejects_missing_finished(self):
        bad = _event_row()
        del bad["finished"]
        with pytest.raises(Exception):
            EventModel.model_validate(bad)

    def test_rejects_missing_deadline_time_epoch(self):
        bad = _event_row()
        del bad["deadline_time_epoch"]
        with pytest.raises(Exception):
            EventModel.model_validate(bad)

    def test_rejects_missing_state_flags(self):
        for flag in ("is_current", "is_next", "can_enter", "cup_leagues_created"):
            bad = _event_row()
            del bad[flag]
            with pytest.raises(Exception):
                EventModel.model_validate(bad)


# ---------------------------------------------------------------------------
# Contract: ElementTypeModel
# ---------------------------------------------------------------------------


class TestElementTypeContract:
    def test_validates_element_types(self):
        for raw in ELEMENT_TYPES_PAYLOAD:
            et = ElementTypeModel.model_validate(raw)
            assert et.id > 0

    def test_required_name_fields_present(self):
        et = ElementTypeModel.model_validate(ELEMENT_TYPES_PAYLOAD[1])
        assert et.singular_name == "Midfielder"
        assert et.singular_name_short == "MID"
        assert et.plural_name == "Midfielders"
        assert et.plural_name_short == "MIDs"

    def test_required_squad_rule_fields_present(self):
        et = ElementTypeModel.model_validate(ELEMENT_TYPES_PAYLOAD[1])
        assert et.squad_select == 5
        assert et.squad_min_play == 2
        assert et.squad_max_play == 5
        assert et.element_count == 250
        assert et.ui_shirt_specific is False

    def test_rejects_missing_singular_name(self):
        bad = _element_type_row()
        del bad["singular_name"]
        with pytest.raises(Exception):
            ElementTypeModel.model_validate(bad)

    def test_rejects_missing_squad_fields(self):
        bad = _element_type_row()
        del bad["squad_select"]
        with pytest.raises(Exception):
            ElementTypeModel.model_validate(bad)

    def test_rejects_missing_element_count(self):
        bad = _element_type_row()
        del bad["element_count"]
        with pytest.raises(Exception):
            ElementTypeModel.model_validate(bad)


# ---------------------------------------------------------------------------
# Contract: GameweekModel (via live endpoint)
# ---------------------------------------------------------------------------


class TestGameweekContract:
    """GameweekModel must accept flattened live-endpoint data."""

    def test_validates_from_live(self):
        flat = flatten_live_elements(LIVE_GW_PAYLOAD["elements"], gameweek=2)
        for row in flat:
            gw = GameweekModel.model_validate(row)
            assert gw.element_id > 0
            assert gw.round == 2

    def test_critical_stat_fields(self):
        """Fields used by downstream Δ-calculations must be present."""
        flat = flatten_live_elements(LIVE_GW_PAYLOAD["elements"], gameweek=2)
        gw = GameweekModel.model_validate(flat[0])
        assert gw.minutes == 90
        assert gw.goals_scored == 1
        assert gw.assists == 1
        assert gw.expected_goals == pytest.approx(0.85)
        assert gw.expected_assists == pytest.approx(0.42)
        assert gw.expected_goal_involvements == pytest.approx(1.27)
        assert gw.total_points == 12
        assert gw.bonus == 3
        assert gw.starts == 1

    def test_in_dreamteam_present(self):
        flat = flatten_live_elements(LIVE_GW_PAYLOAD["elements"], gameweek=2)
        gw = GameweekModel.model_validate(flat[0])
        assert gw.in_dreamteam is True

    def test_rejects_missing_element_id(self):
        with pytest.raises(Exception):
            GameweekModel.model_validate({"round": 1, "minutes": 90})

    def test_rejects_unknown_fields(self):
        row = dict(flatten_live_elements(LIVE_GW_PAYLOAD["elements"], gameweek=2)[0], invented_metric=5)
        with pytest.raises(Exception):
            GameweekModel.model_validate(row)


# ---------------------------------------------------------------------------
# Contract: PlayerHistoryModel (via element-summary endpoint)
# ---------------------------------------------------------------------------


class TestPlayerHistoryContract:
    """PlayerHistoryModel must preserve per-fixture element-summary rows."""

    def test_validates_element_summary_history_rows(self):
        for raw in PLAYER_HISTORY_PAYLOAD["history"]:
            history = PlayerHistoryModel.model_validate(raw)
            assert history.element_id == 1
            assert history.fixture > 0

    def test_allows_multiple_fixtures_in_same_round(self):
        rows = [
            PlayerHistoryModel.model_validate(raw)
            for raw in PLAYER_HISTORY_PAYLOAD["history"]
        ]
        assert {(row.round, row.fixture) for row in rows} == {(2, 101), (2, 102)}

    def test_enrichment_fields_non_null(self):
        """Fields the element-summary API always provides must not be null after ingestion."""
        row = PlayerHistoryModel.model_validate(_history_row())
        assert row.opponent_team is not None
        assert row.was_home is not None
        assert row.kickoff_time is not None
        assert row.value is not None
        assert row.selected is not None
        assert row.transfers_in is not None
        assert row.transfers_out is not None
        assert row.transfers_balance is not None
        assert row.in_dreamteam is not None

    def test_enrichment_fields_correct_values(self):
        row = PlayerHistoryModel.model_validate(_history_row(
            opponent_team=7, was_home=True, kickoff_time="2025-08-16T14:00:00Z",
            value=130, selected=4200000, transfers_in=85000, transfers_out=32000,
            transfers_balance=53000,
        ))
        assert row.opponent_team == 7
        assert row.was_home is True
        assert row.kickoff_time == "2025-08-16T14:00:00Z"
        assert row.value == 130
        assert row.selected == 4200000
        assert row.transfers_in == 85000
        assert row.transfers_out == 32000
        assert row.transfers_balance == 53000

    def test_finished_fixture_has_scores(self):
        row = PlayerHistoryModel.model_validate(_history_row(team_h_score=2, team_a_score=1))
        assert row.team_h_score == 2
        assert row.team_a_score == 1

    def test_unfinished_fixture_scores_are_null(self):
        row = PlayerHistoryModel.model_validate(_history_row(team_h_score=None, team_a_score=None))
        assert row.team_h_score is None
        assert row.team_a_score is None

    def test_rejects_missing_opponent_team(self):
        bad = _history_row()
        del bad["opponent_team"]
        with pytest.raises(Exception):
            PlayerHistoryModel.model_validate(bad)

    def test_rejects_missing_value(self):
        bad = _history_row()
        del bad["value"]
        with pytest.raises(Exception):
            PlayerHistoryModel.model_validate(bad)

    def test_rejects_missing_kickoff_time(self):
        bad = _history_row()
        del bad["kickoff_time"]
        with pytest.raises(Exception):
            PlayerHistoryModel.model_validate(bad)

    def test_rejects_missing_fixture(self):
        with pytest.raises(Exception):
            PlayerHistoryModel.model_validate({"element": 1, "round": 2, "minutes": 90})


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

    @responses.activate
    def test_retry_after_fallback_handles_malformed_header(self):
        responses.add(
            responses.GET,
            ENDPOINTS["bootstrap"],
            status=429,
            headers={"Retry-After": "not-a-number"},
        )
        responses.add(
            responses.GET,
            ENDPOINTS["bootstrap"],
            json=BOOTSTRAP_PAYLOAD,
            status=200,
        )

        client = FPLClient(request_delay=0, max_retries=2)
        with (
            patch("fpl_ingest.transport.sync_http.time.sleep"),
            patch("fpl_ingest.transport.sync_http.random.uniform", return_value=0),
        ):
            data = client.get_bootstrap(force=True)

        assert data["events"][0]["id"] == 1
        assert len(responses.calls) == 2

    @responses.activate
    def test_does_not_retry_non_retryable_404(self):
        responses.add(
            responses.GET,
            ENDPOINTS["fixtures"],
            status=404,
        )

        client = FPLClient(request_delay=0, max_retries=3)
        with (
            patch("fpl_ingest.transport.sync_http.time.sleep"),
            patch("fpl_ingest.transport.sync_http.random.uniform", return_value=0),
        ):
            data = client.get_fixtures()

        assert data is None
        assert len(responses.calls) == 1
