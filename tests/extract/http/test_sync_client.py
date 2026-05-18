"""Tests for FPLClient (synchronous HTTP client) contract.

Verifies that FPLClient correctly parses API responses and exposes the
expected helper methods (get_bootstrap, get_current_gw, get_gw_deadline,
get_fixtures, get_gw, get_player_history).
"""

from __future__ import annotations

from unittest.mock import patch

import pytest
import responses

from fpl_ingest.extract.http.sync_client import ENDPOINTS, FPLClient
from tests.factories import (
    event_row as _event_row,
    fixture_row as _fixture_row,
    history_row as _history_row,
    player_row as _player_row,
    team_row as _team_row,
)

pytestmark = pytest.mark.unit

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
                "minutes": 90, "goals_scored": 1, "assists": 1, "clean_sheets": 0,
                "goals_conceded": 2, "own_goals": 0, "penalties_saved": 0,
                "penalties_missed": 0, "yellow_cards": 0, "red_cards": 0, "saves": 0,
                "bonus": 3, "bps": 42, "total_points": 12,
                "influence": "55.2", "creativity": "48.7", "threat": "62.0",
                "ict_index": "16.6", "expected_goals": "0.85", "expected_assists": "0.42",
                "expected_goal_involvements": "1.27", "expected_goals_conceded": "1.50",
                "starts": 1, "in_dreamteam": True,
            },
        },
    ]
}


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
        assert len(data["elements"]) == 1

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
            patch("fpl_ingest.extract.http.sync_http.time.sleep"),
            patch("fpl_ingest.extract.http.sync_http.random.uniform", return_value=0),
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
            patch("fpl_ingest.extract.http.sync_http.time.sleep"),
            patch("fpl_ingest.extract.http.sync_http.random.uniform", return_value=0),
        ):
            data = client.get_fixtures()

        assert data is None
        assert len(responses.calls) == 1
