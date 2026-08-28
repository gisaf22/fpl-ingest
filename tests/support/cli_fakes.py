"""Shared CLI test doubles and payload constants.

Extracted verbatim from the pre-migration ``tests/test_cli.py`` so the unit
and integration halves of those tests can share one set of fakes instead of
duplicating them.
"""

from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import AsyncMock, patch

from fpl_ingest.cli import main
from tests.factories import player_row, team_row


MINIMAL_BOOTSTRAP = {
    "events": [],
    "elements": [
        player_row(id=1, team=11, element_type=3, now_cost=130),
        player_row(id=2, first_name="Erling", second_name="Haaland", web_name="Haaland",
                   team=13, team_code=43, element_type=4, now_cost=145, code=223094,
                   form_rank=2, form_rank_type=1, points_per_game_rank=2,
                   points_per_game_rank_type=1, influence_rank=4, influence_rank_type=2,
                   creativity_rank=80, creativity_rank_type=20,
                   threat_rank=1, threat_rank_type=1,
                   ict_index_rank=3, ict_index_rank_type=2),
    ],
    "teams": [
        team_row(id=11, name="Liverpool", short_name="LIV", code=14),
        team_row(id=13, name="Man City", short_name="MCI", code=43, position=2),
    ],
    "element_types": [],
    "phases": [],
}

PLAYER_HISTORY_1 = {
    "history": [{"element": 1, "round": 1, "fixture": 11, "minutes": 90, "total_points": 2}],
    "fixtures": [],
    "history_past": [],
}
PLAYER_HISTORY_2 = {
    "history": [{"element": 2, "round": 1, "fixture": 22, "minutes": 90, "total_points": 5}],
    "fixtures": [],
    "history_past": [],
}


def _element_summary_payload_paths(raw: Path, pid: int) -> list[Path]:
    return sorted((raw / "fpl" / "element-summary" / str(pid)).rglob("payload.json"))

VALID_BOOTSTRAP = {
    "events": [],
    "elements": [player_row(id=1, web_name="Salah", team=11, element_type=3, now_cost=130)],
    "teams": [],
    "element_types": [],
}



def _raw_response(url: str, payload):
    """A RawResponse a capture stage can write, for client stubs."""
    from datetime import datetime, timezone

    from fpl_ingest.extract.http.client import RawResponse

    now = datetime.now(timezone.utc)
    return RawResponse(
        url=url,
        status=200,
        headers={"content-type": "application/json"},
        body=json.dumps(payload).encode("utf-8"),
        requested_at=now,
        received_at=now,
    )


def _raw_fixtures_response(payload=()):
    return _raw_response("https://fantasy.premierleague.com/api/fixtures/", list(payload))


def _raw_event_status_response(payload=None):
    return _raw_response(
        "https://fantasy.premierleague.com/api/event-status/",
        payload if payload is not None else {"status": [], "leagues": ""},
    )


def _raw_bootstrap_response(payload=None):
    return _raw_response(
        "https://fantasy.premierleague.com/api/bootstrap-static/",
        MINIMAL_BOOTSTRAP if payload is None else payload,
    )


def _make_async_client(bootstrap=MINIMAL_BOOTSTRAP, history_side_effect=None):
    client = AsyncMock()
    client.__aenter__ = AsyncMock(return_value=client)
    client.__aexit__ = AsyncMock(return_value=None)
    client.get_bootstrap = AsyncMock(return_value=bootstrap)
    client.get_bootstrap_raw = AsyncMock(return_value=_raw_bootstrap_response(bootstrap))
    client.get_fixtures = AsyncMock(return_value=[])
    client.get_fixtures_raw = AsyncMock(return_value=_raw_fixtures_response())
    client.get_event_status_raw = AsyncMock(return_value=_raw_event_status_response())
    client.get_gw = AsyncMock(return_value=None)

    async def _fetch_raw(pid):
        if history_side_effect is not None:
            payload = await history_side_effect(pid)
        else:
            payload = PLAYER_HISTORY_1 if pid == 1 else PLAYER_HISTORY_2
        return _raw_response(
            f"https://fantasy.premierleague.com/api/element-summary/{pid}/", payload
        )

    client.get_element_summary_raw = AsyncMock(side_effect=_fetch_raw)
    return client


def _run(argv: list[str], mock_client, tmp_path) -> Path:
    raw = tmp_path / "raw"
    with patch("fpl_ingest.orchestration.runner.AsyncFPLClient", return_value=mock_client):
        try:
            main(["--raw-dir", str(raw)] + argv)
        except SystemExit as exc:
            if exc.code != 0:
                raise
    return raw


class FakeClient:
    """Minimal async-context-manager client that tracks whether it was closed."""

    def __init__(self):
        self.closed = False
        _empty_bootstrap = {
            "events": [], "elements": [], "teams": [], "element_types": [], "phases": [],
        }
        self.get_bootstrap = AsyncMock(return_value=_empty_bootstrap)
        self.get_bootstrap_raw = AsyncMock(
            return_value=_raw_bootstrap_response(_empty_bootstrap)
        )
        self.get_fixtures = AsyncMock(return_value=[])
        self.get_fixtures_raw = AsyncMock(return_value=_raw_fixtures_response())
        self.get_event_status_raw = AsyncMock(return_value=_raw_event_status_response())
        self.get_gw = AsyncMock(return_value=None)
        self.get_element_summary_raw = AsyncMock(
            side_effect=lambda pid: _raw_response(
                f"https://fantasy.premierleague.com/api/element-summary/{pid}/",
                {"history": [], "fixtures": [], "history_past": []},
            )
        )

    async def __aenter__(self):
        return self

    async def __aexit__(self, *args):
        self.closed = True
