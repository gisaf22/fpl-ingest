"""event-live's ratification marker and element-summary's settlement marker
must stay independent.

Each marker may only ever mean "MY stage's capture for this gameweek
succeeded." A shared flag would let one stage's success vouch for the other's
completion whenever the two diverge in the same run. These tests run both real
stage functions against one shared raw root for two consecutive runs — only
the FPL client is faked — and check, for every success/failure combination of
the first run, which marker it wrote and what the second run re-fetches.
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock

import pytest

from fpl_ingest.extract.http.client import RawResponse
from fpl_ingest.extract.http.local_writer import LocalRawWriter
from fpl_ingest.extract.http.sync_http import FPLClientError
from fpl_ingest.extract.stages.bootstrap import GameweekInfo
from fpl_ingest.extract.stages.element_summary import ingest_player_histories
from fpl_ingest.extract.stages.gameweeks import ingest_gameweeks

pytestmark = pytest.mark.asyncio

GW = 1
PLAYERS = [1, 2]
EVENTS = [GameweekInfo(id=GW, finished=True, is_current=True)]
RATIFIED = {GW: {"points": "r", "bonus_added": True}}

EVENT_LIVE_MARKER = f"fpl/_settlement/event-live/{GW}/marker.json"
ELEMENT_SUMMARY_MARKER = f"fpl/_settlement/element-summary/{GW}/marker.json"

OK = "ok"
FETCH_ERROR = "fetch_error"
SHAPE_FAILURE = "shape_failure"


def _raw(body: bytes) -> RawResponse:
    at = datetime(2026, 8, 24, 8, 0, 0, tzinfo=timezone.utc)
    return RawResponse(
        url="https://fantasy.premierleague.com/api/",
        status=200,
        headers={"content-type": "application/json"},
        body=body,
        requested_at=at,
        received_at=at,
        attempt_count=1,
    )


def _live_response(outcome: str) -> RawResponse | Exception:
    if outcome == FETCH_ERROR:
        return FPLClientError("event-live down")
    if outcome == SHAPE_FAILURE:
        return _raw(b"<html>502 Bad Gateway</html>")
    return _raw(json.dumps({"elements": [{"id": 1, "stats": {}, "explain": []}]}).encode())


def _summary_response(player_id: int, *, fail: bool) -> RawResponse | Exception:
    if fail:
        return FPLClientError(f"element-summary {player_id} down")
    history = [{"element": player_id, "round": GW, "fixture": 1, "minutes": 90, "total_points": 2}]
    return _raw(json.dumps({"history": history, "fixtures": [], "history_past": []}).encode())


def _client(*, live: str = OK, failing_players: frozenset[int] = frozenset()) -> MagicMock:
    async def get_gameweek_live_raw(_gw: int) -> RawResponse:
        response = _live_response(live)
        if isinstance(response, Exception):
            raise response
        return response

    async def get_element_summary_raw(player_id: int) -> RawResponse:
        response = _summary_response(player_id, fail=player_id in failing_players)
        if isinstance(response, Exception):
            raise response
        return response

    client = MagicMock()
    client.get_gameweek_live_raw = AsyncMock(side_effect=get_gameweek_live_raw)
    client.get_element_summary_raw = AsyncMock(side_effect=get_element_summary_raw)
    return client


async def _run(root: Path, run_id: str, client: MagicMock) -> None:
    """One run of the two stages, sharing a writer exactly as the runner does."""
    writer = LocalRawWriter(root, "fpl", run_id=run_id)
    await ingest_gameweeks(client, writer, EVENTS, event_finality=RATIFIED)
    await ingest_player_histories(client, writer, PLAYERS, EVENTS, event_finality=RATIFIED)


def _fetched_live(client: MagicMock) -> bool:
    return client.get_gameweek_live_raw.await_count > 0


def _fetched_summaries(client: MagicMock) -> list[int]:
    return sorted(c.args[0] for c in client.get_element_summary_raw.call_args_list)


@pytest.mark.parametrize(
    "first_run, live_marked, summary_marked",
    [
        pytest.param(
            {"live": OK, "failing_players": frozenset({2})},
            True, False,
            id="event_live_ok__element_summary_partial_failure",
        ),
        pytest.param(
            {"live": FETCH_ERROR},
            False, True,
            id="element_summary_ok__event_live_fetch_error",
        ),
        pytest.param(
            {"live": SHAPE_FAILURE},
            False, True,
            id="element_summary_ok__event_live_shape_failure",
        ),
        pytest.param(
            {"live": OK},
            True, True,
            id="both_ok",
        ),
        pytest.param(
            {"live": FETCH_ERROR, "failing_players": frozenset({2})},
            False, False,
            id="both_fail",
        ),
    ],
)
async def test_each_marker_vouches_only_for_its_own_stage(
    tmp_path, first_run, live_marked, summary_marked
):
    root = tmp_path / "raw"

    await _run(root, "20260824T080000Z-aaaaaa", _client(**first_run))

    assert (root / EVENT_LIVE_MARKER).exists() is live_marked
    assert (root / ELEMENT_SUMMARY_MARKER).exists() is summary_marked

    second = _client()
    await _run(root, "20260824T083000Z-bbbbbb", second)

    # A stage whose marker was written is not re-fetched; a stage whose marker
    # was withheld is retried — and only that stage. element-summary's retry
    # is its forced full re-fetch, so every player is fetched again.
    assert _fetched_live(second) is not live_marked
    assert _fetched_summaries(second) == ([] if summary_marked else PLAYERS)

    # The retry completes whatever was missing, so both markers now exist.
    assert (root / EVENT_LIVE_MARKER).exists()
    assert (root / ELEMENT_SUMMARY_MARKER).exists()


async def test_after_both_markers_neither_stage_refetches_again(tmp_path):
    """Both succeed: markers are written independently, and no later run
    re-fetches either endpoint for the ratified gameweek."""
    root = tmp_path / "raw"
    await _run(root, "20260824T080000Z-aaaaaa", _client())

    for run_id in ("20260824T083000Z-bbbbbb", "20260824T090000Z-cccccc"):
        client = _client()
        await _run(root, run_id, client)

        assert not _fetched_live(client)
        assert _fetched_summaries(client) == []
