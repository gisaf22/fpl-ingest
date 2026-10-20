"""Streak-abort integration test for the picks stage (#116 AC12).

The abort machinery itself (``LocalRawWriter.aborted`` / ``observe_response``,
#109) and the bounded dispatch loop (``dispatch_bounded``, #115) already
exist and are reused as-is. This is only the picks-specific wiring test: N
consecutive ``http_error`` responses on picks requests must stop dispatch,
finish in-flight work, write ``abort_reason`` on the manifest, exit non-zero,
and leave the affected entries retried next run.

The stub stage (``fpl_ingest.extract.stages.entry_picks.ingest_entry_picks``)
never dispatches anything yet, so this is expected to FAIL until the real
implementation wires the picks fetch through ``dispatch_bounded``.
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock

import pytest

from fpl_ingest.extract.http.client import RawResponse
from fpl_ingest.extract.http.local_writer import LocalRawWriter
from fpl_ingest.extract.http.raw_keys import ABORT_REASON_HTTP_ERROR_STREAK, DEFAULT_HTTP_ERROR_STREAK
from fpl_ingest.extract.stages.entry_picks import COHORT_RANDOM, ingest_entry_picks

pytestmark = pytest.mark.asyncio

SEASON = "2026-27"
GW1 = {"id": 1, "finished": True, "data_checked": True}
N_ENTRIES = 40  # comfortably more than DEFAULT_HTTP_ERROR_STREAK (10)
ENTRIES = [(100 + i, COHORT_RANDOM) for i in range(N_ENTRIES)]


def _http_error(entry_id: int) -> RawResponse:
    at = datetime(2026, 10, 19, 4, 30, tzinfo=timezone.utc)
    return RawResponse(
        url=f"https://fantasy.premierleague.com/api/entry/{entry_id}/event/1/picks/", status=403,
        headers={"content-type": "application/json"}, body=b'{"detail":"forbidden"}',
        requested_at=at, received_at=at, attempt_count=1,
    )


def _ok() -> RawResponse:
    at = datetime(2026, 10, 19, 5, 0, tzinfo=timezone.utc)
    return RawResponse(
        url="https://fantasy.premierleague.com/api/entry/x/event/1/picks/", status=200,
        headers={"content-type": "application/json"}, body=b'{"picks": [{"element": 1, "position": 1}]}',
        requested_at=at, received_at=at, attempt_count=1,
    )


def _blocked_client(*, blocked: bool = True):
    calls: list[int] = []

    async def _fetch(gw: int, entry_id: int) -> RawResponse:
        calls.append(entry_id)
        return _http_error(entry_id) if blocked else _ok()

    client = MagicMock()
    client.get_entry_picks_raw = AsyncMock(side_effect=_fetch)
    client.calls = calls
    return client


def _writer(root: Path, run_id: str) -> LocalRawWriter:
    return LocalRawWriter(root, "fpl", run_id=run_id)


class TestStreakAbort:

    @pytest.mark.covers("#116 AC12")
    async def test_a_streak_of_http_errors_stops_dispatching_and_aborts(self, tmp_path):
        root = tmp_path / "raw"
        writer = _writer(root, "20261019T043000Z-aaaaaa")
        client = _blocked_client()

        await ingest_entry_picks(client, writer, season=SEASON, entries=ENTRIES, gameweeks=[GW1])
        result = writer.finalize("FAILED")
        manifest = json.loads((root / result.manifest_key).read_text())

        assert manifest.get("abort_reason") == ABORT_REASON_HTTP_ERROR_STREAK
        assert DEFAULT_HTTP_ERROR_STREAK <= len(client.calls) < N_ENTRIES, (
            "expected dispatch to stop after the streak threshold, well before every entry was requested"
        )

    @pytest.mark.covers("#116 AC12")
    async def test_the_affected_entries_are_retried_on_the_next_run(self, tmp_path):
        root = tmp_path / "raw"
        first = _blocked_client()
        await ingest_entry_picks(first, _writer(root, "20261019T043000Z-bbbbbb"), season=SEASON, entries=ENTRIES, gameweeks=[GW1])
        assert first.calls, "expected the first run to have dispatched at least one request"

        # The API recovers. A blocked second run would just abort again on the same first entries.
        second = _blocked_client(blocked=False)
        await ingest_entry_picks(second, _writer(root, "20261019T050000Z-cccccc"), season=SEASON, entries=ENTRIES, gameweeks=[GW1])

        assert set(first.calls) <= set(second.calls), "an entry that got an http_error must be retried"
        assert {e for e, _ in ENTRIES} - set(first.calls) <= set(second.calls), (
            "an entry the abort kept from being dispatched must be requested on the next run"
        )
