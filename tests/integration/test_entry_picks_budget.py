"""Time budget and throughput record for picks capture (#116 AC4/AC5).

Same direct-call style as ``test_entry_picks_capture.py``. Written against
the ``ingest_entry_picks`` stub (``fpl_ingest.extract.stages.entry_picks``),
which has no budget or throughput logic yet, so every test here is expected
to FAIL until the real implementation lands.
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock

import pytest

from fpl_ingest.extract.http.client import RawResponse
from fpl_ingest.extract.http.local_writer import LocalRawWriter
from fpl_ingest.extract.stages.entry_picks import COHORT_RANDOM, ingest_entry_picks

pytestmark = pytest.mark.asyncio

SEASON = "2026-27"
GW1 = {"id": 1, "finished": True, "data_checked": True}
PICKS_15 = {"picks": [{"element": i, "position": i, "multiplier": 1} for i in range(1, 16)]}
ENTRIES = [(e, COHORT_RANDOM) for e in range(100, 120)]  # 20 entries


def _ok() -> RawResponse:
    at = datetime(2026, 10, 19, 4, 30, tzinfo=timezone.utc)
    return RawResponse(
        url="https://fantasy.premierleague.com/api/entry/x/event/y/picks/", status=200,
        headers={"content-type": "application/json"}, body=json.dumps(PICKS_15).encode(),
        requested_at=at, received_at=at, attempt_count=1,
    )


def _slow_client(delay_seconds: float):
    """Every request succeeds but sleeps first, so a small budget is exhausted quickly."""
    import asyncio

    calls: list[int] = []

    async def _fetch(gw: int, entry_id: int) -> RawResponse:
        calls.append(entry_id)
        await asyncio.sleep(delay_seconds)
        return _ok()

    client = MagicMock()
    client.get_entry_picks_raw = AsyncMock(side_effect=_fetch)
    client.calls = calls
    return client


def _writer(root: Path, run_id: str) -> LocalRawWriter:
    return LocalRawWriter(root, "fpl", run_id=run_id)


class TestBudget:

    @pytest.mark.covers("#116 AC4")
    async def test_a_small_budget_stops_cleanly_and_leaves_unreached_entries_absent(self, tmp_path):
        root = tmp_path / "raw"
        writer = _writer(root, "20261019T043000Z-aaaaaa")
        client = _slow_client(0.05)

        outcome = await ingest_entry_picks(
            client, writer, season=SEASON, entries=ENTRIES, gameweeks=[GW1], budget_seconds=0.12,
        )

        assert len(client.calls) < len(ENTRIES), "expected the budget to stop before every entry was reached"
        reason = getattr(outcome.result, "stopped_reason", None) or outcome.metadata.get("stopped_reason")
        assert reason == "stopped_at_budget"

    @pytest.mark.covers("#116 AC4")
    async def test_a_second_run_resumes_at_the_first_uncaptured_gameweek(self, tmp_path):
        root = tmp_path / "raw"
        first = _slow_client(0.05)
        await ingest_entry_picks(
            first, _writer(root, "20261019T043000Z-bbbbbb"),
            season=SEASON, entries=ENTRIES, gameweeks=[GW1], budget_seconds=0.12,
        )
        assert len(first.calls) < len(ENTRIES)

        remaining_before = set(e for e, _ in ENTRIES) - set(first.calls)
        second = _slow_client(0.0)
        await ingest_entry_picks(
            second, _writer(root, "20261019T050000Z-cccccc"),
            season=SEASON, entries=ENTRIES, gameweeks=[GW1], budget_seconds=None,
        )

        assert set(second.calls) == remaining_before, (
            f"expected the resume run to fetch exactly the entries the first run missed; "
            f"missed {remaining_before}, resumed {set(second.calls)}"
        )

    @pytest.mark.covers("#116 AC4")
    async def test_a_budget_stop_that_captured_nothing_exits_non_zero(self, tmp_path):
        root = tmp_path / "raw"
        writer = _writer(root, "20261019T043000Z-dddddd")
        client = _slow_client(10.0)  # far exceeds the budget before even one request resolves

        outcome = await ingest_entry_picks(
            client, writer, season=SEASON, entries=ENTRIES, gameweeks=[GW1], budget_seconds=0.01,
        )

        assert outcome.result.written == 0
        reason = getattr(outcome.result, "stopped_reason", None) or outcome.metadata.get("stopped_reason")
        assert reason == "stopped_at_budget"
        assert outcome.metadata.get("exit_non_zero") == "true" or outcome.result.errors > 0, (
            "a budget stop that captured nothing must be surfaced as a failure, not a quiet SUCCESS"
        )


class TestThroughputRecord:

    @pytest.mark.covers("#116 AC5")
    async def test_the_first_run_records_requests_per_second_total_time_and_gameweeks_completed(self, tmp_path):
        root = tmp_path / "raw"
        writer = _writer(root, "20261019T043000Z-eeeeee")
        client = _slow_client(0.0)

        outcome = await ingest_entry_picks(client, writer, season=SEASON, entries=ENTRIES, gameweeks=[GW1])
        result = writer.finalize("SUCCESS")
        manifest = json.loads((root / result.manifest_key).read_text())

        throughput = manifest.get("endpoints", {}).get("entry-picks", {}).get("throughput", None)
        assert throughput is not None, "expected a throughput record on the finalized manifest"
        assert {"requests_per_second", "total_seconds", "gameweeks_completed"} <= set(throughput)
        assert throughput["gameweeks_completed"] == [1]
