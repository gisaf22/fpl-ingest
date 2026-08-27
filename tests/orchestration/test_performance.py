"""Performance benchmarks for pipeline concurrency."""

from __future__ import annotations

import asyncio
import json
import time
from datetime import datetime, timedelta, timezone
from unittest.mock import AsyncMock

import pytest

from fpl_ingest.extract.http.client import RawResponse
from fpl_ingest.extract.http.local_writer import LocalRawWriter
from fpl_ingest.extract.stages.element_summary import ingest_player_histories
from tests.factories import history_row as _history_row_factory

pytestmark = pytest.mark.perf


def _history_payload(pid: int, rounds: int = 1) -> dict:
    return {
        "history": [
            _history_row_factory(element=pid, round=r, fixture=pid * 100 + r)
            for r in range(1, rounds + 1)
        ],
        "fixtures": [],
        "history_past": [],
    }


def _raw(pid: int) -> RawResponse:
    requested_at = datetime(2026, 8, 24, 8, 0, 0, tzinfo=timezone.utc)
    body = json.dumps(_history_payload(pid)).encode("utf-8")
    return RawResponse(
        url=f"https://fantasy.premierleague.com/api/element-summary/{pid}/",
        status=200,
        headers={"content-type": "application/json"},
        body=body,
        requested_at=requested_at,
        received_at=requested_at + timedelta(milliseconds=1),
        attempt_count=1,
    )


def _writer(tmp_path) -> LocalRawWriter:
    return LocalRawWriter(tmp_path / "raw", "fpl", run_id="20260824T080000Z-abc123")


class TestConcurrentFetchThroughput:
    """Async gather must not bottleneck on zero-latency (mock) client calls."""

    def test_100_players_async_gather(self, tmp_path):
        """Fetching 100 players via asyncio.gather completes in under 2 seconds."""
        client = AsyncMock()
        client.get_element_summary_raw = AsyncMock(side_effect=lambda pid: _raw(pid))

        player_ids = list(range(1, 101))
        writer = _writer(tmp_path)

        async def _run():
            return await ingest_player_histories(client, writer, player_ids)

        start = time.perf_counter()
        result = asyncio.run(_run()).result
        elapsed = time.perf_counter() - start

        assert result.errors == 0
        assert result.fetched == 100
        assert result.written == 100
        assert elapsed < 2.0, f"async gather fetch took {elapsed:.3f}s (limit: 2.0s)"


class TestCaptureWriteThroughput:
    """Every run captures every player fresh — nothing suppresses a fetch."""

    def test_100_players_are_captured_every_run(self, tmp_path):
        """Pre-existing raw captures must NOT suppress network fetches."""
        raw_dir = tmp_path / "raw"
        raw_dir.mkdir(parents=True)
        player_ids = list(range(1, 101))

        client = AsyncMock()
        client.get_element_summary_raw = AsyncMock(side_effect=lambda pid: _raw(pid))
        writer = _writer(tmp_path)

        async def _run():
            return await ingest_player_histories(client, writer, player_ids)

        start = time.perf_counter()
        result = asyncio.run(_run()).result
        elapsed = time.perf_counter() - start

        assert result.errors == 0
        assert result.fetched == 100
        assert result.written == 100
        assert elapsed < 2.0, f"capture took {elapsed:.3f}s (limit: 2.0s)"
