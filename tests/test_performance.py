"""Performance benchmarks for store throughput and pipeline concurrency.

These tests are not run by default. Invoke explicitly:
    pytest -m perf tests/test_performance.py -v

Each test asserts a wall-clock ceiling. The ceilings are intentionally
generous (CI machines vary) — the goal is to catch O(n²) regressions,
not to enforce microsecond precision.
"""

from __future__ import annotations

import asyncio
import json
import time
from unittest.mock import AsyncMock

import pytest

from fpl_ingest.models import PlayerHistoryModel
from fpl_ingest.pipeline.history import ingest_player_histories
from fpl_ingest.pipeline.schema import setup_store
from fpl_ingest.store import SQLiteStore

pytestmark = pytest.mark.perf


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _history_row(element_id: int, round_: int, fixture: int) -> dict:
    return {
        "element": element_id,
        "round": round_,
        "fixture": fixture,
        "minutes": 90,
        "total_points": 6,
    }


def _history_payload(pid: int, rounds: int = 1) -> dict:
    return {
        "history": [_history_row(pid, r, pid * 100 + r) for r in range(1, rounds + 1)],
        "history_past": [],
    }


# ---------------------------------------------------------------------------
# Store throughput
# ---------------------------------------------------------------------------


class TestStoreThroughput:
    """Bulk upsert must handle realistic loads without degrading."""

    def test_upsert_1000_rows(self, tmp_path):
        """1000 PlayerHistory rows upserted in under 1 second."""
        store = SQLiteStore(tmp_path / "perf.db")
        setup_store(store)

        # 50 players x 20 GWs = 1000 rows
        rows = [
            PlayerHistoryModel.prepare(_history_row(pid, gw, pid * 100 + gw))
            for pid in range(1, 51)
            for gw in range(1, 21)
        ]

        start = time.perf_counter()
        with store.transaction():
            inserted, skipped = store.upsert_models("player_histories", PlayerHistoryModel, rows)
        elapsed = time.perf_counter() - start

        assert inserted == 1000
        assert skipped == 0
        assert elapsed < 1.0, f"upsert took {elapsed:.3f}s (limit: 1.0s)"

    def test_idempotent_upsert_1000_rows(self, tmp_path):
        """Re-upserting the same 1000 rows (ON CONFLICT DO UPDATE) completes in under 1 second."""
        store = SQLiteStore(tmp_path / "perf_idem.db")
        setup_store(store)

        rows = [
            PlayerHistoryModel.prepare(_history_row(pid, gw, pid * 100 + gw))
            for pid in range(1, 51)
            for gw in range(1, 21)
        ]

        with store.transaction():
            store.upsert_models("player_histories", PlayerHistoryModel, rows)

        # Second upsert — every row conflicts and is updated in-place.
        start = time.perf_counter()
        with store.transaction():
            store.upsert_models("player_histories", PlayerHistoryModel, rows)
        elapsed = time.perf_counter() - start

        assert elapsed < 1.0, f"idempotent re-upsert took {elapsed:.3f}s (limit: 1.0s)"


# ---------------------------------------------------------------------------
# Concurrent fetch throughput
# ---------------------------------------------------------------------------


class TestConcurrentFetchThroughput:
    """Async gather must not bottleneck on zero-latency (mock) client calls."""

    def test_100_players_async_gather(self, tmp_path):
        """Fetching 100 players via asyncio.gather completes in under 2 seconds."""
        client = AsyncMock()
        client.get_player_history = AsyncMock(side_effect=lambda pid: _history_payload(pid))

        store = SQLiteStore(tmp_path / "perf_concurrent.db")
        setup_store(store)

        player_ids = list(range(1, 101))

        async def _run():
            with store.transaction():
                return await ingest_player_histories(
                    client, store, tmp_path / "raw", player_ids
                )

        start = time.perf_counter()
        result = asyncio.run(_run())
        elapsed = time.perf_counter() - start

        assert result.errors == 0
        assert result.fetched == 100
        assert elapsed < 2.0, f"async gather fetch took {elapsed:.3f}s (limit: 2.0s)"


# ---------------------------------------------------------------------------
# Cache read throughput
# ---------------------------------------------------------------------------


class TestCacheReadThroughput:
    """Pre-cached JSON reads must not hit the network and stay fast."""

    def test_100_cached_players(self, tmp_path):
        """Reading 100 pre-cached JSON files from disk completes in under 1 second."""
        players_dir = tmp_path / "raw" / "players"
        players_dir.mkdir(parents=True)
        player_ids = list(range(1, 101))

        for pid in player_ids:
            (players_dir / f"{pid}.json").write_text(
                json.dumps(_history_payload(pid)), encoding="utf-8"
            )

        client = AsyncMock()
        store = SQLiteStore(tmp_path / "perf_cache.db")
        setup_store(store)

        async def _run():
            with store.transaction():
                return await ingest_player_histories(
                    client, store, tmp_path / "raw", player_ids
                )

        start = time.perf_counter()
        result = asyncio.run(_run())
        elapsed = time.perf_counter() - start

        client.get_player_history.assert_not_called()
        assert result.errors == 0
        assert result.fetched == 100
        assert elapsed < 1.0, f"cache read took {elapsed:.3f}s (limit: 1.0s)"
