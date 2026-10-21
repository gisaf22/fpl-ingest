"""Entry-picks S3 calls overlap across entries, and an interrupted run's manifests stay honest (#123 AC1, AC2, AC4, AC6).

Run 39552536884 captured 1.8 requests/s because every S3 call, from every worker,
queued behind one lock. These tests run the real stage over ``S3Backend`` with a
fake client whose every call takes 50-80 ms, so serialized calls show as a wall
time at least the summed call time.
"""

from __future__ import annotations

import asyncio
import json
import os
import random
import threading
import time
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from fpl_ingest.extract.http.client import RawResponse
from fpl_ingest.extract.http.local_writer import LocalFilesystemBackend, LocalRawWriter
from fpl_ingest.extract.http.raw_keys import panel_key
from fpl_ingest.extract.http.rate_config import MAX_CONCURRENT_REQUESTS
from fpl_ingest.extract.http.s3_backend import S3Backend
from fpl_ingest.extract.stages.entry_picks import COHORT_RANDOM, ingest_entry_picks
from tests.integration.test_entry_picks_cli import BOOTSTRAP, _entry_picks
from tests.support.cli_fakes import _make_async_client
from tests.support.latency_s3 import LatencyS3Client

pytestmark = pytest.mark.asyncio

SEASON = "2026-27"
GW1 = {"id": 1, "finished": True, "data_checked": True}
AT = datetime(2026, 10, 21, 6, 0, tzinfo=timezone.utc)
PICKS = json.dumps({"picks": [{"element": 1, "position": 1, "multiplier": 1}]}).encode()
ENTRIES = [(e, COHORT_RANDOM) for e in range(100, 130)]  # 30 entries, three waves of 10
BATCH = {"manifest_flush_every": 50, "manifest_flush_seconds": 10.0}


def _client(delay_seconds: float = 0.0):
    async def _fetch(gw: int, entry_id: int) -> RawResponse:
        if delay_seconds:
            await asyncio.sleep(delay_seconds)
        return RawResponse(
            url=f"https://fantasy.premierleague.com/api/entry/{entry_id}/event/{gw}/picks/", status=200,
            headers={"content-type": "application/json"}, body=PICKS, requested_at=AT, received_at=AT,
        )

    return MagicMock(get_entry_picks_raw=AsyncMock(side_effect=_fetch))


def _s3_writer(tmp_path: Path, s3: LatencyS3Client, run_id: str) -> LocalRawWriter:
    return LocalRawWriter(tmp_path, "fpl", run_id=run_id, backend=S3Backend("fpl-data-safari", client=s3), **BATCH)


def _bodies_on_disk(root: Path) -> set[str]:
    stored = list(root.rglob("payload.json")) + list(root.rglob("response.body"))
    return {p.relative_to(root).as_posix() for p in stored}


class TestOverlappingS3Calls:

    @pytest.mark.covers("#123 AC1")
    async def test_s3_calls_for_different_entries_overlap(self, tmp_path):
        s3 = LatencyS3Client()
        writer = _s3_writer(tmp_path, s3, "20261021T060000Z-a12301")

        started = time.monotonic()
        outcome = await ingest_entry_picks(_client(), writer, season=SEASON, entries=ENTRIES, gameweeks=[GW1])
        wall = time.monotonic() - started

        assert outcome.result.written == len(ENTRIES)
        assert wall < 0.35 * s3.total_call_seconds, (
            f"stage took {wall:.2f}s for {s3.total_call_seconds:.2f}s of S3 calls: they ran one after another"
        )

    @pytest.mark.covers("#123 AC2")
    async def test_the_pool_reaches_the_dispatch_limit_on_a_one_cpu_machine(self, tmp_path, monkeypatch):
        # The loop's default executor would size itself min(32, cpu + 4) = 5 here.
        monkeypatch.setattr(os, "cpu_count", lambda: 1)
        s3 = LatencyS3Client()
        writer = _s3_writer(tmp_path, s3, "20261021T060000Z-a12302")

        await ingest_entry_picks(_client(), writer, season=SEASON, entries=ENTRIES, gameweeks=[GW1])

        assert s3.peak_in_flight == MAX_CONCURRENT_REQUESTS


class FailingBackend(LocalFilesystemBackend):
    """Blocks each call, as boto3 does, and fails a resume check once ``fail_after`` payloads are stored.

    The check fails only while another PUT is in flight, and PUTs take a jittered
    30-70 ms, so when the stage raises some write is still running: it is on the
    trace only if the stage waits for it before its last flush.
    """

    def __init__(self, root: Path, fail_after: int) -> None:
        super().__init__(root)
        self.fail_after = fail_after
        self.payloads = 0
        self.puts_in_flight = 0
        self._random = random.Random(123)
        self._lock = threading.Lock()

    def exists_prefix(self, prefix: str) -> bool:
        time.sleep(0.005)
        with self._lock:
            if self.payloads >= self.fail_after and self.puts_in_flight:
                raise RuntimeError("storage unavailable")
        return super().exists_prefix(prefix)

    def put_bytes(self, key: str, data: bytes, *, overwrite: bool = False) -> str:
        with self._lock:
            self.puts_in_flight += 1
            delay = self._random.uniform(0.03, 0.07)
        try:
            time.sleep(delay)
            location = super().put_bytes(key, data, overwrite=overwrite)
        finally:
            with self._lock:
                self.puts_in_flight -= 1
        if key.endswith("payload.json"):
            with self._lock:
                self.payloads += 1
        return location


class TestInterruptedRun:

    @pytest.mark.covers("#123 AC6")
    async def test_a_stage_that_raises_leaves_a_trace_counting_every_stored_object(self, tmp_path):
        run_id = "20261021T060000Z-a12303"
        backend = FailingBackend(tmp_path, fail_after=12)
        writer = LocalRawWriter(tmp_path, "fpl", run_id=run_id, backend=backend, **BATCH)

        with pytest.raises(RuntimeError, match="storage unavailable"):
            await ingest_entry_picks(_client(), writer, season=SEASON, entries=ENTRIES, gameweeks=[GW1])
        # Anything still writing after the stage returned lands now, and must already be on the trace.
        await asyncio.sleep(0.3)

        stored = _bodies_on_disk(tmp_path / "fpl")
        trace = json.loads((tmp_path / f"fpl/_manifests/{writer.extraction_date}/{run_id}/manifest.json").read_text())
        assert trace["status"] == "IN_PROGRESS"
        assert 0 < len(stored) < 50, "the failure must land inside the first batch to test the stage's own flush"
        assert trace["totals"]["written"] == len(stored)

    @pytest.mark.covers("#123 AC6")
    async def test_a_budget_stopped_run_finalizes_with_every_stored_body_in_captures(self, tmp_path):
        s3 = LatencyS3Client()
        writer = _s3_writer(tmp_path, s3, "20261021T060000Z-a12304")

        outcome = await ingest_entry_picks(
            _client(0.2), writer, season=SEASON, entries=ENTRIES, gameweeks=[GW1], budget_seconds=0.1,
        )
        final = writer.finalize("SUCCESS").manifest

        assert outcome.metadata.get("stopped_reason") == "stopped_at_budget"
        stored = {k for k in s3.objects if k.endswith(("payload.json", "response.body"))}
        assert 0 < len(stored) < len(ENTRIES)
        assert {c["key"] for c in final["captures"]} == stored


class TestPicksRunBatchesItsFlush:

    @pytest.mark.covers("#123 AC4")
    async def test_a_picks_run_rewrites_its_manifest_a_handful_of_times_not_once_per_object(self, tmp_path):
        raw = tmp_path / "raw"
        panel = {"season": SEASON, "entries": [
            {"entry_id": e, "cohort": COHORT_RANDOM, "joined_gw": 1} for e in range(1000, 1060)
        ]}
        LocalFilesystemBackend(raw).put_bytes(panel_key("fpl", SEASON), json.dumps(panel).encode())
        client = _make_async_client(bootstrap=BOOTSTRAP)
        client.get_entry_picks_raw = _client().get_entry_picks_raw
        manifest_puts: list[str] = []
        real_put = LocalFilesystemBackend.put_bytes

        def counting_put(self, key, data, *, overwrite=False):
            if "/_manifests/" in key:
                manifest_puts.append(key)
            return real_put(self, key, data, overwrite=overwrite)

        with patch.object(LocalFilesystemBackend, "put_bytes", counting_put):
            exit_code = await asyncio.to_thread(_entry_picks, raw, client)

        objects = len(_bodies_on_disk(raw / "fpl"))
        assert exit_code == 0
        assert objects == 121, "bootstrap-static plus 60 managers in each of gameweeks 1 and 2"
        assert len(manifest_puts) <= 8, f"{len(manifest_puts)} manifest writes for {objects} objects"
