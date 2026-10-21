"""The writer under concurrent threads, and when it flushes the IN_PROGRESS manifest (#123 AC3, AC4, AC5, AC7).

Stages now run S3 calls for different entries on separate threads at once, so the
writer serializes only its own state and its manifest flush. An entry-picks run
batches the flush; every other run still flushes after every object.
"""

from __future__ import annotations

import json
import random
import threading
import time
from datetime import datetime, timezone
from pathlib import Path

import pytest

from fpl_ingest.extract.http import local_writer
from fpl_ingest.extract.http.local_writer import LocalFilesystemBackend, LocalRawWriter

RUN_START = datetime(2026, 10, 21, 5, 55, tzinfo=timezone.utc)
RUN_ID = "20261021T055500Z-c0ffee"
MANIFEST_KEY = f"fpl/_manifests/2026-10-21/{RUN_ID}/manifest.json"
AT = datetime(2026, 10, 21, 6, 0, tzinfo=timezone.utc)
PICKS = b'{"picks": [{"element": 1, "position": 1}]}'


class ManifestRecordingBackend(LocalFilesystemBackend):
    """Records the object count of every manifest PUT, in the order the PUTs complete.

    ``jitter`` makes each call block its thread for up to that many seconds, as boto3 does,
    so threads interleave inside the backend.
    """

    def __init__(self, root: Path, *, jitter: float = 0.0) -> None:
        super().__init__(root)
        self.manifest_counts: list[int] = []
        self._jitter = jitter
        self._local = threading.local()

    def put_bytes(self, key: str, data: bytes, *, overwrite: bool = False) -> str:
        if self._jitter:
            rng = getattr(self._local, "rng", None) or random.Random(threading.get_ident())
            self._local.rng = rng
            time.sleep(rng.uniform(0, self._jitter))
        location = super().put_bytes(key, data, overwrite=overwrite)
        if key == MANIFEST_KEY:
            self.manifest_counts.append(json.loads(data)["totals"]["written"])
        return location


def _writer(root: Path, backend: LocalFilesystemBackend, **extra) -> LocalRawWriter:
    return LocalRawWriter(root, "fpl", run_id=RUN_ID, started_at=RUN_START, backend=backend, **extra)


def _write(writer: LocalRawWriter, n: int) -> None:
    writer.write_object(
        f"entry-picks/2026-27/1/{n}", PICKS,
        request_url=f"https://fantasy.premierleague.com/api/entry/{n}/event/1/picks/",
        requested_at=AT, received_at=AT, http_status=200,
        shape_validation={"ok": True, "checks": [], "failures": [], "record_count": 1},
    )


def _stored_manifest(root: Path) -> dict:
    return json.loads((root / MANIFEST_KEY).read_text())


class TestManyThreads:
    THREADS = 12
    PER_THREAD = 40

    @pytest.mark.covers("#123 AC3")
    def test_concurrent_writes_lose_no_count_and_never_store_an_older_manifest_over_a_newer_one(self, tmp_path):
        backend = ManifestRecordingBackend(tmp_path, jitter=0.002)
        writer = _writer(tmp_path, backend)
        total = self.THREADS * self.PER_THREAD
        start = threading.Barrier(self.THREADS)
        errors: list[BaseException] = []

        def work(thread_index: int) -> None:
            try:
                start.wait()
                for i in range(self.PER_THREAD):
                    _write(writer, thread_index * 1000 + i)
            except BaseException as exc:  # noqa: BLE001 - surfaced by the assertion below
                errors.append(exc)

        threads = [threading.Thread(target=work, args=(t,)) for t in range(self.THREADS)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert errors == []
        outcome = writer.endpoint_outcomes["entry-picks"]
        assert (outcome["attempted"], outcome["usable"]) == (total, total)
        assert len(writer.manifest_snapshot["objects"]) == total
        assert backend.manifest_counts == sorted(backend.manifest_counts), (
            "a manifest PUT carried fewer objects than the one before it: an older snapshot overwrote a newer one"
        )
        assert _stored_manifest(tmp_path)["totals"]["written"] == total

        final = writer.finalize("SUCCESS").manifest
        assert len(final["captures"]) == total
        assert final["totals"]["written"] == total


class TestBatchedFlush:

    @pytest.fixture
    def clock(self, monkeypatch):
        now = [0.0]
        monkeypatch.setattr(local_writer, "_monotonic", lambda: now[0])
        return now

    @pytest.mark.covers("#123 AC4")
    def test_objects_flush_every_fifty_not_every_one(self, tmp_path, clock):
        backend = ManifestRecordingBackend(tmp_path)
        writer = _writer(tmp_path, backend, manifest_flush_every=50, manifest_flush_seconds=10.0)

        for n in range(120):
            _write(writer, n)

        assert backend.manifest_counts == [50, 100]

    @pytest.mark.covers("#123 AC4")
    def test_the_next_object_after_the_interval_flushes_even_below_the_count(self, tmp_path, clock):
        backend = ManifestRecordingBackend(tmp_path)
        writer = _writer(tmp_path, backend, manifest_flush_every=50, manifest_flush_seconds=10.0)

        _write(writer, 1)
        clock[0] = 9.9
        _write(writer, 2)
        assert backend.manifest_counts == []

        clock[0] = 10.0
        _write(writer, 3)
        assert backend.manifest_counts == [3]

    @pytest.mark.covers("#123 AC4")
    def test_failures_are_batched_like_objects(self, tmp_path, clock):
        backend = ManifestRecordingBackend(tmp_path)
        writer = _writer(tmp_path, backend, manifest_flush_every=50, manifest_flush_seconds=10.0)

        for n in range(3):
            writer.record_failure(f"entry-picks/2026-27/1/{n}", request_url="https://x/", error_class="TimeoutError")

        assert backend.manifest_counts == []

    @pytest.mark.covers("#123 AC4")
    def test_flush_manifest_writes_what_is_pending_and_nothing_when_nothing_is(self, tmp_path, clock):
        backend = ManifestRecordingBackend(tmp_path)
        writer = _writer(tmp_path, backend, manifest_flush_every=50, manifest_flush_seconds=10.0)
        _write(writer, 1)

        writer.flush_manifest()
        writer.flush_manifest()

        assert backend.manifest_counts == [1]
        assert _stored_manifest(tmp_path)["status"] == "IN_PROGRESS"

    @pytest.mark.covers("#123 AC4")
    @pytest.mark.parametrize("every, seconds", [(0, 10.0), (50, 0.0), (50, -1.0)])
    def test_a_nonsense_batch_setting_is_refused(self, tmp_path, every, seconds):
        with pytest.raises(ValueError):
            _writer(tmp_path, LocalFilesystemBackend(tmp_path), manifest_flush_every=every, manifest_flush_seconds=seconds)

    @pytest.mark.covers("#123 AC5")
    @pytest.mark.parametrize("event", [
        "abort", "budget_stop", "marker_withheld", "endpoint_detail", "not_attempted",
    ])
    def test_a_run_level_record_flushes_at_once(self, tmp_path, clock, event):
        backend = ManifestRecordingBackend(tmp_path)
        writer = _writer(
            tmp_path, backend, manifest_flush_every=50, manifest_flush_seconds=10.0, http_error_streak=1,
        )
        _write(writer, 1)
        assert backend.manifest_counts == []

        if event == "abort":
            writer.observe_response("http_error", 429)
        elif event == "budget_stop":
            writer.record_stop("stopped_at_budget")
        elif event == "marker_withheld":
            writer.record_marker_withheld("entry-picks", event=1, reason="stopped_at_budget")
        elif event == "endpoint_detail":
            writer.record_endpoint_detail("entry-picks", "throughput", {"requests": 1})
        else:
            writer.record_not_attempted("element-summary", reason="earlier stage failed")

        assert backend.manifest_counts == [1], f"{event} left the stored manifest stale"
        if event == "abort":
            assert _stored_manifest(tmp_path)["abort_reason"] == "aborted_http_error_streak"


class TestDefaultFlush:

    @pytest.mark.covers("#123 AC7")
    def test_a_writer_without_batch_settings_flushes_after_every_object(self, tmp_path):
        backend = ManifestRecordingBackend(tmp_path)
        writer = _writer(tmp_path, backend)

        for n in range(3):
            _write(writer, n)
        writer.record_failure("entry-picks/2026-27/1/9", request_url="https://x/", error_class="TimeoutError")

        assert backend.manifest_counts == [1, 2, 3, 3]
        assert _stored_manifest(tmp_path)["totals"]["failed"] == 1
