"""The manager panel keeps the event loop free, names failures by class, and logs progress (#115, post-run follow-up: AC16-AC19).

Runs 39365465979 and 39366583920 failed the same 10 ids with an un-retried
TimeoutError: synchronous S3 calls blocked the loop while the first requests'
30 s timers ran.
"""

from __future__ import annotations

import asyncio
import logging
import time
from datetime import datetime, timezone
from unittest.mock import AsyncMock

import pytest

from fpl_ingest.extract.http.local_writer import LocalFilesystemBackend, LocalRawWriter
from fpl_ingest.extract.http.sync_http import FPLClientError
from fpl_ingest.extract.stages.manager_panel import ingest_manager_panel
from tests.integration.test_manager_panel_capture import SELF_IDS, _history
from tests.support.cli_fakes import _make_async_client

BLOCK_SECONDS = 0.05


class SlowBackend(LocalFilesystemBackend):
    """A backend whose calls block their thread, as boto3 does."""

    def exists_prefix(self, prefix: str) -> bool:
        time.sleep(BLOCK_SECONDS)
        return super().exists_prefix(prefix)

    def put_bytes(self, key: str, data: bytes, *, overwrite: bool = False) -> str:
        time.sleep(BLOCK_SECONDS)
        return super().put_bytes(key, data, overwrite=overwrite)


def _writer(tmp_path, backend=None):
    return LocalRawWriter(
        tmp_path, source="fpl", started_at=datetime(2026, 10, 20, 3, 30, tzinfo=timezone.utc),
        backend=backend or LocalFilesystemBackend(tmp_path),
    )


def _client(fail=None):
    client = _make_async_client(bootstrap={})

    async def _fetch(entry_id: int):
        if fail and entry_id in fail:
            raise fail[entry_id]
        return _history(entry_id, "entry_history_self" if entry_id in SELF_IDS else "entry_history_active")

    client.get_entry_history_raw = AsyncMock(side_effect=_fetch)
    return client


def _ingest(writer, client, **extra):
    return ingest_manager_panel(client, writer, season="2026-27", total_players=40, seed=1,
                                panel_size=40, self_cohort=(), **extra)


@pytest.mark.covers("#115 AC17")
def test_blocking_backend_calls_do_not_stall_the_event_loop(tmp_path):
    gaps: list[float] = []

    async def heartbeat(stop: asyncio.Event):
        last = time.monotonic()
        while not stop.is_set():
            await asyncio.sleep(0.005)
            now = time.monotonic()
            gaps.append(now - last)
            last = now

    async def main():
        stop = asyncio.Event()
        beat = asyncio.create_task(heartbeat(stop))
        await _ingest(_writer(tmp_path, SlowBackend(tmp_path)), _client())
        stop.set()
        await beat

    asyncio.run(main())

    # One blocking call is BLOCK_SECONDS; on the loop thread the gaps would be that long or more.
    assert max(gaps) < BLOCK_SECONDS * 0.8


@pytest.mark.covers("#115 AC17")
def test_off_loop_calls_still_store_every_id(tmp_path):
    writer = _writer(tmp_path, SlowBackend(tmp_path))
    outcome = asyncio.run(_ingest(writer, _client()))

    assert outcome.result.errors == 0
    assert outcome.result.written == 40


@pytest.mark.covers("#115 AC18")
def test_failure_line_names_the_exception_class_when_its_message_is_empty(tmp_path, caplog):
    caplog.set_level(logging.ERROR)
    writer = _writer(tmp_path)

    outcome = asyncio.run(_ingest(writer, _client(fail={3: asyncio.TimeoutError()})))

    assert outcome.result.errors == 1
    [line] = [r.getMessage() for r in caplog.records if "history 3" in r.getMessage()]
    assert "TimeoutError" in line


@pytest.mark.covers("#115 AC19")
def test_progress_is_logged_with_attempted_stored_failed(tmp_path, caplog):
    caplog.set_level(logging.INFO, logger="fpl_ingest.extract.stages.manager_panel")
    writer = _writer(tmp_path, SlowBackend(tmp_path))
    client = _client(fail={3: FPLClientError("503 after retries")})

    asyncio.run(_ingest(writer, client, progress_interval=0.1))

    lines = [r.getMessage() for r in caplog.records if "progress" in r.getMessage().lower()]
    assert len(lines) >= 2
    assert all(f in lines[-1] for f in ("attempted", "stored", "failed"))
    assert "failed 1" in lines[-1] or "failed=1" in lines[-1]
