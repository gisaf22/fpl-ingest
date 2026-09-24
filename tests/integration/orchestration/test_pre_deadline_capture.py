"""The ``pre-deadline`` command and the manifest ``trigger`` field, end to end.

Drives ``fpl_ingest.cli.main`` against ``tmp_path`` with the FPL client faked.
Deadlines are built relative to the real clock, so the gate runs unpatched.
"""

from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import AsyncMock, patch

import pytest

from fpl_ingest.cli import main
from fpl_ingest.extract.http.sync_http import FPLClientError
from tests.support.cli_fakes import MINIMAL_BOOTSTRAP, _make_async_client, _run


def _bootstrap_with_deadline_in(minutes: float) -> dict:
    deadline = datetime.now(timezone.utc) + timedelta(minutes=minutes)
    return {
        **MINIMAL_BOOTSTRAP,
        "events": [
            {
                "id": 6,
                "finished": False,
                "is_current": False,
                "deadline_time": deadline.strftime("%Y-%m-%dT%H:%M:%SZ"),
            }
        ],
    }


def _pre_deadline(raw: Path, client) -> int:
    with patch("fpl_ingest.orchestration.runner.AsyncFPLClient", return_value=client):
        with pytest.raises(SystemExit) as exc:
            main(["--raw-dir", str(raw), "pre-deadline"])
    return int(exc.value.code or 0)


def _manifests(raw: Path) -> list[dict]:
    return [json.loads(p.read_text()) for p in sorted((raw / "fpl" / "_manifests").rglob("manifest.json"))]


class TestPreDeadlineCapture:

    def test_in_window_writes_bootstrap_only_with_trigger(self, tmp_path):
        raw = tmp_path / "raw"
        client = _make_async_client(bootstrap=_bootstrap_with_deadline_in(60))

        assert _pre_deadline(raw, client) == 0

        payloads = sorted((raw / "fpl" / "bootstrap-static").rglob("payload.json"))
        assert len(payloads) == 1
        [manifest] = _manifests(raw)
        assert manifest["trigger"] == "pre_deadline"
        assert manifest["status"] == "SUCCESS"
        assert set(manifest["objects"]) == {"bootstrap-static"}
        # Only bootstrap-static is fetched; no other endpoint is touched.
        client.get_event_status_raw.assert_not_awaited()
        client.get_fixtures_raw.assert_not_awaited()
        client.get_element_summary_raw.assert_not_awaited()

    def test_out_of_window_writes_nothing(self, tmp_path):
        raw = tmp_path / "raw"
        client = _make_async_client(bootstrap=_bootstrap_with_deadline_in(180))

        assert _pre_deadline(raw, client) == 0

        client.get_bootstrap_raw.assert_awaited_once()
        assert not raw.exists() or not any(p.is_file() for p in raw.rglob("*"))

    def test_passed_deadline_writes_nothing(self, tmp_path):
        raw = tmp_path / "raw"
        client = _make_async_client(bootstrap=_bootstrap_with_deadline_in(-5))

        assert _pre_deadline(raw, client) == 0
        assert not raw.exists() or not any(p.is_file() for p in raw.rglob("*"))

    def test_fetch_failure_exits_non_zero_and_writes_nothing(self, tmp_path):
        raw = tmp_path / "raw"
        client = _make_async_client()
        client.get_bootstrap_raw = AsyncMock(side_effect=FPLClientError("boom"))

        assert _pre_deadline(raw, client) == 1
        assert not raw.exists() or not any(p.is_file() for p in raw.rglob("*"))


class TestRunTrigger:

    @pytest.mark.parametrize("trigger", ["scheduled", "manual"])
    def test_run_records_the_given_trigger(self, tmp_path, trigger):
        raw = _run(["run", "--trigger", trigger], _make_async_client(), tmp_path)
        [manifest] = _manifests(raw)
        assert manifest["trigger"] == trigger

    def test_run_without_trigger_records_null(self, tmp_path):
        raw = _run([], _make_async_client(), tmp_path)
        [manifest] = _manifests(raw)
        assert "trigger" in manifest
        assert manifest["trigger"] is None
