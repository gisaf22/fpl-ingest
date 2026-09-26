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
from tests.support.cli_fakes import (
    MINIMAL_BOOTSTRAP,
    _make_async_client,
    _raw_fixtures_response,
    _run,
)


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


def _pre_deadline(raw: Path, client, *extra: str) -> int:
    with patch("fpl_ingest.orchestration.runner.AsyncFPLClient", return_value=client):
        with pytest.raises(SystemExit) as exc:
            main(["--raw-dir", str(raw), "pre-deadline", *extra])
    return int(exc.value.code or 0)


def _manifests(raw: Path) -> list[dict]:
    return [json.loads(p.read_text()) for p in sorted((raw / "fpl" / "_manifests").rglob("manifest.json"))]


class TestPreDeadlineCapture:

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


def _payloads(raw: Path, endpoint: str) -> list[Path]:
    return sorted((raw / "fpl" / endpoint).rglob("payload.json"))


_FIXTURES = [
    {"id": 51, "event": 6, "team_h": 11, "team_a": 13, "kickoff_time": "2026-10-10T11:30:00Z",
     "team_h_difficulty": 4, "team_a_difficulty": 5, "finished": False},
    {"id": 52, "event": 6, "team_h": 13, "team_a": 11, "kickoff_time": "2026-10-10T14:00:00Z",
     "team_h_difficulty": 5, "team_a_difficulty": 4, "finished": False},
]


class TestPreDeadlineCapturesFixtures:

    @pytest.mark.covers("#46 AC1")
    @pytest.mark.parametrize("fixtures", [_FIXTURES, []], ids=["normal", "empty-list"])
    def test_in_window_run_saves_fixtures_verbatim_alongside_bootstrap(self, tmp_path, fixtures):
        raw = tmp_path / "raw"
        client = _make_async_client(bootstrap=_bootstrap_with_deadline_in(60))
        fetched = _raw_fixtures_response(fixtures)
        client.get_fixtures_raw = AsyncMock(return_value=fetched)

        assert _pre_deadline(raw, client) == 0

        assert len(_payloads(raw, "bootstrap-static")) == 1
        [fixtures_payload] = _payloads(raw, "fixtures")
        assert fixtures_payload.read_bytes() == fetched.body
        # Same run: one manifest covers both captures.
        assert len(_manifests(raw)) == 1

    @pytest.mark.covers("#46 AC2")
    def test_in_window_manifest_lists_both_captures_as_pre_deadline(self, tmp_path):
        raw = tmp_path / "raw"
        client = _make_async_client(bootstrap=_bootstrap_with_deadline_in(60))
        client.get_fixtures_raw = AsyncMock(return_value=_raw_fixtures_response(_FIXTURES))

        assert _pre_deadline(raw, client) == 0

        [manifest] = _manifests(raw)
        assert manifest["trigger"] == "pre_deadline"
        assert manifest["status"] == "SUCCESS"
        assert set(manifest["objects"]) == {"bootstrap-static", "fixtures"}

    @pytest.mark.covers("#46 AC3")
    def test_fixtures_failure_keeps_bootstrap_and_reports_failure(self, tmp_path):
        raw = tmp_path / "raw"
        client = _make_async_client(bootstrap=_bootstrap_with_deadline_in(60))
        # FPLClientError is what the client raises once its retries are exhausted.
        client.get_fixtures_raw = AsyncMock(side_effect=FPLClientError("503 after retries"))

        assert _pre_deadline(raw, client) != 0

        assert len(_payloads(raw, "bootstrap-static")) == 1
        assert _payloads(raw, "fixtures") == []
        [manifest] = _manifests(raw)
        assert manifest["status"] != "SUCCESS"
        assert [f["endpoint"] for f in manifest["failures"]] == ["fixtures"]


class TestForce:
    """``--force`` is what the workflow's ``force`` dispatch input maps to."""

    def test_without_force_still_no_ops_outside_the_window(self, tmp_path):
        raw = tmp_path / "raw"
        client = _make_async_client(bootstrap=_bootstrap_with_deadline_in(180))

        assert _pre_deadline(raw, client) == 0
        assert not raw.exists() or not any(p.is_file() for p in raw.rglob("*"))

    @pytest.mark.covers("#46 AC5")
    def test_force_captures_outside_the_window_as_manual(self, tmp_path):
        raw = tmp_path / "raw"
        client = _make_async_client(bootstrap=_bootstrap_with_deadline_in(180))

        assert _pre_deadline(raw, client, "--force") == 0

        assert len(_payloads(raw, "bootstrap-static")) == 1
        assert len(_payloads(raw, "fixtures")) == 1
        [manifest] = _manifests(raw)
        assert manifest["trigger"] == "manual"
        assert set(manifest["objects"]) == {"bootstrap-static", "fixtures"}

    @pytest.mark.covers("#46 AC5")
    def test_force_keeps_fixtures_when_bootstrap_fails_and_reports_failure(self, tmp_path):
        raw = tmp_path / "raw"
        client = _make_async_client(bootstrap=_bootstrap_with_deadline_in(180))
        client.get_bootstrap_raw = AsyncMock(side_effect=FPLClientError("503 after retries"))

        assert _pre_deadline(raw, client, "--force") != 0

        client.get_bootstrap_raw.assert_awaited()
        client.get_fixtures_raw.assert_awaited()
        assert _payloads(raw, "bootstrap-static") == []
        assert len(_payloads(raw, "fixtures")) == 1
        [manifest] = _manifests(raw)
        assert manifest["trigger"] == "manual"
        assert manifest["status"] != "SUCCESS"
        assert [f["endpoint"] for f in manifest["failures"]] == ["bootstrap-static"]


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
