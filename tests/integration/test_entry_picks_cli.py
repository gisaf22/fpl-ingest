"""The ``entry-picks`` command and the stage's settlement rules (#116, proposed AC16 and AC17).

Neither is in the issue's table: AC16 pins the command wiring (panel in, captures and
manifest out, exit code), AC17 pins the decision "marker only when every kept entry is
captured, oldest gameweek first, the next never starting before the earlier settles",
which AC2 and AC10 imply but do not state. Proposed; the owner removes them if rejected.
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from fpl_ingest.cli import main
from fpl_ingest.config import DEFAULT_ENTRY_PICKS_MAX_SECONDS, resolve_entry_picks_max_seconds
from fpl_ingest.extract.http.client import RawResponse
from fpl_ingest.extract.http.local_writer import LocalFilesystemBackend, LocalRawWriter
from fpl_ingest.extract.http.raw_keys import panel_key
from fpl_ingest.extract.stages.entry_picks import COHORT_RANDOM, COHORT_SELF, ingest_entry_picks
from tests.support.cli_fakes import MINIMAL_BOOTSTRAP, _make_async_client
from tests.support.fixture_payloads import payload_bytes

SEASON = "2026-27"
SELF_ID = 8450051
RANDOM_A, RANDOM_B = 11, 12
BOOTSTRAP = {
    **MINIMAL_BOOTSTRAP,
    "events": [
        {"id": 1, "finished": True, "data_checked": True, "is_current": False, "deadline_time": "2026-08-14T17:30:00Z"},
        {"id": 2, "finished": True, "data_checked": True, "is_current": False, "deadline_time": "2026-08-21T17:30:00Z"},
        {"id": 3, "finished": True, "data_checked": False, "is_current": False, "deadline_time": "2026-08-28T17:30:00Z"},
        {"id": 4, "finished": False, "data_checked": False, "is_current": True, "deadline_time": "2026-09-04T17:30:00Z"},
    ],
}
AT = datetime(2026, 10, 20, 7, 0, tzinfo=timezone.utc)


def _raw(status: int, body: bytes) -> RawResponse:
    return RawResponse(
        url="https://fantasy.premierleague.com/api/x/", status=status,
        headers={"content-type": "application/json"}, body=body, requested_at=AT, received_at=AT,
    )


def _write_panel(raw: Path) -> None:
    panel = {"season": SEASON, "entries": [
        {"entry_id": RANDOM_A, "cohort": COHORT_RANDOM, "joined_gw": 1},
        {"entry_id": SELF_ID, "cohort": COHORT_SELF, "joined_gw": 1},
    ]}
    LocalFilesystemBackend(raw).put_bytes(panel_key("fpl", SEASON), json.dumps(panel).encode())


def _client(delay_seconds: float = 0.0):
    client = _make_async_client(bootstrap=BOOTSTRAP)
    calls: list[tuple[int, int]] = []

    async def _fetch(gw: int, entry_id: int) -> RawResponse:
        calls.append((gw, entry_id))
        if delay_seconds:
            import asyncio

            await asyncio.sleep(delay_seconds)
        return _raw(200, payload_bytes("entry_picks_self"))

    client.get_entry_picks_raw = AsyncMock(side_effect=_fetch)
    client.calls = calls
    return client


def _entry_picks(raw: Path, client, *extra: str) -> int:
    with patch("fpl_ingest.orchestration.runner.AsyncFPLClient", return_value=client):
        with pytest.raises(SystemExit) as exc:
            main(["--raw-dir", str(raw), "entry-picks", *extra])
    return int(exc.value.code or 0)


def _manifests(raw: Path) -> list[dict]:
    return [json.loads(p.read_text()) for p in sorted((raw / "fpl" / "_manifests").rglob("manifest.json"))]


def _marker(raw: Path, gw: int) -> Path:
    return raw / "fpl" / "_settlement" / "entry-picks" / str(gw) / "marker.json"


class TestCommand:

    @pytest.mark.covers("#116 AC16")
    def test_it_reads_the_panel_and_captures_only_settled_gameweeks_and_exits_zero(self, tmp_path):
        raw = tmp_path / "raw"
        _write_panel(raw)
        client = _client()

        assert _entry_picks(raw, client) == 0

        assert sorted(client.calls) == [(1, RANDOM_A), (1, SELF_ID), (2, RANDOM_A), (2, SELF_ID)]
        assert _marker(raw, 1).exists() and _marker(raw, 2).exists()
        assert not _marker(raw, 3).exists() and not _marker(raw, 4).exists()
        [manifest] = _manifests(raw)
        assert manifest["status"] == "SUCCESS"
        assert manifest["endpoints"]["entry-picks"]["throughput"]["gameweeks_completed"] == [1, 2]
        assert manifest["config"]["max_seconds"] == DEFAULT_ENTRY_PICKS_MAX_SECONDS

    @pytest.mark.covers("#116 AC16")
    def test_a_second_run_requests_nothing(self, tmp_path):
        raw = tmp_path / "raw"
        _write_panel(raw)
        assert _entry_picks(raw, _client()) == 0

        second = _client()
        assert _entry_picks(raw, second) == 0

        assert second.calls == []

    @pytest.mark.covers("#116 AC16")
    def test_no_panel_exits_non_zero_and_fetches_nothing(self, tmp_path):
        raw = tmp_path / "raw"
        client = _client()

        assert _entry_picks(raw, client) == 1

        assert client.calls == []
        [manifest] = _manifests(raw)
        assert manifest["endpoints"]["entry-picks"]["outcome"] == "FAILED"
        assert "manager-panel" in manifest["endpoints"]["entry-picks"]["failures"][0]["reason"]

    @pytest.mark.covers("#116 AC16")
    def test_a_budget_spent_before_any_capture_exits_non_zero_and_a_rerun_resumes(self, tmp_path):
        raw = tmp_path / "raw"
        _write_panel(raw)

        # 1 ns is spent by the time the first request is due, whatever the machine's speed.
        assert _entry_picks(raw, _client(), "--max-seconds", "1e-9") == 1

        [manifest] = _manifests(raw)
        assert manifest["stop_reason"] == "stopped_at_budget"
        assert manifest["endpoints"]["entry-picks"]["outcome"] == "FAILED"
        resumed = _client()
        assert _entry_picks(raw, resumed) == 0
        assert len(resumed.calls) == 4

    @pytest.mark.covers("#116 AC16")
    def test_a_budget_stop_that_captured_something_exits_zero_with_the_reason_recorded(self, tmp_path):
        raw = tmp_path / "raw"
        _write_panel(raw)

        # Both entries of gameweek 1 are in flight when the 0.05 s budget runs out and finish; gameweek 2 never starts.
        assert _entry_picks(raw, _client(delay_seconds=0.3), "--max-seconds", "0.05") == 0

        [manifest] = _manifests(raw)
        assert manifest["stop_reason"] == "stopped_at_budget"
        assert manifest["status"] == "SUCCESS"
        assert _marker(raw, 1).exists() and not _marker(raw, 2).exists()
        assert manifest["config"]["max_seconds"] == 0.05


class TestTrigger:
    """The manifest says what started the run, so a scheduled picks run is not recorded as manual."""

    @pytest.mark.covers("#104 AC4")
    @pytest.mark.parametrize("trigger", ["scheduled", "manual"])
    def test_the_manifest_records_the_trigger_given(self, tmp_path, trigger):
        raw = tmp_path / "raw"
        _write_panel(raw)

        assert _entry_picks(raw, _client(), "--trigger", trigger) == 0

        [manifest] = _manifests(raw)
        assert manifest["trigger"] == trigger

    @pytest.mark.covers("#104 AC4")
    def test_without_a_trigger_the_manifest_records_manual_as_before(self, tmp_path):
        raw = tmp_path / "raw"
        _write_panel(raw)

        assert _entry_picks(raw, _client()) == 0

        [manifest] = _manifests(raw)
        assert manifest["trigger"] == "manual"


class TestBudgetSetting:

    @pytest.mark.covers("#116 AC16")
    def test_flag_beats_env_beats_default(self, monkeypatch):
        monkeypatch.delenv("FPL_ENTRY_PICKS_MAX_SECONDS", raising=False)
        assert resolve_entry_picks_max_seconds() == DEFAULT_ENTRY_PICKS_MAX_SECONDS
        monkeypatch.setenv("FPL_ENTRY_PICKS_MAX_SECONDS", "120")
        assert resolve_entry_picks_max_seconds() == 120.0
        assert resolve_entry_picks_max_seconds(30.0) == 30.0

    @pytest.mark.covers("#116 AC16")
    @pytest.mark.parametrize("bad", ["0", "-5"])
    def test_a_non_positive_budget_is_rejected_rather_than_switching_the_guard_off(self, monkeypatch, bad):
        monkeypatch.setenv("FPL_ENTRY_PICKS_MAX_SECONDS", bad)

        with pytest.raises(ValueError):
            resolve_entry_picks_max_seconds()

    @pytest.mark.covers("#116 AC16")
    def test_the_default_budget_is_inside_the_one_hour_credential_window(self):
        assert 0 < DEFAULT_ENTRY_PICKS_MAX_SECONDS < 3600


class TestSettlementOrder:

    def _stage(self, root: Path, client, run_id: str, gameweeks, entries):
        return ingest_entry_picks(
            client, LocalRawWriter(root, "fpl", run_id=run_id), season=SEASON, entries=entries, gameweeks=gameweeks
        )

    @pytest.mark.covers("#116 AC17")
    @pytest.mark.asyncio
    async def test_a_gameweek_with_an_unresolved_entry_blocks_the_next_one_and_the_run_resumes_there(self, tmp_path):
        root = tmp_path / "raw"
        gws = [{"id": 1, "finished": True, "data_checked": True}, {"id": 2, "finished": True, "data_checked": True}]
        entries = [(RANDOM_A, COHORT_RANDOM), (RANDOM_B, COHORT_RANDOM)]
        calls: list[tuple[int, int]] = []
        refusing = {"on": True}

        async def _fetch(gw: int, entry_id: int) -> RawResponse:
            calls.append((gw, entry_id))
            if (gw, entry_id) == (1, RANDOM_B) and refusing["on"]:
                return _raw(403, b'{"detail":"forbidden"}')
            return _raw(200, payload_bytes("entry_picks_self"))

        client = MagicMock(get_entry_picks_raw=AsyncMock(side_effect=_fetch))

        await self._stage(root, client, "20261020T070000Z-a00001", gws, entries)

        assert {gw for gw, _ in calls} == {1}, "gameweek 2 must not start while gameweek 1 is unsettled"
        assert not _marker(root, 1).exists() and not _marker(root, 2).exists()

        calls.clear()
        refusing["on"] = False
        await self._stage(root, client, "20261020T080000Z-a00002", gws, entries)

        assert sorted(calls) == [(1, RANDOM_B), (2, RANDOM_A), (2, RANDOM_B)], (
            "the rerun fetches only the unresolved entry of gameweek 1, then gameweek 2"
        )
        assert _marker(root, 1).exists() and _marker(root, 2).exists()

    @pytest.mark.covers("#116 AC17")
    @pytest.mark.asyncio
    async def test_the_marker_is_written_only_when_every_kept_entry_is_captured(self, tmp_path):
        root = tmp_path / "raw"
        gws = [{"id": 1, "finished": True, "data_checked": True}]
        client = MagicMock(get_entry_picks_raw=AsyncMock(return_value=_raw(200, payload_bytes("entry_picks_self"))))

        await self._stage(root, client, "20261020T070000Z-a00003", gws, [(RANDOM_A, COHORT_RANDOM), (SELF_ID, COHORT_SELF)])

        marker = json.loads(_marker(root, 1).read_text())
        assert marker["event"] == 1 and marker["endpoint"] == "entry-picks" and marker["entries"] == 2
        assert not (root / "fpl" / "_settlement" / "event-live").exists()
        assert not (root / "fpl" / "_settlement" / "element-summary").exists()
