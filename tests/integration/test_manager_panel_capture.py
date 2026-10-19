"""The ``manager-panel`` command end to end (#102).

Drives ``fpl_ingest.cli.main`` against ``tmp_path`` with the FPL client faked
by real ``entry/{id}/history`` samples, and checks what lands in raw storage.
"""

from __future__ import annotations

import json
import re
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import AsyncMock, patch

import pytest

from fpl_ingest.cli import main
from fpl_ingest.extract.http.client import RawResponse
from fpl_ingest.extract.http.sync_http import FPLClientError
from tests.support.cli_fakes import MINIMAL_BOOTSTRAP, _make_async_client
from tests.support.fixture_payloads import payload_bytes

SEASON = "2026-27"
SELF_IDS = (8450051, 4685107)
BOOTSTRAP = {
    **MINIMAL_BOOTSTRAP,
    "total_players": 3000,
    "events": [
        {"id": 1, "finished": True, "data_checked": True, "is_current": False,
         "deadline_time": "2026-08-14T17:30:00Z"},
    ],
}
RUN_ID = r"\d{8}T\d{6}Z-[0-9a-f]{6}"
DATE = r"\d{4}-\d{2}-\d{2}"


def _history(entry_id: int, fixture: str) -> RawResponse:
    at = datetime(2026, 10, 19, 4, 30, tzinfo=timezone.utc)
    return RawResponse(
        url=f"https://fantasy.premierleague.com/api/entry/{entry_id}/history/",
        status=404 if fixture == "entry_history_not_found" else 200,
        headers={"content-type": "application/json"},
        body=payload_bytes(fixture),
        requested_at=at,
        received_at=at,
    )


def _client(*, fail_for: int | None = None):
    """Every id has picks except those divisible by 5 (no picks) or 7 (404)."""
    client = _make_async_client(bootstrap=BOOTSTRAP)

    async def _fetch(entry_id: int) -> RawResponse:
        if entry_id == fail_for:
            raise FPLClientError("503 after retries")
        if entry_id in SELF_IDS:
            return _history(entry_id, "entry_history_self")
        if entry_id % 7 == 0:
            return _history(entry_id, "entry_history_not_found")
        if entry_id % 5 == 0:
            return _history(entry_id, "entry_history_no_picks")
        return _history(entry_id, "entry_history_active")

    client.get_entry_history_raw = AsyncMock(side_effect=_fetch)
    return client


def _manager_panel(raw: Path, client, *extra: str) -> int:
    with patch("fpl_ingest.orchestration.runner.AsyncFPLClient", return_value=client):
        with pytest.raises(SystemExit) as exc:
            main(["--raw-dir", str(raw), "manager-panel", "--seed", "5", *extra])
    return int(exc.value.code or 0)


def _panel_path(raw: Path) -> Path:
    return raw / "fpl" / "_panels" / SEASON / "manager-panel.json"


def _manifests(raw: Path) -> list[dict]:
    return [json.loads(p.read_text()) for p in sorted((raw / "fpl" / "_manifests").rglob("manifest.json"))]


def _files_under(raw: Path, prefix: str) -> list[str]:
    return sorted(str(p.relative_to(raw)) for p in (raw / prefix).rglob("*") if p.is_file())


class TestLayout:

    @pytest.mark.covers("#102 AC5")
    def test_history_and_panel_land_at_exactly_the_decided_keys(self, tmp_path):
        raw = tmp_path / "raw"

        assert _manager_panel(raw, _client()) == 0

        panel = json.loads(_panel_path(raw).read_text())
        kept = {e["entry_id"] for e in panel["entries"]}
        history_files = _files_under(raw, "fpl/entry-history")
        pattern = re.compile(
            rf"^fpl/entry-history/{SEASON}/(\d+)/{DATE}/{RUN_ID}/(payload\.json|metadata\.json)$"
        )
        assert history_files and all(pattern.match(f) for f in history_files)
        assert {int(pattern.match(f).group(1)) for f in history_files} == kept
        assert _files_under(raw, "fpl/_panels") == [f"fpl/_panels/{SEASON}/manager-panel.json"]

    @pytest.mark.covers("#102 AC5")
    def test_every_history_capture_is_indexed_in_the_manifest(self, tmp_path):
        raw = tmp_path / "raw"

        assert _manager_panel(raw, _client()) == 0

        [manifest] = _manifests(raw)
        payloads = {f"raw/{f}" for f in _files_under(raw, "fpl/entry-history") if f.endswith("payload.json")}
        captured = {c["key"] for c in manifest["captures"] if c["key"].startswith("raw/fpl/entry-history/")}
        assert captured == payloads
        assert manifest["endpoints"]["entry-history"]["outcome"] == "SUCCESS"

    @pytest.mark.covers("#102 AC5")
    def test_ids_not_kept_have_no_history_stored(self, tmp_path):
        raw = tmp_path / "raw"

        assert _manager_panel(raw, _client()) == 0

        stored = {int(p.name) for p in (raw / "fpl" / "entry-history" / SEASON).iterdir()}
        assert not any(i % 7 == 0 or (i % 5 == 0 and i not in SELF_IDS) for i in stored)


class TestDrawnOnce:

    @pytest.mark.covers("#102 AC6")
    def test_a_second_run_draws_nothing_and_fetches_no_history(self, tmp_path):
        raw = tmp_path / "raw"
        assert _manager_panel(raw, _client()) == 0
        panel_before = _panel_path(raw).read_bytes()
        history_before = _files_under(raw, "fpl/entry-history")

        again = _client()
        assert _manager_panel(raw, again) == 0

        again.get_entry_history_raw.assert_not_awaited()
        assert _panel_path(raw).read_bytes() == panel_before
        assert _files_under(raw, "fpl/entry-history") == history_before


class TestFailsLoud:

    @pytest.mark.covers("#102 AC9")
    def test_a_non_404_history_failure_writes_no_panel_and_exits_non_zero(self, tmp_path):
        raw = tmp_path / "raw"

        assert _manager_panel(raw, _client(fail_for=SELF_IDS[0])) != 0

        assert not _panel_path(raw).exists()
        [manifest] = _manifests(raw)
        assert manifest["status"] != "SUCCESS"
        assert [f["endpoint"] for f in manifest["failures"]] == [f"entry-history/{SEASON}/{SELF_IDS[0]}"]

    @pytest.mark.covers("#102 AC9")
    def test_a_bootstrap_without_total_players_draws_nothing_and_exits_non_zero(self, tmp_path):
        raw = tmp_path / "raw"
        client = _client()
        client.get_bootstrap_raw = _make_async_client(
            bootstrap={k: v for k, v in BOOTSTRAP.items() if k != "total_players"}
        ).get_bootstrap_raw

        assert _manager_panel(raw, client) != 0

        assert not _panel_path(raw).exists()
        client.get_entry_history_raw.assert_not_awaited()
        [manifest] = _manifests(raw)
        assert manifest["status"] != "SUCCESS"
