"""Per-endpoint outcomes in the run manifest, end to end.

Drives ``fpl_ingest.cli.main`` against ``tmp_path`` with the FPL client faked,
so the real stages, runner and writer produce the manifest under test.
"""

from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import AsyncMock, patch

import pytest

from fpl_ingest.cli import main
from fpl_ingest.extract.http.sync_http import FPLClientError
from tests.factories import event_row
from tests.support.cli_fakes import (
    MINIMAL_BOOTSTRAP,
    _make_async_client,
    _raw_event_status_response,
    _raw_response,
)
from tests.support.run_helpers import _history_failing_for, _manifest, _seed_settled_gameweek

_OUTCOMES = {"SUCCESS", "PARTIAL", "FAILED"}
# Sidecar fields that legitimately differ between two runs of the same capture.
_PER_RUN_SIDECAR_FIELDS = {"run_id", "extraction_date", "requested_at", "received_at"}


def _full_run(raw: Path, client) -> int:
    with patch("fpl_ingest.orchestration.runner.AsyncFPLClient", return_value=client):
        try:
            main(["--raw-dir", str(raw)])
        except SystemExit as exc:
            return int(exc.code or 0)
    return 0


def _endpoint(manifest: dict, name: str) -> dict:
    endpoints = manifest.get("endpoints")
    assert isinstance(endpoints, dict), "manifest has no per-endpoint block"
    assert name in endpoints, f"{name} missing from the record: {sorted(endpoints)}"
    return endpoints[name]


def _assert_consistent(entry: dict) -> None:
    """The outcome agrees with the counts, and every failure carries a reason.

    One rule, shared with run status: SUCCESS when everything attempted is usable,
    PARTIAL when some is usable and some failed, FAILED when nothing is usable.
    """
    assert entry["outcome"] in _OUTCOMES
    assert entry["attempted"] == entry["usable"] + entry["failed"]
    if entry["attempted"] and entry["usable"] == entry["attempted"]:
        assert entry["outcome"] == "SUCCESS"
    elif entry["usable"] > 0:
        assert entry["outcome"] == "PARTIAL"
    else:
        assert entry["outcome"] == "FAILED"
    assert len(entry["failures"]) >= entry["failed"]
    assert all(f.get("reason") for f in entry["failures"])
    if entry["outcome"] != "SUCCESS":
        assert entry["failures"], "a failed or partial endpoint must say why"


class TestEachEndpointHasAnOutcome:

    @pytest.mark.covers("#49 AC1")
    def test_clean_run_marks_every_captured_endpoint_successful(self, tmp_path):
        raw = tmp_path / "raw"
        assert _full_run(raw, _make_async_client()) == 0

        manifest = _manifest(raw)
        for name, count in [
            ("event-status", 1), ("bootstrap-static", 1), ("fixtures", 1), ("element-summary", 2),
        ]:
            entry = _endpoint(manifest, name)
            _assert_consistent(entry)
            assert (entry["attempted"], entry["usable"], entry["failed"]) == (count, count, 0)
            assert entry["outcome"] == "SUCCESS"

    @pytest.mark.covers("#49 AC1")
    def test_some_players_failing_marks_element_summary_partial(self, tmp_path):
        raw = tmp_path / "raw"
        _full_run(raw, _make_async_client(history_side_effect=_history_failing_for(2)))

        entry = _endpoint(_manifest(raw), "element-summary")
        _assert_consistent(entry)
        assert (entry["attempted"], entry["usable"], entry["failed"]) == (2, 1, 1)
        assert entry["outcome"] == "PARTIAL"
        assert any("player 2 unreachable" in f["reason"] for f in entry["failures"])

    @pytest.mark.covers("#49 AC1")
    def test_every_player_failing_marks_element_summary_failed(self, tmp_path):
        raw = tmp_path / "raw"
        _full_run(raw, _make_async_client(history_side_effect=_history_failing_for(1, 2)))

        entry = _endpoint(_manifest(raw), "element-summary")
        _assert_consistent(entry)
        assert (entry["attempted"], entry["usable"], entry["failed"]) == (2, 0, 2)
        assert entry["outcome"] == "FAILED"

    @pytest.mark.covers("#49 AC1")
    def test_shape_invalid_fixtures_is_stored_but_marked_failed(self, tmp_path):
        raw = tmp_path / "raw"
        client = _make_async_client()
        client.get_fixtures_raw = AsyncMock(
            return_value=_raw_response("https://fantasy.premierleague.com/api/fixtures/", {"not": "a list"})
        )
        _full_run(raw, client)

        # Still written, as today.
        assert sorted((raw / "fpl" / "fixtures").rglob("payload.json"))
        entry = _endpoint(_manifest(raw), "fixtures")
        _assert_consistent(entry)
        assert (entry["attempted"], entry["usable"], entry["failed"]) == (1, 0, 1)
        assert entry["outcome"] == "FAILED"
        reasons = " ".join(f["reason"] for f in entry["failures"])
        assert "shape" in reasons.lower()
        assert "top_level_is_list" in reasons

    @pytest.mark.covers("#49 AC1")
    def test_fixtures_fetch_failure_marks_fixtures_failed_with_the_error(self, tmp_path):
        raw = tmp_path / "raw"
        client = _make_async_client()
        client.get_fixtures_raw = AsyncMock(side_effect=FPLClientError("fixtures unreachable"))
        _full_run(raw, client)

        entry = _endpoint(_manifest(raw), "fixtures")
        _assert_consistent(entry)
        assert (entry["attempted"], entry["usable"], entry["failed"]) == (1, 0, 1)
        assert entry["outcome"] == "FAILED"
        assert any("fixtures unreachable" in f["reason"] for f in entry["failures"])

    @pytest.mark.covers("#49 AC1")
    @pytest.mark.parametrize("skipped_endpoint", ["event-live", "element-summary"])
    def test_endpoint_skipped_after_an_earlier_stage_failed_is_recorded_as_not_attempted(
        self, tmp_path, skipped_endpoint
    ):
        raw = tmp_path / "raw"
        client = _make_async_client()
        client.get_fixtures_raw = AsyncMock(side_effect=FPLClientError("fixtures unreachable"))
        _full_run(raw, client)

        client.get_element_summary_raw.assert_not_awaited()
        entry = _endpoint(_manifest(raw), skipped_endpoint)
        _assert_consistent(entry)
        assert entry["attempted"] == 0
        assert entry["outcome"] == "FAILED"
        reasons = " ".join(f["reason"] for f in entry["failures"]).lower()
        assert "not attempted" in reasons
        assert "fixtures" in reasons


def _sidecar_and_payload(raw: Path, endpoint: str) -> tuple[dict, bytes, tuple[str, ...]]:
    [payload] = sorted((raw / "fpl" / endpoint).rglob("payload.json"))
    sidecar = json.loads((payload.parent / "metadata.json").read_text())
    for name in _PER_RUN_SIDECAR_FIELDS:
        sidecar.pop(name)
    # Key layout with the per-run segments (extraction_date, run_id) removed.
    layout = payload.relative_to(raw).parts[:-3] + payload.relative_to(raw).parts[-1:]
    return sidecar, payload.read_bytes(), layout


class TestGoodCapturesAreUnaffectedByOtherFailures:

    @pytest.mark.covers("#49 AC2")
    @pytest.mark.parametrize(
        ("break_run", "good_endpoints"),
        [
            pytest.param(
                lambda c: setattr(c, "get_fixtures_raw", AsyncMock(side_effect=FPLClientError("down"))),
                ["event-status", "bootstrap-static"],
                id="fixtures-fetch-fails",
            ),
            pytest.param(
                lambda c: setattr(
                    c, "get_element_summary_raw",
                    _make_async_client(history_side_effect=_history_failing_for(2)).get_element_summary_raw,
                ),
                ["element-summary/1", "fixtures"],
                id="one-player-fails",
            ),
        ],
    )
    def test_usable_capture_is_stored_and_described_as_in_a_clean_run(
        self, tmp_path, break_run, good_endpoints
    ):
        clean_raw, broken_raw = tmp_path / "clean", tmp_path / "broken"
        _full_run(clean_raw, _make_async_client())
        broken_client = _make_async_client()
        break_run(broken_client)
        _full_run(broken_raw, broken_client)

        clean_manifest, broken_manifest = _manifest(clean_raw), _manifest(broken_raw)
        for endpoint in good_endpoints:
            assert _sidecar_and_payload(broken_raw, endpoint) == _sidecar_and_payload(clean_raw, endpoint)
            name = endpoint.split("/")[0]
            if name != "element-summary":
                assert _endpoint(broken_manifest, name) == _endpoint(clean_manifest, name)
                assert _endpoint(broken_manifest, name)["outcome"] == "SUCCESS"


class TestDeliberateNonFetchIsNotAFailure:

    @staticmethod
    def _assert_not_a_failure(manifest: dict, name: str) -> None:
        entry = manifest.get("endpoints", {}).get(name)
        if entry is not None:
            assert entry["outcome"] not in {"FAILED", "PARTIAL"}, entry
            assert entry["failed"] == 0
        assert not any(f["endpoint"].split("/")[0] == name for f in manifest["failures"])

    @pytest.mark.covers("#49 AC3")
    def test_unratified_gameweek_event_live_not_fetched_is_not_a_failure(self, tmp_path):
        raw = tmp_path / "raw"
        client = _make_async_client(
            bootstrap={**MINIMAL_BOOTSTRAP, "events": [event_row(id=5, finished=False, is_current=True)]}
        )
        client.get_event_status_raw = AsyncMock(return_value=_raw_event_status_response(
            {"status": [{"event": 5, "points": "p", "bonus_added": False, "date": "2026-09-26"}],
             "leagues": ""}
        ))
        assert _full_run(raw, client) == 0

        client.get_gameweek_live_raw.assert_not_awaited()
        manifest = _manifest(raw)
        assert "endpoints" in manifest
        self._assert_not_a_failure(manifest, "event-live")

    @pytest.mark.covers("#49 AC3")
    def test_already_settled_players_not_refetched_is_not_a_failure(self, tmp_path):
        raw = tmp_path / "raw"
        _seed_settled_gameweek(raw, 5, [1, 2])
        client = _make_async_client(
            bootstrap={**MINIMAL_BOOTSTRAP, "events": [event_row(id=5, finished=True, is_current=True)]}
        )
        client.get_event_status_raw = AsyncMock(return_value=_raw_event_status_response(
            {"status": [{"event": 5, "points": "r", "bonus_added": True, "date": "2026-09-26"}],
             "leagues": ""}
        ))
        assert _full_run(raw, client) == 0

        client.get_element_summary_raw.assert_not_awaited()
        client.get_gameweek_live_raw.assert_not_awaited()
        manifest = _manifest(raw)
        assert "endpoints" in manifest
        self._assert_not_a_failure(manifest, "element-summary")
        self._assert_not_a_failure(manifest, "event-live")
