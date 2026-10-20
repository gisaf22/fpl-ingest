"""The manager panel on the shared router, and the abort on its draw (#115 AC3-AC5, AC8-AC10).

Drives ``fpl_ingest.cli.main`` against ``tmp_path`` with only the FPL client faked,
as ``test_manager_panel_capture`` does.
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import AsyncMock

import pytest

from fpl_ingest.extract.http.client import RawResponse
from fpl_ingest.extract.http.sync_http import FPLClientError
from tests.integration.test_manager_panel_capture import (
    BOOTSTRAP,
    SELF_IDS,
    SEASON,
    _files_under,
    _history,
    _manager_panel,
    _manifests,
    _panel_path,
)
from tests.support.cli_fakes import _make_async_client

RANDOM_ID = 11  # in range (1..300), and not a self id
OTHER_IDS = 12


def _status(entry_id: int, status: int, body: bytes = b"{}") -> RawResponse:
    at = datetime(2026, 10, 19, 4, 30, tzinfo=timezone.utc)
    return RawResponse(
        url=f"https://fantasy.premierleague.com/api/entry/{entry_id}/history/", status=status,
        headers={"content-type": "application/json"}, body=body, requested_at=at, received_at=at,
        attempt_count=3 if status in (429, 500) else 1,
    )


def _client(special: dict[int, object]):
    """Every id captures cleanly (picks) except those in ``special``: a RawResponse, a fixture name, or an exception."""
    client = _make_async_client(bootstrap=BOOTSTRAP)
    calls: list[int] = []

    async def _fetch(entry_id: int) -> RawResponse:
        calls.append(entry_id)
        what = special.get(entry_id)
        if isinstance(what, Exception):
            raise what
        if isinstance(what, RawResponse):
            return what
        if isinstance(what, str):
            return _history(entry_id, what)
        return _history(entry_id, "entry_history_self" if entry_id in SELF_IDS else "entry_history_active")

    client.get_entry_history_raw = AsyncMock(side_effect=_fetch)
    client.calls = calls
    return client


def _under(raw: Path, prefix: str) -> list[str]:
    return _files_under(raw, prefix)


def _panel(raw: Path) -> dict:
    return json.loads(_panel_path(raw).read_text())


def _run(tmp_path, special):
    raw = tmp_path / "raw"
    code = _manager_panel(raw, _client(special))
    [manifest] = _manifests(raw)
    return raw, code, manifest


class TestEveryClassIsStoredWhereItBelongs:

    @pytest.mark.covers("#115 AC3")
    def test_a_random_404_is_stored_as_not_found_and_nothing_is_at_payload_json(self, tmp_path):
        raw, _, _ = _run(tmp_path, {RANDOM_ID: "entry_history_not_found"})

        assert any(f"/{RANDOM_ID}/" in f for f in _under(raw, "fpl/_invalid/not_found"))
        assert not any(f"/{RANDOM_ID}/" in f for f in _under(raw, "fpl/entry-history"))

    @pytest.mark.covers("#115 AC3")
    def test_a_self_404_and_a_403_are_stored_as_http_error(self, tmp_path):
        raw, _, _ = _run(tmp_path, {SELF_IDS[0]: "entry_history_not_found", RANDOM_ID: _status(RANDOM_ID, 403)})

        stored = " ".join(_under(raw, "fpl/_invalid/http_error"))
        assert f"/{SELF_IDS[0]}/" in stored and f"/{RANDOM_ID}/" in stored
        assert not any(f"/{SELF_IDS[0]}/" in f or f"/{RANDOM_ID}/" in f for f in _under(raw, "fpl/entry-history"))

    @pytest.mark.covers("#115 AC3")
    def test_a_shape_invalid_200_is_stored_as_shape_invalid(self, tmp_path):
        raw, _, _ = _run(tmp_path, {RANDOM_ID: _status(RANDOM_ID, 200, b'{"chips": []}')})

        assert any(f"/{RANDOM_ID}/" in f for f in _under(raw, "fpl/_invalid/shape_invalid"))

    @pytest.mark.covers("#115 AC3")
    def test_a_429_after_retries_stores_nothing_and_is_a_failed_fetch(self, tmp_path):
        raw, _, manifest = _run(tmp_path, {RANDOM_ID: _status(RANDOM_ID, 429)})

        assert not any(f"/{RANDOM_ID}/" in f for f in _under(raw, "fpl"))
        failure = next(f for f in manifest["failures"] if f["endpoint"].endswith(f"/{RANDOM_ID}"))
        assert failure["error_class"] == "FetchFailed"

    @pytest.mark.covers("#115 AC3")
    @pytest.mark.parametrize("special", [
        {RANDOM_ID: "entry_history_not_found"},   # expected, so the panel is still written
        {OTHER_IDS: "entry_history_no_picks"},
    ], ids=["random_404", "random_empty_current"])
    def test_the_panel_file_is_written_when_only_expected_outcomes_were_dropped(self, tmp_path, special):
        raw, code, _ = _run(tmp_path, special)

        assert code == 0 and _panel_path(raw).exists()

    @pytest.mark.covers("#115 AC3")
    @pytest.mark.parametrize("failing", [
        {RANDOM_ID: _status(RANDOM_ID, 403)},
        {RANDOM_ID: _status(RANDOM_ID, 429)},
        {SELF_IDS[0]: "entry_history_not_found"},
        {RANDOM_ID: _status(RANDOM_ID, 200, b'{"chips": []}')},
    ], ids=["403", "429", "self_404", "shape_invalid"])
    def test_the_panel_file_is_not_written_while_any_other_id_failed(self, tmp_path, failing):
        raw, code, _ = _run(tmp_path, failing)

        assert code != 0 and not _panel_path(raw).exists()


class TestResume:

    @pytest.mark.covers("#115 AC4")
    def test_a_rerun_skips_kept_managers_refetches_the_404_and_writes_the_panel(self, tmp_path):
        raw = tmp_path / "raw"
        assert _manager_panel(raw, _client({RANDOM_ID: "entry_history_not_found", OTHER_IDS: _status(OTHER_IDS, 403)})) != 0
        assert not _panel_path(raw).exists()

        again = _client({RANDOM_ID: "entry_history_not_found"})
        assert _manager_panel(raw, again) == 0

        assert 1 not in again.calls, "a kept manager is not requested again"
        assert RANDOM_ID in again.calls, "an id that 404'd is requested again, as today"
        assert _panel_path(raw).exists()


class TestAbortOnAStreakOfErrors:

    @pytest.mark.covers("#115 AC5")
    def test_the_draw_stops_dispatching_aborts_exits_non_zero_and_writes_no_panel(self, tmp_path):
        everything_403 = {i: _status(i, 403) for i in range(1, 301)} | {i: _status(i, 403) for i in SELF_IDS}
        client = _client(everything_403)
        raw = tmp_path / "raw"

        assert _manager_panel(raw, client) != 0

        [manifest] = _manifests(raw)
        assert manifest["abort_reason"] == "aborted_http_error_streak"
        assert 10 <= len(client.calls) < 300, "no request is dispatched after the Nth"
        assert not _panel_path(raw).exists()
        stored = {m.split("/")[4] for m in _under(raw, "fpl/_invalid/http_error") if m.endswith("response.body")}
        assert len(stored) == len(client.calls), "responses in flight were still stored"

    @pytest.mark.covers("#115 AC5")
    def test_a_rerun_requests_the_affected_ids_again(self, tmp_path):
        raw = tmp_path / "raw"
        blocked = {i: _status(i, 403) for i in range(1, 301)}
        assert _manager_panel(raw, _client(blocked)) != 0

        again = _client({})
        assert _manager_panel(raw, again) == 0

        assert {1, 2, 3} <= set(again.calls)


class TestExpectedDrops:

    @pytest.mark.covers("#115 AC8")
    def test_a_random_empty_current_is_a_visible_normal_drop(self, tmp_path):
        client = _client({RANDOM_ID: "entry_history_no_picks"})
        raw = tmp_path / "raw"

        assert _manager_panel(raw, client) == 0

        [manifest] = _manifests(raw)
        panel = _panel(raw)
        assert any(f"/{RANDOM_ID}/" in f for f in _under(raw, "fpl/_invalid/shape_invalid"))
        assert not any(f"/{RANDOM_ID}/" in f for f in _under(raw, "fpl/entry-history"))
        assert RANDOM_ID not in {e["entry_id"] for e in panel["entries"]}
        assert panel["drawn_not_kept"]["no_picks"] == 1
        assert client.calls.count(RANDOM_ID) == 1
        assert manifest["invalid_counts"]["shape_invalid"] == 1
        assert manifest["status"] == "SUCCESS"

    @pytest.mark.covers("#115 AC8")
    def test_the_same_empty_history_in_the_self_cohort_is_stored_and_kept(self, tmp_path):
        raw, code, _ = _run(tmp_path, {SELF_IDS[0]: "entry_history_no_picks"})

        assert code == 0
        assert SELF_IDS[0] in {e["entry_id"] for e in _panel(raw)["entries"]}
        assert any(f"/{SELF_IDS[0]}/" in f and f.endswith("payload.json") for f in _under(raw, "fpl/entry-history"))

    @pytest.mark.covers("#115 AC10")
    def test_a_random_404_is_a_visible_normal_drop(self, tmp_path):
        raw, code, manifest = _run(tmp_path, {RANDOM_ID: "entry_history_not_found"})

        assert code == 0
        assert any(f"/{RANDOM_ID}/" in f for f in _under(raw, "fpl/_invalid/not_found"))
        assert manifest["invalid_counts"]["not_found"] == 1
        assert manifest["endpoints"]["entry-history"]["outcome"] == "SUCCESS"
        assert manifest["status"] == "SUCCESS"
        assert _panel(raw)["drawn_not_kept"]["not_found"] == 1


class TestUndeclaredOutcomesStillAlert:

    @pytest.mark.covers("#115 AC9")
    @pytest.mark.parametrize("special, stored_under", [
        ({RANDOM_ID: lambda: _status(RANDOM_ID, 403)}, "fpl/_invalid/http_error"),
        ({RANDOM_ID: lambda: _status(RANDOM_ID, 200, b'{"chips": []}')}, "fpl/_invalid/shape_invalid"),
        ({RANDOM_ID: lambda: _status(RANDOM_ID, 500)}, None),
        ({SELF_IDS[0]: lambda: _status(SELF_IDS[0], 404)}, "fpl/_invalid/http_error"),
    ], ids=["random_403", "random_malformed", "random_500", "self_404"])
    def test_each_marks_the_endpoint_failed_exits_non_zero_and_writes_no_panel(self, tmp_path, special, stored_under):
        raw, code, manifest = _run(tmp_path, {k: v() for k, v in special.items()})

        assert code != 0
        assert manifest["status"] in ("PARTIAL", "FAILED")
        assert manifest["endpoints"]["entry-history"]["failed"] >= 1
        assert not _panel_path(raw).exists()
        key = next(iter(special))
        found = [f for f in _under(raw, "fpl/_invalid") if f"/{key}/" in f]
        assert bool(found) is (stored_under is not None)
        if stored_under:
            assert all(f.startswith(stored_under) for f in found)
