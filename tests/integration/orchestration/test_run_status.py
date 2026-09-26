"""Run status derived from what the run left usable, end to end.

Drives ``fpl_ingest.cli.main`` against ``tmp_path`` with the FPL client faked,
so the real stages, runner and writer produce the status and exit code under
test. Both full runs and pre-deadline runs are covered.
"""

from __future__ import annotations

import logging
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
from tests.support.run_helpers import (
    _bootstrap_with_deadline_in,
    _history_failing_for,
    _manifest,
    _seed_settled_gameweek,
)

_FIXTURES_URL = "https://fantasy.premierleague.com/api/fixtures/"


def _run(raw: Path, client, *argv: str) -> int:
    with patch("fpl_ingest.orchestration.runner.AsyncFPLClient", return_value=client):
        try:
            main(["--raw-dir", str(raw), *argv])
        except SystemExit as exc:
            return int(exc.code or 0)
    return 0


def _shape_invalid_fixtures(client) -> None:
    client.get_fixtures_raw = AsyncMock(return_value=_raw_response(_FIXTURES_URL, {"not": "a list"}))


def _fixtures_fetch_fails(client) -> None:
    # FPLClientError is what the client raises once its retries are exhausted.
    client.get_fixtures_raw = AsyncMock(side_effect=FPLClientError("fixtures 503 after retries"))


def _settled_policy_skip_only_client():
    """A daily run whose only non-fetches are deliberate under the refetch policy."""
    client = _make_async_client(
        bootstrap={**MINIMAL_BOOTSTRAP, "events": [event_row(id=5, finished=True, is_current=True)]}
    )
    client.get_event_status_raw = AsyncMock(return_value=_raw_event_status_response(
        {"status": [{"event": 5, "points": "r", "bonus_added": True, "date": "2026-09-26"}],
         "leagues": ""}
    ))
    return client


class TestEverythingUsableIsSuccess:

    @pytest.mark.covers("#48 AC1")
    def test_clean_daily_run_is_success_and_exits_zero(self, tmp_path):
        raw = tmp_path / "raw"
        assert _run(raw, _make_async_client()) == 0
        assert _manifest(raw)["status"] == "SUCCESS"

    @pytest.mark.covers("#48 AC1")
    def test_daily_run_whose_only_non_fetches_are_policy_skips_is_success(self, tmp_path):
        raw = tmp_path / "raw"
        _seed_settled_gameweek(raw, 5, [1, 2])
        client = _settled_policy_skip_only_client()

        assert _run(raw, client) == 0

        client.get_element_summary_raw.assert_not_awaited()
        client.get_gameweek_live_raw.assert_not_awaited()
        assert _manifest(raw)["status"] == "SUCCESS"

    @pytest.mark.covers("#48 AC1")
    def test_clean_pre_deadline_run_is_success_and_exits_zero(self, tmp_path):
        raw = tmp_path / "raw"
        client = _make_async_client(bootstrap=_bootstrap_with_deadline_in(60))
        assert _run(raw, client, "pre-deadline") == 0
        assert _manifest(raw)["status"] == "SUCCESS"


class TestSomethingUsableSomethingFailedIsPartial:

    @pytest.mark.covers("#48 AC2")
    def test_daily_run_with_fixtures_failing_after_retries_is_partial(self, tmp_path):
        raw = tmp_path / "raw"
        client = _make_async_client()
        _fixtures_fetch_fails(client)
        _run(raw, client)
        assert _manifest(raw)["status"] == "PARTIAL"

    @pytest.mark.covers("#48 AC2")
    def test_pre_deadline_run_with_fixtures_failing_is_partial(self, tmp_path):
        raw = tmp_path / "raw"
        client = _make_async_client(bootstrap=_bootstrap_with_deadline_in(60))
        _fixtures_fetch_fails(client)
        _run(raw, client, "pre-deadline")
        assert _manifest(raw)["status"] == "PARTIAL"

    @pytest.mark.covers("#48 AC2")
    def test_shape_invalid_payload_alongside_valid_captures_is_partial_and_is_kept(self, tmp_path):
        raw = tmp_path / "raw"
        client = _make_async_client()
        _shape_invalid_fixtures(client)
        _run(raw, client)

        assert sorted((raw / "fpl" / "fixtures").rglob("payload.json"))
        assert _manifest(raw)["status"] == "PARTIAL"

    @pytest.mark.covers("#48 AC2")
    @pytest.mark.parametrize("break_fixtures", [_fixtures_fetch_fails, _shape_invalid_fixtures],
                             ids=["fetch-failure", "shape-failure"])
    def test_shape_failure_and_fetch_failure_give_the_same_run_status(self, tmp_path, break_fixtures):
        raw = tmp_path / "raw"
        client = _make_async_client()
        break_fixtures(client)
        _run(raw, client)
        assert _manifest(raw)["status"] == "PARTIAL"

    @pytest.mark.covers("#48 AC2")
    def test_strict_run_aborting_after_usable_captures_is_partial_and_exits_non_zero(self, tmp_path):
        raw = tmp_path / "raw"
        client = _make_async_client()
        _fixtures_fetch_fails(client)

        assert _run(raw, client, "--strict") != 0

        client.get_element_summary_raw.assert_not_awaited()
        manifest = _manifest(raw)
        assert manifest["status"] == "PARTIAL"
        endpoints = manifest["endpoints"]
        assert endpoints["event-status"]["outcome"] == "SUCCESS"
        assert endpoints["bootstrap-static"]["outcome"] == "SUCCESS"
        for skipped in ("event-live", "element-summary"):
            entry = endpoints[skipped]
            assert (entry["attempted"], entry["outcome"]) == (0, "FAILED")
            reasons = " ".join(f["reason"] for f in entry["failures"])
            assert "not attempted" in reasons
            assert "fixtures" in reasons


class TestNothingUsableIsFailed:

    @pytest.mark.covers("#48 AC3")
    def test_run_where_every_fetch_fails_is_failed(self, tmp_path):
        raw = tmp_path / "raw"
        client = _make_async_client()
        down = FPLClientError("503 after retries")
        for method in ("get_event_status_raw", "get_bootstrap_raw", "get_fixtures_raw",
                       "get_gameweek_live_raw", "get_element_summary_raw"):
            setattr(client, method, AsyncMock(side_effect=down))

        _run(raw, client)

        assert _manifest(raw)["status"] == "FAILED"

    @pytest.mark.covers("#48 AC3")
    def test_run_whose_only_written_payload_fails_its_shape_check_is_failed(self, tmp_path):
        raw = tmp_path / "raw"
        client = _make_async_client()
        client.get_bootstrap_raw = AsyncMock(side_effect=FPLClientError("503 after retries"))
        _shape_invalid_fixtures(client)

        _run(raw, client, "pre-deadline", "--force")

        assert len(sorted((raw / "fpl").rglob("payload.json"))) == 1
        assert _manifest(raw)["status"] == "FAILED"


def _error_output(caplog) -> str:
    return "\n".join(r.getMessage() for r in caplog.records if r.levelno >= logging.ERROR)


class TestUnsuccessfulRunIsLoudAndSaysWhatFailed:

    @pytest.mark.covers("#48 AC4")
    def test_partial_run_exits_non_zero_and_names_each_failed_endpoint(self, tmp_path, caplog):
        caplog.set_level(logging.INFO)
        raw = tmp_path / "raw"
        client = _make_async_client()
        _fixtures_fetch_fails(client)

        assert _run(raw, client) != 0

        assert _manifest(raw)["status"] == "PARTIAL"
        output = _error_output(caplog)
        for failed in ("fixtures", "event-live", "element-summary"):
            assert failed in output, f"{failed} not named in:\n{output}"

    @pytest.mark.covers("#48 AC4")
    def test_partial_run_with_one_player_failing_names_element_summary(self, tmp_path, caplog):
        caplog.set_level(logging.INFO)
        raw = tmp_path / "raw"
        client = _make_async_client(history_side_effect=_history_failing_for(2))

        assert _run(raw, client) != 0

        assert _manifest(raw)["status"] == "PARTIAL"
        assert "element-summary" in _error_output(caplog)

    @pytest.mark.covers("#48 AC4")
    def test_failed_run_exits_non_zero_and_names_each_failed_endpoint(self, tmp_path, caplog):
        caplog.set_level(logging.INFO)
        raw = tmp_path / "raw"
        client = _make_async_client()
        client.get_bootstrap_raw = AsyncMock(side_effect=FPLClientError("503 after retries"))
        _shape_invalid_fixtures(client)

        assert _run(raw, client, "pre-deadline", "--force") != 0

        assert _manifest(raw)["status"] == "FAILED"
        output = _error_output(caplog)
        for failed in ("bootstrap-static", "fixtures"):
            assert failed in output, f"{failed} not named in:\n{output}"
