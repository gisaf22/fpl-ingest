"""A run aborts after consecutive ``http_error`` responses (#109 AC1-AC3)."""

from __future__ import annotations

import itertools
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from fpl_ingest import config as config_module
from fpl_ingest.cli import build_parser
from fpl_ingest.config import resolve_http_error_streak
from fpl_ingest.extract.http.client import RawResponse
from fpl_ingest.extract.http.local_writer import LocalRawWriter
from fpl_ingest.extract.http.response_router import route_response

PASSING = {"ok": True, "checks": [], "failures": [], "record_count": 1}
FAILING = {"ok": False, "checks": [], "failures": ["top_level_is_list: got dict"], "record_count": None}
ORIGIN = {"kind": "local", "workflow": None, "ref": None, "github_run_id": None, "aws_principal": None}

_ENDPOINT_IDS = itertools.count()
FORBIDDEN = (403, FAILING, False)
NOT_FOUND = (404, FAILING, True)
VALID = (200, PASSING, False)
SHAPE_INVALID = (200, FAILING, False)
FETCH_FAILED = (503, FAILING, False)


def _response(status: int) -> RawResponse:
    requested_at = datetime(2026, 10, 19, 8, 0, 0, tzinfo=timezone.utc)
    return RawResponse(
        url="https://fantasy.premierleague.com/api/synthetic/1/",
        status=status,
        headers={"content-type": "application/json"},
        body=b"[]",
        requested_at=requested_at,
        received_at=requested_at + timedelta(seconds=1),
    )


def _writer(tmp_path: Path, **kwargs) -> LocalRawWriter:
    return LocalRawWriter(tmp_path / "raw", "fpl", origin=ORIGIN, **kwargs)


def _feed(writer: LocalRawWriter, steps: list[tuple]) -> int | None:
    """Route each step in order; return the 1-based step the run aborted on, if any."""
    aborted_on = None
    for number, (status, shape, absence_ok) in enumerate(steps, start=1):
        # A fresh endpoint per call: the writer refuses a second object at one key.
        endpoint = f"synthetic/{next(_ENDPOINT_IDS)}"
        route_response(writer, endpoint, _response(status), shape, absence_ok=absence_ok)
        if aborted_on is None and writer.aborted:
            aborted_on = number
    return aborted_on


@pytest.mark.covers("#109 AC1")
def test_run_aborts_after_the_default_number_of_consecutive_http_errors(tmp_path):
    writer = _writer(tmp_path)

    assert _feed(writer, [FORBIDDEN] * 9) is None
    assert _feed(writer, [FORBIDDEN]) == 1


@pytest.mark.covers("#109 AC1")
def test_responses_already_in_flight_are_still_stored_after_the_abort(tmp_path):
    writer = _writer(tmp_path)
    _feed(writer, [FORBIDDEN] * 10)
    assert writer.aborted

    route_response(writer, "synthetic/in-flight", _response(403), FAILING)

    stored = list((tmp_path / "raw" / "fpl" / "_invalid" / "http_error").rglob("response.body"))
    assert len(stored) == 11


@pytest.mark.covers("#109 AC1")
def test_finalized_manifest_names_the_abort_reason(tmp_path):
    writer = _writer(tmp_path)
    _feed(writer, [FORBIDDEN] * 10)

    manifest = writer.finalize("FAILED").manifest

    assert manifest["abort_reason"] == "aborted_http_error_streak"


@pytest.mark.covers("#109 AC1")
def test_run_that_never_aborted_carries_no_abort_reason(tmp_path):
    writer = _writer(tmp_path)
    _feed(writer, [FORBIDDEN] * 9)

    manifest = writer.finalize("FAILED").manifest

    assert manifest.get("abort_reason") is None


@pytest.mark.parametrize(
    ("steps", "aborts_on"),
    [
        pytest.param([FORBIDDEN] * 10, 10, id="ten_straight_403s_abort_on_the_tenth"),
        pytest.param([FORBIDDEN] * 9 + [NOT_FOUND] + [FORBIDDEN], 11, id="declared_404_mid_streak_does_not_reset"),
        pytest.param([FORBIDDEN] * 9 + [NOT_FOUND], None, id="declared_404_does_not_count"),
        pytest.param([FORBIDDEN] * 9 + [VALID] + [FORBIDDEN], None, id="2xx_mid_streak_resets"),
        pytest.param([FORBIDDEN] * 5 + [VALID] + [FORBIDDEN] * 10, 16, id="streak_restarts_after_a_2xx"),
        pytest.param([FORBIDDEN] * 9 + [SHAPE_INVALID] + [FORBIDDEN], 11, id="shape_invalid_neither_counts_nor_resets"),
        pytest.param([FORBIDDEN] * 9 + [FETCH_FAILED] + [FORBIDDEN], 11, id="fetch_failed_neither_counts_nor_resets"),
    ],
)
@pytest.mark.covers("#109 AC2")
def test_streak_counts_only_http_errors(tmp_path, steps, aborts_on):
    assert _feed(_writer(tmp_path), steps) == aborts_on


@pytest.mark.covers("#109 AC3")
def test_configured_threshold_replaces_the_default(tmp_path):
    writer = _writer(tmp_path, http_error_streak=3)

    assert _feed(writer, [FORBIDDEN] * 3) == 3


@pytest.fixture
def _no_config_file(tmp_path, monkeypatch):
    monkeypatch.setattr(config_module, "_CONFIG_FILE", tmp_path / "config.yaml")
    monkeypatch.delenv("FPL_HTTP_ERROR_STREAK", raising=False)


@pytest.mark.covers("#109 AC3")
def test_threshold_defaults_to_ten(_no_config_file):
    assert resolve_http_error_streak() == 10


@pytest.mark.covers("#109 AC3")
def test_env_var_sets_the_threshold(_no_config_file, monkeypatch):
    monkeypatch.setenv("FPL_HTTP_ERROR_STREAK", "4")

    assert resolve_http_error_streak() == 4


@pytest.mark.covers("#109 AC3")
def test_flag_overrides_the_env_var(_no_config_file, monkeypatch):
    monkeypatch.setenv("FPL_HTTP_ERROR_STREAK", "4")

    args = build_parser().parse_args(["run", "--http-error-streak", "2"])

    assert resolve_http_error_streak(args.http_error_streak) == 2


@pytest.mark.covers("#109 AC7")
def test_abort_logs_the_streak_length_threshold_and_last_status(tmp_path, caplog):
    writer = _writer(tmp_path, http_error_streak=4)

    with caplog.at_level("WARNING"):
        _feed(writer, [FORBIDDEN] * 4)

    lines = [r.getMessage() for r in caplog.records if "aborted_http_error_streak" in r.getMessage()]
    assert len(lines) == 1
    assert "streak=4" in lines[0]
    assert "threshold=4" in lines[0]
    assert "last_status=403" in lines[0]


@pytest.mark.covers("#109 AC7")
def test_abort_is_logged_once_however_many_responses_follow(tmp_path, caplog):
    writer = _writer(tmp_path, http_error_streak=2)

    with caplog.at_level("WARNING"):
        _feed(writer, [FORBIDDEN] * 5)

    assert len([r for r in caplog.records if "aborted_http_error_streak" in r.getMessage()]) == 1
