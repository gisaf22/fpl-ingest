"""Expected outcomes and the http_error streak (#115 AC11)."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from itertools import count

import pytest

from fpl_ingest.extract.http.client import RawResponse
from fpl_ingest.extract.http.local_writer import LocalRawWriter
from fpl_ingest.extract.http.response_router import route_response

PASSING = {"ok": True, "checks": [], "failures": [], "record_count": 1}
NO_PICKS = {"ok": False, "checks": [], "failures": ["current_is_empty"], "record_count": None, "outcome": "no_picks"}
_IDS = count()


def _response(status: int) -> RawResponse:
    at = datetime(2026, 10, 19, 8, 0, 0, tzinfo=timezone.utc)
    return RawResponse(url="https://x/api/e/1/", status=status, headers={}, body=b"{}",
                       requested_at=at, received_at=at + timedelta(seconds=1))


def _writer(tmp_path, streak: int = 10) -> LocalRawWriter:
    return LocalRawWriter(tmp_path / "raw", "fpl", http_error_streak=streak)


def _send(writer, status, shape=NO_PICKS, **declared):
    return route_response(writer, f"e/{next(_IDS)}", _response(status), shape, **declared)


EXPECTED_404 = {"status": 404, "absence_ok": True, "expected": frozenset({"not_found"})}
EXPECTED_EMPTY = {"status": 200, "expected": frozenset({"no_picks"})}


@pytest.mark.covers("#115 AC11")
@pytest.mark.parametrize("declared", [EXPECTED_404, EXPECTED_EMPTY], ids=["404", "empty_current"])
def test_an_expected_response_between_errors_neither_counts_nor_resets_the_streak(tmp_path, declared):
    writer = _writer(tmp_path)
    for _ in range(9):
        _send(writer, 403)
    assert not writer.aborted

    _send(writer, **declared)
    assert not writer.aborted, "an expected response must not count"
    _send(writer, 403)

    assert writer.aborted, "the tenth http_error aborts: the expected one did not reset the streak"


@pytest.mark.covers("#115 AC11")
@pytest.mark.parametrize("declared", [EXPECTED_404, EXPECTED_EMPTY], ids=["404", "empty_current"])
def test_any_number_of_expected_responses_leaves_the_streak_at_zero(tmp_path, declared):
    writer = _writer(tmp_path, streak=3)

    for _ in range(12):
        _send(writer, **declared)

    assert not writer.aborted
