"""Shared response classifier and router (#54 AC8)."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from fpl_ingest.extract.http.client import RawResponse
from fpl_ingest.extract.http.local_writer import LocalRawWriter
from fpl_ingest.extract.http.response_router import route_response

ENDPOINT = "synthetic/1"
PASSING = {"ok": True, "checks": ["top_level_is_list"], "failures": [], "record_count": 1}
FAILING = {"ok": False, "checks": ["top_level_is_list"], "failures": ["top_level_is_list: got dict"], "record_count": None}
INVALID_ROOT = "fpl/_invalid"


def _response(status: int, body: bytes = b"[]") -> RawResponse:
    requested_at = datetime(2026, 10, 19, 8, 0, 0, tzinfo=timezone.utc)
    return RawResponse(
        url="https://fantasy.premierleague.com/api/synthetic/1/",
        status=status,
        headers={"content-type": "application/json"},
        body=body,
        requested_at=requested_at,
        received_at=requested_at + timedelta(seconds=1),
    )


def _written_keys(root: Path) -> list[str]:
    """Every key under raw storage except the run manifest."""
    return sorted(
        path.relative_to(root).as_posix()
        for path in root.rglob("*")
        if path.is_file() and "_manifests" not in path.parts
    )


# (case, status, shape verdict, absence_ok) -> (class, where it is stored)
CASES = [
    pytest.param(200, PASSING, False, "valid", f"fpl/{ENDPOINT}/", id="200_passing_verdict"),
    pytest.param(200, FAILING, False, "shape_invalid", f"{INVALID_ROOT}/shape_invalid/{ENDPOINT}/", id="200_failing_verdict"),
    pytest.param(404, FAILING, True, "not_found", f"{INVALID_ROOT}/not_found/{ENDPOINT}/", id="404_absence_declared"),
    pytest.param(404, FAILING, False, "http_error", f"{INVALID_ROOT}/http_error/{ENDPOINT}/", id="404_absence_not_declared"),
    pytest.param(403, FAILING, False, "http_error", f"{INVALID_ROOT}/http_error/{ENDPOINT}/", id="403"),
    pytest.param(429, FAILING, False, "fetch_failed", None, id="429_after_retries"),
    pytest.param(503, FAILING, False, "fetch_failed", None, id="503_after_retries"),
]


@pytest.mark.covers("#54 AC8")
@pytest.mark.parametrize("status, shape, absence_ok, expected_class, expected_prefix", CASES)
def test_a_response_gets_its_class_and_is_stored_where_the_class_says(
    tmp_path, status, shape, absence_ok, expected_class, expected_prefix
):
    writer = LocalRawWriter(tmp_path / "raw", "fpl")

    routed = route_response(writer, ENDPOINT, _response(status), shape, absence_ok=absence_ok)

    assert routed.response_class == expected_class
    keys = _written_keys(tmp_path / "raw")
    if expected_prefix is None:
        assert keys == [], "a failed fetch stores nothing"
    else:
        assert keys and all(key.startswith(expected_prefix) for key in keys), keys


@pytest.mark.covers("#54 AC8")
def test_no_response_is_a_failed_fetch_that_stores_nothing(tmp_path):
    writer = LocalRawWriter(tmp_path / "raw", "fpl")

    routed = route_response(writer, ENDPOINT, None, None)

    assert routed.response_class == "fetch_failed"
    assert _written_keys(tmp_path / "raw") == []


@pytest.mark.covers("#54 AC8")
def test_absence_ok_is_read_per_call_so_one_endpoint_can_give_either_class(tmp_path):
    declared = route_response(
        LocalRawWriter(tmp_path / "a", "fpl"), ENDPOINT, _response(404), FAILING, absence_ok=True
    )
    not_declared = route_response(
        LocalRawWriter(tmp_path / "b", "fpl"), ENDPOINT, _response(404), FAILING, absence_ok=False
    )

    assert (declared.response_class, not_declared.response_class) == ("not_found", "http_error")


@pytest.mark.covers("#54 AC8")
def test_a_404_defaults_to_an_error_when_the_caller_says_nothing(tmp_path):
    routed = route_response(LocalRawWriter(tmp_path / "raw", "fpl"), "fixtures", _response(404), FAILING)

    assert routed.response_class == "http_error"
    assert all(key.startswith(f"{INVALID_ROOT}/http_error/fixtures/") for key in _written_keys(tmp_path / "raw"))
