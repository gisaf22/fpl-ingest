"""The shared fetch-classify-store-record path (#115 AC1)."""

from __future__ import annotations

import asyncio
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from fpl_ingest.extract.http.client import RawResponse
from fpl_ingest.extract.http.local_writer import LocalRawWriter
from fpl_ingest.extract.http.response_router import fetch_and_route
from fpl_ingest.extract.http.sync_http import FPLClientError

URL = "https://fantasy.premierleague.com/api/synthetic/1/"
ENDPOINT = "synthetic/1"
PASSING = {"ok": True, "checks": [], "failures": [], "record_count": 1}
FAILING = {"ok": False, "checks": [], "failures": ["top_level_is_list: got dict"], "record_count": None}
INVALID = "fpl/_invalid"


def _response(status: int) -> RawResponse:
    at = datetime(2026, 10, 19, 8, 0, 0, tzinfo=timezone.utc)
    return RawResponse(
        url=URL, status=status, headers={"content-type": "application/json"}, body=b"[]",
        requested_at=at, received_at=at + timedelta(seconds=1), attempt_count=3,
    )


def _route(tmp_path: Path, outcome, verdict, **declared):
    writer = LocalRawWriter(tmp_path / "raw", "fpl")

    async def fetch():
        if isinstance(outcome, Exception):
            raise outcome
        return outcome

    routed = asyncio.run(
        fetch_and_route(writer, ENDPOINT, fetch, lambda _raw: verdict, request_url=URL, **declared)
    )
    return writer, routed


def _keys(root: Path) -> list[str]:
    return sorted(str(p.relative_to(root)) for p in root.rglob("*") if p.is_file())


@pytest.mark.covers("#115 AC1")
@pytest.mark.parametrize(
    "status, verdict, declared, response_class, stored_under",
    [
        (200, PASSING, {}, "valid", f"fpl/{ENDPOINT}/"),
        (200, FAILING, {}, "shape_invalid", f"{INVALID}/shape_invalid/{ENDPOINT}/"),
        (403, FAILING, {}, "http_error", f"{INVALID}/http_error/{ENDPOINT}/"),
        (404, FAILING, {"absence_ok": True}, "not_found", f"{INVALID}/not_found/{ENDPOINT}/"),
        (429, FAILING, {}, "fetch_failed", None),
        (503, FAILING, {}, "fetch_failed", None),
    ],
)
def test_each_response_class_lands_where_its_class_calls_for(
    tmp_path, status, verdict, declared, response_class, stored_under
):
    writer, routed = _route(tmp_path, _response(status), verdict, **declared)

    assert routed.response_class == response_class
    keys = _keys(tmp_path / "raw")
    if stored_under is None:
        assert not any(k.endswith(("payload.json", "response.body")) for k in keys)
    else:
        assert keys and all(k.startswith(stored_under) for k in keys), keys


@pytest.mark.covers("#115 AC1")
@pytest.mark.parametrize("status, verdict, usable", [(200, PASSING, 1), (200, FAILING, 0), (403, FAILING, 0)])
def test_the_manifest_records_the_capture_with_the_same_fields_the_stages_record_today(
    tmp_path, status, verdict, usable
):
    writer, _ = _route(tmp_path, _response(status), verdict)

    manifest = writer.finalize("SUCCESS" if usable else "FAILED").manifest

    (entry,) = manifest["captures"]
    assert entry["usable"] is bool(usable)
    assert set(entry) >= {"key", "shape_ok", "usable", "season"}
    assert manifest["endpoints"]["synthetic"]["usable"] == usable


@pytest.mark.covers("#115 AC1")
def test_a_429_after_retries_stores_nothing_and_is_a_manifest_failure_with_its_url(tmp_path):
    writer, _ = _route(tmp_path, _response(429), FAILING)

    (failure,) = writer.finalize("FAILED").manifest["failures"]
    assert failure["request_url"] == URL
    assert failure["endpoint"] == ENDPOINT
    assert failure["error_class"] == "FetchFailed"


@pytest.mark.covers("#115 AC1")
def test_a_request_that_got_no_response_is_a_manifest_failure_with_url_and_error_class(tmp_path):
    writer, routed = _route(tmp_path, FPLClientError("connection reset"), None)

    assert routed.response_class == "fetch_failed"
    assert not [k for k in _keys(tmp_path / "raw") if k.endswith(("payload.json", "response.body"))]
    (failure,) = writer.finalize("FAILED").manifest["failures"]
    assert failure["request_url"] == URL
    assert failure["error_class"] == "FPLClientError"


@pytest.mark.covers("#115 AC1")
@pytest.mark.parametrize("status, verdict, declared", [(200, FAILING, {}), (403, FAILING, {}), (404, FAILING, {"absence_ok": True})])
def test_with_nothing_declared_expected_every_invalid_class_marks_the_endpoint_failed(
    tmp_path, status, verdict, declared
):
    writer, _ = _route(tmp_path, _response(status), verdict, **declared)

    endpoint = writer.finalize("FAILED").manifest["endpoints"]["synthetic"]
    assert (endpoint["failed"], endpoint["outcome"]) == (1, "FAILED")
