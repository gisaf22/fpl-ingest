"""Per-class invalid counts and the streak abort, through real components (#109 AC4, AC5).

AC4 drives the router and writer directly. AC5 drives ``fpl_ingest.cli.main`` with only
the FPL client faked; fixtures is the one stage on the router so far (#54), so a streak
of one is the only abort a single run can reach.
"""

from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import patch

import pytest

from fpl_ingest.cli import main
from fpl_ingest.extract.http.client import RawResponse
from fpl_ingest.extract.http.local_writer import LocalRawWriter
from fpl_ingest.extract.http.response_router import route_response
from tests.integration.test_invalid_response_storage import VALID_BODY, _client, baselines  # noqa: F401
from tests.support.run_helpers import _manifest

PASSING = {"ok": True, "checks": [], "failures": [], "record_count": 1}
FAILING = {"ok": False, "checks": [], "failures": ["top_level_is_list: got dict"], "record_count": None}
ORIGIN = {"kind": "local", "workflow": None, "ref": None, "github_run_id": None, "aws_principal": None}


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


@pytest.mark.covers("#109 AC4")
def test_manifest_counts_invalid_captures_per_class_and_nothing_else(tmp_path):
    writer = LocalRawWriter(tmp_path / "raw", "fpl", origin=ORIGIN)
    routed = [
        (404, FAILING, True),    # not_found
        (200, FAILING, False),   # shape_invalid
        (200, FAILING, False),   # shape_invalid
        (403, FAILING, False),   # http_error
        (400, FAILING, False),   # http_error
        (404, FAILING, False),   # http_error: absence not declared
        (200, PASSING, False),   # valid: counted nowhere
        (503, FAILING, False),   # fetch_failed: counted nowhere
    ]
    for number, (status, shape, absence_ok) in enumerate(routed):
        route_response(writer, f"synthetic/{number}", _response(status), shape, absence_ok=absence_ok)

    manifest = writer.finalize("PARTIAL").manifest

    assert manifest["invalid_counts"] == {"not_found": 1, "shape_invalid": 2, "http_error": 3}


@pytest.mark.covers("#109 AC4")
def test_run_with_no_invalid_captures_counts_zero_in_every_class(tmp_path):
    writer = LocalRawWriter(tmp_path / "raw", "fpl", origin=ORIGIN)
    route_response(writer, "synthetic/1", _response(200), PASSING)

    manifest = writer.finalize("SUCCESS").manifest

    assert manifest["invalid_counts"] == {"not_found": 0, "shape_invalid": 0, "http_error": 0}


def _run(raw: Path, client, monkeypatch, streak: str) -> int:
    monkeypatch.setenv("FPL_HTTP_ERROR_STREAK", streak)
    code = 0
    with patch("fpl_ingest.orchestration.runner.AsyncFPLClient", return_value=client):
        try:
            main(["--raw-dir", str(raw)])
        except SystemExit as exc:
            code = int(exc.code or 0)
    return code


@pytest.mark.covers("#109 AC1")
@pytest.mark.covers("#109 AC5")
def test_aborted_run_records_the_reason_and_exits_non_zero(tmp_path, baselines, monkeypatch):  # noqa: F811
    raw = tmp_path / "raw"

    code = _run(raw, _client(status=403, body=b'{"detail": "forbidden"}'), monkeypatch, streak="1")

    assert code != 0
    assert _manifest(raw)["abort_reason"] == "aborted_http_error_streak"


@pytest.mark.covers("#109 AC5")
def test_items_that_got_http_error_are_captured_by_the_next_run(tmp_path, baselines, monkeypatch):  # noqa: F811
    raw = tmp_path / "raw"
    _run(raw, _client(status=403, body=b'{"detail": "forbidden"}'), monkeypatch, streak="1")
    first = _manifest(raw)
    assert first["abort_reason"] == "aborted_http_error_streak"

    code = _run(raw, _client(), monkeypatch, streak="1")

    assert code == 0
    assert list((raw / "fpl" / "fixtures").rglob("payload.json"))
    manifests = [json.loads(p.read_text()) for p in sorted((raw / "fpl" / "_manifests").rglob("manifest.json"))]
    assert [m.get("abort_reason") for m in manifests].count("aborted_http_error_streak") == 1
