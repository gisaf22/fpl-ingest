"""The finalized manifest's per-capture index, ``captures[]`` (#62).

One entry per payload written, built from the same in-memory record as the
payload's sidecar. Fetch failures have no payload and so no entry, and an
IN_PROGRESS manifest carries no ``captures`` at all (#62 D4).
"""

from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from fpl_ingest.extract.http.local_writer import LocalRawWriter
from fpl_ingest.orchestration.run_status import classify_run

RUN_START = datetime(2026, 8, 24, 8, 0, 12, tzinfo=timezone.utc)
RUN_ID = "20260824T080012Z-a3f19c"
PAYLOAD = b'{"elements":[]}'
SHAPE_OK = {"ok": True, "checks": ["top_level_is_object"], "failures": []}
SHAPE_INVALID = {"ok": False, "checks": ["top_level_is_list"], "failures": ["top_level_is_list: got dict"]}

CAPTURE_FIELDS = {
    "key", "endpoint", "received_at", "content_sha256", "content_length",
    "http_status", "shape_ok", "usable", "season",
}


@pytest.fixture
def writer(tmp_path: Path) -> LocalRawWriter:
    return LocalRawWriter(tmp_path, "fpl", run_id=RUN_ID, started_at=RUN_START)


def _write(writer: LocalRawWriter, endpoint: str, *, shape, season: str | None = "2026-27"):
    return writer.write_object(
        endpoint,
        PAYLOAD,
        request_url=f"https://fantasy.premierleague.com/api/{endpoint}/",
        requested_at=RUN_START,
        received_at=RUN_START + timedelta(seconds=1),
        http_status=200,
        shape_validation=shape,
        season=season,
    )


def _finalize(writer: LocalRawWriter) -> dict:
    return writer.finalize(classify_run(writer.endpoint_outcomes)).manifest


@pytest.mark.covers("#62 AC1")
def test_capture_entry_fields(writer):
    result = _write(writer, "bootstrap-static", shape=SHAPE_OK)

    manifest = _finalize(writer)

    assert manifest["raw_contract_version"] == "2.4.0"
    (entry,) = manifest["captures"]
    assert set(entry) == CAPTURE_FIELDS
    assert entry == {
        "key": f"raw/{result.payload_key}",
        "endpoint": "bootstrap-static",
        "received_at": "2026-08-24T08:00:13Z",
        "content_sha256": result.content_sha256,
        "content_length": len(PAYLOAD),
        "http_status": 200,
        "shape_ok": True,
        "usable": True,
        "season": "2026-27",
    }


@pytest.mark.covers("#62 AC1")
@pytest.mark.parametrize(
    ("shape", "shape_ok"),
    [
        pytest.param(SHAPE_OK, True, id="shape-valid"),
        pytest.param(SHAPE_INVALID, False, id="shape-invalid"),
        pytest.param(None, True, id="no-verdict-counts-as-valid"),
    ],
)
def test_capture_entry_usable_follows_shape_verdict(writer, shape, shape_ok):
    _write(writer, "fixtures", shape=shape)

    (entry,) = _finalize(writer)["captures"]

    assert entry["shape_ok"] is shape_ok
    assert entry["usable"] is shape_ok


@pytest.mark.covers("#62 AC1")
def test_capture_entry_records_null_season(writer):
    _write(writer, "event-status", shape=SHAPE_OK, season=None)

    (entry,) = _finalize(writer)["captures"]

    assert entry["season"] is None


@pytest.mark.covers("#62 AC1")
def test_one_capture_per_payload_and_none_for_a_fetch_failure(writer):
    written = [
        _write(writer, "event-status", shape=SHAPE_OK),
        _write(writer, "bootstrap-static", shape=SHAPE_OK),
        _write(writer, "element-summary/1", shape=SHAPE_INVALID),
    ]
    writer.record_failure(
        "element-summary/2",
        request_url="https://fantasy.premierleague.com/api/element-summary/2/",
        error_class="FPLClientError",
    )

    captures = _finalize(writer)["captures"]

    assert [c["key"] for c in captures] == [f"raw/{w.payload_key}" for w in written]
    assert not any("element-summary/2/" in c["key"] for c in captures)


@pytest.mark.covers("#62 AC1")
def test_in_progress_manifest_has_no_captures(tmp_path, writer):
    _write(writer, "bootstrap-static", shape=SHAPE_OK)

    (path,) = (tmp_path / "fpl" / "_manifests").rglob("manifest.json")
    on_disk = json.loads(path.read_text(encoding="utf-8"))

    assert on_disk["status"] == "IN_PROGRESS"
    assert "captures" not in on_disk
