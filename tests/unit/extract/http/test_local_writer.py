"""Unit tests for local_writer.py and raw_keys.py.

Covers:
  - key layout for the three known source/endpoint shapes (fpl JSON,
    understat match-info's payload.json + source.html, reep's .csv)
  - verbatim payload bytes and sha256 against known input
  - atomic write: a failing write leaves neither a .tmp nor a final file
  - duplicate write raises instead of overwriting
  - sidecar content matches what was passed in
  - manifest accumulation across writes/failures and terminal finalisation
  - run id and key-segment validation
"""

from __future__ import annotations

import hashlib
import json
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from fpl_ingest.extract.http import raw_keys
from fpl_ingest.extract.http.local_writer import (
    LocalFilesystemBackend,
    LocalRawWriter,
    RawObjectExistsError,
)
from fpl_ingest.extract.http.raw_keys import RawKeyError

pytestmark = pytest.mark.unit

RUN_START = datetime(2026, 8, 24, 8, 0, 12, tzinfo=timezone.utc)
RUN_ID = "20260824T080012Z-a3f19c"
BOOTSTRAP_BYTES = b'{"elements":[{"id":1,"team":1,"now_cost":55}]}'
HEADERS = {
    "Date": "Mon, 24 Aug 2026 08:00:12 GMT",
    "Age": "41",
    "Cache-Control": "max-age=300",
    "ETag": '"abc123"',
}


# ---------------------------------------------------------------------------
# Fixtures / helpers
# ---------------------------------------------------------------------------


@pytest.fixture
def writer(tmp_path: Path) -> LocalRawWriter:
    return LocalRawWriter(tmp_path, "fpl", run_id=RUN_ID, started_at=RUN_START)


def _write_bootstrap(writer: LocalRawWriter, **overrides: object):
    kwargs: dict = {
        "request_url": "https://fantasy.premierleague.com/api/bootstrap-static/",
        "requested_at": RUN_START,
        "received_at": RUN_START + timedelta(seconds=1),
        "http_status": 200,
        "response_headers": HEADERS,
        "attempt_count": 1,
    }
    kwargs.update(overrides)
    return writer.write_object("bootstrap-static", BOOTSTRAP_BYTES, **kwargs)


def _read_json(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))


# ---------------------------------------------------------------------------
# Key layout
# ---------------------------------------------------------------------------


def test_fpl_json_key_layout(tmp_path: Path, writer: LocalRawWriter):
    result = _write_bootstrap(writer)

    assert result.payload_key == "fpl/bootstrap-static/2026-08-24/20260824T080012Z-a3f19c/payload.json"
    assert result.metadata_key == "fpl/bootstrap-static/2026-08-24/20260824T080012Z-a3f19c/metadata.json"
    assert (tmp_path / result.payload_key).is_file()
    assert (tmp_path / result.metadata_key).is_file()


def test_multi_segment_endpoint_keeps_id_before_extraction_date(writer: LocalRawWriter):
    result = writer.write_object(
        "element-summary/115",
        b"{}",
        request_url="https://fantasy.premierleague.com/api/element-summary/115/",
        requested_at=RUN_START,
        received_at=RUN_START,
        http_status=200,
    )
    assert result.payload_key == (
        "fpl/element-summary/115/2026-08-24/20260824T080012Z-a3f19c/payload.json"
    )


def test_exists_prefix_true_after_a_capture_under_a_different_date_and_run(tmp_path: Path, writer: LocalRawWriter):
    _write_bootstrap(writer)

    assert writer.backend.exists_prefix("fpl/bootstrap-static") is True


def test_exists_prefix_false_when_nothing_was_ever_captured(writer: LocalRawWriter):
    assert writer.backend.exists_prefix("fpl/bootstrap-static") is False


def test_writer_backend_property_is_the_backend_it_writes_through(tmp_path: Path):
    backend = LocalFilesystemBackend(tmp_path)
    writer = LocalRawWriter(tmp_path, "fpl", run_id=RUN_ID, started_at=RUN_START, backend=backend)

    assert writer.backend is backend


def test_understat_match_info_writes_payload_and_source_html(tmp_path: Path):
    writer = LocalRawWriter(tmp_path, "understat", run_id=RUN_ID, started_at=RUN_START)
    html = b"<html>var match_info = JSON.parse('{}')</html>"

    result = writer.write_object(
        "match-info/28778",
        b'{"id":"28778"}',
        request_url="https://understat.com/match/28778",
        requested_at=RUN_START,
        received_at=RUN_START,
        http_status=200,
        companions={"source.html": html},
    )

    prefix = tmp_path / "understat/match-info/28778/2026-08-24/20260824T080012Z-a3f19c"
    assert sorted(p.name for p in prefix.iterdir()) == [
        "metadata.json",
        "payload.json",
        "source.html",
    ]
    assert (prefix / "source.html").read_bytes() == html
    assert result.companion_keys["source.html"].endswith("/source.html")
    assert _read_json(prefix / "metadata.json")["companion_files"] == ["source.html"]


def test_reep_csv_extension(tmp_path: Path):
    writer = LocalRawWriter(tmp_path, "reep", run_id=RUN_ID, started_at=RUN_START)
    result = writer.write_object(
        "people",
        b"key_opta_numeric,key_understat\n1,2\n",
        request_url="https://raw.githubusercontent.com/withqwerty/reep/main/data/people.csv",
        requested_at=RUN_START,
        received_at=RUN_START,
        http_status=200,
        extension="csv",
    )
    assert result.payload_key.endswith("/payload.csv")
    assert (tmp_path / result.payload_key).is_file()


def test_manifest_key_is_sibling_of_endpoint_prefixes(tmp_path: Path, writer: LocalRawWriter):
    _write_bootstrap(writer)
    manifest = writer.finalize("SUCCESS")

    assert manifest.manifest_key == "fpl/_manifests/2026-08-24/20260824T080012Z-a3f19c/manifest.json"
    assert (tmp_path / manifest.manifest_key).is_file()


def test_settlement_marker_key_is_sibling_of_endpoint_prefixes():
    """``_settlement`` sits beside ``_manifests``, and no real endpoint can
    collide with either: ``validate_endpoint`` requires every segment to start
    with an alphanumeric, so a leading underscore is unreachable."""
    from fpl_ingest.extract.http.raw_keys import RawKeyError, settlement_marker_key, validate_endpoint

    assert (
        settlement_marker_key("fpl", "element-summary", 2)
        == "fpl/_settlement/element-summary/2/marker.json"
    )
    with pytest.raises(RawKeyError):
        validate_endpoint("_settlement/element-summary")


def test_extraction_date_comes_from_run_start_not_write_time(tmp_path: Path):
    start = datetime(2026, 8, 24, 23, 59, 50, tzinfo=timezone.utc)
    writer = LocalRawWriter(tmp_path, "fpl", started_at=start)
    assert writer.extraction_date == "2026-08-24"

    result = _write_bootstrap(writer, received_at=start + timedelta(minutes=5))
    assert "/2026-08-24/" in result.payload_key


# ---------------------------------------------------------------------------
# Payload bytes and checksum
# ---------------------------------------------------------------------------


def test_payload_written_byte_for_byte(tmp_path: Path, writer: LocalRawWriter):
    ugly = b'{"b":1,   "a":2,"unicode":"S\xc3\xb8rensen"}'
    result = writer.write_object(
        "fixtures",
        ugly,
        request_url="https://fantasy.premierleague.com/api/fixtures/",
        requested_at=RUN_START,
        received_at=RUN_START,
        http_status=200,
    )
    assert (tmp_path / result.payload_key).read_bytes() == ugly


def test_content_sha256_and_length_against_known_bytes(tmp_path: Path, writer: LocalRawWriter):
    result = _write_bootstrap(writer)
    expected = hashlib.sha256(BOOTSTRAP_BYTES).hexdigest()

    assert result.content_sha256 == expected
    assert result.content_length == len(BOOTSTRAP_BYTES)
    sidecar = _read_json(tmp_path / result.metadata_key)
    assert sidecar["content_sha256"] == expected
    assert sidecar["content_length"] == len(BOOTSTRAP_BYTES)


# ---------------------------------------------------------------------------
# Sidecar content
# ---------------------------------------------------------------------------


def test_sidecar_records_what_was_passed_in(tmp_path: Path, writer: LocalRawWriter):
    result = _write_bootstrap(
        writer,
        attempt_count=3,
        shape_validation={"status": "pass", "checks": ["elements_present"]},
    )
    sidecar = _read_json(tmp_path / result.metadata_key)

    assert sidecar["source"] == "fpl"
    assert sidecar["endpoint"] == "bootstrap-static"
    assert sidecar["run_id"] == RUN_ID
    assert sidecar["extraction_date"] == "2026-08-24"
    assert sidecar["request_url"] == "https://fantasy.premierleague.com/api/bootstrap-static/"
    assert sidecar["requested_at"] == "2026-08-24T08:00:12Z"
    assert sidecar["received_at"] == "2026-08-24T08:00:13Z"
    assert sidecar["http_status"] == 200
    assert sidecar["attempt_count"] == 3
    assert sidecar["shape_validation"] == {"status": "pass", "checks": ["elements_present"]}
    assert sidecar["raw_contract_version"] == raw_keys.RAW_CONTRACT_VERSION


def test_sidecar_headers_are_lowercased_and_complete(tmp_path: Path, writer: LocalRawWriter):
    result = _write_bootstrap(writer)
    headers = _read_json(tmp_path / result.metadata_key)["response_headers"]

    assert headers["age"] == "41"
    assert headers["cache-control"] == "max-age=300"
    assert headers["etag"] == '"abc123"'
    assert headers["date"] == "Mon, 24 Aug 2026 08:00:12 GMT"


def test_sidecar_shape_validation_slot_defaults_to_none(tmp_path: Path, writer: LocalRawWriter):
    result = _write_bootstrap(writer)
    assert _read_json(tmp_path / result.metadata_key)["shape_validation"] is None


# ---------------------------------------------------------------------------
# Atomicity and immutability
# ---------------------------------------------------------------------------


class _FailingBackend(LocalFilesystemBackend):
    """Writes a partial temporary file, then fails, on a chosen key."""

    def __init__(self, root: Path, fail_on: str) -> None:
        super().__init__(root)
        self.fail_on = fail_on

    def _write_tmp(self, tmp: Path, data: bytes) -> None:
        if tmp.name.startswith(self.fail_on):
            tmp.write_bytes(data[: len(data) // 2])
            raise OSError("disk full")
        super()._write_tmp(tmp, data)


def test_failed_write_leaves_no_tmp_and_no_final_file(tmp_path: Path):
    writer = LocalRawWriter(
        tmp_path,
        "fpl",
        run_id=RUN_ID,
        started_at=RUN_START,
        backend=_FailingBackend(tmp_path, "payload"),
    )

    with pytest.raises(OSError, match="disk full"):
        _write_bootstrap(writer)

    prefix = tmp_path / "fpl/bootstrap-static/2026-08-24" / RUN_ID
    assert list(prefix.iterdir()) == [], "no partial or final file may remain"


def test_duplicate_write_raises_instead_of_overwriting(tmp_path: Path, writer: LocalRawWriter):
    result = _write_bootstrap(writer)

    with pytest.raises(RawObjectExistsError):
        _write_bootstrap(writer)

    assert (tmp_path / result.payload_key).read_bytes() == BOOTSTRAP_BYTES


def test_duplicate_run_id_across_writers_raises(tmp_path: Path):
    first = LocalRawWriter(tmp_path, "fpl", run_id=RUN_ID, started_at=RUN_START)
    _write_bootstrap(first)

    second = LocalRawWriter(tmp_path, "fpl", run_id=RUN_ID, started_at=RUN_START)
    with pytest.raises(RawObjectExistsError):
        _write_bootstrap(second)


def test_companion_may_not_shadow_reserved_filenames(writer: LocalRawWriter):
    with pytest.raises(ValueError, match="reserved"):
        _write_bootstrap(writer, companions={"metadata.json": b"{}"})


# ---------------------------------------------------------------------------
# Manifest
# ---------------------------------------------------------------------------


def test_manifest_accumulates_across_writes(tmp_path: Path, writer: LocalRawWriter):
    _write_bootstrap(writer)
    writer.write_object(
        "event-live/01",
        b'{"elements":[]}',
        request_url="https://fantasy.premierleague.com/api/event/1/live/",
        requested_at=RUN_START,
        received_at=RUN_START,
        http_status=200,
    )
    writer.record_failure(
        "element-summary/500",
        request_url="https://fantasy.premierleague.com/api/element-summary/500/",
        error_class="FPLClientError",
        http_status=503,
        attempt_count=4,
    )

    manifest = writer.finalize(
        "FAILED_PARTIAL",
        git_sha="1134a88",
        ingest_version="fpl-ingest/1.0.0",
        config={"rate": 5.0, "concurrency": 10, "strict": False, "force": False},
        ended_at=RUN_START + timedelta(seconds=90),
    ).manifest

    assert manifest["status"] == "FAILED_PARTIAL"
    assert manifest["objects"]["bootstrap-static"] == {
        "attempted": 1,
        "written": 1,
        "failed": 0,
        "bytes": len(BOOTSTRAP_BYTES),
    }
    assert manifest["objects"]["element-summary/500"]["failed"] == 1
    assert manifest["totals"] == {
        "attempted": 3,
        "written": 2,
        "failed": 1,
        "bytes": len(BOOTSTRAP_BYTES) + len(b'{"elements":[]}'),
    }
    assert manifest["failures"][0]["error_class"] == "FPLClientError"
    assert manifest["failures"][0]["http_status"] == 503
    assert manifest["duration_seconds"] == 90.0
    assert manifest["started_at"] == "2026-08-24T08:00:12Z"
    assert manifest["ended_at"] == "2026-08-24T08:01:42Z"
    assert manifest["git_sha"] == "1134a88"
    assert manifest["config"]["concurrency"] == 10
    assert "finality" not in manifest


def test_partial_manifest_is_on_disk_before_finalize(tmp_path: Path, writer: LocalRawWriter):
    _write_bootstrap(writer)

    path = tmp_path / "fpl/_manifests/2026-08-24" / RUN_ID / "manifest.json"
    on_disk = _read_json(path)
    assert on_disk["status"] == "IN_PROGRESS"
    assert on_disk["totals"]["written"] == 1
    assert on_disk["ended_at"] is None


def test_finalize_overwrites_the_in_progress_manifest(tmp_path: Path, writer: LocalRawWriter):
    _write_bootstrap(writer)
    writer.finalize("SUCCESS")

    path = tmp_path / "fpl/_manifests/2026-08-24" / RUN_ID / "manifest.json"
    assert _read_json(path)["status"] == "SUCCESS"


def test_finality_block_is_passed_through_untouched(writer: LocalRawWriter):
    finality = [{"event": 1, "points": "p", "bonus_added": False}]
    manifest = writer.finalize("SUCCESS", finality=finality).manifest
    assert manifest["finality"] == finality


def test_writer_rejects_use_after_finalize(writer: LocalRawWriter):
    writer.finalize("SUCCESS")
    with pytest.raises(RuntimeError, match="already been finalized"):
        _write_bootstrap(writer)


# ---------------------------------------------------------------------------
# raw_keys validation
# ---------------------------------------------------------------------------


def test_generated_run_id_matches_contract_format():
    run_id = raw_keys.new_run_id(RUN_START)
    assert run_id.startswith("20260824T080012Z-")
    assert ":" not in run_id
    assert run_id.count(".") == 0
    raw_keys.validate_run_id(run_id)


def test_generated_run_ids_are_unique_within_one_second():
    ids = {raw_keys.new_run_id(RUN_START) for _ in range(200)}
    assert len(ids) == 200


@pytest.mark.parametrize(
    "run_id",
    ["2026-08-24T08:00:12Z-a3f19c", "20260824T080012Z", "20260824T080012Z-A3F19C", ""],
)
def test_invalid_run_ids_rejected(run_id: str):
    with pytest.raises(RawKeyError):
        raw_keys.validate_run_id(run_id)


@pytest.mark.parametrize("endpoint", ["", "/fixtures", "fixtures/", "a//b", "../etc", "_manifests/x"])
def test_invalid_endpoints_rejected(endpoint: str):
    with pytest.raises(RawKeyError):
        raw_keys.validate_endpoint(endpoint)


def test_manifests_prefix_is_reserved_as_a_source():
    with pytest.raises(RawKeyError):
        raw_keys.validate_source("_manifests")


def test_naive_run_start_is_treated_as_utc(tmp_path: Path):
    writer = LocalRawWriter(tmp_path, "fpl", started_at=datetime(2026, 8, 24, 8, 0, 12))
    assert writer.extraction_date == "2026-08-24"
    assert writer.run_id.startswith("20260824T080012Z-")
