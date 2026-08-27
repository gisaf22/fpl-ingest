"""Unit tests for the event-status raw-capture stage.

What it must guarantee:
  - the response body reaches raw storage byte-for-byte, under source ``fpl``
    and endpoint ``event-status``;
  - the sidecar carries the capture metadata the raw contract promises;
  - a payload that fails shape validation is still written, flagged in the
    sidecar, and reported so the run manifest reads FAILED_PARTIAL;
  - a shape-valid payload is parsed into a per-event finality map, aggregating
    multiple match-date entries per event (strategy doc 2.1);
  - a fetch failure or shape failure yields ``output=None`` — "finality
    unknown," never "everything is settled";
  - nothing in this module can upsert rows.
"""

from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock

import pytest

from fpl_ingest.extract.http.client import RawResponse
from fpl_ingest.extract.http.local_writer import LocalRawWriter
from fpl_ingest.extract.http.raw_keys import iso_utc
from fpl_ingest.extract.http.sync_http import FPLClientError
from fpl_ingest.extract.stages import event_status as event_status_stage
from fpl_ingest.extract.stages.event_status import (
    ingest_event_status,
    validate_event_status_shape,
)
from fpl_ingest.orchestration.execution_state import PipelineExecutionState
from fpl_ingest.orchestration.run_status import (
    RUN_STATUS_FAILED_PARTIAL,
    RUN_STATUS_SUCCESS,
    classify_run_from_results,
)

pytestmark = pytest.mark.unit

_EVENT_STATUS_URL = "https://fantasy.premierleague.com/api/event-status/"

_VALID_PAYLOAD = {
    "status": [
        {"bonus_added": True, "date": "2026-08-15", "event": 1, "points": "r"},
        {"bonus_added": False, "date": "2026-08-22", "event": 2, "points": "p"},
    ],
    "leagues": "",
}


def _raw(
    payload: object | None = None,
    *,
    status: int = 200,
    body: bytes | None = None,
    headers: dict[str, str] | None = None,
    attempt_count: int = 1,
) -> RawResponse:
    requested_at = datetime(2026, 8, 24, 8, 0, 0, tzinfo=timezone.utc)
    if body is None:
        body = json.dumps(payload).encode("utf-8")
    return RawResponse(
        url=_EVENT_STATUS_URL,
        status=status,
        headers=headers or {"content-type": "application/json", "etag": 'W/"abc"'},
        body=body,
        requested_at=requested_at,
        received_at=requested_at + timedelta(seconds=1),
        attempt_count=attempt_count,
    )


def _client(raw: RawResponse | None = None, *, error: Exception | None = None) -> MagicMock:
    client = MagicMock()
    client.get_event_status_raw = AsyncMock(
        return_value=raw, side_effect=error if error is not None else None
    )
    return client


def _writer(tmp_path: Path) -> LocalRawWriter:
    return LocalRawWriter(tmp_path / "raw", "fpl", run_id="20260824T080000Z-abc123")


def _read(root: Path, key: str) -> dict:
    return json.loads((root / Path(key)).read_text(encoding="utf-8"))


# ---------------------------------------------------------------------------
# Happy path
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_ingest_event_status_writes_raw_object_under_fpl_event_status(tmp_path):
    raw = _raw(_VALID_PAYLOAD)
    writer = _writer(tmp_path)

    outcome = await ingest_event_status(_client(raw), writer)

    root = tmp_path / "raw"
    payload_path = root / "fpl" / "event-status" / writer.extraction_date / writer.run_id / "payload.json"
    assert payload_path.exists(), "payload must land under {source}/{endpoint}/..."
    assert payload_path.read_bytes() == raw.body, "bytes must be stored verbatim"

    assert outcome.result.stage == "event_status"
    assert outcome.result.errors == 0
    assert outcome.result.written == 1
    assert outcome.result.skipped == 0


@pytest.mark.asyncio
async def test_ingest_event_status_sidecar_carries_capture_metadata(tmp_path):
    raw = _raw(_VALID_PAYLOAD, attempt_count=3)
    writer = _writer(tmp_path)

    await ingest_event_status(_client(raw), writer)

    root = tmp_path / "raw"
    sidecar = _read(
        root, f"fpl/event-status/{writer.extraction_date}/{writer.run_id}/metadata.json"
    )
    assert sidecar["source"] == "fpl"
    assert sidecar["endpoint"] == "event-status"
    assert sidecar["request_url"] == _EVENT_STATUS_URL
    assert sidecar["http_status"] == 200
    assert sidecar["requested_at"] == iso_utc(raw.requested_at)
    assert sidecar["received_at"] == iso_utc(raw.received_at)
    assert sidecar["content_length"] == len(raw.body)
    assert sidecar["attempt_count"] == 3
    assert sidecar["response_headers"]["etag"] == 'W/"abc"'
    assert sidecar["shape_validation"]["ok"] is True


@pytest.mark.asyncio
async def test_clean_capture_finalizes_the_manifest_as_success(tmp_path):
    writer = _writer(tmp_path)
    outcome = await ingest_event_status(_client(_raw(_VALID_PAYLOAD)), writer)

    status = classify_run_from_results([outcome.result], strict_mode=False)
    assert status == RUN_STATUS_SUCCESS

    manifest = writer.finalize(status, finality=outcome.output).manifest
    assert manifest["status"] == RUN_STATUS_SUCCESS
    assert manifest["objects"]["event-status"]["written"] == 1
    assert manifest["totals"]["failed"] == 0
    assert manifest["finality"] == {1: {"points": "r", "bonus_added": True},
                                     2: {"points": "p", "bonus_added": False}}


# ---------------------------------------------------------------------------
# Finality parsing
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_output_is_the_parsed_finality_map(tmp_path):
    outcome = await ingest_event_status(_client(_raw(_VALID_PAYLOAD)), _writer(tmp_path))

    assert outcome.output == {
        1: {"points": "r", "bonus_added": True},
        2: {"points": "p", "bonus_added": False},
    }


@pytest.mark.asyncio
async def test_an_event_is_settled_only_when_every_listed_date_is_final(tmp_path):
    """Strategy doc 2.1: status carries one entry per match date, not per event."""
    payload = {
        "status": [
            {"bonus_added": True, "date": "2026-08-15", "event": 1, "points": "r"},
            {"bonus_added": False, "date": "2026-08-16", "event": 1, "points": "p"},
        ],
        "leagues": "",
    }

    outcome = await ingest_event_status(_client(_raw(payload)), _writer(tmp_path))

    assert outcome.output == {1: {"points": "p", "bonus_added": False}}, (
        "one still-provisional date keeps the whole event provisional"
    )


@pytest.mark.asyncio
async def test_an_event_is_settled_when_every_listed_date_is_final(tmp_path):
    payload = {
        "status": [
            {"bonus_added": True, "date": "2026-08-15", "event": 1, "points": "r"},
            {"bonus_added": True, "date": "2026-08-16", "event": 1, "points": "r"},
        ],
        "leagues": "",
    }

    outcome = await ingest_event_status(_client(_raw(payload)), _writer(tmp_path))

    assert outcome.output == {1: {"points": "r", "bonus_added": True}}


# ---------------------------------------------------------------------------
# Shape validation
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "raw_kwargs, expected_failure",
    [
        pytest.param({"status": 500, "body": b"{}"}, "http_status_2xx", id="non_2xx"),
        pytest.param({"body": b"<html>oops</html>"}, "body_parses_as_json", id="not_json"),
        pytest.param({"payload": [1, 2]}, "top_level_is_object", id="not_an_object"),
        pytest.param({"payload": {}}, "status_key_present_and_is_list", id="no_status_key"),
        pytest.param(
            {"payload": {"status": {}}},
            "status_key_present_and_is_list",
            id="status_not_a_list",
        ),
        pytest.param(
            {"payload": {"status": [{"event": 1}]}},
            "sampled_record_has_identifying_fields",
            id="missing_identifying_fields",
        ),
    ],
)
def test_validate_event_status_shape_flags_each_structural_failure(raw_kwargs, expected_failure):
    verdict = validate_event_status_shape(_raw(**raw_kwargs))

    assert verdict["ok"] is False
    assert any(f.startswith(expected_failure) for f in verdict["failures"]), verdict


def test_validate_event_status_shape_passes_a_well_formed_payload():
    verdict = validate_event_status_shape(_raw(_VALID_PAYLOAD))

    assert verdict["ok"] is True
    assert verdict["failures"] == []
    assert verdict["record_count"] == 2


def test_validate_event_status_shape_accepts_an_empty_status_list():
    verdict = validate_event_status_shape(_raw({"status": [], "leagues": ""}))

    assert verdict["ok"] is True
    assert verdict["record_count"] == 0


@pytest.mark.asyncio
async def test_shape_failure_still_writes_the_payload_and_yields_no_finality(tmp_path):
    """The one unrecoverable mistake here would be discarding a surprising payload."""
    raw = _raw(body=b"<html>502 Bad Gateway</html>")
    writer = _writer(tmp_path)

    outcome = await ingest_event_status(_client(raw), writer)

    root = tmp_path / "raw"
    payload_path = root / "fpl" / "event-status" / writer.extraction_date / writer.run_id / "payload.json"
    assert payload_path.read_bytes() == raw.body, "a failed payload must still be written"

    sidecar = _read(
        root, f"fpl/event-status/{writer.extraction_date}/{writer.run_id}/metadata.json"
    )
    assert sidecar["shape_validation"]["ok"] is False
    assert sidecar["shape_validation"]["failures"]

    assert outcome.result.errors == 0, "a shape failure is partial, not a hard error"
    assert outcome.result.skipped == 1
    assert outcome.output is None, "an invalid payload must never be trusted for finality"

    status = classify_run_from_results([outcome.result], strict_mode=False)
    assert status == RUN_STATUS_FAILED_PARTIAL
    assert writer.finalize(status).manifest["status"] == RUN_STATUS_FAILED_PARTIAL


# ---------------------------------------------------------------------------
# Failure and skip paths
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_fetch_failure_records_a_manifest_failure_and_writes_no_payload(tmp_path):
    writer = _writer(tmp_path)

    outcome = await ingest_event_status(
        _client(error=FPLClientError("unreachable")), writer
    )

    assert outcome.result.errors == 1
    assert outcome.output is None
    assert not (tmp_path / "raw" / "fpl" / "event-status").exists()

    manifest = writer.manifest_snapshot
    assert manifest["objects"]["event-status"]["failed"] == 1
    assert manifest["failures"][0]["endpoint"] == "event-status"
    assert manifest["failures"][0]["error_class"] == "FPLClientError"


@pytest.mark.asyncio
async def test_fail_fast_skips_the_capture_entirely(tmp_path):
    state = PipelineExecutionState()
    state.fail()
    client = _client(_raw(_VALID_PAYLOAD))
    writer = _writer(tmp_path)

    outcome = await ingest_event_status(client, writer, execution_state=state)

    client.get_event_status_raw.assert_not_called()
    assert outcome.result.fetched == 0
    assert outcome.output is None
    assert not (tmp_path / "raw" / "fpl").exists()


# ---------------------------------------------------------------------------
# Stage contract
# ---------------------------------------------------------------------------


def test_stage_declares_no_output_tables():
    assert event_status_stage.EVENT_STATUS_STAGE.output_tables == ()
