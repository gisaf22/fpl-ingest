"""Unit tests for the fixtures raw-capture stage.

The stage no longer writes SQLite. What it must guarantee now:
  - the response body reaches raw storage byte-for-byte, under source ``fpl``
    and endpoint ``fixtures``;
  - the sidecar carries the capture metadata the raw contract promises;
  - a payload that fails shape validation is still written, flagged in the
    sidecar, and reported so the run manifest reads FAILED_PARTIAL;
  - nothing in this module can upsert rows.
"""

from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock

import pytest

from fpl_ingest.extract.http.client import RawResponse
from fpl_ingest.extract.http.local_writer import LocalRawWriter
from fpl_ingest.extract.http.raw_keys import iso_utc
from fpl_ingest.extract.http.sync_http import FPLClientError
from fpl_ingest.extract.stages import fixtures as fixtures_stage
from fpl_ingest.extract.stages.fixtures import ingest_fixtures, validate_fixtures_shape
from fpl_ingest.orchestration.run_status import (
    RUN_STATUS_FAILED_PARTIAL,
    RUN_STATUS_SUCCESS,
    classify_run_from_results,
)

pytestmark = pytest.mark.unit

_FIXTURES_URL = "https://fantasy.premierleague.com/api/fixtures/"

_VALID_PAYLOAD = [
    {"id": 101, "team_h": 11, "team_a": 7, "event": 1},
    {"id": 102, "team_h": 13, "team_a": 11, "event": 2},
]


def _raw(
    payload: object | None = None,
    *,
    status: int = 200,
    body: bytes | None = None,
    headers: dict[str, str] | None = None,
    attempt_count: int = 1,
) -> RawResponse:
    from datetime import datetime, timedelta, timezone

    requested_at = datetime(2026, 8, 24, 8, 0, 0, tzinfo=timezone.utc)
    if body is None:
        body = json.dumps(payload).encode("utf-8")
    return RawResponse(
        url=_FIXTURES_URL,
        status=status,
        headers=headers or {"content-type": "application/json", "etag": 'W/"abc"'},
        body=body,
        requested_at=requested_at,
        received_at=requested_at + timedelta(seconds=1),
        attempt_count=attempt_count,
    )


def _client(raw: RawResponse | None = None, *, error: Exception | None = None) -> MagicMock:
    client = MagicMock()
    client.get_fixtures_raw = AsyncMock(
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


# ---------------------------------------------------------------------------
# Shape validation
# ---------------------------------------------------------------------------


# ---------------------------------------------------------------------------
# Failure and skip paths
# ---------------------------------------------------------------------------


# ---------------------------------------------------------------------------
# The retired SQLite path
# ---------------------------------------------------------------------------


class TestShapeValidation:
    """Shape-validation verdicts for this endpoint's payload."""

    @pytest.mark.parametrize(
        "raw_kwargs, expected_failure",
        [
            pytest.param({"status": 500, "body": b"[]"}, "http_status_2xx", id="non_2xx"),
            pytest.param({"body": b"<html>oops</html>"}, "body_parses_as_json", id="not_json"),
            pytest.param({"payload": {"fixtures": []}}, "top_level_is_list", id="not_a_list"),
            pytest.param(
                {"payload": [{"id": 1}]},
                "sampled_record_has_identifying_fields",
                id="missing_identifying_fields",
            ),
        ],
    )
    def test_validate_fixtures_shape_flags_each_structural_failure(self, raw_kwargs, expected_failure):
        verdict = validate_fixtures_shape(_raw(**raw_kwargs))

        assert verdict["ok"] is False
        assert any(f.startswith(expected_failure) for f in verdict["failures"]), verdict

    def test_validate_fixtures_shape_passes_a_well_formed_payload(self):
        verdict = validate_fixtures_shape(_raw(_VALID_PAYLOAD))

        assert verdict["ok"] is True
        assert verdict["failures"] == []
        assert verdict["record_count"] == 2

    def test_validate_fixtures_shape_accepts_an_empty_list(self):
        """An empty fixture list is not a shape failure — the football decides that."""
        verdict = validate_fixtures_shape(_raw([]))

        assert verdict["ok"] is True
        assert verdict["record_count"] == 0

    def test_validate_fixtures_shape_samples_rather_than_scans(self):
        """Only the first record is inspected; later records are the warehouse's problem."""
        verdict = validate_fixtures_shape(_raw([_VALID_PAYLOAD[0], {"nonsense": True}]))

        assert verdict["ok"] is True

    @pytest.mark.asyncio
    async def test_shape_failure_still_writes_the_payload_and_flags_the_manifest(self, tmp_path):
        """The one unrecoverable mistake here would be discarding a surprising payload."""
        raw = _raw(body=b"<html>502 Bad Gateway</html>")
        writer = _writer(tmp_path)

        outcome = await ingest_fixtures(_client(raw), writer)

        root = tmp_path / "raw"
        payload_path = root / "fpl" / "fixtures" / writer.extraction_date / writer.run_id / "payload.json"
        assert payload_path.read_bytes() == raw.body, "a failed payload must still be written"

        sidecar = _read(
            root, f"fpl/fixtures/{writer.extraction_date}/{writer.run_id}/metadata.json"
        )
        assert sidecar["shape_validation"]["ok"] is False
        assert sidecar["shape_validation"]["failures"]

        assert outcome.result.errors == 0, "a shape failure is partial, not a hard error"
        assert outcome.result.skipped == 1

        status = classify_run_from_results([outcome.result], strict_mode=False)
        assert status == RUN_STATUS_FAILED_PARTIAL
        assert writer.finalize(status).manifest["status"] == RUN_STATUS_FAILED_PARTIAL


class TestFetchAndCapture:
    """The happy path: bytes reach raw storage with correct metadata."""

    @pytest.mark.asyncio
    async def test_ingest_fixtures_writes_raw_object_under_fpl_fixtures(self, tmp_path):
        raw = _raw(_VALID_PAYLOAD)
        writer = _writer(tmp_path)

        outcome = await ingest_fixtures(_client(raw), writer)

        root = tmp_path / "raw"
        payload_path = root / "fpl" / "fixtures" / writer.extraction_date / writer.run_id / "payload.json"
        assert payload_path.exists(), "payload must land under {source}/{endpoint}/..."
        assert payload_path.read_bytes() == raw.body, "bytes must be stored verbatim"

        assert outcome.result.stage == "fixtures"
        assert outcome.result.errors == 0
        assert outcome.result.written == 1
        assert outcome.result.skipped == 0

    @pytest.mark.asyncio
    async def test_ingest_fixtures_sidecar_carries_capture_metadata(self, tmp_path):
        raw = _raw(_VALID_PAYLOAD, attempt_count=3)
        writer = _writer(tmp_path)

        await ingest_fixtures(_client(raw), writer)

        root = tmp_path / "raw"
        sidecar = _read(
            root, f"fpl/fixtures/{writer.extraction_date}/{writer.run_id}/metadata.json"
        )
        assert sidecar["source"] == "fpl"
        assert sidecar["endpoint"] == "fixtures"
        assert sidecar["request_url"] == _FIXTURES_URL
        assert sidecar["http_status"] == 200
        assert sidecar["requested_at"] == iso_utc(raw.requested_at)
        assert sidecar["received_at"] == iso_utc(raw.received_at)
        assert sidecar["content_length"] == len(raw.body)
        assert sidecar["attempt_count"] == 3
        assert sidecar["response_headers"]["etag"] == 'W/"abc"'
        assert sidecar["shape_validation"]["ok"] is True

    @pytest.mark.asyncio
    async def test_clean_capture_finalizes_the_manifest_as_success(self, tmp_path):
        writer = _writer(tmp_path)
        outcome = await ingest_fixtures(_client(_raw(_VALID_PAYLOAD)), writer)

        status = classify_run_from_results([outcome.result], strict_mode=False)
        assert status == RUN_STATUS_SUCCESS

        manifest = writer.finalize(status).manifest
        assert manifest["status"] == RUN_STATUS_SUCCESS
        assert manifest["objects"]["fixtures"]["written"] == 1
        assert manifest["totals"]["failed"] == 0


class TestErrorHandling:
    """Fetch failures, partial failures, and the fail-fast sentinel."""

    @pytest.mark.asyncio
    async def test_fetch_failure_records_a_manifest_failure_and_writes_no_payload(self, tmp_path):
        writer = _writer(tmp_path)

        outcome = await ingest_fixtures(
            _client(error=FPLClientError("unreachable")), writer
        )

        assert outcome.result.errors == 1
        assert not (tmp_path / "raw" / "fpl" / "fixtures").exists()

        manifest = writer.manifest_snapshot
        assert manifest["objects"]["fixtures"]["failed"] == 1
        assert manifest["failures"][0]["endpoint"] == "fixtures"
        assert manifest["failures"][0]["error_class"] == "FPLClientError"

    @pytest.mark.asyncio
    async def test_fail_fast_skips_the_capture_entirely(self, tmp_path):
        from fpl_ingest.orchestration.execution_state import PipelineExecutionState

        state = PipelineExecutionState()
        state.fail()
        client = _client(_raw(_VALID_PAYLOAD))
        writer = _writer(tmp_path)

        outcome = await ingest_fixtures(client, writer, execution_state=state)

        client.get_fixtures_raw.assert_not_called()
        assert outcome.result.fetched == 0
        assert not (tmp_path / "raw" / "fpl").exists()


class TestStageContract:
    """Stage-level invariants: key layout, declared tables, retired API."""

    def test_sqlite_upsert_helpers_are_gone(self):
        """These were removed with the flatten/upsert retirement — they must not return."""
        for name in (
            "process_fixtures_payload",
            "upsert_fixtures",
            "flatten_fixture_stat_rows",
            "upsert_fixture_stats",
        ):
            assert not hasattr(fixtures_stage, name), f"{name} must stay removed"

    def test_stage_declares_no_output_tables(self):
        assert fixtures_stage.FIXTURES_STAGE.output_tables == ()
