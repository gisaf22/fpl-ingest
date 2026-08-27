"""Unit tests for the bootstrap-static raw-capture stage.

The stage no longer writes SQLite. What it must guarantee now:
  - the response body reaches raw storage byte-for-byte, under source ``fpl``
    and endpoint ``bootstrap-static``;
  - the sidecar carries the capture metadata the raw contract promises;
  - a payload that fails shape validation is still written, flagged in the
    sidecar, and reported so the run manifest reads FAILED_PARTIAL;
  - it shares the run's single manifest with the other capture stages;
  - the two stages not yet migrated still get their in-memory handoff;
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
from fpl_ingest.extract.stages import bootstrap as bootstrap_stage
from fpl_ingest.extract.stages.bootstrap import (
    core_handoff,
    ingest_core_data,
    validate_bootstrap_shape,
)
from fpl_ingest.orchestration.run_status import (
    RUN_STATUS_FAILED_PARTIAL,
    RUN_STATUS_SUCCESS,
    classify_run_from_results,
)
from tests.factories import event_row, player_row, team_row

pytestmark = pytest.mark.unit

_BOOTSTRAP_URL = "https://fantasy.premierleague.com/api/bootstrap-static/"


def _element_type(type_id: int, short: str) -> dict:
    return {
        "id": type_id,
        "singular_name": short,
        "singular_name_short": short,
        "plural_name": f"{short}s",
        "plural_name_short": f"{short}S",
        "squad_select": 2,
        "squad_min_select": None,
        "squad_max_select": None,
        "squad_min_play": 1,
        "squad_max_play": 1,
        "ui_shirt_specific": False,
        "element_count": 10,
    }


_VALID_PAYLOAD = {
    "elements": [
        player_row(id=1, team=11, element_type=3, now_cost=130),
        player_row(id=2, team=13, element_type=4, now_cost=145),
    ],
    "teams": [team_row(id=11), team_row(id=13, name="Man City", short_name="MCI", code=43)],
    "events": [
        event_row(id=1, finished=True, is_previous=True, is_current=False, is_next=False),
        event_row(id=2, name="Gameweek 2", finished=False, data_checked=False,
                  is_previous=False, is_current=True, is_next=False),
    ],
    "element_types": [_element_type(1, "GKP"), _element_type(3, "MID")],
}


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
        url=_BOOTSTRAP_URL,
        status=status,
        headers=headers or {"content-type": "application/json", "etag": 'W/"abc"'},
        body=body,
        requested_at=requested_at,
        received_at=requested_at + timedelta(seconds=1),
        attempt_count=attempt_count,
    )


def _client(raw: RawResponse | None = None, *, error: Exception | None = None) -> MagicMock:
    client = MagicMock()
    client.get_bootstrap_raw = AsyncMock(
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
async def test_ingest_core_data_writes_raw_object_under_fpl_bootstrap_static(tmp_path):
    raw = _raw(_VALID_PAYLOAD)
    writer = _writer(tmp_path)

    outcome = await ingest_core_data(_client(raw), writer)

    root = tmp_path / "raw"
    payload_path = (
        root / "fpl" / "bootstrap-static" / writer.extraction_date / writer.run_id / "payload.json"
    )
    assert payload_path.exists(), "payload must land under {source}/{endpoint}/..."
    assert payload_path.read_bytes() == raw.body, "bytes must be stored verbatim"

    assert outcome.result.stage == "core"
    assert outcome.result.errors == 0
    assert outcome.result.written == 1
    assert outcome.result.skipped == 0


@pytest.mark.asyncio
async def test_ingest_core_data_sidecar_carries_capture_metadata(tmp_path):
    raw = _raw(_VALID_PAYLOAD, attempt_count=3)
    writer = _writer(tmp_path)

    await ingest_core_data(_client(raw), writer)

    root = tmp_path / "raw"
    sidecar = _read(
        root, f"fpl/bootstrap-static/{writer.extraction_date}/{writer.run_id}/metadata.json"
    )
    assert sidecar["source"] == "fpl"
    assert sidecar["endpoint"] == "bootstrap-static"
    assert sidecar["request_url"] == _BOOTSTRAP_URL
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
    outcome = await ingest_core_data(_client(_raw(_VALID_PAYLOAD)), writer)

    status = classify_run_from_results([outcome.result], strict_mode=False)
    assert status == RUN_STATUS_SUCCESS

    manifest = writer.finalize(status).manifest
    assert manifest["status"] == RUN_STATUS_SUCCESS
    assert manifest["objects"]["bootstrap-static"]["written"] == 1
    assert manifest["totals"]["failed"] == 0


@pytest.mark.asyncio
async def test_bootstrap_and_fixtures_share_one_run_manifest(tmp_path):
    """A run touches several endpoints; they all belong to one manifest."""
    from fpl_ingest.extract.stages.fixtures import ingest_fixtures

    writer = _writer(tmp_path)

    await ingest_core_data(_client(_raw(_VALID_PAYLOAD)), writer)

    fixtures_client = MagicMock()
    fixtures_client.get_fixtures_raw = AsyncMock(
        return_value=RawResponse(
            url="https://fantasy.premierleague.com/api/fixtures/",
            status=200,
            headers={},
            body=b'[{"id": 1, "team_h": 11, "team_a": 7, "event": 1}]',
            requested_at=_raw().requested_at,
            received_at=_raw().received_at,
        )
    )
    await ingest_fixtures(fixtures_client, writer)

    manifest = writer.finalize(RUN_STATUS_SUCCESS).manifest
    assert set(manifest["objects"]) == {"bootstrap-static", "fixtures"}
    assert manifest["totals"]["written"] == 2

    manifests = list((tmp_path / "raw").rglob("manifest.json"))
    assert len(manifests) == 1, f"one manifest per run, found {manifests}"


# ---------------------------------------------------------------------------
# Shape validation
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "raw_kwargs, expected_failure",
    [
        pytest.param({"status": 500, "body": b"{}"}, "http_status_2xx", id="non_2xx"),
        pytest.param({"body": b"<html>oops</html>"}, "body_parses_as_json", id="not_json"),
        pytest.param({"payload": [1, 2, 3]}, "top_level_is_object", id="not_an_object"),
        pytest.param(
            {"payload": {"elements": [], "teams": []}},
            "required_top_level_keys_present",
            id="missing_top_level_keys",
        ),
        pytest.param(
            {"payload": {**_VALID_PAYLOAD, "elements": [{"id": 1}]}},
            "sampled_record_has_identifying_fields",
            id="missing_identifying_fields",
        ),
    ],
)
def test_validate_bootstrap_shape_flags_each_structural_failure(raw_kwargs, expected_failure):
    verdict = validate_bootstrap_shape(_raw(**raw_kwargs))

    assert verdict["ok"] is False
    assert any(f.startswith(expected_failure) for f in verdict["failures"]), verdict


def test_validate_bootstrap_shape_passes_a_well_formed_payload():
    verdict = validate_bootstrap_shape(_raw(_VALID_PAYLOAD))

    assert verdict["ok"] is True
    assert verdict["failures"] == []
    assert verdict["record_count"] == 2


def test_validate_bootstrap_shape_requires_the_documented_keys():
    """The key list is strategy doc B.2's, not a superset of convenience."""
    for key in ("elements", "teams", "events", "element_types"):
        payload = {k: v for k, v in _VALID_PAYLOAD.items() if k != key}
        verdict = validate_bootstrap_shape(_raw(payload))
        assert verdict["ok"] is False, key
        assert any(key in f for f in verdict["failures"]), verdict


def test_validate_bootstrap_shape_accepts_an_empty_elements_list():
    """An empty elements list is not a shape failure at this boundary."""
    verdict = validate_bootstrap_shape(_raw({**_VALID_PAYLOAD, "elements": []}))

    assert verdict["ok"] is True
    assert verdict["record_count"] == 0


def test_validate_bootstrap_shape_samples_rather_than_scans():
    """Only the first element is inspected; later ones are the warehouse's problem."""
    payload = {**_VALID_PAYLOAD, "elements": [_VALID_PAYLOAD["elements"][0], {"nonsense": True}]}
    verdict = validate_bootstrap_shape(_raw(payload))

    assert verdict["ok"] is True


def test_validate_bootstrap_shape_ignores_extra_top_level_keys():
    """The API adding a key is not a failure; only a missing one is."""
    verdict = validate_bootstrap_shape(_raw({**_VALID_PAYLOAD, "chips": [], "phases": []}))

    assert verdict["ok"] is True


@pytest.mark.asyncio
async def test_shape_failure_still_writes_the_payload_and_flags_the_manifest(tmp_path):
    """The one unrecoverable mistake here would be discarding a surprising payload."""
    raw = _raw(body=b"<html>502 Bad Gateway</html>")
    writer = _writer(tmp_path)

    outcome = await ingest_core_data(_client(raw), writer)

    root = tmp_path / "raw"
    payload_path = (
        root / "fpl" / "bootstrap-static" / writer.extraction_date / writer.run_id / "payload.json"
    )
    assert payload_path.read_bytes() == raw.body, "a failed payload must still be written"

    sidecar = _read(
        root, f"fpl/bootstrap-static/{writer.extraction_date}/{writer.run_id}/metadata.json"
    )
    assert sidecar["shape_validation"]["ok"] is False
    assert sidecar["shape_validation"]["failures"]

    assert outcome.result.errors == 0, "a shape failure is partial, not a hard error"
    assert outcome.result.skipped == 1

    status = classify_run_from_results([outcome.result], strict_mode=False)
    assert status == RUN_STATUS_FAILED_PARTIAL
    assert writer.finalize(status).manifest["status"] == RUN_STATUS_FAILED_PARTIAL


# ---------------------------------------------------------------------------
# The downstream handoff
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_capture_hands_events_and_element_ids_to_the_unmigrated_stages(tmp_path):
    outcome = await ingest_core_data(_client(_raw(_VALID_PAYLOAD)), _writer(tmp_path))

    core = outcome.output
    assert core.player_ids == [1, 2]
    assert [event.id for event in core.events] == [1, 2]
    assert [event.is_current for event in core.events] == [False, True]


def test_core_handoff_is_empty_for_an_unusable_payload():
    assert core_handoff(_raw(body=b"<html>nope</html>")) == ([], [])
    assert core_handoff(_raw([1, 2, 3])) == ([], [])


def test_core_handoff_survives_a_payload_that_failed_shape_validation():
    """A degraded downstream stage beats an exception; nothing here is persisted."""
    core = core_handoff(_raw({"elements": [{"id": 7}], "events": "not a list"}))

    assert core.player_ids == [7]
    assert core.events == []


# ---------------------------------------------------------------------------
# Failure and skip paths
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_fetch_failure_records_a_manifest_failure_and_writes_no_payload(tmp_path):
    writer = _writer(tmp_path)

    outcome = await ingest_core_data(_client(error=FPLClientError("unreachable")), writer)

    assert outcome.result.errors == 1
    assert outcome.output == ([], [])
    assert not (tmp_path / "raw" / "fpl" / "bootstrap-static").exists()

    manifest = writer.manifest_snapshot
    assert manifest["objects"]["bootstrap-static"]["failed"] == 1
    assert manifest["failures"][0]["endpoint"] == "bootstrap-static"
    assert manifest["failures"][0]["error_class"] == "FPLClientError"


@pytest.mark.asyncio
async def test_fail_fast_skips_the_capture_entirely(tmp_path):
    from fpl_ingest.orchestration.execution_state import PipelineExecutionState

    state = PipelineExecutionState()
    state.fail()
    client = _client(_raw(_VALID_PAYLOAD))
    writer = _writer(tmp_path)

    outcome = await ingest_core_data(client, writer, execution_state=state)

    client.get_bootstrap_raw.assert_not_called()
    assert outcome.result.fetched == 0
    assert outcome.output == ([], [])
    assert not (tmp_path / "raw" / "fpl").exists()


# ---------------------------------------------------------------------------
# The retired SQLite path
# ---------------------------------------------------------------------------


def test_sqlite_upsert_helpers_are_gone():
    """These were removed with the flatten/upsert retirement — they must not return."""
    for name in (
        "process_core_payload",
        "ingest_players",
        "ingest_teams",
        "ingest_events",
        "ingest_element_types",
        "_assert_store_validation_consistency",
    ):
        assert not hasattr(bootstrap_stage, name), f"{name} must stay removed"


def test_stage_declares_no_output_tables():
    assert bootstrap_stage.CORE_STAGE.output_tables == ()
