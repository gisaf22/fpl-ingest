"""Unit tests for the gameweek raw-capture stage.

The stage no longer writes SQLite. What it must guarantee now:
  - each fetched gameweek's response body reaches raw storage byte-for-byte,
    under source ``fpl`` and endpoint ``event-live/{gw:02d}``;
  - the sidecar carries the capture metadata the raw contract promises;
  - a payload that fails shape validation is still written, flagged in the
    sidecar, and reported so the run manifest reads FAILED_PARTIAL — without
    discounting the gameweeks that captured cleanly;
  - the concurrent fetch and its strict-mode cancellation still behave as they
    did;
  - ``_select_gameweeks_to_fetch`` decides by event-status finality, not
    ``gw_{n}.json`` file-existence (strategy doc 4.2): a finished gameweek is
    fetched unless event-status reports it settled AND it has already been
    captured at least once, and a missing finality signal fails safe by
    fetching everything uncertain rather than silently under-fetching;
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
from fpl_ingest.extract.stages import gameweeks as gameweeks_stage
from fpl_ingest.extract.stages.gameweeks import (
    _select_gameweeks_to_fetch,
    ingest_gameweeks,
    raw_endpoint,
    validate_gameweek_shape,
)
from fpl_ingest.orchestration.execution_state import PipelineExecutionState
from fpl_ingest.orchestration.run_status import (
    RUN_STATUS_FAILED_PARTIAL,
    RUN_STATUS_SUCCESS,
    classify_run_from_results,
)
from fpl_ingest.extract.stages.bootstrap import GameweekInfo

pytestmark = pytest.mark.unit


def _live_url(gw: int) -> str:
    return f"https://fantasy.premierleague.com/api/event/{gw}/live/"


def _payload(player_ids: list[int]) -> dict:
    return {
        "elements": [
            {
                "id": player_id,
                "stats": {"minutes": 90, "total_points": player_id},
                "explain": [],
            }
            for player_id in player_ids
        ]
    }


def _raw(
    payload: object | None = None,
    *,
    gw: int = 1,
    status: int = 200,
    body: bytes | None = None,
    headers: dict[str, str] | None = None,
    attempt_count: int = 1,
) -> RawResponse:
    requested_at = datetime(2026, 8, 24, 8, 0, 0, tzinfo=timezone.utc)
    if body is None:
        body = json.dumps(payload).encode("utf-8")
    return RawResponse(
        url=_live_url(gw),
        status=status,
        headers=headers or {"content-type": "application/json", "etag": 'W/"abc"'},
        body=body,
        requested_at=requested_at,
        received_at=requested_at + timedelta(seconds=1),
        attempt_count=attempt_count,
    )


def _client(responses: dict[int, RawResponse | Exception]) -> MagicMock:
    """Client whose raw live fetch is driven by a per-gameweek mapping."""

    async def get_gameweek_live_raw(gameweek: int) -> RawResponse:
        outcome = responses[gameweek]
        if isinstance(outcome, Exception):
            raise outcome
        return outcome

    client = MagicMock()
    client.get_gameweek_live_raw = AsyncMock(side_effect=get_gameweek_live_raw)
    return client


def _writer(tmp_path: Path) -> LocalRawWriter:
    return LocalRawWriter(tmp_path / "raw", "fpl", run_id="20260824T080000Z-abc123")


def _read(root: Path, key: str) -> dict:
    return json.loads((root / Path(key)).read_text(encoding="utf-8"))


def _event(event_id: int, *, finished: bool, is_current: bool = False) -> GameweekInfo:
    return GameweekInfo(id=event_id, finished=finished, is_current=is_current)


# ---------------------------------------------------------------------------
# Happy path
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_ingest_gameweeks_writes_a_raw_object_per_gameweek(tmp_path):
    raw_dir = tmp_path / "raw"
    raw_dir.mkdir()
    writer = _writer(tmp_path)
    responses = {1: _raw(_payload([1, 2]), gw=1), 2: _raw(_payload([3]), gw=2)}

    outcome = await ingest_gameweeks(
        _client(responses),
        writer,
        raw_dir,
        [_event(1, finished=True), _event(2, finished=True)],
        force=False,
        event_finality=None,
    )

    for gw in (1, 2):
        payload_path = (
            raw_dir / "fpl" / f"event-live/{gw:02d}" / writer.extraction_date / writer.run_id / "payload.json"
        )
        assert payload_path.exists(), "payload must land under event-live/{gw:02d}"
        assert payload_path.read_bytes() == responses[gw].body, "bytes must be stored verbatim"

    assert outcome.result.stage == "gameweeks"
    assert outcome.result.fetched == 2
    assert outcome.result.written == 2
    assert outcome.result.skipped == 0
    assert outcome.result.errors == 0


def test_raw_endpoint_zero_pads_the_gameweek():
    """Zero padding keeps a plain lexicographic listing numerically ordered."""
    assert raw_endpoint(2) == "event-live/02"
    assert raw_endpoint(38) == "event-live/38"
    assert sorted([raw_endpoint(2), raw_endpoint(10)]) == ["event-live/02", "event-live/10"]


@pytest.mark.asyncio
async def test_ingest_gameweeks_sidecar_carries_capture_metadata(tmp_path):
    raw_dir = tmp_path / "raw"
    raw_dir.mkdir()
    writer = _writer(tmp_path)
    raw = _raw(_payload([1]), gw=7, attempt_count=3)

    await ingest_gameweeks(
        _client({7: raw}), writer, raw_dir, [_event(7, finished=True)], force=False, event_finality=None
    )

    sidecar = _read(
        raw_dir, f"fpl/event-live/07/{writer.extraction_date}/{writer.run_id}/metadata.json"
    )
    assert sidecar["source"] == "fpl"
    assert sidecar["endpoint"] == "event-live/07"
    assert sidecar["request_url"] == _live_url(7)
    assert sidecar["http_status"] == 200
    assert sidecar["requested_at"] == iso_utc(raw.requested_at)
    assert sidecar["received_at"] == iso_utc(raw.received_at)
    assert sidecar["content_length"] == len(raw.body)
    assert sidecar["attempt_count"] == 3
    assert sidecar["response_headers"]["etag"] == 'W/"abc"'
    assert sidecar["shape_validation"]["ok"] is True
    assert sidecar["shape_validation"]["record_count"] == 1


@pytest.mark.asyncio
async def test_all_gameweeks_share_one_run_manifest(tmp_path):
    raw_dir = tmp_path / "raw"
    raw_dir.mkdir()
    writer = _writer(tmp_path)
    responses = {gw: _raw(_payload([gw]), gw=gw) for gw in (1, 2, 3)}

    outcome = await ingest_gameweeks(
        _client(responses),
        writer,
        raw_dir,
        [_event(gw, finished=True) for gw in (1, 2, 3)],
        force=False,
        event_finality=None,
    )

    status = classify_run_from_results([outcome.result], strict_mode=False)
    assert status == RUN_STATUS_SUCCESS

    manifest = writer.finalize(status).manifest
    assert set(manifest["objects"]) == {"event-live/01", "event-live/02", "event-live/03"}
    assert all(counts["written"] == 1 for counts in manifest["objects"].values())
    assert manifest["totals"]["written"] == 3
    assert manifest["totals"]["failed"] == 0


@pytest.mark.asyncio
async def test_lineage_lists_every_captured_payload_key(tmp_path):
    raw_dir = tmp_path / "raw"
    raw_dir.mkdir()
    writer = _writer(tmp_path)
    responses = {gw: _raw(_payload([gw]), gw=gw) for gw in (1, 2)}

    outcome = await ingest_gameweeks(
        _client(responses),
        writer,
        raw_dir,
        [_event(1, finished=True), _event(2, finished=True)],
        force=False,
        event_finality=None,
    )

    assert outcome.lineage is not None
    assert outcome.lineage.output_tables == ()
    assert [key.split("/")[1] for key in outcome.lineage.raw_artifacts] == [
        "event-live",
        "event-live",
    ]
    assert all(key.endswith("payload.json") for key in outcome.lineage.raw_artifacts)


# ---------------------------------------------------------------------------
# Shape validation
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "raw_kwargs, expected_failure",
    [
        pytest.param({"status": 500, "body": b"{}"}, "http_status_2xx", id="non_2xx"),
        pytest.param({"body": b"<html>oops</html>"}, "body_parses_as_json", id="not_json"),
        pytest.param({"payload": [1, 2]}, "top_level_is_object", id="not_an_object"),
        pytest.param({"payload": {}}, "required_top_level_keys_present", id="no_elements_key"),
        pytest.param(
            {"payload": {"elements": {}}},
            "required_top_level_keys_present",
            id="elements_not_a_list",
        ),
        pytest.param(
            {"payload": {"elements": [{"id": 1}]}},
            "sampled_record_has_identifying_fields",
            id="missing_identifying_fields",
        ),
    ],
)
def test_validate_gameweek_shape_flags_each_structural_failure(raw_kwargs, expected_failure):
    verdict = validate_gameweek_shape(_raw(**raw_kwargs))

    assert verdict["ok"] is False
    assert any(f.startswith(expected_failure) for f in verdict["failures"]), verdict


def test_validate_gameweek_shape_passes_a_well_formed_payload():
    verdict = validate_gameweek_shape(_raw(_payload([1, 2])))

    assert verdict["ok"] is True
    assert verdict["failures"] == []
    assert verdict["record_count"] == 2


def test_validate_gameweek_shape_accepts_an_empty_elements_list():
    """An unplayed gameweek is not a shape failure — the football decides that."""
    verdict = validate_gameweek_shape(_raw({"elements": []}))

    assert verdict["ok"] is True
    assert verdict["record_count"] == 0


def test_validate_gameweek_shape_samples_rather_than_scans():
    """Only the first element is inspected; later ones are the warehouse's problem."""
    payload = _payload([1])
    payload["elements"].append({"nonsense": True})

    assert validate_gameweek_shape(_raw(payload))["ok"] is True


def test_validate_gameweek_shape_ignores_extra_top_level_keys():
    """Only ``elements`` is required; the API may add siblings without failing us."""
    payload = dict(_payload([1]), extra_block={"anything": 1})

    assert validate_gameweek_shape(_raw(payload))["ok"] is True


@pytest.mark.asyncio
async def test_shape_failure_still_writes_the_payload_and_flags_the_manifest(tmp_path):
    """The one unrecoverable mistake here would be discarding a surprising payload."""
    raw_dir = tmp_path / "raw"
    raw_dir.mkdir()
    writer = _writer(tmp_path)
    raw = _raw(gw=1, body=b"<html>502 Bad Gateway</html>")

    outcome = await ingest_gameweeks(
        _client({1: raw}), writer, raw_dir, [_event(1, finished=True)], force=False, event_finality=None
    )

    payload_path = (
        raw_dir / "fpl" / "event-live" / "01" / writer.extraction_date / writer.run_id / "payload.json"
    )
    assert payload_path.read_bytes() == raw.body, "a failed payload must still be written"

    sidecar = _read(
        raw_dir, f"fpl/event-live/01/{writer.extraction_date}/{writer.run_id}/metadata.json"
    )
    assert sidecar["shape_validation"]["ok"] is False
    assert sidecar["shape_validation"]["failures"]

    assert outcome.result.errors == 0, "a shape failure is partial, not a hard error"
    assert outcome.result.skipped == 1

    status = classify_run_from_results([outcome.result], strict_mode=False)
    assert status == RUN_STATUS_FAILED_PARTIAL
    assert writer.finalize(status).manifest["status"] == RUN_STATUS_FAILED_PARTIAL


@pytest.mark.asyncio
async def test_one_bad_gameweek_does_not_discount_the_good_ones(tmp_path):
    """Partial failure is per object; the other gameweeks still count as written."""
    raw_dir = tmp_path / "raw"
    raw_dir.mkdir()
    writer = _writer(tmp_path)
    responses = {
        1: _raw(_payload([1]), gw=1),
        2: _raw(gw=2, body=b"not json at all"),
        3: _raw(_payload([3]), gw=3),
    }

    outcome = await ingest_gameweeks(
        _client(responses),
        writer,
        raw_dir,
        [_event(gw, finished=True) for gw in (1, 2, 3)],
        force=False,
        event_finality=None,
    )

    assert outcome.result.fetched == 3
    assert outcome.result.written == 2
    assert outcome.result.skipped == 1

    manifest = writer.manifest_snapshot
    assert manifest["totals"]["written"] == 3, "every payload is written, good or bad"
    assert _read(
        raw_dir, f"fpl/event-live/01/{writer.extraction_date}/{writer.run_id}/metadata.json"
    )["shape_validation"]["ok"] is True
    assert _read(
        raw_dir, f"fpl/event-live/02/{writer.extraction_date}/{writer.run_id}/metadata.json"
    )["shape_validation"]["ok"] is False


# ---------------------------------------------------------------------------
# Fetch failures, strict mode, fail-fast
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_non_strict_failure_captures_the_other_gameweeks(tmp_path):
    raw_dir = tmp_path / "raw"
    raw_dir.mkdir()
    writer = _writer(tmp_path)
    responses: dict = {1: FPLClientError("network down"), 2: _raw(_payload([20, 21]), gw=2)}

    outcome = await ingest_gameweeks(
        _client(responses),
        writer,
        raw_dir,
        [_event(1, finished=True), _event(2, finished=True)],
        force=False,
        event_finality=None,
        strict=False,
    )

    assert outcome.result.errors == 1
    assert outcome.result.written == 1
    assert not (raw_dir / "fpl" / "event-live" / "01").exists()
    assert (raw_dir / "fpl" / "event-live" / "02").exists()

    manifest = writer.manifest_snapshot
    assert manifest["objects"]["event-live/01"]["failed"] == 1
    assert manifest["objects"]["event-live/02"]["written"] == 1
    assert manifest["failures"][0]["endpoint"] == "event-live/01"
    assert manifest["failures"][0]["error_class"] == "FPLClientError"


@pytest.mark.asyncio
async def test_strict_failure_writes_nothing_and_trips_the_fail_fast_sentinel(tmp_path):
    raw_dir = tmp_path / "raw"
    raw_dir.mkdir()
    writer = _writer(tmp_path)
    state = PipelineExecutionState()
    responses: dict = {1: FPLClientError("network down"), 2: _raw(_payload([1]), gw=2)}

    outcome = await ingest_gameweeks(
        _client(responses),
        writer,
        raw_dir,
        [_event(1, finished=True), _event(2, finished=True)],
        force=False,
        event_finality=None,
        strict=True,
        execution_state=state,
    )

    assert state.is_failed
    assert outcome.result.errors == 1
    assert outcome.result.written == 0
    assert not (raw_dir / "fpl" / "event-live" / "01").exists()
    assert writer.manifest_snapshot["totals"]["written"] == 0


@pytest.mark.asyncio
async def test_fail_fast_skips_the_capture_entirely(tmp_path):
    raw_dir = tmp_path / "raw"
    raw_dir.mkdir()
    state = PipelineExecutionState()
    state.fail()
    client = _client({1: _raw(_payload([1]), gw=1)})
    writer = _writer(tmp_path)

    outcome = await ingest_gameweeks(
        client, writer, raw_dir, [_event(1, finished=True)], force=False, event_finality=None, execution_state=state
    )

    client.get_gameweek_live_raw.assert_not_called()
    assert outcome.result.fetched == 0
    assert not (raw_dir / "fpl").exists()


@pytest.mark.asyncio
async def test_no_events_captures_nothing(tmp_path):
    raw_dir = tmp_path / "raw"
    raw_dir.mkdir()
    client = _client({})

    outcome = await ingest_gameweeks(client, _writer(tmp_path), raw_dir, [], force=False, event_finality=None)

    client.get_gameweek_live_raw.assert_not_called()
    assert outcome.result.fetched == 0
    assert outcome.result.errors == 0


# ---------------------------------------------------------------------------
# Finality-driven selection — replaces the file-existence heuristic
# ---------------------------------------------------------------------------


def _settled(*event_ids: int) -> dict:
    return {event_id: {"points": "r", "bonus_added": True} for event_id in event_ids}


def _provisional(*event_ids: int) -> dict:
    return {event_id: {"points": "p", "bonus_added": False} for event_id in event_ids}


class TestSelectGameweeksToFetch:
    """``_select_gameweeks_to_fetch`` now decides by event-status finality
    (strategy doc 4.2/A.5), not ``gw_{n}.json`` file-existence. A finished
    gameweek is fetched unless event-status reports it settled AND it has
    already been captured at least once.
    """

    def test_never_captured_gameweek_is_fetched_even_if_reported_settled(self, tmp_path):
        """A settled gameweek that was somehow never captured must still be fetched."""
        assert _select_gameweeks_to_fetch(
            tmp_path, [_event(1, finished=True)], force=False, event_finality=_settled(1)
        ) == [1]

    def test_captured_but_provisional_gameweek_is_refetched(self, tmp_path):
        (tmp_path / "fpl" / "event-live" / "01").mkdir(parents=True)

        assert _select_gameweeks_to_fetch(
            tmp_path, [_event(1, finished=True)], force=False, event_finality=_provisional(1)
        ) == [1]

    def test_captured_and_settled_gameweek_is_skipped(self, tmp_path):
        (tmp_path / "fpl" / "event-live" / "01").mkdir(parents=True)

        assert _select_gameweeks_to_fetch(
            tmp_path, [_event(1, finished=True)], force=False, event_finality=_settled(1)
        ) == []

    def test_missing_finality_signal_fetches_all_uncertain_gameweeks(self, tmp_path):
        """event-status fetch failure (``event_finality=None``) must fail safe:
        fetch every finished gameweek rather than silently skip any of them,
        even one that was captured settled by a previous run."""
        (tmp_path / "fpl" / "event-live" / "01").mkdir(parents=True)

        assert _select_gameweeks_to_fetch(
            tmp_path,
            [_event(1, finished=True), _event(2, finished=True)],
            force=False,
            event_finality=None,
        ) == [1, 2]

    def test_force_refetches_an_already_settled_gameweek(self, tmp_path):
        """``--force`` still serves a purpose: forcing a refetch of a settled GW."""
        (tmp_path / "fpl" / "event-live" / "01").mkdir(parents=True)

        assert _select_gameweeks_to_fetch(
            tmp_path, [_event(1, finished=True)], force=True, event_finality=_settled(1)
        ) == [1]

    def test_current_unfinished_gameweek_is_always_included(self, tmp_path):
        (tmp_path / "fpl" / "event-live" / "01").mkdir(parents=True)
        events = [_event(1, finished=True), _event(2, finished=False, is_current=True)]

        assert _select_gameweeks_to_fetch(
            tmp_path, events, force=False, event_finality=_settled(1)
        ) == [2]

    def test_current_gameweek_is_not_duplicated_when_already_selected(self, tmp_path):
        events = [_event(1, finished=True, is_current=True)]

        assert _select_gameweeks_to_fetch(
            tmp_path, events, force=False, event_finality=None
        ) == [1]


@pytest.mark.asyncio
async def test_a_settled_and_already_captured_gameweek_is_never_fetched(tmp_path):
    raw_dir = tmp_path / "raw"
    (raw_dir / "fpl" / "event-live" / "01").mkdir(parents=True)
    client = _client({})

    outcome = await ingest_gameweeks(
        client,
        _writer(tmp_path),
        raw_dir,
        [_event(1, finished=True)],
        force=False,
        event_finality=_settled(1),
    )

    client.get_gameweek_live_raw.assert_not_called()
    assert outcome.result.fetched == 0


# ---------------------------------------------------------------------------
# The retired SQLite path
# ---------------------------------------------------------------------------


def test_sqlite_upsert_helpers_are_gone():
    """These were removed with the flatten/upsert retirement — they must not return."""
    for name in ("upsert_gameweek_rows", "process_gameweek_payloads"):
        assert not hasattr(gameweeks_stage, name), f"{name} must stay removed"


def test_stage_declares_no_output_tables():
    assert gameweeks_stage.GAMEWEEKS_STAGE.output_tables == ()
