"""Unit tests for the element-summary raw-capture stage.

What it must guarantee:
  - each fetched player's response body reaches raw storage byte-for-byte,
    under source ``fpl`` and endpoint ``element-summary/{player_id}``;
  - the sidecar carries the capture metadata the raw contract promises;
  - a payload that fails shape validation is still written, flagged in the
    sidecar, and reported so the run manifest reads FAILED_PARTIAL — without
    discounting the players that captured cleanly;
  - the concurrent fetch and its strict-mode cancellation still behave as
    they did before the redirect;
  - many players share one run manifest;
  - the retired flatten/upsert helpers and the dead ``force`` parameter never
    return;
  - nothing in this module can upsert rows.
"""

from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock

import pytest

from fpl_ingest.extract.http.client import RawResponse
from fpl_ingest.extract.http.local_writer import LocalFilesystemBackend, LocalRawWriter
from fpl_ingest.extract.http.raw_keys import iso_utc
from fpl_ingest.extract.http.s3_backend import S3Backend
from fpl_ingest.extract.http.sync_http import FPLClientError
from fpl_ingest.extract.stages import element_summary as element_summary_stage
from fpl_ingest.extract.stages import event_status as event_status_stage
from fpl_ingest.extract.stages.bootstrap import GameweekInfo
from fpl_ingest.extract.stages.element_summary import (
    _select_players_to_fetch,
    ingest_player_histories,
    raw_endpoint,
    validate_element_summary_shape,
)
from fpl_ingest.orchestration.execution_state import PipelineExecutionState
from fpl_ingest.orchestration.run_status import (
    RUN_STATUS_FAILED_PARTIAL,
    RUN_STATUS_SUCCESS,
    classify_run_from_results,
)
from tests.support.fixture_payloads import payload_bytes

pytestmark = pytest.mark.unit


def _player_url(player_id: int) -> str:
    return f"https://fantasy.premierleague.com/api/element-summary/{player_id}/"


def _payload(player_id: int, *, history: list | None = None) -> dict:
    return {
        "history": (
            history
            if history is not None
            else [
                {
                    "element": player_id,
                    "round": 1,
                    "fixture": 100 + player_id,
                    "minutes": 90,
                    "total_points": player_id,
                }
            ]
        ),
        "fixtures": [{"id": 500 + player_id, "difficulty": 3, "is_home": True}],
        "history_past": [],
    }


def _raw(
    payload: object | None = None,
    *,
    player_id: int = 1,
    status: int = 200,
    body: bytes | None = None,
    headers: dict[str, str] | None = None,
    attempt_count: int = 1,
) -> RawResponse:
    requested_at = datetime(2026, 8, 24, 8, 0, 0, tzinfo=timezone.utc)
    if body is None:
        body = json.dumps(payload).encode("utf-8")
    return RawResponse(
        url=_player_url(player_id),
        status=status,
        headers=headers or {"content-type": "application/json", "etag": 'W/"abc"'},
        body=body,
        requested_at=requested_at,
        received_at=requested_at + timedelta(seconds=1),
        attempt_count=attempt_count,
    )


def _client(responses: dict[int, RawResponse | Exception]) -> MagicMock:
    """Client whose raw element-summary fetch is driven by a per-player mapping."""

    async def get_element_summary_raw(player_id: int) -> RawResponse:
        outcome = responses[player_id]
        if isinstance(outcome, Exception):
            raise outcome
        return outcome

    client = MagicMock()
    client.get_element_summary_raw = AsyncMock(side_effect=get_element_summary_raw)
    return client


def _writer(tmp_path: Path) -> LocalRawWriter:
    return LocalRawWriter(tmp_path / "raw", "fpl", run_id="20260824T080000Z-abc123")


class _FakeS3Client:
    """Minimal in-memory stand-in for a boto3 S3 client, only ``list_objects_v2``
    (what ``S3Backend.exists_prefix`` calls) is exercised here."""

    def __init__(self) -> None:
        self.objects: dict[str, bytes] = {}

    def list_objects_v2(self, *, Bucket: str, Prefix: str, MaxKeys: int = 1000) -> dict:
        matches = [key for key in self.objects if key.startswith(Prefix)][:MaxKeys]
        return {"Contents": [{"Key": key} for key in matches]} if matches else {}


def _backend(tmp_path: Path) -> LocalFilesystemBackend:
    """A backend over ``tmp_path`` matching ``_select_players_to_fetch``'s
    ``backend`` parameter, so existing directory-based capture fixtures keep
    working unchanged."""
    return LocalFilesystemBackend(tmp_path)


def _read(root: Path, key: str) -> dict:
    return json.loads((root / Path(key)).read_text(encoding="utf-8"))


def _event(event_id: int, *, finished: bool, is_current: bool = True) -> GameweekInfo:
    return GameweekInfo(id=event_id, finished=finished, is_current=is_current)


def _settled(*event_ids: int) -> dict:
    return {event_id: {"points": "r", "bonus_added": True} for event_id in event_ids}


def _provisional(*event_ids: int) -> dict:
    return {event_id: {"points": "p", "bonus_added": False} for event_id in event_ids}


def _event_status_raw(body: bytes) -> RawResponse:
    """Wrap an event-status payload body for ``event_status._parse_finality``."""
    requested_at = datetime(2026, 8, 28, 6, 0, 0, tzinfo=timezone.utc)
    return RawResponse(
        url="https://fantasy.premierleague.com/api/event-status/",
        status=200,
        headers={"content-type": "application/json"},
        body=body,
        requested_at=requested_at,
        received_at=requested_at + timedelta(seconds=1),
        attempt_count=1,
    )


def _finality_from_fixture(name: str) -> dict:
    """Parse a real (or hand-edited) event-status fixture the way the stage would."""
    return event_status_stage._parse_finality(_event_status_raw(payload_bytes(name)))


# ---------------------------------------------------------------------------
# Happy path
# ---------------------------------------------------------------------------


# ---------------------------------------------------------------------------
# Shape validation
# ---------------------------------------------------------------------------


# ---------------------------------------------------------------------------
# Fetch failures, strict mode, fail-fast
# ---------------------------------------------------------------------------


# ---------------------------------------------------------------------------
# The retired SQLite path and the dead `force` parameter
# ---------------------------------------------------------------------------


class TestShapeValidation:
    """Shape-validation verdicts for this endpoint's payload."""

    @pytest.mark.parametrize(
        "raw_kwargs, expected_failure",
        [
            pytest.param({"status": 500, "body": b"{}"}, "http_status_2xx", id="non_2xx"),
            pytest.param({"body": b"<html>oops</html>"}, "body_parses_as_json", id="not_json"),
            pytest.param({"payload": [1, 2]}, "top_level_is_object", id="not_an_object"),
            pytest.param(
                {"payload": {"history": [], "history_past": []}},
                "required_top_level_keys_present",
                id="missing_fixtures_key",
            ),
            pytest.param(
                {"payload": {"fixtures": [], "history_past": []}},
                "required_top_level_keys_present",
                id="missing_history_key",
            ),
            pytest.param(
                {"payload": {"history": {}, "fixtures": [], "history_past": []}},
                "required_top_level_keys_present",
                id="history_not_a_list",
            ),
            pytest.param(
                {
                    "payload": {
                        "history": [{"element": 1}],
                        "fixtures": [],
                        "history_past": [],
                    }
                },
                "sampled_record_has_identifying_fields",
                id="missing_identifying_fields",
            ),
        ],
    )
    def test_validate_element_summary_shape_flags_each_structural_failure(self, raw_kwargs, expected_failure):
        verdict = validate_element_summary_shape(_raw(**raw_kwargs))

        assert verdict["ok"] is False
        assert any(f.startswith(expected_failure) for f in verdict["failures"]), verdict

    def test_validate_element_summary_shape_passes_a_well_formed_payload(self):
        verdict = validate_element_summary_shape(_raw(_payload(1)))

        assert verdict["ok"] is True
        assert verdict["failures"] == []
        assert verdict["record_count"] == 1

    def test_validate_element_summary_shape_accepts_an_empty_history_list(self):
        """A player with no fixtures played yet is not a shape failure."""
        verdict = validate_element_summary_shape(
            _raw({"history": [], "fixtures": [], "history_past": []})
        )

        assert verdict["ok"] is True
        assert verdict["record_count"] == 0

    def test_validate_element_summary_shape_samples_rather_than_scans(self):
        """Only the first history row is inspected; later ones are the warehouse's problem."""
        payload = _payload(1, history=[
            {"element": 1, "round": 1, "fixture": 101, "minutes": 90, "total_points": 5},
            {"nonsense": True},
        ])

        assert validate_element_summary_shape(_raw(payload))["ok"] is True

    @pytest.mark.asyncio
    async def test_shape_failure_still_writes_the_payload_and_flags_the_manifest(self, tmp_path):
        """The one unrecoverable mistake here would be discarding a surprising payload."""
        writer = _writer(tmp_path)
        raw = _raw(player_id=1, body=b"<html>502 Bad Gateway</html>")

        outcome = await ingest_player_histories(
            _client({1: raw}), writer, [1], [], event_finality=None
        )

        payload_path = (
            tmp_path / "raw" / "fpl" / "element-summary" / "1" / writer.extraction_date / writer.run_id / "payload.json"
        )
        assert payload_path.read_bytes() == raw.body, "a failed payload must still be written"

        sidecar = _read(
            tmp_path / "raw",
            f"fpl/element-summary/1/{writer.extraction_date}/{writer.run_id}/metadata.json",
        )
        assert sidecar["shape_validation"]["ok"] is False
        assert sidecar["shape_validation"]["failures"]

        assert outcome.result.errors == 0, "a shape failure is partial, not a hard error"
        assert outcome.result.skipped == 1

        status = classify_run_from_results([outcome.result], strict_mode=False)
        assert status == RUN_STATUS_FAILED_PARTIAL
        assert writer.finalize(status).manifest["status"] == RUN_STATUS_FAILED_PARTIAL


class TestSelectionLogic:
    """Which players this stage decides to fetch, and which it skips.

    Existence-based skip, gated on the *current* gameweek's settlement.
    Unlike gameweeks (settled/provisional decided per gameweek), this is a
    single season-wide fact applied uniformly across players: existence alone
    only decides the outcome once the current gameweek is settled.

    The last test additionally checks that ``ingest_player_histories`` wires
    the selection through to the client, on top of the unit coverage for the
    selection rule itself.
    """

    def test_settled_and_captured_player_is_skipped(self, tmp_path):
        (tmp_path / "fpl" / "element-summary" / "1").mkdir(parents=True)

        assert _select_players_to_fetch(
            _backend(tmp_path), [1], [_event(1, finished=True)], event_finality=_settled(1)
        ) == []

    def test_settled_but_never_captured_player_is_fetched(self, tmp_path):
        assert _select_players_to_fetch(
            _backend(tmp_path), [1], [_event(1, finished=True)], event_finality=_settled(1)
        ) == [1]

    def test_provisional_gameweek_refetches_even_if_already_captured(self, tmp_path):
        (tmp_path / "fpl" / "element-summary" / "1").mkdir(parents=True)

        assert _select_players_to_fetch(
            _backend(tmp_path), [1], [_event(1, finished=False)], event_finality=_provisional(1)
        ) == [1]

    def test_unknown_finality_fetches_everyone(self, tmp_path):
        (tmp_path / "fpl" / "element-summary" / "1").mkdir(parents=True)

        assert _select_players_to_fetch(
            _backend(tmp_path), [1, 2], [_event(1, finished=True)], event_finality=None
        ) == [1, 2]

    def test_mixed_capture_state_only_fetches_the_uncaptured(self, tmp_path):
        (tmp_path / "fpl" / "element-summary" / "1").mkdir(parents=True)

        assert _select_players_to_fetch(
            _backend(tmp_path), [1, 2], [_event(1, finished=True)], event_finality=_settled(1)
        ) == [2]

    @pytest.mark.asyncio
    async def test_settled_and_captured_player_is_not_fetched(self, tmp_path):
        raw_dir = tmp_path / "raw"
        (raw_dir / "fpl" / "element-summary" / "1").mkdir(parents=True)

        client = _client({})
        outcome = await ingest_player_histories(
            client,
            _writer(tmp_path),
            [1],
            [_event(1, finished=True)],
            event_finality=_settled(1),
        )

        client.get_element_summary_raw.assert_not_called()
        assert outcome.result.fetched == 0

    @pytest.mark.asyncio
    async def test_second_run_after_settlement_refetches_no_players(self, tmp_path):
        """A run captures a player; a later run, once the gameweek settles, must not refetch it."""
        raw_dir = tmp_path / "raw"
        writer = _writer(tmp_path)
        player_id = 1
        real_payload = payload_bytes("element_summary_settled_capture")
        events = [_event(1, finished=True)]

        first_run_client = _client(
            {player_id: _raw(body=real_payload, player_id=player_id)}
        )
        first_outcome = await ingest_player_histories(
            first_run_client, writer, [player_id], events,
            event_finality=_provisional(1),
        )
        first_run_client.get_element_summary_raw.assert_called_once()
        assert first_outcome.result.fetched == 1

        settled_finality = _finality_from_fixture("event_status_settled")
        assert settled_finality[1]["bonus_added"] is True

        second_run_client = _client({})
        second_outcome = await ingest_player_histories(
            second_run_client, _writer(tmp_path), [player_id], events,
            event_finality=settled_finality,
        )

        second_run_client.get_element_summary_raw.assert_not_called()
        assert second_outcome.result.fetched == 0

    @pytest.mark.regression
    def test_empty_finality_map_does_not_skip_every_player(self, tmp_path):
        """Pins the bug in ``_latest_gameweek_settled``: an empty (not None)
        finality map for the current gameweek used to be read the same way as
        an old gameweek's dates having rolled out of event-status's window —
        i.e. "settled" — which silently skipped every already-captured
        player even though settlement is actually unknown. An empty map must
        be treated like ``event_finality is None``: fetch everyone.
        """
        (tmp_path / "fpl" / "element-summary" / "1").mkdir(parents=True)

        empty_finality = _finality_from_fixture("event_status_empty_map")
        assert empty_finality == {}

        assert _select_players_to_fetch(
            _backend(tmp_path), [1], [_event(1, finished=True)], event_finality=empty_finality
        ) == [1]

    @pytest.mark.regression
    def test_old_gameweek_absent_from_nonempty_map_is_still_settled(self, tmp_path):
        """Old-gameweek regression check for the empty-map fix above.

        The new "empty map means unknown" guard in ``_latest_gameweek_settled``
        must only fire on a fully empty ``event_finality`` dict. A *non-empty*
        map that simply has no entry for the current gameweek — because its
        dates have rolled out of event-status's current-window array, the
        normal state for a gameweek settled well in the past — must still
        read as settled and skip an already-captured player, exactly as
        before this pass's change.
        """
        (tmp_path / "fpl" / "element-summary" / "1").mkdir(parents=True)

        finality_missing_current_gameweek = _settled(2)  # non-empty, but no entry for event 1

        assert _select_players_to_fetch(
            _backend(tmp_path), [1], [_event(1, finished=True)],
            event_finality=finality_missing_current_gameweek,
        ) == []

    @pytest.mark.regression
    def test_s3_backed_aged_out_player_is_recognized_as_already_captured(self):
        """Pins the storage-backend-bypass bug shared with
        ``gameweeks._has_event_live_capture``: ``_has_element_summary_capture``
        used to do a raw ``Path.is_dir()`` check against the local filesystem,
        no matter which backend the run was actually configured to use.
        Against S3 that check was always False, so a player genuinely
        captured in S3, once the current gameweek's finality entry rolls out
        of event-status's window, could never be recognized as already
        captured. Also pins the prefix boundary: player 1's capture must not
        be read as satisfying player 11's — S3 prefix matching is
        string-based, not path-segment-based, so an unguarded prefix check
        would incorrectly treat ``element-summary/1`` as a match for a key
        under ``element-summary/11``."""
        s3_client = _FakeS3Client()
        s3_client.objects["raw/fpl/element-summary/1/2026-08-10/20260810T080000Z-aaaaaa/payload.json"] = b"{}"
        backend = S3Backend("fpl-data-safari", client=s3_client)

        assert _select_players_to_fetch(
            backend, [1, 11], [_event(1, finished=True)], event_finality=_settled(2)
        ) == [11]

    @pytest.mark.parametrize(
        "event_finality",
        [
            pytest.param(None, id="unknown_finality"),
            pytest.param(_provisional(1), id="provisional_gameweek"),
            pytest.param(_settled(1), id="settled_gameweek"),
        ],
    )
    def test_new_player_absent_from_prior_run_is_always_fetched(self, tmp_path, event_finality):
        """A player with no capture directory must never be skipped, regardless of settlement."""
        (tmp_path / "fpl" / "element-summary" / "1").mkdir(parents=True)
        new_player_id = 2

        result = _select_players_to_fetch(
            _backend(tmp_path), [1, new_player_id], [_event(1, finished=True)], event_finality=event_finality
        )

        assert new_player_id in result


class TestFetchAndCapture:
    """The happy path: bytes reach raw storage with correct metadata."""

    @pytest.mark.asyncio
    async def test_ingest_player_histories_writes_a_raw_object_per_player(self, tmp_path):
        writer = _writer(tmp_path)
        responses = {1: _raw(_payload(1), player_id=1), 2: _raw(_payload(2), player_id=2)}

        outcome = await ingest_player_histories(
            _client(responses), writer, [1, 2], [], event_finality=None
        )

        root = tmp_path / "raw"
        for pid in (1, 2):
            payload_path = (
                root / "fpl" / f"element-summary/{pid}" / writer.extraction_date / writer.run_id / "payload.json"
            )
            assert payload_path.exists(), "payload must land under element-summary/{player_id}"
            assert payload_path.read_bytes() == responses[pid].body, "bytes must be stored verbatim"

        assert outcome.result.stage == "player_histories"
        assert outcome.result.fetched == 2
        assert outcome.result.written == 2
        assert outcome.result.skipped == 0
        assert outcome.result.errors == 0

    @pytest.mark.asyncio
    async def test_ingest_player_histories_sidecar_carries_capture_metadata(self, tmp_path):
        writer = _writer(tmp_path)
        raw = _raw(_payload(7), player_id=7, attempt_count=3)

        await ingest_player_histories(
            _client({7: raw}), writer, [7], [], event_finality=None
        )

        sidecar = _read(
            tmp_path / "raw",
            f"fpl/element-summary/7/{writer.extraction_date}/{writer.run_id}/metadata.json",
        )
        assert sidecar["source"] == "fpl"
        assert sidecar["endpoint"] == "element-summary/7"
        assert sidecar["request_url"] == _player_url(7)
        assert sidecar["http_status"] == 200
        assert sidecar["requested_at"] == iso_utc(raw.requested_at)
        assert sidecar["received_at"] == iso_utc(raw.received_at)
        assert sidecar["content_length"] == len(raw.body)
        assert sidecar["attempt_count"] == 3
        assert sidecar["response_headers"]["etag"] == 'W/"abc"'
        assert sidecar["shape_validation"]["ok"] is True

    @pytest.mark.asyncio
    async def test_all_players_share_one_run_manifest(self, tmp_path):
        writer = _writer(tmp_path)
        responses = {pid: _raw(_payload(pid), player_id=pid) for pid in (1, 2, 3)}

        outcome = await ingest_player_histories(
            _client(responses), writer, [1, 2, 3], [], event_finality=None
        )

        status = classify_run_from_results([outcome.result], strict_mode=False)
        assert status == RUN_STATUS_SUCCESS

        manifest = writer.finalize(status).manifest
        assert set(manifest["objects"]) == {
            "element-summary/1",
            "element-summary/2",
            "element-summary/3",
        }
        assert all(counts["written"] == 1 for counts in manifest["objects"].values())
        assert manifest["totals"]["written"] == 3
        assert manifest["totals"]["failed"] == 0

    @pytest.mark.asyncio
    async def test_lineage_lists_every_captured_payload_key(self, tmp_path):
        writer = _writer(tmp_path)
        responses = {pid: _raw(_payload(pid), player_id=pid) for pid in (1, 2)}

        outcome = await ingest_player_histories(
            _client(responses), writer, [1, 2], [], event_finality=None
        )

        assert outcome.lineage is not None
        assert outcome.lineage.output_tables == ()
        assert [key.split("/")[1] for key in outcome.lineage.raw_artifacts] == [
            "element-summary",
            "element-summary",
        ]
        assert all(key.endswith("payload.json") for key in outcome.lineage.raw_artifacts)


class TestErrorHandling:
    """Fetch failures, partial failures, and the fail-fast sentinel."""

    @pytest.mark.asyncio
    async def test_one_bad_player_does_not_discount_the_good_ones(self, tmp_path):
        writer = _writer(tmp_path)
        responses = {
            1: _raw(_payload(1), player_id=1),
            2: _raw(player_id=2, body=b"not json at all"),
            3: _raw(_payload(3), player_id=3),
        }

        outcome = await ingest_player_histories(
            _client(responses), writer, [1, 2, 3], [], event_finality=None
        )

        assert outcome.result.fetched == 3
        assert outcome.result.written == 2
        assert outcome.result.skipped == 1

        manifest = writer.manifest_snapshot
        assert manifest["totals"]["written"] == 3, "every payload is written, good or bad"

    @pytest.mark.asyncio
    async def test_non_strict_failure_captures_the_other_players(self, tmp_path):
        writer = _writer(tmp_path)
        responses: dict = {1: FPLClientError("network down"), 2: _raw(_payload(2), player_id=2)}

        outcome = await ingest_player_histories(
            _client(responses), writer, [1, 2], [], event_finality=None, strict=False
        )

        assert outcome.result.errors == 1
        assert outcome.result.written == 1
        assert not (tmp_path / "raw" / "fpl" / "element-summary" / "1").exists()
        assert (tmp_path / "raw" / "fpl" / "element-summary" / "2").exists()

        manifest = writer.manifest_snapshot
        assert manifest["objects"]["element-summary/1"]["failed"] == 1
        assert manifest["objects"]["element-summary/2"]["written"] == 1
        assert manifest["failures"][0]["endpoint"] == "element-summary/1"
        assert manifest["failures"][0]["error_class"] == "FPLClientError"

    @pytest.mark.asyncio
    async def test_strict_failure_writes_nothing_and_trips_the_fail_fast_sentinel(self, tmp_path):
        writer = _writer(tmp_path)
        state = PipelineExecutionState()
        responses: dict = {1: FPLClientError("network down"), 2: _raw(_payload(2), player_id=2)}

        outcome = await ingest_player_histories(
            _client(responses), writer, [1, 2], [],
            event_finality=None, strict=True, execution_state=state
        )

        assert state.is_failed
        assert outcome.result.errors == 1
        assert outcome.result.written == 0
        assert not (tmp_path / "raw" / "fpl" / "element-summary" / "1").exists()
        assert writer.manifest_snapshot["totals"]["written"] == 0

    @pytest.mark.asyncio
    async def test_fail_fast_skips_the_capture_entirely(self, tmp_path):
        state = PipelineExecutionState()
        state.fail()
        client = _client({1: _raw(_payload(1), player_id=1)})
        writer = _writer(tmp_path)

        outcome = await ingest_player_histories(
            client, writer, [1], [], event_finality=None, execution_state=state
        )

        client.get_element_summary_raw.assert_not_called()
        assert outcome.result.fetched == 0
        assert not (tmp_path / "raw" / "fpl").exists()

    @pytest.mark.asyncio
    async def test_not_settled_gameweek_logs_fetching_all_not_already_captured(self, tmp_path, caplog):
        """Regression: the not-settled branch must not claim players were
        skipped because the gameweek settled — it fetched everyone precisely
        because it isn't settled yet."""
        writer = _writer(tmp_path)
        responses = {1: _raw(_payload(1), player_id=1)}

        with caplog.at_level("INFO", logger=element_summary_stage.__name__):
            await ingest_player_histories(
                _client(responses), writer, [1],
                [_event(1, finished=False)], event_finality=_provisional(1),
            )

        messages = [r.message for r in caplog.records]
        assert any("gameweek not yet settled, fetching all 1 players" in m for m in messages)
        assert not any("already captured, latest gameweek settled" in m for m in messages)

    @pytest.mark.asyncio
    async def test_no_player_ids_captures_nothing(self, tmp_path):
        client = _client({})

        outcome = await ingest_player_histories(
            client, _writer(tmp_path), [], [], event_finality=None
        )

        client.get_element_summary_raw.assert_not_called()
        assert outcome.result.fetched == 0
        assert outcome.result.errors == 0


class TestStageContract:
    """Stage-level invariants: key layout, declared tables, retired API."""

    def test_raw_endpoint_puts_player_id_before_extraction_date(self):
        """strategy doc A.3: {player_id} is part of the endpoint segment."""
        assert raw_endpoint(115) == "element-summary/115"

    def test_sqlite_upsert_helpers_are_gone(self):
        """These were removed with the flatten/upsert retirement — they must not return."""
        for name in ("raw_history_rows", "upsert_history_rows"):
            assert not hasattr(element_summary_stage, name), f"{name} must stay removed"

    @pytest.mark.asyncio
    async def test_ingest_player_histories_has_no_force_parameter(self, tmp_path):
        """strategy doc §4.2: `force` was dead code — never read — and is not ported."""
        import inspect

        signature = inspect.signature(ingest_player_histories)
        assert "force" not in signature.parameters

    def test_stage_declares_no_output_tables(self):
        assert element_summary_stage.PLAYER_HISTORIES_STAGE.output_tables == ()
