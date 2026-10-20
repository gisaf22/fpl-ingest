"""Picks capture key layout, eligibility, idempotence and non-2xx cohort handling (#116).

Drives ``ingest_entry_picks`` directly against a real ``LocalRawWriter`` over
``tmp_path``, with only the FPL client faked — the same style as
``tests/integration/orchestration/test_settlement_marker_independence.py``.

These tests are written against the stub in
``fpl_ingest.extract.stages.entry_picks`` (#116 stage 2: red tests before the
real implementation) and are expected to FAIL until that stage is built.
"""

from __future__ import annotations

import json
import re
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock

import pytest

from fpl_ingest.extract.http.client import RawResponse
from fpl_ingest.extract.http.local_writer import LocalRawWriter
from fpl_ingest.extract.http.sync_http import FPLClientError
from fpl_ingest.orchestration.run_status import classify_run
from fpl_ingest.extract.stages.entry_picks import (
    COHORT_RANDOM,
    COHORT_SELF,
    ingest_entry_picks,
    raw_endpoint,
)

pytestmark = pytest.mark.asyncio

SEASON = "2026-27"
SELF_ID = 8450051
RANDOM_A = 11
RANDOM_B = 12
RANDOM_C = 13

GW1 = {"id": 1, "finished": True, "data_checked": True}
GW2 = {"id": 2, "finished": True, "data_checked": True}
GW_FINISHED_ONLY = {"id": 3, "finished": True, "data_checked": False}
GW_NOT_FINISHED = {"id": 4, "finished": False, "data_checked": False}

ENTRIES = [(RANDOM_A, COHORT_RANDOM), (SELF_ID, COHORT_SELF)]

PICKS_15 = {"picks": [{"element": i, "position": i, "multiplier": 1} for i in range(1, 16)]}


def _raw(status: int, body: bytes) -> RawResponse:
    at = datetime(2026, 10, 19, 4, 30, tzinfo=timezone.utc)
    return RawResponse(
        url="https://fantasy.premierleague.com/api/entry/x/event/y/picks/",
        status=status, headers={"content-type": "application/json"}, body=body,
        requested_at=at, received_at=at, attempt_count=3 if status in (429, 500) else 1,
    )


def _ok() -> RawResponse:
    return _raw(200, json.dumps(PICKS_15).encode())


def _client(responses: dict[tuple[int, int], object]):
    """``responses[(gw, entry_id)]`` is a RawResponse or an Exception; anything else gets a clean 200."""
    calls: list[tuple[int, int]] = []

    async def _fetch(gw: int, entry_id: int) -> RawResponse:
        calls.append((gw, entry_id))
        what = responses.get((gw, entry_id))
        if isinstance(what, Exception):
            raise what
        if isinstance(what, RawResponse):
            return what
        return _ok()

    client = MagicMock()
    client.get_entry_picks_raw = AsyncMock(side_effect=_fetch)
    client.calls = calls
    return client


def _writer(root: Path, run_id: str) -> LocalRawWriter:
    return LocalRawWriter(root, "fpl", run_id=run_id)


def _manifest(writer: LocalRawWriter, root: Path) -> dict:
    # Status comes from the endpoint outcomes, as the runner derives it; a hard-coded status would make every status assertion vacuous.
    result = writer.finalize(classify_run(writer.endpoint_outcomes))
    return json.loads((root / result.manifest_key).read_text())


def _files_under(root: Path, prefix: str) -> list[str]:
    return sorted(str(p.relative_to(root)) for p in (root / prefix).rglob("*") if p.is_file())


class TestKeyLayout:

    @pytest.mark.covers("#116 AC1")
    async def test_picks_land_at_exactly_the_decided_key(self, tmp_path):
        root = tmp_path / "raw"
        writer = _writer(root, "20261019T043000Z-aaaaaa")
        client = _client({})

        await ingest_entry_picks(client, writer, season=SEASON, entries=ENTRIES, gameweeks=[GW1])

        files = _files_under(root, "fpl/entry-picks")
        pattern = re.compile(
            rf"^fpl/entry-picks/{SEASON}/1/(\d+)/\d{{4}}-\d{{2}}-\d{{2}}/"
            r"\d{8}T\d{6}Z-[0-9a-f]{6}/(payload\.json|metadata\.json)$"
        )
        assert files, "expected at least one entry-picks capture to have been written"
        assert all(pattern.match(f) for f in files)


class TestEligibility:

    @pytest.mark.covers("#116 AC2")
    async def test_only_finished_and_data_checked_gameweeks_are_captured(self, tmp_path):
        root = tmp_path / "raw"
        writer = _writer(root, "20261019T043000Z-bbbbbb")
        client = _client({})

        await ingest_entry_picks(
            client, writer, season=SEASON, entries=ENTRIES,
            gameweeks=[GW1, GW_FINISHED_ONLY, GW_NOT_FINISHED],
        )

        requested_gws = {gw for gw, _ in client.calls}
        assert requested_gws == {1}, f"expected only gw1 requested, got {requested_gws}"

    @pytest.mark.covers("#116 AC2")
    async def test_eligible_gameweeks_are_captured_oldest_first(self, tmp_path):
        root = tmp_path / "raw"
        writer = _writer(root, "20261019T043000Z-cccccc")
        client = _client({})

        await ingest_entry_picks(client, writer, season=SEASON, entries=ENTRIES, gameweeks=[GW2, GW1])

        order = [gw for gw, entry in client.calls if entry == RANDOM_A]
        assert order == sorted(order), f"expected oldest-first dispatch, got {order}"
        assert order, "expected at least one request for the random entry"


class TestIdempotentRerun:

    @pytest.mark.covers("#116 AC3")
    async def test_a_second_run_captures_nothing_new(self, tmp_path):
        root = tmp_path / "raw"
        first = _client({})
        await ingest_entry_picks(
            MagicMock(get_entry_picks_raw=first.get_entry_picks_raw),
            _writer(root, "20261019T043000Z-dddddd"),
            season=SEASON, entries=ENTRIES, gameweeks=[GW1],
        )
        assert first.calls, "expected the first run to actually capture something before testing idempotence"

        second = _client({})
        await ingest_entry_picks(
            second, _writer(root, "20261019T050000Z-eeeeee"),
            season=SEASON, entries=ENTRIES, gameweeks=[GW1],
        )

        assert second.calls == [], f"expected a completed capture to fetch nothing on rerun, got {second.calls}"


class TestNon2xxCohortHandling:

    @pytest.mark.covers("#116 AC9")
    async def test_a_random_cohort_404_is_stored_as_not_found_not_payload_and_does_not_fail_the_run(self, tmp_path):
        root = tmp_path / "raw"
        writer = _writer(root, "20261019T043000Z-ffffff")
        client = _client({(1, RANDOM_A): _raw(404, b'{"detail":"Not found."}')})

        # RANDOM_B is clean: "nothing else failed" needs something usable, because classify_run
        # reads a run with no usable capture as FAILED even when every outcome was expected.
        await ingest_entry_picks(
            client, writer, season=SEASON, entries=[(RANDOM_A, COHORT_RANDOM), (RANDOM_B, COHORT_RANDOM)], gameweeks=[GW1],
        )
        manifest = _manifest(writer, root)

        assert any(f"/{RANDOM_A}/" in f for f in _files_under(root, "fpl/_invalid/not_found"))
        assert not any(f"/{RANDOM_A}/" in f for f in _files_under(root, "fpl/entry-picks"))
        assert manifest["status"] == "SUCCESS"
        assert manifest.get("invalid_counts", {}).get("not_found", 0) == 1

    @pytest.mark.covers("#116 AC9")
    async def test_a_rerun_skips_the_random_cohort_404_via_exists_prefix(self, tmp_path):
        root = tmp_path / "raw"
        first = _client({(1, RANDOM_A): _raw(404, b'{"detail":"Not found."}')})
        await ingest_entry_picks(
            first, _writer(root, "20261019T043000Z-a00007"),
            season=SEASON, entries=[(RANDOM_A, COHORT_RANDOM)], gameweeks=[GW1],
        )

        second = _client({})
        await ingest_entry_picks(
            second, _writer(root, "20261019T050000Z-a00008"),
            season=SEASON, entries=[(RANDOM_A, COHORT_RANDOM)], gameweeks=[GW1],
        )

        assert second.calls == [], "a stored not_found 404 should count as captured and not be retried"

    @pytest.mark.covers("#116 AC10")
    @pytest.mark.parametrize("response, label", [
        (_raw(404, b'{"detail":"Not found."}'), "self_404"),
        (_raw(403, b'{"detail":"forbidden"}'), "random_403"),
    ], ids=["self_404", "random_403"])
    async def test_a_self_404_or_a_non_404_4xx_is_http_error_retried_and_blocks_the_marker(self, tmp_path, response, label):
        root = tmp_path / "raw"
        entry_id, cohort = (SELF_ID, COHORT_SELF) if label == "self_404" else (RANDOM_B, COHORT_RANDOM)
        writer = _writer(root, "20261019T043000Z-a00009")
        client = _client({(1, entry_id): response})

        await ingest_entry_picks(client, writer, season=SEASON, entries=[(entry_id, cohort)], gameweeks=[GW1])
        manifest = _manifest(writer, root)

        assert any(f"/{entry_id}/" in f for f in _files_under(root, "fpl/_invalid/http_error"))
        assert not (root / "fpl" / "_settlement" / "entry-picks" / "1" / "marker.json").exists()
        assert manifest["status"] != "SUCCESS"

        rerun_client = _client({})
        await ingest_entry_picks(
            rerun_client, _writer(root, "20261019T050000Z-a0000a"),
            season=SEASON, entries=[(entry_id, cohort)], gameweeks=[GW1],
        )
        assert (1, entry_id) in rerun_client.calls, "an http_error entry must be retried, not skipped"

    @pytest.mark.covers("#116 AC10")
    async def test_a_network_error_or_5xx_stores_nothing_and_blocks_the_marker(self, tmp_path):
        root = tmp_path / "raw"
        writer = _writer(root, "20261019T043000Z-a0000b")
        client = _client({(1, RANDOM_A): FPLClientError("503 after retries")})

        await ingest_entry_picks(client, writer, season=SEASON, entries=[(RANDOM_A, COHORT_RANDOM)], gameweeks=[GW1])

        assert not any(f"/{RANDOM_A}/" in f for f in _files_under(root, "fpl"))
        assert not (root / "fpl" / "_settlement" / "entry-picks" / "1" / "marker.json").exists()


class TestSpikeCount:

    @pytest.mark.covers("#116 AC11")
    async def test_the_manifest_carries_a_per_gameweek_count_of_stored_non_2xx_responses(self, tmp_path):
        root = tmp_path / "raw"
        writer = _writer(root, "20261019T043000Z-a0000c")
        client = _client({
            (1, RANDOM_A): _raw(404, b'{"detail":"Not found."}'),
            (1, RANDOM_B): _raw(403, b'{"detail":"forbidden"}'),
        })

        await ingest_entry_picks(
            client, writer, season=SEASON,
            entries=[(RANDOM_A, COHORT_RANDOM), (RANDOM_B, COHORT_RANDOM)], gameweeks=[GW1],
        )
        manifest = _manifest(writer, root)

        counts = manifest.get("endpoints", {}).get("entry-picks", {}).get("non_2xx_by_gameweek")
        assert counts == {"1": 2}, f"expected a per-gameweek non-2xx count, got {counts!r}"


class TestResumeGranularityPerClass:

    @pytest.mark.covers("#116 AC13")
    async def test_not_found_is_skipped_but_shape_invalid_and_http_error_are_retried_and_block_the_marker(self, tmp_path):
        root = tmp_path / "raw"
        a, b, c = RANDOM_A, RANDOM_B, RANDOM_C
        first = _client({
            (1, a): _raw(404, b'{"detail":"Not found."}'),
            (1, b): _raw(200, b'{"picks": []}'),
            (1, c): _raw(403, b'{"detail":"forbidden"}'),
        })
        await ingest_entry_picks(
            first, _writer(root, "20261019T043000Z-a0000d"),
            season=SEASON, entries=[(a, COHORT_RANDOM), (b, COHORT_RANDOM), (c, COHORT_RANDOM)], gameweeks=[GW1],
        )

        marker = root / "fpl" / "_settlement" / "entry-picks" / "1" / "marker.json"
        assert not marker.exists(), "B and C are unresolved, so the gameweek must not settle"

        second = _client({})
        await ingest_entry_picks(
            second, _writer(root, "20261019T050000Z-b00010"),
            season=SEASON, entries=[(a, COHORT_RANDOM), (b, COHORT_RANDOM), (c, COHORT_RANDOM)], gameweeks=[GW1],
        )

        requested = {entry for _, entry in second.calls}
        assert a not in requested, "a stored not_found capture must not be re-requested"
        assert b in requested, "a stored shape_invalid capture must be re-requested"
        assert c in requested, "a stored http_error capture must be re-requested"
        assert marker.exists(), "B and C now succeed (the second client answers 200), so the gameweek settles"


class TestCohortDependentFourOhFour:

    @pytest.mark.covers("#116 AC14")
    async def test_random_404_and_self_404_land_in_different_trees_with_different_outcomes(self, tmp_path):
        root = tmp_path / "raw"
        writer = _writer(root, "20261019T043000Z-a0000f")
        client = _client({
            (1, RANDOM_A): _raw(404, b'{"detail":"Not found."}'),
            (1, SELF_ID): _raw(404, b'{"detail":"Not found."}'),
        })

        await ingest_entry_picks(
            client, writer, season=SEASON,
            entries=[(RANDOM_A, COHORT_RANDOM), (SELF_ID, COHORT_SELF)], gameweeks=[GW1],
        )
        manifest = _manifest(writer, root)

        assert any(f"/{RANDOM_A}/" in f for f in _files_under(root, "fpl/_invalid/not_found"))
        assert any(f"/{SELF_ID}/" in f for f in _files_under(root, "fpl/_invalid/http_error"))
        assert manifest["status"] != "SUCCESS"
        marker = root / "fpl" / "_settlement" / "entry-picks" / "1" / "marker.json"
        assert not marker.exists(), "the self-cohort 404 is unresolved, so the gameweek must not settle"

        rerun = _client({})
        await ingest_entry_picks(
            rerun, _writer(root, "20261019T050000Z-b00010"),
            season=SEASON, entries=[(RANDOM_A, COHORT_RANDOM), (SELF_ID, COHORT_SELF)], gameweeks=[GW1],
        )
        requested = {entry for _, entry in rerun.calls}
        assert RANDOM_A not in requested
        assert SELF_ID in requested
        assert marker.exists(), "the self id now answers 200 and the random 404 counts as captured, so the gameweek settles"


class TestEmptyPicksBothCohorts:

    @pytest.mark.covers("#116 AC15")
    @pytest.mark.parametrize("entry_id, cohort", [(RANDOM_A, COHORT_RANDOM), (SELF_ID, COHORT_SELF)])
    async def test_an_empty_picks_200_is_shape_invalid_for_both_cohorts_and_blocks_the_marker(self, tmp_path, entry_id, cohort):
        root = tmp_path / "raw"
        writer = _writer(root, "20261019T043000Z-b00011")
        client = _client({(1, entry_id): _raw(200, b'{"picks": []}')})

        await ingest_entry_picks(client, writer, season=SEASON, entries=[(entry_id, cohort)], gameweeks=[GW1])

        assert any(f"/{entry_id}/" in f for f in _files_under(root, "fpl/_invalid/shape_invalid"))
        assert not any(f"/{entry_id}/" in f for f in _files_under(root, "fpl/entry-picks"))
        assert not (root / "fpl" / "_settlement" / "entry-picks" / "1" / "marker.json").exists()

        rerun = _client({})
        await ingest_entry_picks(
            rerun, _writer(root, "20261019T050000Z-b00012"),
            season=SEASON, entries=[(entry_id, cohort)], gameweeks=[GW1],
        )
        assert (1, entry_id) in rerun.calls, "a shape_invalid capture must be re-requested on rerun"
