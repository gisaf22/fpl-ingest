"""A picks or history capture is checked against its committed baseline (#116 AC8).

Captures through the real stages (``ingest_entry_picks``, ``ingest_manager_panel``)
into a real ``LocalRawWriter`` pointed at the committed ``schemas/payload-baseline/``,
with only the FPL client faked, then reads the sidecar the writer stored and checks the
CONTENT of its ``drift`` block. A conforming real-shaped sample is ``ok``; a mutated one
is ``drift`` with exactly the entries the mutation caused. If a family had no committed
baseline, the block would say ``unavailable`` (or be absent), and the first test fails.
"""

from __future__ import annotations

import copy
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Callable
from unittest.mock import AsyncMock, MagicMock

import pytest

from fpl_ingest.extract.http.client import RawResponse
from fpl_ingest.extract.http.local_writer import LocalRawWriter
from fpl_ingest.extract.stages.entry_picks import COHORT_SELF, ingest_entry_picks
from fpl_ingest.extract.stages.manager_panel import ingest_manager_panel
from fpl_ingest.schema.payload_baseline import BASELINE_DIR
from tests.support.fixture_payloads import payload_json

pytestmark = pytest.mark.asyncio

SEASON = "2026-27"
SELF_ID = 8450051
GW = {"id": 4, "finished": True, "data_checked": True}
AT = datetime(2026, 10, 20, 7, 0, tzinfo=timezone.utc)

# A real picks week with a non-empty automatic_subs, and a real history, as captured.
PICKS = payload_json("entry_picks_automatic_subs")
HISTORY = payload_json("entry_history_self")


def _raw(payload: object) -> RawResponse:
    return RawResponse(
        url="https://fantasy.premierleague.com/api/x/", status=200,
        headers={"content-type": "application/json"}, body=json.dumps(payload).encode(),
        requested_at=AT, received_at=AT,
    )


def _writer(root: Path) -> LocalRawWriter:
    return LocalRawWriter(root, "fpl", baseline_dir=BASELINE_DIR)


async def _capture_picks(root: Path, payload: object) -> dict:
    client = MagicMock(get_entry_picks_raw=AsyncMock(return_value=_raw(payload)))
    await ingest_entry_picks(
        client, _writer(root), season=SEASON, entries=[(SELF_ID, COHORT_SELF)], gameweeks=[GW]
    )
    return _sidecar(root, "entry-picks")


async def _capture_history(root: Path, payload: object) -> dict:
    client = MagicMock(get_entry_history_raw=AsyncMock(return_value=_raw(payload)))
    # panel_size 0: no random ids, so the only history fetched is the self cohort's.
    await ingest_manager_panel(
        client, _writer(root), season=SEASON, total_players=12_000_000, seed=1, panel_size=0,
        self_cohort=(SELF_ID,),
    )
    return _sidecar(root, "entry-history")


def _sidecar(root: Path, family: str) -> dict:
    [path] = (root / "fpl" / family).rglob("metadata.json")
    return json.loads(path.read_text())


def _shape(drift: dict) -> list[tuple]:
    """The drift entries reduced to what the mutation determines: path, kind and the type change."""
    return [(e["path"], e["kind"], e["baseline_types"], e["observed_types"]) for e in drift["entries"]]


def _picks_add_field(p: dict) -> None:
    for pick in p["picks"]:
        pick["new_field"] = "x"


def _picks_remove_block(p: dict) -> None:
    del p["entry_history"]


def _picks_change_type(p: dict) -> None:
    for pick in p["picks"]:
        pick["multiplier"] = str(pick["multiplier"])


def _history_add_field(p: dict) -> None:
    for row in p["current"]:
        row["new_field"] = 1


def _history_remove_block(p: dict) -> None:
    del p["chips"]


def _history_change_type(p: dict) -> None:
    for row in p["current"]:
        row["bank"] = str(row["bank"])


PICKS_CASES = [
    (_picks_add_field, [("$.picks[].new_field", "added", [], ["string"])]),
    (_picks_remove_block, [("$.entry_history", "removed", ["object"], [])]),
    (_picks_change_type, [("$.picks[].multiplier", "type_changed", ["number"], ["string"])]),
]
HISTORY_CASES = [
    (_history_add_field, [("$.current[].new_field", "added", [], ["number"])]),
    (_history_remove_block, [("$.chips", "removed", ["array"], [])]),
    (_history_change_type, [("$.current[].bank", "type_changed", ["number"], ["string"])]),
]


class TestConformingSample:

    @pytest.mark.covers("#116 AC8")
    async def test_a_real_picks_sample_is_ok_against_the_committed_baseline(self, tmp_path):
        sidecar = await _capture_picks(tmp_path, PICKS)

        assert sidecar["drift"] == {"status": "ok", "reason": None, "entries": []}

    @pytest.mark.covers("#116 AC8")
    async def test_a_real_history_sample_is_ok_against_the_committed_baseline(self, tmp_path):
        sidecar = await _capture_history(tmp_path, HISTORY)

        assert sidecar["drift"] == {"status": "ok", "reason": None, "entries": []}


class TestMutatedSample:

    @pytest.mark.covers("#116 AC8")
    @pytest.mark.parametrize(("mutate", "expected"), PICKS_CASES, ids=lambda c: getattr(c, "__name__", "expected"))
    async def test_a_changed_picks_payload_is_drift_with_exactly_the_changed_paths(
        self, tmp_path, mutate: Callable[[dict], None], expected: list[tuple]
    ):
        payload = copy.deepcopy(PICKS)
        mutate(payload)

        sidecar = await _capture_picks(tmp_path, payload)

        assert sidecar["drift"]["status"] == "drift"
        assert sidecar["drift"]["reason"] is None
        assert _shape(sidecar["drift"]) == expected

    @pytest.mark.covers("#116 AC8")
    @pytest.mark.parametrize(("mutate", "expected"), HISTORY_CASES, ids=lambda c: getattr(c, "__name__", "expected"))
    async def test_a_changed_history_payload_is_drift_with_exactly_the_changed_paths(
        self, tmp_path, mutate: Callable[[dict], None], expected: list[tuple]
    ):
        payload = copy.deepcopy(HISTORY)
        mutate(payload)

        sidecar = await _capture_history(tmp_path, payload)

        assert sidecar["drift"]["status"] == "drift"
        assert sidecar["drift"]["reason"] is None
        assert _shape(sidecar["drift"]) == expected

    @pytest.mark.covers("#116 AC8")
    async def test_the_added_field_entry_counts_every_pick_that_carries_it(self, tmp_path):
        payload = copy.deepcopy(PICKS)
        _picks_add_field(payload)

        sidecar = await _capture_picks(tmp_path, payload)

        [entry] = sidecar["drift"]["entries"]
        assert entry["count"] == len(payload["picks"]) == 15


class TestWithoutABaseline:

    @pytest.mark.covers("#116 AC8")
    async def test_a_missing_baseline_is_unavailable_not_a_silent_ok(self, tmp_path):
        client = MagicMock(get_entry_picks_raw=AsyncMock(return_value=_raw(PICKS)))
        writer = LocalRawWriter(tmp_path / "raw", "fpl", baseline_dir=tmp_path / "no-baselines")
        await ingest_entry_picks(
            client, writer, season=SEASON, entries=[(SELF_ID, COHORT_SELF)], gameweeks=[GW]
        )

        sidecar = _sidecar(tmp_path / "raw", "entry-picks")

        assert sidecar["drift"]["status"] == "unavailable"
        assert "entry-picks.json not found" in sidecar["drift"]["reason"]
