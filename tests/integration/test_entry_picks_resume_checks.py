"""What the resume check costs: which trees it asks about, and when it asks at all (#116 AC3, AC9, AC13).

Counts the ``exists_prefix`` calls a real run makes against a recording backend. ADR 006
sizes the rerun at one or two calls per entry per unsettled gameweek and none for a settled
one; these tests hold the stage to that, because a wrong tree or a skipped marker check
returns the same captures at a different cost, which no capture-level assertion sees.
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock

import pytest

from fpl_ingest.extract.http.client import RawResponse
from fpl_ingest.extract.http.local_writer import LocalFilesystemBackend, LocalRawWriter
from fpl_ingest.extract.stages.entry_picks import COHORT_RANDOM, COHORT_SELF, ingest_entry_picks

pytestmark = pytest.mark.asyncio

SEASON = "2026-27"
GW1 = {"id": 1, "finished": True, "data_checked": True}
PICKS = json.dumps({"picks": [{"element": 1, "position": 1}]}).encode()
AT = datetime(2026, 10, 20, 7, 0, tzinfo=timezone.utc)


class RecordingBackend(LocalFilesystemBackend):
    def __init__(self, root):
        super().__init__(root)
        self.asked: list[str] = []

    def exists_prefix(self, prefix: str) -> bool:
        self.asked.append(prefix)
        return super().exists_prefix(prefix)


def _client(status: int = 200, body: bytes = PICKS):
    raw = RawResponse(url="https://x/", status=status, headers={}, body=body, requested_at=AT, received_at=AT)
    return MagicMock(get_entry_picks_raw=AsyncMock(return_value=raw))


async def _run(root, run_id: str, entries, *, client=None) -> RecordingBackend:
    backend = RecordingBackend(root)
    writer = LocalRawWriter(root, "fpl", run_id=run_id, backend=backend)
    await ingest_entry_picks(client or _client(), writer, season=SEASON, entries=entries, gameweeks=[GW1])
    return backend


class TestWhichTreesAreAsked:

    @pytest.mark.covers("#116 AC9")
    async def test_a_random_entry_with_no_capture_is_asked_about_the_payload_tree_then_the_not_found_tree(self, tmp_path):
        backend = await _run(tmp_path, "20261020T070000Z-a20001", [(11, COHORT_RANDOM)])

        assert backend.asked == [
            "fpl/_settlement/entry-picks/1",
            f"fpl/entry-picks/{SEASON}/1/11",
            f"fpl/_invalid/not_found/entry-picks/{SEASON}/1/11",
        ]

    @pytest.mark.covers("#116 AC9")
    async def test_a_self_entry_is_asked_about_the_payload_tree_only(self, tmp_path):
        backend = await _run(tmp_path, "20261020T070000Z-a20002", [(8450051, COHORT_SELF)])

        assert backend.asked == ["fpl/_settlement/entry-picks/1", f"fpl/entry-picks/{SEASON}/1/8450051"]

    @pytest.mark.covers("#116 AC13")
    async def test_neither_the_shape_invalid_nor_the_http_error_tree_is_ever_asked_about(self, tmp_path):
        first = await _run(tmp_path, "20261020T070000Z-a20003", [(11, COHORT_RANDOM)], client=_client(200, b'{"picks": []}'))
        again = await _run(tmp_path, "20261020T080000Z-a20004", [(11, COHORT_RANDOM)])

        assert not [p for p in first.asked + again.asked if "shape_invalid" in p or "http_error" in p]


class TestSettledGameweek:

    @pytest.mark.covers("#116 AC3")
    async def test_a_settled_gameweek_costs_one_existence_check_and_no_request(self, tmp_path):
        await _run(tmp_path, "20261020T070000Z-a20005", [(11, COHORT_RANDOM), (8450051, COHORT_SELF)])
        client = _client()

        backend = await _run(tmp_path, "20261020T080000Z-a20006", [(11, COHORT_RANDOM), (8450051, COHORT_SELF)], client=client)

        assert backend.asked == ["fpl/_settlement/entry-picks/1"]
        client.get_entry_picks_raw.assert_not_called()
