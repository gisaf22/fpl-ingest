"""A hard bootstrap-static failure must be distinguishable, downstream, from
a genuine "core succeeded with nothing new" result.

Before this fix, ``ingest_core_data`` returned ``CoreData(events=[],
player_ids=[])`` identically on both a caught ``FPLClientError`` and a
legitimately empty bootstrap payload, and never tripped the fail-fast
sentinel on the error path. ``ingest_gameweeks`` and ``ingest_player_histories``
then had no way to tell the two cases apart: both logged the same
"nothing to fetch" message (or, for player histories, nothing at all).

The fix makes ``ingest_core_data`` call ``execution_state.fail()`` on the
``FPLClientError`` path, which is the same sentinel ``ingest_gameweeks`` and
``ingest_player_histories`` already check and already log distinctly for
("Fail-fast tripped; skipping ..."). This test wires the three real stage
functions together (no mocked stage outcomes) and asserts the two scenarios
now produce different log signals, while confirming the run's terminal
classification was — and remains — correctly FAILED vs. SUCCESS in both
cases, driven by the core stage's own ``errors`` count rather than by this
fix.
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import MagicMock

import pytest

from fpl_ingest.extract.http.client import RawResponse
from fpl_ingest.extract.http.local_writer import LocalRawWriter
from fpl_ingest.extract.http.sync_http import FPLClientError
from fpl_ingest.extract.stages.bootstrap import ingest_core_data
from fpl_ingest.extract.stages.element_summary import ingest_player_histories
from fpl_ingest.extract.stages.gameweeks import ingest_gameweeks
from fpl_ingest.orchestration.execution_state import PipelineExecutionState
from fpl_ingest.orchestration.run_status import (
    RUN_STATUS_FAILED,
    RUN_STATUS_SUCCESS,
    classify_run,
)

pytestmark = pytest.mark.asyncio


def _writer(tmp_path: Path, run_id: str) -> LocalRawWriter:
    return LocalRawWriter(tmp_path / "raw", "fpl", run_id=run_id)


def _hard_failing_core_client() -> MagicMock:
    async def _raise(*_a, **_k):
        raise FPLClientError("unreachable")

    client = MagicMock()
    client.get_bootstrap_raw = _raise
    return client


def _empty_success_core_client() -> MagicMock:
    requested_at = datetime(2026, 8, 24, 8, 0, 0, tzinfo=timezone.utc)
    raw = RawResponse(
        url="https://fantasy.premierleague.com/api/bootstrap-static/",
        status=200,
        headers={"content-type": "application/json"},
        body=b'{"events": [], "elements": [], "teams": [], "element_types": []}',
        requested_at=requested_at,
        received_at=requested_at,
        attempt_count=1,
    )

    async def _return_raw(*_a, **_k):
        return raw

    client = MagicMock()
    client.get_bootstrap_raw = _return_raw
    return client


class TestCoreFailureIsDistinctFromGenuineEmptyResult:
    async def test_hard_bootstrap_failure_trips_fail_fast_for_every_downstream_stage(
        self, tmp_path, caplog
    ):
        caplog.set_level(logging.INFO)
        state = PipelineExecutionState()
        writer = _writer(tmp_path, "20260824T080000Z-fa17ed")

        core_outcome = await ingest_core_data(
            _hard_failing_core_client(), writer, execution_state=state
        )

        # The core stage itself already reported the failure distinctly
        # (errors=1) before this fix — that part was never the gap.
        assert core_outcome.result.errors == 1
        assert core_outcome.output.events == []
        assert core_outcome.output.player_ids == []

        # This is the actual fix: the shared fail-fast sentinel is now
        # tripped on a hard core failure, not left at its initial state.
        assert state.is_failed is True

        gw_outcome = await ingest_gameweeks(
            MagicMock(),
            writer,
            core_outcome.output.events,
            event_finality=None,
            execution_state=state,
        )
        hist_outcome = await ingest_player_histories(
            MagicMock(),
            writer,
            core_outcome.output.player_ids,
            core_outcome.output.events,
            event_finality=None,
            execution_state=state,
        )

        assert "Fail-fast tripped; skipping gameweek capture" in caplog.text
        assert "Fail-fast tripped; skipping element-summary capture" in caplog.text
        # Neither of the ambiguous "nothing new" messages should appear —
        # downstream stages never reach that branch once fail-fast is tripped.
        assert "No newly ratified gameweeks; nothing to capture." not in caplog.text
        assert "no players known this run; nothing to fetch" not in caplog.text

        assert gw_outcome.result.errors == 0 and gw_outcome.result.skipped == 0
        assert hist_outcome.result.errors == 0 and hist_outcome.result.skipped == 0

        status = classify_run(writer.endpoint_outcomes)
        assert status == RUN_STATUS_FAILED

    async def test_genuine_empty_bootstrap_success_does_not_trip_fail_fast(
        self, tmp_path, caplog
    ):
        caplog.set_level(logging.INFO)
        state = PipelineExecutionState()
        writer = _writer(tmp_path, "20260824T080000Z-abc123")

        core_outcome = await ingest_core_data(
            _empty_success_core_client(), writer, execution_state=state
        )

        assert core_outcome.result.errors == 0
        assert core_outcome.output.events == []
        assert core_outcome.output.player_ids == []

        # Same empty CoreData as the hard-failure case, but this time the
        # sentinel must stay clean — this run genuinely had nothing to do.
        assert state.is_failed is False

        gw_outcome = await ingest_gameweeks(
            MagicMock(),
            writer,
            core_outcome.output.events,
            event_finality=None,
            execution_state=state,
        )
        hist_outcome = await ingest_player_histories(
            MagicMock(),
            writer,
            core_outcome.output.player_ids,
            core_outcome.output.events,
            event_finality=None,
            execution_state=state,
        )

        assert "Fail-fast tripped" not in caplog.text
        assert "No newly ratified gameweeks; nothing to capture." in caplog.text
        assert "no players known this run; nothing to fetch" in caplog.text

        status = classify_run(writer.endpoint_outcomes)
        assert status == RUN_STATUS_SUCCESS
