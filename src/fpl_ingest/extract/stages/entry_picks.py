"""Entry-picks raw-capture pipeline stage (#116).

STUB ONLY — no real behavior yet. This module exists so tests written ahead
of the implementation (stage 2, AGENT_WORKFLOW.md) can import real names and
fail for a missing-behavior reason rather than an ``ImportError``. None of
the resume, settlement-marker, budget, throughput-record, streak-abort or
cohort-aware absence handling described on #116 is implemented here.

Intended raw key (#116 D3, not yet produced by anything in this module)::

    fpl/entry-picks/{season}/{gw}/{entry_id}/{extraction_date}/{run_id}/payload.json

Cohort handling mirrors ``manager_panel.py``: a random-cohort entry's 404 is
expected (``absence_ok=True``); a self-cohort entry's 404 is not
(``absence_ok=False``), per #116 Q9 — neither is implemented here either.
"""

from __future__ import annotations

from typing import Any, Mapping

from fpl_ingest.extract.http.client import AsyncFPLClient, RawResponse
from fpl_ingest.extract.http.local_writer import LocalRawWriter
from fpl_ingest.orchestration.stage_result import StageMetadata, StageOutcome, StageResult

RAW_SOURCE = "fpl"
PICKS_FAMILY = "entry-picks"

COHORT_RANDOM = "random"
COHORT_SELF = "self"

#: The stage writes raw objects only, no tables.
ENTRY_PICKS_STAGE = StageMetadata(
    name="entry_picks", dependencies=("core",), raw_artifacts=(), output_tables=()
)


def raw_endpoint(season: str, gw: int, entry_id: int) -> str:
    """Return the raw-contract endpoint identity for one manager's picks in one gameweek (#116 D3/AC1)."""
    return f"{PICKS_FAMILY}/{season}/{gw}/{entry_id}"


def validate_entry_picks_shape(raw: RawResponse) -> Mapping[str, Any]:
    """Shape check for an ``entry-picks`` response (#116 AC15) — STUB.

    Not implemented: always reports ``ok``, regardless of whether ``picks``
    is present, non-empty, or the body is even an object. A real
    implementation must reject an empty or missing ``picks`` list and a
    non-dict body as ``shape_invalid``.
    """
    return {"ok": True, "checks": [], "failures": [], "record_count": None}


async def ingest_entry_picks(
    client: AsyncFPLClient,
    raw_writer: LocalRawWriter,
    *,
    season: str,
    entries: list[tuple[int, str]],
    gameweeks: list[Mapping[str, Any]],
    budget_seconds: float | None = None,
) -> StageOutcome[dict[str, Any]]:
    """Capture each kept manager's picks for each eligible gameweek (#116) — STUB.

    Not implemented: this always returns immediately having fetched, stored
    and marked nothing, whatever ``entries``, ``gameweeks`` or
    ``budget_seconds`` say. No request is ever sent to ``client``, no key is
    ever written through ``raw_writer``, and no ``_settlement/entry-picks/{gw}``
    marker is ever written.
    """
    return StageOutcome(result=StageResult(stage=ENTRY_PICKS_STAGE.name), output={"entries": []})
