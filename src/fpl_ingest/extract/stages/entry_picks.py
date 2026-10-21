"""Entry-picks raw-capture stage (#116).

Captures each kept panel manager's picks for every finished, data-checked
gameweek, verbatim, under endpoint ``entry-picks/{season}/{gw}/{entry_id}``
(``fpl/entry-picks/{season}/{gw}/{entry_id}/{date}/{run_id}/payload.json``, #100 D3).

Every response goes through the shared router (#115, ADR 004). A random-cohort
404 is a manager's pre-join week: it is stored under ``fpl/_invalid/not_found/``,
declared expected, and counts as captured. Anything else that is not a valid
capture (a self-cohort 404, any other 4xx, an empty ``picks`` list) is stored
under ``_invalid`` as a failure, is retried next run, and blocks the gameweek's
settlement marker. A network error or 5xx stores nothing and is retried.

Resume and settlement are recorded in ADR 006. In short: an entry is captured
when ``exists_prefix`` finds its payload tree (or, for the random cohort only,
its ``not_found`` tree); a gameweek is settled when every kept entry is
captured, and only then is ``_settlement/entry-picks/{gw}`` written. Gameweeks
run oldest first and the next never starts before the earlier is settled. A
wall-clock budget, checked before each dispatch, ends the run cleanly: unreached
entries are absent, not failed, and the next run resumes. The panel's entries
and cohorts are given by the caller (the runner reads the panel file); this
module reads nothing back from storage (ADR 005).
"""

from __future__ import annotations

import asyncio
import json
import logging
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from functools import partial
from typing import Any, Mapping, Sequence

from fpl_ingest.extract.http.client import _ENDPOINTS, AsyncFPLClient, RawResponse
from fpl_ingest.extract.http.dispatch import OffLoop, dispatch_bounded
from fpl_ingest.extract.http.local_writer import LocalRawWriter, RawObjectExistsError, RawStorageBackend
from fpl_ingest.extract.http.rate_config import MAX_CONCURRENT_REQUESTS
from fpl_ingest.extract.http.raw_keys import (
    INVALID_PREFIX,
    SETTLEMENT_PREFIX,
    STOP_REASON_BUDGET,
    iso_utc,
    settlement_marker_key,
)
from fpl_ingest.extract.http.response_router import ResponseClass, fetch_and_route, record_no_response
from fpl_ingest.orchestration.stage_result import StageMetadata, StageOutcome, StageResult

logger = logging.getLogger(__name__)

RAW_SOURCE = "fpl"
PICKS_FAMILY = "entry-picks"

COHORT_RANDOM = "random"
COHORT_SELF = "self"

#: A picks run rewrites its IN_PROGRESS manifest every this many objects or seconds,
#: whichever comes first, not after each one: the rewrite is one more S3 round trip
#: whose body grows with every object (#123 D3).
MANIFEST_FLUSH_EVERY = 50
MANIFEST_FLUSH_SECONDS = 10.0

#: The stage writes raw objects only, no tables.
ENTRY_PICKS_STAGE = StageMetadata(
    name="entry_picks", dependencies=("core",), raw_artifacts=(), output_tables=()
)

# What one entry's capture attempt came to.
_RESUMED = "resumed"
_KEPT = "kept"
_NOT_FOUND = "not_found"
_SHAPE_INVALID = "shape_invalid"
_HTTP_ERROR = "http_error"
_FETCH_FAILED = "fetch_failed"

#: Outcomes after which an entry counts as captured: a payload exists, or a
#: legitimate 404 is stored. Only these let the gameweek settle (#116 Q4, #54 Q2).
_CAPTURED = frozenset({_RESUMED, _KEPT, _NOT_FOUND})
#: Stored responses with a non-2xx status, counted per gameweek for spike visibility (AC11).
_NON_2XX = frozenset({_NOT_FOUND, _HTTP_ERROR})

_CLASS_TO_VERDICT = {
    ResponseClass.VALID: _KEPT,
    ResponseClass.NOT_FOUND: _NOT_FOUND,
    ResponseClass.SHAPE_INVALID: _SHAPE_INVALID,
    ResponseClass.HTTP_ERROR: _HTTP_ERROR,
    ResponseClass.FETCH_FAILED: _FETCH_FAILED,
}


def raw_endpoint(season: str, gw: int, entry_id: int) -> str:
    """Return the raw-contract endpoint identity for one manager's picks in one gameweek (#116 D3/AC1)."""
    return f"{PICKS_FAMILY}/{season}/{gw}/{entry_id}"


def eligible_gameweeks(gameweeks: Sequence[Mapping[str, Any]]) -> list[int]:
    """Return the ids of finished, data-checked gameweeks, oldest first (#100 D2, D6).

    ``gameweeks`` are bootstrap-static ``events``. A gameweek that is only
    finished has provisional picks data and waits; a missing flag counts as not set.
    """
    return sorted(
        gw["id"] for gw in gameweeks
        if gw.get("finished") is True and gw.get("data_checked") is True and isinstance(gw.get("id"), int)
    )


def validate_entry_picks_shape(raw: RawResponse) -> dict[str, Any]:
    """Return the raw-boundary structural verdict for an entry-picks response (#116 AC15).

    Valid is a 2xx whose body is a JSON object with a non-empty ``picks`` list.
    An empty or missing list, and a body that is not an object, are
    ``shape_invalid`` for both cohorts: nothing declares them expected, so they
    fail the run and block the gameweek. Anything finer is warehouse work.
    """
    checks = ["http_status_2xx", "body_parses_as_json", "top_level_is_object", "picks_is_non_empty_list"]
    failures: list[str] = []
    record_count = None
    payload = raw.json()
    if not 200 <= raw.status < 300:
        failures.append(f"http_status_2xx: got {raw.status}")
    elif payload is None:
        failures.append("body_parses_as_json: body is not valid JSON")
    elif not isinstance(payload, dict):
        failures.append(f"top_level_is_object: got {type(payload).__name__}")
    elif not isinstance(payload.get("picks"), list):
        failures.append("picks_is_non_empty_list: picks missing or not a list")
    elif not payload["picks"]:
        failures.append("picks_is_non_empty_list: picks is empty")
    else:
        record_count = len(payload["picks"])
    return {"ok": not failures, "checks": checks, "failures": failures, "record_count": record_count}


@dataclass
class _Budget:
    """The run's wall-clock allowance and whether it was what ended the run."""

    seconds: float | None
    started: float = field(default_factory=time.monotonic)
    exhausted: bool = False

    def expired(self) -> bool:
        return self.seconds is not None and time.monotonic() - self.started >= self.seconds

    def stop_dispatching(self, raw_writer: LocalRawWriter) -> bool:
        """Whether no further request should be sent: the run aborted or its budget is spent.

        Called only when a request is about to be sent, so a True from the budget
        always means work was left undone.
        """
        if raw_writer.aborted:
            return True
        if self.expired():
            self.exhausted = True
            return True
        return False


async def ingest_entry_picks(
    client: AsyncFPLClient,
    raw_writer: LocalRawWriter,
    *,
    season: str,
    entries: Sequence[tuple[int, str]],
    gameweeks: Sequence[Mapping[str, Any]],
    budget_seconds: float | None = None,
) -> StageOutcome[dict[str, Any]]:
    """Capture each kept manager's picks for each eligible gameweek, oldest first.

    Args:
        client: Fetches picks.
        raw_writer: This run's writer; its backend answers every resume check.
        season: The run's season, for the key.
        entries: ``(entry_id, cohort)`` for each kept panel manager; cohort is
            ``random`` or ``self`` and decides whether a 404 is legitimate.
        gameweeks: bootstrap-static ``events``; only ``id``, ``finished`` and
            ``data_checked`` are read.
        budget_seconds: Wall-clock allowance for the stage, checked before each
            request; None is unbounded. On expiry no more requests are sent,
            in-flight ones finish, and the manifest records ``stop_reason``.
            A stop that captured nothing new is recorded as a failure, so the
            run exits non-zero: a budget too small to make progress must be seen.
    """
    budget = _Budget(budget_seconds)
    backend = raw_writer.backend
    stats = _Stats()
    completed: list[int] = []

    try:
        with OffLoop() as io:
            for gw in eligible_gameweeks(gameweeks):
                if await io(backend.exists_prefix, _marker_prefix(gw)):
                    continue
                if budget.stop_dispatching(raw_writer):
                    break
                if not await _capture_gameweek(client, raw_writer, io, budget, stats, season, gw, entries):
                    break
                completed.append(gw)
    finally:
        # Leaving OffLoop waited for storage calls in flight, so the trace holds
        # everything stored, even when the stage is raising (#123 AC6).
        await asyncio.to_thread(raw_writer.flush_manifest)

    return _finish(raw_writer, budget, stats, completed)


@dataclass
class _Stats:
    """Tallies over the whole stage run."""

    requests: int = 0
    verdicts: dict[str, int] = field(default_factory=dict)
    non_2xx_by_gameweek: dict[str, int] = field(default_factory=dict)

    def count(self, verdict: str) -> None:
        self.verdicts[verdict] = self.verdicts.get(verdict, 0) + 1

    def get(self, verdict: str) -> int:
        return self.verdicts.get(verdict, 0)


async def _capture_gameweek(
    client: AsyncFPLClient,
    raw_writer: LocalRawWriter,
    io: OffLoop,
    budget: _Budget,
    stats: _Stats,
    season: str,
    gw: int,
    entries: Sequence[tuple[int, str]],
) -> bool:
    """Capture one gameweek's entries; return True when it settled (its marker was written)."""

    async def capture(entry: tuple[int, str]) -> str:
        verdict = await _capture_entry(client, raw_writer, io, season, gw, *entry)
        if verdict != _RESUMED:
            stats.requests += 1
        return verdict

    dispatched = await dispatch_bounded(
        entries, capture, aborted=lambda: budget.stop_dispatching(raw_writer), limit=MAX_CONCURRENT_REQUESTS
    )
    verdicts = [verdict for _, verdict in dispatched]
    for verdict in verdicts:
        stats.count(verdict)
    if any(v != _RESUMED for v in verdicts):
        stats.non_2xx_by_gameweek[str(gw)] = sum(1 for v in verdicts if v in _NON_2XX)

    # Entries the stop kept from being dispatched are unresolved, like a failed one.
    unresolved = len(entries) - sum(1 for v in verdicts if v in _CAPTURED)
    if unresolved == 0:
        await io(_record_settlement, raw_writer, gw, len(entries))
        logger.info("entry-picks: gameweek %d settled (%d entries)", gw, len(entries))
        return True

    reason = _withheld_reason(raw_writer, budget, unresolved)
    logger.warning("entry-picks: gameweek %d not settled, marker withheld: %s", gw, reason)
    await io(raw_writer.record_marker_withheld, PICKS_FAMILY, event=gw, reason=reason)
    return False


def _withheld_reason(raw_writer: LocalRawWriter, budget: _Budget, unresolved: int) -> str:
    if raw_writer.aborted:
        return "aborted_http_error_streak"
    if budget.exhausted:
        return STOP_REASON_BUDGET
    return f"{unresolved} entries not captured cleanly"


def _finish(
    raw_writer: LocalRawWriter, budget: _Budget, stats: _Stats, completed: list[int]
) -> StageOutcome[dict[str, Any]]:
    elapsed = time.monotonic() - budget.started
    fetched = stats.get(_KEPT) + stats.get(_NOT_FOUND) + stats.get(_SHAPE_INVALID) + stats.get(_HTTP_ERROR)
    new_captures = stats.get(_KEPT) + stats.get(_NOT_FOUND)
    result = StageResult(
        stage=ENTRY_PICKS_STAGE.name,
        fetched=fetched, validated=fetched, written=stats.get(_KEPT),
        errors=stats.get(_SHAPE_INVALID) + stats.get(_HTTP_ERROR) + stats.get(_FETCH_FAILED),
    )
    metadata: dict[str, str] = {}

    if stats.requests:
        # The attempted family is what carries these in the manifest; no request, no block.
        raw_writer.record_endpoint_detail(PICKS_FAMILY, "non_2xx_by_gameweek", dict(stats.non_2xx_by_gameweek))
        raw_writer.record_endpoint_detail(PICKS_FAMILY, "throughput", {
            "requests": stats.requests,
            "total_seconds": round(elapsed, 3),
            "requests_per_second": round(stats.requests / elapsed, 3) if elapsed > 0 else 0.0,
            "gameweeks_completed": list(completed),
        })
    logger.info(
        "entry-picks: %d request(s) in %.1fs; gameweeks settled this run: %s", stats.requests, elapsed, completed or "none"
    )

    if budget.exhausted:
        metadata["stopped_reason"] = STOP_REASON_BUDGET
        raw_writer.record_stop(STOP_REASON_BUDGET)
        logger.warning("entry-picks: stopped at the wall-clock budget; a rerun resumes")
        if new_captures == 0:
            raw_writer.record_not_attempted(
                PICKS_FAMILY, reason=f"{STOP_REASON_BUDGET}: the budget was spent before any entry was captured"
            )
    return StageOutcome(
        result=result,
        output={"gameweeks_completed": completed, "requests": stats.requests, "new_captures": new_captures},
        metadata=metadata,
    )


async def _capture_entry(
    client: AsyncFPLClient, raw_writer: LocalRawWriter, io: OffLoop, season: str, gw: int, entry_id: int, cohort: str
) -> str:
    """Capture one entry's picks for one gameweek; return its verdict.

    Failures are recorded in the manifest here, so the caller only counts them.
    """
    random_cohort = cohort == COHORT_RANDOM
    if await io(_already_captured, raw_writer.backend, season, gw, entry_id, random_cohort):
        return _RESUMED

    endpoint = raw_endpoint(season, gw, entry_id)
    url = _ENDPOINTS["entry_picks"].format(entry_id=entry_id, gw=gw)
    label = f"picks {entry_id} gw{gw}"
    try:
        routed = await fetch_and_route(
            raw_writer, endpoint, partial(client.get_entry_picks_raw, gw, entry_id), validate_entry_picks_shape,
            request_url=url, label=label,
            # A self id is configured, so it must have picks: its 404 is an http_error (Q9).
            absence_ok=random_cohort, expected=frozenset({"not_found"}) if random_cohort else frozenset(),
            run_blocking=io,
        )
    except Exception as exc:  # noqa: BLE001 - any fetch failure is recorded, never raised
        await io(record_no_response, raw_writer, endpoint, request_url=url, exc=exc, label=label)
        return _FETCH_FAILED
    return _CLASS_TO_VERDICT[routed.response_class]


def _already_captured(backend: RawStorageBackend, season: str, gw: int, entry_id: int, random_cohort: bool) -> bool:
    """Whether this entry's picks for this gameweek are already captured (ADR 006, option A).

    Two existence checks, never a content read: the payload tree, then, only
    where a 404 is legitimate, the ``not_found`` tree. ``shape_invalid`` and
    ``http_error`` trees are deliberately not checked, so those entries retry.
    """
    endpoint = raw_endpoint(season, gw, entry_id)
    if backend.exists_prefix(f"{RAW_SOURCE}/{endpoint}"):
        return True
    return random_cohort and backend.exists_prefix(f"{RAW_SOURCE}/{INVALID_PREFIX}/not_found/{endpoint}")


def _marker_prefix(gw: int) -> str:
    """Return the prefix ``exists_prefix`` is asked about for one gameweek's marker."""
    return f"{RAW_SOURCE}/{SETTLEMENT_PREFIX}/{PICKS_FAMILY}/{gw}"


def _record_settlement(raw_writer: LocalRawWriter, gw: int, entry_count: int) -> None:
    """Mark this gameweek's picks as captured for every kept entry.

    Called only once every entry has a payload or a stored legitimate 404, so
    a failed or unreached entry leaves the marker absent. Separate from the
    event-live and element-summary markers by design: it vouches for this
    stage's capture alone.
    """
    marker = {
        "event": gw,
        "endpoint": PICKS_FAMILY,
        "run_id": raw_writer.run_id,
        "recorded_at": iso_utc(datetime.now(timezone.utc)),
        "entries": entry_count,
    }
    try:
        raw_writer.backend.put_bytes(
            settlement_marker_key(RAW_SOURCE, PICKS_FAMILY, gw), json.dumps(marker, sort_keys=True).encode("utf-8")
        )
    except RawObjectExistsError:
        logger.info("entry-picks: gameweek %d marker already recorded", gw)
