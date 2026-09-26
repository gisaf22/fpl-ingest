"""Gameweek raw-capture pipeline stage.

Concurrently fetches the live endpoint for every gameweek that needs
collecting and writes each response verbatim through ``LocalRawWriter`` —
payload bytes, per-object metadata sidecar, and the shared run manifest — under
endpoint ``event-live/{gw:02d}``. The zero-padded gameweek (strategy doc A.2)
keeps a plain lexicographic listing of the prefix numerically ordered.

This stage no longer writes SQLite. ``upsert_gameweek_rows`` and
``process_gameweek_payloads`` were removed deliberately (strategy doc B.1):
flatten-and-upsert is warehouse work, and the decision was taken not to
dual-write during the migration. The ``gameweeks`` table is gone from the
schema contract and is no longer created.

``_collect_gameweeks`` keeps its concurrency and strict-mode cancellation
semantics exactly — only what it does with each response changed.

``_select_gameweeks_to_fetch`` captures each gameweek exactly once, after it
is ratified. It no longer decides by ``gw_{n}.json`` file-existence (strategy
doc 4.2: that heuristic answers "have I fetched this before," not "is this
finished"), and no longer by payload existence either: a gameweek captured
while still provisional satisfied an existence check once it settled, so the
ratified payload (bonus, ratification-only fields) was never fetched and the
provisional capture was frozen as final. Selection now reads the
``event-status`` finality map fetched earlier in the same run (strategy doc
A.5, ``event_status.py``) against a per-gameweek ratification marker under
``_settlement/event-live/{gw}``: a ratified gameweek with no marker is fetched
once, and the marker is written only after that gameweek's capture completed,
passed shape validation, and carries published ICT (``readiness.ict_ready``:
a capture taken before FPL populates influence/creativity/threat/ict_index is
still written, but earns no marker, so the next run fetches it again). A
provisional gameweek is not fetched, and an
unknown finality signal fetches nothing — a missed run leaves the marker
absent, so the next run with a known signal catches up.

The marker is deliberately separate from ``element_summary``'s settlement
marker (``_settlement/element-summary/{gw}``): each one only ever means "this
stage's own capture for that gameweek succeeded." A shared flag would let one
stage's success vouch for the other's when the two diverge within a run.
"""

from __future__ import annotations

import asyncio
import json
import logging
from datetime import datetime, timezone
from typing import Any

from fpl_ingest.extract.http.client import (
    _ENDPOINTS,
    AsyncFPLClient,
    RawResponse,
    cancel_pending_tasks,
)
from fpl_ingest.extract.http.local_writer import (
    LocalRawWriter,
    RawObjectExistsError,
    RawStorageBackend,
)
from fpl_ingest.extract.http.raw_keys import (
    SETTLEMENT_PREFIX,
    iso_utc,
    settlement_marker_key,
)
from fpl_ingest.extract.stages.bootstrap import GameweekInfo
from fpl_ingest.extract.stages.event_status import Finality
from fpl_ingest.extract.stages.readiness import ICT_NOT_READY_REASON, ict_ready
from fpl_ingest.orchestration.execution_state import PipelineExecutionState
from fpl_ingest.orchestration.stage_result import StageLineage, StageMetadata, StageOutcome, StageResult

logger = logging.getLogger(__name__)

RAW_SOURCE = "fpl"

#: Endpoint segment of this stage's ratification marker key. Never shared with
#: ``element_summary``'s marker — see the module docstring.
_MARKER_ENDPOINT = "event-live"

#: Top-level keys an ``event/{gw}/live`` payload must carry (strategy doc B.2).
_REQUIRED_TOP_LEVEL_KEYS = ("elements",)

#: Identifying fields a sampled live element must carry (strategy doc B.2).
_SAMPLED_ELEMENT_FIELDS = ("id", "stats", "explain")

#: The stage writes one raw object per gameweek and no tables.
#: ``raw_artifacts`` is filled in per run with the actual payload keys.
GAMEWEEKS_STAGE = StageMetadata(
    name="gameweeks",
    dependencies=("core",),
    raw_artifacts=(),
    output_tables=(),
)


def raw_endpoint(gameweek_id: int) -> str:
    """Return the raw-contract endpoint identity for one gameweek.

    Zero-padded so that ``event-live/02`` sorts before ``event-live/10`` in a
    plain S3 or filesystem listing (strategy doc A.2).
    """
    return f"event-live/{gameweek_id:02d}"


class _StrictFetchFailure(RuntimeError):
    """Raised to abort a concurrent strict-mode fetch batch immediately."""


async def ingest_gameweeks(
    client: AsyncFPLClient,
    raw_writer: LocalRawWriter,
    events: list[GameweekInfo],
    *,
    event_finality: Finality | None,
    strict: bool = False,
    execution_state: PipelineExecutionState | None = None,
) -> StageOutcome[None]:
    """Fetch live gameweek data concurrently and capture each response verbatim.

    Args:
        client: Async FPL client for the HTTP fetches.
        raw_writer: Writer for this run; also accumulates the run manifest.
            The same writer the other capture stages use — one manifest per
            run covers every endpoint and every gameweek it touches. Its
            ``backend`` is also queried for each gameweek's ratification
            marker (§module docstring) — always the actual active backend
            (local filesystem or S3), never a hardcoded local path.
        events: GameweekInfo list from the core stage.
        event_finality: The per-event finality map from this run's
            event-status capture (``event_status.ingest_event_status``), or
            ``None`` when that capture failed or did not validate. ``None``
            (or an empty map) means finality is unknown, and this stage then
            fetches nothing: fetching could only capture a possibly
            provisional payload, and writing no marker leaves every gameweek
            to be picked up by the next run whose signal is known.
        strict: If True, the first failed fetch cancels the rest of the batch.
        execution_state: Fail-fast sentinel.

    Returns:
        StageOutcome whose result counts captured objects, not rows — this
        stage no longer produces rows. Each gameweek that fails shape
        validation contributes one ``skipped`` (strict mode aborts on it) and
        is counted as not usable by the writer, while every other gameweek
        still counts as written;
        the payload is written either way, but only a clean capture earns
        its gameweek's ratification marker.
    """
    if execution_state is not None and execution_state.is_failed:
        logger.info("Fail-fast tripped; skipping gameweek capture")
        return StageOutcome(result=StageResult(stage="gameweeks"))

    gameweek_ids_to_fetch = _select_gameweeks_to_fetch(
        raw_writer.backend, events, event_finality=event_finality
    )

    if not gameweek_ids_to_fetch:
        logger.info("No newly ratified gameweeks; nothing to capture.")
        return StageOutcome(result=StageResult(stage="gameweeks"), lineage=StageLineage.from_metadata(GAMEWEEKS_STAGE))

    logger.info("Collecting %d gameweeks...", len(gameweek_ids_to_fetch))

    fetched, error_count = await _fetch_gameweeks_concurrently(
        client, gameweek_ids_to_fetch, raw_writer, strict=strict
    )

    if strict and error_count > 0:
        if execution_state is not None:
            execution_state.fail()
        return StageOutcome(
            result=StageResult(
                stage="gameweeks",
                fetched=len(fetched),
                skipped=len(fetched),
                errors=error_count,
            ),
            lineage=StageLineage.from_metadata(GAMEWEEKS_STAGE),
        )

    payload_keys: list[str] = []
    validated = 0
    for gameweek_id in sorted(fetched):
        raw = fetched[gameweek_id]
        endpoint = raw_endpoint(gameweek_id)
        shape = validate_gameweek_shape(raw)
        if not shape["ok"]:
            logger.error(
                "Gameweek %d payload failed shape validation (%s); writing it anyway",
                gameweek_id,
                ", ".join(shape["failures"]),
            )
        else:
            validated += 1
        write = raw_writer.write_object(
            endpoint,
            raw.body,
            request_url=raw.url,
            requested_at=raw.requested_at,
            received_at=raw.received_at,
            http_status=raw.status,
            response_headers=raw.headers,
            attempt_count=raw.attempt_count,
            shape_validation=shape,
        )
        payload_keys.append(write.payload_key)
        logger.info(
            "Captured gameweek %d: %d bytes -> %s",
            gameweek_id,
            write.content_length,
            write.payload_key,
        )
        if shape["ok"] and not ict_ready(_live_stats_rows(raw)):
            logger.warning(
                "Gameweek %d captured but ICT not yet populated; ratification marker "
                "withheld, the next run retries",
                gameweek_id,
            )
            raw_writer.record_marker_withheld(
                _MARKER_ENDPOINT, event=gameweek_id, reason=ICT_NOT_READY_REASON
            )
        elif shape["ok"]:
            _record_ratified_capture(raw_writer, gameweek_id, write.payload_key)
        else:
            logger.warning(
                "Gameweek %d ratification marker withheld; the next run retries it",
                gameweek_id,
            )

    # StageResult counts objects here, not rows: one captured object per
    # gameweek. Its invariants (fetched >= validated >= written, skipped ==
    # fetched - validated) mean a shape failure must be reported as not
    # validated and not written even though the payload was deliberately still
    # written to raw storage — the sidecar's shape_validation field is where
    # that fact lives. Run status comes from the writer, which counts those
    # payloads as not usable without discounting the gameweeks that captured
    # cleanly; skipped > 0 is what strict mode aborts on.
    fetched_count = len(fetched)
    return StageOutcome(
        result=StageResult(
            stage="gameweeks",
            fetched=fetched_count,
            validated=validated,
            written=validated,
            skipped=fetched_count - validated,
            errors=error_count,
        ),
        lineage=StageLineage.from_metadata(GAMEWEEKS_STAGE, raw_artifacts=payload_keys),
    )


def validate_gameweek_shape(raw: RawResponse) -> dict[str, Any]:
    """Return the raw-boundary structural verdict for an ``event/{gw}/live`` response.

    Checks exactly what strategy doc B.2 permits at this boundary and stops:
    the status is 2xx, the body parses as JSON, the top level is an object,
    ``elements`` is present, and a sampled element carries its identifying
    fields. Nothing about types, ranges, or cross-record consistency — that is
    warehouse work.

    Args:
        raw: The captured response.

    Returns:
        A JSON-serialisable dict for the sidecar's ``shape_validation`` field:
        ``ok``, the list of ``checks`` run, and any ``failures``.
    """
    checks: list[str] = []
    failures: list[str] = []

    checks.append("http_status_2xx")
    if not 200 <= raw.status < 300:
        failures.append(f"http_status_2xx: got {raw.status}")
        return _verdict(checks, failures, record_count=None)

    checks.append("body_parses_as_json")
    payload = raw.json()
    if payload is None:
        failures.append("body_parses_as_json: body is not valid JSON")
        return _verdict(checks, failures, record_count=None)

    checks.append("top_level_is_object")
    if not isinstance(payload, dict):
        failures.append(f"top_level_is_object: got {type(payload).__name__}")
        return _verdict(checks, failures, record_count=None)

    checks.append("required_top_level_keys_present")
    missing_keys = [key for key in _REQUIRED_TOP_LEVEL_KEYS if key not in payload]
    if missing_keys:
        failures.append(
            "required_top_level_keys_present: missing " + ", ".join(missing_keys)
        )

    elements = payload.get("elements")
    if not isinstance(elements, list):
        # Without an elements list there is no record to sample; the missing-key
        # or type problem is already reported above.
        if "elements" in payload:
            failures.append(
                f"required_top_level_keys_present: elements is {type(elements).__name__}"
            )
        return _verdict(checks, failures, record_count=None)

    checks.append("sampled_record_has_identifying_fields")
    if elements:
        sample = elements[0]
        if not isinstance(sample, dict):
            failures.append(
                "sampled_record_has_identifying_fields: record is "
                f"{type(sample).__name__}"
            )
        else:
            missing = [f for f in _SAMPLED_ELEMENT_FIELDS if f not in sample]
            if missing:
                failures.append(
                    "sampled_record_has_identifying_fields: missing "
                    + ", ".join(missing)
                )

    return _verdict(checks, failures, record_count=len(elements))


def _verdict(
    checks: list[str], failures: list[str], *, record_count: int | None
) -> dict[str, Any]:
    """Assemble the sidecar-shaped validation result."""
    return {
        "ok": not failures,
        "checks": checks,
        "failures": failures,
        "record_count": record_count,
    }


def _select_gameweeks_to_fetch(
    backend: RawStorageBackend,
    events: list[GameweekInfo],
    *,
    event_finality: Finality | None,
) -> list[int]:
    """Determine which gameweek IDs need to be fetched.

    Candidates are every finished gameweek plus the current gameweek (which
    may or may not also be finished — FPL keeps a gameweek "current" for a
    while after it finishes, until the next one's deadline, and ratification
    is not guaranteed to trail ``finished``). Each candidate is then decided
    by :func:`_needs_fetch`.
    """
    finished = [e for e in events if e.finished]
    current = next((e for e in events if e.is_current), None)
    logger.info(
        "Found %d finished gameweeks, current gameweek: %s",
        len(finished), current.id if current is not None else None,
    )

    if not event_finality:
        if finished or current is not None:
            logger.warning(
                "event-status finality unknown; capturing no gameweeks this run "
                "(the next run with a known signal catches up)"
            )
        return []

    candidates = list(finished)
    if current is not None and current not in candidates:
        candidates.append(current)

    return [e.id for e in candidates if _needs_fetch(backend, e, event_finality)]


def _needs_fetch(backend: RawStorageBackend, event: GameweekInfo, event_finality: Finality) -> bool:
    """Decide whether one gameweek must be fetched this run.

    ===============================  ===============  ======
    gameweek                         marker exists?   action
    ===============================  ===============  ======
    ratified                         no               fetch — once
    ratified                         yes              skip
    provisional                      either           skip — not final yet
    finality unknown (whole map)     either           skip — see caller
    ===============================  ===============  ======

    Ratification is decided by :func:`_is_ratified`. Payload existence is
    deliberately not consulted: a capture taken while the gameweek was
    provisional exists but is not the ratified payload.
    """
    if not _is_ratified(event, event_finality):
        return False
    return not backend.exists_prefix(_ratification_marker_prefix(event.id))


def _is_ratified(event: GameweekInfo, event_finality: Finality) -> bool:
    """Whether event-status reports this gameweek ratified.

    A gameweek listed in the map is ratified once ``bonus_added`` (``_parse_
    finality`` only sets it when every date reports ``points == "r"`` too).
    A gameweek *absent* from a non-empty map has rolled out of event-status's
    current-window ``status`` array — the normal state for one settled well in
    the past — but only if bootstrap also reports it ``finished``; a current
    gameweek that has not started yet is absent for the opposite reason.

    Reading "finished and absent" as ratified assumes a round ratifies before
    it leaves the window. That is an observation, not a guarantee: fpl-warehouse
    CLAUDE.md ("Ratification lead before a round leaves the window", measured
    2026-09-23) records it for rounds 2-4 of 2026-27 with at least 70.0h of
    margin, over every event-status capture in S3.
    """
    info = event_finality.get(event.id)
    if info is None:
        return event.finished
    return bool(info.get("bonus_added"))


def _live_stats_rows(raw: RawResponse) -> list[Any]:
    """Return each live element's ``stats`` mapping, for :func:`ict_ready`.

    Called only on a shape-valid payload, so ``elements`` is a list. An
    element that is not an object, or has no ``stats``, yields a non-mapping
    row, which ``ict_ready`` reads as not ready.
    """
    payload = raw.json()
    elements = payload.get("elements", []) if isinstance(payload, dict) else []
    return [e.get("stats") if isinstance(e, dict) else None for e in elements]


def _ratification_marker_prefix(gameweek_id: int) -> str:
    """Return the prefix ``exists_prefix`` is asked about for one gameweek."""
    return f"{RAW_SOURCE}/{SETTLEMENT_PREFIX}/{_MARKER_ENDPOINT}/{gameweek_id}"


def _record_ratified_capture(
    raw_writer: LocalRawWriter, gameweek_id: int, payload_key: str
) -> None:
    """Mark this gameweek's ratified payload as captured.

    Called only after the payload was written and passed shape validation, so
    a failed fetch or a shape failure leaves the marker absent and the next
    run retries the gameweek. A marker that already exists (an overlapping
    run got there first) already says what this one would.
    """
    marker = {
        "event": gameweek_id,
        "endpoint": _MARKER_ENDPOINT,
        "run_id": raw_writer.run_id,
        "recorded_at": iso_utc(datetime.now(timezone.utc)),
        "payload_key": payload_key,
    }
    try:
        raw_writer.backend.put_bytes(
            settlement_marker_key(RAW_SOURCE, _MARKER_ENDPOINT, gameweek_id),
            json.dumps(marker, sort_keys=True).encode("utf-8"),
        )
    except RawObjectExistsError:
        logger.info("Gameweek %d ratification marker already recorded", gameweek_id)


async def _fetch_gameweeks_concurrently(
    client: AsyncFPLClient,
    gameweek_ids: list[int],
    raw_writer: LocalRawWriter,
    *,
    strict: bool,
) -> tuple[dict[int, RawResponse], int]:
    """Fetch all gameweeks in parallel and return (responses_by_id, error_count)."""
    return await _collect_gameweeks(client, gameweek_ids, raw_writer, strict=strict)


async def _collect_gameweeks(
    client: AsyncFPLClient,
    gameweek_ids: list[int],
    raw_writer: LocalRawWriter,
    *,
    strict: bool,
) -> tuple[dict[int, RawResponse], int]:
    """Fetch gameweeks, cancelling pending work on the first strict failure."""
    fetched: dict[int, RawResponse] = {}
    error_count = 0

    if not strict:
        raw_results = await asyncio.gather(
            *[_fetch_one_gameweek(client, gw) for gw in gameweek_ids],
            return_exceptions=True,
        )

        for gameweek_id, result in zip(gameweek_ids, raw_results):
            if isinstance(result, BaseException):
                error_count += 1
                logger.error("Failed gameweek %d: %s", gameweek_id, result)
                _record_fetch_failure(raw_writer, gameweek_id, result)
                continue
            gw_id, raw = result
            fetched[gw_id] = raw

        return fetched, error_count

    tasks = {
        asyncio.create_task(_fetch_one_gameweek(client, gw)): gw
        for gw in gameweek_ids
    }

    try:
        pending = set(tasks)
        while pending:
            done, pending = await asyncio.wait(pending, return_when=asyncio.FIRST_COMPLETED)
            for task in done:
                gameweek_id = tasks[task]
                try:
                    gw_id, raw = task.result()
                except Exception as exc:
                    error_count += 1
                    logger.error("Failed gameweek %d: %s", gameweek_id, exc)
                    _record_fetch_failure(raw_writer, gameweek_id, exc)
                    await cancel_pending_tasks(pending)
                    raise _StrictFetchFailure from exc
                fetched[gw_id] = raw
    except _StrictFetchFailure:
        return fetched, error_count

    return fetched, error_count


def _record_fetch_failure(
    raw_writer: LocalRawWriter, gameweek_id: int, exc: BaseException
) -> None:
    """Record one gameweek's failed capture in the shared run manifest."""
    raw_writer.record_failure(
        raw_endpoint(gameweek_id),
        request_url=_ENDPOINTS["live"].format(gw=gameweek_id),
        error_class=type(exc).__name__,
        message=str(exc),
    )


async def _fetch_one_gameweek(
    client: AsyncFPLClient,
    gameweek_id: int,
) -> tuple[int, RawResponse]:
    raw = await client.get_gameweek_live_raw(gameweek_id)
    logger.info("Gameweek %d — %d bytes fetched", gameweek_id, len(raw.body))
    return gameweek_id, raw
