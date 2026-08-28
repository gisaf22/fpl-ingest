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

``_select_gameweeks_to_fetch`` no longer decides by ``gw_{n}.json``
file-existence (strategy doc 4.2: that heuristic answers "have I fetched this
before," not "is this finished," and freezes a gameweek captured while still
provisional at that state forever). It now uses the ``event-status`` finality
map fetched earlier in the same run (strategy doc A.5, ``event_status.py``): a
finished gameweek is fetched unless event-status reports it settled AND it has
already been captured at least once. The ``gw_{n}.json`` marker files and
``_write_gameweek_caches`` are retired along with the heuristic they existed
solely to serve — the captured payload under ``event-live/{gw:02d}`` is the
only record of what the API returned.
"""

from __future__ import annotations

import asyncio
import logging
from pathlib import Path
from typing import Any

from fpl_ingest.extract.http.client import (
    _ENDPOINTS,
    AsyncFPLClient,
    RawResponse,
    cancel_pending_tasks,
)
from fpl_ingest.extract.http.local_writer import LocalRawWriter
from fpl_ingest.extract.stages.bootstrap import GameweekInfo
from fpl_ingest.extract.stages.event_status import Finality
from fpl_ingest.orchestration.execution_state import PipelineExecutionState
from fpl_ingest.orchestration.stage_result import StageLineage, StageMetadata, StageOutcome, StageResult

logger = logging.getLogger(__name__)

RAW_SOURCE = "fpl"

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
    raw_dir: Path,
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
            run covers every endpoint and every gameweek it touches.
        raw_dir: Local raw-capture root; used to check whether a gameweek has
            ever been captured before (§below).
        events: GameweekInfo list from the core stage.
        event_finality: The per-event finality map from this run's
            event-status capture (``event_status.ingest_event_status``), or
            ``None`` when that capture failed or did not validate. ``None``
            is treated as "nothing is known settled," never as "everything is
            settled" — a missing finality signal must never cause
            under-fetching.
        strict: If True, the first failed fetch cancels the rest of the batch.
        execution_state: Fail-fast sentinel.

    Returns:
        StageOutcome whose result counts captured objects, not rows — this
        stage no longer produces rows. Each gameweek that fails shape
        validation contributes one ``skipped`` so ``classify_run`` marks the
        run FAILED_PARTIAL while every other gameweek still counts as written;
        the payload is written either way.
    """
    if execution_state is not None and execution_state.is_failed:
        logger.info("Fail-fast tripped; skipping gameweek capture")
        return StageOutcome(result=StageResult(stage="gameweeks"))

    gameweek_ids_to_fetch = _select_gameweeks_to_fetch(
        raw_dir, events, event_finality=event_finality
    )

    if not gameweek_ids_to_fetch:
        logger.info("All finished gameweeks already collected.")
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

    # StageResult counts objects here, not rows: one captured object per
    # gameweek. Its invariants (fetched >= validated >= written, skipped ==
    # fetched - validated) mean a shape failure must be reported as not
    # validated and not written even though the payload was deliberately still
    # written to raw storage — the sidecar's shape_validation field is where
    # that fact lives. skipped > 0 is what makes classify_run mark the run
    # FAILED_PARTIAL, and it does so without discounting the gameweeks that
    # captured cleanly.
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
    raw_dir: Path,
    events: list[GameweekInfo],
    *,
    event_finality: Finality | None,
) -> list[int]:
    """Determine which gameweek IDs need to be fetched.

    A finished gameweek is fetched unless ``event_finality`` reports it
    settled (``points == "r"`` and ``bonus_added``) AND it has already been
    captured at least once — a settled gameweek that was somehow never
    fetched is still fetched, never silently skipped.
    """
    finished_ids = [e.id for e in events if e.finished]
    current_id = next((e.id for e in events if e.is_current), None)
    logger.info(
        "Found %d finished gameweeks, current gameweek: %s",
        len(finished_ids), current_id,
    )

    known_settled = _known_settled_gameweeks(event_finality)
    to_fetch = [
        gw
        for gw in finished_ids
        if gw not in known_settled or not _has_event_live_capture(raw_dir, gw)
    ]

    # Always include the current gameweek if it isn't already selected.
    if current_id and current_id not in to_fetch:
        return to_fetch + [current_id]
    return to_fetch


def _known_settled_gameweeks(event_finality: Finality | None) -> set[int]:
    """Return the event ids event-status reports as fully settled.

    ``event_finality`` is None when this run's event-status capture failed or
    did not validate; an empty set here means "nothing is known settled," so
    every finished gameweek is fetched — the fail-safe strategy doc 4 calls
    for rather than silently under-fetching on a missing finality signal.
    """
    if not event_finality:
        return set()
    return {event for event, info in event_finality.items() if info.get("bonus_added")}


def _has_event_live_capture(raw_dir: Path, gameweek_id: int) -> bool:
    """Whether this gameweek's live endpoint has ever been captured to raw storage."""
    return (raw_dir / RAW_SOURCE / raw_endpoint(gameweek_id)).is_dir()


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
