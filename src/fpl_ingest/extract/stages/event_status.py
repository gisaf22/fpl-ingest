"""Event-status raw-capture pipeline stage.

Captures the FPL ``event-status/`` endpoint verbatim — the finality signal
(strategy doc 2.2/A.5) that says whether the rest of a run's payloads are
provisional or settled. Captured first, before any other endpoint, because it
determines how everything else in the run should be interpreted (strategy doc
A.5): capturing it last would mean the finality signal describes a moment
after the payloads it labels.

Also parses the captured payload into a compact per-event finality map —
``{event_id: {"points": "p"|"r", "bonus_added": bool}}`` — which becomes the
run manifest's ``finality`` block (strategy doc A.5) and is the signal
``gameweeks.py`` uses in place of the retired file-existence heuristic
(strategy doc 4.2).
"""

from __future__ import annotations

import logging
from typing import Any

from fpl_ingest.extract.http.client import _ENDPOINTS, AsyncFPLClient, RawResponse
from fpl_ingest.extract.http.local_writer import LocalRawWriter
from fpl_ingest.extract.http.sync_http import FPLClientError
from fpl_ingest.orchestration.execution_state import PipelineExecutionState
from fpl_ingest.orchestration.stage_result import (
    StageLineage,
    StageMetadata,
    StageOutcome,
    StageResult,
)

logger = logging.getLogger(__name__)

RAW_SOURCE = "fpl"
RAW_ENDPOINT = "event-status"

#: Identifying fields a sampled status entry must carry (strategy doc 2.2).
_SAMPLED_STATUS_FIELDS = ("event", "points", "bonus_added", "date")

#: The stage writes one raw object and no tables.
EVENT_STATUS_STAGE = StageMetadata(
    name="event_status",
    raw_artifacts=(),
    output_tables=(),
)

#: Finality type alias: per-event id, whether that event is fully settled.
Finality = dict[int, dict[str, Any]]


async def ingest_event_status(
    client: AsyncFPLClient,
    raw_writer: LocalRawWriter,
    *,
    execution_state: PipelineExecutionState | None = None,
) -> StageOutcome[Finality | None]:
    """Fetch event-status and capture the response verbatim into raw storage.

    Args:
        client: Async FPL client for the HTTP fetch.
        raw_writer: Writer for this run; also accumulates the run manifest.
        execution_state: Fail-fast sentinel.

    Returns:
        StageOutcome whose ``output`` is the parsed per-event finality map when
        the capture is shape-valid, or ``None`` when the fetch failed or the
        payload did not validate. Callers — the gameweeks stage's selection
        logic and the run manifest — must treat ``None`` as "finality unknown"
        and fail safe (over-fetch, or omit the manifest block), never as
        "everything is settled."
    """
    if execution_state is not None and execution_state.is_failed:
        logger.info("Fail-fast tripped; skipping event-status capture")
        return StageOutcome(result=StageResult(stage="event_status"))

    logger.info("Fetching event-status...")
    try:
        raw = await client.get_event_status_raw()
    except FPLClientError as exc:
        logger.error("Failed to fetch event-status: %s", exc)
        raw_writer.record_failure(
            RAW_ENDPOINT,
            request_url=_ENDPOINTS["event_status"],
            error_class=type(exc).__name__,
            message=str(exc),
        )
        return StageOutcome(result=StageResult(stage="event_status", errors=1), output=None)

    shape = validate_event_status_shape(raw)
    if not shape["ok"]:
        logger.error(
            "event-status payload failed shape validation (%s); writing it anyway",
            ", ".join(shape["failures"]),
        )

    write = raw_writer.write_object(
        RAW_ENDPOINT,
        raw.body,
        request_url=raw.url,
        requested_at=raw.requested_at,
        received_at=raw.received_at,
        http_status=raw.status,
        response_headers=raw.headers,
        attempt_count=raw.attempt_count,
        shape_validation=shape,
    )
    logger.info(
        "Captured event-status: %d bytes -> %s", write.content_length, write.payload_key
    )

    # StageResult counts objects here, not rows: one captured object per run.
    # A shape failure must be reported as validated=0/written=0 even though
    # the payload was deliberately still written to raw storage — the
    # sidecar's shape_validation field is where that fact lives.
    ok = bool(shape["ok"])
    finality = _parse_finality(raw) if ok else None
    result = StageResult(
        stage="event_status",
        fetched=1,
        validated=1 if ok else 0,
        written=1 if ok else 0,
        skipped=0 if ok else 1,
    )
    return StageOutcome(
        result=result,
        lineage=StageLineage.from_metadata(
            EVENT_STATUS_STAGE, raw_artifacts=(write.payload_key,)
        ),
        output=finality,
    )


def validate_event_status_shape(raw: RawResponse) -> dict[str, Any]:
    """Return the raw-boundary structural verdict for an event-status response.

    Checks exactly what strategy doc B.2's pattern permits at this boundary
    and stops: the status is 2xx, the body parses as JSON, the top level is an
    object, ``status`` is present and is a list, and a sampled entry carries
    its identifying fields. Nothing about types, ranges, or cross-record
    consistency — that is warehouse work.

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

    checks.append("status_key_present_and_is_list")
    status = payload.get("status")
    if not isinstance(status, list):
        failures.append(
            "status_key_present_and_is_list: "
            + ("missing" if "status" not in payload else f"got {type(status).__name__}")
        )
        return _verdict(checks, failures, record_count=None)

    checks.append("sampled_record_has_identifying_fields")
    if status:
        sample = status[0]
        if not isinstance(sample, dict):
            failures.append(
                f"sampled_record_has_identifying_fields: record is {type(sample).__name__}"
            )
        else:
            missing = [f for f in _SAMPLED_STATUS_FIELDS if f not in sample]
            if missing:
                failures.append(
                    "sampled_record_has_identifying_fields: missing " + ", ".join(missing)
                )

    return _verdict(checks, failures, record_count=len(status))


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


def _parse_finality(raw: RawResponse) -> Finality:
    """Build the per-event finality map from a shape-valid event-status payload.

    Strategy doc 2.1 (INFERENCE): ``status`` carries one entry per match date
    within the current event window, not one per event — an event can have
    several dates. An event counts as settled only when every one of its
    listed dates reports ``points == "r"`` and ``bonus_added``; any date still
    ``"p"`` keeps the whole event provisional.
    """
    payload = raw.json()
    status = payload.get("status", []) if isinstance(payload, dict) else []

    by_event: dict[int, list[dict[str, Any]]] = {}
    for entry in status:
        if not isinstance(entry, dict):
            continue
        event = entry.get("event")
        if not isinstance(event, int):
            continue
        by_event.setdefault(event, []).append(entry)

    finality: Finality = {}
    for event, entries in by_event.items():
        settled = all(
            entry.get("points") == "r" and entry.get("bonus_added") is True
            for entry in entries
        )
        finality[event] = {"points": "r" if settled else "p", "bonus_added": settled}
    return finality
