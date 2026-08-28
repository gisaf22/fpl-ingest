"""Core (bootstrap-static) raw-capture pipeline stage.

Fetches the FPL bootstrap-static endpoint and writes the response verbatim
through ``LocalRawWriter`` — payload bytes, per-object metadata sidecar, and
the run manifest — into the same manifest the rest of the run shares. It
performs the minimal structural checks the raw boundary calls for (strategy
doc B.2) and nothing more.

This stage no longer writes SQLite. ``process_core_payload``,
``ingest_players``, ``ingest_teams``, ``ingest_events``,
``ingest_element_types``, and ``_assert_store_validation_consistency`` were
removed deliberately (strategy doc B.1): flatten-and-upsert is warehouse work,
and the decision was taken not to dual-write during the migration. The
``players``, ``teams``, ``events``, and ``element_types`` tables are gone from
the schema contract and are no longer created.

What the stage still does beyond capture is hand two in-memory values to the
stages that have not been migrated yet: the gameweek events (as
``GameweekInfo``, holding only the fields gameweek selection needs) and the
element ids. ``gameweeks.py`` and ``element_summary.py`` take those as
arguments — they never read them back out of SQLite — so the handoff keeps
working exactly as before without any table behind it. It disappears when those
two stages are redirected to raw capture.
"""

from __future__ import annotations

import logging
from typing import Any, NamedTuple

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
RAW_ENDPOINT = "bootstrap-static"

#: Top-level keys a bootstrap-static payload must carry (strategy doc B.2).
_REQUIRED_TOP_LEVEL_KEYS = ("elements", "teams", "events", "element_types")

#: Identifying fields a sampled element record must carry (strategy doc B.2).
_SAMPLED_ELEMENT_FIELDS = ("id", "team", "now_cost")

#: The stage writes one raw object and no tables. ``raw_artifacts`` is filled
#: in per run with the actual payload key, so the metadata default is empty.
CORE_STAGE = StageMetadata(
    name="core",
    raw_artifacts=(),
    output_tables=(),
)


class GameweekInfo(NamedTuple):
    """The subset of a bootstrap-static event needed for gameweek selection.

    Not a validated model and not persisted — just the three fields
    ``gameweeks.py``'s ``_select_gameweeks_to_fetch`` reads off each event.
    """

    id: int
    finished: bool
    is_current: bool


class CoreData(NamedTuple):
    """The in-memory handoff to the stages not yet redirected to raw capture.

    Not a persisted shape and not a validated view of bootstrap-static — only
    the two things downstream stages take as arguments.
    """

    events: list[GameweekInfo]
    player_ids: list[int]


async def ingest_core_data(
    client: AsyncFPLClient,
    raw_writer: LocalRawWriter,
    *,
    execution_state: PipelineExecutionState | None = None,
) -> StageOutcome[CoreData]:
    """Fetch bootstrap-static and capture the response verbatim into raw storage.

    Args:
        client: Async FPL client for the HTTP fetch.
        raw_writer: Writer for this run; also accumulates the run manifest.
            The same writer the other capture stages use — one manifest per
            run covers every endpoint it touches.
        execution_state: Fail-fast sentinel. When a previous stage has already
            failed, the capture is skipped rather than written.

    Returns:
        StageOutcome whose result counts captured objects, not rows — this
        stage no longer produces rows. A shape-validation failure reports
        ``skipped=1`` so ``classify_run`` marks the run FAILED_PARTIAL; the
        payload is written regardless. The output carries the downstream
        handoff, empty when nothing was captured.
    """
    if execution_state is not None and execution_state.is_failed:
        logger.info("Fail-fast tripped; skipping bootstrap-static capture")
        return StageOutcome(
            result=StageResult(stage="core"), output=CoreData(events=[], player_ids=[])
        )

    logger.info("Fetching bootstrap-static...")
    try:
        raw = await client.get_bootstrap_raw()
    except FPLClientError as exc:
        logger.error("Failed to fetch bootstrap-static: %s", exc)
        raw_writer.record_failure(
            RAW_ENDPOINT,
            request_url=_ENDPOINTS["bootstrap"],
            error_class=type(exc).__name__,
            message=str(exc),
        )
        return StageOutcome(
            result=StageResult(stage="core", errors=1),
            output=CoreData(events=[], player_ids=[]),
        )

    shape = validate_bootstrap_shape(raw)
    if not shape["ok"]:
        logger.error(
            "bootstrap-static payload failed shape validation (%s); writing it anyway",
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
        "Captured bootstrap-static: %d bytes -> %s",
        write.content_length,
        write.payload_key,
    )

    # StageResult counts objects here, not rows: one captured object per run.
    # Its invariants (fetched >= validated >= written, skipped == fetched -
    # validated) mean a shape failure must be reported as validated=0/written=0
    # even though the payload was deliberately still written to raw storage —
    # the sidecar's shape_validation field is where that fact lives. skipped=1
    # is what makes classify_run mark the run FAILED_PARTIAL.
    ok = bool(shape["ok"])
    result = StageResult(
        stage="core",
        fetched=1,
        validated=1 if ok else 0,
        written=1 if ok else 0,
        skipped=0 if ok else 1,
    )
    return StageOutcome(
        result=result,
        output=core_handoff(raw),
        lineage=StageLineage.from_metadata(
            CORE_STAGE, raw_artifacts=(write.payload_key,)
        ),
    )


def core_handoff(raw: RawResponse) -> CoreData:
    """Extract the downstream handoff from a captured bootstrap payload.

    Deliberately tolerant: a payload that failed shape validation still yields
    whatever it does carry, and an unusable one yields empty lists rather than
    raising. Nothing here is persisted, so a bad extraction costs a degraded
    downstream stage, not corrupt data.
    """
    payload = raw.json()
    if not isinstance(payload, dict):
        return CoreData(events=[], player_ids=[])

    raw_events = payload.get("events") or []
    events: list[GameweekInfo] = []
    if isinstance(raw_events, list):
        for e in raw_events:
            if not isinstance(e, dict):
                continue
            if not isinstance(e.get("id"), int):
                continue
            if not isinstance(e.get("finished"), bool) or not isinstance(e.get("is_current"), bool):
                continue
            events.append(GameweekInfo(id=e["id"], finished=e["finished"], is_current=e["is_current"]))

    raw_elements = payload.get("elements") or []
    player_ids = [
        element["id"]
        for element in raw_elements
        if isinstance(element, dict) and isinstance(element.get("id"), int)
    ] if isinstance(raw_elements, list) else []

    return CoreData(events=events, player_ids=player_ids)


def validate_bootstrap_shape(raw: RawResponse) -> dict[str, Any]:
    """Return the raw-boundary structural verdict for a bootstrap-static response.

    Checks exactly what strategy doc B.2 permits at this boundary and stops:
    the status is 2xx, the body parses as JSON, the top level is an object, the
    required top-level keys are present, and a sampled element record carries
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
