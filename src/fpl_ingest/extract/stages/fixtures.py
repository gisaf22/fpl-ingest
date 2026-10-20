"""Fixture raw-capture pipeline stage.

Fetches the FPL fixtures endpoint and writes the response verbatim through
``LocalRawWriter`` — payload bytes, per-object metadata sidecar, and the run
manifest. It performs the minimal structural checks the raw boundary calls for
(strategy doc B.2) and nothing more.

This stage no longer writes SQLite. ``process_fixtures_payload``,
``upsert_fixtures``, ``flatten_fixture_stat_rows``, and ``upsert_fixture_stats``
were removed deliberately (strategy doc B.1): flatten-and-upsert is warehouse
work, and the decision was taken not to dual-write during the migration. The
``fixtures`` and ``fixture_stats`` tables in fpl.db are consequently frozen at
their last-written state until fpl-warehouse reads from raw storage.
"""

from __future__ import annotations

import logging
from typing import Any

from fpl_ingest.extract.http.client import _ENDPOINTS, AsyncFPLClient, RawResponse
from fpl_ingest.extract.http.local_writer import LocalRawWriter
from fpl_ingest.extract.http.response_router import ResponseClass, record_fetch_failed, route_response
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
RAW_ENDPOINT = "fixtures"

#: Identifying fields a sampled fixture record must carry (strategy doc B.2).
_SAMPLED_FIXTURE_FIELDS = ("id", "team_h", "team_a", "event")

#: The stage writes one raw object and no tables. ``raw_artifacts`` is filled
#: in per run with the actual payload key, so the metadata default is empty.
FIXTURES_STAGE = StageMetadata(
    name="fixtures",
    raw_artifacts=(),
    output_tables=(),
)


async def ingest_fixtures(
    client: AsyncFPLClient,
    raw_writer: LocalRawWriter,
    *,
    execution_state: PipelineExecutionState | None = None,
) -> StageOutcome[None]:
    """Fetch fixtures and capture the response verbatim into raw storage.

    Args:
        client: Async FPL client for the HTTP fetch.
        raw_writer: Writer for this run; also accumulates the run manifest.
        execution_state: Fail-fast sentinel. When a previous stage has already
            failed, the capture is skipped rather than written.

    Returns:
        StageOutcome whose result counts captured objects, not rows — this
        stage no longer produces rows. An invalid response (a non-2xx status or
        a failed shape check) reports ``skipped=1``, which strict mode aborts
        on; it is kept under ``fpl/_invalid/``, never as ``payload.json``, and
        the writer counts the endpoint as failed.
    """
    if execution_state is not None and execution_state.is_failed:
        logger.info("Fail-fast tripped; skipping fixtures capture")
        return StageOutcome(result=StageResult(stage="fixtures"))

    logger.info("Fetching fixtures...")
    try:
        raw = await client.get_fixtures_raw()
    except FPLClientError as exc:
        logger.error("Failed to fetch fixtures: %s", exc)
        raw_writer.record_failure(
            RAW_ENDPOINT,
            request_url=_ENDPOINTS["fixtures"],
            error_class=type(exc).__name__,
            message=str(exc),
        )
        return StageOutcome(result=StageResult(stage="fixtures", errors=1))

    shape = validate_fixtures_shape(raw)
    routed = route_response(raw_writer, RAW_ENDPOINT, raw, shape)
    write = routed.write
    if write is None:  # a 429 or 5xx that outlived the client's retries
        logger.error("Failed to fetch fixtures: HTTP %d", raw.status)
        record_fetch_failed(raw_writer, RAW_ENDPOINT, raw)
        return StageOutcome(result=StageResult(stage="fixtures", errors=1))

    if routed.response_class is not ResponseClass.VALID:
        logger.error(
            "Fixtures response is %s (%s); kept at %s, not as a payload",
            routed.response_class.value,
            ", ".join(shape["failures"]),
            write.payload_key,
        )
        # Counted as skipped, which strict mode aborts on. Nothing usable was
        # written, so the writer has already marked the endpoint failed.
        return StageOutcome(
            result=StageResult(stage="fixtures", fetched=1, skipped=1),
            lineage=StageLineage.from_metadata(FIXTURES_STAGE, raw_artifacts=()),
        )

    logger.info(
        "Captured fixtures: %d bytes -> %s", write.content_length, write.payload_key
    )
    result = StageResult(stage="fixtures", fetched=1, validated=1, written=1)
    return StageOutcome(
        result=result,
        lineage=StageLineage.from_metadata(
            FIXTURES_STAGE, raw_artifacts=(write.payload_key,)
        ),
    )


def validate_fixtures_shape(raw: RawResponse) -> dict[str, Any]:
    """Return the raw-boundary structural verdict for a fixtures response.

    Checks exactly what strategy doc B.2 permits at this boundary and stops:
    the status is 2xx, the body parses as JSON, the top level is a list, and a
    sampled record carries its identifying fields. Nothing about types, ranges,
    or cross-record consistency — that is warehouse work.

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

    checks.append("top_level_is_list")
    if not isinstance(payload, list):
        failures.append(f"top_level_is_list: got {type(payload).__name__}")
        return _verdict(checks, failures, record_count=None)

    checks.append("sampled_record_has_identifying_fields")
    if payload:
        sample = payload[0]
        if not isinstance(sample, dict):
            failures.append(
                f"sampled_record_has_identifying_fields: record is {type(sample).__name__}"
            )
        else:
            missing = [f for f in _SAMPLED_FIXTURE_FIELDS if f not in sample]
            if missing:
                failures.append(
                    "sampled_record_has_identifying_fields: missing "
                    + ", ".join(missing)
                )

    return _verdict(checks, failures, record_count=len(payload))


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
