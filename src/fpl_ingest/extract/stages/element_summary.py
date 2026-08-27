"""Element-summary raw-capture pipeline stage.

Concurrently fetches the ``element-summary`` endpoint for every player and
writes each response verbatim through ``LocalRawWriter`` — payload bytes,
per-object metadata sidecar, and the shared run manifest — under endpoint
``element-summary/{player_id}`` (strategy doc A.3). One object per player per
run; ``{player_id}`` is part of the endpoint segment, so it sits before
``{extraction_date}`` in the key, matching every other captured endpoint.

This stage no longer writes SQLite. ``raw_history_rows`` and
``upsert_history_rows`` were removed deliberately (strategy doc B.1):
flatten-and-upsert is warehouse work, and ``player_histories`` was the last
table in the schema contract — it, and the compiler/DDL machinery that
existed to serve it, are retired in the same change. The redirect also fixes,
for free, the ``fixtures``/``history_past`` loss the strategy doc's audit
found (§2.1/§6): writing the payload whole means nothing captured is
discarded any more — previously only ``history`` was ever read from the
cached file.

``_fetch_player_histories`` keeps its concurrency and strict-mode
cancellation semantics exactly — only what it fetches, and what it does with
each response, changed. The dead ``force`` parameter (strategy doc §4.2:
never read in the function body, so its effect was always "fetch every
player, every run") is not ported through the redirect — this stage
unconditionally fetches every player on every run, which was already the
real behaviour.
"""

from __future__ import annotations

import asyncio
import logging
from typing import Any

from fpl_ingest.extract.http.client import (
    _ENDPOINTS,
    AsyncFPLClient,
    RawResponse,
    cancel_pending_tasks,
)
from fpl_ingest.extract.http.local_writer import LocalRawWriter
from fpl_ingest.orchestration.execution_state import PipelineExecutionState
from fpl_ingest.orchestration.stage_result import StageLineage, StageMetadata, StageOutcome, StageResult

logger = logging.getLogger(__name__)

RAW_SOURCE = "fpl"

#: Top-level keys an ``element-summary/{player_id}`` payload must carry (strategy doc B.2).
_REQUIRED_TOP_LEVEL_KEYS = ("history", "fixtures", "history_past")

#: Identifying fields a sampled history row must carry (strategy doc B.2).
_SAMPLED_HISTORY_FIELDS = ("element", "round", "fixture", "minutes", "total_points")

#: The stage writes one raw object per player and no tables.
PLAYER_HISTORIES_STAGE = StageMetadata(
    name="player_histories",
    dependencies=("core",),
    raw_artifacts=(),
    output_tables=(),
)


class _StrictFetchFailure(RuntimeError):
    """Raised to abort a concurrent strict-mode fetch batch immediately."""


def raw_endpoint(player_id: int) -> str:
    """Return the raw-contract endpoint identity for one player (strategy doc A.3)."""
    return f"element-summary/{player_id}"


async def ingest_player_histories(
    client: AsyncFPLClient,
    raw_writer: LocalRawWriter,
    player_ids: list[int],
    *,
    strict: bool = False,
    execution_state: PipelineExecutionState | None = None,
) -> StageOutcome[None]:
    """Fetch element-summary for every player and capture each response verbatim.

    Args:
        client: Async FPL client for the HTTP fetches.
        raw_writer: Writer for this run; also accumulates the run manifest.
            The same writer the other capture stages use — one manifest per
            run covers every endpoint and every player it touches.
        player_ids: FPL element IDs to fetch this run — every player, every
            run (strategy doc §4.2: the old ``force`` parameter was already
            dead, so this was always the real behaviour).
        strict: If True, the first failed fetch cancels the rest of the batch.
        execution_state: Fail-fast sentinel.

    Returns:
        StageOutcome whose result counts captured objects, not rows — this
        stage no longer produces rows. Each player whose payload fails shape
        validation contributes one ``skipped`` so ``classify_run`` marks the
        run FAILED_PARTIAL while every other player still counts as written;
        the payload is written either way.
    """
    if execution_state is not None and execution_state.is_failed:
        logger.info("Fail-fast tripped; skipping element-summary capture")
        return StageOutcome(result=StageResult(stage="player_histories"))

    if not player_ids:
        return StageOutcome(
            result=StageResult(stage="player_histories"),
            lineage=StageLineage.from_metadata(PLAYER_HISTORIES_STAGE),
        )

    logger.info("Collecting element-summary for %d players...", len(player_ids))

    fetched, error_count = await _fetch_player_histories(
        client, player_ids, raw_writer, strict=strict
    )

    if strict and error_count > 0:
        if execution_state is not None:
            execution_state.fail()
        return StageOutcome(
            result=StageResult(
                stage="player_histories",
                fetched=len(fetched),
                skipped=len(fetched),
                errors=error_count,
            ),
            lineage=StageLineage.from_metadata(PLAYER_HISTORIES_STAGE),
        )

    payload_keys: list[str] = []
    validated = 0
    for player_id in sorted(fetched):
        raw = fetched[player_id]
        endpoint = raw_endpoint(player_id)
        shape = validate_element_summary_shape(raw)
        if not shape["ok"]:
            logger.error(
                "Player %d payload failed shape validation (%s); writing it anyway",
                player_id,
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
        logger.debug(
            "Captured player %d: %d bytes -> %s",
            player_id,
            write.content_length,
            write.payload_key,
        )

    # StageResult counts objects here, not rows: one captured object per
    # player. Its invariants (fetched >= validated >= written, skipped ==
    # fetched - validated) mean a shape failure must be reported as not
    # validated and not written even though the payload was deliberately
    # still written to raw storage — the sidecar's shape_validation field is
    # where that fact lives. skipped > 0 is what makes classify_run mark the
    # run FAILED_PARTIAL, and it does so without discounting the players that
    # captured cleanly.
    fetched_count = len(fetched)
    return StageOutcome(
        result=StageResult(
            stage="player_histories",
            fetched=fetched_count,
            validated=validated,
            written=validated,
            skipped=fetched_count - validated,
            errors=error_count,
        ),
        lineage=StageLineage.from_metadata(PLAYER_HISTORIES_STAGE, raw_artifacts=payload_keys),
    )


def validate_element_summary_shape(raw: RawResponse) -> dict[str, Any]:
    """Return the raw-boundary structural verdict for an ``element-summary`` response.

    Checks exactly what strategy doc B.2 permits at this boundary and stops:
    the status is 2xx, the body parses as JSON, the top level is an object,
    ``history``/``fixtures``/``history_past`` are present, and a sampled
    ``history`` row carries its identifying fields. Nothing about types,
    ranges, or cross-record consistency — that is warehouse work.

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

    history = payload.get("history")
    if not isinstance(history, list):
        # Without a history list there is no record to sample; the missing-key
        # or type problem is already reported above.
        if "history" in payload:
            failures.append(
                f"required_top_level_keys_present: history is {type(history).__name__}"
            )
        return _verdict(checks, failures, record_count=None)

    checks.append("sampled_record_has_identifying_fields")
    if history:
        sample = history[0]
        if not isinstance(sample, dict):
            failures.append(
                "sampled_record_has_identifying_fields: record is "
                f"{type(sample).__name__}"
            )
        else:
            missing = [f for f in _SAMPLED_HISTORY_FIELDS if f not in sample]
            if missing:
                failures.append(
                    "sampled_record_has_identifying_fields: missing "
                    + ", ".join(missing)
                )

    return _verdict(checks, failures, record_count=len(history))


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


async def _fetch_player_histories(
    client: AsyncFPLClient,
    player_ids: list[int],
    raw_writer: LocalRawWriter,
    *,
    strict: bool,
) -> tuple[dict[int, RawResponse], int]:
    """Fetch every player's element-summary, cancelling on the first strict failure."""
    fetched: dict[int, RawResponse] = {}
    error_count = 0

    if not strict:
        raw_results = await asyncio.gather(
            *[_fetch_one_player(client, pid) for pid in player_ids],
            return_exceptions=True,
        )

        for player_id, result in zip(player_ids, raw_results):
            if isinstance(result, BaseException):
                error_count += 1
                logger.error("Failed player fetch %d: %s", player_id, result)
                _record_fetch_failure(raw_writer, player_id, result)
                continue
            pid, raw = result
            fetched[pid] = raw

        return fetched, error_count

    tasks = {
        asyncio.create_task(_fetch_one_player(client, pid)): pid
        for pid in player_ids
    }

    try:
        pending = set(tasks)
        while pending:
            done, pending = await asyncio.wait(pending, return_when=asyncio.FIRST_COMPLETED)
            for task in done:
                player_id = tasks[task]
                try:
                    pid, raw = task.result()
                except Exception as exc:
                    error_count += 1
                    logger.error("Failed player fetch %d: %s", player_id, exc)
                    _record_fetch_failure(raw_writer, player_id, exc)
                    await cancel_pending_tasks(pending)
                    raise _StrictFetchFailure from exc
                fetched[pid] = raw
    except _StrictFetchFailure:
        return fetched, error_count

    return fetched, error_count


def _record_fetch_failure(
    raw_writer: LocalRawWriter, player_id: int, exc: BaseException
) -> None:
    """Record one player's failed capture in the shared run manifest."""
    raw_writer.record_failure(
        raw_endpoint(player_id),
        request_url=_ENDPOINTS["player"].format(player_id=player_id),
        error_class=type(exc).__name__,
        message=str(exc),
    )


async def _fetch_one_player(
    client: AsyncFPLClient,
    player_id: int,
) -> tuple[int, RawResponse]:
    raw = await client.get_element_summary_raw(player_id)
    logger.debug("Player %d — %d bytes fetched", player_id, len(raw.body))
    return player_id, raw
