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
each response, changed.

Finality-aware skipping (matching ``gameweeks.py``'s pattern): a player whose
``element-summary`` has already been captured is skipped once the latest
gameweek is settled, on the theory that ``history`` (the only field with no
equivalent elsewhere) cannot change further once the gameweek it reports has
settled. This is existence-based, not content-based — it checks whether a
capture directory exists, exactly like ``gameweeks._has_event_live_capture``,
and deliberately does not read any payload back (``RawStorageBackend`` is
write-only by design; see its docstring). The embedded ``fixtures`` block can
go stale between settlements as a result (a mid-week reschedule wouldn't be
reflected until the next gameweek settles and the player is refetched), but
every field in it (``is_home``, ``difficulty``, ``event_name``) is derivable
from the global ``fixtures`` endpoint and ``bootstrap-static``, both captured
every run regardless — so nothing is actually lost.

A gameweek still in progress must always trigger a fetch, existence or not: a
player captured mid-gameweek has a ``history`` missing that gameweek's row,
and once the gameweek settles there is no later trigger to go back and get
it. Settlement state is read off the *current* gameweek (from ``events``,
the same ``GameweekInfo`` list ``gameweeks.py`` uses) against the same
``event_finality`` map produced by the event-status stage.
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
    raw_dir: Path,
    player_ids: list[int],
    events: list[GameweekInfo],
    *,
    event_finality: Finality | None,
    strict: bool = False,
    execution_state: PipelineExecutionState | None = None,
) -> StageOutcome[None]:
    """Fetch element-summary for every player that needs it and capture verbatim.

    Args:
        client: Async FPL client for the HTTP fetches.
        raw_writer: Writer for this run; also accumulates the run manifest.
            The same writer the other capture stages use — one manifest per
            run covers every endpoint and every player it touches.
        raw_dir: Local raw-capture root; used to check whether a player has
            ever been captured before (§below).
        player_ids: FPL element IDs known this run (from the core stage).
        events: GameweekInfo list from the core stage; used to find the
            current gameweek's settlement state.
        event_finality: The per-event finality map from this run's
            event-status capture, or ``None`` when that capture failed or did
            not validate. ``None`` means "nothing is known settled" and must
            never cause under-fetching.
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

    player_ids_to_fetch = _select_players_to_fetch(
        raw_dir, player_ids, events, event_finality=event_finality
    )
    logger.info(
        "element-summary: %d players fetched, %d skipped (already captured, latest gameweek settled)",
        len(player_ids_to_fetch),
        len(player_ids) - len(player_ids_to_fetch),
    )

    if not player_ids_to_fetch:
        logger.info("All players already captured for the settled gameweek.")
        return StageOutcome(
            result=StageResult(stage="player_histories"),
            lineage=StageLineage.from_metadata(PLAYER_HISTORIES_STAGE),
        )

    logger.info("Collecting element-summary for %d players...", len(player_ids_to_fetch))

    fetched, error_count = await _fetch_player_histories(
        client, player_ids_to_fetch, raw_writer, strict=strict
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


def _select_players_to_fetch(
    raw_dir: Path,
    player_ids: list[int],
    events: list[GameweekInfo],
    *,
    event_finality: Finality | None,
) -> list[int]:
    """Determine which player IDs need an element-summary fetch this run.

    ============================  ===============  ======
    latest gameweek               capture exists?   action
    ============================  ===============  ======
    settled                       yes               skip
    settled                       no                fetch — new player / backfill
    provisional / unknown         either             fetch — history incomplete
    ============================  ===============  ======

    "Latest gameweek settled" is a single season-wide fact, not a per-player
    one: it is the current gameweek's finality, read the same way
    ``gameweeks._needs_fetch`` reads it. A gameweek still in progress means
    every player's ``history`` is missing that gameweek's row, so existence
    alone must not skip anyone until it settles.
    """
    if _latest_gameweek_settled(events, event_finality) is not True:
        return list(player_ids)

    return [
        player_id
        for player_id in player_ids
        if not _has_element_summary_capture(raw_dir, player_id)
    ]


def _latest_gameweek_settled(
    events: list[GameweekInfo], event_finality: Finality | None
) -> bool | None:
    """Whether the current gameweek is fully settled.

    Returns ``None`` (treated as "not settled", i.e. fetch everything) when
    that cannot be determined: no finality map, or no current gameweek found
    in ``events`` yet (e.g. pre-season). Mirrors the fail-safe rule in
    ``gameweeks._needs_fetch`` — an unknown state must never cause
    under-fetching.
    """
    if event_finality is None:
        return None

    current_id = next((e.id for e in events if e.is_current), None)
    if current_id is None:
        return None

    info = event_finality.get(current_id)
    if info is None:
        # No entry means event-status's current-window array has rolled past
        # this gameweek — the normal state for one settled well in the past.
        return True

    return bool(info.get("bonus_added"))


def _has_element_summary_capture(raw_dir: Path, player_id: int) -> bool:
    """Whether this player's element-summary endpoint has ever been captured."""
    return (raw_dir / RAW_SOURCE / raw_endpoint(player_id)).is_dir()


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
