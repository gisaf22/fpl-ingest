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

Existence alone is not enough at the moment a gameweek settles, though.
``influence``/``creativity``/``threat``/``ict_index`` are only populated by
FPL *at* ratification, so a player captured while the gameweek was still
provisional carries zeroes in all four, and existence-based skipping would
freeze those zeroes permanently. The run that first observes the current
gameweek as settled therefore forces a re-fetch of *every* player, not just
the never-captured ones, and only then records a ``_settlement`` marker for
that gameweek; from the next run on, normal existence-based skipping resumes
and holds. The marker is the sole piece of cross-run state here, and it is
written only when the forced re-fetch fully succeeded, so a partial failure
retries on the following run instead of stranding stale captures.

A gameweek still in progress must always trigger a fetch, existence or not: a
player captured mid-gameweek has a ``history`` missing that gameweek's row,
and once the gameweek settles there is no later trigger to go back and get
it. Settlement state is read off the *current* gameweek (from ``events``,
the same ``GameweekInfo`` list ``gameweeks.py`` uses) against the same
``event_finality`` map produced by the event-status stage.
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
from fpl_ingest.extract.http.local_writer import LocalRawWriter, RawStorageBackend
from fpl_ingest.extract.http.raw_keys import (
    SETTLEMENT_PREFIX,
    iso_utc,
    settlement_marker_key,
)
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
            run covers every endpoint and every player it touches. Its
            ``backend`` is also queried to check whether a player has ever
            been captured before (§below) — always the actual active backend
            (local filesystem or S3), never a hardcoded local path.
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

    settlement_event_id = _settlement_refetch_event(
        raw_writer.backend, events, event_finality
    )
    player_ids_to_fetch = _select_players_to_fetch(
        raw_writer.backend,
        player_ids,
        events,
        event_finality=event_finality,
        force_all=settlement_event_id is not None,
    )
    skipped_count = len(player_ids) - len(player_ids_to_fetch)
    if _latest_gameweek_settled(events, event_finality) is not True:
        logger.info(
            "element-summary: gameweek not yet settled, fetching all %d players",
            len(player_ids_to_fetch),
        )
    elif settlement_event_id is not None:
        logger.info(
            "element-summary: gameweek %d newly settled, forcing a re-fetch of all %d "
            "players so ratification-only fields (influence/creativity/threat/ict_index) "
            "are captured",
            settlement_event_id,
            len(player_ids_to_fetch),
        )
    else:
        logger.info(
            "element-summary: %d players fetched, %d skipped (already captured, latest gameweek settled)",
            len(player_ids_to_fetch),
            skipped_count,
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
    if settlement_event_id is not None:
        if error_count == 0 and validated == len(player_ids_to_fetch):
            _record_settlement_refetch(raw_writer, settlement_event_id, validated)
            logger.info(
                "element-summary: settlement re-fetch for gameweek %d complete (%d players)",
                settlement_event_id,
                validated,
            )
        else:
            logger.warning(
                "element-summary: settlement re-fetch for gameweek %d incomplete "
                "(%d/%d captured cleanly, %d errors); marker withheld so the next run retries",
                settlement_event_id,
                validated,
                len(player_ids_to_fetch),
                error_count,
            )
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
    backend: RawStorageBackend,
    player_ids: list[int],
    events: list[GameweekInfo],
    *,
    event_finality: Finality | None,
    force_all: bool = False,
) -> list[int]:
    """Determine which player IDs need an element-summary fetch this run.

    ============================  ===============  ======
    latest gameweek               capture exists?   action
    ============================  ===============  ======
    settled, settlement run       either            fetch — forced re-fetch
    settled                       yes               skip
    settled                       no                fetch — new player / backfill
    provisional / unknown         either             fetch — history incomplete
    ============================  ===============  ======

    ``force_all`` is the settlement-transition case, decided by
    :func:`_settlement_refetch_event`: the ratification-only fields make an
    existing capture wrong rather than merely old, so every player is
    re-fetched exactly once regardless of what exists.

    "Latest gameweek settled" is a single season-wide fact, not a per-player
    one: it is the current gameweek's finality, read the same way
    ``gameweeks._needs_fetch`` reads it. A gameweek still in progress means
    every player's ``history`` is missing that gameweek's row, so existence
    alone must not skip anyone until it settles.
    """
    if force_all or _latest_gameweek_settled(events, event_finality) is not True:
        return list(player_ids)

    return [
        player_id
        for player_id in player_ids
        if not _has_element_summary_capture(backend, player_id)
    ]


def _latest_gameweek_settled(
    events: list[GameweekInfo], event_finality: Finality | None
) -> bool | None:
    """Whether the current gameweek is fully settled.

    Returns ``None`` (treated as "not settled", i.e. fetch everything) when
    that cannot be determined: no finality map, an *empty* finality map, or no
    current gameweek found in ``events`` yet (e.g. pre-season). Mirrors the
    fail-safe rule in ``gameweeks._needs_fetch`` — an unknown state must never
    cause under-fetching.

    An empty map is treated the same as ``None`` deliberately, and
    differently from ``gameweeks._needs_fetch``'s per-gameweek absent-key
    check: this function only ever asks about *one* gameweek — the current
    one — never an already-finished one whose dates could legitimately have
    aged out of event-status's window. A current gameweek missing from a
    *non-empty* map (checked below) means event-status covers other dates and
    genuinely has none for this one, which is the aged-out case. A current
    gameweek missing because the whole map is empty instead means event-status
    returned no per-date data at all — an unknown state, not evidence of
    settlement — so it must not be read as "skip everyone."
    """
    if not event_finality:
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


def _settlement_refetch_event(
    backend: RawStorageBackend,
    events: list[GameweekInfo],
    event_finality: Finality | None,
) -> int | None:
    """Return the gameweek whose settlement still owes a forced full re-fetch.

    ``None`` — the common case — means no forced re-fetch is due: either the
    current gameweek is not settled (or settlement is unknown), in which case
    everyone is fetched anyway, or it is settled and this run is not the
    transition, in which case existence-based skipping applies.

    The transition is detected against a durable per-gameweek marker rather
    than against the previous run's finality, which nothing persists. Existence
    of the marker is the entire signal, so this needs no read surface beyond
    ``exists_prefix`` (see ``RawStorageBackend``'s docstring).
    """
    if _latest_gameweek_settled(events, event_finality) is not True:
        return None

    current_id = next((e.id for e in events if e.is_current), None)
    if current_id is None:  # pragma: no cover - implied by the check above
        return None

    if backend.exists_prefix(_settlement_marker_prefix(current_id)):
        return None

    return current_id


def _settlement_marker_prefix(event_id: int) -> str:
    """Return the prefix ``exists_prefix`` is asked about for one gameweek."""
    return f"{RAW_SOURCE}/{SETTLEMENT_PREFIX}/element-summary/{event_id}"


def _record_settlement_refetch(
    raw_writer: LocalRawWriter, event_id: int, player_count: int
) -> None:
    """Mark this gameweek's forced post-settlement re-fetch as done.

    Called only once every selected player captured cleanly, so a partial
    failure leaves the marker absent and the next run retries the forced
    re-fetch rather than freezing the players that did not make it.
    """
    marker = {
        "event": event_id,
        "endpoint": "element-summary",
        "run_id": raw_writer.run_id,
        "recorded_at": iso_utc(datetime.now(timezone.utc)),
        "players_refetched": player_count,
    }
    raw_writer.backend.put_bytes(
        settlement_marker_key(RAW_SOURCE, "element-summary", event_id),
        json.dumps(marker, sort_keys=True).encode("utf-8"),
    )


def _has_element_summary_capture(backend: RawStorageBackend, player_id: int) -> bool:
    """Whether this player's element-summary endpoint has ever been captured.

    Queries the actual active backend (local filesystem or S3) rather than a
    hardcoded local path — see ``gameweeks._has_event_live_capture``, which
    shares this exact pattern and the bug it was fixed alongside.
    """
    return backend.exists_prefix(f"{RAW_SOURCE}/{raw_endpoint(player_id)}")


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
