"""Lightweight structural smoke test for upstream FPL API drift."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from typing import Any

from fpl_ingest.transport.async_client import AsyncFPLClient

DEFAULT_SAMPLE_SIZE = 5


# ---------------------------------------------------------------------------
# Result types
# ---------------------------------------------------------------------------


class SmokeTestFailure(RuntimeError):
    """Raised when the upstream API shape no longer matches expectations."""


@dataclass(frozen=True)
class SmokeTestResult:
    """Summary of the smoke test execution."""

    endpoints_checked: tuple[str, ...]
    sample_size: int


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def run_smoke_test(*, sample_size: int = DEFAULT_SAMPLE_SIZE) -> SmokeTestResult:
    """Run the smoke test and return a summary or raise on structural drift."""
    return asyncio.run(run_smoke_test_async(sample_size=sample_size))


async def run_smoke_test_async(*, sample_size: int = DEFAULT_SAMPLE_SIZE) -> SmokeTestResult:
    """Run lightweight structural checks against key FPL endpoints."""
    async with AsyncFPLClient() as client:
        bootstrap = await client.get_bootstrap()
        _check_bootstrap_static(bootstrap, sample_size=sample_size)

        fixtures = await client.get_fixtures()
        _check_fixtures(fixtures, sample_size=sample_size)

        player_ids = _sample_player_ids(bootstrap, sample_size=sample_size)
        await _check_player_history(client, player_ids, sample_size=sample_size)

    return SmokeTestResult(
        endpoints_checked=("bootstrap-static", "fixtures", "element-summary"),
        sample_size=sample_size,
    )


# ---------------------------------------------------------------------------
# Endpoint checks
# ---------------------------------------------------------------------------


def _check_bootstrap_static(payload: Any, *, sample_size: int) -> None:
    _require_mapping(payload, "bootstrap-static")

    for key in ("elements", "teams", "events", "element_types"):
        _require_key(payload, key, "bootstrap-static")

    _check_record_list(
        payload["elements"],
        label="elements[]",
        required_fields=("id", "team", "now_cost"),
        sample_size=sample_size,
    )
    _check_record_list(
        payload["teams"],
        label="teams[]",
        required_fields=("id", "name", "short_name"),
        sample_size=sample_size,
    )
    _check_record_list(
        payload["events"],
        label="events[]",
        required_fields=("id", "deadline_time", "finished"),
        sample_size=sample_size,
    )
    _check_record_list(
        payload["element_types"],
        label="element_types[]",
        required_fields=("id", "singular_name_short", "plural_name_short"),
        sample_size=sample_size,
    )


def _check_fixtures(payload: Any, *, sample_size: int) -> None:
    _require_list(payload, "fixtures")
    _check_record_list(
        payload,
        label="fixtures[]",
        required_fields=("id", "team_h", "team_a", "event"),
        sample_size=sample_size,
    )


async def _check_player_history(
    client: AsyncFPLClient,
    player_ids: list[int],
    *,
    sample_size: int,
) -> None:
    if not player_ids:
        raise SmokeTestFailure("Missing field: elements[].id")

    last_empty_payload = False
    for player_id in player_ids:
        payload = await client.get_player_history(player_id)
        _require_mapping(payload, f"element-summary[{player_id}]")
        _require_key(payload, "history", "element-summary")
        _require_key(payload, "history_past", "element-summary")

        history = payload["history"]
        _require_list(history, "history")
        if not history:
            last_empty_payload = True
            continue

        _check_record_list(
            history,
            label="history[]",
            required_fields=("element", "round", "fixture", "minutes", "total_points"),
            sample_size=sample_size,
        )
        return

    if last_empty_payload:
        return


# ---------------------------------------------------------------------------
# Sampling helpers
# ---------------------------------------------------------------------------


def _sample_player_ids(bootstrap: dict[str, Any], *, sample_size: int) -> list[int]:
    elements = bootstrap.get("elements", [])
    if not isinstance(elements, list):
        raise SmokeTestFailure("Missing field: bootstrap-static.elements")

    player_ids: list[int] = []
    for record in elements[:sample_size]:
        if isinstance(record, dict) and isinstance(record.get("id"), int):
            player_ids.append(record["id"])
    return player_ids


# ---------------------------------------------------------------------------
# Validation helpers
# ---------------------------------------------------------------------------


def _check_record_list(
    payload: Any,
    *,
    label: str,
    required_fields: tuple[str, ...],
    sample_size: int,
) -> None:
    _require_list(payload, label)
    for record in payload[:sample_size]:
        _require_mapping(record, label)
        for field_name in required_fields:
            if field_name not in record:
                raise SmokeTestFailure(f"Missing field: {label}.{field_name}")


def _require_key(payload: dict[str, Any], key: str, label: str) -> None:
    if key not in payload:
        raise SmokeTestFailure(f"Missing field: {label}.{key}")


def _require_mapping(payload: Any, label: str) -> None:
    if not isinstance(payload, dict):
        raise SmokeTestFailure(f"Expected object: {label}")


def _require_list(payload: Any, label: str) -> None:
    if not isinstance(payload, list):
        raise SmokeTestFailure(f"Expected list: {label}")
