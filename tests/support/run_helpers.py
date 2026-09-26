"""Helpers shared by the end-to-end run tests under ``tests/integration/orchestration``."""

from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from pathlib import Path

from fpl_ingest.extract.http.local_writer import LocalFilesystemBackend
from fpl_ingest.extract.http.raw_keys import SETTLEMENT_MARKER_FILENAME, SETTLEMENT_PREFIX
from fpl_ingest.extract.http.sync_http import FPLClientError
from tests.support.cli_fakes import MINIMAL_BOOTSTRAP, PLAYER_HISTORY_1, PLAYER_HISTORY_2


def _manifest(raw: Path) -> dict:
    """Return the only run manifest under ``raw``."""
    paths = sorted((raw / "fpl" / "_manifests").rglob("manifest.json"))
    assert len(paths) == 1, paths
    return json.loads(paths[0].read_text())


def _history_failing_for(*failing: int):
    async def side_effect(pid):
        if pid in failing:
            raise FPLClientError(f"player {pid} unreachable")
        return PLAYER_HISTORY_1 if pid == 1 else PLAYER_HISTORY_2

    return side_effect


def _seed_settled_gameweek(raw: Path, gw: int, player_ids: list[int]) -> None:
    """Leave raw storage as a previous run would after gameweek ``gw`` settled."""
    backend = LocalFilesystemBackend(raw)
    for endpoint in ("event-live", "element-summary"):
        backend.put_bytes(
            f"fpl/{SETTLEMENT_PREFIX}/{endpoint}/{gw}/{SETTLEMENT_MARKER_FILENAME}", b"{}"
        )
    for pid in player_ids:
        backend.put_bytes(f"fpl/element-summary/{pid}/2026-09-01/seed/payload.json", b"{}")


def _bootstrap_with_deadline_in(minutes: float) -> dict:
    deadline = datetime.now(timezone.utc) + timedelta(minutes=minutes)
    return {
        **MINIMAL_BOOTSTRAP,
        "events": [
            {
                "id": 6,
                "finished": False,
                "is_current": False,
                "deadline_time": deadline.strftime("%Y-%m-%dT%H:%M:%SZ"),
            }
        ],
    }
