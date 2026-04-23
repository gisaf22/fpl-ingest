"""Shared pipeline helpers for cache writes and strict async cancellation."""

from __future__ import annotations

import asyncio
import json
from pathlib import Path
from typing import Any

from fpl_ingest.domain.execution_state import PipelineExecutionState


async def cancel_pending_tasks(tasks: set[asyncio.Task[Any]]) -> None:
    """Cancel pending tasks and await their completion."""
    if not tasks:
        return
    for task in tasks:
        task.cancel()
    await asyncio.gather(*tasks, return_exceptions=True)


def write_json_cache(
    path: Path,
    data: object,
    *,
    execution_state: PipelineExecutionState | None = None,
) -> None:
    """Write a JSON cache file atomically unless fail-fast has already tripped."""
    if execution_state is not None and execution_state.is_failed:
        return
    tmp = path.with_suffix(".tmp")
    tmp.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
    tmp.rename(path)
