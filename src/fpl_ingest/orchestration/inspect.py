"""Manifest-only run inspection for the ``inspect`` CLI command.

Reads run manifests back from ``{raw_dir}/{source}/_manifests/{date}/{run_id}/manifest.json``
(``LocalRawWriter``'s own layout — see ``raw_keys.manifest_key``) and nothing
else: no payload directories are walked, and no per-object sidecars are read.
This is deliberate (strategy doc A.5) so the same code works unchanged once
storage moves to S3, where listing payload prefixes is expensive but listing
``_manifests/`` is not.

Run ids embed a UTC start timestamp (``raw_keys.new_run_id``), so lexical sort
on the run id is chronological sort — no manifest field needs parsing to
order runs.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

RAW_SOURCE = "fpl"


@dataclass(frozen=True)
class RunManifest:
    """One run's manifest, plus the path it was read from."""

    path: Path
    data: dict[str, Any]

    @property
    def run_id(self) -> str:
        return str(self.data.get("run_id", ""))


def list_run_manifests(raw_dir: Path, *, source: str = RAW_SOURCE) -> list[RunManifest]:
    """Return every manifest under ``raw_dir``, oldest first.

    Globs only ``{raw_dir}/{source}/_manifests/**/manifest.json`` — never the
    endpoint payload prefixes alongside it.
    """
    manifests_root = Path(raw_dir) / source / "_manifests"
    if not manifests_root.is_dir():
        return []

    results: list[RunManifest] = []
    for path in manifests_root.glob("*/*/manifest.json"):
        try:
            data = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            continue
        results.append(RunManifest(path=path, data=data))

    results.sort(key=lambda m: (m.run_id, str(m.path)))
    return results


def most_recent_run(raw_dir: Path, *, source: str = RAW_SOURCE) -> RunManifest | None:
    """Return the manifest for the most recently started run, or None."""
    manifests = list_run_manifests(raw_dir, source=source)
    return manifests[-1] if manifests else None


def recent_runs(raw_dir: Path, *, source: str = RAW_SOURCE, limit: int = 10) -> list[RunManifest]:
    """Return up to ``limit`` most recent manifests, newest first."""
    manifests = list_run_manifests(raw_dir, source=source)
    return list(reversed(manifests))[:limit]
