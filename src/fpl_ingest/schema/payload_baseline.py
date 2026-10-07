"""Payload baselines: the expected field paths and types per endpoint (#81).

A baseline maps every field path an endpoint's payload carries, such as
``$.elements[].stats.bps``, to the JSON type(s) observed there. It is built
from live fetches, never from S3 captures (``RawStorageBackend`` is write-only),
and unioned across samples and with the committed baseline so state-dependent
fields do not drop out (#80 D3). Rendering is deterministic: a regeneration
diff shows only real change, and merging it is how a drift is accepted.

Integer and decimal are one type, ``number``: FPL sends some numeric fields as
whole numbers in some records and decimals in others (#81 decision).
"""

from __future__ import annotations

import json
from collections.abc import Iterable
from pathlib import Path
from typing import Any

#: Every captured endpoint, named as the baseline files are.
ENDPOINTS = ("bootstrap-static", "fixtures", "event-status", "event-live", "element-summary")

#: Where the committed baselines live.
BASELINE_DIR = Path(__file__).resolve().parents[3] / "schemas" / "payload-baseline"

Paths = dict[str, set[str]]


def json_type(value: Any) -> str:
    """Return the JSON type name of a decoded value."""
    if value is None:
        return "null"
    if isinstance(value, bool):  # before number: bool is an int subclass
        return "boolean"
    if isinstance(value, (int, float)):
        return "number"
    if isinstance(value, str):
        return "string"
    if isinstance(value, list):
        return "array"
    return "object"


def infer_paths(payload: Any) -> Paths:
    """Return every field path in ``payload`` with the types observed there.

    Arrays contribute an ``[]`` item path from all their items. An empty array
    gives no evidence about its items, so it adds no item paths.
    """
    paths: Paths = {}
    _walk(payload, "$", paths)
    return paths


def _walk(value: Any, path: str, paths: Paths) -> None:
    paths.setdefault(path, set()).add(json_type(value))
    if isinstance(value, dict):
        for key, child in value.items():
            _walk(child, f"{path}.{key}", paths)
    elif isinstance(value, list):
        for item in value:
            _walk(item, f"{path}[]", paths)


def merge_paths(into: Paths, other: Paths) -> Paths:
    """Union ``other`` into ``into`` and return it."""
    for path, types in other.items():
        into.setdefault(path, set()).update(types)
    return into


def build_baseline(
    endpoint: str,
    payloads: Iterable[Any],
    *,
    samples: Iterable[str] = (),
    existing: dict[str, Any] | None = None,
    replace: bool = False,
) -> dict[str, Any]:
    """Build a baseline from fetched payloads.

    Unless ``replace``, ``existing`` (a committed baseline) is unioned in, so a
    regeneration never silently drops a rarely seen field or sample.
    """
    paths: Paths = {}
    sample_ids = set(samples)
    if existing is not None and not replace:
        merge_paths(paths, {p: set(t) for p, t in existing["paths"].items()})
        sample_ids.update(existing.get("samples", ()))
    for payload in payloads:
        merge_paths(paths, infer_paths(payload))
    return {"endpoint": endpoint, "samples": sample_ids, "paths": paths}


def render_baseline(baseline: dict[str, Any]) -> str:
    """Render a baseline as canonical JSON: sorted keys and types, trailing newline."""
    document = {
        "endpoint": baseline["endpoint"],
        "samples": sorted(baseline.get("samples", ())),
        "paths": {path: sorted(types) for path, types in baseline["paths"].items()},
    }
    return json.dumps(document, indent=2, sort_keys=True) + "\n"


def load_baseline(path: Path) -> dict[str, Any] | None:
    """Read a baseline file, or ``None`` when it does not exist."""
    if not path.exists():
        return None
    return json.loads(path.read_text(encoding="utf-8"))
