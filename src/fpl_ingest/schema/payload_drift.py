"""Payload drift: a capture's shape compared with its committed baseline (#82).

Ingest observes and records drift; it never gates on it (#80 D1). Every record
is compared, null is compatible with any type, and an empty array or object
carries no evidence about its contents (#80 D2, D3). A change is reported once
at its highest path: a new object as one ``added``, an absent one as one
``removed``. A rename is one removal plus one addition.

The check fails open (#80 D7): a missing or corrupt baseline, a payload that is
not JSON, or any internal error yields status ``unavailable`` with a reason,
never an exception.
"""

from __future__ import annotations

import json
from collections import Counter
from collections.abc import Mapping, MutableMapping
from pathlib import Path
from typing import Any

from fpl_ingest.schema.payload_baseline import BASELINE_DIR, ENDPOINTS, json_type


def baseline_family(endpoint_key: str) -> str | None:
    """Map a capture endpoint (``element-summary/115``) to its baseline name, or None."""
    family = endpoint_key.split("/", 1)[0]
    return family if family in ENDPOINTS else None


def check_drift(
    endpoint_key: str,
    payload_bytes: bytes,
    *,
    baseline_dir: Path | None = None,
    cache: MutableMapping[str, Any] | None = None,
) -> dict[str, Any] | None:
    """Return the sidecar ``drift`` block for one payload, or None when not checked.

    ``cache`` holds each family's loaded baseline (or the reason it could not
    be loaded) so a run reads each baseline file once.
    """
    family = baseline_family(endpoint_key)
    if family is None:
        return None
    try:
        baseline = _load(family, baseline_dir or BASELINE_DIR, cache if cache is not None else {})
        if isinstance(baseline, str):
            return _block("unavailable", reason=baseline)
        try:
            payload = json.loads(payload_bytes)
        except ValueError:
            return _block("unavailable", reason="payload is not JSON")
        entries = diff_payload(family, baseline, payload)
        return _block("drift" if entries else "ok", entries=entries)
    except Exception as exc:  # noqa: BLE001 - the check must never cost a capture (#80 D7)
        return _block("unavailable", reason=f"drift check failed: {exc!r}")


def _load(family: str, baseline_dir: Path, cache: MutableMapping[str, Any]) -> Mapping[str, Any] | str:
    if family not in cache:
        path = baseline_dir / f"{family}.json"
        try:
            cache[family] = json.loads(path.read_text(encoding="utf-8"))["paths"]
        except FileNotFoundError:
            cache[family] = f"baseline {path.name} not found in {baseline_dir}"
        except (OSError, ValueError, KeyError, TypeError) as exc:
            cache[family] = f"baseline {path.name} is unreadable: {exc!r}"
    return cache[family]


def _block(status: str, *, reason: str | None = None, entries: list | None = None) -> dict[str, Any]:
    return {"status": status, "reason": reason, "entries": entries or []}


def diff_payload(endpoint: str, baseline: Mapping[str, Any], payload: Any) -> list[dict[str, Any]]:
    """Compare ``payload`` with a baseline's ``paths`` and return the drift entries."""
    expected = {path: set(types) for path, types in baseline.items()}
    observed: dict[str, set[str]] = {}
    counts: Counter[str] = Counter()
    filled: set[str] = set()  # paths seen at least once as a non-empty object
    _walk(payload, "$", observed, counts, filled)

    changed = {
        path for path in expected.keys() & observed.keys()
        if (expected[path] - {"null"}) and observed[path] - {"null"} - expected[path]
    }
    entries = []
    for path in sorted(observed.keys() - expected.keys()):
        parent = _parent(path)
        # Children of a path whose type changed are part of that one change.
        if parent in expected and parent not in changed:
            entries.append(_entry(endpoint, path, "added", set(), observed[path], counts[path]))
    for path in sorted(expected.keys() - observed.keys()):
        parent = _parent(path)
        # A key child needs its parent seen as a non-empty object; an item
        # path ("[]") is absent only when every array was empty: no evidence.
        if not path.endswith("[]") and parent in filled:
            entries.append(_entry(endpoint, path, "removed", expected[path], set(), 0))
    for path in sorted(changed):
        entries.append(_entry(endpoint, path, "type_changed", expected[path], observed[path], counts[path]))
    return sorted(entries, key=lambda e: (e["path"], e["kind"]))


def _walk(value: Any, path: str, observed: dict, counts: Counter, filled: set) -> None:
    observed.setdefault(path, set()).add(json_type(value))
    counts[path] += 1
    if isinstance(value, dict):
        if value:
            filled.add(path)
        for key, child in value.items():
            _walk(child, f"{path}.{key}", observed, counts, filled)
    elif isinstance(value, list):
        for item in value:
            _walk(item, f"{path}[]", observed, counts, filled)


def _parent(path: str) -> str:
    if path.endswith("[]"):
        return path[:-2]
    return path[: path.rfind(".")]


def _entry(endpoint: str, path: str, kind: str, baseline: set, observed: set, count: int) -> dict[str, Any]:
    return {
        "endpoint": endpoint,
        "path": path,
        "kind": kind,
        "baseline_types": sorted(baseline),
        "observed_types": sorted(observed),
        "count": count,
    }
