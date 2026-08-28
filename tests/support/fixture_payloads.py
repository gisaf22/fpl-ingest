"""Loader for the real-capture fixture library under ``tests/fixtures/payloads``.

Each subdirectory holds one endpoint/scenario's ``payload.json`` and a
``PROVENANCE.md`` documenting where it came from and what, if anything, was
hand-edited. See the individual ``PROVENANCE.md`` files for details.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

_PAYLOADS_DIR = Path(__file__).resolve().parent.parent / "fixtures" / "payloads"


def payload_bytes(name: str) -> bytes:
    """Return the verbatim bytes of one fixture's ``payload.json``."""
    return (_PAYLOADS_DIR / name / "payload.json").read_bytes()


def payload_json(name: str) -> Any:
    """Return one fixture's ``payload.json``, parsed."""
    return json.loads(payload_bytes(name))
