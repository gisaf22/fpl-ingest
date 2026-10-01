"""Load the raw-contract JSON Schemas that replace strategy doc §A.5's tables (#62 D7)."""

from __future__ import annotations

import json
from pathlib import Path

from jsonschema import Draft202012Validator

SCHEMA_DIR = Path(__file__).resolve().parents[2] / "schemas" / "raw-contract" / "2.1.0"


def validator(name: str) -> Draft202012Validator:
    """Return a validator for ``sidecar``, ``manifest`` or ``backfill-catalog``."""
    schema = json.loads((SCHEMA_DIR / f"{name}.schema.json").read_text(encoding="utf-8"))
    Draft202012Validator.check_schema(schema)
    return Draft202012Validator(schema)


def errors(name: str, document: dict) -> list[str]:
    """Return every schema violation in ``document``, empty when it is valid."""
    return [e.message for e in validator(name).iter_errors(document)]
