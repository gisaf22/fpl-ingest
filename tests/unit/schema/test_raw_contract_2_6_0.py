"""Raw contract 2.6.0: per-class invalid counts and the abort reason (#109 AC6)."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from fpl_ingest.extract.http.raw_keys import RAW_CONTRACT_VERSION

SCHEMA = Path(__file__).resolve().parents[3] / "schemas" / "raw-contract" / "2.6.0" / "manifest.schema.json"


def _properties() -> dict:
    return json.loads(SCHEMA.read_text())["properties"]


@pytest.mark.covers("#109 AC6")
def test_contract_version_is_one_minor_above_2_5_0():
    assert RAW_CONTRACT_VERSION == "2.6.0"


@pytest.mark.covers("#109 AC6")
def test_manifest_schema_documents_invalid_counts_per_class():
    counts = _properties()["invalid_counts"]

    assert sorted(counts["properties"]) == ["http_error", "not_found", "shape_invalid"]
    assert sorted(counts["required"]) == ["http_error", "not_found", "shape_invalid"]


@pytest.mark.covers("#109 AC6")
def test_manifest_schema_documents_the_abort_reason():
    assert "aborted_http_error_streak" in json.dumps(_properties()["abort_reason"])
