"""Strategy doc §A.5 points at the JSON Schemas instead of listing fields (#62 AC4).

The field tables drifted from the code; the schemas under
``schemas/raw-contract/`` replace them. The "why" notes stay.
"""

from __future__ import annotations

from pathlib import Path

import pytest

_DOC = Path(__file__).resolve().parents[2] / "docs" / "architecture" / "fpl-ingest-strategy.md"


def _section_a5() -> str:
    text = _DOC.read_text(encoding="utf-8")
    start = text.index("## A.5 ")
    end = text.index("## A.6 ", start)
    return text[start:end]


@pytest.mark.covers("#62 AC4")
def test_a5_has_no_field_tables():
    table_headers = [
        line for line in _section_a5().splitlines() if line.lstrip().startswith("| Field")
    ]
    assert table_headers == []


@pytest.mark.covers("#62 AC4")
def test_a5_points_at_the_schemas():
    assert "schemas/raw-contract/2.1.0/" in _section_a5()


@pytest.mark.covers("#62 AC4")
@pytest.mark.parametrize(
    "why_note",
    [
        "user metadata is capped at 2 KB",
        "the only way to know how stale",
        "without opening any payload",
        "never accidentally reads manifests as payloads",
    ],
)
def test_a5_keeps_the_why_notes(why_note):
    assert why_note in _section_a5()
