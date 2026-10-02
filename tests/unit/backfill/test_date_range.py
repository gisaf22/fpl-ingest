"""The date range selects whole runs by their run_id start date (#67 AC4; E2).

Selecting single payloads by date could catalog part of a run, and write-once
would then freeze it incomplete. So a run is in or out as a whole, by the date
its ``run_id`` starts on, even when some of its payloads sit under another date.
"""

from __future__ import annotations

import datetime as dt
import hashlib
import json

import pytest

from fpl_ingest.backfill import LocalTree, build_backfill
from tests.support.backfill_tree import (
    HISTORY_RUN,
    RUN_1_0_0,
    RUN_2_0_0,
    RUN_BAD_BOOTSTRAP,
    RUN_NO_BOOTSTRAP,
    VALIDATOR_VERSION,
    build_tree,
    catalog_key,
    read_catalog,
)
from tests.support.cli_fakes import PLAYER_HISTORY_1


def _built(root, from_date=None, to_date=None) -> set[str]:
    report = build_backfill(
        LocalTree(root), validator_version=VALIDATOR_VERSION, from_date=from_date, to_date=to_date,
    )
    return set(report.written)


def _add_payload_after_midnight(tree) -> str:
    """RUN_1_0_0 started 2026-08-29; this payload of it is stored under 2026-08-30."""
    body = json.dumps(PLAYER_HISTORY_1).encode()
    prefix = f"raw/fpl/element-summary/5/2026-08-30/{RUN_1_0_0}"
    sidecar = {
        "raw_contract_version": "1.0.0", "source": "fpl", "endpoint": "element-summary/5",
        "run_id": RUN_1_0_0, "extraction_date": "2026-08-30", "received_at": "2026-08-30T00:00:05Z",
        "http_status": 200, "content_length": len(body),
        "content_sha256": hashlib.sha256(body).hexdigest(),
    }
    tree._put(f"{prefix}/payload.json", body)
    tree._put(f"{prefix}/metadata.json", json.dumps(sidecar).encode())
    return f"{prefix}/payload.json"


@pytest.mark.covers("#67 AC4")
@pytest.mark.parametrize(
    ("from_date", "to_date", "expected"),
    [
        pytest.param(None, None, {RUN_1_0_0, RUN_2_0_0, RUN_NO_BOOTSTRAP, RUN_BAD_BOOTSTRAP, HISTORY_RUN},
                     id="no-range-builds-everything"),
        pytest.param(dt.date(2026, 9, 2), dt.date(2026, 9, 5), {RUN_NO_BOOTSTRAP, RUN_BAD_BOOTSTRAP},
                     id="both-bounds-inclusive"),
        pytest.param(dt.date(2026, 9, 6), None, {RUN_2_0_0}, id="from-only"),
        pytest.param(None, dt.date(2026, 8, 29), {RUN_1_0_0, HISTORY_RUN}, id="to-only-includes-history"),
        pytest.param(dt.date(2026, 5, 26), dt.date(2026, 5, 26), {HISTORY_RUN}, id="history-by-its-run-date"),
        pytest.param(dt.date(2026, 8, 1), dt.date(2026, 8, 28), set(), id="empty-range"),
    ],
)
def test_only_runs_starting_in_range_are_built(tmp_path, from_date, to_date, expected):
    build_tree(tmp_path)

    assert _built(tmp_path, from_date, to_date) == {catalog_key(r) for r in expected}


@pytest.mark.covers("#67 AC4")
def test_a_run_is_never_split_by_its_payload_dates(tmp_path):
    tree = build_tree(tmp_path)
    late_key = _add_payload_after_midnight(tree)

    assert _built(tmp_path, dt.date(2026, 8, 30), dt.date(2026, 8, 30)) == set()
    assert _built(tmp_path, dt.date(2026, 8, 29), dt.date(2026, 8, 29)) == {catalog_key(RUN_1_0_0)}
    keys = {e["key"] for e in read_catalog(tmp_path, RUN_1_0_0)["captures"]}
    assert late_key in keys
    assert set(tree.payload_keys[RUN_1_0_0]) <= keys
