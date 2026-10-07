"""Payload drift: comparing a capture's shape with its baseline (#82).

Every record is compared, null is compatible with any type, and empty
containers carry no evidence (#80 D2, D3). Kinds are ``added``, ``removed``
and ``type_changed``; a rename is one removal plus one addition.
"""

from __future__ import annotations

import json

import pytest

from fpl_ingest.schema.payload_baseline import build_baseline, render_baseline
from fpl_ingest.schema.payload_drift import check_drift, diff_payload

BASE = {
    "$": ["object"],
    "$.rows": ["array"],
    "$.rows[]": ["object"],
    "$.rows[].id": ["number"],
    "$.rows[].metric": ["number"],
    "$.rows[].nested": ["object"],
    "$.rows[].nested.x": ["number"],
}


def _rows(*rows):
    return {"rows": list(rows)}


def _row(**overrides):
    row = {"id": 1, "metric": 2, "nested": {"x": 3}}
    row.update(overrides)
    return row


def _kinds(entries):
    return {(e["path"], e["kind"]) for e in entries}


# ---------------------------------------------------------------------------
# AC1 — a matching payload records nothing
# ---------------------------------------------------------------------------


@pytest.mark.covers("#82 AC1")
def test_a_payload_matching_the_baseline_has_no_drift():
    assert diff_payload("fixtures", BASE, _rows(_row(), _row(id=2))) == []


# ---------------------------------------------------------------------------
# AC2 — each drift kind, with endpoint, path, kind, baseline vs observed type
# ---------------------------------------------------------------------------


@pytest.mark.covers("#82 AC2")
def test_a_new_field_is_added_with_its_type_and_record_count():
    entries = diff_payload("fixtures", BASE, _rows(_row(fresh="a"), _row(), _row(fresh="b")))
    assert entries == [
        {
            "endpoint": "fixtures",
            "path": "$.rows[].fresh",
            "kind": "added",
            "baseline_types": [],
            "observed_types": ["string"],
            "count": 2,
        }
    ]


@pytest.mark.covers("#82 AC2")
def test_a_field_missing_from_every_record_is_removed():
    payload = _rows({"id": 1, "nested": {"x": 3}})
    assert diff_payload("fixtures", BASE, payload) == [
        {
            "endpoint": "fixtures",
            "path": "$.rows[].metric",
            "kind": "removed",
            "baseline_types": ["number"],
            "observed_types": [],
            "count": 0,
        }
    ]


@pytest.mark.covers("#82 AC2")
def test_a_rename_is_one_removal_plus_one_addition():
    payload = _rows({"id": 1, "score": 2, "nested": {"x": 3}})
    assert _kinds(diff_payload("fixtures", BASE, payload)) == {
        ("$.rows[].metric", "removed"),
        ("$.rows[].score", "added"),
    }


@pytest.mark.covers("#82 AC2")
def test_a_type_change_records_baseline_and_observed_types():
    entries = diff_payload("fixtures", BASE, _rows(_row(metric="2")))
    assert entries == [
        {
            "endpoint": "fixtures",
            "path": "$.rows[].metric",
            "kind": "type_changed",
            "baseline_types": ["number"],
            "observed_types": ["string"],
            "count": 1,
        }
    ]


@pytest.mark.covers("#82 AC2")
def test_a_nested_change_is_reported_at_the_nested_path():
    entries = diff_payload("fixtures", BASE, _rows(_row(nested={"y": 3})))
    assert _kinds(entries) == {("$.rows[].nested.x", "removed"), ("$.rows[].nested.y", "added")}


@pytest.mark.covers("#82 AC2")
def test_mixed_types_in_one_payload_give_one_entry_with_both_types():
    entries = diff_payload("fixtures", BASE, _rows(_row(), _row(metric="2")))
    assert len(entries) == 1
    assert entries[0]["observed_types"] == ["number", "string"]


@pytest.mark.covers("#82 AC2")
def test_a_container_changing_kind_is_a_type_change():
    entries = diff_payload("fixtures", BASE, {"rows": {"id": 1}})
    assert ("$.rows", "type_changed") in _kinds(entries)


@pytest.mark.covers("#82 AC2")
def test_an_absent_object_is_reported_once_at_its_highest_absent_path():
    entries = diff_payload("fixtures", BASE, _rows({"id": 1, "metric": 2}))
    assert _kinds(entries) == {("$.rows[].nested", "removed")}


@pytest.mark.covers("#82 AC2")
def test_a_new_object_is_reported_once_at_its_highest_new_path():
    entries = diff_payload("fixtures", BASE, _rows(_row(extra={"a": 1, "b": {"c": 2}})))
    assert _kinds(entries) == {("$.rows[].extra", "added")}


@pytest.mark.covers("#82 AC2")
def test_entries_are_sorted_by_path_then_kind():
    payload = _rows({"id": "1", "zeta": 1, "alpha": 1, "nested": {"x": 3}})
    paths = [(e["path"], e["kind"]) for e in diff_payload("fixtures", BASE, payload)]
    assert paths == sorted(paths)


# ---------------------------------------------------------------------------
# AC3 — every record is compared, not only the first
# ---------------------------------------------------------------------------


@pytest.mark.covers("#82 AC3")
def test_drift_only_in_the_last_record_is_detected():
    entries = diff_payload("fixtures", BASE, _rows(_row(), _row(), _row(metric=[1])))
    assert _kinds(entries) == {("$.rows[].metric", "type_changed")}


@pytest.mark.covers("#82 AC3")
def test_a_field_present_in_any_record_is_not_removed():
    entries = diff_payload("fixtures", BASE, _rows({"id": 1, "nested": {"x": 1}}, _row()))
    assert entries == []


# ---------------------------------------------------------------------------
# AC4 — null is compatible with any type
# ---------------------------------------------------------------------------


@pytest.mark.covers("#82 AC4")
@pytest.mark.parametrize(
    "rows",
    [
        [_row(metric=None)],
        [_row(metric=None), _row()],
        [_row(nested=None)],
    ],
    ids=["all_null", "some_null", "null_object_hides_no_children"],
)
def test_null_where_the_baseline_has_a_type_is_not_drift(rows):
    assert diff_payload("fixtures", BASE, _rows(*rows)) == []


@pytest.mark.covers("#82 AC4")
def test_a_value_where_the_baseline_only_ever_saw_null_is_not_drift():
    base = {**BASE, "$.rows[].maybe": ["null"]}
    assert diff_payload("fixtures", base, _rows(_row(maybe=4))) == []


# ---------------------------------------------------------------------------
# AC5 — empty payloads and lists: no false drift, no crash
# ---------------------------------------------------------------------------


@pytest.mark.covers("#82 AC5")
@pytest.mark.parametrize(
    "payload",
    [{"rows": []}, {}, [], _rows(_row(nested={}))],
    ids=["empty_list", "empty_object", "empty_top_level_list", "empty_nested_object"],
)
def test_empty_containers_carry_no_evidence(payload):
    assert diff_payload("fixtures", BASE, payload) == []


@pytest.mark.covers("#82 AC5")
def test_an_empty_top_level_list_against_a_list_baseline_is_not_drift():
    base = {"$": ["array"], "$[]": ["object"], "$[].id": ["number"]}
    assert diff_payload("fixtures", base, []) == []


# ---------------------------------------------------------------------------
# AC8 — the check fails open: missing/corrupt baseline, internal error
# ---------------------------------------------------------------------------


def _write_baseline(directory, endpoint, payload):
    directory.mkdir(parents=True, exist_ok=True)
    (directory / f"{endpoint}.json").write_text(render_baseline(build_baseline(endpoint, [payload])))


@pytest.mark.covers("#82 AC8")
def test_a_missing_baseline_is_unavailable_with_a_reason(tmp_path):
    block = check_drift("fixtures", b"[]", baseline_dir=tmp_path)
    assert block["status"] == "unavailable"
    assert "fixtures.json" in block["reason"]
    assert block["entries"] == []


@pytest.mark.covers("#82 AC8")
def test_a_corrupt_baseline_is_unavailable_with_a_reason(tmp_path):
    (tmp_path / "fixtures.json").write_text("{not json")
    block = check_drift("fixtures", b"[]", baseline_dir=tmp_path)
    assert block["status"] == "unavailable"
    assert block["reason"]


@pytest.mark.covers("#82 AC8")
def test_an_internal_error_is_unavailable_and_never_raises(tmp_path, monkeypatch):
    _write_baseline(tmp_path, "fixtures", [])

    def boom(*_args, **_kwargs):
        raise RuntimeError("diff exploded")

    monkeypatch.setattr("fpl_ingest.schema.payload_drift.diff_payload", boom)
    block = check_drift("fixtures", b"[]", baseline_dir=tmp_path)
    assert block["status"] == "unavailable"
    assert "diff exploded" in block["reason"]


@pytest.mark.covers("#82 AC8")
def test_a_non_json_payload_is_unavailable(tmp_path):
    _write_baseline(tmp_path, "fixtures", [])
    block = check_drift("fixtures", b"<html>", baseline_dir=tmp_path)
    assert block["status"] == "unavailable"


# ---------------------------------------------------------------------------
# AC9 — the block's shape, and per-sample endpoints map to their baseline
# ---------------------------------------------------------------------------


@pytest.mark.covers("#82 AC9")
@pytest.mark.parametrize(
    ("endpoint_key", "baseline"),
    [("element-summary/115", "element-summary"), ("event-live/01", "event-live"), ("fixtures", "fixtures")],
)
def test_the_block_reports_status_and_entries_against_the_endpoint_baseline(tmp_path, endpoint_key, baseline):
    _write_baseline(tmp_path, baseline, {"rows": [{"id": 1}]})

    ok = check_drift(endpoint_key, json.dumps({"rows": [{"id": 1}]}).encode(), baseline_dir=tmp_path)
    drift = check_drift(endpoint_key, json.dumps({"rows": [{"id": 1, "new": 1}]}).encode(), baseline_dir=tmp_path)

    assert ok == {"status": "ok", "reason": None, "entries": []}
    assert drift["status"] == "drift"
    assert drift["reason"] is None
    assert drift["entries"][0]["endpoint"] == baseline
    assert set(drift["entries"][0]) == {"endpoint", "path", "kind", "baseline_types", "observed_types", "count"}



# ---------------------------------------------------------------------------
# AC11 — endpoints outside the five FPL families get no drift block
# ---------------------------------------------------------------------------


@pytest.mark.covers("#82 AC11")
@pytest.mark.parametrize("endpoint_key", ["understat/match", "reep/players", "entry/1"])
def test_an_endpoint_without_a_baseline_family_is_not_checked(tmp_path, endpoint_key):
    assert check_drift(endpoint_key, b"{}", baseline_dir=tmp_path) is None


@pytest.mark.covers("#82 AC11")
def test_a_non_fpl_source_writes_no_drift_block(tmp_path):
    from datetime import datetime, timezone

    from fpl_ingest.extract.http.local_writer import LocalRawWriter

    _write_baseline(tmp_path / "baselines", "fixtures", [])
    writer = LocalRawWriter(tmp_path / "raw", "understat", baseline_dir=tmp_path / "baselines")
    at = datetime(2026, 10, 6, tzinfo=timezone.utc)
    result = writer.write_object(
        "fixtures", b"[]", request_url="u", requested_at=at, received_at=at, http_status=200,
        shape_validation={"ok": True, "checks": [], "failures": []},
    )

    sidecar = json.loads((tmp_path / "raw" / result.metadata_key).read_text())
    assert "drift" not in sidecar
