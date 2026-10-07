"""Payload baseline files and the baseline CLI (#81).

A baseline lists every field path an endpoint's payload carries, with the JSON
type(s) observed there. It is built from live fetches, unioned across samples
and with the committed baseline (#80 D3), and rendered deterministically so a
regeneration diff shows only real change.
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

import pytest

from fpl_ingest.cli import build_parser, run_baseline
from fpl_ingest.extract.http.client import FPLClientError, RawResponse
from fpl_ingest.schema.payload_baseline import (
    BASELINE_DIR,
    ENDPOINTS,
    build_baseline,
    infer_paths,
    render_baseline,
)

AT = datetime(2026, 10, 6, 12, 0, tzinfo=timezone.utc)
REPO_BASELINE_DIR = Path(__file__).resolve().parents[3] / "schemas" / "payload-baseline"


def _raw(payload, status: int = 200, body: bytes | None = None) -> RawResponse:
    return RawResponse(
        url="https://fantasy.premierleague.com/api/x/",
        status=status,
        headers={},
        body=json.dumps(payload).encode() if body is None else body,
        requested_at=AT,
        received_at=AT,
    )


class FakeClient:
    """Stands in for AsyncFPLClient's raw fetches."""

    def __init__(self, payloads: dict, *, fail: bool = False, status: int = 200, body: bytes | None = None):
        self._payloads = payloads
        self._fail = fail
        self._status = status
        self._body = body

    async def __aenter__(self):
        return self

    async def __aexit__(self, *_):
        return None

    async def close(self):
        return None

    def _get(self, key):
        if self._fail or key not in self._payloads:
            raise FPLClientError(f"no response for {key}")
        return _raw(self._payloads[key], status=self._status, body=self._body)

    async def get_bootstrap_raw(self):
        return self._get("bootstrap-static")

    async def get_fixtures_raw(self):
        return self._get("fixtures")

    async def get_event_status_raw(self):
        return self._get("event-status")

    async def get_gameweek_live_raw(self, gameweek: int):
        return self._get(f"event-live/{gameweek}")

    async def get_element_summary_raw(self, player_id: int):
        return self._get(f"element-summary/{player_id}")


def _run(tmp_path: Path, argv: list[str], client: FakeClient) -> int:
    args = build_parser().parse_args(["baseline", *argv, "--out-dir", str(tmp_path)])
    return run_baseline(args, client=client)


def _paths(tmp_path: Path, endpoint: str) -> dict:
    return json.loads((tmp_path / f"{endpoint}.json").read_text())["paths"]


# ---------------------------------------------------------------------------
# AC1 — every field path, nested objects and arrays of records, with type(s)
# ---------------------------------------------------------------------------


@pytest.mark.covers("#81 AC1")
def test_nested_objects_and_arrays_of_records_get_their_own_paths():
    payload = {
        "elements": [{"id": 1, "stats": {"bps": 3}, "explain": [{"fixture": 9}]}],
        "total_players": 10,
    }
    assert infer_paths(payload) == {
        "$": {"object"},
        "$.elements": {"array"},
        "$.elements[]": {"object"},
        "$.elements[].id": {"number"},
        "$.elements[].stats": {"object"},
        "$.elements[].stats.bps": {"number"},
        "$.elements[].explain": {"array"},
        "$.elements[].explain[]": {"object"},
        "$.elements[].explain[].fixture": {"number"},
        "$.total_players": {"number"},
    }


@pytest.mark.covers("#81 AC1")
def test_a_top_level_list_of_records_is_rooted_at_the_array():
    assert infer_paths([{"id": 1, "event": None}]) == {
        "$": {"array"},
        "$[]": {"object"},
        "$[].id": {"number"},
        "$[].event": {"null"},
    }


@pytest.mark.covers("#81 AC1")
@pytest.mark.parametrize(
    ("value", "json_type"),
    [
        ("4.5", "string"),
        (7, "number"),
        (7.5, "number"),
        (True, "boolean"),
        (False, "boolean"),
        (None, "null"),
        ({}, "object"),
        ([], "array"),
    ],
)
def test_each_json_value_is_recorded_under_its_json_type(value, json_type):
    assert infer_paths({"f": value})["$.f"] == {json_type}


@pytest.mark.covers("#81 AC1")
def test_an_array_of_scalars_records_the_item_type():
    assert infer_paths({"f": [1, 2]})["$.f[]"] == {"number"}


@pytest.mark.covers("#81 AC1")
def test_an_empty_array_records_the_array_but_no_item_paths():
    paths = infer_paths({"history": []})
    assert paths == {"$": {"object"}, "$.history": {"array"}}


@pytest.mark.covers("#81 AC1")
def test_an_array_mixing_records_and_scalars_records_both_item_types():
    paths = infer_paths({"f": [{"a": 1}, "x"]})
    assert paths["$.f[]"] == {"object", "string"}
    assert paths["$.f[].a"] == {"number"}


@pytest.mark.covers("#81 AC1")
@pytest.mark.parametrize(("payload", "root"), [({}, "object"), ([], "array")])
def test_an_empty_payload_yields_only_the_root(payload, root):
    assert infer_paths(payload) == {"$": {root}}


@pytest.mark.covers("#81 AC1")
@pytest.mark.parametrize(
    ("argv", "key", "endpoint"),
    [
        (["bootstrap-static"], "bootstrap-static", "bootstrap-static"),
        (["fixtures"], "fixtures", "fixtures"),
        (["event-status"], "event-status", "event-status"),
        (["event-live", "--gameweeks", "5"], "event-live/5", "event-live"),
        (["element-summary", "--players", "7"], "element-summary/7", "element-summary"),
    ],
)
def test_the_cli_writes_a_baseline_of_the_fetched_payload(tmp_path, argv, key, endpoint):
    payload = {"rows": [{"id": 1, "nested": {"x": "a"}}]}

    rc = _run(tmp_path, argv, FakeClient({key: payload}))

    assert rc == 0
    written = json.loads((tmp_path / f"{endpoint}.json").read_text())
    assert written["endpoint"] == endpoint
    assert written["paths"] == {
        "$": ["object"],
        "$.rows": ["array"],
        "$.rows[]": ["object"],
        "$.rows[].id": ["number"],
        "$.rows[].nested": ["object"],
        "$.rows[].nested.x": ["string"],
    }


@pytest.mark.covers("#81 AC1")
@pytest.mark.parametrize("client", [FakeClient({}, fail=True), FakeClient({"fixtures": {}}, status=503)])
def test_a_failed_fetch_exits_non_zero_and_leaves_the_baseline_untouched(tmp_path, client):
    existing = tmp_path / "fixtures.json"
    existing.write_text("previous\n")

    rc = _run(tmp_path, ["fixtures"], client)

    assert rc != 0
    assert existing.read_text() == "previous\n"


@pytest.mark.covers("#81 AC1")
def test_one_failed_sample_among_several_writes_nothing(tmp_path):
    client = FakeClient({"element-summary/1": {"history": []}})  # player 2 has no response

    rc = _run(tmp_path, ["element-summary", "--players", "1,2"], client)

    assert rc != 0
    assert not (tmp_path / "element-summary.json").exists()


@pytest.mark.covers("#81 AC1")
def test_a_non_json_body_exits_non_zero_and_writes_nothing(tmp_path):
    client = FakeClient({"fixtures": None}, body=b"<html>maintenance</html>")

    rc = _run(tmp_path, ["fixtures"], client)

    assert rc != 0
    assert list(tmp_path.iterdir()) == []


@pytest.mark.covers("#81 AC1")
@pytest.mark.parametrize(
    ("argv", "client", "reason"),
    [
        (["fixtures"], FakeClient({}, fail=True), "fixtures: no response for fixtures"),
        (["fixtures"], FakeClient({"fixtures": {}}, status=503), "fixtures: HTTP 503"),
        (["fixtures"], FakeClient({"fixtures": None}, body=b"<html>"), "fixtures: body is not valid JSON"),
        (
            ["element-summary", "--players", "1,2"],
            FakeClient({"element-summary/1": {"history": []}}),
            "element-summary/2: no response for element-summary/2",
        ),
    ],
)
def test_every_fetch_failure_logs_the_failing_sample_and_its_reason(tmp_path, caplog, argv, client, reason):
    with caplog.at_level("ERROR", logger="fpl_ingest"):
        assert _run(tmp_path, argv, client) != 0

    assert any(reason in r.getMessage() for r in caplog.records if r.levelname == "ERROR"), caplog.text


# ---------------------------------------------------------------------------
# AC2 — byte-identical output for the same input
# ---------------------------------------------------------------------------


@pytest.mark.covers("#81 AC2")
def test_rendering_ignores_key_and_record_order():
    a = build_baseline("fixtures", [[{"b": 1, "a": "x"}, {"c": None}]])
    b = build_baseline("fixtures", [[{"c": None}, {"a": "x", "b": 1}]])
    assert render_baseline(a) == render_baseline(b)


@pytest.mark.covers("#81 AC2")
def test_rendered_baseline_is_sorted_with_sorted_types_and_a_trailing_newline():
    text = render_baseline(build_baseline("fixtures", [[{"z": 1, "a": None}, {"a": "s"}]]))
    parsed = json.loads(text)
    assert list(parsed["paths"]) == sorted(parsed["paths"])
    assert parsed["paths"]["$[].a"] == ["null", "string"]
    assert text.endswith("}\n")


@pytest.mark.covers("#81 AC2")
def test_running_the_cli_twice_on_the_same_input_is_byte_identical(tmp_path):
    client = FakeClient({"bootstrap-static": {"events": [{"id": 1, "finished": True}], "teams": []}})

    assert _run(tmp_path, ["bootstrap-static"], client) == 0
    first = (tmp_path / "bootstrap-static.json").read_bytes()
    assert _run(tmp_path, ["bootstrap-static"], client) == 0

    assert (tmp_path / "bootstrap-static.json").read_bytes() == first


# ---------------------------------------------------------------------------
# AC3 — fields seen in only some records/fetches are included (union, D3)
# ---------------------------------------------------------------------------


@pytest.mark.covers("#81 AC3")
def test_a_field_in_only_some_records_is_included():
    paths = build_baseline("fixtures", [[{"id": 1}, {"id": 2, "provisional_start_time": False}]])["paths"]
    assert paths["$[].provisional_start_time"] == {"boolean"}


@pytest.mark.covers("#81 AC3")
def test_types_seen_across_records_are_unioned():
    paths = build_baseline("fixtures", [[{"event": 3}, {"event": None}]])["paths"]
    assert paths["$[].event"] == {"null", "number"}


@pytest.mark.covers("#81 AC3")
def test_a_field_in_only_one_of_several_fetches_is_included(tmp_path):
    client = FakeClient(
        {
            "element-summary/1": {"history": []},
            "element-summary/2": {"history": [{"element": 2, "minutes": 90}]},
        }
    )

    assert _run(tmp_path, ["element-summary", "--players", "1,2"], client) == 0

    paths = _paths(tmp_path, "element-summary")
    assert paths["$.history[].minutes"] == ["number"]


@pytest.mark.covers("#81 AC3")
def test_the_committed_baseline_is_unioned_by_default(tmp_path):
    client = FakeClient({"event-live/5": {"elements": [{"id": 1}]}})
    assert _run(tmp_path, ["event-live", "--gameweeks", "5"], client) == 0
    client = FakeClient({"event-live/6": {"elements": [{"id": 1, "modified": True}]}})
    assert _run(tmp_path, ["event-live", "--gameweeks", "6"], client) == 0

    client = FakeClient({"event-live/7": {"elements": [{"id": 1}]}})
    assert _run(tmp_path, ["event-live", "--gameweeks", "7"], client) == 0

    assert _paths(tmp_path, "event-live")["$.elements[].modified"] == ["boolean"]


@pytest.mark.covers("#81 AC3")
def test_the_replace_flag_counts_only_the_fresh_fetches(tmp_path):
    client = FakeClient({"event-status": {"status": [{"event": 1, "points": "r"}], "leagues": "Updated"}})
    assert _run(tmp_path, ["event-status"], client) == 0

    client = FakeClient({"event-status": {"status": [{"event": 1}]}})
    assert _run(tmp_path, ["event-status", "--replace"], client) == 0

    paths = _paths(tmp_path, "event-status")
    assert "$.leagues" not in paths
    assert "$.status[].points" not in paths
    assert paths["$.status[].event"] == ["number"]


@pytest.mark.covers("#81 AC3")
def test_unioning_with_an_existing_baseline_in_memory():
    existing = build_baseline("fixtures", [[{"id": 1, "old": "x"}]])
    merged = build_baseline("fixtures", [[{"id": "1"}]], existing=existing)
    assert merged["paths"]["$[].old"] == {"string"}
    assert merged["paths"]["$[].id"] == {"number", "string"}

    replaced = build_baseline("fixtures", [[{"id": "1"}]], existing=existing, replace=True)
    assert "$[].old" not in replaced["paths"]


# ---------------------------------------------------------------------------
# AC4 — every captured endpoint has a committed baseline
# ---------------------------------------------------------------------------


@pytest.mark.covers("#81 AC4")
def test_the_five_captured_endpoints_are_the_baseline_endpoints():
    assert set(ENDPOINTS) == {
        "bootstrap-static",
        "fixtures",
        "event-status",
        "event-live",
        "element-summary",
    }
    assert BASELINE_DIR == REPO_BASELINE_DIR


@pytest.mark.covers("#81 AC4")
@pytest.mark.parametrize(
    "endpoint",
    ["bootstrap-static", "fixtures", "event-status", "event-live", "element-summary"],
)
def test_each_endpoint_has_a_committed_canonical_baseline(endpoint):
    path = REPO_BASELINE_DIR / f"{endpoint}.json"
    text = path.read_text()
    baseline = json.loads(text)

    assert baseline["endpoint"] == endpoint
    assert len(baseline["paths"]) > 1, "a baseline with only the root was not built from a real payload"
    as_sets = {**baseline, "paths": {k: set(v) for k, v in baseline["paths"].items()}}
    assert render_baseline(as_sets) == text, "committed file is not in the CLI's canonical form"


# ---------------------------------------------------------------------------
# AC5 — sample flags required where the endpoint is per-sample
# ---------------------------------------------------------------------------


@pytest.mark.covers("#81 AC5")
@pytest.mark.parametrize("endpoint", ["element-summary", "event-live"])
def test_a_per_sample_endpoint_without_samples_exits_non_zero_and_writes_nothing(tmp_path, endpoint):
    rc = _run(tmp_path, [endpoint], FakeClient({}))

    assert rc != 0
    assert list(tmp_path.iterdir()) == []


@pytest.mark.covers("#81 AC5")
@pytest.mark.parametrize(("endpoint", "flag"), [("element-summary", "--players"), ("event-live", "--gameweeks")])
def test_a_missing_sample_flag_logs_which_flag_is_needed(tmp_path, caplog, endpoint, flag):
    with caplog.at_level("ERROR", logger="fpl_ingest"):
        assert _run(tmp_path, [endpoint], FakeClient({})) != 0

    assert any(f"{endpoint} needs {flag}" in r.getMessage() for r in caplog.records), caplog.text


# ---------------------------------------------------------------------------
# AC6 — the file lists the samples it was built from
# ---------------------------------------------------------------------------


def _samples(tmp_path: Path, endpoint: str) -> list:
    return json.loads((tmp_path / f"{endpoint}.json").read_text())["samples"]


@pytest.mark.covers("#81 AC6")
def test_the_baseline_lists_its_sample_ids_sorted(tmp_path):
    client = FakeClient({f"element-summary/{p}": {"history": []} for p in (12, 3)})

    assert _run(tmp_path, ["element-summary", "--players", "12,3"], client) == 0

    assert _samples(tmp_path, "element-summary") == ["element-summary/12", "element-summary/3"]


@pytest.mark.covers("#81 AC6")
def test_a_single_fetch_endpoint_lists_itself_as_the_sample(tmp_path):
    assert _run(tmp_path, ["fixtures"], FakeClient({"fixtures": []})) == 0
    assert _samples(tmp_path, "fixtures") == ["fixtures"]


@pytest.mark.covers("#81 AC6")
def test_samples_are_unioned_with_the_committed_baseline_unless_replaced(tmp_path):
    assert _run(tmp_path, ["event-live", "--gameweeks", "5"], FakeClient({"event-live/5": {"elements": []}})) == 0
    assert _run(tmp_path, ["event-live", "--gameweeks", "6"], FakeClient({"event-live/6": {"elements": []}})) == 0
    assert _samples(tmp_path, "event-live") == ["event-live/5", "event-live/6"]

    client = FakeClient({"event-live/7": {"elements": []}})
    assert _run(tmp_path, ["event-live", "--gameweeks", "7", "--replace"], client) == 0
    assert _samples(tmp_path, "event-live") == ["event-live/7"]


@pytest.mark.covers("#81 AC6")
def test_the_baseline_carries_no_timestamp():
    baseline = build_baseline("fixtures", [[{"id": 1}]], samples=["fixtures"])
    assert set(json.loads(render_baseline(baseline))) == {"endpoint", "samples", "paths"}


@pytest.mark.covers("#81 AC6")
def test_rendering_sorts_samples_whatever_order_they_arrive_in():
    baseline = {"endpoint": "event-live", "samples": ["event-live/9", "event-live/1", "event-live/5"], "paths": {}}
    assert json.loads(render_baseline(baseline))["samples"] == ["event-live/1", "event-live/5", "event-live/9"]
