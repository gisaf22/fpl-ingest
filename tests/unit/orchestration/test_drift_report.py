"""Drift recorded by a run is surfaced in the job summary, as annotations and as issues.

``report_drift`` reads the drift report the capture job leaves behind and runs
the real ``gh`` through ``PATH``. These tests put a stub ``gh`` first on
``PATH``: it records every call (arguments, and the body of any issue it is
asked to create), answers ``issue list`` from a JSON file, prints a new issue
URL for ``issue create``, and fails whichever subcommands the test names. No
request leaves the machine.
"""

from __future__ import annotations

import json
import os
import stat
import sys
from pathlib import Path

import pytest

from fpl_ingest.orchestration.drift_report import report_drift

_STUB_GH = """#!{python}
import json, os, sys
argv = sys.argv[1:]
call = {{"argv": argv}}
if "--body-file" in argv:
    src = argv[argv.index("--body-file") + 1]
    call["body"] = sys.stdin.read() if src == "-" else open(src).read()
elif "--body" in argv:
    call["body"] = argv[argv.index("--body") + 1]
with open(os.environ["GH_LOG"], "a") as log:
    log.write(json.dumps(call) + "\\n")
sub = " ".join(argv[:2])
if sub in os.environ.get("GH_FAIL", "").split(","):
    sys.stderr.write("HTTP 403: API rate limit exceeded\\n")
    sys.exit(1)
if sub == "issue list":
    sys.stdout.write(open(os.environ["GH_ISSUES"]).read())
elif sub == "issue create":
    counter = os.environ["GH_COUNTER"]
    n = int(open(counter).read()) + 1 if os.path.exists(counter) else 100
    open(counter, "w").write(str(n))
    sys.stdout.write(f"https://github.com/gisaf22/fpl-ingest/issues/{{n}}\\n")
"""


def _entry(path, kind="added", baseline=(), observed=("number",), payloads=1):
    return {
        "path": path,
        "kind": kind,
        "baseline_types": list(baseline),
        "observed_types": list(observed),
        "payloads": payloads,
    }


def _drift(status, entries=(), checked=1, reasons=()):
    return {"status": status, "checked": checked, "reasons": list(reasons), "entries": list(entries)}


def _write_report(tmp_path: Path, endpoints: dict) -> Path:
    path = tmp_path / "drift-report.json"
    path.write_text(
        json.dumps({"run_id": "run-1", "raw_contract_version": "2.4.0", "endpoints": endpoints})
    )
    return path


class Gh:
    """The stub ``gh``'s state: existing issues in, recorded calls out."""

    def __init__(self, tmp_path: Path, monkeypatch, issues=(), fail=()):
        self.dir = tmp_path / "gh"
        self.dir.mkdir(exist_ok=True)
        stub = self.dir / "gh"
        stub.write_text(_STUB_GH.format(python=sys.executable))
        stub.chmod(stub.stat().st_mode | stat.S_IEXEC)
        self.log = self.dir / "calls.jsonl"
        self.issues_file = self.dir / "issues.json"
        self.issues_file.write_text(json.dumps(list(issues)))
        monkeypatch.setenv("PATH", f"{self.dir}{os.pathsep}{os.environ['PATH']}")
        monkeypatch.setenv("GH_LOG", str(self.log))
        monkeypatch.setenv("GH_ISSUES", str(self.issues_file))
        monkeypatch.setenv("GH_COUNTER", str(self.dir / "counter"))
        monkeypatch.setenv("GH_FAIL", ",".join(fail))
        monkeypatch.setenv("GITHUB_SERVER_URL", "https://github.com")
        monkeypatch.setenv("GITHUB_REPOSITORY", "gisaf22/fpl-ingest")
        monkeypatch.setenv("GITHUB_RUN_ID", "42")
        monkeypatch.setenv("GITHUB_WORKFLOW", "Scheduled Ingestion — Daily")

    def calls(self) -> list[dict]:
        if not self.log.exists():
            return []
        return [json.loads(line) for line in self.log.read_text().splitlines()]

    def created(self) -> list[dict]:
        return [c for c in self.calls() if c["argv"][:2] == ["issue", "create"]]

    def reset_calls(self) -> None:
        self.log.unlink(missing_ok=True)

    def adopt_created(self, state: str = "OPEN") -> None:
        """Make every issue created so far exist on the next run, in ``state``."""
        issues = json.loads(self.issues_file.read_text())
        for n, call in enumerate(self.created(), start=100 + len(issues)):
            issues.append({"number": n, "state": state, "body": call["body"]})
        self.issues_file.write_text(json.dumps(issues))
        self.reset_calls()


def _run(report: Path | None, tmp_path: Path, capsys):
    summary = tmp_path / "summary.md"
    code = report_drift(report if report is not None else tmp_path / "absent.json", summary)
    out = capsys.readouterr().out
    text = summary.read_text() if summary.exists() else ""
    return code, text, [line for line in out.splitlines() if line.startswith("::warning")]


def _label(call: dict) -> str | None:
    argv = call["argv"]
    return argv[argv.index("--label") + 1] if "--label" in argv else None


# -- AC1 ------------------------------------------------------------------------


@pytest.mark.covers("#83 AC1")
def test_the_summary_lists_every_drift_with_its_types(tmp_path, monkeypatch, capsys):
    Gh(tmp_path, monkeypatch)
    report = _write_report(tmp_path, {
        "fixtures": _drift("drift", [_entry("$[].pulse_id", "type_changed", ["number"], ["string"])]),
        "element-summary": _drift("drift", [_entry("$.history[].new", "added", [], ["number"], 667)], 667),
    })

    code, summary, _ = _run(report, tmp_path, capsys)

    assert code == 0
    for text in ["fixtures", "$[].pulse_id", "type_changed", "string",
                 "element-summary", "$.history[].new", "added"]:
        assert text in summary


@pytest.mark.covers("#83 AC1")
def test_one_annotation_is_raised_per_endpoint_with_drift(tmp_path, monkeypatch, capsys):
    Gh(tmp_path, monkeypatch)
    report = _write_report(tmp_path, {
        "fixtures": _drift("drift", [_entry("$[].a"), _entry("$[].b", "removed", ["string"], [])]),
        "bootstrap-static": _drift("drift", [_entry("$.events[].c")]),
        "event-status": _drift("ok"),
    })

    _, _, warnings = _run(report, tmp_path, capsys)

    assert len([w for w in warnings if "fixtures" in w]) == 1
    assert len([w for w in warnings if "bootstrap-static" in w]) == 1
    assert not [w for w in warnings if "event-status" in w]


# -- AC2 ------------------------------------------------------------------------


@pytest.mark.covers("#83 AC2")
def test_a_new_drift_opens_exactly_one_labelled_issue_with_its_key(tmp_path, monkeypatch, capsys):
    gh = Gh(tmp_path, monkeypatch)
    report = _write_report(tmp_path, {"fixtures": _drift("drift", [_entry("$[].pulse_id")])})

    code, _, _ = _run(report, tmp_path, capsys)

    assert code == 0
    created = gh.created()
    assert len(created) == 1
    assert _label(created[0]) == "schema-drift"
    assert "<!-- drift-key: " in created[0]["body"]


# -- AC3 ------------------------------------------------------------------------


@pytest.mark.covers("#83 AC3")
@pytest.mark.parametrize("state", ["OPEN", "CLOSED"])
def test_a_drift_already_carrying_an_issue_opens_no_new_one(tmp_path, monkeypatch, capsys, state):
    gh = Gh(tmp_path, monkeypatch)
    report = _write_report(tmp_path, {"fixtures": _drift("drift", [_entry("$[].pulse_id")])})
    _run(report, tmp_path, capsys)
    gh.adopt_created(state)

    code, _, _ = _run(report, tmp_path, capsys)

    assert code == 0
    assert gh.created() == []


@pytest.mark.covers("#83 AC3")
def test_a_drift_with_different_types_is_not_suppressed_by_an_earlier_key(tmp_path, monkeypatch, capsys):
    gh = Gh(tmp_path, monkeypatch)
    first = _write_report(tmp_path, {"fixtures": _drift("drift", [_entry("$[].x", "type_changed", ["number"], ["string"])])})
    _run(first, tmp_path, capsys)
    gh.adopt_created()

    second = _write_report(tmp_path, {"fixtures": _drift("drift", [_entry("$[].x", "type_changed", ["number"], ["null"])])})
    _run(second, tmp_path, capsys)

    assert len(gh.created()) == 1


# -- AC4 ------------------------------------------------------------------------


@pytest.mark.covers("#83 AC4")
def test_two_distinct_drifts_open_two_issues(tmp_path, monkeypatch, capsys):
    gh = Gh(tmp_path, monkeypatch)
    report = _write_report(tmp_path, {
        "fixtures": _drift("drift", [_entry("$[].a")]),
        "bootstrap-static": _drift("drift", [_entry("$.events[].b")]),
    })

    _run(report, tmp_path, capsys)

    created = gh.created()
    assert len(created) == 2
    keys = {c["body"].split("<!-- drift-key: ")[1].split(" -->")[0] for c in created}
    assert len(keys) == 2


# -- AC5 ------------------------------------------------------------------------


@pytest.mark.covers("#83 AC5")
def test_a_failed_issue_create_warns_and_continues_with_the_next_key(tmp_path, monkeypatch, capsys):
    gh = Gh(tmp_path, monkeypatch, fail=["issue create"])
    report = _write_report(tmp_path, {
        "fixtures": _drift("drift", [_entry("$[].a")]),
        "bootstrap-static": _drift("drift", [_entry("$.events[].b")]),
    })

    code, _, warnings = _run(report, tmp_path, capsys)

    assert code == 0
    assert len(gh.created()) == 2
    assert len([w for w in warnings if "issue" in w.lower()]) >= 1


@pytest.mark.covers("#83 AC5")
@pytest.mark.parametrize("failing", ["issue list", "label create"])
def test_a_failed_lookup_or_label_create_warns_and_exits_zero(tmp_path, monkeypatch, capsys, failing):
    Gh(tmp_path, monkeypatch, fail=[failing])
    report = _write_report(tmp_path, {"fixtures": _drift("drift", [_entry("$[].a")])})

    code, summary, warnings = _run(report, tmp_path, capsys)

    assert code == 0
    assert "$[].a" in summary
    assert any(failing.split()[0] in w for w in warnings)


@pytest.mark.covers("#83 AC5")
def test_a_failed_lookup_opens_no_issue_rather_than_risk_duplicates(tmp_path, monkeypatch, capsys):
    gh = Gh(tmp_path, monkeypatch, fail=["issue list"])
    report = _write_report(tmp_path, {"fixtures": _drift("drift", [_entry("$[].a")])})

    _, summary, _ = _run(report, tmp_path, capsys)

    assert gh.created() == []
    assert "not opened" in summary


# -- AC6 ------------------------------------------------------------------------


@pytest.mark.covers("#83 AC6")
def test_a_clean_run_reports_no_drift_with_each_checked_count(tmp_path, monkeypatch, capsys):
    gh = Gh(tmp_path, monkeypatch)
    report = _write_report(tmp_path, {
        "bootstrap-static": _drift("ok", checked=1),
        "element-summary": _drift("ok", checked=667),
    })

    code, summary, _ = _run(report, tmp_path, capsys)

    assert code == 0
    assert gh.created() == []
    assert "No schema drift" in summary
    assert "bootstrap-static" in summary and "element-summary" in summary
    assert "667" in summary


# -- AC10 -----------------------------------------------------------------------


@pytest.mark.covers("#83 AC10")
def test_no_report_says_drift_was_not_checked(tmp_path, monkeypatch, capsys):
    gh = Gh(tmp_path, monkeypatch)

    code, summary, _ = _run(None, tmp_path, capsys)

    assert code == 0
    assert "No capture this run, drift not checked" in summary
    assert "No schema drift" not in summary
    assert gh.created() == []


# -- AC11 -----------------------------------------------------------------------


@pytest.mark.covers("#83 AC11")
def test_the_label_is_created_before_the_first_issue(tmp_path, monkeypatch, capsys):
    gh = Gh(tmp_path, monkeypatch)
    report = _write_report(tmp_path, {"fixtures": _drift("drift", [_entry("$[].a")])})

    _run(report, tmp_path, capsys)

    subs = [c["argv"][:2] for c in gh.calls()]
    label = subs.index(["label", "create"])
    create = subs.index(["issue", "create"])
    assert label < create
    assert gh.calls()[label]["argv"][2] == "schema-drift"
    assert "--force" in gh.calls()[label]["argv"]
    assert _label(gh.created()[0]) == "schema-drift"


# -- AC12 -----------------------------------------------------------------------


@pytest.mark.covers("#83 AC12")
def test_the_summary_marks_known_new_and_failed_issues_without_commenting(tmp_path, monkeypatch, capsys):
    gh = Gh(tmp_path, monkeypatch)
    known_open = _entry("$[].open_one")
    known_closed = _entry("$[].closed_one")
    _run(_write_report(tmp_path, {"fixtures": _drift("drift", [known_open])}), tmp_path, capsys)
    gh.adopt_created("OPEN")  # issue 100
    _run(_write_report(tmp_path, {"fixtures": _drift("drift", [known_closed])}), tmp_path, capsys)
    gh.adopt_created("CLOSED")  # issue 101

    report = _write_report(tmp_path, {"fixtures": _drift("drift", [known_open, known_closed, _entry("$[].fresh")])})
    _, summary, _ = _run(report, tmp_path, capsys)

    assert "known (#100, open)" in summary
    assert "known (#101, closed)" in summary
    assert len(gh.created()) == 1
    new_number = int((tmp_path / "gh" / "counter").read_text())
    assert f"#{new_number} new" in summary
    assert not [c for c in gh.calls() if c["argv"][:2] == ["issue", "comment"]]


@pytest.mark.covers("#83 AC12")
def test_the_summary_marks_an_issue_that_failed_to_open(tmp_path, monkeypatch, capsys):
    Gh(tmp_path, monkeypatch, fail=["issue create"])
    report = _write_report(tmp_path, {"fixtures": _drift("drift", [_entry("$[].a")])})

    _, summary, _ = _run(report, tmp_path, capsys)

    assert "not opened" in summary


# -- AC8 ------------------------------------------------------------------------


@pytest.mark.covers("#83 AC8")
def test_an_unavailable_check_is_shown_with_its_reason_and_opens_one_issue(tmp_path, monkeypatch, capsys):
    gh = Gh(tmp_path, monkeypatch)
    report = _write_report(tmp_path, {
        "event-live": _drift("unavailable", checked=0, reasons=["baseline missing: event-live.json"]),
    })

    code, summary, _ = _run(report, tmp_path, capsys)

    assert code == 0
    assert "baseline missing: event-live.json" in summary
    created = gh.created()
    assert len(created) == 1
    assert _label(created[0]) == "schema-drift"
    assert "event-live" in created[0]["argv"][created[0]["argv"].index("--title") + 1]


@pytest.mark.covers("#83 AC8")
def test_a_recurring_unavailable_check_opens_no_duplicate_even_with_new_reason_text(tmp_path, monkeypatch, capsys):
    gh = Gh(tmp_path, monkeypatch)
    _run(_write_report(tmp_path, {"event-live": _drift("unavailable", checked=0, reasons=["corrupt baseline"])}),
         tmp_path, capsys)
    gh.adopt_created()

    _run(_write_report(tmp_path, {"event-live": _drift("unavailable", checked=0, reasons=["internal error: KeyError"])}),
         tmp_path, capsys)

    assert gh.created() == []


# -- AC9 ------------------------------------------------------------------------


@pytest.mark.covers("#83 AC9")
def test_entries_on_an_unavailable_endpoint_are_surfaced_like_drift(tmp_path, monkeypatch, capsys):
    gh = Gh(tmp_path, monkeypatch)
    report = _write_report(tmp_path, {
        "element-summary": _drift(
            "unavailable", [_entry("$.history[].new", payloads=600)], checked=600,
            reasons=["payload is not JSON"],
        ),
    })

    _, summary, warnings = _run(report, tmp_path, capsys)

    assert "$.history[].new" in summary
    assert "payload is not JSON" in summary
    assert len([w for w in warnings if "element-summary" in w]) >= 1
    titles = [c["argv"][c["argv"].index("--title") + 1] for c in gh.created()]
    assert len(titles) == 2
    assert any("$.history[].new" in t for t in titles)
    assert any("unavailable" in t for t in titles)


# -- AC14 -----------------------------------------------------------------------


@pytest.mark.covers("#83 AC14")
def test_the_same_key_twice_in_one_report_opens_one_issue(tmp_path, monkeypatch, capsys):
    gh = Gh(tmp_path, monkeypatch)
    report = _write_report(tmp_path, {
        "fixtures": _drift("drift", [_entry("$[].a"), _entry("$[].a")]),
    })

    _run(report, tmp_path, capsys)

    assert len(gh.created()) == 1
