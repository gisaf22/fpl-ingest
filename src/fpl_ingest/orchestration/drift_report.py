"""Surface a run's payload drift: job summary, annotations, one issue per distinct drift (#83).

Runs in the ``report-drift`` job, which holds an issue-writing token and no AWS
credentials (#80 D5). It reads the ``drift-report.json`` the capture job wrote
from its finalized manifest and calls the real ``gh``.

Every decision here is on #83:

- each manifest drift entry, and each ``unavailable`` endpoint, is one distinct
  drift with a dedup key; entries are surfaced whatever the endpoint's status;
- one ``gh issue list`` per run finds existing keys, open or closed; a key
  that already has an issue is never re-opened or commented on;
- the summary and annotations are written before any issue call, then
  rewritten with the issue outcomes;
- nothing here can fail the job: every ``gh`` error is a ``::warning::`` and
  the function returns 0. A failed lookup opens no issues, since without it
  dedup cannot work.
"""

from __future__ import annotations

import hashlib
import json
import os
import re
import subprocess
from dataclasses import dataclass
from pathlib import Path
from typing import Any

LABEL = "schema-drift"
_KEY_MARKER = re.compile(r"<!-- drift-key: ([0-9a-f]+) -->")
_NOT_OPENED = "not opened"
_GH_TIMEOUT_SECONDS = 60


@dataclass
class _Drift:
    key: str
    endpoint: str
    title: str
    entry: dict[str, Any] | None  # None for an unavailable check
    checked: int
    reasons: list[str]
    issue: str = _NOT_OPENED


def _types(types: Any) -> str:
    return ",".join(sorted(str(t) for t in types or ()))


def _shown(types: Any) -> str:
    return ", ".join(sorted(str(t) for t in types or ())) or "none"


def _key(text: str) -> str:
    return hashlib.sha256(text.encode()).hexdigest()[:16]


def _drifts(endpoints: dict[str, Any]) -> list[_Drift]:
    found: dict[str, _Drift] = {}
    for endpoint in sorted(endpoints):
        block = (endpoints[endpoint] or {}).get("drift") or {}
        checked = int(block.get("checked") or 0)
        reasons = [str(r) for r in block.get("reasons") or ()]
        for entry in block.get("entries") or ():
            key = _key(
                f"{endpoint}|{entry['path']}|{entry['kind']}|"
                f"{_types(entry.get('baseline_types'))}|{_types(entry.get('observed_types'))}"
            )
            title = (
                f"Schema drift: {endpoint} {entry['path']} {entry['kind']} "
                f"({_shown(entry.get('baseline_types'))} → {_shown(entry.get('observed_types'))})"
            )
            found.setdefault(key, _Drift(key, endpoint, title, entry, checked, reasons))
        if block.get("status") == "unavailable":
            key = _key(f"{endpoint}|unavailable")
            title = f"Schema drift check unavailable: {endpoint}"
            found.setdefault(key, _Drift(key, endpoint, title, None, checked, reasons))
    return list(found.values())


def _warn(message: str) -> None:
    print(f"::warning title=Schema drift::{message}", flush=True)


def _gh(*args: str, stdin: str | None = None) -> str | None:
    """Run ``gh``; on any failure warn and return None."""
    try:
        done = subprocess.run(
            ["gh", *args], input=stdin, capture_output=True, text=True, timeout=_GH_TIMEOUT_SECONDS, check=False
        )
    except (OSError, subprocess.SubprocessError) as exc:
        _warn(f"gh {' '.join(args[:2])} failed: {exc}")
        return None
    if done.returncode != 0:
        _warn(f"gh {' '.join(args[:2])} failed: {done.stderr.strip() or done.returncode}")
        return None
    return done.stdout


def _render(report: dict[str, Any], drifts: list[_Drift]) -> str:
    endpoints = report.get("endpoints") or {}
    lines = ["## Schema drift", "", f"Run `{report.get('run_id')}`, raw contract {report.get('raw_contract_version')}.", ""]
    entries = [d for d in drifts if d.entry is not None]
    unavailable = [d for d in drifts if d.entry is None]
    if not drifts:
        lines += ["No schema drift.", ""]
    if entries:
        lines += [
            "| Endpoint | Path | Kind | Baseline | Observed | Payloads | Issue |",
            "|---|---|---|---|---|---|---|",
        ]
        for d in entries:
            e = d.entry or {}
            lines.append(
                f"| {d.endpoint} | `{e['path']}` | {e['kind']} | {_shown(e.get('baseline_types'))} "
                f"| {_shown(e.get('observed_types'))} | {e.get('payloads')} of {d.checked} | {d.issue} |"
            )
        lines.append("")
    if unavailable:
        lines += ["**Drift check unavailable:**", ""]
        for d in unavailable:
            reasons = "; ".join(d.reasons) or "no reason given"
            lines.append(f"- {d.endpoint}: {reasons} (issue: {d.issue})")
        lines.append("")
    lines += ["| Endpoint checked | Payloads checked |", "|---|---|"]
    for endpoint in sorted(endpoints):
        block = (endpoints[endpoint] or {}).get("drift") or {}
        lines.append(f"| {endpoint} | {block.get('checked', 0)} |")
    return "\n".join(lines) + "\n"


def _body(report: dict[str, Any], drift: _Drift) -> str:
    server = os.environ.get("GITHUB_SERVER_URL", "https://github.com")
    repo = os.environ.get("GITHUB_REPOSITORY", "")
    run_id = os.environ.get("GITHUB_RUN_ID", "")
    run_link = f"{server}/{repo}/actions/runs/{run_id}" if repo and run_id else "(not run in Actions)"
    lines = [f"<!-- drift-key: {drift.key} -->", ""]
    if drift.entry is not None:
        e = drift.entry
        lines += [
            "| Endpoint | Path | Kind | Baseline types | Observed types | Payloads |",
            "|---|---|---|---|---|---|",
            f"| {drift.endpoint} | `{e['path']}` | {e['kind']} | {_shown(e.get('baseline_types'))} "
            f"| {_shown(e.get('observed_types'))} | {e.get('payloads')} of {drift.checked} |",
        ]
    else:
        lines += [
            f"The payload drift check was unavailable for `{drift.endpoint}`.",
            "",
            *[f"- {r}" for r in drift.reasons or ["no reason given"]],
        ]
    lines += [
        "",
        f"First seen in run {run_link} (ingest run `{report.get('run_id')}`, "
        f"workflow {os.environ.get('GITHUB_WORKFLOW', 'unknown')}).",
        "",
        "**To accept:** regenerate the baseline with "
        f"`uv run fpl-ingest baseline {drift.endpoint}` (a `removed` entry needs `--replace`), "
        "and open a PR that says `Closes #<this issue>`.",
        "**To dismiss:** close this issue as not planned. Never remove the `schema-drift` label: "
        "the label is how later runs find this issue and stay quiet.",
    ]
    return "\n".join(lines) + "\n"


def _open_issues(report: dict[str, Any], drifts: list[_Drift]) -> None:
    listed = _gh("issue", "list", "--label", LABEL, "--state", "all", "--limit", "1000",
                 "--json", "number,state,body")
    if listed is None:
        return  # without the lookup, dedup cannot work: open nothing
    known: dict[str, tuple[int, str]] = {}
    try:
        for issue in json.loads(listed or "[]"):
            for key in _KEY_MARKER.findall(issue.get("body") or ""):
                known.setdefault(key, (int(issue["number"]), str(issue["state"]).lower()))
    except (ValueError, KeyError, TypeError) as exc:
        _warn(f"gh issue list returned unreadable output: {exc}")
        return
    new = []
    for drift in drifts:
        if drift.key in known:
            number, state = known[drift.key]
            drift.issue = f"known (#{number}, {state})"
        else:
            new.append(drift)
    if not new:
        return
    _gh("label", "create", LABEL, "--force", "--color", "D93F0B",
        "--description", "Payload drift found by a scheduled run")
    for drift in new:
        created = _gh("issue", "create", "--title", drift.title, "--label", LABEL,
                      "--body-file", "-", stdin=_body(report, drift))
        tail = (created or "").strip().rstrip("/").rsplit("/", 1)[-1]
        if tail.isdigit():
            drift.issue = f"#{tail} new"
        elif created is not None:
            _warn(f"gh issue create for key {drift.key} printed no issue URL")


def report_drift(report_path: Path, summary_path: Path) -> int:
    """Surface the drift in ``report_path``; always returns 0."""
    start = summary_path.stat().st_size if summary_path.exists() else 0

    def write(text: str) -> None:
        with summary_path.open("r+" if summary_path.exists() else "w", encoding="utf-8") as fh:
            fh.seek(start)
            fh.truncate()
            fh.write(text)

    try:
        if not report_path.exists():
            print("::notice title=Schema drift::No capture this run, drift not checked", flush=True)
            write("## Schema drift\n\nNo capture this run, drift not checked.\n")
            return 0
        report = json.loads(report_path.read_text(encoding="utf-8"))
        drifts = _drifts(report.get("endpoints") or {})

        write(_render(report, drifts))
        for endpoint in sorted({d.endpoint for d in drifts if d.entry is not None}):
            count = sum(1 for d in drifts if d.entry is not None and d.endpoint == endpoint)
            _warn(f"{endpoint}: {count} payload drift entr{'y' if count == 1 else 'ies'}")
        for d in drifts:
            if d.entry is None:
                _warn(f"{d.endpoint}: drift check unavailable: {'; '.join(d.reasons) or 'no reason given'}")

        if drifts:
            _open_issues(report, drifts)
            write(_render(report, drifts))
    except Exception as exc:  # noqa: BLE001 - surfacing must never fail the run
        _warn(f"report-drift failed: {exc!r}")
    return 0
