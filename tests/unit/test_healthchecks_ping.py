"""The healthchecks.io ping script reports a job's outcome and never fails the job.

``.github/scripts/healthchecks_ping.sh`` runs as the last step of each
scheduled workflow. These tests run the real script with a stub ``curl`` first
on ``PATH``, which records each invocation and exits with a chosen code, so no
request leaves the machine.
"""

from __future__ import annotations

import os
import stat
import subprocess
from pathlib import Path

import pytest

_SCRIPT = Path(__file__).resolve().parents[2] / ".github" / "scripts" / "healthchecks_ping.sh"
_URL = "https://hc-ping.example/00000000-0000-0000-0000-000000000000"

# A jobs-API request (the only GET to /actions/runs/) is logged to API_LOG and
# answered from JOBS_JSON with API_EXIT; any other request is the ping, logged
# to CURL_LOG with its stdin body appended to BODY_LOG, and exits CURL_EXIT.
_STUB_CURL = """#!/bin/sh
case "$*" in
  */actions/runs/*)
    printf '%s\\n' "$*" >> "$API_LOG"
    if [ "${API_EXIT:-0}" != 0 ]; then exit "$API_EXIT"; fi
    cat "$JOBS_JSON"
    exit 0
    ;;
esac
printf '%s\\n' "$*" >> "$CURL_LOG"
cat >> "$BODY_LOG"
exit "${CURL_EXIT:-0}"
"""


def _run(
    tmp_path: Path,
    status: str,
    *,
    url: str | None = _URL,
    curl_exit: int = 0,
    extra_env: dict[str, str] | None = None,
):
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    curl = bin_dir / "curl"
    curl.write_text(_STUB_CURL)
    curl.chmod(curl.stat().st_mode | stat.S_IEXEC)
    log = tmp_path / "curl.log"

    env = {
        "PATH": f"{bin_dir}{os.pathsep}{os.environ['PATH']}",
        "CURL_LOG": str(log),
        "CURL_EXIT": str(curl_exit),
        "BODY_LOG": str(tmp_path / "body.log"),
        "API_LOG": str(tmp_path / "api.log"),
        "JOBS_JSON": str(tmp_path / "jobs.json"),
        **(extra_env or {}),
    }
    if url is not None:
        env["HEALTHCHECKS_PING_URL"] = url
    result = subprocess.run(
        ["bash", str(_SCRIPT), status], env=env, capture_output=True, text=True, timeout=30
    )
    requests = log.read_text().splitlines() if log.exists() else []
    return result, requests


@pytest.mark.covers("#57 AC1")
def test_a_successful_job_pings_the_plain_url_once(tmp_path):
    result, requests = _run(tmp_path, "success")

    assert result.returncode == 0
    assert len(requests) == 1
    assert requests[0].split()[-1] == _URL


@pytest.mark.covers("#57 AC2")
@pytest.mark.parametrize("status", ["failure", "cancelled"])
def test_a_failed_or_cancelled_job_pings_the_fail_url(tmp_path, status):
    result, requests = _run(tmp_path, status)

    assert result.returncode == 0
    assert len(requests) == 1
    assert requests[0].split()[-1] == f"{_URL}/fail"


@pytest.mark.covers("#57 AC3")
@pytest.mark.parametrize(
    "curl_exit",
    [7, 28, 22],
    ids=["connection refused", "timed out", "http error"],
)
@pytest.mark.parametrize("status", ["success", "failure"])
def test_a_failed_request_warns_and_still_exits_zero(tmp_path, status, curl_exit):
    result, requests = _run(tmp_path, status, curl_exit=curl_exit)

    assert result.returncode == 0
    assert len(requests) == 1
    assert "::warning::" in result.stdout


@pytest.mark.covers("#57 AC4")
@pytest.mark.parametrize("url", ["", None], ids=["empty secret", "unset secret"])
def test_a_missing_ping_url_skips_the_request_with_a_notice(tmp_path, url):
    result, requests = _run(tmp_path, "success", url=url)

    assert result.returncode == 0
    assert requests == []
    assert "::notice::" in result.stdout


# The ping body (#130). GitHub's default environment, as a scheduled job sees it.
_TOKEN = "ghs_fake-token-never-in-the-body"
_GITHUB_ENV = {
    "GITHUB_SERVER_URL": "https://github.com",
    "GITHUB_REPOSITORY": "gisaf22/example",
    "GITHUB_RUN_ID": "123456789",
    "GITHUB_RUN_ATTEMPT": "2",
    "GITHUB_WORKFLOW": "Scheduled run (daily)",
    "GITHUB_API_URL": "https://api.github.example",
    "RUNNER_NAME": "GitHub Actions 7",
    "GITHUB_TOKEN": _TOKEN,
}
_RUN_URL = "https://github.com/gisaf22/example/actions/runs/123456789"

_JOBS = {
    "jobs": [
        {
            "name": "build",
            "status": "in_progress",
            "runner_name": "GitHub Actions 7",
            "steps": [
                {"name": "Check input freshness", "status": "completed", "conclusion": "success"},
                {"name": "Build and test against live S3", "status": "completed", "conclusion": "failure"},
                {"name": "Publish served tables to S3", "status": "completed", "conclusion": "skipped"},
                {"name": "Record build metrics", "status": "completed", "conclusion": "cancelled"},
                {"name": "Ping healthchecks.io", "status": "in_progress", "conclusion": None},
            ],
        },
        {
            "name": "another job on another runner",
            "status": "completed",
            "runner_name": "GitHub Actions 9",
            "steps": [{"name": "Not this job's step", "status": "completed", "conclusion": "failure"}],
        },
    ]
}

_RUN_RESULTS = {
    "results": [
        {"unique_id": "model.fpl_warehouse.stg_player", "status": "success"},
        {"unique_id": "test.fpl_warehouse.not_null_stg_player_fpl_id.ab12", "status": "pass"},
        {"unique_id": "test.fpl_warehouse.accepted_values_stg_player_status.cd34", "status": "fail"},
        {"unique_id": "test.fpl_warehouse.stg_test_later_failure", "status": "error"},
    ]
}


def _with_github(tmp_path: Path, *, jobs=_JOBS, run_results=_RUN_RESULTS, **overrides):
    import json

    (tmp_path / "jobs.json").write_text(json.dumps(jobs))
    env = {**_GITHUB_ENV}
    if run_results is not None:
        results = tmp_path / "run_results.json"
        results.write_text(json.dumps(run_results))
        env["DBT_RUN_RESULTS"] = str(results)
    env.update(overrides)
    return {k: v for k, v in env.items() if v is not None}


def _body(tmp_path: Path) -> str:
    log = tmp_path / "body.log"
    return log.read_text() if log.exists() else ""


def _assert_no_secret(body: str) -> None:
    assert _URL not in body
    assert "hc-ping" not in body
    assert _TOKEN not in body


@pytest.mark.covers("#130 AC1")
def test_a_success_ping_posts_the_workflow_status_and_run_url(tmp_path):
    result, requests = _run(tmp_path, "success", extra_env=_with_github(tmp_path))
    body = _body(tmp_path)

    assert result.returncode == 0
    assert len(requests) == 1
    assert requests[0].split()[-1] == _URL
    assert "--data-binary @-" in requests[0]
    assert body.splitlines() == [
        "workflow: Scheduled run (daily)",
        "status: success",
        f"run: {_RUN_URL}",
    ]
    # A success needs no step lookup, so it makes no API request.
    assert not (tmp_path / "api.log").exists()
    _assert_no_secret(body)


@pytest.mark.covers("#130 AC2")
def test_a_fail_ping_names_the_failed_steps_and_first_failing_dbt_test(tmp_path):
    result, requests = _run(tmp_path, "failure", extra_env=_with_github(tmp_path))
    body = _body(tmp_path)
    api = (tmp_path / "api.log").read_text()

    assert result.returncode == 0
    assert len(requests) == 1
    assert requests[0].split()[-1] == f"{_URL}/fail"
    assert body.splitlines() == [
        "workflow: Scheduled run (daily)",
        "status: failure",
        f"run: {_RUN_URL}",
        "failed steps:",
        "- Build and test against live S3",
        "- Record build metrics (cancelled)",
        "first failing dbt test: test.fpl_warehouse.accepted_values_stg_player_status.cd34",
    ]
    # The lookup reads this run attempt's jobs, authorised by the token.
    assert (
        "https://api.github.example/repos/gisaf22/example/actions/runs/123456789/attempts/2/jobs"
        in api
    )
    assert f"Authorization: Bearer {_TOKEN}" in api
    _assert_no_secret(body)


@pytest.mark.covers("#130 AC2")
def test_a_fail_ping_without_run_results_omits_the_dbt_line(tmp_path):
    # This repo sets no DBT_RUN_RESULTS: the body has no dbt line at all.
    result, requests = _run(
        tmp_path, "failure", extra_env=_with_github(tmp_path, run_results=None)
    )
    body = _body(tmp_path)

    assert result.returncode == 0
    assert len(requests) == 1
    assert "failed steps:" in body
    assert "dbt" not in body


@pytest.mark.covers("#130 AC3")
@pytest.mark.parametrize("curl_exit", [7, 28, 22], ids=["connection refused", "timed out", "http error"])
@pytest.mark.parametrize("status", ["success", "failure"])
def test_a_failed_ping_with_a_body_warns_and_still_exits_zero(tmp_path, status, curl_exit):
    result, requests = _run(
        tmp_path, status, curl_exit=curl_exit, extra_env=_with_github(tmp_path)
    )

    assert result.returncode == 0
    assert len(requests) == 1
    assert "::warning::healthchecks.io ping failed" in result.stdout
    assert _URL not in result.stdout + result.stderr


@pytest.mark.covers("#130 AC4")
@pytest.mark.parametrize(
    ("overrides", "expected"),
    [
        ({"API_EXIT": "22"}, "unavailable (jobs API request failed)"),
        ({"GITHUB_TOKEN": None}, "unavailable (needs GITHUB_TOKEN with actions: read, and jq)"),
    ],
    ids=["jobs API fails", "no token"],
)
def test_an_unreadable_detail_is_reported_and_the_ping_still_sent(tmp_path, overrides, expected):
    extra = _with_github(tmp_path, **overrides)
    extra["DBT_RUN_RESULTS"] = str(tmp_path / "missing.json")
    result, requests = _run(tmp_path, "failure", extra_env=extra)
    body = _body(tmp_path)

    assert result.returncode == 0
    assert len(requests) == 1
    assert requests[0].split()[-1] == f"{_URL}/fail"
    assert expected in body
    assert "first failing dbt test: unavailable (no run_results.json" in body
    _assert_no_secret(body)


@pytest.mark.covers("#130 AC4")
def test_a_garbled_jobs_response_is_reported_and_the_ping_still_sent(tmp_path):
    extra = _with_github(tmp_path)
    (tmp_path / "jobs.json").write_text("<html>not json</html>")
    result, requests = _run(tmp_path, "failure", extra_env=extra)

    assert result.returncode == 0
    assert len(requests) == 1
    assert "unavailable (jobs API response unreadable)" in _body(tmp_path)
