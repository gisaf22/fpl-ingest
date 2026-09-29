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

_STUB_CURL = """#!/bin/sh
printf '%s\\n' "$*" >> "$CURL_LOG"
exit "${CURL_EXIT:-0}"
"""


def _run(tmp_path: Path, status: str, *, url: str | None = _URL, curl_exit: int = 0):
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
