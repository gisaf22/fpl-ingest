"""Canonical terminal run-status classification for ingest runs.

A run's status describes what it left usable, and it is derived from the
per-endpoint outcomes in the run manifest's ``endpoints`` block by the same
rule those outcomes follow:

* SUCCESS — everything attempted is usable.
* PARTIAL — some usable, some failed.
* FAILED  — nothing usable, including a run that recorded no endpoints.

A deliberate non-fetch under the refetch policy is not recorded as an
endpoint, so it never makes a run PARTIAL. Whether to alert is a separate
question answered by the exit code, which is non-zero for anything but
SUCCESS. Every caller — the full run and the pre-deadline run — uses
``classify_run`` so status is assigned consistently.
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any, Literal

RunStatus = Literal["SUCCESS", "PARTIAL", "FAILED"]

RUN_STATUS_SUCCESS: Literal["SUCCESS"] = "SUCCESS"
RUN_STATUS_PARTIAL: Literal["PARTIAL"] = "PARTIAL"
RUN_STATUS_FAILED: Literal["FAILED"] = "FAILED"


def classify_run(endpoints: Mapping[str, Mapping[str, Any]]) -> RunStatus:
    """Return the run status for a manifest's ``endpoints`` block."""
    usable = sum(entry["usable"] for entry in endpoints.values())
    if usable == 0:
        return RUN_STATUS_FAILED
    if all(entry["outcome"] == RUN_STATUS_SUCCESS for entry in endpoints.values()):
        return RUN_STATUS_SUCCESS
    return RUN_STATUS_PARTIAL
