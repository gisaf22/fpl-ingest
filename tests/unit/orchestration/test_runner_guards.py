"""Unit tests for the runner's strict-mode guard helper.

Split out of ``tests/integration/orchestration/test_runner.py`` during the
test-taxonomy migration: these exercise ``_warn_or_raise_on_unclean_stage``
in isolation, with no pipeline, no filesystem, and no client.
"""

from __future__ import annotations

import pytest

from fpl_ingest.orchestration.runner import StrictRunFailure, _warn_or_raise_on_unclean_stage
from fpl_ingest.orchestration.stage_result import StageResult


def _skipped(stage: str) -> StageResult:
    return StageResult(stage=stage, fetched=10, validated=8, written=8, skipped=2)


def _errored(stage: str) -> StageResult:
    return StageResult(stage=stage, fetched=10, validated=10, written=10, skipped=0, errors=1)


def _clean(stage: str) -> StageResult:
    return StageResult(stage=stage, fetched=10, validated=10, written=10, skipped=0)


class TestWarnOrRaise:
    pytestmark = pytest.mark.unit

    def test_clean_stage_does_not_raise(self):
        r = _clean("core")
        _warn_or_raise_on_unclean_stage(r, strict=True)  # must not raise

    def test_unclean_non_strict_does_not_raise(self):
        r = _errored("core")
        _warn_or_raise_on_unclean_stage(r, strict=False)  # logs warning, no raise

    def test_unclean_strict_raises_strict_run_failure(self):
        r = _errored("core")
        with pytest.raises(StrictRunFailure) as exc_info:
            _warn_or_raise_on_unclean_stage(r, strict=True)
        assert exc_info.value.result is r

    def test_strict_run_failure_carries_failure_reason(self):
        r = _skipped("fixtures")
        with pytest.raises(StrictRunFailure) as exc_info:
            _warn_or_raise_on_unclean_stage(r, strict=True)
        assert exc_info.value.failure_reason == "skipped_records"
