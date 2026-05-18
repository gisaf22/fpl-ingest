"""Tests for StageResult invariants and derived properties."""

from __future__ import annotations

import pytest

from fpl_ingest.orchestration.stage_result import StageResult

pytestmark = pytest.mark.unit


class TestInvariantViolations:

    def test_validated_exceeds_fetched_raises(self):
        with pytest.raises(ValueError, match="fetched"):
            StageResult(stage="core", fetched=5, validated=6, written=5, skipped=0)

    def test_written_exceeds_fetched_raises(self):
        with pytest.raises(ValueError, match="validated"):
            StageResult(stage="core", fetched=5, validated=5, written=6, skipped=0)

    def test_written_exceeds_validated_raises(self):
        with pytest.raises(ValueError, match="validated"):
            StageResult(stage="core", fetched=10, validated=5, written=8, skipped=5)

    def test_skipped_mismatch_raises(self):
        with pytest.raises(ValueError, match="skipped"):
            StageResult(stage="core", fetched=10, validated=8, written=8, skipped=3)


class TestValidConstruction:

    def test_clean_result(self):
        r = StageResult(stage="core", fetched=10, validated=10, written=10, skipped=0, errors=0)
        assert r.fetched == 10
        assert r.errors == 0

    def test_partial_result(self):
        r = StageResult(stage="core", fetched=10, validated=8, written=8, skipped=2, errors=0)
        assert r.skipped == 2
        assert r.validated == 8

    def test_error_result(self):
        r = StageResult(stage="core", fetched=10, validated=8, written=8, skipped=2, errors=1)
        assert r.errors == 1

    def test_zero_result(self):
        r = StageResult(stage="core")
        assert r.fetched == r.validated == r.written == r.skipped == r.errors == 0


class TestDerivedProperties:

    def test_is_clean_true_when_no_errors_and_no_skips(self):
        r = StageResult(stage="core", fetched=10, validated=10, written=10, skipped=0)
        assert r.is_clean is True

    def test_is_clean_false_when_skipped(self):
        r = StageResult(stage="core", fetched=10, validated=8, written=8, skipped=2)
        assert r.is_clean is False

    def test_is_clean_false_when_errors(self):
        r = StageResult(stage="core", fetched=10, validated=10, written=10, skipped=0, errors=1)
        assert r.is_clean is False

    def test_failure_reason_errors(self):
        r = StageResult(stage="core", fetched=10, validated=10, written=10, skipped=0, errors=1)
        assert r.failure_reason == "validation_error"

    def test_failure_reason_skipped(self):
        r = StageResult(stage="core", fetched=10, validated=8, written=8, skipped=2)
        assert r.failure_reason == "skipped_records"

    def test_failure_reason_none_when_clean(self):
        r = StageResult(stage="core", fetched=10, validated=10, written=10, skipped=0)
        assert r.failure_reason is None

    def test_totals_aggregates_across_results(self):
        r1 = StageResult(stage="core", fetched=10, validated=10, written=10, skipped=0, errors=0)
        r2 = StageResult(stage="fixtures", fetched=5, validated=4, written=4, skipped=1, errors=1)
        assert StageResult.totals([r1, r2]) == (15, 14, 14, 1, 1)

    def test_totals_empty_list(self):
        assert StageResult.totals([]) == (0, 0, 0, 0, 0)
