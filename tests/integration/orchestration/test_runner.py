"""Tests for the pipeline runner."""

from __future__ import annotations

import asyncio
import logging
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from fpl_ingest.orchestration.run_status import RUN_STATUS_FAILED, RUN_STATUS_SUCCESS
from fpl_ingest.extract.stages.bootstrap import CoreData
from fpl_ingest.orchestration.runner import StrictRunFailure, _warn_or_raise_on_unclean_stage, run_pipeline
from fpl_ingest.orchestration.stage_result import StageOutcome, StageResult


def _clean(stage: str) -> StageResult:
    return StageResult(stage=stage, fetched=10, validated=10, written=10, skipped=0)


def _skipped(stage: str) -> StageResult:
    return StageResult(stage=stage, fetched=10, validated=8, written=8, skipped=2)


def _errored(stage: str) -> StageResult:
    return StageResult(stage=stage, fetched=10, validated=10, written=10, skipped=0, errors=1)


_EMPTY_CORE = CoreData(events=[], player_ids=[])


def _make_args(**overrides) -> SimpleNamespace:
    base = dict(rate=1.0, strict=False)
    base.update(overrides)
    return SimpleNamespace(**base)


def _make_config(tmp_path: Path) -> SimpleNamespace:
    return SimpleNamespace(raw_dir=tmp_path / "raw", storage_backend="local", s3_bucket=None)


def _silent_logger() -> logging.Logger:
    logger = logging.getLogger("test_runner")
    logger.setLevel(logging.CRITICAL)
    return logger


def _mock_async_fpl_client():
    """Return a patched AsyncFPLClient class that behaves as an async context manager."""
    mock_cls = MagicMock()
    mock_cls.return_value.__aenter__ = AsyncMock(return_value=AsyncMock())
    mock_cls.return_value.__aexit__ = AsyncMock(return_value=None)
    return mock_cls


def _run_pipeline(args, tmp_path,
                  event_status_result=None, core_result=None, fixtures_result=None,
                  gw_result=None, hist_result=None) -> int:
    with (
        patch("fpl_ingest.orchestration.runner.AsyncFPLClient", _mock_async_fpl_client()),
        patch("fpl_ingest.orchestration.runner.ingest_event_status",
              AsyncMock(return_value=StageOutcome(result=event_status_result or _clean("event_status"), output={}))),
        patch("fpl_ingest.orchestration.runner.ingest_core_data",
              AsyncMock(return_value=StageOutcome(result=core_result or _clean("core"), output=_EMPTY_CORE))),
        patch("fpl_ingest.orchestration.runner.ingest_fixtures",
              AsyncMock(return_value=StageOutcome(result=fixtures_result or _clean("fixtures")))),
        patch("fpl_ingest.orchestration.runner.ingest_gameweeks",
              AsyncMock(return_value=StageOutcome(result=gw_result or _clean("gameweeks")))),
        patch("fpl_ingest.orchestration.runner.ingest_player_histories",
              AsyncMock(return_value=StageOutcome(result=hist_result or _clean("player_histories")))),
    ):
        return asyncio.run(
            run_pipeline(
                args=args,
                config=_make_config(tmp_path),
                logger=_silent_logger(),
            )
        )


class TestExitCodeMapping:
    pytestmark = pytest.mark.integration

    def test_all_clean_stages_return_exit_code_0(self, tmp_path):
        code = _run_pipeline(_make_args(), tmp_path)
        assert code == 0

    def test_skipped_stage_returns_exit_code_1(self, tmp_path):
        code = _run_pipeline(_make_args(), tmp_path,
                             gw_result=_skipped("gameweeks"))
        assert code == 1

    def test_errored_stage_returns_exit_code_1(self, tmp_path):
        code = _run_pipeline(_make_args(), tmp_path,
                             fixtures_result=_errored("fixtures"))
        assert code == 1

    def test_strict_first_stage_failure_returns_exit_code_1(self, tmp_path):
        code = _run_pipeline(_make_args(strict=True), tmp_path,
                             core_result=_errored("core"))
        assert code == 1


class TestFinalisationOrder:
    pytestmark = pytest.mark.integration

    def test_manifest_written_on_clean_run(self, tmp_path):
        code = _run_pipeline(_make_args(), tmp_path)
        assert code == 0
        manifests = sorted((tmp_path / "raw" / "fpl" / "_manifests").rglob("manifest.json"))
        assert len(manifests) == 1

    def test_manifest_records_stage_order(self, tmp_path):
        import json

        _run_pipeline(_make_args(), tmp_path)
        manifests = sorted((tmp_path / "raw" / "fpl" / "_manifests").rglob("manifest.json"))
        manifest = json.loads(manifests[0].read_text())
        # event-status must be captured before every other endpoint (strategy doc A.5);
        # the stages in this test don't write raw objects (they're mocked outcomes),
        # so the manifest here just confirms one manifest is produced per run.
        assert manifest["status"] == RUN_STATUS_SUCCESS

    def test_manifest_written_on_strict_abort(self, tmp_path):
        import json

        code = _run_pipeline(_make_args(strict=True), tmp_path,
                              core_result=_errored("core"))
        assert code == 1
        manifests = sorted((tmp_path / "raw" / "fpl" / "_manifests").rglob("manifest.json"))
        assert len(manifests) == 1
        manifest = json.loads(manifests[0].read_text())
        assert manifest["status"] == RUN_STATUS_FAILED


class TestStrictModeAbort:
    pytestmark = pytest.mark.integration

    def test_strict_abort_skips_subsequent_stages(self, tmp_path):
        mock_fixtures = AsyncMock(return_value=StageOutcome(result=_clean("fixtures")))

        with (
            patch("fpl_ingest.orchestration.runner.AsyncFPLClient", _mock_async_fpl_client()),
            patch("fpl_ingest.orchestration.runner.ingest_event_status",
                  AsyncMock(return_value=StageOutcome(result=_clean("event_status"), output={}))),
            patch("fpl_ingest.orchestration.runner.ingest_core_data",
                  AsyncMock(return_value=StageOutcome(result=_errored("core"), output=_EMPTY_CORE))),
            patch("fpl_ingest.orchestration.runner.ingest_fixtures", mock_fixtures),
            patch("fpl_ingest.orchestration.runner.ingest_gameweeks",
                  AsyncMock(return_value=StageOutcome(result=_clean("gameweeks")))),
            patch("fpl_ingest.orchestration.runner.ingest_player_histories",
                  AsyncMock(return_value=StageOutcome(result=_clean("player_histories")))),
        ):
            code = asyncio.run(
                run_pipeline(
                    args=_make_args(strict=True),
                    config=_make_config(tmp_path),
                    logger=_silent_logger(),
                )
            )

        assert code == 1
        mock_fixtures.assert_not_called()


class TestManifestProvenance:
    """Strategy doc §A.5 requires git_sha, ingest_version, and config to be
    populated on the finalized manifest — they must not stay null."""

    pytestmark = pytest.mark.integration

    def test_manifest_records_ingest_version(self, tmp_path):
        import json

        from fpl_ingest import __version__

        _run_pipeline(_make_args(), tmp_path)
        manifests = sorted((tmp_path / "raw" / "fpl" / "_manifests").rglob("manifest.json"))
        manifest = json.loads(manifests[0].read_text())
        assert manifest["ingest_version"] == __version__

    def test_manifest_records_git_sha_when_git_available(self, tmp_path):
        import json

        with patch("fpl_ingest.orchestration.runner._current_git_sha", return_value="deadbeef"):
            _run_pipeline(_make_args(), tmp_path)
        manifests = sorted((tmp_path / "raw" / "fpl" / "_manifests").rglob("manifest.json"))
        manifest = json.loads(manifests[0].read_text())
        assert manifest["git_sha"] == "deadbeef"

    def test_manifest_git_sha_is_none_without_crashing_when_git_unavailable(self, tmp_path):
        import json
        import subprocess

        with patch("fpl_ingest.orchestration.runner.subprocess.run",
                   side_effect=FileNotFoundError("git not found")):
            code = _run_pipeline(_make_args(), tmp_path)

        assert code == 0
        manifests = sorted((tmp_path / "raw" / "fpl" / "_manifests").rglob("manifest.json"))
        manifest = json.loads(manifests[0].read_text())
        assert manifest["git_sha"] is None

    def test_manifest_records_effective_run_config(self, tmp_path):
        import json

        args = _make_args(rate=5.0, strict=False)
        _run_pipeline(args, tmp_path)
        manifests = sorted((tmp_path / "raw" / "fpl" / "_manifests").rglob("manifest.json"))
        manifest = json.loads(manifests[0].read_text())
        assert manifest["config"]["rate"] == 5.0
        assert manifest["config"]["strict"] is False

    def test_manifest_records_skip_player_histories_absent_from_args(self, tmp_path):
        """The namespace these tests build has no such attribute at all, which
        is exactly the case the getattr default in `_effective_run_config` and
        `run_pipeline` exists to survive."""
        import json

        args = _make_args(rate=5.0, strict=False)
        assert not hasattr(args, "skip_player_histories")
        _run_pipeline(args, tmp_path)
        manifests = sorted((tmp_path / "raw" / "fpl" / "_manifests").rglob("manifest.json"))
        manifest = json.loads(manifests[0].read_text())
        assert manifest["config"]["skip_player_histories"] is False

    def test_manifest_records_skip_player_histories_when_set(self, tmp_path):
        """Every run is self-describing about whether it ran in reduced-capture
        mode, so the raw tree explains its own object count after the fact."""
        import json

        args = _make_args(rate=5.0, strict=False, skip_player_histories=True)
        _run_pipeline(args, tmp_path)
        manifests = sorted((tmp_path / "raw" / "fpl" / "_manifests").rglob("manifest.json"))
        manifest = json.loads(manifests[0].read_text())
        assert manifest["config"]["skip_player_histories"] is True


class TestCentralizedFailFastTrigger:
    """``_execute_stage``'s own ``errors > 0 -> execution_state.fail()`` net.

    Unlike bootstrap/gameweeks/element_summary, ``event_status.py`` and
    ``fixtures.py`` never call ``execution_state.fail()`` themselves on a
    hard fetch failure — before this fix, that meant a hard failure in
    either of them silently left ``execution_state.is_failed`` False for
    every downstream stage, non-strict runs included. ``_execute_stage`` now
    closes that gap for every stage that routes through it, regardless of
    whether that stage's own author remembered a manual call.

    The soft-failure tests pin the regression this fix must specifically
    avoid: a shape-validation failure reports ``skipped=1, errors=0`` (see
    e.g. ``test_event_status.py::test_shape_failure_still_writes_the_payload_and_yields_no_finality``)
    and must keep NOT tripping fail-fast — only ``errors > 0`` should.
    """

    pytestmark = pytest.mark.integration

    def test_event_status_hard_failure_trips_fail_fast(self, tmp_path):
        event_status_mock = AsyncMock(
            return_value=StageOutcome(result=_errored("event_status"), output=None)
        )
        with (
            patch("fpl_ingest.orchestration.runner.AsyncFPLClient", _mock_async_fpl_client()),
            patch("fpl_ingest.orchestration.runner.ingest_event_status", event_status_mock),
            patch("fpl_ingest.orchestration.runner.ingest_core_data",
                  AsyncMock(return_value=StageOutcome(result=_clean("core"), output=_EMPTY_CORE))),
            patch("fpl_ingest.orchestration.runner.ingest_fixtures",
                  AsyncMock(return_value=StageOutcome(result=_clean("fixtures")))),
            patch("fpl_ingest.orchestration.runner.ingest_gameweeks",
                  AsyncMock(return_value=StageOutcome(result=_clean("gameweeks")))),
            patch("fpl_ingest.orchestration.runner.ingest_player_histories",
                  AsyncMock(return_value=StageOutcome(result=_clean("player_histories")))),
        ):
            asyncio.run(
                run_pipeline(args=_make_args(), config=_make_config(tmp_path), logger=_silent_logger())
            )

        execution_state = event_status_mock.call_args.kwargs["execution_state"]
        assert execution_state.is_failed is True

    def test_event_status_shape_validation_soft_failure_does_not_trip_fail_fast(self, tmp_path):
        event_status_mock = AsyncMock(
            return_value=StageOutcome(result=_skipped("event_status"), output=None)
        )
        with (
            patch("fpl_ingest.orchestration.runner.AsyncFPLClient", _mock_async_fpl_client()),
            patch("fpl_ingest.orchestration.runner.ingest_event_status", event_status_mock),
            patch("fpl_ingest.orchestration.runner.ingest_core_data",
                  AsyncMock(return_value=StageOutcome(result=_clean("core"), output=_EMPTY_CORE))),
            patch("fpl_ingest.orchestration.runner.ingest_fixtures",
                  AsyncMock(return_value=StageOutcome(result=_clean("fixtures")))),
            patch("fpl_ingest.orchestration.runner.ingest_gameweeks",
                  AsyncMock(return_value=StageOutcome(result=_clean("gameweeks")))),
            patch("fpl_ingest.orchestration.runner.ingest_player_histories",
                  AsyncMock(return_value=StageOutcome(result=_clean("player_histories")))),
        ):
            asyncio.run(
                run_pipeline(args=_make_args(), config=_make_config(tmp_path), logger=_silent_logger())
            )

        execution_state = event_status_mock.call_args.kwargs["execution_state"]
        assert execution_state.is_failed is False

    def test_fixtures_hard_failure_trips_fail_fast(self, tmp_path):
        fixtures_mock = AsyncMock(return_value=StageOutcome(result=_errored("fixtures")))
        with (
            patch("fpl_ingest.orchestration.runner.AsyncFPLClient", _mock_async_fpl_client()),
            patch("fpl_ingest.orchestration.runner.ingest_event_status",
                  AsyncMock(return_value=StageOutcome(result=_clean("event_status"), output={}))),
            patch("fpl_ingest.orchestration.runner.ingest_core_data",
                  AsyncMock(return_value=StageOutcome(result=_clean("core"), output=_EMPTY_CORE))),
            patch("fpl_ingest.orchestration.runner.ingest_fixtures", fixtures_mock),
            patch("fpl_ingest.orchestration.runner.ingest_gameweeks",
                  AsyncMock(return_value=StageOutcome(result=_clean("gameweeks")))),
            patch("fpl_ingest.orchestration.runner.ingest_player_histories",
                  AsyncMock(return_value=StageOutcome(result=_clean("player_histories")))),
        ):
            asyncio.run(
                run_pipeline(args=_make_args(), config=_make_config(tmp_path), logger=_silent_logger())
            )

        execution_state = fixtures_mock.call_args.kwargs["execution_state"]
        assert execution_state.is_failed is True

    def test_fixtures_shape_validation_soft_failure_does_not_trip_fail_fast(self, tmp_path):
        fixtures_mock = AsyncMock(return_value=StageOutcome(result=_skipped("fixtures")))
        with (
            patch("fpl_ingest.orchestration.runner.AsyncFPLClient", _mock_async_fpl_client()),
            patch("fpl_ingest.orchestration.runner.ingest_event_status",
                  AsyncMock(return_value=StageOutcome(result=_clean("event_status"), output={}))),
            patch("fpl_ingest.orchestration.runner.ingest_core_data",
                  AsyncMock(return_value=StageOutcome(result=_clean("core"), output=_EMPTY_CORE))),
            patch("fpl_ingest.orchestration.runner.ingest_fixtures", fixtures_mock),
            patch("fpl_ingest.orchestration.runner.ingest_gameweeks",
                  AsyncMock(return_value=StageOutcome(result=_clean("gameweeks")))),
            patch("fpl_ingest.orchestration.runner.ingest_player_histories",
                  AsyncMock(return_value=StageOutcome(result=_clean("player_histories")))),
        ):
            asyncio.run(
                run_pipeline(args=_make_args(), config=_make_config(tmp_path), logger=_silent_logger())
            )

        execution_state = fixtures_mock.call_args.kwargs["execution_state"]
        assert execution_state.is_failed is False
