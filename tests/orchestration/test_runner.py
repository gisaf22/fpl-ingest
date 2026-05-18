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
from fpl_ingest.load.db_setup import setup_store
from fpl_ingest.orchestration.runner import StrictRunFailure, _warn_or_raise_on_unclean_stage, run_pipeline
from fpl_ingest.orchestration.stage_result import StageOutcome, StageResult
from fpl_ingest.load.store import SQLiteStore
from tests.factories import fixture_row, player_row, team_row


def _clean(stage: str) -> StageResult:
    return StageResult(stage=stage, fetched=10, validated=10, written=10, skipped=0)


def _skipped(stage: str) -> StageResult:
    return StageResult(stage=stage, fetched=10, validated=8, written=8, skipped=2)


def _errored(stage: str) -> StageResult:
    return StageResult(stage=stage, fetched=10, validated=10, written=10, skipped=0, errors=1)


_EMPTY_CORE = CoreData(players=[], teams=[], events=[], element_types=[])


def _make_store(tmp_path: Path) -> SQLiteStore:
    return SQLiteStore(tmp_path / "test.db")


def _make_args(**overrides) -> SimpleNamespace:
    base = dict(rate=1.0, force=False, strict=False, stale_after_hours=None)
    base.update(overrides)
    return SimpleNamespace(**base)


def _make_config(tmp_path: Path) -> SimpleNamespace:
    return SimpleNamespace(raw_dir=tmp_path / "raw")


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


def _run_pipeline(store, args, tmp_path,
                  core_result=None, fixtures_result=None,
                  gw_result=None, hist_result=None) -> int:
    with (
        patch("fpl_ingest.orchestration.runner.AsyncFPLClient", _mock_async_fpl_client()),
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
                store=store,
            )
        )


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


class TestExitCodeMapping:
    pytestmark = pytest.mark.integration

    def test_all_clean_stages_return_exit_code_0(self, tmp_path):
        store = _make_store(tmp_path)
        code = _run_pipeline(store, _make_args(), tmp_path)
        assert code == 0

    def test_skipped_stage_returns_exit_code_1(self, tmp_path):
        store = _make_store(tmp_path)
        code = _run_pipeline(store, _make_args(), tmp_path,
                             gw_result=_skipped("gameweeks"))
        assert code == 1

    def test_errored_stage_returns_exit_code_1(self, tmp_path):
        store = _make_store(tmp_path)
        code = _run_pipeline(store, _make_args(), tmp_path,
                             fixtures_result=_errored("fixtures"))
        assert code == 1

    def test_strict_first_stage_failure_returns_exit_code_1(self, tmp_path):
        store = _make_store(tmp_path)
        code = _run_pipeline(store, _make_args(strict=True), tmp_path,
                             core_result=_errored("core"))
        assert code == 1


class TestFinalisationOrder:
    pytestmark = pytest.mark.integration

    def test_runs_table_written_on_clean_run(self, tmp_path):
        store = _make_store(tmp_path)
        _run_pipeline(store, _make_args(), tmp_path)
        rows = store.query("SELECT stage FROM _runs ORDER BY id")
        stages = [r["stage"] for r in rows]
        assert "core" in stages
        assert "fixtures" in stages
        assert "gameweeks" in stages
        assert "player_histories" in stages

    def test_last_successful_run_at_written_on_clean_run(self, tmp_path):
        store = _make_store(tmp_path)
        _run_pipeline(store, _make_args(), tmp_path)
        meta = store.query("SELECT value FROM _metadata WHERE key = 'last_successful_run_at'")
        assert meta and meta[0]["value"] is not None

    def test_last_successful_run_at_not_written_on_skipped_run(self, tmp_path):
        store = _make_store(tmp_path)
        _run_pipeline(store, _make_args(), tmp_path,
                      gw_result=_skipped("gameweeks"))
        meta = store.query("SELECT value FROM _metadata WHERE key = 'last_successful_run_at'")
        assert meta == [], "last_successful_run_at must not be set when the run is not clean"

    def test_last_successful_run_at_not_written_on_errored_run(self, tmp_path):
        store = _make_store(tmp_path)
        _run_pipeline(store, _make_args(), tmp_path,
                      fixtures_result=_errored("fixtures"))
        meta = store.query("SELECT value FROM _metadata WHERE key = 'last_successful_run_at'")
        assert meta == []

    def test_runs_table_written_on_strict_abort(self, tmp_path):
        store = _make_store(tmp_path)
        _run_pipeline(store, _make_args(strict=True), tmp_path,
                      core_result=_errored("core"))
        rows = store.query("SELECT stage, status FROM _runs")
        assert len(rows) == 1
        assert rows[0]["stage"] == "core"
        assert rows[0]["status"] == RUN_STATUS_FAILED

    def test_integrity_violation_on_clean_run_returns_1(self, tmp_path):
        from fpl_ingest.load.integrity import IntegrityViolation

        class _FailingStore(SQLiteStore):
            def run_integrity_checks(self) -> None:
                raise IntegrityViolation("synthetic violation for test")

        store = _FailingStore(tmp_path / "test.db")
        code = _run_pipeline(store, _make_args(), tmp_path)
        assert code == 1
        meta = store.query("SELECT value FROM _metadata WHERE key = 'last_successful_run_at'")
        assert meta == [], "last_successful_run_at must not be set when integrity check fails"


class TestExitCodeIntegrityViolation:
    pytestmark = pytest.mark.integration

    def test_integrity_violation_returns_exit_code_1(self, tmp_path):
        from types import SimpleNamespace
        from unittest.mock import MagicMock, patch

        from fpl_ingest.orchestration.runner import _exit_code
        from fpl_ingest.orchestration.stage_result import StageResult
        from fpl_ingest.load.integrity import IntegrityViolation

        store = SQLiteStore(tmp_path / "test.db")
        store.setup_runs_table()
        store.setup_metadata_table()

        run_started_at = "2026-05-14T08:00:00+00:00"
        store.record_run(run_started_at, "core", 10, 10, 10, 0, 0)

        stage_results = [StageResult(stage="core", fetched=10, validated=10, written=10, skipped=0)]
        core = SimpleNamespace(players=[], events=[])

        logger = MagicMock()

        with patch.object(store, "run_integrity_checks", side_effect=IntegrityViolation("orphan elements")):
            code = _exit_code(logger, stage_results, store, run_started_at, core)

        assert code == 1
        rows = store.query("SELECT status FROM _runs WHERE started_at = ?", (run_started_at,))
        assert all(r["status"] == RUN_STATUS_FAILED for r in rows)
        meta = store.query("SELECT key FROM _metadata WHERE key = 'last_successful_run_at'")
        assert meta == []


class TestStrictModeAbort:
    pytestmark = pytest.mark.integration

    def test_strict_abort_skips_subsequent_stages(self, tmp_path):
        store = _make_store(tmp_path)
        mock_fixtures = AsyncMock(return_value=StageOutcome(result=_clean("fixtures")))

        with (
            patch("fpl_ingest.orchestration.runner.AsyncFPLClient", _mock_async_fpl_client()),
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
                    store=store,
                )
            )

        assert code == 1
        mock_fixtures.assert_not_called()

    def test_prior_committed_data_persists_after_strict_abort(self, tmp_path):
        from fpl_ingest.transform.models import FixtureModel, PlayerModel, TeamModel

        store = _make_store(tmp_path)

        with store.transaction():
            setup_store(store)
            store.upsert_models("teams", TeamModel, [team_row(id=10)])
            store.upsert_models("players", PlayerModel, [player_row(id=1, team=10)])
            store.upsert_models(
                "fixtures", FixtureModel,
                [fixture_row(id=101, team_h=10, team_a=7)],
            )

        _run_pipeline(store, _make_args(strict=True), tmp_path,
                      gw_result=_errored("gameweeks"))

        players = store.query("SELECT COUNT(*) as n FROM players")[0]["n"]
        fixtures = store.query("SELECT COUNT(*) as n FROM fixtures")[0]["n"]
        assert players == 1, "player rows committed before strict abort must persist"
        assert fixtures == 1, "fixture rows committed before strict abort must persist"
