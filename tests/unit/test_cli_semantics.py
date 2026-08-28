"""CLI run-status and exit-code semantics.

Split out of the pre-migration ``tests/test_cli.py``; bodies unchanged.
"""

from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from fpl_ingest.cli import (
    DEFAULT_RATE,
    MAX_RATE,
    build_parser,
    main,
)
from fpl_ingest.extract.http.rate_limiter import TokenBucketLimiter
from fpl_ingest.orchestration.run_status import RUN_STATUS_FAILED, RUN_STATUS_FAILED_PARTIAL, RUN_STATUS_SUCCESS
from fpl_ingest.orchestration.stage_result import StageOutcome, StageResult
from fpl_ingest.orchestration.runner import _exit_code, _log_fail_fast_failure, _resolve_applied_rate
from fpl_ingest.orchestration.run_status import classify_run
from fpl_ingest.extract.stages.bootstrap import CoreData
from tests.support.cli_fakes import (
    FakeClient,
    MINIMAL_BOOTSTRAP,
    PLAYER_HISTORY_1,
    PLAYER_HISTORY_2,
    VALID_BOOTSTRAP,
    _element_summary_payload_paths,
    _make_async_client,
    _run,
)


class TestRunSuccessSemantics:

    def test_exit_code_success_requires_zero_errors_and_zero_skipped(self):
        logger = MagicMock()

        exit_code = _exit_code(
            logger,
            [StageResult(stage="core", fetched=1, validated=1, written=1, skipped=0, errors=0)],
        )

        assert exit_code == 0

    def test_exit_code_fails_when_skipped_rows_exist_even_without_errors(self):
        logger = MagicMock()

        exit_code = _exit_code(
            logger,
            [StageResult(stage="core", fetched=3, validated=1, written=1, skipped=2, errors=0)],
        )

        assert exit_code == 1
        assert logger.info.call_args_list[0].args[0] == "[run] status=%s total_fetched=%d total_validated=%d total_written=%d total_skipped=%d total_errors=%d"
        assert logger.info.call_args_list[0].args[1:] == (RUN_STATUS_FAILED_PARTIAL, 3, 1, 1, 2, 0)

    def test_exit_code_fails_when_errors_exist(self):
        logger = MagicMock()

        exit_code = _exit_code(
            logger,
            [StageResult(stage="core", fetched=1, validated=1, written=1, skipped=0, errors=1)],
        )

        assert exit_code == 1
        assert "run failed" in logger.warning.call_args.args[0]

    @pytest.mark.parametrize("strict", [True, False])
    def test_exits_non_zero_when_stage_reports_skipped_rows(self, tmp_path, strict):
        raw = tmp_path / "raw"
        client = _make_async_client()
        core_data = CoreData(events=[], player_ids=[])
        core_stage = StageResult(stage="core", fetched=1, validated=0, written=0, skipped=1, errors=0)
        argv = ["--raw-dir", str(raw)]
        if strict:
            argv.append("--strict")

        with (
            patch("fpl_ingest.orchestration.runner.AsyncFPLClient", return_value=client),
            patch("fpl_ingest.orchestration.runner.ingest_core_data", new=AsyncMock(return_value=StageOutcome(result=core_stage, output=core_data))),
            patch("fpl_ingest.orchestration.runner.ingest_fixtures", new=AsyncMock(return_value=StageOutcome(result=StageResult(stage="fixtures")))),
            patch("fpl_ingest.orchestration.runner.ingest_gameweeks", new=AsyncMock(return_value=StageOutcome(result=StageResult(stage="gameweeks")))),
            patch("fpl_ingest.orchestration.runner.ingest_player_histories", new=AsyncMock(return_value=StageOutcome(result=StageResult(stage="player_histories")))),
        ):
            with pytest.raises(SystemExit) as exc:
                main(argv)

        assert exc.value.code == 1

    def test_fail_fast_logging_includes_mode_reason_and_stage(self):
        logger = MagicMock()

        _log_fail_fast_failure(logger, StageResult(stage="core", fetched=4, validated=3, written=3, skipped=1, errors=0))

        error_message = logger.error.call_args_list[0].args[0]
        assert "Run failed fast:" in error_message
        assert "failure_reason=%s" in error_message
        assert "failed_stage=%s" in error_message
        assert logger.error.call_args_list[0].args[1:] == ("skipped_records", "core", 4, 3, 3, 1, 0)
        assert "run failed" in logger.warning.call_args.args[0]

    def test_failed_run_exit_code_is_nonzero(self):
        logger = MagicMock()

        exit_code = _exit_code(
            logger,
            [StageResult(stage="core", fetched=1, validated=1, written=1, skipped=0, errors=1)],
        )

        assert exit_code == 1

    def test_final_run_status_classification(self):
        assert classify_run(errors=0, skipped=0, strict_mode=False) == RUN_STATUS_SUCCESS
        assert classify_run(errors=1, skipped=0, strict_mode=False) == RUN_STATUS_FAILED
        assert classify_run(errors=0, skipped=1, strict_mode=False) == RUN_STATUS_FAILED_PARTIAL
        assert classify_run(errors=0, skipped=1, strict_mode=True) == RUN_STATUS_FAILED
