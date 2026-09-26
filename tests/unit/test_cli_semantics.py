"""CLI run-status and exit-code semantics.

Split out of the pre-migration ``tests/test_cli.py``; bodies unchanged.
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from fpl_ingest.cli import (
    DEFAULT_RATE,
    MAX_RATE,
    build_parser,
    main,
)
from fpl_ingest.extract.http.local_writer import LocalRawWriter
from fpl_ingest.extract.http.rate_limiter import TokenBucketLimiter
from fpl_ingest.orchestration.run_status import RUN_STATUS_FAILED, RUN_STATUS_PARTIAL, RUN_STATUS_SUCCESS
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


def _writer(tmp_path: Path) -> LocalRawWriter:
    return LocalRawWriter(tmp_path, "fpl")


def _capture(writer: LocalRawWriter, endpoint: str, *, usable: bool = True) -> None:
    now = datetime.now(timezone.utc)
    writer.write_object(
        endpoint, b"{}", request_url="https://example.test/", requested_at=now, received_at=now,
        http_status=200, shape_validation={"ok": usable, "failures": [] if usable else ["top_level_is_list: got dict"]},
    )


def _fetch_failure(writer: LocalRawWriter, endpoint: str) -> None:
    writer.record_failure(endpoint, request_url="https://example.test/", error_class="FPLClientError")


class TestRunSuccessSemantics:

    def test_exit_code_success_requires_every_attempted_capture_usable(self, tmp_path):
        logger = MagicMock()
        writer = _writer(tmp_path)
        _capture(writer, "bootstrap-static")

        exit_code = _exit_code(
            logger,
            writer,
            [StageResult(stage="core", fetched=1, validated=1, written=1, skipped=0, errors=0)],
        )

        assert exit_code == 0

    def test_exit_code_fails_when_a_capture_fails_its_shape_check_even_without_errors(self, tmp_path):
        logger = MagicMock()
        writer = _writer(tmp_path)
        _capture(writer, "element-summary/1")
        _capture(writer, "element-summary/2", usable=False)
        _capture(writer, "element-summary/3", usable=False)

        exit_code = _exit_code(
            logger,
            writer,
            [StageResult(stage="player_histories", fetched=3, validated=1, written=1, skipped=2, errors=0)],
        )

        assert exit_code == 1
        assert logger.info.call_args_list[0].args[0] == "[run] status=%s total_fetched=%d total_validated=%d total_written=%d total_skipped=%d total_errors=%d"
        assert logger.info.call_args_list[0].args[1:] == (RUN_STATUS_PARTIAL, 3, 1, 1, 2, 0)

    def test_exit_code_fails_when_errors_exist(self, tmp_path):
        logger = MagicMock()
        writer = _writer(tmp_path)
        _capture(writer, "event-status")
        _fetch_failure(writer, "bootstrap-static")

        exit_code = _exit_code(
            logger,
            writer,
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

        async def core(client, raw_writer, **kwargs):
            # Record the shape failure the result reports: run status comes from the writer.
            _capture(raw_writer, "bootstrap-static", usable=False)
            return StageOutcome(result=core_stage, output=core_data)
        argv = ["--raw-dir", str(raw)]
        if strict:
            argv.append("--strict")

        with (
            patch("fpl_ingest.orchestration.runner.AsyncFPLClient", return_value=client),
            patch("fpl_ingest.orchestration.runner.ingest_core_data", new=AsyncMock(side_effect=core)),
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

    def test_failed_run_exit_code_is_nonzero(self, tmp_path):
        logger = MagicMock()
        writer = _writer(tmp_path)
        _fetch_failure(writer, "bootstrap-static")

        exit_code = _exit_code(
            logger,
            writer,
            [StageResult(stage="core", fetched=1, validated=1, written=1, skipped=0, errors=1)],
        )

        assert exit_code == 1

    def test_final_run_status_classification(self, tmp_path):
        def status(*records) -> str:
            writer = LocalRawWriter(tmp_path / str(len(list(tmp_path.iterdir()))), "fpl")
            for record in records:
                record(writer)
            return classify_run(writer.endpoint_outcomes)

        usable = lambda w: _capture(w, "bootstrap-static")  # noqa: E731
        shape_failed = lambda w: _capture(w, "fixtures", usable=False)  # noqa: E731
        fetch_failed = lambda w: _fetch_failure(w, "fixtures")  # noqa: E731

        assert status(usable) == RUN_STATUS_SUCCESS
        assert status(usable, fetch_failed) == RUN_STATUS_PARTIAL
        assert status(usable, shape_failed) == RUN_STATUS_PARTIAL
        assert status(fetch_failed) == RUN_STATUS_FAILED
        assert status(shape_failed) == RUN_STATUS_FAILED
        assert status() == RUN_STATUS_FAILED
