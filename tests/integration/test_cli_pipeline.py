"""CLI tests that drive the real pipeline against tmp_path.

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


class TestConcurrentPlayerFetch:

    def test_all_players_fetched(self, tmp_path):
        client = _make_async_client()
        _run([], client, tmp_path)

        called_ids = {c.args[0] for c in client.get_element_summary_raw.call_args_list}
        assert called_ids == {1, 2}

    @pytest.mark.integration
    def test_json_written_to_disk(self, tmp_path):
        client = _make_async_client()
        raw = _run([], client, tmp_path)

        for pid, expected in [(1, PLAYER_HISTORY_1), (2, PLAYER_HISTORY_2)]:
            paths = _element_summary_payload_paths(raw, pid)
            assert paths, f"element-summary/{pid} payload not written"
            assert json.loads(paths[0].read_text()) == expected

    def test_error_on_one_player_continues_others(self, tmp_path):
        async def side_effect(pid):
            if pid == 1:
                raise RuntimeError("network failure")
            return PLAYER_HISTORY_2

        client = _make_async_client(history_side_effect=side_effect)
        raw = tmp_path / "raw"
        with patch("fpl_ingest.orchestration.runner.AsyncFPLClient", return_value=client):
            try:
                main(["--raw-dir", str(raw)])
            except SystemExit:
                pass

        assert not _element_summary_payload_paths(raw, 1)
        assert _element_summary_payload_paths(raw, 2)

    def test_strict_mode_aborts_on_player_error(self, tmp_path):
        async def side_effect(pid):
            if pid == 1:
                raise RuntimeError("network failure")
            return PLAYER_HISTORY_2

        client = _make_async_client(history_side_effect=side_effect)
        raw = tmp_path / "raw"
        with patch("fpl_ingest.orchestration.runner.AsyncFPLClient", return_value=client):
            with pytest.raises(SystemExit) as exc:
                main(["--raw-dir", str(raw), "--strict"])

        assert exc.value.code == 1


class TestCliLifecycle:

    @pytest.mark.integration
    def test_stage_1_and_2_captures_persist_when_stage_3_fails(self, tmp_path):
        """Raw captures are per-object writes, not a transaction: stage 3 failing
        must not retroactively remove what stages 1 and 2 already wrote to disk."""
        raw = tmp_path / "raw"
        client = _make_async_client(bootstrap=MINIMAL_BOOTSTRAP)

        with (
            patch("fpl_ingest.orchestration.runner.AsyncFPLClient", return_value=client),
            patch("fpl_ingest.orchestration.runner.ingest_gameweeks",
                  new=AsyncMock(side_effect=RuntimeError("stage 3 fails"))),
        ):
            with pytest.raises(SystemExit) as exc:
                main(["--raw-dir", str(raw)])

        assert exc.value.code == 1
        bootstrap_payloads = sorted((raw / "fpl" / "bootstrap-static").rglob("payload.json"))
        fixtures_payloads = sorted((raw / "fpl" / "fixtures").rglob("payload.json"))
        assert bootstrap_payloads, "the core stage's raw capture must persist after stage 3 failure"
        assert fixtures_payloads, "the fixtures stage's raw capture must persist too"

    def test_closes_client_on_success(self, tmp_path):
        raw = tmp_path / "raw"
        client = FakeClient()
        core = CoreData(events=[], player_ids=[])

        with (
            patch("fpl_ingest.orchestration.runner.AsyncFPLClient", return_value=client),
            patch("fpl_ingest.orchestration.runner.ingest_core_data", new=AsyncMock(return_value=StageOutcome(result=StageResult(stage="core"), output=core))),
            patch("fpl_ingest.orchestration.runner.ingest_fixtures", new=AsyncMock(return_value=StageOutcome(result=StageResult(stage="fixtures")))),
            patch("fpl_ingest.orchestration.runner.ingest_gameweeks", new=AsyncMock(return_value=StageOutcome(result=StageResult(stage="gameweeks")))),
            patch("fpl_ingest.orchestration.runner.ingest_player_histories", new=AsyncMock(return_value=StageOutcome(result=StageResult(stage="player_histories")))),
        ):
            try:
                main(["--raw-dir", str(raw)])
            except SystemExit as exc:
                if exc.code != 0:
                    raise

        assert client.closed, "client session must be closed after successful pipeline"

    def test_closes_client_when_stage_fails(self, tmp_path):
        raw = tmp_path / "raw"
        client = FakeClient()
        core = CoreData(events=[], player_ids=[])

        with (
            patch("fpl_ingest.orchestration.runner.AsyncFPLClient", return_value=client),
            patch("fpl_ingest.orchestration.runner.ingest_core_data", new=AsyncMock(return_value=StageOutcome(result=StageResult(stage="core"), output=core))),
            patch("fpl_ingest.orchestration.runner.ingest_fixtures", new=AsyncMock(side_effect=RuntimeError("boom"))),
            patch("fpl_ingest.orchestration.runner.ingest_gameweeks", new=AsyncMock(return_value=StageOutcome(result=StageResult(stage="gameweeks")))),
            patch("fpl_ingest.orchestration.runner.ingest_player_histories", new=AsyncMock(return_value=StageOutcome(result=StageResult(stage="player_histories")))),
        ):
            with pytest.raises(SystemExit) as exc:
                main(["--raw-dir", str(raw)])

        assert exc.value.code == 1
        assert client.closed, "client session must be closed even when a stage fails"
        # An unexpected exception (as opposed to a StrictRunFailure) does not
        # finalize the raw manifest, matching pre-existing runner behaviour —
        # unrelated to this change. It's left IN_PROGRESS rather than faked.
        manifests = sorted((raw / "fpl" / "_manifests").rglob("manifest.json"))
        assert len(manifests) == 1
        manifest = json.loads(manifests[0].read_text())
        assert manifest["status"] == "IN_PROGRESS"

    def test_parser_uses_config_defaults(self, monkeypatch):
        monkeypatch.setenv("FPL_RAW_DIR", "/tmp/fpl-raw")

        parser = build_parser()
        args = parser.parse_args([])

        assert args.raw_dir is None
        assert args.rate == DEFAULT_RATE

    def test_raw_dir_honored_before_subcommand(self):
        parser = build_parser()
        args = parser.parse_args(["--raw-dir", "/tmp/x", "run"])
        assert args.raw_dir == Path("/tmp/x")

    def test_raw_dir_honored_after_subcommand(self):
        parser = build_parser()
        args = parser.parse_args(["run", "--raw-dir", "/tmp/x"])
        assert args.raw_dir == Path("/tmp/x")

    def test_raw_dir_after_subcommand_overrides_before(self):
        parser = build_parser()
        args = parser.parse_args(["--raw-dir", "/tmp/before", "run", "--raw-dir", "/tmp/after"])
        assert args.raw_dir == Path("/tmp/after")

    def test_raw_dir_before_subcommand_end_to_end(self, tmp_path):
        """Regression test for the defect where --raw-dir before the subcommand
        was silently clobbered by the run subparser's own default."""
        raw = tmp_path / "raw"
        client = FakeClient()

        with patch("fpl_ingest.orchestration.runner.AsyncFPLClient", return_value=client):
            try:
                main(["--raw-dir", str(raw), "run"])
            except SystemExit as exc:
                if exc.code != 0:
                    raise

        assert raw.exists(), "--raw-dir passed before the subcommand must be honored"

    def test_smoke_test_command_runs_without_triggering_ingestion(self, tmp_path):
        with patch("fpl_ingest.cli.execute_smoke_test") as run_smoke_test:
            run_smoke_test.return_value.endpoints_checked = ("bootstrap-static", "fixtures", "element-summary")
            run_smoke_test.return_value.sample_size = 5

            with pytest.raises(SystemExit) as exc:
                main(["--raw-dir", str(tmp_path / "raw"), "smoke-test"])

        assert exc.value.code == 0

    @pytest.mark.integration
    def test_main_runs_real_stages_with_mocked_client(self, tmp_path):
        raw = tmp_path / "raw"
        client = _make_async_client(
            bootstrap=VALID_BOOTSTRAP,
            history_side_effect=AsyncMock(return_value=PLAYER_HISTORY_1),
        )

        with patch("fpl_ingest.orchestration.runner.AsyncFPLClient", return_value=client):
            try:
                main(["--raw-dir", str(raw)])
            except SystemExit as exc:
                if exc.code != 0:
                    raise

        assert not (raw / "bootstrap.json").exists(), \
            "bootstrap-static is captured raw now, not cached as bootstrap.json"
        player_payloads = _element_summary_payload_paths(raw, 1)
        assert player_payloads
        assert json.loads(player_payloads[0].read_text()) == PLAYER_HISTORY_1

        # The run must leave exactly one finalized raw manifest, and the
        # fixtures capture must appear in it.
        manifests = sorted((raw / "fpl" / "_manifests").rglob("manifest.json"))
        assert len(manifests) == 1, manifests
        manifest = json.loads(manifests[0].read_text())
        assert manifest["source"] == "fpl"
        assert manifest["status"] in {"SUCCESS", "FAILED_PARTIAL", "FAILED"}
        assert manifest["objects"]["fixtures"]["written"] == 1
        assert manifest["objects"]["bootstrap-static"]["written"] == 1, \
            "both captures belong to the one run manifest"
        assert manifest["objects"]["element-summary/1"]["written"] == 1, \
            "element-summary capture belongs to the same run manifest too"
        assert manifest["ended_at"] is not None, "manifest must be finalized, not IN_PROGRESS"

        payloads = sorted((raw / "fpl" / "fixtures").rglob("payload.json"))
        assert len(payloads) == 1
        assert json.loads(payloads[0].read_text()) == []

        bootstrap_payloads = sorted((raw / "fpl" / "bootstrap-static").rglob("payload.json"))
        assert len(bootstrap_payloads) == 1
        assert json.loads(bootstrap_payloads[0].read_text()) == VALID_BOOTSTRAP


    def test_strict_mode_aborts_before_later_stages_execute(self, tmp_path):
        raw = tmp_path / "raw"
        client = _make_async_client()
        core_data = CoreData(events=[], player_ids=[])
        ingest_fixtures = AsyncMock(return_value=StageOutcome(result=StageResult(stage="fixtures")))
        ingest_gameweeks = AsyncMock(return_value=StageOutcome(result=StageResult(stage="gameweeks")))
        ingest_player_histories = AsyncMock(return_value=StageOutcome(result=StageResult(stage="player_histories")))

        with (
            patch("fpl_ingest.orchestration.runner.AsyncFPLClient", return_value=client),
            patch(
                "fpl_ingest.orchestration.runner.ingest_core_data",
                new=AsyncMock(return_value=StageOutcome(result=StageResult(stage="core", fetched=1, validated=0, written=0, skipped=1), output=core_data)),
            ),
            patch("fpl_ingest.orchestration.runner.ingest_fixtures", new=ingest_fixtures),
            patch("fpl_ingest.orchestration.runner.ingest_gameweeks", new=ingest_gameweeks),
            patch("fpl_ingest.orchestration.runner.ingest_player_histories", new=ingest_player_histories),
        ):
            with pytest.raises(SystemExit) as exc:
                main(["--raw-dir", str(raw), "--strict"])

        assert exc.value.code == 1
        ingest_fixtures.assert_not_called()
        ingest_gameweeks.assert_not_called()
        ingest_player_histories.assert_not_called()


class TestRateLimiting:

    def test_rate_above_max_safe_is_clamped_and_warned(self):
        logger = MagicMock()
        applied_rate = _resolve_applied_rate(logger, MAX_RATE + 10)
        assert applied_rate == MAX_RATE
        logger.warning.assert_called_once()
        assert "requested_rate" in logger.warning.call_args.args[0]

    def test_pipeline_uses_clamped_rate_for_token_bucket(self, tmp_path):
        raw = tmp_path / "raw"
        client = _make_async_client()
        applied_rates: list[float] = []

        _OriginalTBL = TokenBucketLimiter

        class CapturingLimiter(_OriginalTBL):
            def __init__(self, *, rate: float, **kwargs):  # type: ignore[override]
                super().__init__(rate=rate, **kwargs)
                applied_rates.append(self.rate)

        with (
            patch("fpl_ingest.orchestration.runner.AsyncFPLClient", return_value=client),
            patch("fpl_ingest.orchestration.runner.TokenBucketLimiter", CapturingLimiter),
            patch(
                "fpl_ingest.orchestration.runner.ingest_core_data",
                new=AsyncMock(return_value=StageOutcome(result=StageResult(stage="core"), output=CoreData(events=[], player_ids=[]))),
            ),
            patch("fpl_ingest.orchestration.runner.ingest_fixtures", new=AsyncMock(return_value=StageOutcome(result=StageResult(stage="fixtures")))),
            patch("fpl_ingest.orchestration.runner.ingest_gameweeks", new=AsyncMock(return_value=StageOutcome(result=StageResult(stage="gameweeks")))),
            patch("fpl_ingest.orchestration.runner.ingest_player_histories", new=AsyncMock(return_value=StageOutcome(result=StageResult(stage="player_histories")))),
        ):
            with pytest.raises(SystemExit) as exc:
                main(["--raw-dir", str(raw), "--rate", "99"])

        assert exc.value.code == 0
        assert applied_rates, "TokenBucketLimiter must be instantiated during the pipeline"
        # Observe the effective rate on the actual limiter instance, not the constructor arg.
        assert applied_rates[0] == MAX_RATE, \
            f"effective rate {applied_rates[0]} must equal MAX_RATE {MAX_RATE} when --rate 99 is passed"
