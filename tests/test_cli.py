"""Tests for the CLI entry point and async pipeline stage integration."""

from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

pytestmark = pytest.mark.unit

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
from tests.factories import event_row, player_row, team_row

MINIMAL_BOOTSTRAP = {
    "events": [],
    "elements": [
        player_row(id=1, team=11, element_type=3, now_cost=130),
        player_row(id=2, first_name="Erling", second_name="Haaland", web_name="Haaland",
                   team=13, team_code=43, element_type=4, now_cost=145, code=223094,
                   form_rank=2, form_rank_type=1, points_per_game_rank=2,
                   points_per_game_rank_type=1, influence_rank=4, influence_rank_type=2,
                   creativity_rank=80, creativity_rank_type=20,
                   threat_rank=1, threat_rank_type=1,
                   ict_index_rank=3, ict_index_rank_type=2),
    ],
    "teams": [
        team_row(id=11, name="Liverpool", short_name="LIV", code=14),
        team_row(id=13, name="Man City", short_name="MCI", code=43, position=2),
    ],
    "element_types": [],
    "phases": [],
}

PLAYER_HISTORY_1 = {
    "history": [{"element": 1, "round": 1, "fixture": 11, "minutes": 90, "total_points": 2}],
    "fixtures": [],
    "history_past": [],
}
PLAYER_HISTORY_2 = {
    "history": [{"element": 2, "round": 1, "fixture": 22, "minutes": 90, "total_points": 5}],
    "fixtures": [],
    "history_past": [],
}


def _element_summary_payload_paths(raw: Path, pid: int) -> list[Path]:
    return sorted((raw / "fpl" / "element-summary" / str(pid)).rglob("payload.json"))

VALID_BOOTSTRAP = {
    "events": [],
    "elements": [player_row(id=1, web_name="Salah", team=11, element_type=3, now_cost=130)],
    "teams": [],
    "element_types": [],
}



def _raw_response(url: str, payload):
    """A RawResponse a capture stage can write, for client stubs."""
    from datetime import datetime, timezone

    from fpl_ingest.extract.http.client import RawResponse

    now = datetime.now(timezone.utc)
    return RawResponse(
        url=url,
        status=200,
        headers={"content-type": "application/json"},
        body=json.dumps(payload).encode("utf-8"),
        requested_at=now,
        received_at=now,
    )


def _raw_fixtures_response(payload=()):
    return _raw_response("https://fantasy.premierleague.com/api/fixtures/", list(payload))


def _raw_event_status_response(payload=None):
    return _raw_response(
        "https://fantasy.premierleague.com/api/event-status/",
        payload if payload is not None else {"status": [], "leagues": ""},
    )


def _raw_bootstrap_response(payload=None):
    return _raw_response(
        "https://fantasy.premierleague.com/api/bootstrap-static/",
        MINIMAL_BOOTSTRAP if payload is None else payload,
    )


def _make_async_client(bootstrap=MINIMAL_BOOTSTRAP, history_side_effect=None):
    client = AsyncMock()
    client.__aenter__ = AsyncMock(return_value=client)
    client.__aexit__ = AsyncMock(return_value=None)
    client.get_bootstrap = AsyncMock(return_value=bootstrap)
    client.get_bootstrap_raw = AsyncMock(return_value=_raw_bootstrap_response(bootstrap))
    client.get_fixtures = AsyncMock(return_value=[])
    client.get_fixtures_raw = AsyncMock(return_value=_raw_fixtures_response())
    client.get_event_status_raw = AsyncMock(return_value=_raw_event_status_response())
    client.get_gw = AsyncMock(return_value=None)

    async def _fetch_raw(pid):
        if history_side_effect is not None:
            payload = await history_side_effect(pid)
        else:
            payload = PLAYER_HISTORY_1 if pid == 1 else PLAYER_HISTORY_2
        return _raw_response(
            f"https://fantasy.premierleague.com/api/element-summary/{pid}/", payload
        )

    client.get_element_summary_raw = AsyncMock(side_effect=_fetch_raw)
    return client


def _run(argv: list[str], mock_client, tmp_path) -> Path:
    raw = tmp_path / "raw"
    db = tmp_path / "test.db"
    with patch("fpl_ingest.orchestration.runner.AsyncFPLClient", return_value=mock_client):
        try:
            main(["--db", str(db), "--raw-dir", str(raw)] + argv)
        except SystemExit as exc:
            if exc.code != 0:
                raise
    return raw


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
        db = tmp_path / "test.db"
        with patch("fpl_ingest.orchestration.runner.AsyncFPLClient", return_value=client):
            try:
                main(["--db", str(db), "--raw-dir", str(raw)])
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
        db = tmp_path / "test.db"
        with patch("fpl_ingest.orchestration.runner.AsyncFPLClient", return_value=client):
            with pytest.raises(SystemExit) as exc:
                main(["--db", str(db), "--raw-dir", str(raw), "--strict"])

        assert exc.value.code == 1


class FakeClient:
    """Minimal async-context-manager client that tracks whether it was closed."""

    def __init__(self):
        self.closed = False
        _empty_bootstrap = {
            "events": [], "elements": [], "teams": [], "element_types": [], "phases": [],
        }
        self.get_bootstrap = AsyncMock(return_value=_empty_bootstrap)
        self.get_bootstrap_raw = AsyncMock(
            return_value=_raw_bootstrap_response(_empty_bootstrap)
        )
        self.get_fixtures = AsyncMock(return_value=[])
        self.get_fixtures_raw = AsyncMock(return_value=_raw_fixtures_response())
        self.get_event_status_raw = AsyncMock(return_value=_raw_event_status_response())
        self.get_gw = AsyncMock(return_value=None)
        self.get_element_summary_raw = AsyncMock(
            side_effect=lambda pid: _raw_response(
                f"https://fantasy.premierleague.com/api/element-summary/{pid}/",
                {"history": [], "fixtures": [], "history_past": []},
            )
        )

    async def __aenter__(self):
        return self

    async def __aexit__(self, *args):
        self.closed = True


class TestCliLifecycle:

    @pytest.mark.integration
    def test_stage_1_and_2_captures_persist_when_stage_3_fails(self, tmp_path):
        """Raw captures are per-object writes, not a transaction: stage 3 failing
        must not retroactively remove what stages 1 and 2 already wrote to disk."""
        raw = tmp_path / "raw"
        db = tmp_path / "test.db"
        client = _make_async_client(bootstrap=MINIMAL_BOOTSTRAP)

        with (
            patch("fpl_ingest.orchestration.runner.AsyncFPLClient", return_value=client),
            patch("fpl_ingest.orchestration.runner.ingest_gameweeks",
                  new=AsyncMock(side_effect=RuntimeError("stage 3 fails"))),
        ):
            with pytest.raises(SystemExit) as exc:
                main(["--db", str(db), "--raw-dir", str(raw)])

        assert exc.value.code == 1
        bootstrap_payloads = sorted((raw / "fpl" / "bootstrap-static").rglob("payload.json"))
        fixtures_payloads = sorted((raw / "fpl" / "fixtures").rglob("payload.json"))
        assert bootstrap_payloads, "the core stage's raw capture must persist after stage 3 failure"
        assert fixtures_payloads, "the fixtures stage's raw capture must persist too"

    def test_closes_client_on_success(self, tmp_path):
        raw = tmp_path / "raw"
        db = tmp_path / "test.db"
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
                main(["--db", str(db), "--raw-dir", str(raw)])
            except SystemExit as exc:
                if exc.code != 0:
                    raise

        assert client.closed, "client session must be closed after successful pipeline"

    def test_closes_client_when_stage_fails(self, tmp_path):
        raw = tmp_path / "raw"
        db = tmp_path / "test.db"
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
                main(["--db", str(db), "--raw-dir", str(raw)])

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
        monkeypatch.setenv("FPL_DB_PATH", "/tmp/fpl-test.db")
        monkeypatch.setenv("FPL_RAW_DIR", "/tmp/fpl-raw")

        parser = build_parser()
        args = parser.parse_args([])

        assert args.db is None
        assert args.raw_dir is None
        assert args.rate == DEFAULT_RATE

    def test_smoke_test_command_runs_without_triggering_ingestion(self, tmp_path):
        db = tmp_path / "test.db"

        with patch("fpl_ingest.cli.execute_smoke_test") as run_smoke_test:
            run_smoke_test.return_value.endpoints_checked = ("bootstrap-static", "fixtures", "element-summary")
            run_smoke_test.return_value.sample_size = 5

            with pytest.raises(SystemExit) as exc:
                main(["--db", str(db), "--raw-dir", str(tmp_path / "raw"), "smoke-test"])

        assert exc.value.code == 0
        # Prove no ingestion occurred: the DB is never touched at all now that
        # the run pipeline no longer opens a store.
        assert not db.exists()

    @pytest.mark.integration
    def test_main_runs_real_stages_with_mocked_client(self, tmp_path):
        raw = tmp_path / "raw"
        db = tmp_path / "test.db"
        client = _make_async_client(
            bootstrap=VALID_BOOTSTRAP,
            history_side_effect=AsyncMock(return_value=PLAYER_HISTORY_1),
        )

        with patch("fpl_ingest.orchestration.runner.AsyncFPLClient", return_value=client):
            try:
                main(["--db", str(db), "--raw-dir", str(raw)])
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
        db = tmp_path / "test.db"
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
                main(["--db", str(db), "--raw-dir", str(raw), "--strict"])

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
        db = tmp_path / "test.db"
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
                main(["--db", str(db), "--raw-dir", str(raw), "--rate", "99"])

        assert exc.value.code == 0
        assert applied_rates, "TokenBucketLimiter must be instantiated during the pipeline"
        # Observe the effective rate on the actual limiter instance, not the constructor arg.
        assert applied_rates[0] == MAX_RATE, \
            f"effective rate {applied_rates[0]} must equal MAX_RATE {MAX_RATE} when --rate 99 is passed"


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
        db = tmp_path / "test.db"
        client = _make_async_client()
        core_data = CoreData(events=[], player_ids=[])
        core_stage = StageResult(stage="core", fetched=1, validated=0, written=0, skipped=1, errors=0)
        argv = ["--db", str(db), "--raw-dir", str(raw)]
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
