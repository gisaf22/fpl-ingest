"""Tests for the CLI entry point and async pipeline stage integration."""

from __future__ import annotations

import json
from contextlib import contextmanager
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

pytestmark = pytest.mark.unit

import sqlite3

from fpl_ingest.cli import (
    DEFAULT_RATE,
    MAX_RATE,
    build_parser,
    main,
)
from fpl_ingest.extract.http.rate_limiter import TokenBucketLimiter
from fpl_ingest.load.store import SQLiteStore
from fpl_ingest.orchestration.run_status import RUN_STATUS_FAILED, RUN_STATUS_FAILED_PARTIAL, RUN_STATUS_SUCCESS
from fpl_ingest.orchestration.stage_result import StageOutcome, StageResult
from fpl_ingest.orchestration.runner import _exit_code, _log_fail_fast_failure, _resolve_applied_rate
from fpl_ingest.orchestration.run_status import classify_run
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

PLAYER_HISTORY_1 = {"history": [{"element_id": 1, "round": 1, "fixture": 11}], "history_past": []}
PLAYER_HISTORY_2 = {"history": [{"element_id": 2, "round": 1, "fixture": 22}], "history_past": []}

VALID_BOOTSTRAP = {
    "events": [],
    "elements": [player_row(id=1, web_name="Salah", team=11, element_type=3, now_cost=130)],
    "teams": [],
    "element_types": [],
}


def _make_async_client(bootstrap=MINIMAL_BOOTSTRAP, history_side_effect=None):
    client = AsyncMock()
    client.__aenter__ = AsyncMock(return_value=client)
    client.__aexit__ = AsyncMock(return_value=None)
    client.get_bootstrap = AsyncMock(return_value=bootstrap)
    client.get_fixtures = AsyncMock(return_value=[])
    client.get_gw = AsyncMock(return_value=None)
    if history_side_effect is not None:
        client.get_player_history = AsyncMock(side_effect=history_side_effect)
    else:
        client.get_player_history = AsyncMock(
            side_effect=lambda pid: PLAYER_HISTORY_1 if pid == 1 else PLAYER_HISTORY_2
        )
    return client


def _make_mock_store():
    store = MagicMock()
    store.upsert_models.return_value = (0, 0)
    return store


def _run(argv: list[str], mock_client, mock_store, tmp_path) -> Path:
    raw = tmp_path / "raw"
    db = tmp_path / "test.db"
    with (
        patch("fpl_ingest.orchestration.runner.AsyncFPLClient", return_value=mock_client),
        patch("fpl_ingest.cli.SQLiteStore", return_value=mock_store),
    ):
        try:
            main(["--db", str(db), "--raw-dir", str(raw)] + argv)
        except SystemExit as exc:
            if exc.code != 0:
                raise
    return raw


class TestConcurrentPlayerFetch:

    def test_all_players_fetched(self, tmp_path):
        client = _make_async_client()
        store = _make_mock_store()
        _run([], client, store, tmp_path)

        called_ids = {c.args[0] for c in client.get_player_history.call_args_list}
        assert called_ids == {1, 2}

    @pytest.mark.integration
    def test_json_written_to_disk(self, tmp_path):
        client = _make_async_client()
        store = _make_mock_store()
        raw = _run([], client, store, tmp_path)

        for pid, expected in [(1, PLAYER_HISTORY_1), (2, PLAYER_HISTORY_2)]:
            path = raw / "players" / f"{pid}.json"
            assert path.exists(), f"players/{pid}.json not written"
            assert json.loads(path.read_text()) == expected

    def test_error_on_one_player_continues_others(self, tmp_path):
        async def side_effect(pid):
            if pid == 1:
                raise RuntimeError("network failure")
            return PLAYER_HISTORY_2

        client = _make_async_client(history_side_effect=side_effect)
        store = _make_mock_store()
        raw = tmp_path / "raw"
        db = tmp_path / "test.db"
        with (
            patch("fpl_ingest.orchestration.runner.AsyncFPLClient", return_value=client),
            patch("fpl_ingest.cli.SQLiteStore", return_value=store),
        ):
            try:
                main(["--db", str(db), "--raw-dir", str(raw)])
            except SystemExit:
                pass

        assert not (raw / "players" / "1.json").exists()
        assert (raw / "players" / "2.json").exists()

    def test_strict_mode_aborts_on_player_error(self, tmp_path):
        async def side_effect(pid):
            if pid == 1:
                raise RuntimeError("network failure")
            return PLAYER_HISTORY_2

        client = _make_async_client(history_side_effect=side_effect)
        store = _make_mock_store()
        raw = tmp_path / "raw"
        db = tmp_path / "test.db"
        with (
            patch("fpl_ingest.orchestration.runner.AsyncFPLClient", return_value=client),
            patch("fpl_ingest.cli.SQLiteStore", return_value=store),
        ):
            with pytest.raises(SystemExit) as exc:
                main(["--db", str(db), "--raw-dir", str(raw), "--strict"])

        assert exc.value.code == 1


class FakeClient:
    """Minimal async-context-manager client that tracks whether it was closed."""

    def __init__(self):
        self.closed = False
        self.get_bootstrap = AsyncMock(return_value={
            "events": [], "elements": [], "teams": [], "element_types": [], "phases": [],
        })
        self.get_fixtures = AsyncMock(return_value=[])
        self.get_gw = AsyncMock(return_value=None)
        self.get_player_history = AsyncMock(return_value={"history": [], "history_past": []})

    async def __aenter__(self):
        return self

    async def __aexit__(self, *args):
        self.closed = True


class RecordingStore:
    def __init__(self) -> None:
        self.transaction_entries = 0
        self.metadata_updates: list[tuple[str, str]] = []
        self.run_status_updates: list[tuple[str, str]] = []

    @contextmanager
    def transaction(self):
        self.transaction_entries += 1
        yield

    def record_run(self, *args, **kwargs) -> None:
        pass

    def record_stage_result(self, started_at: str, result: StageResult) -> None:
        self.record_run(started_at, result.stage, result.fetched, result.validated, result.written, result.skipped, result.errors)

    def record_stage_lineage(self, *args, **kwargs) -> None:
        pass

    def finalize_run(self, started_at: str, status: str | None = None, *, metadata_updates=None, **kwargs) -> str:
        status = status or classify_run(
            errors=kwargs.get("errors", 0),
            skipped=kwargs.get("skipped", 0),
            strict_mode=kwargs.get("strict_mode", False),
        )
        self.run_status_updates.append((started_at, status))
        for key, value in (metadata_updates or {}).items():
            self.set_metadata(key, value)
        return status

    def set_metadata(self, key: str, value: str) -> None:
        self.metadata_updates.append((key, value))


class IntegrationStore(RecordingStore):
    def __init__(self) -> None:
        super().__init__()
        self.register_contract_table = MagicMock()
        self.upsert_models = MagicMock(return_value=(0, 0))
        self.setup_runs_table = MagicMock()
        self.setup_metadata_table = MagicMock()


class TestCliLifecycle:

    @pytest.mark.integration
    def test_uses_separate_transactions_per_stage(self, tmp_path):
        """Each stage commits independently: data from stages 1+2 persists even when stage 3 fails."""
        raw = tmp_path / "raw"
        db = tmp_path / "test.db"
        store = SQLiteStore(db)
        client = _make_async_client(bootstrap=MINIMAL_BOOTSTRAP)

        with (
            patch("fpl_ingest.orchestration.runner.AsyncFPLClient", return_value=client),
            patch("fpl_ingest.cli.SQLiteStore", return_value=store),
            patch("fpl_ingest.orchestration.runner.ingest_gameweeks",
                  new=AsyncMock(side_effect=RuntimeError("stage 3 fails"))),
        ):
            with pytest.raises(SystemExit) as exc:
                main(["--db", str(db), "--raw-dir", str(raw)])

        assert exc.value.code == 1
        # Stages 1 (core: players, teams) and 2 (fixtures) committed before stage 3 failed.
        players_n = store.query("SELECT COUNT(*) as n FROM players")[0]["n"]
        assert players_n == 2, "player rows from stage 1 must persist after stage 3 failure"

    def test_closes_client_on_success(self, tmp_path):
        raw = tmp_path / "raw"
        db = tmp_path / "test.db"
        client = FakeClient()
        store = RecordingStore()
        core = SimpleNamespace(events=[], players=[])

        with (
            patch("fpl_ingest.orchestration.runner.AsyncFPLClient", return_value=client),
            patch("fpl_ingest.cli.SQLiteStore", return_value=store),
            patch("fpl_ingest.orchestration.runner.setup_store"),
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
        store = RecordingStore()
        core = SimpleNamespace(events=[], players=[])

        with (
            patch("fpl_ingest.orchestration.runner.AsyncFPLClient", return_value=client),
            patch("fpl_ingest.cli.SQLiteStore", return_value=store),
            patch("fpl_ingest.orchestration.runner.setup_store"),
            patch("fpl_ingest.orchestration.runner.ingest_core_data", new=AsyncMock(return_value=StageOutcome(result=StageResult(stage="core"), output=core))),
            patch("fpl_ingest.orchestration.runner.ingest_fixtures", new=AsyncMock(side_effect=RuntimeError("boom"))),
            patch("fpl_ingest.orchestration.runner.ingest_gameweeks", new=AsyncMock(return_value=StageOutcome(result=StageResult(stage="gameweeks")))),
            patch("fpl_ingest.orchestration.runner.ingest_player_histories", new=AsyncMock(return_value=StageOutcome(result=StageResult(stage="player_histories")))),
        ):
            with pytest.raises(SystemExit) as exc:
                main(["--db", str(db), "--raw-dir", str(raw)])

        assert exc.value.code == 1
        assert client.closed, "client session must be closed even when a stage fails"
        assert store.run_status_updates[-1][1] == RUN_STATUS_FAILED

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
        # Prove no ingestion occurred: none of the data tables exist in the DB.
        ingestion_tables = {"players", "teams", "fixtures", "gameweeks", "player_histories"}
        if db.exists():
            conn = sqlite3.connect(db)
            existing = {r[0] for r in conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table'"
            ).fetchall()}
            conn.close()
            assert not existing & ingestion_tables, \
                f"smoke-test must not write to ingestion tables; found: {existing & ingestion_tables}"

    @pytest.mark.integration
    def test_main_runs_real_stages_with_mocked_client_and_store(self, tmp_path):
        raw = tmp_path / "raw"
        db = tmp_path / "test.db"
        client = _make_async_client(
            bootstrap=VALID_BOOTSTRAP,
            history_side_effect=AsyncMock(return_value=PLAYER_HISTORY_1),
        )
        store = IntegrationStore()

        with (
            patch("fpl_ingest.orchestration.runner.AsyncFPLClient", return_value=client),
            patch("fpl_ingest.cli.SQLiteStore", return_value=store),
        ):
            try:
                main(["--db", str(db), "--raw-dir", str(raw)])
            except SystemExit as exc:
                if exc.code != 0:
                    raise

        assert (raw / "bootstrap.json").exists()
        assert json.loads((raw / "bootstrap.json").read_text()) == VALID_BOOTSTRAP
        assert (raw / "players" / "1.json").exists()
        assert json.loads((raw / "players" / "1.json").read_text()) == PLAYER_HISTORY_1
        assert store.register_contract_table.call_count > 0

        upserted_tables = [call.args[0] for call in store.upsert_models.call_args_list]
        assert "players" in upserted_tables
        assert "player_histories" in upserted_tables


    def test_strict_mode_aborts_before_later_stages_execute(self, tmp_path):
        raw = tmp_path / "raw"
        db = tmp_path / "test.db"
        client = _make_async_client()
        store = RecordingStore()
        core_data = SimpleNamespace(events=[], players=[])
        ingest_fixtures = AsyncMock(return_value=StageOutcome(result=StageResult(stage="fixtures")))
        ingest_gameweeks = AsyncMock(return_value=StageOutcome(result=StageResult(stage="gameweeks")))
        ingest_player_histories = AsyncMock(return_value=StageOutcome(result=StageResult(stage="player_histories")))

        with (
            patch("fpl_ingest.orchestration.runner.AsyncFPLClient", return_value=client),
            patch("fpl_ingest.cli.SQLiteStore", return_value=store),
            patch("fpl_ingest.orchestration.runner.setup_store"),
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
        store = RecordingStore()
        applied_rates: list[float] = []

        _OriginalTBL = TokenBucketLimiter

        class CapturingLimiter(_OriginalTBL):
            def __init__(self, *, rate: float, **kwargs):  # type: ignore[override]
                super().__init__(rate=rate, **kwargs)
                applied_rates.append(self.rate)

        with (
            patch("fpl_ingest.orchestration.runner.AsyncFPLClient", return_value=client),
            patch("fpl_ingest.cli.SQLiteStore", return_value=store),
            patch("fpl_ingest.orchestration.runner.TokenBucketLimiter", CapturingLimiter),
            patch("fpl_ingest.orchestration.runner.setup_store"),
            patch(
                "fpl_ingest.orchestration.runner.ingest_core_data",
                new=AsyncMock(return_value=StageOutcome(result=StageResult(stage="core"), output=SimpleNamespace(events=[], players=[]))),
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
        store = RecordingStore()
        core = SimpleNamespace(events=[], players=[SimpleNamespace(id=1)])

        exit_code = _exit_code(
            logger,
            [StageResult(stage="core", fetched=1, validated=1, written=1, skipped=0, errors=0)],
            store,
            "2026-04-21T00:00:00+00:00",
            core,
        )

        assert exit_code == 0
        assert ("last_successful_run_at", "2026-04-21T00:00:00+00:00") in store.metadata_updates
        assert ("2026-04-21T00:00:00+00:00", RUN_STATUS_SUCCESS) in store.run_status_updates

    def test_exit_code_fails_when_skipped_rows_exist_even_without_errors(self):
        logger = MagicMock()
        store = RecordingStore()
        core = SimpleNamespace(events=[], players=[SimpleNamespace(id=1)])

        exit_code = _exit_code(
            logger,
            [StageResult(stage="core", fetched=3, validated=1, written=1, skipped=2, errors=0)],
            store,
            "2026-04-21T00:00:00+00:00",
            core,
        )

        assert exit_code == 1
        assert store.metadata_updates == []
        assert ("2026-04-21T00:00:00+00:00", RUN_STATUS_FAILED_PARTIAL) in store.run_status_updates
        assert logger.info.call_args_list[0].args[0] == "[run] status=%s total_fetched=%d total_validated=%d total_written=%d total_skipped=%d total_errors=%d"
        assert logger.info.call_args_list[0].args[1:] == (RUN_STATUS_FAILED_PARTIAL, 3, 1, 1, 2, 0)

    def test_exit_code_fails_when_errors_exist(self):
        logger = MagicMock()
        store = RecordingStore()
        core = SimpleNamespace(events=[], players=[SimpleNamespace(id=1)])

        exit_code = _exit_code(
            logger,
            [StageResult(stage="core", fetched=1, validated=1, written=1, skipped=0, errors=1)],
            store,
            "2026-04-21T00:00:00+00:00",
            core,
        )

        assert exit_code == 1
        assert store.metadata_updates == []
        assert ("2026-04-21T00:00:00+00:00", RUN_STATUS_FAILED) in store.run_status_updates
        assert "run failed" in logger.warning.call_args.args[0]

    @pytest.mark.parametrize("strict", [True, False])
    def test_exits_non_zero_when_stage_reports_skipped_rows(self, tmp_path, strict):
        raw = tmp_path / "raw"
        db = tmp_path / "test.db"
        client = _make_async_client()
        store = RecordingStore()
        core_data = SimpleNamespace(events=[], players=[])
        core_stage = StageResult(stage="core", fetched=1, validated=0, written=0, skipped=1, errors=0)
        argv = ["--db", str(db), "--raw-dir", str(raw)]
        if strict:
            argv.append("--strict")

        with (
            patch("fpl_ingest.orchestration.runner.AsyncFPLClient", return_value=client),
            patch("fpl_ingest.cli.SQLiteStore", return_value=store),
            patch("fpl_ingest.orchestration.runner.setup_store"),
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

    def test_failed_run_never_updates_last_successful_metadata(self):
        logger = MagicMock()
        store = RecordingStore()
        core = SimpleNamespace(events=[SimpleNamespace(id=1, is_current=True)], players=[SimpleNamespace(id=1)])

        exit_code = _exit_code(
            logger,
            [StageResult(stage="core", fetched=1, validated=1, written=1, skipped=0, errors=1)],
            store,
            "2026-04-21T00:00:00+00:00",
            core,
        )

        assert exit_code == 1
        assert ("last_successful_run_at", "2026-04-21T00:00:00+00:00") not in store.metadata_updates
        assert ("2026-04-21T00:00:00+00:00", RUN_STATUS_FAILED) in store.run_status_updates

    def test_final_run_status_classification(self):
        assert classify_run(errors=0, skipped=0, strict_mode=False) == RUN_STATUS_SUCCESS
        assert classify_run(errors=1, skipped=0, strict_mode=False) == RUN_STATUS_FAILED
        assert classify_run(errors=0, skipped=1, strict_mode=False) == RUN_STATUS_FAILED_PARTIAL
        assert classify_run(errors=0, skipped=1, strict_mode=True) == RUN_STATUS_FAILED


class TestSchemaValidateCLI:
    pytestmark = pytest.mark.integration

    def test_cli_validate_prints_warning_on_mismatch(self, tmp_path, capsys):
        import argparse
        import json
        from unittest.mock import patch

        from fpl_ingest.cli import run_schema
        from fpl_ingest.schema.definition import SCHEMA_VERSION, ValidationResult

        artifact = tmp_path / "schema_contract.json"
        artifact.write_text(
            json.dumps({"schema_version": "0.1.0", "tables": {}}),
            encoding="utf-8",
        )
        args = argparse.Namespace(db=None, schema_command="validate")
        stale_result = ValidationResult(
            status="valid",
            schema_version=SCHEMA_VERSION,
            db_path=str(tmp_path / "test.db"),
            checked_at="",
            version_mismatch=True,
            artifact_version="0.1.0",
        )
        with (
            patch("fpl_ingest.cli.validate_contract", return_value=stale_result),
            patch("fpl_ingest.cli.resolve_db_path_with_source", return_value=(str(tmp_path / "test.db"), "flag")),
            patch("fpl_ingest.cli.format_schema_output", return_value="schema ok"),
        ):
            run_schema(args)

        captured = capsys.readouterr()
        assert "0.1.0" in captured.err
        assert "SCHEMA_VERSION" in captured.err or "schema_version" in captured.err.lower()

    def test_cli_validate_no_warning_on_matching_versions(self, tmp_path, capsys):
        import argparse
        from unittest.mock import patch

        from fpl_ingest.cli import run_schema
        from fpl_ingest.schema.definition import SCHEMA_VERSION, ValidationResult

        args = argparse.Namespace(db=None, schema_command="validate")
        clean_result = ValidationResult(
            status="valid",
            schema_version=SCHEMA_VERSION,
            db_path=str(tmp_path / "test.db"),
            checked_at="",
            version_mismatch=False,
            artifact_version=None,
        )
        with (
            patch("fpl_ingest.cli.validate_contract", return_value=clean_result),
            patch("fpl_ingest.cli.resolve_db_path_with_source", return_value=(str(tmp_path / "test.db"), "flag")),
            patch("fpl_ingest.cli.format_schema_output", return_value="schema ok"),
        ):
            run_schema(args)

        captured = capsys.readouterr()
        assert "WARNING" not in captured.err
