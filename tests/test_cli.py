"""Tests for the CLI entry point and async pipeline stage integration.

Covers:
  - Argument parsing and config defaults
  - Per-stage transaction scoping (one transaction per stage)
  - Client lifecycle: session opened on entry, closed on exit and on failure
  - Concurrent player fetch: all IDs fetched, errors isolated, files written
  - Strict mode: aborts run when any stage reports skipped rows or errors
  - Real pipeline stages executed with mocked client and store
"""

from __future__ import annotations

import json
from contextlib import contextmanager
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

pytestmark = pytest.mark.unit

from fpl_ingest.cli import build_parser, main
from fpl_ingest.pipeline import StageResult

# ---------------------------------------------------------------------------
# Shared fixtures
# ---------------------------------------------------------------------------

MINIMAL_BOOTSTRAP = {
    "events": [],
    "elements": [
        {"id": 1, "team": 11, "element_type": 3, "now_cost": 130},
        {"id": 2, "team": 13, "element_type": 4, "now_cost": 145},
    ],
    "teams": [
        {"id": 11, "name": "Liverpool", "short_name": "LIV"},
        {"id": 13, "name": "Man City", "short_name": "MCI"},
    ],
    "element_types": [],
    "phases": [],
}

PLAYER_HISTORY_1 = {"history": [{"element_id": 1, "round": 1, "fixture": 11}], "history_past": []}
PLAYER_HISTORY_2 = {"history": [{"element_id": 2, "round": 1, "fixture": 22}], "history_past": []}

VALID_BOOTSTRAP = {
    "events": [],
    "elements": [
        {
            "id": 1,
            "first_name": "Mohamed",
            "second_name": "Salah",
            "web_name": "Salah",
            "team": 11,
            "element_type": 3,
            "now_cost": 130,
            "status": "a",
            "chance_of_playing_next_round": 100,
            "total_points": 42,
            "form": "8.5",
            "points_per_game": "7.0",
            "selected_by_percent": "55.2",
        }
    ],
    "teams": [],
    "element_types": [],
}


def _make_async_client(bootstrap=MINIMAL_BOOTSTRAP, history_side_effect=None):
    """Build an AsyncMock client suitable for patching AsyncFPLClient."""
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
    """Run main() with mocked client + store, returning the raw dir path."""
    raw = tmp_path / "raw"
    db = tmp_path / "test.db"
    with (
        patch("fpl_ingest.cli.AsyncFPLClient", return_value=mock_client),
        patch("fpl_ingest.cli.SQLiteStore", return_value=mock_store),
    ):
        main(["--db", str(db), "--raw-dir", str(raw)] + argv)
    return raw


# ---------------------------------------------------------------------------
# Concurrent player fetch
# ---------------------------------------------------------------------------


class TestConcurrentPlayerFetch:
    """asyncio.gather-based element-summary fetch."""

    def test_all_players_fetched(self, tmp_path):
        """get_player_history is called exactly once per player ID."""
        client = _make_async_client()
        store = _make_mock_store()
        _run([], client, store, tmp_path)

        called_ids = {c.args[0] for c in client.get_player_history.call_args_list}
        assert called_ids == {1, 2}

    def test_json_written_to_disk(self, tmp_path):
        """Successful fetch writes players/{pid}.json to the raw dir."""
        client = _make_async_client()
        store = _make_mock_store()
        raw = _run([], client, store, tmp_path)

        for pid, expected in [(1, PLAYER_HISTORY_1), (2, PLAYER_HISTORY_2)]:
            path = raw / "players" / f"{pid}.json"
            assert path.exists(), f"players/{pid}.json not written"
            assert json.loads(path.read_text()) == expected

    def test_error_on_one_player_continues_others(self, tmp_path):
        """A failed fetch for one player is counted but the run continues."""
        async def side_effect(pid):
            if pid == 1:
                raise RuntimeError("network failure")
            return PLAYER_HISTORY_2

        client = _make_async_client(history_side_effect=side_effect)
        store = _make_mock_store()
        raw = tmp_path / "raw"
        db = tmp_path / "test.db"
        with (
            patch("fpl_ingest.cli.AsyncFPLClient", return_value=client),
            patch("fpl_ingest.cli.SQLiteStore", return_value=store),
        ):
            main(["--db", str(db), "--raw-dir", str(raw)])

        assert not (raw / "players" / "1.json").exists()
        assert (raw / "players" / "2.json").exists()

    def test_strict_mode_aborts_on_player_error(self, tmp_path):
        """With --strict, any fetch error raises RuntimeError."""
        async def side_effect(pid):
            if pid == 1:
                raise RuntimeError("network failure")
            return PLAYER_HISTORY_2

        client = _make_async_client(history_side_effect=side_effect)
        store = _make_mock_store()
        raw = tmp_path / "raw"
        db = tmp_path / "test.db"
        with (
            patch("fpl_ingest.cli.AsyncFPLClient", return_value=client),
            patch("fpl_ingest.cli.SQLiteStore", return_value=store),
        ):
            with pytest.raises(RuntimeError, match="player_histories"):
                main(["--db", str(db), "--raw-dir", str(raw), "--strict"])


# ---------------------------------------------------------------------------
# Store doubles
# ---------------------------------------------------------------------------


class RecordingStore:
    """Minimal store double that records transaction entry count."""

    def __init__(self) -> None:
        self.transaction_entries = 0

    @contextmanager
    def transaction(self):
        self.transaction_entries += 1
        yield

    def record_run(self, *args, **kwargs) -> None:
        pass


class IntegrationStore(RecordingStore):
    """Store double for running real pipeline stages from the CLI."""

    def __init__(self) -> None:
        super().__init__()
        self.register_table = MagicMock()
        self.create_index = MagicMock()
        self.upsert_models = MagicMock(return_value=(0, 0))
        self.setup_runs_table = MagicMock()


# ---------------------------------------------------------------------------
# CLI lifecycle
# ---------------------------------------------------------------------------


class TestCliLifecycle:

    def test_uses_separate_transactions_per_stage(self, tmp_path):
        raw = tmp_path / "raw"
        db = tmp_path / "test.db"
        client = _make_async_client()
        store = RecordingStore()
        core = SimpleNamespace(
            events=[SimpleNamespace(id=1, finished=True, is_current=False)],
            players=[SimpleNamespace(id=1), SimpleNamespace(id=2)],
        )

        with (
            patch("fpl_ingest.cli.AsyncFPLClient", return_value=client),
            patch("fpl_ingest.cli.SQLiteStore", return_value=store),
            patch("fpl_ingest.cli.setup_store"),
            patch("fpl_ingest.cli.ingest_core_data", new=AsyncMock(return_value=(core, StageResult(stage="core")))),
            patch("fpl_ingest.cli.ingest_fixtures", new=AsyncMock(return_value=StageResult(stage="fixtures"))),
            patch("fpl_ingest.cli.ingest_gameweeks", new=AsyncMock(return_value=StageResult(stage="gameweeks"))),
            patch("fpl_ingest.cli.ingest_player_histories", new=AsyncMock(return_value=StageResult(stage="player_histories"))),
        ):
            main(["--db", str(db), "--raw-dir", str(raw)])

        assert store.transaction_entries == 4

    def test_closes_client_on_success(self, tmp_path):
        raw = tmp_path / "raw"
        db = tmp_path / "test.db"
        client = _make_async_client()
        store = RecordingStore()
        core = SimpleNamespace(events=[], players=[])

        with (
            patch("fpl_ingest.cli.AsyncFPLClient", return_value=client),
            patch("fpl_ingest.cli.SQLiteStore", return_value=store),
            patch("fpl_ingest.cli.setup_store"),
            patch("fpl_ingest.cli.ingest_core_data", new=AsyncMock(return_value=(core, StageResult(stage="core")))),
            patch("fpl_ingest.cli.ingest_fixtures", new=AsyncMock(return_value=StageResult(stage="fixtures"))),
            patch("fpl_ingest.cli.ingest_gameweeks", new=AsyncMock(return_value=StageResult(stage="gameweeks"))),
            patch("fpl_ingest.cli.ingest_player_histories", new=AsyncMock(return_value=StageResult(stage="player_histories"))),
        ):
            main(["--db", str(db), "--raw-dir", str(raw)])

        client.__aexit__.assert_called_once()

    def test_closes_client_when_stage_fails(self, tmp_path):
        raw = tmp_path / "raw"
        db = tmp_path / "test.db"
        client = _make_async_client()
        store = RecordingStore()
        core = SimpleNamespace(events=[], players=[])

        with (
            patch("fpl_ingest.cli.AsyncFPLClient", return_value=client),
            patch("fpl_ingest.cli.SQLiteStore", return_value=store),
            patch("fpl_ingest.cli.setup_store"),
            patch("fpl_ingest.cli.ingest_core_data", new=AsyncMock(return_value=(core, StageResult(stage="core")))),
            patch("fpl_ingest.cli.ingest_fixtures", new=AsyncMock(side_effect=RuntimeError("boom"))),
            patch("fpl_ingest.cli.ingest_gameweeks", new=AsyncMock(return_value=StageResult(stage="gameweeks"))),
            patch("fpl_ingest.cli.ingest_player_histories", new=AsyncMock(return_value=StageResult(stage="player_histories"))),
        ):
            with pytest.raises(RuntimeError, match="boom"):
                main(["--db", str(db), "--raw-dir", str(raw)])

        client.__aexit__.assert_called_once()

    def test_parser_uses_config_defaults(self, monkeypatch):
        monkeypatch.setenv("FPL_DB_PATH", "/tmp/fpl-test.db")
        monkeypatch.setenv("FPL_RAW_DIR", "/tmp/fpl-raw")

        parser = build_parser()
        args = parser.parse_args([])

        assert args.db is None
        assert args.raw_dir is None

    def test_main_runs_real_stages_with_mocked_client_and_store(self, tmp_path):
        raw = tmp_path / "raw"
        db = tmp_path / "test.db"
        client = _make_async_client(
            bootstrap=VALID_BOOTSTRAP,
            history_side_effect=AsyncMock(return_value=PLAYER_HISTORY_1),
        )
        store = IntegrationStore()

        with (
            patch("fpl_ingest.cli.AsyncFPLClient", return_value=client),
            patch("fpl_ingest.cli.SQLiteStore", return_value=store),
        ):
            main(["--db", str(db), "--raw-dir", str(raw)])

        assert (raw / "bootstrap.json").exists()
        assert json.loads((raw / "bootstrap.json").read_text()) == VALID_BOOTSTRAP
        assert (raw / "players" / "1.json").exists()
        assert json.loads((raw / "players" / "1.json").read_text()) == PLAYER_HISTORY_1
        assert store.register_table.call_count > 0
        assert store.create_index.call_count > 0

        upserted_tables = [call.args[0] for call in store.upsert_models.call_args_list]
        assert "players" in upserted_tables
        assert "player_histories" in upserted_tables

    def test_strict_mode_fails_when_core_stage_reports_skipped_rows(self, tmp_path):
        raw = tmp_path / "raw"
        db = tmp_path / "test.db"
        client = _make_async_client()
        store = RecordingStore()
        core_data = SimpleNamespace(events=[], players=[])
        core_stage = StageResult(stage="core", fetched=1, upserted=1, skipped=1, errors=0)

        with (
            patch("fpl_ingest.cli.AsyncFPLClient", return_value=client),
            patch("fpl_ingest.cli.SQLiteStore", return_value=store),
            patch("fpl_ingest.cli.setup_store"),
            patch("fpl_ingest.cli.ingest_core_data", new=AsyncMock(return_value=(core_data, core_stage))),
            patch("fpl_ingest.cli.ingest_fixtures", new=AsyncMock(return_value=StageResult(stage="fixtures"))),
            patch("fpl_ingest.cli.ingest_gameweeks", new=AsyncMock(return_value=StageResult(stage="gameweeks"))),
            patch("fpl_ingest.cli.ingest_player_histories", new=AsyncMock(return_value=StageResult(stage="player_histories"))),
        ):
            with pytest.raises(RuntimeError, match="core: .*skipped=1"):
                main(["--db", str(db), "--raw-dir", str(raw), "--strict"])
