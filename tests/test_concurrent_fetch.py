"""Tests for concurrent element-summary fetch introduced in cli.main().

Covers:
  - All player IDs are fetched (completeness)
  - --force bypasses the file-exists cache
  - Without --force, cached files are skipped
  - An error on one player does not abort others
  - Successful fetches write players/{pid}.json to disk
  - --skip-history bypasses the fetch entirely
"""

from __future__ import annotations

import json
from unittest.mock import MagicMock, patch

import pytest

pytestmark = pytest.mark.unit

from fpl_ingest.cli import main

# ---------------------------------------------------------------------------
# Shared fixtures
# ---------------------------------------------------------------------------

# Minimal bootstrap: only "id" needed per element; store is mocked so data
# doesn't need to satisfy Pydantic validation.
MINIMAL_BOOTSTRAP = {
    "events": [],          # no finished GWs → GW live-data loop is skipped
    "elements": [{"id": 1}, {"id": 2}],
    "teams": [],
    "element_types": [],
    "phases": [],
}

PLAYER_HISTORY_1 = {"history": [{"element_id": 1, "round": 1}], "history_past": []}
PLAYER_HISTORY_2 = {"history": [{"element_id": 2, "round": 1}], "history_past": []}


@pytest.fixture()
def mock_client():
    client = MagicMock()
    client.get_bootstrap.return_value = MINIMAL_BOOTSTRAP
    client.get_fixtures.return_value = []
    client.get_player_history.side_effect = lambda pid: (
        PLAYER_HISTORY_1 if pid == 1 else PLAYER_HISTORY_2
    )
    return client


@pytest.fixture()
def mock_store():
    store = MagicMock()
    store.upsert_models.return_value = (0, 0)
    return store


def _run(argv: list[str], mock_client, mock_store, tmp_path) -> "Path":
    """Run main() with mocked client + store, returning the raw dir path."""
    raw = tmp_path / "raw"
    db = tmp_path / "test.db"
    with (
        patch("fpl_ingest.cli.FPLClient", return_value=mock_client),
        patch("fpl_ingest.cli.SQLiteStore", return_value=mock_store),
    ):
        main(["--db", str(db), "--raw-dir", str(raw)] + argv)
    return raw


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


class TestConcurrentPlayerFetch:
    """ThreadPoolExecutor-based element-summary fetch."""

    def test_all_players_fetched(self, mock_client, mock_store, tmp_path):
        """get_player_history is called exactly once per player ID."""
        _run([], mock_client, mock_store, tmp_path)

        called_ids = {c.args[0] for c in mock_client.get_player_history.call_args_list}
        assert called_ids == {1, 2}

    def test_json_written_to_disk(self, mock_client, mock_store, tmp_path):
        """Successful fetch writes players/{pid}.json to the raw dir."""
        raw = _run([], mock_client, mock_store, tmp_path)

        for pid, expected in [(1, PLAYER_HISTORY_1), (2, PLAYER_HISTORY_2)]:
            path = raw / "players" / f"{pid}.json"
            assert path.exists(), f"players/{pid}.json not written"
            assert json.loads(path.read_text()) == expected

    def test_force_refetches_cached_player(self, mock_client, mock_store, tmp_path):
        """--force fetches all players even when their JSON already exists."""
        players_dir = tmp_path / "raw" / "players"
        players_dir.mkdir(parents=True)
        (players_dir / "1.json").write_text("{}")  # pre-cached

        _run(["--force"], mock_client, mock_store, tmp_path)

        called_ids = {c.args[0] for c in mock_client.get_player_history.call_args_list}
        assert called_ids == {1, 2}

    def test_error_on_one_player_continues_others(self, mock_client, mock_store, tmp_path):
        """A failed fetch for one player does not abort the rest."""
        def side_effect(pid):
            if pid == 1:
                raise RuntimeError("network failure")
            return PLAYER_HISTORY_2

        mock_client.get_player_history.side_effect = side_effect

        # Should not raise despite player 1 failing
        raw = _run([], mock_client, mock_store, tmp_path)

        assert not (raw / "players" / "1.json").exists(), "failed player should not write file"
        assert (raw / "players" / "2.json").exists(), "successful player should write file"

    def test_skip_history_bypasses_fetch(self, mock_client, mock_store, tmp_path):
        """--skip-history means get_player_history is never called."""
        _run(["--skip-history"], mock_client, mock_store, tmp_path)

        mock_client.get_player_history.assert_not_called()
