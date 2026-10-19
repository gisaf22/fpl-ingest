"""Unit tests for the manager-panel draw and its one-off history capture (#102).

What it must guarantee:
  - the panel is a pure function of seed, ``total_players`` and the history
    responses, so the same inputs give the same panel file;
  - an id is kept only if its history shows picks this season, and an id the
    API does not know (404) is skipped, not failed;
  - the self cohort is always present and never counted in the random draw;
  - the only storage read anywhere is the panel file under ``fpl/_panels/``;
  - nothing stored carries a name, and history is stored as received.
"""

from __future__ import annotations

import ast
import json
import os
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock

import pytest

from fpl_ingest.extract.http.client import RawResponse
from fpl_ingest.extract.http.local_writer import (
    LocalFilesystemBackend,
    LocalRawWriter,
    RawStorageBackend,
)
from fpl_ingest.extract.http.raw_keys import RawKeyError, panel_key
from fpl_ingest.extract.http.s3_backend import S3Backend
from fpl_ingest.extract.stages.manager_panel import (
    default_seed,
    ingest_manager_panel,
    read_panel,
)
from tests.support.fixture_payloads import payload_bytes

SEASON = "2026-27"
SRC = Path(__file__).resolve().parents[4] / "src" / "fpl_ingest"

ACTIVE = "entry_history_active"
NO_PICKS = "entry_history_no_picks"
NOT_FOUND = "entry_history_not_found"


def _raw(entry_id: int, fixture: str) -> RawResponse:
    at = datetime(2026, 10, 19, 4, 30, tzinfo=timezone.utc)
    return RawResponse(
        url=f"https://fantasy.premierleague.com/api/entry/{entry_id}/history/",
        status=404 if fixture == NOT_FOUND else 200,
        headers={"content-type": "application/json"},
        body=payload_bytes(fixture),
        requested_at=at,
        received_at=at,
    )


def _client(responses: dict[int, str] | None = None, default: str = ACTIVE) -> MagicMock:
    """A fake client answering each entry id with the named real fixture."""
    responses = responses or {}
    client = MagicMock()

    async def _history(entry_id: int) -> RawResponse:
        return _raw(entry_id, responses.get(entry_id, default))

    client.get_entry_history_raw = AsyncMock(side_effect=_history)
    return client


def _writer(root: Path) -> LocalRawWriter:
    return LocalRawWriter(root, "fpl", started_at=datetime(2026, 10, 19, 4, 30, tzinfo=timezone.utc))


async def _draw(root: Path, client, *, seed: int = 7, total_players: int = 50, panel_size: int = 10,
                self_cohort: tuple[int, ...] = (8450051, 4685107)):
    writer = _writer(root)
    writer.set_season(SEASON)
    outcome = await ingest_manager_panel(
        client, writer, season=SEASON, total_players=total_players, seed=seed,
        panel_size=panel_size, self_cohort=self_cohort,
    )
    return writer, outcome


def _panel(root: Path) -> dict:
    return json.loads((root / panel_key("fpl", SEASON)).read_bytes())


def _kept(panel: dict, cohort: str | None = None) -> list[int]:
    return [e["entry_id"] for e in panel["entries"] if cohort is None or e["cohort"] == cohort]


class TestPanelIsReproducible:

    @pytest.mark.covers("#102 AC1")
    @pytest.mark.asyncio
    async def test_same_seed_and_responses_give_an_identical_panel_file(self, tmp_path):
        await _draw(tmp_path / "a", _client(), seed=11)
        await _draw(tmp_path / "b", _client(), seed=11)

        first = (tmp_path / "a" / panel_key("fpl", SEASON)).read_bytes()
        second = (tmp_path / "b" / panel_key("fpl", SEASON)).read_bytes()
        assert first == second

    @pytest.mark.covers("#102 AC1")
    @pytest.mark.asyncio
    async def test_panel_records_seed_total_players_counts_cohort_and_joined_gameweek(self, tmp_path):
        await _draw(tmp_path, _client(), seed=11, total_players=50, panel_size=10)

        panel = _panel(tmp_path)
        assert panel["season"] == SEASON
        assert panel["seed"] == 11
        assert panel["total_players"] == 50
        assert panel["drawn"] == 10
        assert panel["kept"] == len(_kept(panel, "random")) == 10
        assert {e["cohort"] for e in panel["entries"]} == {"random", "self"}
        assert {e["joined_gw"] for e in panel["entries"]} == {1}

    @pytest.mark.covers("#102 AC1")
    @pytest.mark.asyncio
    async def test_a_different_seed_draws_different_ids(self, tmp_path):
        await _draw(tmp_path / "a", _client(), seed=11, total_players=10_000)
        await _draw(tmp_path / "b", _client(), seed=12, total_players=10_000)

        assert _kept(_panel(tmp_path / "a"), "random") != _kept(_panel(tmp_path / "b"), "random")


class TestDefaultSeed:

    @pytest.mark.covers("#102 AC12")
    def test_the_default_seed_is_identical_across_calls_for_one_season(self):
        assert default_seed("2026-27") == default_seed("2026-27")

    @pytest.mark.covers("#102 AC12")
    def test_the_default_seed_differs_across_seasons(self):
        assert default_seed("2026-27") != default_seed("2027-28")

    @pytest.mark.covers("#102 AC12")
    def test_the_default_seed_does_not_depend_on_pythons_per_process_string_hash(self):
        code = "from fpl_ingest.extract.stages.manager_panel import default_seed; print(default_seed('2026-27'))"
        seeds = {
            subprocess.run(
                [sys.executable, "-c", code], env={**os.environ, "PYTHONHASHSEED": hash_seed},
                capture_output=True, text=True, check=True,
            ).stdout.strip()
            for hash_seed in ("1", "2")
        }
        assert len(seeds) == 1


class TestUnknownIdsAreSkipped:

    @pytest.mark.covers("#102 AC2")
    @pytest.mark.asyncio
    async def test_404_ids_are_counted_as_drawn_not_kept_and_are_not_failures(self, tmp_path):
        everyone_missing = _client(default=NOT_FOUND, responses={8450051: ACTIVE, 4685107: ACTIVE})

        writer, outcome = await _draw(tmp_path, everyone_missing, panel_size=10)

        panel = _panel(tmp_path)
        assert _kept(panel, "random") == []
        assert panel["drawn"] == 10
        assert panel["kept"] == 0
        assert panel["drawn_not_kept"]["not_found"] == 10
        assert outcome.result.errors == 0
        assert writer.manifest_snapshot()["failures"] == []


class TestKeepRule:

    @pytest.mark.covers("#102 AC3")
    @pytest.mark.asyncio
    async def test_an_id_with_no_picks_this_season_is_not_kept_and_one_with_picks_is(self, tmp_path):
        writer = _writer(tmp_path)
        writer.set_season(SEASON)
        # total_players == panel_size: every id 1..4 is drawn, so the mapping is exact.
        client = _client(responses={1: NO_PICKS, 2: ACTIVE, 3: NO_PICKS, 4: ACTIVE})

        await ingest_manager_panel(client, writer, season=SEASON, total_players=4, seed=1,
                                   panel_size=4, self_cohort=())

        panel = _panel(tmp_path)
        assert sorted(_kept(panel, "random")) == [2, 4]
        assert panel["drawn_not_kept"]["no_picks"] == 2


class TestSelfCohort:

    @pytest.mark.covers("#102 AC4")
    @pytest.mark.asyncio
    async def test_self_ids_are_present_with_cohort_self(self, tmp_path):
        await _draw(tmp_path, _client())

        assert sorted(_kept(_panel(tmp_path), "self")) == [4685107, 8450051]

    @pytest.mark.covers("#102 AC4")
    @pytest.mark.asyncio
    async def test_self_ids_are_never_among_the_random_draws(self, tmp_path):
        # Population 1..10 with self ids 3 and 7: drawing 8 must take exactly the other 8.
        await _draw(tmp_path, _client(), total_players=10, panel_size=8, self_cohort=(3, 7))

        panel = _panel(tmp_path)
        assert sorted(_kept(panel, "random")) == [1, 2, 4, 5, 6, 8, 9, 10]
        assert sorted(_kept(panel, "self")) == [3, 7]
        assert panel["drawn"] == 8


class TestOnlyThePanelCanBeRead:

    @pytest.mark.covers("#102 AC7")
    def test_the_backend_protocol_offers_no_read_but_the_panel_read(self):
        members = {name for name in vars(RawStorageBackend) if not name.startswith("_")}
        assert members == {"put_bytes", "location", "exists_prefix", "read_panel_bytes"}

    @pytest.mark.covers("#102 AC7")
    @pytest.mark.parametrize("key", [
        "fpl/_manifests/2026-10-19/20261019T043000Z-abcdef/manifest.json",
        "fpl/entry-history/2026-27/1/2026-10-19/20261019T043000Z-abcdef/payload.json",
        "fpl/_panels/../_manifests/x.json",
        "fpl/_panelsx/2026-27/manager-panel.json",
    ])
    def test_local_backend_refuses_to_read_outside_the_panels_prefix(self, tmp_path, key):
        with pytest.raises(RawKeyError):
            LocalFilesystemBackend(tmp_path).read_panel_bytes(key)

    @pytest.mark.covers("#102 AC7")
    def test_s3_backend_refuses_to_read_outside_the_panels_prefix(self):
        s3 = MagicMock()
        backend = S3Backend("fpl-data-safari", client=s3)

        with pytest.raises(RawKeyError):
            backend.read_panel_bytes("fpl/_manifests/2026-10-19/x/manifest.json")
        s3.get_object.assert_not_called()

    @pytest.mark.covers("#102 AC7")
    def test_local_backend_reads_back_the_panel_and_reports_a_missing_one(self, tmp_path):
        backend = LocalFilesystemBackend(tmp_path)
        key = panel_key("fpl", SEASON)
        assert backend.read_panel_bytes(key) is None

        backend.put_bytes(key, b'{"season": "2026-27"}')
        assert backend.read_panel_bytes(key) == b'{"season": "2026-27"}'

    @pytest.mark.covers("#102 AC7")
    def test_no_module_but_the_panel_stage_and_the_backends_reads_storage(self):
        allowed = {
            SRC / "extract" / "stages" / "manager_panel.py",
            SRC / "extract" / "http" / "local_writer.py",
            SRC / "extract" / "http" / "s3_backend.py",
        }
        readers = set()
        for path in SRC.rglob("*.py"):
            for node in ast.walk(ast.parse(path.read_text())):
                if isinstance(node, ast.Attribute) and node.attr in {"read_panel_bytes", "get_object"}:
                    readers.add(path)
        assert readers - allowed == set()

    @pytest.mark.covers("#102 AC7")
    @pytest.mark.asyncio
    async def test_read_panel_returns_the_drawn_panel(self, tmp_path):
        writer, outcome = await _draw(tmp_path, _client())

        assert read_panel(writer.backend, SEASON) == outcome.output == _panel(tmp_path)


class TestNoNamesAndVerbatimHistory:

    @pytest.mark.covers("#102 AC8")
    @pytest.mark.asyncio
    async def test_panel_file_holds_no_names(self, tmp_path):
        await _draw(tmp_path, _client())

        text = (tmp_path / panel_key("fpl", SEASON)).read_text()
        for field in ("name", "player_first_name", "player_last_name", "entry_name"):
            assert f'"{field}"' not in text

    @pytest.mark.covers("#102 AC8")
    @pytest.mark.asyncio
    async def test_history_is_stored_byte_for_byte(self, tmp_path):
        await _draw(tmp_path, _client(), self_cohort=())

        stored = sorted((tmp_path / "fpl" / "entry-history").rglob("payload.json"))
        assert stored
        assert {p.read_bytes() for p in stored} == {payload_bytes(ACTIVE)}
