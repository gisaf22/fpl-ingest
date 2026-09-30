"""The capture-index backfill over a bucket-shaped local tree (#66).

``build_backfill`` reads the tree through ``LocalTree`` and writes one catalog
file per in-scope run under ``raw/fpl/_catalog/backfill/``. In scope: every
run whose manifest is below 2.1.0, plus the ported history run.
"""

from __future__ import annotations

import copy
import json
from pathlib import Path

import pytest

from fpl_ingest.backfill import LocalTree, build_backfill
from tests.support import raw_contract_schema
from tests.support.backfill_tree import (
    HISTORY_RECEIVED_AT,
    HISTORY_RUN,
    IN_SCOPE_RUNS,
    RUN_1_0_0,
    RUN_2_0_0,
    RUN_2_1_0,
    RUN_NO_BOOTSTRAP,
    VALIDATOR_VERSION,
    build_tree,
    catalog_key,
    read_catalog,
)


def _backfill(root: Path):
    return build_backfill(LocalTree(root), validator_version=VALIDATOR_VERSION)


def _catalog_files(root: Path) -> list[str]:
    folder = root / "raw" / "fpl" / "_catalog" / "backfill"
    return sorted(p.stem for p in folder.glob("*.json")) if folder.is_dir() else []


def _entries(root: Path, run_id: str) -> list[dict]:
    return read_catalog(root, run_id)["captures"]


class TestOneCatalogPerRun:

    @pytest.mark.covers("#66 AC1")
    def test_backfill_fixture_tree(self, tmp_path):
        tree = build_tree(tmp_path)

        report = _backfill(tmp_path)

        assert _catalog_files(tmp_path) == sorted(IN_SCOPE_RUNS)
        for run_id in IN_SCOPE_RUNS:
            keys = [e["key"] for e in _entries(tmp_path, run_id)]
            assert len(keys) == len(set(keys)), run_id
            assert sorted(keys) == sorted(tree.payload_keys[run_id]), run_id
        assert report.indexed == sum(len(v) for v in tree.payload_keys.values())

    @pytest.mark.covers("#66 AC1")
    def test_run_already_indexed_by_a_2_1_0_manifest_is_not_backfilled(self, tmp_path):
        build_tree(tmp_path)

        _backfill(tmp_path)

        assert RUN_2_1_0 not in _catalog_files(tmp_path)

    @pytest.mark.covers("#66 AC1")
    def test_catalog_envelope_names_its_run_and_scope(self, tmp_path):
        build_tree(tmp_path)

        _backfill(tmp_path)

        for run_id in IN_SCOPE_RUNS:
            catalog = read_catalog(tmp_path, run_id)
            assert catalog["run_id"] == run_id
            assert catalog["source"] == "fpl"
            assert catalog["scope"] == ("history" if run_id == HISTORY_RUN else "live")


class TestIdempotent:

    @pytest.mark.covers("#66 AC2")
    def test_backfill_idempotent(self, tmp_path):
        build_tree(tmp_path)
        _backfill(tmp_path)
        before = {r: (tmp_path / catalog_key(r)).read_bytes() for r in IN_SCOPE_RUNS}

        report = _backfill(tmp_path)

        assert report.written == []
        assert sorted(report.skipped) == sorted(catalog_key(r) for r in IN_SCOPE_RUNS)
        assert {r: (tmp_path / catalog_key(r)).read_bytes() for r in IN_SCOPE_RUNS} == before

    @pytest.mark.covers("#66 AC2")
    def test_deleted_catalog_file_is_the_only_one_rewritten(self, tmp_path):
        build_tree(tmp_path)
        _backfill(tmp_path)
        (tmp_path / catalog_key(RUN_2_0_0)).unlink()

        report = _backfill(tmp_path)

        assert report.written == [catalog_key(RUN_2_0_0)]
        assert (tmp_path / catalog_key(RUN_2_0_0)).is_file()

    @pytest.mark.covers("#66 AC2")
    def test_existing_catalog_file_is_never_overwritten(self, tmp_path):
        build_tree(tmp_path)
        _backfill(tmp_path)
        sentinel = b'{"sentinel": true}'
        (tmp_path / catalog_key(RUN_1_0_0)).write_bytes(sentinel)

        _backfill(tmp_path)

        assert (tmp_path / catalog_key(RUN_1_0_0)).read_bytes() == sentinel


class TestReport:

    @pytest.mark.covers("#66 AC3")
    def test_report_counts(self, tmp_path):
        tree = build_tree(tmp_path)

        report = _backfill(tmp_path)

        assert report.indexed == sum(len(v) for v in tree.payload_keys.values())
        # RUN_2_0_0's fixtures has no sidecar verdict; no history sidecar has one.
        assert report.revalidated == 1 + len(tree.payload_keys[HISTORY_RUN])

    @pytest.mark.covers("#66 AC3")
    def test_report_lists_every_shape_failure_by_section(self, tmp_path):
        build_tree(tmp_path)

        report = _backfill(tmp_path).to_json()

        (live,) = report["shape_failures"]["live"]
        assert live["run_id"] == RUN_1_0_0
        assert live["endpoint"] == "element-summary/2"
        assert live["extraction_date"] == "2026-08-29"
        assert live["shape_source"] == "sidecar"
        assert live["key"].startswith("raw/fpl/element-summary/2/")
        assert live["failures"]

        (history,) = report["shape_failures"]["history"]
        assert history["run_id"] == HISTORY_RUN
        assert history["endpoint"] == "element-summary/11"
        assert history["shape_source"] == "revalidated"
        assert history["key"].startswith("history/2025-26/fpl/element-summary/11/")
        assert history["failures"]

        assert report["indexed"] == _backfill_counts(tmp_path)["indexed"]

    @pytest.mark.covers("#66 AC3")
    def test_job_summary_has_live_and_history_sections(self, tmp_path):
        build_tree(tmp_path)

        summary = _backfill(tmp_path).to_markdown()

        live_at, history_at = summary.index("Live"), summary.index("History")
        assert live_at < summary.index("element-summary/2/") < history_at
        assert summary.index("element-summary/11/") > history_at

    @pytest.mark.covers("#66 AC3")
    def test_zero_shape_failures_is_stated(self, tmp_path):
        tree = build_tree(tmp_path)
        for run_id in (RUN_1_0_0, HISTORY_RUN):
            for key in tree.payload_keys[run_id]:
                if "/element-summary/2/" in key or "/element-summary/11/" in key:
                    folder = (tmp_path / key).parent
                    for f in folder.iterdir():
                        f.unlink()
                    folder.rmdir()
                    tree.payload_keys[run_id].remove(key)

        backfill = _backfill(tmp_path)

        assert backfill.to_json()["shape_failures"] == {"live": [], "history": []}
        assert "0 shape failures" in backfill.to_markdown()


def _backfill_counts(root: Path) -> dict:
    """Indexed count read back from the catalog files themselves."""
    return {"indexed": sum(len(_entries(root, r)) for r in IN_SCOPE_RUNS)}


class TestSchema:

    @pytest.mark.covers("#66 AC4")
    def test_catalog_files_validate_against_schema(self, tmp_path):
        build_tree(tmp_path)

        _backfill(tmp_path)

        for run_id in IN_SCOPE_RUNS:
            assert raw_contract_schema.errors("backfill-catalog", read_catalog(tmp_path, run_id)) == [], run_id

    @pytest.mark.covers("#66 AC4")
    @pytest.mark.parametrize(
        "mutate",
        [
            pytest.param(lambda d: d.update(unexpected=1), id="file-unknown-field"),
            pytest.param(lambda d: d["captures"][0].update(unexpected=1), id="entry-unknown-field"),
            pytest.param(lambda d: d["captures"][0].pop("shape_source"), id="entry-missing-shape-source"),
            pytest.param(lambda d: d["captures"][0].update(shape_source="guessed"), id="entry-bad-shape-source"),
        ],
    )
    def test_schema_rejects_invalid_catalogs(self, tmp_path, mutate):
        build_tree(tmp_path)
        _backfill(tmp_path)
        catalog = read_catalog(tmp_path, RUN_1_0_0)
        mutated = copy.deepcopy(catalog)
        mutate(mutated)

        assert raw_contract_schema.errors("backfill-catalog", catalog) == []
        assert raw_contract_schema.errors("backfill-catalog", mutated) != []


class TestSeason:

    @pytest.mark.covers("#66 AC6")
    def test_live_runs_take_the_season_from_their_bootstrap(self, tmp_path):
        build_tree(tmp_path)

        _backfill(tmp_path)

        for run_id in (RUN_1_0_0, RUN_2_0_0):
            assert {e["season"] for e in _entries(tmp_path, run_id)} == {"2026-27"}, run_id

    @pytest.mark.covers("#66 AC6")
    def test_live_run_without_bootstrap_has_null_season_and_is_listed(self, tmp_path):
        build_tree(tmp_path)

        report = _backfill(tmp_path)

        assert {e["season"] for e in _entries(tmp_path, RUN_NO_BOOTSTRAP)} == {None}
        assert report.null_season_runs == [RUN_NO_BOOTSTRAP]
        assert RUN_NO_BOOTSTRAP in json.dumps(report.to_json()["null_season_runs"])

    @pytest.mark.covers("#66 AC6")
    def test_history_entries_carry_season_and_the_synthetic_run_instant(self, tmp_path):
        build_tree(tmp_path)

        _backfill(tmp_path)

        entries = _entries(tmp_path, HISTORY_RUN)
        assert {e["season"] for e in entries} == {"2025-26"}
        assert {e["received_at"] for e in entries} == {HISTORY_RECEIVED_AT}
        assert {e["http_status"] for e in entries} == {None}
        assert {e["shape_source"] for e in entries} == {"revalidated"}
