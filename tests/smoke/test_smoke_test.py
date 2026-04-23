from __future__ import annotations

import json
from copy import deepcopy
from pathlib import Path
from unittest.mock import AsyncMock, patch

import pytest

from fpl_ingest.validation.smoke_test import SmokeTestFailure, run_smoke_test

pytestmark = pytest.mark.unit

FIXTURES_DIR = Path(__file__).resolve().parents[1] / "fixtures" / "smoke"


def _load_fixture(name: str):
    return json.loads((FIXTURES_DIR / name).read_text())


def _bootstrap_payload() -> dict:
    return deepcopy(_load_fixture("bootstrap_static.json"))


def _fixtures_payload() -> list[dict]:
    return deepcopy(_load_fixture("fixtures.json"))


def _history_payload() -> dict:
    return deepcopy(_load_fixture("element_summary.json"))


def _make_client():
    client = AsyncMock()
    client.__aenter__ = AsyncMock(return_value=client)
    client.__aexit__ = AsyncMock(return_value=None)
    client.get_bootstrap = AsyncMock(return_value=_bootstrap_payload())
    client.get_fixtures = AsyncMock(return_value=_fixtures_payload())
    client.get_player_history = AsyncMock(return_value=_history_payload())
    return client


def test_run_smoke_test_passes_for_expected_shapes():
    client = _make_client()

    with patch("fpl_ingest.validation.smoke_test.AsyncFPLClient", return_value=client):
        result = run_smoke_test()

    assert result.endpoints_checked == ("bootstrap-static", "fixtures", "element-summary")
    assert result.sample_size == 5
    assert client.get_bootstrap.await_count == 1
    assert client.get_fixtures.await_count == 1
    assert client.get_player_history.await_count == 1
    client.get_player_history.assert_awaited_once_with(1)


def test_run_smoke_test_fails_on_missing_bootstrap_field():
    client = _make_client()
    broken = _bootstrap_payload()
    del broken["elements"][0]["now_cost"]
    client.get_bootstrap = AsyncMock(return_value=broken)

    with patch("fpl_ingest.validation.smoke_test.AsyncFPLClient", return_value=client):
        with pytest.raises(SmokeTestFailure, match=r"Missing field: elements\[\]\.now_cost"):
            run_smoke_test()


def test_run_smoke_test_fails_on_missing_top_level_key():
    client = _make_client()
    broken = _bootstrap_payload()
    del broken["teams"]
    client.get_bootstrap = AsyncMock(return_value=broken)

    with patch("fpl_ingest.validation.smoke_test.AsyncFPLClient", return_value=client):
        with pytest.raises(SmokeTestFailure, match=r"Missing field: bootstrap-static\.teams"):
            run_smoke_test()


def test_run_smoke_test_fails_on_malformed_structure():
    client = _make_client()
    client.get_fixtures = AsyncMock(return_value={"id": 101})

    with patch("fpl_ingest.validation.smoke_test.AsyncFPLClient", return_value=client):
        with pytest.raises(SmokeTestFailure, match=r"Expected list: fixtures"):
            run_smoke_test()
