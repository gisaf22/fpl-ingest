"""``entry-picks`` and ``entry-history`` as `fpl-ingest baseline` endpoints (#116 AC8).

Both families are baseline endpoints, built from live fetches (never S3, never the
panel file) with a new ``--entries`` flag; picks also take ``--gameweeks``.

The fetches are faked: the tests drive ``run_baseline`` with a client returning real
samples, as ``tests/unit/schema/test_payload_baseline.py`` does, so they never reach
the public API. What the committed baselines contain, and that a capture checked
against them carries a ``drift`` block, is in ``test_entry_picks_drift.py``.
"""

from __future__ import annotations

import json
from datetime import datetime, timezone

import pytest

from fpl_ingest.cli import build_parser, run_baseline
from fpl_ingest.extract.http.client import RawResponse
from tests.support.fixture_payloads import payload_bytes

AT = datetime(2026, 10, 20, 7, 0, tzinfo=timezone.utc)


def _raw(body: bytes, status: int = 200) -> RawResponse:
    return RawResponse(
        url="https://fantasy.premierleague.com/api/x/", status=status, headers={}, body=body,
        requested_at=AT, received_at=AT,
    )


class FakeClient:
    """Stands in for AsyncFPLClient's entry fetches; records each request."""

    # run_baseline looks every fetcher up by endpoint name before choosing one, so the others must exist.
    get_bootstrap_raw = get_fixtures_raw = get_event_status_raw = get_gameweek_live_raw = get_element_summary_raw = None

    def __init__(self) -> None:
        self.picks_requests: list[tuple[int, int]] = []
        self.history_requests: list[int] = []

    async def __aenter__(self):
        return self

    async def __aexit__(self, *_):
        return None

    async def get_entry_picks_raw(self, gw: int, entry_id: int) -> RawResponse:
        self.picks_requests.append((gw, entry_id))
        return _raw(payload_bytes("entry_picks_automatic_subs" if gw == 5 else "entry_picks_self"))

    async def get_entry_history_raw(self, entry_id: int) -> RawResponse:
        self.history_requests.append(entry_id)
        return _raw(payload_bytes("entry_history_self"))


def _run(tmp_path, *argv: str, client: FakeClient | None = None) -> tuple[int, FakeClient]:
    client = client or FakeClient()
    args = build_parser().parse_args(["baseline", *argv, "--out-dir", str(tmp_path)])
    return run_baseline(args, client=client), client


class TestEntryPicksBaseline:

    @pytest.mark.covers("#116 AC8")
    def test_entry_picks_baseline_samples_every_entry_in_every_gameweek(self, tmp_path):
        code, client = _run(tmp_path, "entry-picks", "--entries", "8450051,4685107", "--gameweeks", "4,5")

        assert code == 0
        assert sorted(client.picks_requests) == [(4, 4685107), (4, 8450051), (5, 4685107), (5, 8450051)]
        written = json.loads((tmp_path / "entry-picks.json").read_text())
        assert written["endpoint"] == "entry-picks"
        assert written["samples"] == [
            "entry-picks/4685107/4", "entry-picks/4685107/5", "entry-picks/8450051/4", "entry-picks/8450051/5",
        ]
        assert written["paths"]["$.automatic_subs[].element_in"] == ["number"], "the non-empty automatic_subs week is sampled"

    @pytest.mark.covers("#116 AC8")
    @pytest.mark.parametrize("argv", [
        ["entry-picks", "--gameweeks", "1"],
        ["entry-picks", "--entries", "8450051"],
    ], ids=["no_entries", "no_gameweeks"])
    def test_entry_picks_baseline_needs_both_entries_and_gameweeks(self, tmp_path, argv):
        code, client = _run(tmp_path, *argv)

        assert code == 1
        assert client.picks_requests == []
        assert not (tmp_path / "entry-picks.json").exists()


class TestEntryHistoryBaseline:

    @pytest.mark.covers("#116 AC8")
    def test_entry_history_baseline_can_be_built_with_entries_alone(self, tmp_path):
        code, client = _run(tmp_path, "entry-history", "--entries", "8450051,4685107")

        assert code == 0
        assert client.history_requests == [8450051, 4685107]
        written = json.loads((tmp_path / "entry-history.json").read_text())
        assert written["endpoint"] == "entry-history"
        assert written["samples"] == ["entry-history/4685107", "entry-history/8450051"]
        assert written["paths"]["$.current[].points"] == ["number"]

    @pytest.mark.covers("#116 AC8")
    def test_entry_history_baseline_needs_entries(self, tmp_path):
        code, client = _run(tmp_path, "entry-history")

        assert code == 1
        assert client.history_requests == []
