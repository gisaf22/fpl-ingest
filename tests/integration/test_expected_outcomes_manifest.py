"""How expected invalid captures read in the finalized manifest (#115 AC12, plus proposed AC15)."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from itertools import count

import pytest

from fpl_ingest.extract.http.client import RawResponse
from fpl_ingest.extract.http.local_writer import LocalRawWriter
from fpl_ingest.extract.http.response_router import route_response

NO_PICKS = {"ok": False, "checks": [], "failures": ["current_is_empty"], "record_count": None, "outcome": "no_picks"}
PASSING = {"ok": True, "checks": [], "failures": [], "record_count": 1}
MALFORMED = {"ok": False, "checks": [], "failures": ["current_is_list: missing"], "record_count": None}
_IDS = count()


def _response(status: int) -> RawResponse:
    at = datetime(2026, 10, 19, 8, 0, 0, tzinfo=timezone.utc)
    return RawResponse(url="https://x/api/entry/1/history/", status=status, headers={}, body=b"{}",
                       requested_at=at, received_at=at + timedelta(seconds=1))


def _send(writer, status, shape, **declared):
    return route_response(writer, f"entry-history/2026-27/{next(_IDS)}", _response(status), shape, **declared)


def _mixed_run(tmp_path):
    writer = LocalRawWriter(tmp_path / "raw", "fpl")
    _send(writer, 200, PASSING)
    _send(writer, 404, MALFORMED, absence_ok=True, expected=frozenset({"not_found"}))
    _send(writer, 200, NO_PICKS, expected=frozenset({"no_picks"}))
    _send(writer, 200, MALFORMED, expected=frozenset({"no_picks"}))   # malformed, not the declared outcome
    _send(writer, 403, MALFORMED)
    return writer.finalize("PARTIAL").manifest


@pytest.mark.covers("#115 AC12")
def test_expected_is_true_on_exactly_the_expected_invalid_captures(tmp_path):
    manifest = _mixed_run(tmp_path)

    flagged = [c for c in manifest["captures"] if c.get("expected")]
    assert sorted(c["class"] for c in flagged) == ["not_found", "shape_invalid"]
    assert all(c["expected"] is True for c in flagged)
    assert all("expected" not in c for c in manifest["captures"] if c not in flagged), "absent, not false"


@pytest.mark.covers("#115 AC12")
def test_the_expected_sidecars_carry_the_flag_and_the_others_do_not(tmp_path):
    import json

    _mixed_run(tmp_path)

    sidecars = [json.loads(p.read_text()) for p in (tmp_path / "raw" / "fpl" / "_invalid").rglob("metadata.json")]
    assert sorted(s["class"] for s in sidecars if s.get("expected")) == ["not_found", "shape_invalid"]
    assert len([s for s in sidecars if "expected" not in s]) == 2


@pytest.mark.covers("#115 AC12")
def test_the_endpoint_block_accounts_for_every_attempt(tmp_path):
    endpoint = _mixed_run(tmp_path)["endpoints"]["entry-history"]

    assert endpoint["expected"] == 2
    assert endpoint["usable"] + endpoint["expected"] + endpoint["failed"] == endpoint["attempted"] == 5
    assert endpoint["failed"] == 2


@pytest.mark.covers("#115 AC12")
def test_invalid_counts_still_count_expected_captures(tmp_path):
    counts = _mixed_run(tmp_path)["invalid_counts"]

    assert counts == {"not_found": 1, "shape_invalid": 2, "http_error": 1}


@pytest.mark.covers("#115 AC12")
def test_a_run_with_no_expected_outcome_has_no_expected_key_anywhere(tmp_path):
    writer = LocalRawWriter(tmp_path / "raw", "fpl")
    _send(writer, 200, PASSING)
    _send(writer, 403, MALFORMED)

    manifest = writer.finalize("PARTIAL").manifest

    assert "expected" not in manifest["endpoints"]["entry-history"]
    assert all("expected" not in c for c in manifest["captures"])


@pytest.mark.covers("#115 AC15")
def test_an_endpoint_whose_every_attempt_was_expected_is_success(tmp_path):
    """Proposed AC15: nothing failed, so nothing usable must not read as FAILED."""
    writer = LocalRawWriter(tmp_path / "raw", "fpl")
    _send(writer, 404, MALFORMED, absence_ok=True, expected=frozenset({"not_found"}))
    _send(writer, 200, NO_PICKS, expected=frozenset({"no_picks"}))

    endpoint = writer.finalize("SUCCESS").manifest["endpoints"]["entry-history"]

    assert (endpoint["usable"], endpoint["failed"], endpoint["expected"]) == (0, 0, 2)
    assert endpoint["outcome"] == "SUCCESS"
