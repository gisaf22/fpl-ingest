"""Where a backfilled capture's shape verdict comes from (#66 AC5; #63 D7, D10).

A sidecar verdict (a dict with ``ok``) is trusted as recorded. Anything else is
revalidated with the current validator for the capture's endpoint.
"""

from __future__ import annotations

import json

import pytest

from fpl_ingest.backfill import shape_for_capture
from tests.support.backfill_tree import MALFORMED, SHAPE_INVALID, SHAPE_OK, VALIDATOR_VERSION
from tests.support.cli_fakes import MINIMAL_BOOTSTRAP, PLAYER_HISTORY_1

_VALID = json.dumps(PLAYER_HISTORY_1).encode()
_BROKEN = json.dumps(MALFORMED).encode()


def _sidecar(**fields) -> dict:
    return {"endpoint": "element-summary/1", "http_status": 200, **fields}


@pytest.mark.covers("#66 AC5")
@pytest.mark.parametrize(
    ("verdict", "payload", "expected_ok"),
    [
        pytest.param(SHAPE_OK, _BROKEN, True, id="ok-verdict-kept-even-for-a-bad-body"),
        pytest.param(SHAPE_INVALID, _VALID, False, id="failed-verdict-kept-even-for-a-good-body"),
    ],
)
def test_shape_source_prefers_sidecar(verdict, payload, expected_ok):
    result = shape_for_capture(
        "element-summary/1", _sidecar(shape_validation=verdict), payload,
        validator_version=VALIDATOR_VERSION,
    )

    assert result.shape_ok is expected_ok
    assert result.shape_source == "sidecar"
    assert result.validator_version is None


@pytest.mark.covers("#66 AC5")
@pytest.mark.parametrize(
    "sidecar",
    [
        pytest.param(None, id="no-sidecar"),
        pytest.param(_sidecar(), id="verdict-absent"),
        pytest.param(_sidecar(shape_validation=None), id="verdict-null"),
        pytest.param(_sidecar(shape_validation={"status": "pass"}), id="verdict-unrecognised"),
        pytest.param({"endpoint": "element-summary/1"}, id="history-sidecar-no-http-status"),
    ],
)
@pytest.mark.parametrize(
    ("payload", "expected_ok"),
    [pytest.param(_VALID, True, id="valid-body"), pytest.param(_BROKEN, False, id="malformed-body")],
)
def test_revalidate_when_no_sidecar_result(sidecar, payload, expected_ok):
    result = shape_for_capture("element-summary/1", sidecar, payload, validator_version=VALIDATOR_VERSION)

    assert result.shape_source == "revalidated"
    assert result.validator_version == VALIDATOR_VERSION
    assert result.shape_ok is expected_ok
    assert bool(result.failures) is (not expected_ok)


@pytest.mark.covers("#66 AC5")
@pytest.mark.parametrize(
    ("endpoint", "payload"),
    [
        pytest.param("bootstrap-static", MINIMAL_BOOTSTRAP, id="bootstrap-static"),
        pytest.param("fixtures", [{"id": 1, "team_h": 11, "team_a": 13, "event": 1}], id="fixtures"),
        pytest.param("event-status", {"status": [], "leagues": ""}, id="event-status"),
        pytest.param("event-live/01", {"elements": [{"id": 1, "stats": {}, "explain": []}]}, id="event-live"),
        pytest.param("element-summary/115", PLAYER_HISTORY_1, id="element-summary"),
    ],
)
def test_revalidation_uses_the_endpoints_own_validator(endpoint, payload):
    good = shape_for_capture(endpoint, None, json.dumps(payload).encode(), validator_version=VALIDATOR_VERSION)
    bad = shape_for_capture(endpoint, None, b'"not an object or list"', validator_version=VALIDATOR_VERSION)

    assert good.shape_ok is True
    assert bad.shape_ok is False
