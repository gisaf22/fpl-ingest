"""Unit tests for ``readiness.ict_ready``, the shared ICT readiness predicate.

Both settlement markers (event-live, element-summary) are gated on it; the
stage tests cover the wiring, these cover the rule itself.
"""

from __future__ import annotations

import pytest

from fpl_ingest.extract.stages.readiness import ICT_FIELDS, ict_ready

pytestmark = pytest.mark.unit


def _row(minutes: object, *ict: object) -> dict:
    return {"minutes": minutes, **dict(zip(ICT_FIELDS, ict or ("20.6", "3.2", "6.0", "3.0")))}


ZERO = ("0.0", "0.0", "0.0", "0.0")


@pytest.mark.parametrize(
    "rows, ready",
    [
        pytest.param([_row(90)], True, id="populated"),
        pytest.param([_row(90, *ZERO)], False, id="played_all_zero"),
        pytest.param([_row(90), _row(3, *ZERO)], True, id="at_least_one_not_all"),
        pytest.param([_row(0, *ZERO), _row(0, *ZERO)], True, id="nobody_played_vacuous"),
        pytest.param([], True, id="no_rows_vacuous"),
        pytest.param([_row(90, "5.6", "0.0", "0.0", "0.0")], True, id="string_5.6"),
        pytest.param([_row(90, 0.0, 0.0, 0.0, 0.6)], True, id="float_values"),
        pytest.param([_row(90, 0, 0, 0, 0)], False, id="int_zeros"),
        pytest.param([_row("90")], True, id="string_minutes"),
        pytest.param([_row(90), {"minutes": 90}], False, id="played_row_missing_all_ict"),
        pytest.param([_row(90), _row(90, "n/a", "0", "0", "0")], False, id="unparseable_ict"),
        pytest.param([_row(90), _row(90, None, "0", "0", "0")], False, id="null_ict"),
        pytest.param([_row(90), _row(90, True, "0", "0", "0")], False, id="bool_ict"),
        pytest.param([_row(90), _row(90, "nan", "0", "0", "0")], False, id="nan_ict"),
        pytest.param([_row(90), {"influence": "1.0"}], False, id="missing_minutes"),
        pytest.param([_row(90), _row("ninety")], False, id="unparseable_minutes"),
        pytest.param([_row(90), None], False, id="non_mapping_row"),
        pytest.param([_row(0, "n/a")], True, id="unplayed_row_ict_is_not_judged"),
    ],
)
def test_ict_ready(rows, ready):
    assert ict_ready(rows) is ready


def test_ict_ready_accepts_a_one_shot_iterable():
    assert ict_ready(iter([_row(90)])) is True
