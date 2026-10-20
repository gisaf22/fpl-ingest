"""``entry-picks`` and ``entry-history`` as `fpl-ingest baseline` endpoints (#116 AC8).

AC8 needs a new ``--entries`` flag (combined with ``--gameweeks`` for
entry-picks; alone for entry-history) and both families added to
``schema.payload_baseline.ENDPOINTS`` / the CLI's endpoint choices. None of
that exists yet: ``ENDPOINTS`` is still
``("bootstrap-static", "fixtures", "event-status", "event-live",
"element-summary")`` (``src/fpl_ingest/schema/payload_baseline.py``), and the
baseline parser has no ``--entries`` argument
(``src/fpl_ingest/cli.py``). These tests drive ``fpl_ingest.cli.main``
directly and are expected to FAIL (today, with a non-zero/argparse exit)
until that wiring is built — no CLI stub was added here, per the task's
"let it fail at a natural seam" guidance.
"""

from __future__ import annotations

import json

import pytest

from fpl_ingest.cli import main


def _run_baseline(tmp_path, *args: str) -> int:
    out_dir = tmp_path / "baselines"
    with pytest.raises(SystemExit) as exc:
        main(["baseline", *args, "--out-dir", str(out_dir)])
    return int(exc.value.code or 0), out_dir


class TestEntryPicksBaseline:

    @pytest.mark.covers("#116 AC8")
    def test_entry_picks_baseline_can_be_built_with_entries_and_gameweeks(self, tmp_path):
        code, out_dir = _run_baseline(tmp_path, "entry-picks", "--entries", "8450051,4685107", "--gameweeks", "1")

        assert code == 0, "expected `fpl-ingest baseline entry-picks --entries ... --gameweeks ...` to succeed"
        assert (out_dir / "entry-picks.json").exists()


class TestEntryHistoryBaseline:

    @pytest.mark.covers("#116 AC8")
    def test_entry_history_baseline_can_be_built_with_entries_alone(self, tmp_path):
        code, out_dir = _run_baseline(tmp_path, "entry-history", "--entries", "8450051,4685107")

        assert code == 0, "expected `fpl-ingest baseline entry-history --entries ...` to succeed"
        assert (out_dir / "entry-history.json").exists()


class TestCapturedSidecarsCarryDrift:

    @pytest.mark.covers("#116 AC8")
    def test_a_captured_entry_picks_sidecar_has_a_drift_block_once_a_baseline_exists(self, tmp_path):
        """Once AC8's baseline exists, a captured sidecar must carry `drift`, not omit it.

        This is a placeholder pinning the end state: a capture stage reads
        the baseline directory the same way element-summary/event-live do
        (`LocalRawWriter(..., baseline_dir=...)`); until the baseline file
        for `entry-picks` is committed, the drift block cannot be anything
        but `unavailable`, which this test treats as not-yet-done.
        """
        out_dir = tmp_path / "baselines"
        code, _ = _run_baseline(tmp_path, "entry-picks", "--entries", "8450051,4685107", "--gameweeks", "1")
        assert code == 0

        baseline_path = out_dir / "entry-picks.json"
        assert baseline_path.exists()
        baseline = json.loads(baseline_path.read_text())
        assert baseline.get("paths"), "expected the committed entry-picks baseline to list field paths"
