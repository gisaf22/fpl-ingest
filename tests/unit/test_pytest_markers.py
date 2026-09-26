"""The suite's marker configuration: ``covers`` is registered and markers are strict.

``covers("#<issue> AC<n>")`` traces a test to the acceptance criterion it
proves. With ``--strict-markers`` a misspelled marker fails collection instead
of silently dropping that trace. The collection tests run pytest in a
subprocess against a throwaway test file, using this repo's ``pyproject.toml``
as the config, so they exercise the real marker settings.
"""

from __future__ import annotations

import subprocess
import sys
import textwrap
from pathlib import Path

import pytest

_PYPROJECT = Path(__file__).resolve().parents[2] / "pyproject.toml"


def _collect(tmp_path: Path, source: str) -> subprocess.CompletedProcess[str]:
    test_file = tmp_path / "test_sample.py"
    test_file.write_text(textwrap.dedent(source))
    return subprocess.run(
        [
            sys.executable, "-m", "pytest",
            "-c", str(_PYPROJECT),
            "--rootdir", str(tmp_path),
            "-p", "no:cacheprovider",
            str(test_file),
        ],
        capture_output=True,
        text=True,
    )


_COVERS_USAGES = {
    "with an issue and criterion": """
        import pytest

        @pytest.mark.covers("#22 AC1")
        def test_sample():
            pass
    """,
    "with no argument": """
        import pytest

        @pytest.mark.covers
        def test_sample():
            pass
    """,
    "several times on one test": """
        import pytest

        @pytest.mark.covers("#22 AC1")
        @pytest.mark.covers("#22 AC2")
        def test_sample():
            pass
    """,
    "on a class": """
        import pytest

        @pytest.mark.covers("#22 AC1")
        class TestSample:
            def test_one(self):
                pass

            def test_two(self):
                pass
    """,
}


@pytest.mark.covers("#22 AC1")
@pytest.mark.parametrize("source", _COVERS_USAGES.values(), ids=_COVERS_USAGES.keys())
def test_a_test_marked_covers_collects_cleanly(tmp_path, source):
    result = _collect(tmp_path, source)
    output = result.stdout + result.stderr
    assert result.returncode == 0, output
    assert "PytestUnknownMarkWarning" not in output


@pytest.mark.covers("#22 AC2")
def test_a_misspelled_marker_fails_collection_naming_the_marker(tmp_path):
    result = _collect(tmp_path, """
        import pytest

        @pytest.mark.cover("#22 AC2")
        def test_sample():
            pass
    """)
    output = result.stdout + result.stderr
    assert result.returncode != 0, output
    assert "'cover' not found in `markers` configuration option" in output


def _registered_markers(pytestconfig) -> dict[str, str]:
    markers = {}
    for line in pytestconfig.getini("markers"):
        signature, _, description = line.partition(":")
        markers[signature.split("(")[0].strip()] = description.strip()
    return markers


@pytest.mark.covers("#22 AC4")
def test_every_registered_marker_has_a_description(pytestconfig):
    markers = _registered_markers(pytestconfig)
    expected = {
        "unit", "integration", "e2e", "dgw", "bgw", "edge_case",
        "regression", "perf", "asyncio", "covers",
    }
    assert expected <= markers.keys()
    assert {name for name, description in markers.items() if not description} == set()


@pytest.mark.covers("#22 AC4")
def test_the_covers_description_gives_its_format(pytestconfig):
    assert 'covers("#<issue> AC<n>")' in _registered_markers(pytestconfig)["covers"]
