"""The ``smoke-test`` command is retired; the payload drift check supersedes it."""

from __future__ import annotations

import importlib

import pytest

from fpl_ingest import cli_formatters
from fpl_ingest.cli import build_parser


@pytest.mark.covers("#84 AC1")
@pytest.mark.parametrize("argv", [["smoke-test"], ["--raw-dir", "/tmp/x", "smoke-test"]])
def test_smoke_test_subcommand_is_rejected(argv):
    with pytest.raises(SystemExit) as exc:
        build_parser().parse_args(argv)
    assert exc.value.code == 2


@pytest.mark.covers("#84 AC1")
def test_smoke_test_module_is_gone():
    with pytest.raises(ModuleNotFoundError):
        importlib.import_module("fpl_ingest.schema.validation")


@pytest.mark.covers("#84 AC1")
@pytest.mark.parametrize("name", ["format_smoke_test_success", "format_smoke_test_failure"])
def test_smoke_test_formatters_are_gone(name):
    assert not hasattr(cli_formatters, name)
