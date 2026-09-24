"""Local-run safety at the CLI: the S3 write guard and strict argument parsing.

Both must stop a run before any FPL API client is built. The client class is
patched with a mock, and every test asserts that mock was never called.
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from fpl_ingest.cli import build_parser, main
from fpl_ingest.extract.http.s3_backend import LOCAL_S3_OVERRIDE_ENV, S3WriteNotAllowedError
from tests.support.cli_fakes import _make_async_client, _run


@pytest.fixture
def no_ci(monkeypatch):
    for name in ("GITHUB_ACTIONS", "GITHUB_SHA", LOCAL_S3_OVERRIDE_ENV):
        monkeypatch.delenv(name, raising=False)


class TestS3GuardAtTheCli:

    @pytest.mark.parametrize("command", [["run"], ["pre-deadline"], ["pre-deadline", "--force"]])
    def test_s3_outside_ci_stops_before_any_client(self, no_ci, monkeypatch, tmp_path, command):
        monkeypatch.setenv("FPL_STORAGE_BACKEND", "s3")
        monkeypatch.setenv("FPL_S3_BUCKET", "fpl-data-safari")
        client_cls = MagicMock()
        with (
            patch("fpl_ingest.orchestration.runner.AsyncFPLClient", client_cls),
            patch("fpl_ingest.extract.http.s3_backend._default_client") as boto,
        ):
            with pytest.raises(S3WriteNotAllowedError):
                main(["--raw-dir", str(tmp_path / "raw"), *command])
        client_cls.assert_not_called()
        boto.assert_not_called()
        assert not (tmp_path / "raw").exists()

    def test_local_backend_is_never_guarded(self, no_ci, monkeypatch, tmp_path):
        monkeypatch.setenv("FPL_STORAGE_BACKEND", "local")
        raw = _run(["run"], _make_async_client(), tmp_path)
        assert any((raw / "fpl" / "_manifests").rglob("manifest.json"))


class TestStrictParsing:

    @pytest.mark.parametrize(
        "argv",
        [
            ["run", "--stirct"],                      # typo of --strict
            ["run", "--skip-player-histories"],       # removed in 46d3c19
            ["--skip-player-histories"],
            ["pre-deadline", "--froce"],
        ],
    )
    def test_unknown_flag_exits_non_zero_before_any_client(self, no_ci, monkeypatch, tmp_path, argv):
        monkeypatch.setenv("FPL_STORAGE_BACKEND", "local")
        client_cls = MagicMock()
        with patch("fpl_ingest.orchestration.runner.AsyncFPLClient", client_cls):
            with pytest.raises(SystemExit) as exc:
                main(["--raw-dir", str(tmp_path / "raw"), *argv])
        assert exc.value.code != 0
        client_cls.assert_not_called()
        assert not (tmp_path / "raw").exists()

    @pytest.mark.parametrize(
        "argv",
        [
            [],
            ["run"],
            # scheduled_run_daily.yml, both expansions of its --trigger expression
            ["run", "--trigger", "scheduled"],
            ["run", "--trigger", "manual"],
            # scheduled_run_pre_deadline.yml, both expansions of its force expression
            ["pre-deadline"],
            ["pre-deadline", "--force"],
            ["smoke-test"],
            ["inspect"],
            ["inspect", "--list", "--last", "3"],
            ["--raw-dir", "/tmp/x", "--strict"],
            ["--raw-dir", "/tmp/x", "run", "--strict", "--verbose"],
            ["run", "--raw-dir", "/tmp/x", "--rate", "5"],
            ["--rate", "99"],
            ["--raw-dir", "/tmp/x", "smoke-test"],
            ["--raw-dir", "/tmp/x", "pre-deadline", "-v"],
        ],
    )
    def test_valid_invocations_still_parse(self, argv):
        build_parser().parse_args(argv)
