"""Unit tests for the storage-backend selection in config.py.

Covers the env-var priority chain for ``FPL_STORAGE_BACKEND`` /
``FPL_S3_BUCKET`` and their defaults, mirroring the existing ``raw_dir``
chain (flag → env → config file → default).
"""

from __future__ import annotations

from pathlib import Path

import pytest

from fpl_ingest import config as config_module
from fpl_ingest.config import default_config, resolve_config

pytestmark = pytest.mark.unit


@pytest.fixture(autouse=True)
def _isolated_config_file(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Point the ``~/.fpl/config.yaml`` reader at an empty tmp file.

    Without this, a real config file on the machine running the tests could
    leak into the resolution chain and make these tests non-deterministic.
    """
    monkeypatch.setattr(config_module, "_CONFIG_FILE", tmp_path / "config.yaml")


def test_default_backend_is_local(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("FPL_STORAGE_BACKEND", raising=False)
    monkeypatch.delenv("FPL_S3_BUCKET", raising=False)
    cfg = default_config()
    assert cfg.storage_backend == "local"
    assert cfg.s3_bucket is None


def test_env_selects_s3_backend_and_bucket(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("FPL_STORAGE_BACKEND", "s3")
    monkeypatch.setenv("FPL_S3_BUCKET", "fpl-data-safari")
    cfg = default_config()
    assert cfg.storage_backend == "s3"
    assert cfg.s3_bucket == "fpl-data-safari"


def test_resolve_config_applies_same_backend_chain(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("FPL_STORAGE_BACKEND", "s3")
    monkeypatch.setenv("FPL_S3_BUCKET", "fpl-data-safari")
    cfg = resolve_config(raw_dir=None)
    assert cfg.storage_backend == "s3"
    assert cfg.s3_bucket == "fpl-data-safari"
