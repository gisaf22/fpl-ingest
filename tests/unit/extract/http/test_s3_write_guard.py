"""Unit tests for the S3 write guard in ``S3Backend``.

Building a real boto3 client is refused outside CI unless the local override
is set. Run 20260902T163935Z-07eb06 wrote 651 objects to the production bucket
from outside CI; this guard is what would have stopped it.
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from fpl_ingest.extract.http.s3_backend import (
    LOCAL_S3_OVERRIDE_ENV,
    S3Backend,
    S3WriteNotAllowedError,
)

_SHA = "0ce5e3c855bea1827cf38893a9ba2c97ffa3c45e"


@pytest.fixture
def no_ci(monkeypatch):
    for name in ("GITHUB_ACTIONS", "GITHUB_SHA", LOCAL_S3_OVERRIDE_ENV):
        monkeypatch.delenv(name, raising=False)


@pytest.fixture
def default_client():
    with patch("fpl_ingest.extract.http.s3_backend._default_client") as factory:
        factory.return_value = MagicMock(name="boto3-s3-client")
        yield factory


class TestRefused:

    def test_outside_ci_without_override_raises_before_building_a_client(self, no_ci, default_client):
        with pytest.raises(S3WriteNotAllowedError) as exc:
            S3Backend("fpl-data-safari")
        default_client.assert_not_called()
        assert LOCAL_S3_OVERRIDE_ENV in str(exc.value)

    def test_ci_without_sha_raises(self, no_ci, monkeypatch, default_client):
        monkeypatch.setenv("GITHUB_ACTIONS", "true")
        with pytest.raises(S3WriteNotAllowedError):
            S3Backend("fpl-data-safari")
        default_client.assert_not_called()

    def test_ci_with_malformed_sha_raises(self, no_ci, monkeypatch, default_client):
        monkeypatch.setenv("GITHUB_ACTIONS", "true")
        monkeypatch.setenv("GITHUB_SHA", "not-a-sha")
        with pytest.raises(S3WriteNotAllowedError):
            S3Backend("fpl-data-safari")

    def test_sha_without_ci_flag_raises(self, no_ci, monkeypatch, default_client):
        monkeypatch.setenv("GITHUB_SHA", _SHA)
        with pytest.raises(S3WriteNotAllowedError):
            S3Backend("fpl-data-safari")

    @pytest.mark.parametrize("value", ["0", "", "true", "yes"])
    def test_override_must_be_exactly_1(self, no_ci, monkeypatch, default_client, value):
        monkeypatch.setenv(LOCAL_S3_OVERRIDE_ENV, value)
        with pytest.raises(S3WriteNotAllowedError):
            S3Backend("fpl-data-safari")


class TestAllowed:

    def test_ci_with_sha(self, no_ci, monkeypatch, default_client):
        monkeypatch.setenv("GITHUB_ACTIONS", "true")
        monkeypatch.setenv("GITHUB_SHA", _SHA)
        backend = S3Backend("fpl-data-safari")
        default_client.assert_called_once()
        assert backend.bucket == "fpl-data-safari"

    def test_local_override(self, no_ci, monkeypatch, default_client):
        monkeypatch.setenv(LOCAL_S3_OVERRIDE_ENV, "1")
        S3Backend("fpl-data-safari")
        default_client.assert_called_once()

    def test_injected_client_is_not_guarded(self, no_ci, default_client):
        # Tests inject fake clients; only building a real boto3 client is guarded.
        fake = MagicMock()
        backend = S3Backend("fpl-data-safari", client=fake)
        default_client.assert_not_called()
        assert backend._client is fake
