"""Run origin in the manifest (#75).

Every manifest records where its run came from: CI or local, the Actions run
it belongs to, and the AWS principal its credentials resolve to. Contract
2.2.0 makes the block required.
"""

from __future__ import annotations

import json
import logging
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest
from jsonschema import Draft202012Validator

from fpl_ingest.extract.http.local_writer import LocalRawWriter
from fpl_ingest.orchestration.origin import detect_origin
from fpl_ingest.orchestration.run_status import classify_run

RUN_START = datetime(2026, 10, 3, 19, 0, 5, tzinfo=timezone.utc)
RUN_ID = "20261003T190005Z-b71c2e"
SHAPE_OK = {"ok": True, "checks": ["top_level_is_object"], "failures": []}

CI_ENV = {
    "GITHUB_ACTIONS": "true",
    "GITHUB_WORKFLOW": "Pre-deadline run",
    "GITHUB_REF": "refs/heads/main",
    "GITHUB_RUN_ID": "36500000001",
}
ROLE_ARN = "arn:aws:sts::111122223333:assumed-role/github-actions-fpl-ingest/GitHubActions"
USER_ARN = "arn:aws:iam::111122223333:user/safari-admin"
ROOT_ARN = "arn:aws:iam::111122223333:root"

SCHEMA_PATH = (
    Path(__file__).resolve().parents[4] / "schemas" / "raw-contract" / "2.2.0" / "manifest.schema.json"
)

logger = logging.getLogger("test_manifest_origin")


class FakeSTS:
    def __init__(self, arn: str | None = None, error: Exception | None = None):
        self._arn = arn
        self._error = error

    def get_caller_identity(self):
        if self._error is not None:
            raise self._error
        return {"UserId": "AIDAEXAMPLE", "Account": "111122223333", "Arn": self._arn}


def _origin(environ, arn=ROLE_ARN):
    return detect_origin(environ=environ, sts_client=FakeSTS(arn), logger=logger)


def _writer(tmp_path: Path, origin) -> LocalRawWriter:
    return LocalRawWriter(tmp_path, "fpl", run_id=RUN_ID, started_at=RUN_START, origin=origin)


def _write(writer: LocalRawWriter) -> None:
    writer.write_object(
        "fixtures",
        b"[]",
        request_url="https://fantasy.premierleague.com/api/fixtures/",
        requested_at=RUN_START,
        received_at=RUN_START + timedelta(seconds=1),
        http_status=200,
        shape_validation=SHAPE_OK,
        season="2026-27",
    )


def _finalized(tmp_path: Path, origin) -> dict:
    writer = _writer(tmp_path, origin)
    _write(writer)
    return writer.finalize(classify_run(writer.endpoint_outcomes)).manifest


def _on_disk(tmp_path: Path) -> dict:
    (path,) = (tmp_path / "fpl" / "_manifests").rglob("manifest.json")
    return json.loads(path.read_text(encoding="utf-8"))


def _schema_errors(manifest: dict) -> list[str]:
    schema = json.loads(SCHEMA_PATH.read_text(encoding="utf-8"))
    Draft202012Validator.check_schema(schema)
    return [e.message for e in Draft202012Validator(schema).iter_errors(manifest)]


# -- AC1 ---------------------------------------------------------------------


@pytest.mark.covers("#75 AC1")
def test_a_ci_run_records_its_workflow_ref_and_actions_run(tmp_path):
    manifest = _finalized(tmp_path, _origin(CI_ENV))

    assert manifest["origin"] == {
        "kind": "ci",
        "workflow": "Pre-deadline run",
        "ref": "refs/heads/main",
        "github_run_id": "36500000001",
        "aws_principal": "github-actions-fpl-ingest",
    }


@pytest.mark.covers("#75 AC1")
def test_a_ci_run_on_another_branch_records_that_branch(tmp_path):
    env = {**CI_ENV, "GITHUB_REF": "refs/heads/feat/x", "GITHUB_RUN_ID": "42"}

    manifest = _finalized(tmp_path, _origin(env))

    assert manifest["origin"]["ref"] == "refs/heads/feat/x"
    assert manifest["origin"]["github_run_id"] == "42"


# -- AC2 ---------------------------------------------------------------------


@pytest.mark.covers("#75 AC2")
def test_a_run_outside_actions_is_local_with_null_ci_fields(tmp_path):
    manifest = _finalized(tmp_path, _origin({}, arn=USER_ARN))

    assert manifest["origin"] == {
        "kind": "local",
        "workflow": None,
        "ref": None,
        "github_run_id": None,
        "aws_principal": "safari-admin",
    }


@pytest.mark.covers("#75 AC2")
@pytest.mark.parametrize("actions_value", [None, "false", "", "TRUE"])
def test_stray_github_variables_without_actions_true_still_read_local(tmp_path, actions_value):
    env = {k: v for k, v in CI_ENV.items() if k != "GITHUB_ACTIONS"}
    if actions_value is not None:
        env["GITHUB_ACTIONS"] = actions_value

    manifest = _finalized(tmp_path, _origin(env))

    assert manifest["origin"]["kind"] == "local"
    assert manifest["origin"]["workflow"] is None
    assert manifest["origin"]["ref"] is None
    assert manifest["origin"]["github_run_id"] is None


@pytest.mark.covers("#75 AC2")
def test_a_local_run_is_local_whatever_its_git_sha_or_trigger_says(tmp_path):
    writer = _writer(tmp_path, _origin({}))
    _write(writer)

    manifest = writer.finalize(
        classify_run(writer.endpoint_outcomes), git_sha="a" * 40, trigger="scheduled"
    ).manifest

    assert manifest["origin"]["kind"] == "local"


# -- AC3 ---------------------------------------------------------------------


@pytest.mark.covers("#75 AC3")
def test_the_in_progress_manifest_on_disk_carries_the_origin(tmp_path):
    origin = _origin(CI_ENV)
    writer = _writer(tmp_path, origin)
    _write(writer)

    on_disk = _on_disk(tmp_path)

    assert on_disk["status"] == "IN_PROGRESS"
    assert on_disk["origin"] == origin


@pytest.mark.covers("#75 AC3")
def test_the_manifest_snapshot_carries_the_origin(tmp_path):
    origin = _origin({}, arn=USER_ARN)
    writer = _writer(tmp_path, origin)
    _write(writer)

    assert writer.manifest_snapshot["origin"] == origin


# -- AC4 ---------------------------------------------------------------------


@pytest.mark.covers("#75 AC4")
@pytest.mark.parametrize("env", [CI_ENV, {}], ids=["ci", "local"])
def test_finalized_manifests_validate_against_2_2_0(tmp_path, env):
    manifest = _finalized(tmp_path, _origin(env))

    assert manifest["raw_contract_version"] == "2.2.0"
    assert _schema_errors(manifest) == []


@pytest.mark.covers("#75 AC4")
def test_an_in_progress_manifest_validates_against_2_2_0(tmp_path):
    writer = _writer(tmp_path, _origin(CI_ENV))
    _write(writer)

    assert _schema_errors(_on_disk(tmp_path)) == []


@pytest.mark.covers("#75 AC4")
def test_a_manifest_with_a_null_principal_validates(tmp_path):
    origin = detect_origin(environ={}, sts_client=FakeSTS(error=RuntimeError("no creds")), logger=logger)

    assert _schema_errors(_finalized(tmp_path, origin)) == []


@pytest.mark.covers("#75 AC4")
def test_a_manifest_without_origin_fails_validation(tmp_path):
    manifest = _finalized(tmp_path, _origin(CI_ENV))
    del manifest["origin"]

    assert _schema_errors(manifest)


@pytest.mark.covers("#75 AC4")
def test_a_manifest_without_aws_principal_fails_validation(tmp_path):
    manifest = _finalized(tmp_path, _origin(CI_ENV))
    del manifest["origin"]["aws_principal"]

    assert _schema_errors(manifest)


@pytest.mark.covers("#75 AC4")
@pytest.mark.parametrize("field", ["workflow", "ref", "github_run_id"])
def test_a_local_origin_with_a_ci_field_set_fails_validation(tmp_path, field):
    manifest = _finalized(tmp_path, _origin({}))
    manifest["origin"][field] = "refs/heads/main"

    assert _schema_errors(manifest)


@pytest.mark.covers("#75 AC4")
@pytest.mark.parametrize("field", ["workflow", "ref", "github_run_id"])
def test_a_ci_origin_with_a_null_ci_field_fails_validation(tmp_path, field):
    manifest = _finalized(tmp_path, _origin(CI_ENV))
    manifest["origin"][field] = None

    assert _schema_errors(manifest)


@pytest.mark.covers("#75 AC4")
def test_an_unknown_origin_kind_fails_validation(tmp_path):
    manifest = _finalized(tmp_path, _origin(CI_ENV))
    manifest["origin"]["kind"] = "laptop"

    assert _schema_errors(manifest)


# -- AC6 ---------------------------------------------------------------------


@pytest.mark.covers("#75 AC6")
@pytest.mark.parametrize(
    ("arn", "expected"),
    [
        (ROLE_ARN, "github-actions-fpl-ingest"),
        ("arn:aws:sts::111122223333:assumed-role/some-role/some@session.name", "some-role"),
        (USER_ARN, "safari-admin"),
        ("arn:aws:iam::111122223333:user/team/ops/safari-admin", "safari-admin"),
        (ROOT_ARN, "root"),
    ],
    ids=["assumed-role", "assumed-role-email-session", "iam-user", "iam-user-with-path", "root"],
)
def test_the_principal_is_recorded_by_name_only(tmp_path, arn, expected):
    manifest = _finalized(tmp_path, _origin(CI_ENV, arn=arn))

    principal = manifest["origin"]["aws_principal"]
    assert principal == expected
    assert "111122223333" not in json.dumps(manifest)
    assert "arn:" not in principal


@pytest.mark.covers("#75 AC6")
def test_an_sts_failure_records_null_logs_a_warning_and_the_run_completes(tmp_path, caplog):
    sts = FakeSTS(error=RuntimeError("Unable to locate credentials"))

    with caplog.at_level(logging.WARNING, logger="test_manifest_origin"):
        origin = detect_origin(environ=CI_ENV, sts_client=sts, logger=logger)
    manifest = _finalized(tmp_path, origin)

    assert origin["aws_principal"] is None
    assert origin["kind"] == "ci"
    assert any(r.levelno == logging.WARNING for r in caplog.records)
    assert manifest["status"] != "IN_PROGRESS"
    assert manifest["origin"]["aws_principal"] is None
