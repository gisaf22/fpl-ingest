"""Where a run came from, for the manifest's ``origin`` block (#75).

``kind`` and the three Actions fields are a self-report read from the
environment every Actions job sets; anyone can export them on a laptop, so
they are for audit, not enforcement (#74 owns that). ``aws_principal`` is the
name STS resolves the run's credentials to, which is harder to fake.

Provenance must never fail a run: an STS error records ``None`` and logs a
warning, like ``git_sha``.
"""

from __future__ import annotations

import logging
import os
from collections.abc import Mapping
from typing import Any

ORIGIN_CI = "ci"
ORIGIN_LOCAL = "local"


def detect_origin(
    *,
    environ: Mapping[str, str] | None = None,
    sts_client: Any | None = None,
    logger: logging.Logger,
) -> dict[str, str | None]:
    """Return the run's ``origin`` block.

    Args:
        environ: Environment to read; defaults to ``os.environ``.
        sts_client: boto3 STS client, primarily for tests. Defaults to a real
            client built inside the error guard.
        logger: Receives the warning when the principal cannot be read.
    """
    env = os.environ if environ is None else environ
    ci = env.get("GITHUB_ACTIONS") == "true"
    return {
        "kind": ORIGIN_CI if ci else ORIGIN_LOCAL,
        "workflow": env.get("GITHUB_WORKFLOW") if ci else None,
        "ref": env.get("GITHUB_REF") if ci else None,
        "github_run_id": env.get("GITHUB_RUN_ID") if ci else None,
        "aws_principal": _aws_principal(sts_client, logger),
    }


def principal_name(arn: str) -> str:
    """Return the role or user name in a caller ARN, or ``"root"``.

    ``arn:aws:sts::<acct>:assumed-role/<role>/<session>`` -> ``<role>``;
    ``arn:aws:iam::<acct>:user/<path>/<name>`` -> ``<name>``;
    ``arn:aws:iam::<acct>:root`` -> ``"root"``. The account ID and session
    name are dropped.
    """
    resource = arn.split(":", 5)[5]
    if resource == "root":
        return "root"
    kind, _, rest = resource.partition("/")
    if kind == "assumed-role":
        return rest.split("/", 1)[0]
    if kind == "user":
        return rest.rsplit("/", 1)[-1]
    raise ValueError(f"unrecognised caller ARN resource type: {kind!r}")


def _aws_principal(sts_client: Any | None, logger: logging.Logger) -> str | None:
    try:
        client = sts_client if sts_client is not None else _default_sts_client()
        return principal_name(client.get_caller_identity()["Arn"])
    except Exception as exc:  # noqa: BLE001 - provenance must never fail a run
        logger.warning("Could not determine aws_principal for manifest: %s", type(exc).__name__)
        return None


def _default_sts_client() -> Any:
    import boto3
    from botocore.config import Config

    return boto3.client(
        "sts", config=Config(connect_timeout=5, read_timeout=5, retries={"max_attempts": 2})
    )
