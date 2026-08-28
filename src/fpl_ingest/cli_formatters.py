"""Human-readable output formatters for the fpl-ingest CLI.

Converts structured data from the store and smoke test into terminal-safe
strings. Each formatter is a pure function: no I/O, no logging, no side
effects. All CLI output paths pass through this module so formatting changes
stay localised here.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from fpl_ingest.schema.validation import SmokeTestResult


def format_run_detail(manifest: Mapping[str, Any]) -> str:
    """Format a single run manifest as a human-readable summary."""
    lines = [
        f"run_id:      {manifest.get('run_id')}",
        f"status:      {manifest.get('status')}",
        f"started_at:  {manifest.get('started_at')}",
        f"ended_at:    {manifest.get('ended_at')}",
        f"duration:    {manifest.get('duration_seconds')}s"
        if manifest.get("duration_seconds") is not None
        else "duration:    (in progress)",
        f"git_sha:     {manifest.get('git_sha') or '(unknown)'}",
    ]

    objects = manifest.get("objects") or {}
    if objects:
        lines.append("")
        lines.append("endpoints:")
        for endpoint, counts in sorted(objects.items()):
            counts = counts or {}
            lines.append(
                f"  {endpoint}: written={counts.get('written', 0)} "
                f"failed={counts.get('failed', 0)} bytes={counts.get('bytes', 0)}"
            )

    finality = manifest.get("finality")
    if finality:
        settled = sum(1 for info in finality.values() if info.get("bonus_added"))
        lines.append("")
        lines.append(f"finality:    {settled}/{len(finality)} gameweeks settled")

    failures = manifest.get("failures") or []
    if failures:
        lines.append("")
        lines.append("failures:")
        for failure in failures:
            lines.append(
                f"  {failure.get('endpoint')}: {failure.get('error_class')} — {failure.get('message')}"
            )

    return "\n".join(lines)


def format_run_list(manifests: Sequence[Mapping[str, Any]]) -> str:
    """Format a list of run manifests (newest first) as a compact table."""
    if not manifests:
        return "No runs recorded"

    headers = ("run_id", "status", "started_at", "duration_s", "endpoints", "failures")
    rows_data = [
        (
            str(m.get("run_id", "")),
            str(m.get("status", "")),
            str(m.get("started_at", "")),
            str(m.get("duration_seconds") if m.get("duration_seconds") is not None else "-"),
            str(len(m.get("objects") or {})),
            str(len(m.get("failures") or [])),
        )
        for m in manifests
    ]
    col_widths = [
        max(len(h), *(len(row[i]) for row in rows_data))
        for i, h in enumerate(headers)
    ]
    sep = "  "
    header_row = sep.join(h.ljust(col_widths[i]) for i, h in enumerate(headers))
    divider = sep.join("-" * w for w in col_widths)
    table_lines = [header_row, divider] + [
        sep.join(cell.ljust(col_widths[i]) for i, cell in enumerate(row))
        for row in rows_data
    ]
    return "\n".join(table_lines)


def format_smoke_test_success(result: SmokeTestResult) -> str:
    return "\n".join(
        [
            "Smoke test passed.",
            f"Checked endpoints: {', '.join(result.endpoints_checked)}",
            f"Sample size: {result.sample_size}",
        ]
    )


def format_smoke_test_failure(exc: BaseException) -> str:
    return f"Smoke test failed: {exc}"
