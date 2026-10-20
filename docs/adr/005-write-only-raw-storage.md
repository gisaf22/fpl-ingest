# ADR 005 — Write-only raw storage and its one read exception

Date: 2026-10-19
Status: Accepted (#54, recording a rule in force since the S3 migration and #102).

## Context

Ingestion captures FPL responses verbatim; interpreting them is fpl-warehouse's
job. If ingest could read back what it wrote, it would start deciding things from
captured content, and a skip or finality check could drift from the backend that is
actually in use. That happened once: a skip check looked at a hardcoded local path
instead of the active backend (commit `8f6b7bb`).

## Decision

**`RawStorageBackend` is write-only by design.** Its protocol offers `put_bytes`,
`location` and `exists_prefix`. Skip, resume and finality logic is expressed as
existence checks against the active backend (`LocalRawWriter.backend`), never as
content reads. The `_settlement` markers and the `_invalid` trees (ADR 004) are
designed for this: their mere existence is the whole signal.

**The one exception is the manager panel file** (#100 D5, #102). `read_panel_bytes`
reads back `fpl/_panels/{season}/manager-panel.json`, the job's own panel, and
refuses any other key (`raw_keys.validate_panel_key`). Only
`stages/manager_panel.py` calls it. Tests fail if any other module reads through the
backend, or if the ingestion package reads S3 outside `s3_backend.py`. `backfill.py`
is a separate catalog tool with its own client, outside this rule.

## Alternatives Considered

- **Reading sidecars to classify or resume.** Rejected for #54 (ADR 004) and #103:
  resume uses `exists_prefix` on the tree for each class instead.
- **A resume cursor file.** Rejected for #103: it would be a second read exception.
- **Sampling the panel from the file elsewhere** (for example for baselines).
  Rejected: a second read exception that couples a schema tool to the draw.

## Consequences

- A new rule that seems to need a content read is a design question to settle
  here first, not a second exception to add quietly.
- State that must survive between runs is kept as the existence of an object.
