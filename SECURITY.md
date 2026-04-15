# Security Policy

## Supported Use

This project is maintained as an open-source ingestion tool for Fantasy Premier League data.

## Reporting A Vulnerability

Please do not open a public issue for a suspected security problem.

Instead, report it privately to the maintainer at:

- safari.gisa@gmail.com

Include:

- a short description of the issue
- steps to reproduce it
- potential impact
- any suggested remediation, if available

The maintainer will review reports and respond as time allows. If the issue is confirmed, a fix and disclosure plan will be coordinated before broader publication when possible.

## Scope Notes

Security-sensitive areas may include:

- dependency and packaging configuration
- CLI behavior that writes local files
- unsafe handling of external input or HTTP responses
- secrets or credential handling if added in the future
