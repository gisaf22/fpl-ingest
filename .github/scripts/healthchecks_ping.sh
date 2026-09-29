#!/usr/bin/env bash
# Ping a healthchecks.io check with a job's outcome (#57).
#
# Usage: healthchecks_ping.sh <job-status>
#   HEALTHCHECKS_PING_URL  the check's ping URL, from a repository secret
#   <job-status>           GitHub's job.status: success, failure or cancelled
#
# `success` pings the plain URL; anything else pings <url>/fail, so a failed
# run alerts at once rather than waiting out the check's grace.
#
# This script must never fail the job that calls it: it always exits 0.
# - No URL (a fork or fresh clone without the secret): no request, a notice.
# - A failed request (healthchecks.io down, a timeout, an HTTP error): a
#   warning. The check then sees no ping and alerts on absence after its
#   grace, which is the accepted false positive.
#
# It is plain bash and curl, with no Python, so it still runs when
# `uv sync` has failed. The URL is never printed.

set -u

url="${HEALTHCHECKS_PING_URL:-}"
status="${1:-}"

if [ -z "$url" ]; then
  echo "::notice::healthchecks.io ping skipped: HEALTHCHECKS_PING_URL is not set"
  exit 0
fi

if [ "$status" = "success" ]; then
  target="$url"
else
  target="$url/fail"
fi

if curl -fsS --max-time 10 --retry 3 -o /dev/null "$target"; then
  echo "healthchecks.io pinged (job status: ${status:-unknown})"
else
  echo "::warning::healthchecks.io ping failed (job status: ${status:-unknown}); the check will alert on absence after its grace"
fi

exit 0
