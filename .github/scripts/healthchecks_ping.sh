#!/usr/bin/env bash
# Ping a healthchecks.io check with a scheduled job's outcome.
#
# One script, kept byte-identical in two repositories; change both together:
#   gisaf22/fpl-ingest     .github/scripts/healthchecks_ping.sh  (#57)
#   gisaf22/fpl-warehouse  scripts/healthchecks_ping.sh          (#79)
#
# Usage: healthchecks_ping.sh <job-status>
#   <job-status>           GitHub's job.status: success, failure or cancelled
#   HEALTHCHECKS_PING_URL  the check's ping URL, from a repository secret
#   GITHUB_TOKEN           optional; with `actions: read`, used to name the
#                          job's failed steps from the GitHub jobs API
#   DBT_RUN_RESULTS        optional; a dbt run_results.json whose first failing
#                          test is named in the body (fpl-warehouse only)
#
# `success` pings the plain URL; anything else pings <url>/fail, so a failed
# run alerts at once rather than waiting out the check's grace.
#
# Every ping POSTs a short plain-text body, which healthchecks.io stores with
# the ping event: the workflow name, the job status and the run URL, all from
# GitHub's default environment. A non-success ping also names the job's failed
# or cancelled steps and, when DBT_RUN_RESULTS is given, the first failing dbt
# test. The body holds only those fields: never the ping URL, the token or any
# credential. A detail that cannot be read is reported as unavailable and the
# ping is still sent.
#
# This script must never fail the job that calls it: it always exits 0.
# - No URL (a fork or fresh clone without the secret): no request, a notice.
# - A failed request (healthchecks.io down, a timeout, an HTTP error): a
#   warning. The check then sees no ping and alerts on absence after its
#   grace, which is the accepted false positive.
#
# It is plain bash, curl and jq (both on every GitHub-hosted runner), with no
# Python, so it still runs when `uv sync` has failed. The URL is never printed.

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

# The job's failed or cancelled steps, one "- name" per line, from the jobs API
# for this run attempt. The current job is the one on this runner.
failed_steps() {
  local repo="${GITHUB_REPOSITORY:-}" run_id="${GITHUB_RUN_ID:-}" token="${GITHUB_TOKEN:-}"
  if [ -z "$repo" ] || [ -z "$run_id" ] || [ -z "$token" ] || ! command -v jq >/dev/null 2>&1; then
    echo "unavailable (needs GITHUB_TOKEN with actions: read, and jq)"
    return
  fi
  local api="${GITHUB_API_URL:-https://api.github.com}"
  local jobs
  if ! jobs=$(curl -fsS --max-time 10 \
      -H "Authorization: Bearer $token" -H "Accept: application/vnd.github+json" \
      "$api/repos/$repo/actions/runs/$run_id/attempts/${GITHUB_RUN_ATTEMPT:-1}/jobs?per_page=100" 2>/dev/null); then
    echo "unavailable (jobs API request failed)"
    return
  fi
  local steps
  if ! steps=$(printf '%s' "$jobs" | jq -r --arg runner "${RUNNER_NAME:-}" '
      [.jobs[] | select(.runner_name == $runner)] as $mine
      | (if ($mine | length) > 0 then $mine else [.jobs[] | select(.status == "in_progress")] end)
      | .[].steps[]?
      | select(.conclusion == "failure" or .conclusion == "cancelled")
      | "- \(.name)\(if .conclusion == "cancelled" then " (cancelled)" else "" end)"' 2>/dev/null); then
    echo "unavailable (jobs API response unreadable)"
    return
  fi
  if [ -n "$steps" ]; then
    printf '%s\n' "$steps"
  else
    echo "none recorded"
  fi
}

# The first test dbt reports as failed or errored, by unique_id.
first_failing_dbt_test() {
  local file="$1"
  if [ ! -f "$file" ]; then
    echo "unavailable (no run_results.json; the build did not finish)"
    return
  fi
  if ! command -v jq >/dev/null 2>&1; then
    echo "unavailable (jq not found)"
    return
  fi
  local name
  if ! name=$(jq -r '
      first(.results[]
        | select(.status == "fail" or .status == "error")
        | select(.unique_id | startswith("test.") or startswith("unit_test."))
        | .unique_id) // empty' "$file" 2>/dev/null); then
    echo "unavailable (run_results.json unreadable)"
    return
  fi
  echo "${name:-none}"
}

body="workflow: ${GITHUB_WORKFLOW:-unknown}
status: ${status:-unknown}"
if [ -n "${GITHUB_REPOSITORY:-}" ] && [ -n "${GITHUB_RUN_ID:-}" ]; then
  body="$body
run: ${GITHUB_SERVER_URL:-https://github.com}/$GITHUB_REPOSITORY/actions/runs/$GITHUB_RUN_ID"
fi
if [ "$status" != "success" ]; then
  body="$body
failed steps:
$(failed_steps)"
  if [ -n "${DBT_RUN_RESULTS:-}" ]; then
    body="$body
first failing dbt test: $(first_failing_dbt_test "$DBT_RUN_RESULTS")"
  fi
fi

if printf '%s\n' "$body" | curl -fsS --max-time 10 --retry 3 -o /dev/null --data-binary @- "$target"; then
  echo "healthchecks.io pinged (job status: ${status:-unknown})"
else
  echo "::warning::healthchecks.io ping failed (job status: ${status:-unknown}); the check will alert on absence after its grace"
fi

exit 0
