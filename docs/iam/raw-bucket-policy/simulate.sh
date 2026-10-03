#!/usr/bin/env bash
# AC3 for #74: read-only IAM simulation of the proposed bucket policy.
# Usage: BACKFILL_ROLE_ARN=<the backfill role ARN> ./simulate.sh
set -euo pipefail
cd "$(dirname "$0")"
: "${BACKFILL_ROLE_ARN:?set BACKFILL_ROLE_ARN (repo variable of the same name)}"
ACCT=$(aws sts get-caller-identity --query Account --output text)
export ACCT BACKFILL_ROLE_ARN
POLICY=$(mktemp); trap 'rm -f "$POLICY"' EXIT  # rendered policy holds the account ID; never write it into the repo
envsubst < bucket-policy.template.json > "$POLICY"
B=arn:aws:s3:::fpl-data-safari
ADMIN=arn:aws:iam::$ACCT:user/safari-admin
DEPLOY=arn:aws:iam::$ACCT:role/github-actions-fpl-ingest
fail=0
check() {  # check <label> <principal> <action> <key> <expected allowed|explicitDeny|implicitDeny>
  got=$(aws iam simulate-principal-policy --policy-source-arn "$2" --action-names "$3" \
    --resource-arns "$B/$4" --resource-policy "file://$POLICY" \
    --context-entries "ContextKeyName=aws:PrincipalArn,ContextKeyValues=$2,ContextKeyType=string" \
    --query 'EvaluationResults[0].EvalDecision' --output text)
  ok=PASS; [ "$got" = "$5" ] || { ok=FAIL; fail=1; }
  printf '%-4s %-48s expected=%-12s got=%s\n' "$ok" "$1" "$5" "$got"
}
P=raw/fpl/bootstrap-static/2026-10-03/sim/payload.json
check "admin put raw/fpl payload"          "$ADMIN"  s3:PutObject    "$P"                                   explicitDeny
check "deploy put raw/fpl payload"         "$DEPLOY" s3:PutObject    "$P"                                   allowed
check "backfill put raw/fpl/_catalog"      "$BACKFILL_ROLE_ARN" s3:PutObject raw/fpl/_catalog/backfill/sim.json allowed
check "admin delete raw/"                  "$ADMIN"  s3:DeleteObject "$P"                                   explicitDeny
check "deploy delete raw/"                 "$DEPLOY" s3:DeleteObject "$P"                                   explicitDeny
check "backfill delete raw/"               "$BACKFILL_ROLE_ARN" s3:DeleteObject raw/fpl/_catalog/backfill/sim.json explicitDeny
check "admin delete history/"              "$ADMIN"  s3:DeleteObject history/sim.json                       explicitDeny
check "deploy delete history/"             "$DEPLOY" s3:DeleteObject history/sim.json                       explicitDeny
check "backfill delete history/"           "$BACKFILL_ROLE_ARN" s3:DeleteObject history/sim.json            explicitDeny
exit $fail
