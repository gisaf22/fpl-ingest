# Backfill role (#63 D1, D2; #67 E3–E6)

The role `.github/workflows/backfill_catalog.yml` assumes. It is separate from the
ingest role, whose bucket-wide grant could not keep a backfill bug away from raw
captures (#69 narrows that one).

- `trust.json`: only the `backfill` environment's OIDC subject
  (`repo:gisaf22/fpl-ingest:environment:backfill`, audience `sts.amazonaws.com`) can
  assume it. A branch or pull-request token cannot.
- `permissions.json`:
  - `s3:PutObject` only on `raw/fpl/_catalog/*`, and only as a conditional write.
    `Null: s3:if-none-match = false` means a PUT without `If-None-Match` is denied,
    and with `If-None-Match: *` S3 refuses to overwrite an existing key (412).
  - `s3:GetObject` on `raw/fpl/*` and `history/2025-26/*`: what the builder reads.
  - `s3:ListBucket` on the bucket, with no prefix condition. Without bucket-level list
    permission, S3 answers a HEAD or GET on a missing key with 403 rather than 404, and
    the backfill's "catalog already exists?" check would fail on every new key. The cost
    is that the role can list key *names* anywhere in the bucket. It cannot read or
    write them.
  - No delete permission of any kind.

`${ACCOUNT_ID}` in `trust.json` is a placeholder. The maintainer applies both files;
the repo is public, so no real account ID or ARN is committed. The role ARN goes in
the `backfill` environment's `BACKFILL_ROLE_ARN` variable.

## Verifying (#67 AC1)

`simulate-principal-policy` is read-only:

```sh
ROLE_ARN=...   # the applied role
B=arn:aws:s3:::fpl-data-safari
aws iam simulate-principal-policy --policy-source-arn "$ROLE_ARN" \
  --action-names s3:PutObject --resource-arns "$B/raw/fpl/_catalog/backfill/x.json" \
  --context-entries ContextKeyName=s3:if-none-match,ContextKeyValues='*',ContextKeyType=string \
  --query 'EvaluationResults[].EvalDecision'                                 # allowed
aws iam simulate-principal-policy --policy-source-arn "$ROLE_ARN" \
  --action-names s3:PutObject --resource-arns "$B/raw/fpl/_catalog/backfill/x.json" \
  --query 'EvaluationResults[].EvalDecision'                                 # implicitDeny
for key in raw/fpl/element-summary/1/2026-08-29/r/payload.json \
           raw/fpl/_manifests/2026-08-29/r/manifest.json \
           history/2025-26/fpl/fixtures/2026-05-26/r/payload.json \
           served/fct_player_fixture.parquet archive/2025-26/raw/x.json; do
  aws iam simulate-principal-policy --policy-source-arn "$ROLE_ARN" \
    --action-names s3:PutObject --resource-arns "$B/$key" \
    --context-entries ContextKeyName=s3:if-none-match,ContextKeyValues='*',ContextKeyType=string \
    --query 'EvaluationResults[].EvalDecision'                               # implicitDeny each
done
aws iam get-role --role-name <name> --query 'Role.AssumeRolePolicyDocument'  # trust: one subject
```
