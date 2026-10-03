# Raw bucket policy (#74 D2, D2a)

A bucket policy for the capture bucket that limits who can write production raw data and
makes raw and history objects undeletable. Draft: not yet simulated or applied.

- `bucket-policy.template.json`:
  - Denies `s3:PutObject` on `raw/fpl/*` to every principal except the ingest deploy
    role and the backfill role. A laptop run with personal credentials gets AccessDenied
    even with `FPL_ALLOW_LOCAL_S3=1`. What each CI role may write within `raw/fpl/` is
    still set by its own permissions (the backfill role: `raw/fpl/_catalog/*` only).
  - Denies `s3:DeleteObject` and `s3:DeleteObjectVersion` on `raw/*` and `history/*` for
    every principal, CI roles included. Removing an object first means editing the policy.
  - `${ACCT}` and `${BACKFILL_ROLE_ARN}` are placeholders, so no account ID is committed.
- `simulate.sh`: #74 AC3. Fills in the template in a temp file and runs nine read-only
  `simulate-principal-policy` cases (personal put denied, deploy and backfill puts allowed,
  deletes under `raw/` and `history/` denied for all three). Exits non-zero on any mismatch.

  ```sh
  BACKFILL_ROLE_ARN=<repo variable of the same name> docs/iam/raw-bucket-policy/simulate.sh
  ```

Before applying: save the current policy with `get-bucket-policy` (that copy is the
rollback), and merge these statements into it, since `put-bucket-policy` replaces the whole
policy. Rollback: `put-bucket-policy` with the saved copy, or `delete-bucket-policy` if
there was none.
