# Acceptance Tests CI/CD Plan

## Overview

Run S3F2 acceptance tests (IAM + Cognito) in GitHub Actions by deploying a fresh
stack per run and tearing it down afterwards.

## Pre-requisites (one-time manual setup)

### 1. AWS OIDC Identity Provider + IAM Role

Deploy `ci/oidc-role.yaml` in the target AWS account:

```bash
aws cloudformation deploy \
  --template-file ci/oidc-role.yaml \
  --stack-name S3F2-GHA-OIDC \
  --capabilities CAPABILITY_NAMED_IAM \
  --region eu-west-1
```

This creates the GitHub OIDC provider and the `S3F2-GHA-AcceptanceTests` IAM
role. If an OIDC provider already exists, pass its ARN via `OIDCProviderArn`
parameter.

### 2. CI Artefacts Bucket

Deploy `ci/artefacts-bucket.yaml` in the target AWS account:

```bash
aws cloudformation deploy \
  --template-file ci/artefacts-bucket.yaml \
  --stack-name S3F2-CI-ArtefactsBucket \
  --region eu-west-1
```

This creates the `s3f2-ci-artefacts` bucket with a 7-day lifecycle policy and
encryption.

### 3. GitHub Environment

Create an `acceptance-tests` environment in the repo settings with:

- Secret: `AWS_ROLE_ARN` (the IAM role ARN from step 1)
- Secret: `ADMIN_EMAIL` (email for Cognito user pool, can be a shared team
  address)

## Workflow Design

**File**: `.github/workflows/acceptance-tests.yaml`

**Triggers**:

- `push` to `master`
- `workflow_dispatch` (manual, with optional test suite selector:
  all/iam/cognito)

**Concurrency**: Single concurrent run (stack name `S3F2` is hardcoded in the
Makefile).

**Timeout**: 60 minutes

**Region**: `eu-west-1`

## Workflow Steps

### Setup

1. Checkout repo
2. Cache pip + npm dependencies
3. Install system deps (`libsnappy-dev`)
4. Setup Python 3.12, Node 20, Ruby 3.3
5. `pip install virtualenv && make setup`

### Authenticate

6. Configure AWS credentials via OIDC
   (`aws-actions/configure-aws-credentials@v4`)

### Deploy

7. Run `make deploy` with `TEMP_BUCKET=s3f2-ci-artefacts` (a persistent,
   pre-created bucket), `ADMIN_EMAIL`, `REGION=eu-west-1`
8. Deploy `S3F2DataAccessRole` stack from `templates/role.yaml` with
   `SourceAccountId` set to the current account ID

### Test

9. `make test-acceptance-iam`
10. `make test-acceptance-cognito`

### Teardown (runs always, even on failure)

11. Empty S3 buckets created by the stack (WebUI bucket, temp bucket, any test
    data buckets)
12. Delete ECR images from the stack's ECR repository
13. Delete `S3F2` CloudFormation stack and wait for completion
14. Delete `S3F2DataAccessRole` stack

## Key Environment Variables

| Variable             | Value               | Source                               |
| -------------------- | ------------------- | ------------------------------------ |
| `AWS_DEFAULT_REGION` | `eu-west-1`         | Workflow env                         |
| `StackName`          | `S3F2`              | Workflow env (default used by tests) |
| `TEMP_BUCKET`        | `s3f2-ci-artefacts` | Pre-created persistent bucket        |
| `ADMIN_EMAIL`        | Team email          | GitHub secret                        |

## Files to Create/Modify

| File                                      | Action     | Purpose                                                                    |
| ----------------------------------------- | ---------- | -------------------------------------------------------------------------- |
| `ci/oidc-role.yaml`                       | ✅ Created | CFN template for GitHub OIDC provider + IAM role                           |
| `ci/artefacts-bucket.yaml`                | ✅ Created | CFN template for persistent CI artefacts bucket with lifecycle policy      |
| `.github/workflows/acceptance-tests.yaml` | ✅ Created | The workflow (deploy, test with suite selector, teardown)                  |
| `ci/teardown.sh`                          | ✅ Created | Reliable teardown script (empty buckets, delete ECR images, delete stacks) |
| `Makefile`                                | No change  | Stack name stays hardcoded; concurrency group prevents conflicts           |

## Risks and Mitigations

| Risk                                             | Mitigation                                                                                           |
| ------------------------------------------------ | ---------------------------------------------------------------------------------------------------- |
| Teardown fails, leaving orphaned resources       | `ci/teardown.sh` handles partial state; tag resources with `github.run_id` for manual identification |
| Cancelled run leaves stack half-deployed         | Concurrency group set to `cancel-in-progress: false` so runs complete; manual cleanup documented     |
| CloudFormation can't delete non-empty S3 buckets | Teardown script empties all stack-created buckets before stack deletion                              |
| Parallel runs conflict on stack name `S3F2`      | Concurrency group limits to one run at a time                                                        |
| Cost of full stack deploy per run                | Only triggered on `master` merge and manual dispatch, not on PRs                                     |

## Security Risks

### Threat: Malicious PR triggers workflow with modified code

A forked PR could modify test code, the Makefile, or the teardown script to
exfiltrate AWS credentials or deploy malicious resources.

**Mitigations:**

- Only trigger on `push` to `master` (post-merge), not on `pull_request`. This
  means code has already been reviewed.
- `workflow_dispatch` is only available to users with write access to the repo.
- The OIDC trust policy should restrict to `ref:refs/heads/master` so
  credentials are only issued for master branch runs.
- Use a GitHub `environment` with required reviewers if you ever add PR
  triggers.

### Threat: Over-privileged IAM role

The role needs broad permissions to deploy the full stack. If compromised, it
could be used to access other resources in the account.

**Mitigations:**

- Use a dedicated AWS account for CI acceptance tests (no production resources).
- Scope the OIDC trust policy to this specific repo only.
- Set a short session duration (1 hour) on the role.
- Add a permissions boundary to prevent privilege escalation (e.g. prevent
  creating new IAM users/roles outside the stack).

### Threat: Secrets exposed in logs

AWS credentials, Cognito tokens, or API URLs could leak into workflow logs.

**Mitigations:**

- OIDC credentials are short-lived and auto-expire.
- GitHub automatically masks secrets in logs.
- Avoid `echo`-ing sensitive values in scripts.

### Threat: Orphaned resources accumulate cost

Failed teardowns leave stacks, buckets, and Fargate tasks running.

**Mitigations:**

- Tag all resources with `ci:true` for easy identification.
- Set up a scheduled Lambda or EventBridge rule to clean up stacks older than N
  hours with the CI tag.
- The persistent artefacts bucket (`s3f2-ci-artefacts`) should have a lifecycle
  policy to expire old objects.

## Implementation Plan

Incremental approach — get each phase working before moving to the next.

All development happens on a feature branch, merged to `master` via PR. The
workflow uses `workflow_dispatch` from the start so it can be triggered manually
on any branch during development. Each phase is a PR.

### Phase 1: Deploy pre-requisites and verify auth

1. Deploy `ci/oidc-role.yaml` to the target AWS account (OIDC trust policy
   allows all branches during development)
2. Deploy `ci/artefacts-bucket.yaml` to the target AWS account
3. Create the `acceptance-tests` GitHub environment with `AWS_ROLE_ARN` and
   `ADMIN_EMAIL` secrets
4. Open PR with `ci/oidc-role.yaml`, `ci/artefacts-bucket.yaml`, and
   `.github/workflows/acceptance-tests.yaml`, merge to master
5. Trigger via `workflow_dispatch` to verify auth works (smoke test:
   `aws sts get-caller-identity`)

### Phase 2: Verify deploy and teardown

6. Trigger via `workflow_dispatch`, verify the stack deploys and tears down
   cleanly
7. Manually verify no orphaned resources remain after a successful run
8. Test failure path: intentionally fail a step before teardown, confirm
   teardown still runs

### Phase 3: Verify acceptance tests pass

9. Trigger via `workflow_dispatch`, verify both IAM and Cognito tests pass
   against the freshly deployed stack
10. Test the suite selector input (run just `iam` or `cognito` individually)

### Phase 4: Harden

11. Add `push` to `master` as a trigger (so it runs automatically post-merge)
12. Tighten the OIDC trust policy to only allow `master` branch (update
    `ci/oidc-role.yaml` and redeploy)
13. Add a permissions boundary to the IAM role
14. Add a scheduled cleanup mechanism for orphaned resources
15. Document manual cleanup steps for stuck/failed runs
