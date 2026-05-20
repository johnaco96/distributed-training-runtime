# Production S3 Plan (Phase 20F-5 Secure Design Track)

## Current status

- Local filesystem and S3-compatible storage abstraction paths exist.
- MinIO/S3-compatible flows exist for local/dev and test-oriented paths.
- Checkpoint commit path includes checksum-oriented validation.
- Production S3 hardening is not complete.

## Target production S3 model

Production direction requires all of the following:

- Credentials from external secret manager or Kubernetes Secret reference only; never committed in repo.
- Least-privilege IAM policy scoped to required prefixes/actions.
- Bucket/prefix isolation by environment (dev/staging/prod).
- Server-side encryption configured.
- Checksum validation enforced on write/read validation flows.
- Retry/backoff policy tuned for transient object-store failures.
- Multipart upload threshold/concurrency tuning for large checkpoint artifacts.
- Lifecycle cleanup policy with explicit retention safety.
- Object lock/versioning discussion where compliance/recovery needs justify it.
- Regional latency and cross-zone traffic implications documented.
- Mandatory staging validation before production adoption.

## Credential model

- Local MinIO env vars are dev-only convenience.
- Production credentials should come from workload identity and/or external secret manager integration.
- No static long-lived credentials in Kubernetes manifests.
- Rotation plan must define cadence, trigger, rollback, and validation steps.

## Failure modes to design/test

- Partial uploads.
- Checksum mismatch.
- Stale/latest pointer inconsistency.
- Object store unavailable.
- Access denied from IAM misconfiguration.
- High latency/throttling.
- Lifecycle policy deleting still-needed checkpoint artifacts.
- Bucket/prefix collision across environments.

## Acceptance criteria

Production S3 path is not ready until:

- No credentials are committed in repository.
- Checksum mismatch paths are rejected and tested.
- Staging bucket integration tests pass.
- Failure-injection scenarios are exercised and documented.
- Restore tests succeed after cleanup/lifecycle events.
- Retention/lifecycle policies are documented with ownership.
- Cost/latency considerations are explicitly documented.

## Risk register

- Credential leakage.
- Accidental deletion of required artifacts.
- Corrupted checkpoint restore.
- Misconfigured lifecycle policy.
- Cross-environment prefix collision.
- Cost blow-up from benchmark/checkpoint artifact growth.

## Explicit non-claims

This phase does **not** implement:
- Production cloud-provider S3 deployment.
- Automatic IAM provisioning.
- Full production S3 tuning in runtime code.


## Local/dev MinIO validation environment variables

- `DTRUNTIME_RUN_MINIO_TESTS=1` to enable env-gated MinIO tests.
- `DTRUNTIME_S3_ENDPOINT_URL` S3-compatible endpoint (for example local MinIO).
- `DTRUNTIME_S3_REGION` logical region string.
- `DTRUNTIME_S3_BUCKET` target bucket for test data.
- `DTRUNTIME_S3_ACCESS_KEY_ID` access key id for local/dev test user.
- `DTRUNTIME_S3_SECRET_ACCESS_KEY` secret key for local/dev test user.
- `DTRUNTIME_S3_FORCE_PATH_STYLE` optional (`true`/`1`) for MinIO style addressing.
- `DTRUNTIME_S3_KEY_PREFIX` optional test key prefix root.

These are local/dev validation inputs only and must not be committed.
\n- Phase 20 update: benchmark expansion and MinIO/S3 validation are environment-gated; no production-readiness claim.
