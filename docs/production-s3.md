# Production S3 Track (Phase 20F-1)

## Current S3/MinIO support status

- Storage abstraction supports local filesystem and S3-compatible backends.
- Local/dev MinIO-style paths exist for testing.
- Production S3 hardening is not complete.

## Production S3 checklist

- Credentials delivered from secret manager and/or controlled env injection.
- Least-privilege IAM policies.
- Bucket and prefix isolation by environment/run scope.
- Checksum validation enforced.
- Server-side encryption configured.
- Lifecycle policy defined.
- Retry/backoff behavior tuned.
- Multipart thresholds tuned for artifact size distributions.
- Regional latency considerations documented.
- Staging validation runbook.
- Failure-injection scenarios tested.
- Credential rotation procedure tested.

## Current non-claims

This repository does not currently claim full production S3 posture, IAM policy completeness, or finalized tuning defaults.
