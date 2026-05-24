# Production Gates Checklist (Phase 20F-5)

These gates define minimum evidence required before claiming production readiness.

## Security gates

- [ ] mTLS enabled in secure environment.
- [ ] Client identities validated.
- [ ] Authorization enforced on protected RPCs.
- [ ] Audit/security events emitted for key decisions and state changes.
- [ ] No secrets in logs.
- [ ] No raw auth metadata dumps.
- [ ] No privileged pods in deployment manifests.
- [ ] No public service exposure by default.

## Dashboard gates

- [ ] Auth required.
- [ ] No secret/cert/token/object-credential display.
- [ ] Read-only by default.
- [ ] Dashboard access is audited.

## HA gates

- [ ] Leader election implemented.
- [ ] Fencing implemented.
- [ ] Failover tests pass.
- [ ] Checkpoint double-commit prevented.

## S3 gates

- [ ] Least-privilege IAM policy validated.
- [ ] Server-side encryption enabled.
- [ ] Checksum validation enforced.
- [ ] Restore test passes.
- [ ] Lifecycle retention policy tested.

## Benchmark gates

- [ ] Benchmark environment labeled.
- [ ] Commands are repeatable/documented.
- [ ] No inflated claims from local-only data.
- [ ] Local vs cluster benchmark results are clearly separated.

## Notes

- This checklist is a gate system, not evidence that gates are complete today.
- As of Phase 20F-5, several gates remain future implementation work.
