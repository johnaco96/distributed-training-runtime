# Security Authentication & Authorization Plan (Phase 20F-1)

## Current auth status

- Coordinator currently does not enforce a production-grade authn/authz model.
- Local/dev flows prioritize reproducibility and functional runtime semantics.

## Why a static token alone is insufficient

Static shared tokens are not a strong long-term model because they:
- Lack strong workload identity binding.
- Increase blast radius when leaked.
- Are hard to rotate safely at scale.
- Do not provide robust mutual peer authentication by themselves.

## Target model

- mTLS for transport security and mutual service identity proof.
- Workload identity for machine actors (SPIFFE/SPIRE-compatible direction where practical).
- Short-lived credentials instead of long-lived static credentials where possible.
- OIDC-backed human/admin authentication for future dashboard/admin surfaces.
- Explicit authorization policy for coordinator RPCs.

## Local/dev token fallback stance

A local/dev token mechanism may be used as a temporary fallback for experiments, but it is not the target production security state.

## Planned 20F-2 scope

- Add optional TLS/mTLS foundation for coordinator gRPC and Python client.
- Preserve clear local/dev compatibility modes while labeling non-production behavior.

## Planned 20F-3 scope

- Add workload identity mapping.
- Add explicit authorization checks for coordinator RPC surfaces.

## Security hygiene reminder

Never commit real tokens, secrets, private keys, kubeconfigs, or cloud credentials.
\n\n## Phase 20F-2 TLS/mTLS foundation\nPhase 20F-2 adds optional transport security for coordinator gRPC and Python RuntimeClient. Plaintext remains the default local/dev behavior when TLS options are omitted.\n\nCoordinator TLS options:\n- --tls-cert /path/to/server.crt\n- --tls-key /path/to/server.key\n- --tls-client-ca /path/to/ca.crt (optional; enables client cert verification/mTLS)\n\nExample server commands:\n- runtime-coordinator --addr 0.0.0.0:50051 --tls-cert /path/to/server.crt --tls-key /path/to/server.key\n- runtime-coordinator --addr 0.0.0.0:50051 --tls-cert /path/to/server.crt --tls-key /path/to/server.key --tls-client-ca /path/to/ca.crt\n\nPython RuntimeClient supports TLS args and env vars: tls_ca_cert, tls_client_cert, tls_client_key and DTRUNTIME_TLS_CA_CERT, DTRUNTIME_TLS_CLIENT_CERT, DTRUNTIME_TLS_CLIENT_KEY.\n\nNo certificates/keys are committed in-repo; use placeholders such as k8s/local/tls-secret.example.yaml.\nNo workload authorization policy is implemented in 20F-2; that is planned for Phase 20F-3.\nNo production PKI issuance/rotation is implemented in 20F-2.\n


## Phase 20F-3 authorization scaffolding
Phase 20F-3 adds an internal authorization model with optional metadata-based caller identity for local/dev testing. Default mode remains disabled for compatibility. Enable with `runtime-coordinator --addr 0.0.0.0:50051 --auth-mode metadata` or `DTRUNTIME_AUTH_MODE=metadata`. Python client can attach `x-dtruntime-subject`, `x-dtruntime-role`, and optional `x-dtruntime-worker-id` via constructor args or env (`DTRUNTIME_AUTH_SUBJECT`, `DTRUNTIME_AUTH_ROLE`, `DTRUNTIME_AUTH_WORKER_ID`). This is policy scaffolding only (not full SPIFFE/SPIRE or production RBAC). Audit/security events remain planned for Phase 20F-4; dashboard/HA/production S3 hardening remain Phase 20F-5.
