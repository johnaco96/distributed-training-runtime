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
