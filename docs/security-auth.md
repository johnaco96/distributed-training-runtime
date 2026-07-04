# Security Authentication & Authorization

## Overview

Authentication is **layered**: a request is authenticated by the strongest
credential it presents, in this order —

1. **OIDC / JWT bearer token** (`Authorization: Bearer <jwt>`) — validated
   against a rotatable key set.
2. **SPIFFE X.509-SVID** — the workload identity in the mTLS peer certificate's
   URI SAN (`spiffe://…`), mapped to roles.
3. **Dev metadata headers** (`x-dtruntime-subject/role/worker-id`) — a local
   development convenience, disabled in hardened deployments.

Authorization is **role/permission based** (RBAC): the verified identity carries
one or more roles, and each RPC requires a permission that at least one role
must grant.

Enable enforcement with `--auth-mode metadata` (or `DTRUNTIME_AUTH_MODE=metadata`);
`disabled` (the default) serves anonymous requests for local development.

## Roles and permissions

| Role | Permissions | Typical use |
|---|---|---|
| `admin` | everything | Break-glass, worker-plane, configuration |
| `operator` | read + manage runs (lifecycle, register run/dataset, commit checkpoint) | On-call operators |
| `viewer` | read-only (list/describe runs, workers, shards, checkpoints; runtime info) | Dashboards, auditors |
| `worker` | worker-self operations (register, heartbeat, leases, shard progress, checkpoint reports, rendezvous), scoped to its own `worker_id` | Training processes |

Read RPCs require `read` (admin/operator/viewer). Management RPCs require
`manage` (admin/operator). Worker-plane RPCs require the `worker` role and a
matching `worker_id`. An unauthorized call returns `PERMISSION_DENIED`; a missing
or invalid credential returns `UNAUTHENTICATED`.

## OIDC / JWT bearer tokens

The coordinator is an OAuth2 **resource server**: it validates tokens, it does
not mint them. Configure it against any OIDC provider (Okta, Auth0, Keycloak,
Entra ID, Google) or cloud IAM that federates through OIDC:

| Flag | Env | Meaning |
|---|---|---|
| `--oidc-issuer` | `DTRUNTIME_OIDC_ISSUER` | Required `iss` |
| `--oidc-audience` (repeatable) | `DTRUNTIME_OIDC_AUDIENCE` | Accepted `aud` |
| `--oidc-jwks-file` | `DTRUNTIME_OIDC_JWKS_FILE` | JWKS for RS/ES/EdDSA/oct keys |
| `--oidc-hmac-secret-file` | `DTRUNTIME_OIDC_HMAC_SECRET_FILE` | HS256 shared secret |
| `--oidc-public-key-file` | `DTRUNTIME_OIDC_PUBLIC_KEY_FILE` | Single PEM public key |
| `--oidc-algorithm` | `DTRUNTIME_OIDC_ALGORITHM` | Algorithm for HMAC/PEM key |
| `--oidc-role-claim` | `DTRUNTIME_OIDC_ROLE_CLAIM` | Claim carrying roles (default `roles`) |

Validation enforces the signature, `exp`/`nbf` (with a small clock-skew
leeway), and — when configured — `iss` and `aud`. Roles come from the role
claim (a string or an array; unknown groups are ignored). The signing algorithm
is pinned to the configured key's algorithm, blocking algorithm-confusion
attacks. Keys hot-reload on file change — see
[secret-rotation.md](secret-rotation.md).

Python clients attach a token via `RuntimeClient(auth_token=…)` or
`DTRUNTIME_AUTH_TOKEN`; `dtruntime-admin --token …` does the same.

## SPIFFE / SPIRE workload identity

With mTLS enabled, a SPIRE-issued X.509-SVID carries the workload's SPIFFE ID as
a URI SAN. Enable SPIFFE authentication with:

| Flag | Env | Meaning |
|---|---|---|
| `--spiffe-trust-domain` | `DTRUNTIME_SPIFFE_TRUST_DOMAIN` | Required trust domain |
| `--spiffe-role-map-file` | `DTRUNTIME_SPIFFE_ROLE_MAP_FILE` | Path→role rules |

The role-map file maps SPIFFE paths to roles (first match wins; `*` is a prefix
match):

```
# spiffe-path-pattern = role[,role]
/ns/dtruntime/sa/coordinator-admin = admin
/ns/dtruntime/sa/operator          = operator
/workload/worker/*                 = worker
```

A `worker`-mapped identity binds its `worker_id` to the trailing path segment,
so worker-self checks work against the SVID. The TLS trust chain is still
enforced by `--tls-client-ca`; SPIFFE only reads identity from an
already-verified certificate.

## Cloud IAM

Cloud IAM integrates through the OIDC path: EKS/GKE workload identity, GitHub
OIDC, and most managed identity providers issue OIDC ID/access tokens whose JWKS
the coordinator validates via `--oidc-jwks-file`. For AWS SigV4-style IAM
(no OIDC token), front the coordinator with a gateway that exchanges IAM identity
for a short-lived OIDC token, or run behind an IAM-authenticated proxy — the
role/permission model downstream is unchanged.

## Hardening checklist

- Set `--auth-mode metadata`.
- Configure OIDC and/or SPIFFE.
- Add `--auth-disable-header-identity` so caller-asserted headers are never
  trusted (only cryptographically-proven credentials authenticate).
- Enable TLS/mTLS (`--tls-cert`/`--tls-key`/`--tls-client-ca`).
- Send audit events to your SIEM (`--audit-log-file`, see
  [audit-logging.md](audit-logging.md)).

## Why a static token alone is insufficient

Static shared tokens lack workload-identity binding, have a large blast radius
when leaked, are hard to rotate safely, and do not provide mutual peer
authentication. The layered model above replaces them with short-lived,
verifiable credentials.

## Secure local Kubernetes path

`k8s/local-secure/` validates TLS/mTLS with real Kubernetes Secrets (`dtruntime-tls`, `dtruntime-client-ca`, `dtruntime-client-tls`) created from generated local certs in `.dtruntime-certs/`. The coordinator consumes `--tls-cert`, `--tls-key`, and `--tls-client-ca` from mounted secret files, and the PyTorch job uses `DTRUNTIME_TLS_CA_CERT`, `DTRUNTIME_TLS_CLIENT_CERT`, and `DTRUNTIME_TLS_CLIENT_KEY`.

TLS hostname verification is kept enabled end-to-end. Development server certificates generated by `scripts/generate_dev_certs.ps1` include SAN coverage for `runtime-coordinator.dtruntime.svc.cluster.local` and related Kubernetes service DNS names. Do not commit generated `.dtruntime-certs/` material.

## Security hygiene reminder

Never commit real tokens, secrets, private keys, kubeconfigs, or cloud credentials.
