# Secret management and rotation

The coordinator is designed so that its rotation-sensitive secrets live in
**files** — not baked into flags, images, or the binary — so they can be
projected from a secret manager (Vault, AWS/GCP Secret Manager, a Kubernetes
Secret) and rotated **without a restart**.

## JWT verification keys (hot rotation)

The OIDC/JWT signing keys are the most rotation-critical secret. They are loaded
from one of:

| Flag | Contents | Rotate by |
|---|---|---|
| `--oidc-jwks-file` | A JWKS document (RFC 7517), one or more keys with `kid`s | Writing a new JWKS file |
| `--oidc-hmac-secret-file` | A raw HMAC (HS256) shared secret | Overwriting the secret file |
| `--oidc-public-key-file` | A single PEM public key (RS*/ES*/EdDSA) | Overwriting the PEM file |

The coordinator watches the backing file's modification time and reloads it
transparently (checked at most once every few seconds, off the request path).
A reload that fails — e.g. the file is momentarily truncated mid-write — keeps
the last-good keys and logs a warning, so rotation never locks callers out.

### Zero-downtime rotation with a JWKS overlap window

Publish the **new** key alongside the old one, each with a distinct `kid`:

```json
{ "keys": [
  { "kty": "RSA", "kid": "2026-06", "n": "…old…", "e": "AQAB" },
  { "kty": "RSA", "kid": "2026-07", "n": "…new…", "e": "AQAB" }
] }
```

1. Add the new key to the JWKS (both keys present). The coordinator now accepts
   tokens signed by **either** key.
2. Cut your identity provider over to signing with the new `kid`.
3. Once all outstanding tokens signed by the old key have expired, drop the old
   key from the JWKS.

Because the coordinator selects candidate keys by the token's `kid` header, and
falls back to trying all keys when a token omits `kid`, the overlap window is
seamless. This was validated live: rotating the secret file and re-signing with
the new material begins validating immediately, while tokens signed by the
retired key are rejected.

> For an OIDC provider that publishes its own JWKS endpoint, project that JWKS
> to the `--oidc-jwks-file` path (e.g. a sidecar or CronJob that curls the
> provider's `/.well-known/jwks.json` and writes it atomically). The coordinator
> picks up provider key rotations automatically.

## TLS / mTLS certificates

TLS material (`--tls-cert`, `--tls-key`, `--tls-client-ca`) is read at startup.
Rotate certificates with a rolling restart driven by your PKI:

- **cert-manager** (Kubernetes): a `Certificate` renews the Secret; a
  `Reloader`-style annotation or a rollout restart cycles the pods. Because the
  coordinator supports HA standby (PostgreSQL advisory lock), a rolling restart
  does not drop the control plane.
- **SPIRE**: SVIDs are short-lived by design; the SPIFFE identity is read from
  the peer certificate on every request, so re-issued SVIDs are honored as soon
  as the client presents them.

## Object-store credentials

S3 credentials (`--s3-access-key`/`--s3-secret-key`, or the `DTRUNTIME_S3_*`
env) should come from the platform's secret manager. Prefer **short-lived,
scoped** credentials:

- On AWS, run the coordinator with an IRSA/instance role scoped to the
  checkpoint bucket/prefix instead of long-lived keys.
- Grant least privilege: `s3:GetObject`/`PutObject`/`ListBucket` (and
  `DeleteObject` only if retention GC targets S3) on the coordinator's prefix
  only.

## What is never persisted

Secrets, tokens, private keys, and object-store credentials are never written to
logs or audit records (see [audit-logging.md](audit-logging.md)). Only logical
identities (`subject`, `role`, `worker_id`) and resource ids appear in audit
output.
