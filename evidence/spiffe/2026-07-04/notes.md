# SPIFFE / SPIRE authentication — evidence (2026-07-04)

**Verdict: PASS**, and it caught a **critical latent bug** (see below).

Upgrades the SPIFFE row from "validated locally with generated certs" to
"authenticated with a **real SPIRE-issued X.509-SVID**."

## What ran

1. **Real SPIRE server** (`ghcr.io/spiffe/spire-server:1.9.6`, trust domain
   `dtruntime.example.org`) via docker; minted three SVIDs with
   `spire-server x509 mint`:
   - server: `spiffe://dtruntime.example.org/coordinator` (DNS SAN `localhost`)
   - admin: `spiffe://dtruntime.example.org/ns/dtruntime/sa/admin`
   - nobody: `spiffe://dtruntime.example.org/ns/other/sa/nobody` (unmapped)
2. Coordinator run with `--tls-cert/key` (server SVID), `--tls-client-ca` (SPIRE
   bundle), `--auth-mode metadata --auth-disable-header-identity
   --spiffe-trust-domain dtruntime.example.org --spiffe-role-map-file roles.map`.
3. Python `RuntimeClient` made mTLS calls presenting each SVID as its client cert.

## Result

- **admin SVID → AUTHORIZED** as `admin` (audit `source=spiffe`).
- **nobody SVID → REJECTED `UNAUTHENTICATED`** (`NoRolesMapped`) — the SPIFFE
  path→role policy correctly denies an unmapped identity.

See `result.json` and `audit-spiffe-accept.ndjson`.

## Bug found and fixed

The first run **panicked at startup**: rustls 0.23 could not auto-select a
`CryptoProvider` because the dependency tree enables **both** `ring` and
`aws-lc-rs` (the former arrived with `jsonwebtoken` in the security phase). This
broke **all** TLS use (`--tls-cert`), not just SPIFFE. Fixed by installing the
`ring` provider at `main()` startup (`crates/runtime-coordinator/src/main.rs`).
This harness doubles as the regression test.

## Reproduce

See `tools/evidence/spiffe/` (server.conf, roles.map) and the mint commands
above. `docker rm -f spire-server` to clean up.
