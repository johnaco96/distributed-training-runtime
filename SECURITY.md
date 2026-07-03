# Security Policy

## Reporting a vulnerability

Please report suspected security vulnerabilities privately — do **not** open a
public issue. Use GitHub's [private vulnerability
reporting](https://docs.github.com/en/code-security/security-advisories/guidance-on-reporting-and-writing-information-about-vulnerabilities/privately-reporting-a-security-vulnerability)
("Report a vulnerability" under the repository **Security** tab), or email the
maintainers.

Please include a description, affected component/version, reproduction steps or
a proof of concept, and any suggested remediation. We aim to acknowledge reports
within 3 business days and to provide a remediation timeline after triage.

## Supported surfaces

- **Coordinator control plane** (`runtime-coordinator`): authentication,
  authorization, audit, TLS/mTLS, metadata store.
- **Python SDK** (`dtruntime`): client credential handling and the
  `dtruntime-admin` CLI.
- **Kubernetes operator** (`runtime-operator`) and deployment manifests.

## Security model (summary)

See [docs/security-architecture.md](docs/security-architecture.md),
[docs/security-auth.md](docs/security-auth.md), and
[docs/threat-model.md](docs/threat-model.md) for the full model. In brief:

- **Transport**: optional TLS/mTLS (`--tls-cert`/`--tls-key`/`--tls-client-ca`).
- **Authentication** (layered): OIDC/JWT bearer tokens, SPIFFE X.509-SVID from
  the mTLS peer certificate, and a dev-only metadata-header identity that is
  disabled in hardened deployments (`--auth-disable-header-identity`).
- **Authorization**: role/permission RBAC (admin / operator / viewer / worker).
- **Secret rotation**: JWT verification keys hot-reload from a JWKS/secret file.
- **Audit**: structured events to the metadata store and an optional
  NDJSON/SIEM sink (`--audit-log-file`).

## Dependency & vulnerability management

- **Rust**: [`cargo-deny`](https://embarkstudios.github.io/cargo-deny/) gates
  RUSTSEC advisories, yanked crates, wildcard versions, and untrusted sources
  (`deny.toml`). Enforced in CI on every PR.
- **Python**: [`pip-audit`](https://pypi.org/project/pip-audit/) scans the SDK's
  resolved dependencies against the PyPI advisory database. Enforced in CI.
- **Automated updates**: Dependabot opens weekly update PRs for Cargo, pip, and
  GitHub Actions (`.github/dependabot.yml`).
- Security scans run on every pull request and on a weekly schedule
  (`.github/workflows/security-scan.yml`).

## Handling secrets

The coordinator never logs secrets, tokens, private keys, or object-store
credentials (see [docs/audit-logging.md](docs/audit-logging.md)). Provide
secrets via files (projected from a secret manager or a Kubernetes Secret) so
they can be rotated without a restart — see
[docs/secret-rotation.md](docs/secret-rotation.md).
