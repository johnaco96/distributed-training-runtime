# Release process

Releases are cut from a SemVer git tag. Pushing a `vX.Y.Z` tag runs
`.github/workflows/release.yml`, which builds and publishes the Python SDK, the
container images (signed, with provenance), and the Helm chart, then creates a
GitHub Release.

## Versioning

The project uses [Semantic Versioning](https://semver.org/). Three versions are
kept in lockstep for a release:

| Artifact | Source of truth |
|---|---|
| Python SDK (`dtruntime`) | `pyproject.toml` `[project].version` |
| Rust crates | each crate's `Cargo.toml` `version` |
| Helm chart app version | `deploy/helm/dtruntime/Chart.yaml` `appVersion` |

The Helm **chart** version (`Chart.yaml` `version`) is bumped independently
whenever the chart changes, even without an app release.

The gRPC wire protocol has its own compatibility contract enforced separately
(see [protocol-compatibility.md](protocol-compatibility.md)); additive proto
changes are backward compatible and do not force a major bump.

## Cutting a release

1. **Bump versions** in `pyproject.toml`, the crate `Cargo.toml`s, and
   `Chart.yaml` (`version` and `appVersion`).
2. **Update `CHANGELOG.md`** — move the `Unreleased` items under a new
   `## [X.Y.Z] - YYYY-MM-DD` heading.
3. **Green CI**: `ci.yml`, `security-scan.yml`, and `compat.yml` must pass on
   the release commit.
4. **Tag and push**:
   ```bash
   git tag -a vX.Y.Z -m "vX.Y.Z"
   git push origin vX.Y.Z
   ```
5. The release workflow produces:
   - `dtruntime-X.Y.Z.tar.gz` (sdist) + `dtruntime-X.Y.Z-py3-none-any.whl`
     (wheel), `twine check`-validated.
   - `ghcr.io/<owner>/distributed-training-runtime:X.Y.Z` and
     `…-operator:X.Y.Z` — multi-arch (amd64/arm64), with **SLSA provenance**,
     an **SBOM**, and a **Sigstore (cosign) keyless signature**.
   - `dtruntime-<chart>.tgz` Helm package.
   - A GitHub Release with generated notes and the artifacts attached.

## Verifying a signed image

```bash
cosign verify \
  --certificate-identity-regexp 'https://github.com/johnaco96/distributed-training-runtime/.*' \
  --certificate-oidc-issuer https://token.actions.githubusercontent.com \
  ghcr.io/johnaco96/distributed-training-runtime:X.Y.Z
```

Inspect provenance/SBOM attestations:

```bash
cosign download attestation ghcr.io/johnaco96/distributed-training-runtime:X.Y.Z
```

## Publishing boundary

The workflow **pushes container images to GHCR on tag** (the registry the repo
already has write access to). Publishing the Python package to **PyPI** is
intentionally left behind a manual `pypi` environment / trusted-publisher step
(commented in `release.yml`) so a tag does not auto-publish to PyPI; enable it
when you are ready to publish externally. Pushing the Helm chart to an OCI
registry is likewise opt-in.

Until then, artifacts can be built locally for verification:

```bash
python -m build                         # sdist + wheel into dist/
helm package deploy/helm/dtruntime      # chart .tgz
docker build -f Dockerfile -t dtruntime:local .
```
