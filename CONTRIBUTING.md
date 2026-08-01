# Contributing

Thanks for helping improve the Distributed Training Runtime. Bug reports,
focused fixes, documentation corrections, and reproducible performance results
are all welcome.

## Development setup

Follow [docs/quickstart.md](docs/quickstart.md) for prerequisites and local
installation. Before opening a pull request, run the same core checks as CI:

```bash
cargo fmt --all -- --check
cargo clippy --workspace --exclude dtruntime-native --all-targets --all-features -- -D warnings
cargo test --workspace --exclude dtruntime-native --all-features
python -m ruff check .
python -m pytest python/tests -q
python -m build
```

Tests that require MinIO, PostgreSQL, PyTorch, multiple processes, or extended
runtime are gated by environment variables; their test modules and the GitHub
workflows document the relevant flags.

## Project conventions

- Keep Rust unit tests in `#[cfg(test)]` modules and integration tests under a
  crate's `tests/` directory.
- Keep Python tests under `python/tests/`; examples belong in `python/examples/`
  or `demo/`, and benchmark drivers belong in `benchmarks/`.
- Preserve the control-plane/data-plane boundary: the coordinator owns
  decisions and metadata, while workers and storage backends move bulk data.
- Update documentation and tests with behavior changes. Do not commit generated
  build output, credentials, local evidence, or benchmark run artifacts.
- Treat `proto/runtime.proto` as a compatibility contract. After an intentional
  compatible protocol change, regenerate Python bindings with
  `python scripts/generate_python_proto.py` and update the snapshot only when
  the compatibility test instructs you to do so.

## Pull requests

Keep changes focused and explain the behavior, validation performed, and any
compatibility or operational impact. Performance claims should include the
command, environment, raw result, and limitations needed to reproduce them.

Security vulnerabilities should not be filed as public issues; follow
[SECURITY.md](SECURITY.md) instead.
