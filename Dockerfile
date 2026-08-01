# Two images from one file:
#
#   docker build -t dtruntime-coordinator .                  # control plane (default)
#   docker build -t dtruntime-worker --target worker .       # training worker
#
# They are deliberately separate. The coordinator is a self-contained Rust
# binary that needs no Python at all, so shipping it on a ~1 GB torch image
# would be ~30x the necessary attack surface and pull time. Workers are the
# ones that need PyTorch and the SDK.

# ── build stage: the Rust control plane ──────────────────────────────────────
FROM rust:1.95-bookworm AS rust-builder
WORKDIR /build
# protoc for the tonic/prost codegen in runtime-proto's build.rs. SQLite is NOT
# installed: rusqlite uses its `bundled` feature and compiles it from source.
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        protobuf-compiler \
        libprotobuf-dev \
    && rm -rf /var/lib/apt/lists/*
ENV PROTOC=/usr/bin/protoc
ENV PROTOC_INCLUDE=/usr/include
COPY Cargo.toml Cargo.lock ./
COPY crates ./crates
COPY proto ./proto
RUN cargo build --release -p runtime-coordinator -p runtime-bench

# ── worker image: PyTorch + the Python SDK ───────────────────────────────────
# Not the default target. Build it with `--target worker`.
FROM python:3.12-slim-bookworm AS worker
WORKDIR /app
COPY pyproject.toml README.md LICENSE ./
COPY python ./python
# Install the SDK properly rather than leaning on PYTHONPATH, so the console
# scripts (dtruntime-launch, dtruntime-admin) are on PATH and the dependency
# set is the one pyproject declares.
RUN pip install --no-cache-dir --upgrade pip \
    && pip install --no-cache-dir torch --index-url https://download.pytorch.org/whl/cpu \
    && pip install --no-cache-dir .
RUN useradd --create-home --uid 10001 dtruntime
USER 10001
ENTRYPOINT ["dtruntime-launch"]
CMD ["--help"]

# ── coordinator image (default target) ───────────────────────────────────────
# Only glibc and CA certificates are needed: the binary statically links SQLite
# and the certs are for outbound TLS to S3-compatible object storage.
FROM debian:bookworm-slim AS coordinator
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/*

COPY --from=rust-builder /build/target/release/runtime-coordinator /usr/local/bin/runtime-coordinator
COPY --from=rust-builder /build/target/release/runtime-bench /usr/local/bin/runtime-bench

# Run unprivileged, with a writable data directory for the SQLite metadata
# store and local checkpoint artifacts. Mount a volume here to persist them.
RUN useradd --uid 10001 --no-create-home --shell /usr/sbin/nologin dtruntime \
    && mkdir -p /var/lib/dtruntime \
    && chown 10001:10001 /var/lib/dtruntime
USER 10001
WORKDIR /var/lib/dtruntime
VOLUME ["/var/lib/dtruntime"]

EXPOSE 50051
# Override with `--entrypoint runtime-bench` to run the load generator against
# a coordinator from inside the cluster.
ENTRYPOINT ["runtime-coordinator"]
CMD ["--addr", "0.0.0.0:50051", \
     "--metadata-db", "/var/lib/dtruntime/coordinator.db", \
     "--storage-root", "/var/lib/dtruntime/artifacts"]
