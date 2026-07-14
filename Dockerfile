# Local/dev container image for distributed-training-runtime Phase 20E.
FROM rust:1.95-bookworm AS rust-builder
WORKDIR /build
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        protobuf-compiler \
        libprotobuf-dev \
        pkg-config \
        libsqlite3-dev \
        ca-certificates \
    && rm -rf /var/lib/apt/lists/*
ENV PROTOC=/usr/bin/protoc
ENV PROTOC_INCLUDE=/usr/include
COPY Cargo.toml Cargo.lock ./
COPY crates ./crates
COPY proto ./proto
RUN cargo build --release -p runtime-coordinator -p runtime-bench

FROM python:3.12-slim-bookworm AS runtime
WORKDIR /app

COPY --from=rust-builder /build/target/release/runtime-coordinator /usr/local/bin/runtime-coordinator
COPY --from=rust-builder /build/target/release/runtime-bench /usr/local/bin/runtime-bench
COPY python ./python
COPY pyproject.toml README.md ./

RUN pip install --no-cache-dir --upgrade pip \
    && pip install --no-cache-dir grpcio grpcio-tools protobuf numpy \
    && pip install --no-cache-dir "torch>=2.0.0" --index-url https://download.pytorch.org/whl/cpu

ENV PYTHONPATH=/app/python
EXPOSE 50051

# Default entrypoint is the coordinator.  Override with --entrypoint runtime-bench
# (or any other binary) to run benchmarks inside the cluster.
ENTRYPOINT ["runtime-coordinator"]
CMD ["--addr", "0.0.0.0:50051"]
