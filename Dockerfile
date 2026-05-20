# Local/dev container image for distributed-training-runtime Phase 20E.
FROM rust:1.89-bookworm AS rust-builder
WORKDIR /build
COPY Cargo.toml Cargo.lock ./
COPY crates ./crates
RUN cargo build --release -p runtime-coordinator

FROM python:3.12-slim-bookworm AS runtime
WORKDIR /app

COPY --from=rust-builder /build/target/release/runtime-coordinator /usr/local/bin/runtime-coordinator
COPY python ./python
COPY pyproject.toml README.md ./

RUN pip install --no-cache-dir --upgrade pip \
    && pip install --no-cache-dir grpcio grpcio-tools protobuf \
    && pip install --no-cache-dir "torch>=2.0.0" --index-url https://download.pytorch.org/whl/cpu

ENV PYTHONPATH=/app/python
EXPOSE 50051

CMD ["runtime-coordinator", "--addr", "0.0.0.0:50051"]
