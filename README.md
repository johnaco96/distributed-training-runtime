# Distributed Training Runtime

A Rust and Python runtime for distributed ML dataset loading, checkpointing, worker coordination, and recovery.

## Goal

Provide a simple Python API for ML training jobs while a Rust backend coordinates:

- distributed dataset registration
- sharded dataset access
- async I/O
- fault-tolerant checkpoint writes
- worker heartbeats
- recovery from node failure
- local and object-storage-backed persistence

## Architecture

Python SDK talks to a Rust coordinator over gRPC.

```text
Python Training Script
        |
        v
Python SDK
        |
        v
gRPC
        |
        v
Rust Coordinator
        |
        v
Storage / Workers / Checkpoints