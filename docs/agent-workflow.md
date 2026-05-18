# Agent Workflow

This document defines how AI agents should work on this repository.

The goal is to keep development controlled, reviewable, and technically honest while using AI agents to help build the Distributed Training Data and Checkpoint Runtime.

## Project Summary

This project is a distributed runtime for large ML training jobs.

The runtime provides:

- Python-facing APIs for users
- Rust-based core runtime services
- gRPC communication between Python clients and Rust services
- distributed dataset registration
- sharded dataset access
- worker coordination
- async I/O
- fault-tolerant checkpoint writing
- recovery from worker and node failures
- local and object-storage-backed persistence

The Python SDK talks to the Rust coordinator over gRPC.

This project does not replace PyTorch Distributed, DeepSpeed, Ray, Kubernetes, or an ML training framework. It provides infrastructure around training: dataset coordination, checkpoint persistence, runtime metadata, and recovery.

## Core Rule

Agents must work on narrow, well-defined tasks.

Do not modify unrelated files.

Do not redesign the whole project unless explicitly assigned an architecture task.

Do not claim features are production-ready unless they have been implemented, tested, and benchmarked.

## Branch Strategy

The main long-lived branches are:

```text
main
develop