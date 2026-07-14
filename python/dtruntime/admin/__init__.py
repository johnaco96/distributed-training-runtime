"""Administrative CLI and helpers for the Distributed Training Runtime.

The ``dtruntime-admin`` console script (entry point :func:`dtruntime.admin.cli.main`)
provides operators with commands to inspect and manage runs: list/describe runs,
cancel/pause/resume/retry/drain a run, list workers/shards/checkpoints, verify a
checkpoint's artifacts, and a ``doctor`` command that diagnoses stuck workers and
bad checkpoints.
"""

from __future__ import annotations

from .cli import main

__all__ = ["main"]
