"""Structured error reporting for the admin CLI.

Turns a low-level ``grpc.RpcError`` into an :class:`AdminError` carrying a
human-friendly message, an actionable remediation hint, and a stable process
exit code.  This is the "structured error reporting for users" surface: every
failure the operator sees names *what* went wrong and *what to do next*, and in
``--json`` mode is emitted as a machine-readable object.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Optional

# Process exit codes.  0 is success; usage errors come from argparse (2).
EXIT_OK = 0
EXIT_USAGE = 2
EXIT_NOT_FOUND = 3
EXIT_INVALID_STATE = 4
EXIT_UNAVAILABLE = 5
EXIT_PERMISSION = 6
EXIT_TIMEOUT = 7
EXIT_ERROR = 1


@dataclass
class AdminError(Exception):
    """A user-facing CLI failure with a remediation hint and exit code."""

    message: str
    remediation: str = ""
    exit_code: int = EXIT_ERROR
    # The originating gRPC status code name, when the error came from an RPC.
    status: Optional[str] = None

    def __str__(self) -> str:  # pragma: no cover - trivial
        return self.message

    def to_dict(self) -> dict[str, Any]:
        payload: dict[str, Any] = {"error": self.message}
        if self.status:
            payload["status"] = self.status
        if self.remediation:
            payload["remediation"] = self.remediation
        return payload


# gRPC StatusCode name -> (remediation hint, exit code).  We key off the code
# *name* (a string) so this module never has to import grpc at definition time.
_STATUS_TABLE: dict[str, tuple[str, int]] = {
    "NOT_FOUND": (
        "Check the run/checkpoint id with 'dtruntime-admin runs list'.",
        EXIT_NOT_FOUND,
    ),
    "FAILED_PRECONDITION": (
        "The run is not in a state that allows this action; inspect it with "
        "'dtruntime-admin runs describe <run-id>'.",
        EXIT_INVALID_STATE,
    ),
    "INVALID_ARGUMENT": (
        "Re-check the command arguments.",
        EXIT_USAGE,
    ),
    "UNAVAILABLE": (
        "The coordinator is unreachable. Verify --address and that the "
        "coordinator is running.",
        EXIT_UNAVAILABLE,
    ),
    "DEADLINE_EXCEEDED": (
        "The request timed out. Increase --timeout or check coordinator load.",
        EXIT_TIMEOUT,
    ),
    "UNAUTHENTICATED": (
        "Provide caller identity via DTRUNTIME_AUTH_SUBJECT/DTRUNTIME_AUTH_ROLE "
        "(admin role required).",
        EXIT_PERMISSION,
    ),
    "PERMISSION_DENIED": (
        "This operation requires the admin role; set DTRUNTIME_AUTH_ROLE=admin.",
        EXIT_PERMISSION,
    ),
}


def from_rpc_error(exc: Exception) -> AdminError:
    """Translate a ``grpc.RpcError`` into a structured :class:`AdminError`.

    Falls back to a generic message for non-gRPC exceptions so the CLI always
    has something actionable to print.
    """
    code = getattr(exc, "code", None)
    details = getattr(exc, "details", None)
    code_name = None
    if callable(code):
        try:
            status = code()
            code_name = getattr(status, "name", None) or str(status)
        except Exception:  # pragma: no cover - defensive
            code_name = None
    detail_text = ""
    if callable(details):
        try:
            detail_text = details() or ""
        except Exception:  # pragma: no cover - defensive
            detail_text = ""

    if code_name and code_name in _STATUS_TABLE:
        remediation, exit_code = _STATUS_TABLE[code_name]
        message = detail_text or f"coordinator returned {code_name}"
        return AdminError(
            message=message,
            remediation=remediation,
            exit_code=exit_code,
            status=code_name,
        )

    # Unknown gRPC code, or a plain exception.
    message = detail_text or str(exc) or "unexpected error"
    return AdminError(message=message, status=code_name)
