"""Rendering helpers for the admin CLI: enum labels, timestamps, tables.

Kept free of any gRPC/network imports so the formatting logic is trivially unit
testable in isolation.
"""

from __future__ import annotations

from typing import Any, Iterable, Sequence


def strip_enum_prefix(name: str, prefix: str) -> str:
    """``RUN_STATE_RUNNING`` + ``RUN_STATE_`` -> ``running``."""
    if name.startswith(prefix):
        name = name[len(prefix) :]
    return name.lower()


def add_enum_prefix(value: str, prefix: str) -> str:
    """``running`` + ``RUN_STATE_`` -> ``RUN_STATE_RUNNING`` (accepts either form)."""
    token = value.strip().upper().replace("-", "_")
    if not token.startswith(prefix):
        token = prefix + token
    return token


def format_timestamp(ts: Any) -> str:
    """Render a protobuf ``Timestamp`` as ISO-8601 UTC, or ``-`` when unset."""
    if ts is None:
        return "-"
    seconds = getattr(ts, "seconds", 0)
    nanos = getattr(ts, "nanos", 0)
    if seconds == 0 and nanos == 0:
        return "-"
    try:
        dt = ts.ToDatetime()
    except Exception:  # pragma: no cover - defensive
        return "-"
    return dt.replace(microsecond=0).isoformat() + "Z"


def relative_deadline(ts: Any, now_ts: Any) -> str:
    """Signed seconds until ``ts`` relative to ``now_ts`` (server clock).

    Positive = in the future, negative = overdue.  ``-`` when either is unset.
    """
    ts_s = getattr(ts, "seconds", 0)
    now_s = getattr(now_ts, "seconds", 0)
    if ts_s == 0 or now_s == 0:
        return "-"
    delta = ts_s - now_s
    sign = "+" if delta >= 0 else "-"
    return f"{sign}{abs(delta)}s"


def render_table(headers: Sequence[str], rows: Iterable[Sequence[Any]]) -> str:
    """Fixed-width text table.  Empty ``rows`` yields a ``(no rows)`` line."""
    rows = [[("" if c is None else str(c)) for c in row] for row in rows]
    if not rows:
        return "  ".join(headers) + "\n(no rows)"
    widths = [len(h) for h in headers]
    for row in rows:
        for i, cell in enumerate(row):
            widths[i] = max(widths[i], len(cell))
    lines = ["  ".join(h.ljust(widths[i]) for i, h in enumerate(headers))]
    lines.append("  ".join("-" * widths[i] for i in range(len(headers))))
    for row in rows:
        lines.append("  ".join(cell.ljust(widths[i]) for i, cell in enumerate(row)))
    return "\n".join(lines)


def render_kv(pairs: Sequence[tuple[str, Any]]) -> str:
    """Aligned ``key: value`` block for 'describe'-style output."""
    width = max((len(k) for k, _ in pairs), default=0)
    return "\n".join(f"{k.ljust(width)} : {v}" for k, v in pairs)
