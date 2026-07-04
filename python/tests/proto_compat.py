"""Parse proto/runtime.proto into a compatibility manifest.

The manifest captures, for every message and enum, the *wire-significant*
facts that must never change for backward/forward compatibility:

- message field: name → (field_number, type_label)
- enum value:    name → number

Shared by the compatibility test (which compares the current proto against a
committed golden snapshot) and the ``--update`` regeneration path.
"""

from __future__ import annotations

import json
import re
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
PROTO_PATH = REPO_ROOT / "proto" / "runtime.proto"
SNAPSHOT_PATH = REPO_ROOT / "proto" / "compat_snapshot.json"

# Matches: [repeated] <type...> <name> = <number>;  (type may contain
# map<...> with a comma, or a qualified name).
_FIELD_RE = re.compile(
    r"^\s*(?:(repeated)\s+)?([\w.]+(?:<[^>]+>)?)\s+(\w+)\s*=\s*(\d+)\s*;"
)
_ENUM_VALUE_RE = re.compile(r"^\s*(\w+)\s*=\s*(\d+)\s*;")


def _strip_comments(text: str) -> str:
    # Remove // line comments (proto has no block comments in this file).
    return "\n".join(line.split("//", 1)[0] for line in text.splitlines())


def build_manifest(proto_text: str) -> dict:
    """Return {"messages": {msg: {field: [num, type]}}, "enums": {enum: {val: num}}}."""
    text = _strip_comments(proto_text)
    messages: dict[str, dict] = {}
    enums: dict[str, dict] = {}

    stack: list[tuple[str, str]] = []  # (kind, name)
    for line in text.splitlines():
        stripped = line.strip()
        msg_match = re.match(r"^(message|enum|oneof|service)\s+(\w+)\s*\{?", stripped)
        if msg_match:
            kind, name = msg_match.group(1), msg_match.group(2)
            # oneof fields belong to the enclosing message; keep that context.
            if kind == "oneof":
                stack.append(("oneof", stack[-1][1] if stack else name))
            else:
                stack.append((kind, name))
                if kind == "message":
                    messages.setdefault(name, {})
                elif kind == "enum":
                    enums.setdefault(name, {})
            continue
        if stripped.startswith("}"):
            if stack:
                stack.pop()
            continue
        if not stack:
            continue

        kind, name = stack[-1]
        if kind in ("message", "oneof"):
            m = _FIELD_RE.match(line)
            if m:
                _, ftype, fname, fnum = m.groups()
                messages[name][fname] = [int(fnum), ftype]
        elif kind == "enum":
            m = _ENUM_VALUE_RE.match(line)
            if m:
                vname, vnum = m.groups()
                enums[name][vname] = int(vnum)

    return {"messages": messages, "enums": enums}


def current_manifest() -> dict:
    return build_manifest(PROTO_PATH.read_text(encoding="utf-8"))


def load_snapshot() -> dict:
    return json.loads(SNAPSHOT_PATH.read_text(encoding="utf-8"))


def write_snapshot(manifest: dict) -> None:
    SNAPSHOT_PATH.write_text(
        json.dumps(manifest, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )


if __name__ == "__main__":
    if "--update" in sys.argv:
        write_snapshot(current_manifest())
        print(f"wrote {SNAPSHOT_PATH.relative_to(REPO_ROOT)}")
    else:
        print(json.dumps(current_manifest(), indent=2, sort_keys=True))
