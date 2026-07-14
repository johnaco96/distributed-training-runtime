"""Enforce protobuf/gRPC backward/forward compatibility rules.

The wire compatibility contract (see docs/protocol-compatibility.md):

- **Field numbers are forever.** A field's number and type must never change
  once shipped, and a number must never be reused for a different field.
- **Only additive changes are backward/forward compatible.** New fields, new
  messages, new enum values (with new numbers), and new RPCs are allowed.
- **Removing or renaming a field is breaking** — it must instead be
  deprecated/reserved, which this test surfaces so the change is deliberate.

Enforcement is a golden snapshot (``proto/compat_snapshot.json``): every entry
that existed before must still exist with the same number and type.  When you
*intentionally* add fields, regenerate the snapshot:

    python python/tests/proto_compat.py --update

and commit it alongside the proto change — the diff makes the additive change
reviewable.
"""

from __future__ import annotations


from proto_compat import build_manifest, current_manifest, load_snapshot


def test_no_field_numbers_reused_within_a_message() -> None:
    manifest = current_manifest()
    for msg, fields in manifest["messages"].items():
        numbers = [num for num, _type in fields.values()]
        assert len(numbers) == len(set(numbers)), (
            f"message {msg} reuses a field number: {sorted(numbers)}"
        )
    for enum, values in manifest["enums"].items():
        numbers = list(values.values())
        # Enum aliases (same number) are only legal with allow_alias; this
        # proto does not use it, so numbers must be unique.
        assert len(numbers) == len(set(numbers)), (
            f"enum {enum} reuses a value number: {sorted(numbers)}"
        )


def test_every_enum_has_a_zero_default() -> None:
    # proto3 requires the first enum value to be 0; it is the safe default a
    # forward-compatible reader falls back to for unknown values.
    manifest = current_manifest()
    for enum, values in manifest["enums"].items():
        assert 0 in values.values(), f"enum {enum} is missing a 0 default value"


def test_existing_fields_are_unchanged_vs_snapshot() -> None:
    """The core append-only guarantee: nothing that shipped may change."""
    snapshot = load_snapshot()
    current = current_manifest()

    breaking: list[str] = []

    for msg, fields in snapshot["messages"].items():
        if msg not in current["messages"]:
            breaking.append(f"message {msg} was removed")
            continue
        current_fields = current["messages"][msg]
        for fname, (number, ftype) in fields.items():
            if fname not in current_fields:
                breaking.append(f"{msg}.{fname} (#{number}) was removed/renamed")
                continue
            cur_number, cur_type = current_fields[fname]
            if cur_number != number:
                breaking.append(
                    f"{msg}.{fname} changed field number {number} -> {cur_number}"
                )
            if cur_type != ftype:
                breaking.append(
                    f"{msg}.{fname} changed type {ftype} -> {cur_type}"
                )

    for enum, values in snapshot["enums"].items():
        if enum not in current["enums"]:
            breaking.append(f"enum {enum} was removed")
            continue
        current_values = current["enums"][enum]
        for vname, number in values.items():
            if vname not in current_values:
                breaking.append(f"{enum}.{vname} (={number}) was removed/renamed")
            elif current_values[vname] != number:
                breaking.append(
                    f"{enum}.{vname} changed number {number} -> {current_values[vname]}"
                )

    assert not breaking, (
        "breaking proto changes detected (this violates wire compatibility):\n  "
        + "\n  ".join(breaking)
        + "\n\nIf a change is genuinely additive, regenerate the snapshot:\n"
        "  python python/tests/proto_compat.py --update"
    )


def test_parser_detects_a_synthetic_breaking_change() -> None:
    """Guard the guard: the manifest diff must actually catch a renumber."""
    before = build_manifest("message M {\n  string a = 1;\n}\n")
    after = build_manifest("message M {\n  string a = 2;\n}\n")
    assert before["messages"]["M"]["a"] == [1, "string"]
    assert after["messages"]["M"]["a"] == [2, "string"]
    assert before != after
