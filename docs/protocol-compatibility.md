# Protocol Compatibility Notes

## Current protocol status

- Runtime RPCs currently use the `runtime.v1` protobuf package and generated client/server bindings.
- `proto/runtime.proto` is the source of truth for wire protocol definitions.
- Current informational protocol version metadata is `1.0.0`.
- Generated code must not be manually edited.
- Runtime protocol negotiation is **not** implemented; version metadata is currently informational only.
- `CoordinatorService.GetRuntimeInfo` returns current protocol version and coordinator-advertised capabilities.

## Change safety guidance

### Backward-compatible changes (preferred)

- Add new fields with new field numbers.
- Prefer optional/nullable fields and safe defaults.
- Add new RPC methods without changing existing method names/signatures.
- Add enum values carefully, keeping existing numeric values stable.
- Add new RPC request/response messages instead of changing existing wire message shapes where possible.

### Breaking changes (avoid without migration plan)

- Renaming or removing fields used by existing clients.
- Reusing or changing protobuf field numbers.
- Renaming existing RPC methods or changing existing RPC request/response types.
- Changing message/RPC semantics in ways older clients cannot safely tolerate.
- Changing required behavior without server-side fallback and documented rollout.

## Client compatibility expectations (current)

- `GetRuntimeInfo` is an additive RPC and does not change existing RPC behavior.
- Compatibility is maintained by preserving existing `runtime.v1` message and RPC behavior.
- Rust and Python clients should be regenerated after any proto change.
- Older fields should remain stable where possible to avoid breaking pinned clients.
- Version metadata (`ProtocolVersion`, capability strings) is informational unless explicit enforcement is implemented later.

## Capability metadata currently exposed

Current `GetRuntimeInfo` capabilities are:

- `checkpoint-cleanup-library`
- `idempotency-key-validation`
- `lease-active-worker-gate`
- `local-scaling-benchmarks`
- `s3-storage`

## Enforcement (mechanical)

The change-safety rules above are enforced by a golden-snapshot test so a
breaking wire change cannot merge accidentally:

- `proto/compat_snapshot.json` records, for every message and enum, each
  field/value's **number and type** — the wire-significant facts.
- `python/tests/test_proto_compatibility.py` reparses `proto/runtime.proto`
  and asserts that every entry in the snapshot still exists with the **same
  number and type**. Removing, renaming, renumbering, or retyping a shipped
  field fails the test. It also checks no field number is reused within a
  message and that every enum has a 0 default (proto3's forward-compatible
  fallback for unknown values).

Additive changes (new fields/messages/enum values/RPCs) are allowed — they are
not in the snapshot, so they pass. When you make an intentional additive
change, regenerate and commit the snapshot:

```bash
python python/tests/proto_compat.py --update
```

The snapshot diff makes the additive change explicit and reviewable.

## Planned future versioning work

Later hardening phases may add one or both of:

- explicit protocol version fields in selected requests/responses, and/or
- capability negotiation between client and coordinator.

Those mechanisms are beyond the current informational metadata and are not yet enforced at runtime; the snapshot test above is the current backstop for wire compatibility.


## Related reliability notes

For retry safety and idempotent replay behavior at current protocol shape, see `docs/retry-idempotency.md`.
For shard lease ownership/epoch semantics and stale-worker behavior, see `docs/leases.md`.

