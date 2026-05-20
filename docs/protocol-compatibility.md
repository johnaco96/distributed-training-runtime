# Protocol compatibility notes (Phase 17E)

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

## Planned future versioning work

Later hardening phases may add one or both of:

- explicit protocol version fields in selected requests/responses, and/or
- capability negotiation between client and coordinator.

Those mechanisms are beyond the current Phase 17E informational metadata and are not yet enforced at runtime.


## Related reliability notes

For retry safety and idempotent replay behavior at current protocol shape, see `docs/retry-idempotency.md`.
For shard lease ownership/epoch semantics and stale-worker behavior, see `docs/leases.md`.


- Checkpoint cleanup helpers in Phase 17D are library-level storage operations and do not change existing runtime.v1 RPC wire compatibility.
