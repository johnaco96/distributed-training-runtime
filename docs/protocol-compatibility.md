# Protocol compatibility notes (Phase 17A)

## Current protocol status

- Runtime RPCs currently use the `runtime.v1` protobuf package and generated client/server bindings.
- `proto/runtime.proto` is the source of truth for wire protocol definitions.
- Generated code must not be manually edited.
- Runtime protocol negotiation is **not** implemented yet (documentation-only phase).

## Change safety guidance

### Backward-compatible changes (preferred)

- Add new fields with new field numbers.
- Prefer optional/nullable fields and safe defaults.
- Add new RPC methods without changing existing method names/signatures.
- Add enum values carefully, keeping existing numeric values stable.

### Breaking changes (avoid without migration plan)

- Renaming or removing fields used by existing clients.
- Reusing or changing protobuf field numbers.
- Changing message/RPC semantics in ways older clients cannot safely tolerate.
- Changing required behavior without server-side fallback and documented rollout.

## Client compatibility expectations (current)

- No explicit runtime version/capability handshake is available today.
- Compatibility is maintained by preserving existing `runtime.v1` message and RPC behavior.
- Rust and Python clients should be regenerated after any proto change.
- Older fields should remain stable where possible to avoid breaking pinned clients.

## Planned future versioning work

Later hardening phases may add one or both of:

- explicit protocol version fields in selected requests/responses, and/or
- capability negotiation between client and coordinator.

Those mechanisms are not part of Phase 17A and are not yet enforced at runtime.


## Related reliability notes

For retry safety and idempotent replay behavior at current protocol shape, see `docs/retry-idempotency.md`.
For shard lease ownership/epoch semantics and stale-worker behavior, see `docs/leases.md`.
