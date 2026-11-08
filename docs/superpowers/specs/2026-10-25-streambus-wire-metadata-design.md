# StreamBus wire metadata and type identity

Date: 2026-10-25
Status: proposed (separate from the typed-filter design)

## Intent

Typed StreamBus messages currently carry only the serialized payload and
whatever headers the caller supplies. A consumer cannot tell from the wire
what type the producer intended, and there is no automatic message id,
correlation id, or timestamp.

This design adds a small StreamBus-native metadata set without wrapping the
payload in a MassTransit-style envelope. The body stays byte-for-byte the
serialized message; metadata lives in AMQP properties and application
properties.

This is deliberately a separate change from
[`2026-10-25-streambus-typed-filter-design.md`](2026-10-25-streambus-typed-filter-design.md).
Typed filters attach by compile-time `T` and do not depend on wire metadata.

## Goals

- Typed publish automatically writes:
  - a message id (caller-supplied or generated),
  - a creation timestamp (caller-supplied or publish time),
  - a stable message-type identifier for the declared `T`,
  - the caller's correlation id when supplied.
- Typed consume exposes the same values through `MessageMetadata` and validates
  the message-type identifier when it is present.
- A type mismatch fails before deserialization with a clear, dedicated
  exception naming the expected and actual type.
- Raw publish remains an explicit escape hatch: no automatic metadata, but
  callers can supply it.
- Raw consume exposes whatever metadata is present and does not validate a
  type.
- Error hand-off, requeue, and replay preserve the original message metadata.
- Messages without the new metadata remain consumable (backward compatible).

## Non-goals

- No envelope around the payload. The body remains the serialized message.
- No MassTransit fields that StreamBus does not need yet: conversation id,
  request id, initiator id, source/destination/response/fault addresses,
  host metadata, expiration, or a full message-type URN array.
- No polymorphic dispatch and no multi-type stream routing. A declared type
  is a single identity; a mismatch is an error, not a skip.
- No schema/version negotiation.
- No cryptographic authenticity. A type header is routing/diagnostic
  metadata, not proof of the payload's contents.
- No automatic correlation-id generation or ambient propagation in v1.
  Correlation is carried when supplied.
- No `ContentType` stamping in v1; `IStreamSerializer` does not expose one.
- No metadata for raw `PublishRawAsync` unless the caller supplies it.
- No metadata for effectively-once result publishes, whose bytes have no
  declared `T`.

## Wire representation

### AMQP properties

| Metadata | AMQP property | Notes |
|---|---|---|
| Message id | `MessageId` | string; caller value wins, otherwise generated |
| Correlation id | `CorrelationId` | string; caller value only |
| Creation time | `CreationTime` | UTC `DateTime`; caller value wins, otherwise publish time |

`MessageMetadata` already reads all three. `CreationTime` is the timestamp
StreamBus publishes; no separate `SentTime` field is needed.

### Application properties

| Key | Value | Notes |
|---|---|---|
| `streambus.message-type` | stable type identifier | written by typed publish; optional for raw publish |

The existing `PublishOptions.Properties.Headers` remain application
properties. The exact key `streambus.message-type` is library-owned: user
headers carrying that key are rejected at publish time so they cannot
silently override the typed value. This is intentionally narrower than
reserving the whole `streambus.` prefix: `streambus.error.*` remains owned by
the error envelope (which already wins on collision), and
`streambus.routing-key` remains the existing read-side convention. Raw
callers that need a type use `StreamMessageProperties.MessageType`, not a
header.

The body is unchanged. No envelope is added.

### Enforcement points for the reserved key

- `HeaderWriter.Set` rejects `streambus.message-type` so publish and consume
  filters cannot write it through `IStreamHeaderWriter`.
- The typed and raw publish entry points validate
  `PublishOptions.Properties.Headers` before sending; a user-supplied header
  with that key is an `ArgumentException`.
- `StreamMessageProperties.ApplyTo` keeps a final guard for the normal send
  path. `SendEnvelopeAsync` (error hand-off) bypasses it and writes the
  envelope's own `streambus.error.*` keys, so the error path is unaffected.
- Effectively-once result publishes pass `OutputHeaders` through
  `PublishReplayAsync`; they are subject to the same guard. A filter that
  writes `streambus.message-type` fails rather than corrupting result
  metadata.

## Type identity

A type identifier must be stable enough to survive normal refactoring in a
development project, and must not depend on assembly version. The default is:

```csharp
public static string For(Type type) => type.ToString();
```

`Type.ToString()` gives `Namespace.Type` for ordinary types and a
non-assembly-qualified form for closed generics, for example
`MyApp.Envelope\`1[MyApp.OrderEvent]`. `Type.FullName` is not used because
closed generic `FullName` embeds assembly-qualified generic arguments and is
brittle across versions and runtimes.

A future optional `[StreamBusMessageType("orders.order-created.v1")]`
attribute can provide a cross-language, renaming-stable identifier. v1 uses
the default `Type.ToString()` so the wire contract is simple and requires no
new attribute.

The identifier is based on the **declared pipe type** (`T` in
`PublishAsync<T>` / the consumer's `IConsumer<T>`), not the runtime payload
type. Publishing through an interface or base type uses that interface/base
identifier; consuming a concrete type with the same body expects the concrete
identifier. This is exact-match semantics. Polymorphic identity is out of
scope.

## Publishing

Typed publish builds an effective `StreamMessageProperties` once per call,
before the publish pipeline runs:

- `MessageId`: caller value if non-empty; otherwise
  `Guid.NewGuid().ToString("D")`.
- `CorrelationId`: caller value if non-empty; otherwise null.
- `Timestamp`: caller value if supplied; otherwise `DateTimeOffset.UtcNow`.
- `MessageType`: the identity of declared `T`.

The generated values are created before filters and before any send retry, so
a retried publish keeps the same id/timestamp. Publish filters can continue to
add application headers through `StreamPublishFilterContext.Headers`; they do
not currently expose AMQP properties, so they cannot change the generated
id/correlation/timestamp. That can be added later if a real need appears.

`StreamMessageProperties` gains optional `Timestamp` and `MessageType`
members so callers can supply them explicitly for raw publish or advanced
cases. For typed publish, an explicit `MessageType` must equal the declared
`T` identity; a mismatch is an `ArgumentException` rather than silently
writing conflicting metadata.

`PublishRawAsync` does **not** generate metadata. It writes only what the
caller supplied. This keeps replay, hand-off, and pre-serialized payloads
under explicit control.

## Consuming

`MessageMetadata` gains `MessageType` (string?). The existing `MessageId`,
`CorrelationId`, and `Timestamp` already map from AMQP properties.

For a typed receive endpoint with message type `T`, validation runs in the
partition consumer **before eager deserialization and before the consume
filter pipeline**, once per retry attempt:

1. Read `streambus.message-type` from application properties.
2. If absent: accept the message for backward compatibility with old
   StreamBus producers, raw publishers, and external systems.
3. If present and equal to `StreamMessageTypeIdentity.For(typeof(T))`: accept.
4. If present and different: throw `StreamBusMessageTypeMismatchException`
   before deserialization. The exception carries stream, partition, offset,
   expected type, actual type, and is a `StreamBusException`.
5. Run eager deserialization (when typed filters are attached), then the
   filter pipeline, then the typed handler.

Validation therefore cannot be suppressed by a filter setting
`SkipHandler`/`AllowPipelineSkip`, and filters never observe a mismatched
message. The typed endpoint passes its expected type through
`ConsumeOptions.MessageType` (the same internal field used by eager typed
filters). Raw endpoints leave it null and never validate.

Raw `ConsumeAsync` never validates a type. It can inspect
`MessageMetadata.MessageType` if present.

A mismatch is treated like a deserialization failure under the existing
retry/exhaustion policy. It is a permanent payload/configuration problem, so
`FailureMode.Strict` will keep retrying it; users who want poison messages
handed off should use a bounded retry policy with an error stream or queue.
A future general "permanent exception" classification can bypass retries if
that becomes necessary.

### Multi-type streams

This design does **not** make one stream with multiple message types work.
If an endpoint expects `OrderEvent` and receives a message marked
`AuditEvent`, it fails with a type-mismatch error; it does not skip the
message. Supporting shared streams requires separate routing semantics
(skip or dispatch), and that is a separate design.

## Error hand-off, requeue, and replay

`ErrorEnvelope` gains optional `MessageType` and `MessageTimestamp` fields,
stored as `streambus.error.message-type` and
`streambus.error.message-timestamp` alongside the existing message id and
correlation id.

When a failed delivery is handed off, the original message metadata is
captured in the envelope. When requeueing from an error queue or replaying an
error stream, the original `MessageId`, `CorrelationId`, `Timestamp`, and
`MessageType` are restored into `PublishOptions.Properties` instead of being
dropped. This keeps the replayed message self-describing and preserves
idempotency/diagnostic identity.

Output headers added by consume filters continue to be forwarded by the error
envelope; the envelope's own `streambus.error.*` keys win on collision, as
today. A type-mismatch validation failure occurs before filters run, so that
envelope has no filter output headers.

## API changes

New public surface:

```csharp
public sealed class StreamBusMessageTypeMismatchException : StreamBusException
{
    public string Stream { get; }
    public int Partition { get; }
    public ulong Offset { get; }
    public string ExpectedType { get; }
    public string ActualType { get; }
}
```

Changed public surface:

- `StreamMessageProperties` gains `DateTimeOffset? Timestamp` and
  `string? MessageType`.
- `MessageMetadata` gains `string? MessageType`.
- `ErrorEnvelope` gains `string? MessageType` and
  `DateTimeOffset? MessageTimestamp`.
- `ConsumeOptions` gains an internal expected message type for typed
  endpoints; raw/EO pipelines leave it null.
- `StreamBusErrorClassification` does not change in v1; type mismatch follows
  the same path as deserialization failure.

No envelope type is introduced.

## Compatibility and breaking changes

- Existing messages without `streambus.message-type` continue to be consumed.
- Existing raw producers are unaffected.
- Typed publish now adds AMQP `MessageId`, `CreationTime`, and
  `streambus.message-type` when absent. Consumers that ignore those properties
  are unaffected; body bytes are unchanged.
- User headers carrying the exact key `streambus.message-type` now fail
  validation; raw callers use `StreamMessageProperties.MessageType`.
  `streambus.error.*` and `streambus.routing-key` keep their current
  behavior.
- An external producer that writes a different `streambus.message-type`
  format will be rejected by typed consumers. Such producers can omit the key
  to use the legacy path.
- `StreamMessageProperties`/`MessageMetadata` additions are optional members;
  direct construction remains source-compatible.
- Error-envelope additions are optional; existing serialized envelopes remain
  parseable.

## Test plan

Unit:

- typed publish generates a message id and UTC creation time when absent;
- explicit id/correlation/timestamp are preserved;
- explicit message type equal to `T` is accepted; conflicting message type
  throws;
- raw publish adds nothing automatically and writes only explicit metadata;
- `StreamMessageTypeIdentity.For` is stable for ordinary, nested, and closed
  generic types and does not include assembly versions;
- reserved `streambus.message-type` header is rejected at the user-header
  boundary; `streambus.error.*` headers retain collision semantics;
- typed consume accepts matching type, accepts missing type, and throws
  `StreamBusMessageTypeMismatchException` on mismatch;
- mismatch is detected before the filter pipeline, so a skippable filter
  cannot suppress it and filters do not run for that attempt;
- raw consume exposes metadata without validation;
- error envelope round-trips message type/timestamp/id/correlation;
- requeue and replay restore metadata.

Integration:

- typed publish -> typed consume sees the same message id, correlation id,
  timestamp, and type;
- an external/raw publisher without metadata is accepted by a typed endpoint;
- a wrong-type message is rejected before deserialization and reaches the
  configured error destination under a bounded retry policy;
- replay from an error stream preserves the original metadata;
- two message types on one stream both fail on the other's messages (documented
  limitation), rather than silently deserializing.

Docs:

- `docs/guide/publish.md`: automatic metadata, explicit overrides, raw
  behavior, reserved `streambus.message-type` key.
- `docs/guide/consume.md`: metadata fields, type validation and its limits,
  multi-type stream caveat.
- `docs/guide/errors.md` or equivalent: error-envelope metadata and replay
  preservation.

## Open decisions

1. **Type identity format.** Recommended: `Type.ToString()` in v1, optional
   `[StreamBusMessageType]` attribute later. MassTransit-style URNs are not
   required for StreamBus-to-StreamBus communication.
2. **Correlation id.** Recommended: carry explicit values only in v1; add
   ambient propagation from consume to publish later if needed. Auto-generating
   a fresh correlation for every message would make correlation less useful.
3. **Mismatch policy.** Recommended: fail before deserialization and follow the
   existing retry/exhaustion policy. A terminal poison classification is a
   separate improvement.
4. **Multi-type streams.** Recommended: explicitly out of scope; mismatch is
   an error, not a skip. If shared streams are a requirement, design routing
   separately.
5. **Message id format.** Recommended: `Guid.NewGuid().ToString("D")`.
6. **Replay preservation.** Recommended: in scope for this change; otherwise
   replayed messages lose the identity the metadata was added to provide.
7. **Reserved metadata key.** Recommended: reserve only
   `streambus.message-type` (enforced at the user-header boundary and in the
   raw send path), not the whole `streambus.` prefix. Keep `streambus.error.*`
   and `streambus.routing-key` behavior unchanged.

## Definition of done

- Typed publish writes the metadata; raw publish does not generate it.
- Typed consume validates when the type key is present and stays compatible
  when it is absent.
- Error hand-off/requeue/replay preserve metadata.
- Unit and integration tests pass; docs state both the behavior and the
  multi-type limitation.
