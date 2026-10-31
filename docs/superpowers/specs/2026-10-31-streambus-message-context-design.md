# StreamBus message context

Date: 2026-10-31
Status: proposed
Supersedes the context, header, and metadata shape in
`2026-10-27-streambus-single-filter-context-design.md` and the wire-metadata
sections of `2026-10-25-streambus-wire-metadata-design.md`.

## Intent

One context per delivery and one per publish, flat, with no nested
`Raw`/`Metadata` objects and no separate filter context. Filters and consumers
receive the same instance. Wire encoding never appears in the public API: ids
are `Guid`, the send time is `DateTime`, headers are one interface family.

The surface is frozen for 1.0. After this change the names below do not move.

## Why the current shape is wrong

- `StreamConsumeFilterContext.Raw` is a leftover from the removed `Inner`
  wrapper. When the wrapper went away, the base property had to give up the
  name `Message`, so the transport record was renamed `Raw` and left nested.
- Headers have three names for two concepts. `StreamPublishFilterContext.Headers`
  is the outgoing collection; `StreamConsumeFilterContext.OutputHeaders` is
  *not* the received headers but the headers of the result/error message;
  the received headers are reachable only as
  `Raw.Metadata.ApplicationProperties`.
- `MessageMetadata` duplicates broker fields one level down and exposes
  `string?` ids that the consumer must parse.
- `StreamPublishFilterContext.Bus` is never read by any filter or test, and
  `StreamBusRegistrationView` describes the connection rather than the bus.
- `MessageMetadata.Timestamp` is written in one place and read nowhere.
- `MessageType`/`RuntimeMessageType` exist for generic or polymorphic dispatch,
  which StreamBus does not support: one consumer class handles one closed
  message type, and a declared type is a single identity.

## Decisions

### One context, two directions

| Type | Received by |
|---|---|
| `ConsumeContext` | `IConsumer<T>`, `IStreamConsumeFilter`, raw `ConsumeAsync` |
| `ConsumeContext<T>` | `IConsumer<T>` and `IStreamConsumeFilter<T>` for type `T` |
| `PublishContext` | `IStreamPublishFilter`, raw `PublishRawAsync` |
| `PublishContext<T>` | `IStreamPublishFilter<T>`, `PublishAsync<T>` |

`ConsumeContext<T>` derives from `ConsumeContext` and is the same object.
`PublishContext<T>` derives from `PublishContext` and is the same object.

### The nullability rule

> Anything StreamBus sends, StreamBus fills before the pipeline runs:
> non-nullable. Anything StreamBus receives may come from any producer:
> nullable.

Exception: `CorrelationId` is nullable in both directions because StreamBus
never invents one. A fabricated correlation makes unrelated messages look
related.

### Wire mapping

| Context member | Wire location | Encoding | Public type |
|---|---|---|---|
| `MessageId` | AMQP `message-id` | `Guid.ToString("D")` string | `Guid` / `Guid?` |
| `CorrelationId` | AMQP `correlation-id` | `Guid.ToString("D")` string | `Guid?` |
| `SentTime` | AMQP `creation-time` | AMQP timestamp, int64 ms, UTC | `DateTime` / `DateTime?` |
| content type | AMQP `content-type` | symbol | not exposed |
| `Headers` | application properties | map of string/bool/int/long/null | `IStreamHeaders` |
| `SendHeaders` | application properties | same | `IStreamSendHeaders` |
| `RoutingValue` | application property `streambus.routing-key` | string | `string?` |
| `Stream`, `Partition`, `Offset` | broker | partition log position | unchanged |

AMQP properties are used for the standard AMQP fields. Application properties
are used for everything StreamBus-specific. The public API exposes neither:
`Guid` and `DateTime` are the context types, and the encoding is an internal
detail.

Content type is written from `IStreamSerializer.ContentType`, which
`SystemTextJsonStreamSerializer` returns as `application/json`. It is not
exposed on the context; adding `ContentType` later as a nullable property is
non-breaking.

`SentTime` is generated at `DateTime.UtcNow`, **truncated to milliseconds**, so
the value observed by a publish filter and the value observed by a consumer are
exactly equal. Millisecond precision is the AMQP timestamp precision. If
100 ns precision is ever required, an optional `streambus.sent-time`
application property carries the exact value and wins over the AMQP value.

### No generic consumers, no type properties

- `MessageType` and `RuntimeMessageType` are removed from both contexts.
- `TryGetMessage<T>` is not added. A universal filter that needs the typed
  view uses `context is ConsumeContext<OrderEvent> typed`.
- Typed filters (`IStreamConsumeFilter<T>`, `IStreamPublishFilter<T>`) remain the
  supported way to get `T`.

### Headers

```csharp
public interface IStreamHeaders
{
    IReadOnlyDictionary<string, object?> All { get; }
    bool TryGet(string key, out object? value);
    string? GetString(string key);
    int? GetInt(string key);
    long? GetLong(string key);
    bool? GetBool(string key);
    T? GetJson<T>(string key);
}

public interface IStreamSendHeaders : IStreamHeaders
{
    void Set(string key, object? value);
    bool Remove(string key);
}
```

- `Headers` on a consume context is the read-only received collection.
- `Headers` on a publish context is `IStreamSendHeaders`.
- `SendHeaders` on a consume context is the writable collection carried on the
  effectively-once result or the error envelope.

Framework keys are visible in `Headers.All`; they are not hidden. Reserved keys
cannot be written:

- `streambus.message-id`
- `streambus.correlation-id`
- `streambus.sent-time`
- `streambus.routing-key`

Enforced in `StreamSendHeaders.Set` for filters and at the publish entry point
for `PublishOptions.Properties.Headers`, which bypasses filters.

`GetJson<T>` uses the bus serializer, not a hard-coded default, so custom
`JsonSerializerOptions` apply.

### Super-stream routing

The routing key is persisted as the `streambus.routing-key` application
property on every publish where it is set, including effectively-once results
published to a super target. The consume side already reads this key into
`RoutingValue`; the write side is the missing half.

## Final surface

```csharp
/// <summary>One delivery from a stream partition, shared by the handler and every consume filter.</summary>
public class ConsumeContext
{
    public required string Stream { get; init; }
    public required int Partition { get; init; }
    public required ulong Offset { get; init; }
    public required string? RoutingValue { get; init; }
    public required ReadOnlyMemory<byte> Body { get; init; }

    public Guid? MessageId { get; init; }
    public Guid? CorrelationId { get; init; }
    public DateTime? SentTime { get; init; }

    public required IStreamHeaders Headers { get; init; }
    public required IStreamSendHeaders SendHeaders { get; init; }
    public required IServiceProvider Services { get; init; }
    public required CancellationToken CancellationToken { get; init; }

    public T? GetPayload<T>();
    public bool TryGetPayload<T>(out T? payload);
    public void SetPayload<T>(T value);
}

public sealed class ConsumeContext<T> : ConsumeContext
{
    public required T Message { get; init; }
}

/// <summary>One publish, shared by the caller and every publish filter.</summary>
public class PublishContext
{
    public required string Stream { get; init; }
    public required string? RoutingKey { get; init; }
    public required object Payload { get; init; }

    public required Guid MessageId { get; init; }
    public Guid? CorrelationId { get; init; }
    public required DateTime SentTime { get; init; }

    public required IStreamSendHeaders Headers { get; init; }
    public required IServiceProvider Services { get; init; }
    public required CancellationToken CancellationToken { get; init; }
}

public sealed class PublishContext<T> : PublishContext
{
    public T Message => (T)Payload;
}

public interface IConsumer<T>
{
    Task Consume(ConsumeContext<T> context);
}

public interface IStreamConsumeFilter
{
    Task OnConsume(ConsumeContext context, ConsumeDelegate next);
}

public interface IStreamConsumeFilter<T>
{
    Task OnConsume(ConsumeContext<T> context, ConsumeDelegate next);
}

public interface IStreamPublishFilter
{
    Task OnPublish(PublishContext context, PublishDelegate next);
}

public interface IStreamPublishFilter<T>
{
    Task OnPublish(PublishContext<T> context, PublishDelegate next);
}
```

Only headers are mutable. `Stream`, `RoutingKey`, `Payload`, `MessageId`,
`CorrelationId`, `SentTime`, `Services`, and `CancellationToken` are fixed for
the publish; the body, identity, and time are fixed for the delivery.

## Deleted and renamed

| Before | After |
|---|---|
| `StreamConsumeFilterContext` | `ConsumeContext` |
| `StreamConsumeFilterContext<T>` | `ConsumeContext<T>` |
| `StreamPublishFilterContext` | `PublishContext` |
| `StreamPublishFilterContext<T>` | `PublishContext<T>` |
| `ConsumeContext<T>` (record in `Consumer.cs`) | derived from `ConsumeContext` |
| `IStreamHeaderWriter` | `IStreamSendHeaders` |
| `MessageMetadata` | removed; members flatten onto the context |
| `context.Raw.Metadata.GetString(k)` | `context.Headers.GetString(k)` |
| `context.OutputHeaders` | `context.SendHeaders` |
| `context.Bus` | removed; the caller holds the bus |
| `context.MessageType` | removed; use a typed filter or `is` |
| `Consume(ctx, ct)` | `Consume(ctx)`; use `ctx.CancellationToken` |
| `MessageMetadata.MessageId` (`string?`) | `context.MessageId` (`Guid?`) |
| `MessageMetadata.CorrelationId` (`string?`) | `context.CorrelationId` (`Guid?`) |
| `MessageMetadata.Timestamp` (`DateTimeOffset?`) | `context.SentTime` (`DateTime?`) |
| `MessageMetadata.ApplicationProperties` | `context.Headers.All` |
| `StreamMessageProperties.MessageId` (`string?`) | `Guid?` |
| `StreamMessageProperties.CorrelationId` (`string?`) | `Guid?` |
| `ExhaustedContext.Metadata` | flat `MessageId`, `CorrelationId`, `SentTime`, `Headers` |
| `ErrorEnvelope.MessageId` / `CorrelationId` (`string?`) | `Guid?` |

## Implementation notes

1. **AMQP properties.** `StreamMessageProperties.ApplyTo` writes `MessageId`,
   `CorrelationId`, `CreationTime`, `ContentType` and
   `streambus.routing-key`. `WriteAny` in the client has no `Guid` case, so ids
   are written as `ToString("D")` strings.
2. **Reading ids.** `Guid.TryParse` on the string; also accept a 16-byte array
   (an external producer may write a native AMQP UUID, which this client reads
   back as `byte[]`). Anything else is `null`, never an exception.
3. **Reading `CreationTime`.** `Properties.CreationTime` is a non-nullable
   `DateTime` in the client, so an absent timestamp decodes as `default`. Map
   `default` to `null`. `Kind` is `Utc` (the client returns
   `DateTimeOffset.FromUnixTimeMilliseconds(ms).UtcDateTime`), so no
   `SpecifyKind` is needed; delete the existing one.
4. **Truncation.** Generate `SentTime` as
   `DateTimeOffset.FromUnixTimeMilliseconds(DateTimeOffset.UtcNow.ToUnixTimeMilliseconds()).UtcDateTime`.
5. **Content type.** Add `string ContentType { get; }` to `IStreamSerializer`;
   `SystemTextJsonStreamSerializer` returns `application/json`.
6. **Routing key.** Pass `PublishOptions.RoutingKey` into
   `StreamMessageProperties.ApplyTo` from `ProducerHandle.SendAsync` and
   `SuperStreamProducerHandle.SendMessageAsync`. The key is written only when
   non-null and overrides any user value.
7. **Headers objects.** `StreamHeaders` is the read-only view over the decoded
   application properties. `StreamSendHeaders` is the writable collection used
   for publish headers, for the consume `SendHeaders`, and as the seed of the
   publish `Headers`.
8. **Reserved keys.** `StreamSendHeaders.Set` and the publish entry-point
   validation reject the four framework keys.
9. **Context construction.** `ConsumePipeline.CreateContext` builds
   `ConsumeContext<T>` directly for typed endpoints and `ConsumeContext` for raw
   and effectively-once pipelines. The publish path builds `PublishContext<T>`
   where `T` is statically known.
10. **Error envelope.** Envelope ids become `Guid?`; the `streambus.error.*`
    values are written as strings and parsed back with `Guid.TryParse`. The
    envelope carries `SentTime` so a requeued or replayed message keeps its
    original send time.

## Tests

- Publish writes `message-id`, `correlation-id`, `creation-time`, and
  `content-type` to the AMQP properties section and nothing else.
- Publish `MessageId` and `SentTime` are generated when the caller supplies
  none; supplied values are preserved.
- Round trip: consumer `MessageId`/`CorrelationId` equal the published values;
  consumer `SentTime` equals the publish `SentTime` exactly.
- A raw message with properties but no creation time yields `SentTime == null`.
- A raw message with a non-Guid `message-id` yields `MessageId == null`.
- A native AMQP UUID `message-id` (16 bytes) parses to the expected `Guid`.
- Super-stream publish writes `streambus.routing-key`; the consumer's
  `RoutingValue` equals the publishing routing key; an effectively-once result
  published to a super target carries it too.
- `StreamSendHeaders.Set("streambus.message-id", …)` throws;
  `PublishOptions.Properties.Headers` containing a reserved key throws.
- `Headers.All` contains `streambus.*` keys alongside user headers.
- `GetJson<T>` honours the configured serializer options.
- Universal and typed filters receive the same instance (`Assert.Same`).
- No public `Raw`, `Metadata`, `OutputHeaders`, `MessageType`,
  `RuntimeMessageType`, `Bus`, or `MessageMetadata` remains.

## Definition of done

- One consume context type pair and one publish context type pair; no wrapper
  and no filter-specific context.
- `Raw`, `Metadata`, `OutputHeaders`, `MessageType`, `RuntimeMessageType`,
  `Bus`, and `MessageMetadata` are gone from the public surface.
- Ids are `Guid`, `SentTime` is `DateTime`, headers are `IStreamHeaders` /
  `IStreamSendHeaders`.
- AMQP properties carry the standard envelope fields; application properties
  carry everything StreamBus-specific.
- Super-stream routing keys are written and observable as `RoutingValue`.
- Reserved framework keys cannot be written by users.
- `dotnet build` passes with 0 warnings; unit suite passes; brokered
  integration tests pass when a broker is available.
- Guide, examples, and README use the new surface; the changelog records the
  breaking change under 0.3.0.
