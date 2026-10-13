# Consuming

> Two ways to consume: **typed `IConsumer<T>`** (preferred — DI, JSON, hosted)
> and **raw `ConsumeAsync`** (delegates, manual bytes). Plus the full
> `ConsumeOptions` reference.
> Source: `Consumer.cs`, `ConsumeContext.cs`, `ConsumeOptions.cs`,
> `Hosting.cs:112-140,282-399`, `ConsumerRegistry.cs`, `StreamPartitionConsumer.cs`.

## 1. Option A — typed consumer (preferred)

Implement one interface (`Consumer.cs:3-6`):

```csharp
public interface IConsumer<T>
{
    Task Consume(ConsumeContext<T> context, CancellationToken cancellationToken);
}
```

Register stream + options in `Program.cs` (`Hosting.cs:112-118`):

```csharp
services.AddStreamBusConsumer<OrderEvent, OrderConsumer>("orders");
services.AddStreamBusConsumer<OrderEvent, OrderConsumer>("orders-super", o => o
    .SuperStream()
    .Retry(new RetryPolicy { FailureMode = FailureMode.SkipAfterDeadline, MaxAttempts = 3 })
    .ErrorStream(new ErrorStreamOptions { Stream = "orders-errors" }));
```

Write the consumer (transient, DI-resolved, **one scope per message**):

```csharp
public sealed class OrderConsumer(ILogger<OrderConsumer> log, IOrderRepository repo)
    : IConsumer<OrderEvent>
{
    public async Task Consume(ConsumeContext<OrderEvent> ctx, CancellationToken ct)
    {
        // ctx.Message is deserialized JSON; ctx.Body is the raw bytes;
        // ctx.Offset/Partition/Stream/RoutingValue/Metadata describe position.
        await repo.UpsertAsync(ctx.Message, ct);
        // Return = success (store offset per OffsetStore cadence).
        // Throw = retry per RetryPolicy (Strict default = retry forever, in order).
    }
}
```

Runtime behavior (`Hosting.cs:371-399, 602-629`):

- Per delivery: deserialize `Body` with the **bus serializer** → new DI scope →
  resolve `TConsumer` (registered `TryAddTransient`) → call `Consume` with a
  `ConsumeContext<T>` carrying the loop's `CancellationToken`.
- Deserialize failure / null → `StreamBusDeserializationException`
  (stream/partition/offset/type attached) → follows the retry path like any throw.
- Registration order doesn't matter: `AddStreamBusConsumer` **before or after**
  `AddStreamBus` both work; the hosted service matches consumers to buses at
  `StartAsync` and throws if a consumer's marker has no bus.
- Reference defaults to `streambus:<ConsumerFullName>:<Stream>`-style derivation
  (`ReferenceDerivation.Derive(bus, consumer, stream)`, hashed if > 255 bytes).
  Override with `.Reference("my-group")` when you want explicit cursor control.
- Duplicate registration (same marker + message + consumer + stream) throws
  `StreamBusConfigurationException` (`Hosting.cs:334-344`).

## 2. Option B — raw `ConsumeAsync` (delegates)

```csharp
Task<IStreamConsumer> ConsumeAsync(
    string stream,
    ConsumeOptions options,
    Func<ConsumeContext, Task> handler,
    CancellationToken ct = default);
```

```csharp
await using var consumer = await bus.ConsumeAsync("orders",
    new ConsumeOptions { Reference = "billing-projection" },
    ctx =>
    {
        Console.WriteLine($"offset {ctx.Offset}: {Encoding.UTF8.GetString(ctx.Body.Span)}");
        return Task.CompletedTask;
    }, ct);
```

- `ConsumeAsync` awaits **first attach** on all partitions before returning —
  publishes issued *after* it returns are never missed (tail-safe).
- Returns `IStreamConsumer` (`{ Stream, Reference }`, `IAsyncDisposable`).
  Dispose stops delivery and waits for in-flight handlers (a hung handler blocks
  disposal by design — always honor the passed token).
- `ConsumeContext` (raw, `ConsumeContext.cs:12-21`): `Stream`, `Partition`
  (super-stream index, `0` for plain), `Offset` (broker offset),
  `RoutingValue` (from `streambus.routing-key`, super streams),
  `Body` (`ReadOnlyMemory<byte>`), `Metadata`, `CancellationToken` (loop token).
- `MessageMetadata` (`ConsumeContext.cs:3-10`): `MessageId`/`CorrelationId`
  (AMQP props), `ApplicationProperties` (copy), `Timestamp` (creation UTC).

When to use raw: projections without a DTO, multi-type streams, dynamic routing,
operator tools. For domain handlers, prefer typed consumers.

## 3. `ConsumeContext<T>` reference

| Property | Type | Notes |
|---|---|---|
| `Message` | `T` | Deserialized with bus serializer. |
| `Stream` | `string` | Logical stream name you subscribed to. |
| `Partition` | `int` | Super-stream partition index; `0` for plain streams. |
| `Offset` | `ulong` | Broker offset of this message. Stored on success per cadence. |
| `RoutingValue` | `string?` | Super-stream routing key; `null` for plain. |
| `Body` | `ReadOnlyMemory<byte>` | Raw payload (handy for audit/hashing). |
| `Metadata` | `MessageMetadata` | Ids, app properties, timestamp. |
| `CancellationToken` | `CancellationToken` | Loop token — **use it**, not a captured outer token. |

## 4. `ConsumeOptions` reference

Builder: `ConsumeOptionsBuilder` (`Hosting.cs:282-319`); record:
`ConsumeOptions` (`ConsumeOptions.cs:23-36`).

| Field / builder | Default | What it does |
|---|---|---|
| `IsSuperStream` / `.SuperStream(bool = true)` | `false` | `true` = resolve partitions via `QueryPartitionAsync` and consume all; `false` = single plain stream. Must match the broker object. |
| `Reference` / `.Reference(string?)` | `null` → derived | Broker consumer-group / offset cursor. Raw path derives `streambus:<BusFullName>:<stream>`; typed path includes consumer type. ≤ 255 UTF-8 bytes. **Same reference = same cursor** (load-share); **different reference = independent copy** (broadcast). |
| `SingleActiveConsumer` / `.SingleActiveConsumer(bool = true)` | `true` | Exclusive active consumer per (stream, reference). Disabled automatically when the resolved offset is `Next` (SAC + tail attach receives nothing — attach becomes non-exclusive with an info log). |
| `Ordering` / `.Ordering(...)` | `Strict` | Only value today. One message at a time per partition (`_dispatchGate(1,1)`); a failing handler **stalls that partition** until it succeeds / is handed off. Order is never reordered to skip a failure. |
| `Offset` / `.Offset(...)` | `ResumeOrFirst` | Start point — the 5-way enum below. |
| `OffsetStore` / `.OffsetStore(...)` | `EveryMessages: 1`, `MaxInterval: null` | Store cadence: store broker offset every N successes and/or at most every interval. `EveryMessages >= 1`; `MaxInterval > 0` when set. Stores are monotonic (guard rejects rewinds) and serialized per partition. |
| `Retry` / `.Retry(...)` | `Strict`, unbounded | Failure mode + backoff — see [Offsets, Retry & Errors](offsets-retry-errors.md). |
| `ErrorStream` / `.ErrorStream(...)` | `null` | `{ Stream (required), RoutingValue? }`. Only with `SkipAfterDeadline`; target must exist and be **plain**. |
| `OnRetryExhausted` / `.OnRetryExhausted(...)` | `null` | Observe-only hook (`ErrorHandoff`) after handoff. Awaited; throw = warning log; outcome unchanged. |
| `ReadinessPolicy` / `.Readiness(...)` | `FailFastPolicy` | Attach gate: `FailFast` throws `StreamBusNotReadyException` when down; `WaitUpTo`/`Indefinitely` wait. `Fatal` → `StreamBusFatalException`. |

Validation matrix (throws `StreamBusConfigurationException` at start/attach):

- `Strict` + `MaxAttempts != null` → throw (Strict is unbounded by definition).
- `Strict` + `ErrorStream != null` → throw (nothing is ever "exhausted").
- `SkipAfterDeadline` requires **both** `MaxAttempts` and `ErrorStream`.
- `MaxAttempts < 1`, `BaseDelay/MaxDelay <= 0`, `Multiplier < 1` or non-finite,
  `MaxJitter < 0` → throw.
- Error target must exist and be plain (super error target → throw).

## 5. Broadcast vs competing consumers (one rule)

```csharp
// SAME reference on a super stream = compete (partitions split across instances):
new ConsumeOptions { IsSuperStream = true, Reference = "order-processors" }

// DIFFERENT references = broadcast (each gets EVERYTHING):
new ConsumeOptions { Reference = "billing-projection" }
new ConsumeOptions { Reference = "audit-log" }
```

Keep SAC (`true`, default) for competing groups so each partition has exactly
one active reader — this preserves per-key order with hash routing.

## 6. Operator escapes: inspect + move cursors

```csharp
// partition = 0 for plain streams; super streams use the partition index.
ulong? stored = await bus.GetConsumerOffsetAsync("orders", "billing-projection", partition: 0, ct);
await bus.SetConsumerOffsetAsync("orders", "billing-projection", partition: 0, offset: 42, ct);
```

Backed by broker offset queries/stores (`ConsumerRegistry.cs:403-427`). The
local monotonic guard is overwritten so the next store continues from the new
value. Effectively-once references ignore manual sets (resume comes from the
target's publishing ids — see [Effectively-Once](effectively-once-topology.md)).

## 7. AI checklist (consume correctly every time)

1. Prefer `AddStreamBusConsumer<T, C>(stream)`; reserve raw `ConsumeAsync` for
   dynamic/multi-type cases.
2. Always set `IsSuperStream` to match the broker object.
3. Choose the reference deliberately: shared = compete, distinct = broadcast.
4. Leave `Offset = ResumeOrFirst` unless doing an operator replay/tail
   (`AlwaysFirst` / `AlwaysNext`).
5. `Strict` for must-not-lose; `SkipAfterDeadline` + error stream for must-advance.
