# Publishing

> `IStreamBus.PublishAsync` + `PublishResultAsync`: signatures, super streams,
> timeouts, serialization, producer reuse, and every exception you can get.
> Source: `src/StreamBus/Bus.cs`, `ProducerRegistry.cs`, `ProducerHandles.cs`,
> `EffectivelyOnce.cs`, `PublishExceptions.cs`.

## 1. The publish API (one method to learn)

```csharp
Task PublishAsync(
    string stream,
    ReadOnlyMemory<byte> body,
    PublishOptions? options = null,
    CancellationToken ct = default);
```

- StreamBus publishes **raw bytes**. Serialize first — JSON by default:
  ```csharp
  var body = SystemTextJsonStreamSerializer.Default.Serialize(new OrderEvent("o-1", 1));
  await bus.PublishAsync("orders", body, new PublishOptions(), ct);
  ```
- `options` is optional (`null` → `new PublishOptions()`).
- Resolve `bus` how? `IStreamBus` for the default bus,
  `IStreamBus<TBus>` for a named bus — see [Multiple Buses](multiple-buses.md).
- Publishing **never auto-creates** streams. Declare via startup topology or
  `bus.Topology.EnsureStreamAsync(...)` first, or you get
  `StreamBusStreamNotFoundException`.

`PublishOptions` (`PublishOptions.cs:3-9`):

| Field | Default | Meaning |
|---|---|---|
| `ReadinessPolicy` | `FailFastPolicy` | Gate before publish: `FailFast` throws `StreamBusNotReadyException` if not connected; `UpTo(t)` waits up to `t`; `Indefinitely` waits forever. `Fatal` connection → `StreamBusFatalException` regardless. |
| `IsSuperStream` | `false` | `true` = publish into a super stream's partitions (requires `RoutingKey`). |
| `RoutingKey` | `null` | **Required when `IsSuperStream`.** The partition hash input — use your partition key (e.g. `orderId`), never the event-type name. Missing → `StreamBusConfigurationException`. |
| `Timeout` | `10s` | Per-publish broker-confirm timeout. Must be `> Zero`. On expiry the cached producer is discarded and `StreamBusTimeoutException` thrown — **outcome unknown** (may or may not have appended). |

Plain vs super-stream publish:

```csharp
// plain stream
await bus.PublishAsync("orders", body, new PublishOptions(), ct);

// super stream: routing key picks the partition
await bus.PublishAsync("orders-super", body, new PublishOptions
{
    IsSuperStream = true,
    RoutingKey = order.OrderId,
}, ct);
```

## 2. Readiness + timeout patterns

```csharp
// Worker startup: wait once, then publish with FailFast (cheap per call).
var ready = await bus.WaitUntilReadyAsync(ReadinessPolicy.UpTo(TimeSpan.FromSeconds(30)), ct);
if (ready is not BusReadiness.Ready)
    throw new InvalidOperationException($"Bus {ready}");

// Per-call wait (e.g. first publish after idle):
await bus.PublishAsync(stream, body, new PublishOptions
{
    ReadinessPolicy = ReadinessPolicy.UpTo(TimeSpan.FromSeconds(5)),
    Timeout = TimeSpan.FromSeconds(10),
}, ct);
```

Cancellation (`ct`) cancels **your wait**, not the broker append — a cancelled
publish may still have landed. Design publishers idempotent or use
effectively-once pipelines for read-process-write flows.

## 3. Serialization

Default `SystemTextJsonStreamSerializer` (`Serialization.cs:11-24`).
Configure once on the bus (`o.Serializer(...)`) and reuse the same options in
your publishers. The typed-consumer side deserializes with the bus serializer,
so a publisher/consumer option mismatch surfaces as
`StreamBusDeserializationException` on consume (fail-fast, with
stream/partition/offset/type attached).

Custom payload (protobuf etc.): implement `IStreamSerializer`, pass via
`o.Serializer(custom)`, serialize in publishers with the same code.

## 4. What happens under the hood (why publishes are safe to retry — mostly)

- **Producer cache**: one cached producer handle per `plain:{stream}` /
  `super:{stream}` key (`ProducerRegistry.cs:94`). Concurrent publishers share it.
- **Unique broker producer names**: every handle is created with a unique
  reference (`streambus-pub-{stream}-{seq}`, hashed if > 255 bytes,
  `Connector.cs:38-50`). The broker dedups by `(name, publishingId)` and ids
  restart at 1 per handle — reusing a name would silently drop the new handle's
  first messages as duplicates. StreamBus never reuses names for normal publishes.
- **Confirms**: each `Send` awaits broker confirm. Negative confirm →
  `StreamBusPublishException` (handle kept). Transient error →
  `StreamBusPublishTransientException` (handle discarded, safe to retry with a
  fresh handle). Confirm timeout → `StreamBusTimeoutException`, handle discarded,
  **outcome unknown**.
- **Fatal connection** (`ProtocolException` / `SslException` / auth incl. `guest`
  loopback trap) → `StreamBusFatalException`; fix config, restart.

## 5. `PublishResultAsync` (effectively-once building block)

```csharp
Task PublishResultAsync(
    string targetStream,
    ReadOnlyMemory<byte> result,
    ulong sourceOffset,          // becomes the broker publishing-id (dedup key)
    PublishResultOptions options, // required ProducerName (+ RoutingKey if super target)
    CancellationToken ct = default);
```

- Retries **until confirmed or cancelled** — deliberately **no timeout**
  (`Bus.cs:13-22`). A result is never silently skipped.
- `ProducerName` is required, ≤ 255 UTF-8 bytes; super target requires
  `RoutingKey`. One producer name = one logical writer: **never use the same
  name concurrently** from two tasks (managed `ConsumeEffectivelyOnceAsync`
  serializes per name for you).
- Manual `PublishResultAsync` + a normally-storing consumer = **at-least-once**.
  True effectively-once holds only for the managed
  `ConsumeEffectivelyOnceAsync` cycle — see
  [Effectively-Once & Topology](effectively-once-topology.md).

## 6. Exception reference (publish path)

| Exception | When | Retry? |
|---|---|---|
| `StreamBusConfigurationException` | `Timeout <= 0`; super without routing key; name/ref > 255 B; unknown locator/policy; pool > 255; bad topology knobs | Fix config/code |
| `StreamBusNotReadyException` | `FailFast`/`WaitUpTo` publish while `Starting`/`TransientFailure`; null handle | Wait for `Ready`, then retry |
| `StreamBusFatalException` | Connection is `Fatal` (protocol/SSL/auth) | Fix root cause, restart |
| `StreamBusStreamNotFoundException` | Target stream (or super) doesn't exist | Declare it first |
| `StreamBusPublishException` | Broker negative-confirm; producer-create failure; duplicate in-flight publishing id | Inspect broker/message; retry may be safe for creates |
| `StreamBusPublishTransientException` | Cached producer discarded after transient error | **Safe to retry** (fresh handle) |
| `StreamBusTimeoutException` | No confirm within `Timeout` | **Outcome unknown** — dedup/idempotency needed before retry |

## 7. AI checklist (publish correctly every time)

1. `await bus.WaitUntilReadyAsync(...)` before the first publish (or set
   per-call `ReadinessPolicy`).
2. Serialize with the same options as `o.Serializer(...)`.
3. Super stream → always set `RoutingKey` = partition key.
4. Declare the stream (startup topology or `Ensure*Async`) — no auto-create.
5. Catch `StreamBusTimeoutException` as *unknown*, not *failed*.
