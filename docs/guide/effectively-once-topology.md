# Effectively-Once & Topology

> Read-process-write without duplicates (`ConsumeEffectivelyOnceAsync`) and
> declaring streams / super streams (`IStreamTopology`, startup topology).
> Source: `EffectivelyOnce.cs`, `Bus.cs:22-36`, `ConsumerRegistry.cs:276-401`,
> `StreamPartitionConsumer.cs:587-681`, `Topology.cs`, `TopologyRegistry.cs`.

## 1. Effectively-once pipelines

Problem: consume → compute → publish, crash-safe, no duplicates downstream.

```csharp
await using var pipeline = await bus.ConsumeEffectivelyOnceAsync(
    sourceStream: "orders",
    targetStream: "invoices",
    new EffectivelyOnceOptions
    {
        SourceIsSuperStream = false,
        TargetIsSuperStream = false,
        // TargetRouting: default = incoming RoutingValue; required input for super targets.
    },
    async (ctx, ct) =>
    {
        var order = SystemTextJsonStreamSerializer.Default.Deserialize<OrderEvent>(ctx.Body);
        var invoice = new InvoiceEvent(order.OrderId, order.Sequence * 2);
        return SystemTextJsonStreamSerializer.Default.Serialize(invoice);
        // return bytes = result; throw = Strict-unbounded retry (never skip).
    }, ct);
```

How it works:

- **No source offsets stored** (FR-10.14). Resume is recomputed from the
  **target's last publishing id** on every attach/promote/reconnect
  (`QueryStoredAsync` → `ResumeQuery`, max over partitions, `+1` applied once by
  the shared stored+1 rule). Operator `SetConsumerOffset` on the pipeline
  reference has no effect.
- **Publishing id = source offset** (`ResultNaming.PublishingId`), published
  under a stable per-(source-partition, target) producer name
  (`streambus-eo-{partition}-to-{target}`, hashed if long). Broker dedup by
  `(name, id)` absorbs replays after crashes.
- Publish retried **forever until confirmed** (no timeout); only then is the
  next message attempted. Handler throw → same Strict-unbounded backoff as
  normal `Strict` retry.
- Constraints (validated, `EffectivelyOnceValidation`): `Retry` must be
  `Strict` + unbounded; super target requires routing (`TargetRouting` or the
  message's routing value; null → attach fails rather than misrouting); **one
  pipeline per (source, target)** — names collide otherwise; lag gauge
  meaningless on these references (nothing stored to compare).

`EffectivelyOnceOptions` (`EffectivelyOnce.cs:6-14`):

| Field | Default | Notes |
|---|---|---|
| `SourceIsSuperStream` / `TargetIsSuperStream` | `false` | Must match broker objects (validated). |
| `Reference` | `null` → `streambus-eo:<bus>:<source>-><target>` | Stable cursor namespace for the pipeline. |
| `TargetRouting` | `null` → incoming `RoutingValue` | `(ConsumeContext) => string?`. Mandatory effective value for super targets. |
| `Retry` | `Strict` unbounded | Only Strict/unbounded allowed. |
| `ReadinessPolicy` | `FailFastPolicy` | Attach gate. |

Manual escape: `PublishResultAsync(target, result, sourceOffset, new
PublishResultOptions { ProducerName = "my-writer", ... })` — same id rule, but
combined with a storing consumer it is **at-least-once**; effectively-once holds
for the managed cycle only. Never share one `ProducerName` concurrently.

## 2. Topology: declaring streams

Two equivalent paths — **startup declarations** (preferred, code + config) and
**explicit calls** (operator flows, tests):

```csharp
// startup (auto-declared with infinite retry at StartAsync):
o.Topology(t => t
    .DeclareStream("orders", k => k.MaxAge(TimeSpan.FromDays(7)))
    .DeclareSuperStream("orders-super", partitions: 3));

// anytime (IStreamBus.Topology, Bus.cs:42):
await bus.Topology.EnsureStreamAsync(new StreamDeclaration { Name = "orders" }, ct);
await bus.Topology.EnsureSuperStreamAsync(
    new SuperStreamDeclaration { Name = "orders-super", Partitions = 3 }, ct);
```

Shapes (`Topology.cs:10-33`): `StreamDeclaration { Name (required), MaxAge?,
MaxLengthBytes?, MaxSegmentSizeBytes?, LeaderLocator = LeastLeaders }`;
`SuperStreamDeclaration` adds `Partitions (required, >= 1)`.
`StreamLeaderLocator`: `LeastLeaders` (default), `ClientLocal`, `Random`.

Semantics (`TopologyRegistry.cs`):

- **Idempotent**: same spec twice = no-op (in-process fingerprint + tolerate
  already-exists cross-process). Safe to declare on every startup and every replica.
- **Immutable**: conflicting spec (plain↔super swap, partition count, differing
  args on a create race) → `StreamConfigurationConflictException` naming expected
  vs actual. Fix = delete/recreate the stream or align the declaration.
- Plain-over-super name coexistence is allowed by the broker but treated as a
  **conflict** (Ruling R6) — keep names unambiguous.
- Publishing/consuming never auto-create: missing stream → 
  `StreamBusStreamNotFoundException` (publish) / startup-retry + `StreamNotFound`
  gate (consume). Declare first.

Retention guidance: streams are append-only logs — always set `MaxAge` (and/or
`MaxLengthBytes`) on high-volume and error streams, or they grow unboundedly.
