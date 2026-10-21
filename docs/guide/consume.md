# Consume

Endpoints bind a stream to exactly one consumer type and one consumer
group. The group is the cursor identity — the library never derives it.

## ReceiveEndpoint

```csharp
cfg.ReceiveEndpoint("orders", e =>
{
    e.Consumer<OrderConsumer>("order-processors");
});
```

- `Consumer<T>("group")` is required and may be called exactly once per
  endpoint. `T` must implement `IConsumer<>` for one message type.
- The group is used verbatim. Empty groups and groups over the broker's
  255-byte limit are configuration errors.
- Two services sharing one consumer class must supply different groups.
  The same group across instances scales out through a shared cursor.

### Cross-process caveat

Same reference, `SingleActiveConsumer = false`, two processes: both receive
deliveries (at-least-once doubles, never skips). Stores interleave; a stale
process may regress the broker-side offset - never run two writers on one
reference outside SAC.

## Knobs

```csharp
cfg.ReceiveEndpoint("orders", e =>
{
    e.Consumer<OrderConsumer>("order-processors");
    e.SingleActiveConsumer(false);
    e.Offset(ConsumeOffset.AlwaysFirst);
    e.OffsetStore(new OffsetStoreOptions { EveryMessages = 10 });
    e.Readiness(ReadinessPolicy.UpTo(TimeSpan.FromSeconds(30)));
    e.SuperStream(); // expect a super stream without declaring partitions here
});
```

Stream spec knobs live on the endpoint too and feed startup topology
derivation: `Partitions(n)` (super expectation, mirrors the publish
registration's `s.Partitions(n)`), `MaxAge`, `MaxLengthBytes`,
`MaxSegmentSizeBytes`, `LeaderLocator`. Multiple references to one
stream must agree per knob; see `guide/topology.md` for the full
declare-level rules. The endpoint adopts the broker reality (matching
kind is consumed as-is, a kind mismatch is a conflict, a missing stream
is declared from the endpoint's and any publish registration's spec).

## Raw consume

```csharp
await using var sub = await bus.ConsumeAsync(
    "audit",
    new ConsumeOptions { Reference = "ops" },
    handler,
    ct);
```

Raw consume also requires `Reference` — a missing group throws
`StreamBusConfigurationException` at attach instead of guessing.

## Effectively-once

Read-process-write pipelines are unchanged apart from the bus they run
on, which is unambiguous by construction. See
`guide/effectively-once.md` for the full projection example:

```csharp
await using var pipe = await bus.ConsumeEffectivelyOnceAsync("orders", "invoices",
    new EffectivelyOnceOptions(), async (ctx, ct) => resultBytes, ct);
```

## Full example

```csharp
cfg.ReceiveEndpoint("orders", e =>
{
    e.Consumer<OrderConsumer>("order-processors");
    e.SingleActiveConsumer();
    e.Offset(ConsumeOffset.ResumeOrFirst);
    e.OffsetStore(new OffsetStoreOptions { EveryMessages = 10, MaxInterval = TimeSpan.FromSeconds(5) });
});

// Raw tail with manual offset control.
await using var tail = await bus.ConsumeAsync(
    "audit",
    new ConsumeOptions { Offset = ConsumeOffset.AlwaysFirst, Reference = "ops-tail" },
    async (ctx, ct) =>
    {
        await bus.SetConsumerOffsetAsync("audit", "ops-tail", ctx.Partition, ctx.Offset, ct);
    },
    ct);

var stored = await bus.GetConsumerOffsetAsync("audit", "ops-tail", partition: 0, ct);
```
