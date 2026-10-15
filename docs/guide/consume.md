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

Stream declaration knobs live on the endpoint too and feed startup
topology derivation: `SetSuperStream(partitions)`, `SetMaxAge`,
`SetMaxLengthBytes`, `SetMaxSegmentSizeBytes`, `SetLeaderLocator`.
An endpoint stream with an explicit `DeclareStream`/`DeclareSuperStream`
in the same service is declared and verified; without one the endpoint
adopts the broker reality (matching kind is consumed read-only, a kind
mismatch is a conflict, a missing stream is declared when possible and a
configuration error otherwise).

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
on, which is unambiguous by construction:

```csharp
await using var pipe = await bus.ConsumeEffectivelyOnceAsync("orders", "invoices",
    new EffectivelyOnceOptions(), async (ctx, ct) => resultBytes, ct);
```
