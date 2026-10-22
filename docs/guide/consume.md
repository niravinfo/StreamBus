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
stream must agree per knob; see `topology.md` for the full
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
`effectively-once.md` for the full projection example:

```csharp
await using var pipe = await bus.ConsumeEffectivelyOnceAsync("orders", "invoices",
    new EffectivelyOnceOptions(), ctx => Task.FromResult<ReadOnlyMemory<byte>>(resultBytes), ct);
```

## Pipelines

Consume middleware is ordered, additive, and scope-aware. Bus-wide
filters run first, then endpoint filters; the first filter is the
outermost wrapper:

```csharp
services.AddScoped<TenantScope>();
cfg.UseConsumeFilter<TenantFilter>();             // bus-wide
cfg.ReceiveEndpoint("orders", e =>
{
    e.Consumer<OrderConsumer>("order-processors");
    e.UseConsumeFilter<MetricsFilter>();          // appended after bus-wide
});

public sealed class TenantFilter : IStreamConsumeFilter
{
    public Task OnConsume(StreamConsumeFilterContext context, ConsumeDelegate next, CancellationToken ct)
    {
        var scope = (TenantScope)context.Services.GetRequiredService(typeof(TenantScope));
        scope.Tenant = context.Message.Metadata.GetString("x-tenant");
        context.SetPayload(scope.Tenant);
        return next(context);
    }
}
```

One DI scope is created per delivered message and shared by the pipeline
and the typed consumer, so scoped services resolve to the same instance in
filters and consumer constructors. `SetPayload<T>` carries values between
filters in the pipeline. Filters are singletons per bus. A filter must
call `next` exactly once; zero without `SkipHandler`, or more than once,
is retried and then
exhausted rather than silently advancing the offset. Skipping requires
`CanSkip => true`; raw endpoints skip freely, typed endpoints must opt in
with `e.AllowPipelineSkip()`, and effectively-once pipelines reject
skippable filters at attach. Headers written through
`context.OutputHeaders` are carried on the exhaustion envelope and on
effectively-once result publishes. Complete sample:
`examples/StreamBus.Examples.Consumer/TenantFilter.cs`.

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
    ctx => bus.SetConsumerOffsetAsync(
        "audit", "ops-tail", ctx.Partition, ctx.Offset, ctx.CancellationToken),
    ct);

var stored = await bus.GetConsumerOffsetAsync("audit", "ops-tail", partition: 0, ct);
```
