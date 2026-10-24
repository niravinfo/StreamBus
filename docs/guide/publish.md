# Publish

Typed publish routes by message type. Register once, publish without
stream names or manual serialization.

## Plain streams

```csharp
services.AddStreamBus((context, cfg) =>
{
    cfg.Host("localhost", "/", h => h.Credentials("app", "s3cret"));
    cfg.Publish<OrderEvent>("orders");
});

await bus.PublishAsync(new OrderEvent("o-1", 1), ct);
```

The stream and the bus serializer come from the registration. Registering
one message type on two streams of one bus fails fast instead of picking
a random destination.

## Super streams

```csharp
cfg.PublishSuper<OrderEvent>("orders-super", s => s.Partitions(3));

await bus.PublishSuperAsync(new OrderEvent("o-1", 1), "o-1", ct);
```

The routing key is a required `PublishSuperAsync` parameter — forgetting
it is a compile error, not a runtime surprise. A `PublishSuper`
registration carries its own spec (`s.Partitions(n)`), or a consuming
endpoint on the same stream supplies the count with `e.Partitions(n)`;
with neither, startup fails naming the type and the stream. Registration
kind must agree across every reference to the stream: `Publish` for a
super stream (or `PublishSuper` for a plain one) fails at startup naming
the type and the stream.
Calling the wrong method for a registration (`PublishAsync` on a
`PublishSuper` registration and vice versa) throws
`StreamBusConfigurationException` telling you which method to use.

Consumers never route — an endpoint on a super stream reads all
partitions. `Partitions` on the endpoint mirrors the super expectation:

```csharp
cfg.ReceiveEndpoint("orders-super", e =>
{
    e.Consumer<SuperOrderConsumer>("super-processors");
    e.Partitions(3);
});
```

## Overrides and escape hatches

```csharp
await bus.PublishAsync("orders-manual", new OrderEvent("o-1", 1), ct);
await bus.PublishSuperAsync("orders-super", new OrderEvent("o-1", 1), "o-1", ct);
await bus.PublishRawAsync("orders", rawBytes, new PublishOptions(), ct);
```

Explicit-stream targets keep the method-implied kind (plain vs super). An
undeclared target surfaces `StreamBusStreamNotFoundException` — the call
site never auto-creates. `PublishRawAsync` sends pre-serialized bytes.

## Pipelines

Cross-cutting publish logic is an ordered, additive filter pipeline.
Filters run first-registered-first-in / last-out and can enrich the legal
headers (`string`, `bool`, `int`, `long`, `null`) without touching the
payload or routing:

```csharp
services.AddSingleton(new TenantScope()); // ambient state used by the filter
cfg.UsePublishFilter<TenantFilter>();      // bus-wide, applied to every publish
cfg.UsePublishFilter<CorrelationIdFilter>();

public sealed class TenantFilter(TenantScope ambient) : IStreamPublishFilter
{
    public Task OnPublish(StreamPublishFilterContext context, PublishDelegate next, CancellationToken ct)
    {
        context.Headers.Set("x-tenant", ambient.Tenant);
        return next(context);
    }
}
```

Filters are singletons per bus and must be thread-safe; each publish gets
its own DI scope through `context.Services`. A filter must call `next`
exactly once - zero or double continuations throw
`StreamBusConfigurationException` (a second call is rejected before the
duplicate send). Call-level
`PublishOptions.Properties` win over filter headers on key conflicts.
`PublishRawAsync` bypasses the pipeline. Complete samples:
`examples/StreamBus.Examples.Producer/TenantFilter.cs` and
`CorrelationIdFilter.cs`.

## Serializer

```csharp
cfg.Serializer(json => json.PropertyNamingPolicy = JsonNamingPolicy.CamelCase);
```

## Full example

```csharp
services.AddStreamBus((context, cfg) =>
{
    cfg.Host("localhost", "/", h => h.Credentials("app", "s3cret"));
    cfg.PublishSuper<OrderEvent>("orders-super", s => s.Partitions(3));
    cfg.Publish<AuditEvent>("orders-manual"); // declares the explicit target
});

public sealed class CheckoutWorker(IStreamBus bus) : BackgroundService
{
    protected override async Task ExecuteAsync(CancellationToken ct)
    {
        await bus.WaitUntilReadyAsync(ReadinessPolicy.UpTo(TimeSpan.FromSeconds(30)), ct);

        // Routed publish: the key selects the partition.
        await bus.PublishSuperAsync(new OrderEvent("o-1", 1), "o-1", ct);

        // Explicit-stream override to a second registered stream; the target
        // does not need an OrderEvent publish registration.
        await bus.PublishAsync("orders-manual", new OrderEvent("o-2", 1), ct);

        // Raw bytes escape hatch (pre-serialized payload).
        var raw = SystemTextJsonStreamSerializer.Default.Serialize(new OrderEvent("o-3", 1));
        await bus.PublishRawAsync("orders-manual", raw, new PublishOptions(), ct);
    }
}
```
