# Effectively-once

Read-process-write pipelines consume a source stream without storing
source offsets and publish each result with the source offset as its
publishing id. The resume point is recomputed from the target on every
attach, so at-most-one pipeline may run per (source, target) pair.

## Managed pipeline

```csharp
services.AddStreamBus((context, cfg) =>
{
    cfg.Host("localhost", "/", h => h.Credentials("app", "s3cret"));
    cfg.Publish<OrderEvent>("orders");
    cfg.Publish<InvoiceEvent>("invoices");
});

await using var pipe = await bus.ConsumeEffectivelyOnceAsync(
    "orders",
    "invoices",
    new EffectivelyOnceOptions { SourceIsSuperStream = false, TargetIsSuperStream = false },
    ctx =>
    {
        var order = SystemTextJsonStreamSerializer.Default.Deserialize<OrderEvent>(ctx.Body);
        var invoice = new InvoiceEvent(order.OrderId);
        return Task.FromResult<ReadOnlyMemory<byte>>(
            SystemTextJsonStreamSerializer.Default.Serialize(invoice));
    },
    ct);
```

Handler results publish with `publishing-id = source offset`, retrying
until confirmed — a result is never skipped silently. Concurrent use of
one producer name is forbidden; the managed path serializes per name, so
manual publishers must do the same. A manual publish combined with a
storing consumer is at-least-once; effectively-once holds for the
managed cycle only.

## Options

`EffectivelyOnceOptions`: `SourceIsSuperStream` / `TargetIsSuperStream`
for super endpoints, optional `Reference` (derived pipeline names apply
when unset), `TargetRouting` (defaults to the incoming routing value),
plus unbounded `Retry` and `ReadinessPolicy`. Consume filters attached to
an effectively-once pipeline must call `next()` exactly once; returning
without it is a runtime contract error.

## Manual result publish

```csharp
await bus.PublishResultAsync(
    targetStream: "invoices",
    result: invoiceBytes,
    sourceOffset: offset,
    options: new PublishResultOptions { ProducerName = "projection-0" },
    ct: ct);
```

`ProducerName` scopes idempotency: reuse one name per
(source-partition, target) sequence and never share it across
concurrent publishers. Result publishing retries until confirmed;
configure delays through `PublishResultOptions.Retry`.
