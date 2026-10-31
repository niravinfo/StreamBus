# Multi-bus

One `AddStreamBus` registers the default bus (`IStreamBus`); each
`AddStreamBus<TBus>` registers one more (`IStreamBus<TBus>`). The
non-generic overload may be used at most once per service, and each
marker at most once. Registrations sharing a vhost must agree on the
connection identity.

```csharp
services.AddStreamBus<OrdersBus>((context, cfg) =>
{
    cfg.Host("localhost", "/orders", h => h.Credentials("app", "s3cret"));
    cfg.Publish<OrderEvent>("orders");
    cfg.ReceiveEndpoint("orders", e => e.Consumer<OrderConsumer>("order-processors"));
});

services.AddStreamBus<BillingBus>((context, cfg) =>
{
    cfg.Host("localhost", "/billing", h => h.Credentials("app", "s3cret"));
    cfg.ReceiveEndpoint("invoices", e => e.Consumer<InvoiceConsumer>("billing-projector"));
});

public sealed class CheckoutWorker(
    IStreamBus<OrdersBus> orders,
    IStreamBus<BillingBus> billing) : BackgroundService
{
    protected override async Task ExecuteAsync(CancellationToken ct)
    {
        await orders.PublishAsync(new OrderEvent("o-1", 1), ct);
    }
}
```

A consumer class shared by two services (or two buses) must be
registered with a different group per bus — there is no default to
collide on:

```csharp
cfg.ReceiveEndpoint("orders", e => e.Consumer<SharedConsumer>("orders-team"));
cfg.ReceiveEndpoint("invoices", e => e.Consumer<SharedConsumer>("billing-team"));
```

Publish registrations are per bus too: the same message type on two
streams of one bus is an ambiguity error, while the same type on two
buses resolves independently through each bus's own route table.

## Full example

Two services share one consumer class with different groups — no
default exists to collide on, and each bus keeps its own cursor:

```csharp
public sealed class SharedConsumer(ILogger<SharedConsumer> log) : IConsumer<OrderEvent>
{
    public Task Consume(ConsumeContext<OrderEvent> ctx)
    {
        log.LogInformation("Saw {Id} on {Stream}", ctx.Message.OrderId, ctx.Stream);
        return Task.CompletedTask;
    }
}

services.AddStreamBus<OrdersBus>((context, cfg) =>
{
    cfg.Host("localhost", "/orders", h => h.Credentials("app", "s3cret"));
    cfg.Publish<OrderEvent>("orders");
    cfg.ReceiveEndpoint("orders", e => e.Consumer<SharedConsumer>("orders-team"));
});

services.AddStreamBus<BillingBus>((context, cfg) =>
{
    cfg.Host("localhost", "/billing", h => h.Credentials("app", "s3cret"));
    cfg.Publish<OrderEvent>("invoices");
    cfg.ReceiveEndpoint("invoices", e => e.Consumer<SharedConsumer>("billing-team"));
});
```
