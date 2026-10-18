using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using StreamBus;

// Consumer application.
// A different process from the producer: it declares no topology and adopts
// the broker reality instead — "orders" is consumed read-only as-is, and the
// "payments-super" partition count is read from the broker.
// Run the producer example first so both streams exist.

// Content root pinned to the app directory so appsettings.json is found no
// matter which directory `dotnet run` is invoked from.
var builder = Host.CreateApplicationBuilder(new HostApplicationBuilderSettings
{
    Args = args,
    ContentRootPath = AppContext.BaseDirectory,
});

// The library never reads configuration by itself: the user owns it.
// Connection values are read here (in the app) and handed to the configurator.
builder.Services.AddStreamBus((context, cfg) =>
{
    cfg.Host(
        builder.Configuration["StreamBus:Host"] ?? "localhost",
        builder.Configuration["StreamBus:Vhost"] ?? "/",
        h =>
        {
            h.Credentials(
                builder.Configuration["StreamBus:UserName"] ?? "guest",
                builder.Configuration["StreamBus:Password"] ?? "guest");
            h.StreamPort();
            h.Heartbeat(TimeSpan.FromSeconds(
                double.TryParse(builder.Configuration["StreamBus:HeartbeatSeconds"], out var seconds) ? seconds : 60));
        });

    // The group is the cursor identity, used verbatim. The same group
    // across instances scales out through one shared cursor; a different
    // group is an independent cursor that receives everything again.
    cfg.ReceiveEndpoint("orders", e =>
    {
        e.Consumer<OrderConsumer>("example-order-consumers");
    });

    cfg.ReceiveEndpoint("payments-super", e =>
    {
        e.Consumer<PaymentConsumer>("example-payment-consumers");

        // Expect a super stream without declaring partitions here: this
        // service reads all partitions, and the broker rules the count.
        e.SuperStream();
    });
});

builder.Services.AddStreamBusHosting(h => h.BlockStartupUntilReady(TimeSpan.FromSeconds(30)));

var app = builder.Build();
await app.RunAsync(); // Runs until Ctrl+C.

// Same message shape as the producer. In a real solution both applications
// would share one contracts project instead of duplicating the records.
public sealed record OrderEvent(string OrderId, int Sequence);

public sealed record PaymentEvent(string PaymentId, string AccountId, decimal Amount);

public sealed class OrderConsumer(ILogger<OrderConsumer> log) : IConsumer<OrderEvent>
{
    public Task Consume(ConsumeContext<OrderEvent> ctx, CancellationToken ct)
    {
        log.LogInformation("orders [{Partition}] offset {Offset}: {OrderId}",
            ctx.Partition, ctx.Offset, ctx.Message.OrderId);
        return Task.CompletedTask;
    }
}

public sealed class PaymentConsumer(ILogger<PaymentConsumer> log) : IConsumer<PaymentEvent>
{
    public Task Consume(ConsumeContext<PaymentEvent> ctx, CancellationToken ct)
    {
        log.LogInformation("payments-super [{Partition}] offset {Offset}: {PaymentId} key {AccountId}",
            ctx.Partition, ctx.Offset, ctx.Message.PaymentId, ctx.Message.AccountId);
        return Task.CompletedTask;
    }
}
