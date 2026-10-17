using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using StreamBus;

// Producer / topology owner:
// declares the plain stream "orders" and the 3-partition super stream
// "payments-super" at startup, then seeds a few messages into both.

// Content root pinned to the app directory so appsettings.json is found no
// matter which directory `dotnet run` is invoked from.
var builder = Host.CreateApplicationBuilder(new HostApplicationBuilderSettings
{
    Args = args,
    ContentRootPath = AppContext.BaseDirectory,
});

// The section overload binds the connection primitives from appsettings.json
// (docs/guide/config.md); the code lambda adds declarations and publish routes.
builder.Services.AddStreamBus(
    builder.Configuration.GetSection("StreamBus"),
    (context, cfg) =>
    {
        // Topology ownership: this service creates (or verifies) both streams
        // at startup. A conflicting pre-existing spec throws at startup.
        cfg.DeclareStream("orders", k => k
            .MaxAge(TimeSpan.FromDays(7))
            .MaxLengthBytes(500_000_000));

        cfg.DeclareSuperStream("payments-super", partitions: 3);

        // Typed routes: one message type -> one stream per bus.
        cfg.Publish<OrderEvent>("orders");
        cfg.PublishSuper<PaymentEvent>("payments-super");
    });

// Blocking startup waits for connections and topology declarations first.
builder.Services.AddStreamBusHosting(h => h.BlockStartupUntilReady(TimeSpan.FromSeconds(30)));
builder.Services.AddHostedService<SeedWorker>();

var app = builder.Build();
await app.RunAsync();

public sealed record OrderEvent(string OrderId, int Sequence);

public sealed record PaymentEvent(string PaymentId, string AccountId, decimal Amount);

public sealed class SeedWorker(
    IStreamBus bus,
    IHostApplicationLifetime lifetime,
    ILogger<SeedWorker> log) : BackgroundService
{
    protected override async Task ExecuteAsync(CancellationToken ct)
    {
        await bus.WaitUntilReadyAsync(ReadinessPolicy.UpTo(TimeSpan.FromSeconds(30)), ct);

        // Plain stream: PublishAsync resolves the stream from the registration.
        for (var i = 0; i < 5; i++)
        {
            var order = new OrderEvent($"order-{i}", i);
            await bus.PublishAsync(order, ct);
            log.LogInformation("orders <- {OrderId}", order.OrderId);
        }

        // Super stream: the routing key selects the partition, so the same key
        // always lands on the same partition (per-key ordering).
        foreach (var (accountId, count) in new[] { ("acct-1", 3), ("acct-2", 2) })
        {
            for (var i = 0; i < count; i++)
            {
                var payment = new PaymentEvent($"pay-{accountId}-{i}", accountId, (i + 1) * 10m);
                await bus.PublishSuperAsync(payment, accountId, ct);
                log.LogInformation("payments-super <- {PaymentId} (key {Key})", payment.PaymentId, accountId);
            }
        }

        log.LogInformation("Seed complete; stopping the host.");
        lifetime.StopApplication();
    }
}
