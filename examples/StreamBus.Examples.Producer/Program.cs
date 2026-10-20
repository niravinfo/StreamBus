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

// The library never reads configuration by itself: the user owns it.
// Connection values are read here (in the app) and handed to the
// configurator so every knob is validated in code at startup.
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

    // The stream spec lives on the registration that references the
    // stream — no manual DeclareStream/DeclareSuperStream anywhere.
    cfg.Publish<OrderEvent>("orders", s =>
    {
        s.MaxAge(TimeSpan.FromDays(7));
        s.MaxLengthBytes(500_000_000);
    });

    cfg.PublishSuper<PaymentEvent>("payments-super", s =>
    {
        s.Partitions(3);
    });
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
