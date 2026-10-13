# Getting Started — Worker Service

> Goal: a running Worker that declares a stream, publishes JSON messages, and
> consumes them with a typed consumer. ~5 minutes.

## 1. Prerequisites

- .NET 10 SDK.
- RabbitMQ with the **stream plugin** reachable at `HOST:5552`
  (default `127.0.0.1:5552`). Stream port ≠ AMQP port (5672).
- A **non-`guest`** RabbitMQ user with permissions on your vhost.
  `guest` is rejected on non-loopback connections — StreamBus fails fast with
  `StreamBusConfigurationException` rather than a cryptic auth error
  (`src/StreamBus/CredentialValidation.cs`).

```bash
dotnet add package StreamBus --version <version>
# transitive: RabbitMQ.Stream.Client 1.12.1, Microsoft.Extensions.Hosting,
# Diagnostics.HealthChecks, Logging.Abstractions (Directory.Packages.props)
```

## 2. Minimal Worker (`Program.cs`)

```csharp
using StreamBus;

var builder = Host.CreateApplicationBuilder(args);

// 1. Bus: vhost "/" + credentials. Stream declared at startup.
builder.Services.AddStreamBus("/", o => o
    .Credentials("app", "s3cret")
    .Topology(t => t.DeclareStream("orders")));

// 2. Typed consumer: message type + consumer class + stream name.
builder.Services.AddStreamBusConsumer<OrderEvent, OrderConsumer>("orders");

var host = builder.Build();
await host.RunAsync();

// 3. Message + consumer live next to your domain code.
public sealed record OrderEvent(string OrderId, int Sequence);

public sealed class OrderConsumer(ILogger<OrderConsumer> log) : IConsumer<OrderEvent>
{
    public Task Consume(ConsumeContext<OrderEvent> ctx, CancellationToken ct)
    {
        log.LogInformation("Order {Id} seq {Seq} at {Stream} offset {Offset}",
            ctx.Message.OrderId, ctx.Message.Sequence, ctx.Stream, ctx.Offset);
        // Throw to retry (Strict = retry forever, in order). Return to ack + store offset.
        return Task.CompletedTask;
    }
}
```

What happens at startup (`src/StreamBus/Hosting.cs:402-561`):

1. The hosted service validates every typed consumer maps to a registered bus.
2. It opens one supervised connection per distinct vhost (retries forever on
   transient failures).
3. It declares `orders` idempotently (`EnsureStreamAsync`).
4. It starts `OrderConsumer` on `orders` with an auto-derived reference
   `streambus:<ConsumerFullName>:<Stream>`-ish (see [Consuming](consuming.md)).
5. By default startup is **non-blocking**: `RunAsync` returns while the above
   finishes in the background. Add
   `.Hosting(h => h.BlockStartupUntilReady(TimeSpan.FromSeconds(30)))` to block
   instead (recommended for Workers — fail fast on bad topology/credentials).

## 3. Publish from a `BackgroundService`

```csharp
public sealed class OrderPublisher(IStreamBus bus, IStreamSerializer serializer) : BackgroundService
{
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        // Wait until the connection is usable (or throw early with FailFast).
        var readiness = await bus.WaitUntilReadyAsync(
            ReadinessPolicy.UpTo(TimeSpan.FromSeconds(30)), stoppingToken);
        if (readiness is not BusReadiness.Ready)
            throw new InvalidOperationException($"Bus not ready: {readiness}");

        for (var i = 0; i < 10; i++)
        {
            var body = serializer.Serialize(new OrderEvent($"o-{i}", i));
            // StreamBus takes raw bytes; YOU serialize (JSON default, or custom).
            await bus.PublishAsync("orders", body, new PublishOptions(), stoppingToken);
            await Task.Delay(500, stoppingToken);
        }
    }
}
```

Register it: `builder.Services.AddHostedService<OrderPublisher>();`
Inject `IStreamSerializer`? It is not registered in DI — use
`SystemTextJsonStreamSerializer.Default` directly, or register your instance
as a singleton and use it in both publisher and bus options
(`o.Serializer(mySerializer)`). The typed consumer path uses the serializer
from **bus options**, so keep them in sync.

## 4. Configure from `appsettings.json`

StreamBus has no `IConfiguration` overload — bind settings yourself, then call
the builder. Full schema + POCO in [Configuration Reference](configuration-reference.md).
The short version:

```json
{
  "StreamBus": {
    "Vhost": "/",
    "UserName": "app",
    "Password": "s3cret",
    "Host": "127.0.0.1",
    "Port": 5552
  }
}
```

```csharp
var sb = builder.Configuration.GetSection("StreamBus").Get<StreamBusSettings>()
    ?? throw new InvalidOperationException("Missing StreamBus config.");

builder.Services.AddStreamBus(sb.Vhost, o => o
    .Credentials(sb.UserName, sb.Password)
    .Endpoint(sb.Host, sb.Port)
    .Topology(t => t.DeclareStream("orders")));
```

## 5. Run + verify

```bash
dotnet run
# expect: connection Ready, "orders" declared, publisher confirms x10,
# consumer logs Order o-0..o-9 in offset order.
```

Health: `AddStreamBus` registers a `"streambus"` `IHealthCheck`
(`Healthy` only when **all** buses are `Ready`). Wire it into your health
endpoint as usual with `builder.Services.AddHealthChecks()` already called
internally — just map it:

```csharp
var app = builder.Build(); // if web/worker hybrid
// app.MapHealthChecks("/healthz");
```

## 6. Where to go next

- [Configuration Reference](configuration-reference.md) — every knob + validation.
- [Publishing](publishing.md) — super streams, timeouts, errors.
- [Consuming](consuming.md) — options, offsets, retry, SAC.
- [Multiple Buses](multiple-buses.md) — only when you need >1 vhost/bus.
- [Operations](operations.md) — readiness, metrics, troubleshooting.

## Common first-run failures

| Symptom | Cause | Fix |
|---|---|---|
| `StreamBusConfigurationException`: `guest` | Using `guest` user remotely | Create app user, `Credentials("app", ...)` |
| `StreamBusStreamNotFoundException` on publish | Stream not declared | Add `o.Topology(t => t.DeclareStream(...))` or call `bus.Topology.EnsureStreamAsync(...)` |
| `StreamBusNotReadyException` (FailFast) | Publishing before connection ready | `await bus.WaitUntilReadyAsync(ReadinessPolicy.UpTo(...))` first, or set per-call `ReadinessPolicy` |
| Consumer gets `StreamBusDeserializationException` | Payload isn't `T` / null JSON | Publish with the same serializer/options the bus uses |
| Startup throws duplicate registration | `AddStreamBus` twice for default bus, or same consumer twice | One `AddStreamBus` per marker; one consumer registration per (marker, message, consumer, stream) |
