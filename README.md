<p align="center">
  <a href="https://github.com/niravinfo/StreamBus/actions/workflows/build.yml"><img src="https://github.com/niravinfo/StreamBus/actions/workflows/build.yml/badge.svg" alt="Build" /></a>
  <a href="https://www.nuget.org/packages/StreamBus"><img src="https://img.shields.io/nuget/vpre/StreamBus.svg" alt="NuGet" /></a>
  <a href="https://www.nuget.org/packages/StreamBus"><img src="https://img.shields.io/nuget/dt/StreamBus.svg" alt="NuGet downloads" /></a>
  <a href="https://github.com/niravinfo/StreamBus/blob/main/LICENSE"><img src="https://img.shields.io/github/license/niravinfo/StreamBus" alt="License: MIT" /></a>
  <a href="#"><img src="https://img.shields.io/badge/.NET-10-512BD4" alt=".NET 10" /></a>
</p>

# StreamBus

A modern .NET client for RabbitMQ Streams. Publish and consume typed messages, with retries and dead-lettering built in.

- **Consumer groups built in** — each endpoint has a first-class group; on a super stream, partitions spread across instances.
- **Retries and dead-lettering** — global or per-endpoint retry schedules; exhausted messages land in an error stream, a queue, or a default `{stream}_error` queue.
- **Code-only configuration** — every option is a validated configurator call; no settings binder, no JSON discovery.

## Install

```xml
<PackageReference Include="StreamBus" Version="0.4.0-beta.1" />
```

## Quick start

Publish objects, consume them with typed handlers:

```csharp
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using StreamBus;

var builder = Host.CreateApplicationBuilder(args);

builder.Services.AddStreamBus((_, cfg) =>
{
    cfg.Host("localhost", "/", h => h.Credentials("guest", "guest"));

    cfg.Publish<OrderPlaced>("orders");

    cfg.ReceiveEndpoint("orders", e =>
    {
        e.Consumer<OrderConsumer>("billing");
        e.UseRetry(r => r.Immediate(3));
    });
});

builder.Services.AddOptions<StreamBusHostOptions>().Configure(o =>
{
    o.WaitUntilStarted = true;
    o.StartTimeout = TimeSpan.FromSeconds(30);
});

await builder.Build().RunAsync();

public sealed record OrderPlaced(string OrderId);

public sealed class OrderConsumer(ILogger<OrderConsumer> log) : IConsumer<OrderPlaced>
{
    public Task Consume(ConsumeContext<OrderPlaced> context)
    {
        log.LogInformation("Order {OrderId} at offset {Offset}", context.Message.OrderId, context.Offset);
        return Task.CompletedTask;
    }
}
```

Need a local broker? `docker run -d --name rabbitmq -p 5552:5552 -p 5672:5672 -p 15672:15672 rabbitmq:4.2-management`

## Documentation

For more information, see the docs at **[niravinfo.github.io/StreamBus](https://niravinfo.github.io/StreamBus)**:

- [Quickstart](https://niravinfo.github.io/StreamBus/quickstart/) — a runnable publish-and-consume program
- [Introduction](https://niravinfo.github.io/StreamBus/introduction/) — what StreamBus is, and when to use it
- [Architecture](https://niravinfo.github.io/StreamBus/architecture/) — the RabbitMQ Streams model and how StreamBus is shaped by it
- [Configuration](https://niravinfo.github.io/StreamBus/configuration/) — hosts, credentials, ports, TLS, serializers
- [Publishers](https://niravinfo.github.io/StreamBus/publishers/) — publish .NET objects, super streams, raw messages, filters
- [Consumers](https://niravinfo.github.io/StreamBus/consumers/) — consumer groups, scale-out, offsets
- [Error handling](https://niravinfo.github.io/StreamBus/error-handling/) — retry schedules, exhaustion destinations, exceptions
- [Topology](https://niravinfo.github.io/StreamBus/topology/) — who declares streams, agreement rules, runtime API
- [Multiple buses](https://niravinfo.github.io/StreamBus/multiple-buses/) — marker buses, shared consumer classes
- [Lifecycle](https://niravinfo.github.io/StreamBus/lifecycle/) — startup modes, readiness, connection supervision, shutdown
- [Observability](https://niravinfo.github.io/StreamBus/observability/) — health data, metrics, logs
- [Transport security](https://niravinfo.github.io/StreamBus/transport-security/) — secure connections with TLS
- [Middleware](https://niravinfo.github.io/StreamBus/middleware/) — publish and consume filters
- [Releases](https://github.com/niravinfo/StreamBus/releases)

Runnable examples live in [`examples/`](examples/).

## Contributing

Issues and pull requests are welcome. The integration tests need only
Docker — they start their own RabbitMQ (4.2, via Testcontainers).

## License

[MIT](LICENSE)
