[![Build](https://github.com/niravinfo/StreamBus/actions/workflows/build.yml/badge.svg)](https://github.com/niravinfo/StreamBus/actions/workflows/build.yml)
[![NuGet](https://img.shields.io/nuget/vpre/StreamBus.svg)](https://www.nuget.org/packages/StreamBus)
[![NuGet downloads](https://img.shields.io/nuget/dt/StreamBus.svg)](https://www.nuget.org/packages/StreamBus)
[![License: MIT](https://img.shields.io/github/license/niravinfo/StreamBus.svg)](https://github.com/niravinfo/StreamBus/blob/main/LICENSE)
[![.NET 10](https://img.shields.io/badge/.NET-10-512BD4)](https://github.com/niravinfo/StreamBus)
[![.NET 11](https://img.shields.io/badge/.NET-11-512BD4)](https://github.com/niravinfo/StreamBus)

# StreamBus

A modern .NET client for RabbitMQ Streams. Publish and consume typed messages, with retries and dead-lettering built in.

- Every receive endpoint declares its consumer group, so a super stream's partitions spread across instances.
- Retries and dead-lettering are configuration: global or per-endpoint retry schedules, with exhausted messages going to an error stream or a queue.
- Configuration is code only. Every option is a validated configurator call, with no settings binder and no JSON discovery.

## Install

```shell
dotnet add package StreamBus
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

## Documentation

For more information, see the docs at **[niravinfo.github.io/StreamBus](https://niravinfo.github.io/StreamBus)**:

- [Concepts: introduction](https://niravinfo.github.io/StreamBus/concepts/introduction/) - what StreamBus is, and when to use it
- [Get started guide](https://niravinfo.github.io/StreamBus/guides/get-started/) - a runnable publish-and-consume program
- [Streams & messages](https://niravinfo.github.io/StreamBus/concepts/streams/) - the RabbitMQ Streams model and how StreamBus is shaped by it
- [Host & connections](https://niravinfo.github.io/StreamBus/configuration/host-and-connections/) - hosts, credentials, ports, TLS
- [Publish an event](https://niravinfo.github.io/StreamBus/guides/publish-an-event/) - publish .NET objects, super streams, routing keys
- [Consume events](https://niravinfo.github.io/StreamBus/guides/consume-events/) - consumer groups, scale-out, offsets
- [Handle failures](https://niravinfo.github.io/StreamBus/guides/handle-failures/) - retry schedules, exhaustion destinations, exceptions
- [Partition a stream](https://niravinfo.github.io/StreamBus/guides/partition-a-stream/) - super-stream partitioning
- [Use multiple buses](https://niravinfo.github.io/StreamBus/guides/use-multiple-buses/) - marker buses, shared consumer classes
- [Add cross-cutting logic](https://niravinfo.github.io/StreamBus/guides/add-cross-cutting-logic/) - publish and consume filters
- [Endpoint options](https://niravinfo.github.io/StreamBus/configuration/endpoint-options/) - every per-endpoint knob
- [Retry & dead letters](https://niravinfo.github.io/StreamBus/configuration/retry-and-dead-letters/) - retry and dead-letter configuration
- [Observability](https://niravinfo.github.io/StreamBus/configuration/observability/) - health data, metrics, logs
- [Transport security](https://niravinfo.github.io/StreamBus/configuration/transport-security/) - secure connections with TLS
- [Releases](https://github.com/niravinfo/StreamBus/releases)

Runnable examples live in the [examples directory](https://github.com/niravinfo/StreamBus/tree/main/examples).

## Contributing

Issues and pull requests are welcome. The integration tests run against
RabbitMQ 4.x with the stream plugins enabled.

## License

[MIT](https://github.com/niravinfo/StreamBus/blob/main/LICENSE)
