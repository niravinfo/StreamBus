# StreamBus

[![Build](https://github.com/niravinfo/StreamBus/actions/workflows/build.yml/badge.svg)](https://github.com/niravinfo/StreamBus/actions/workflows/build.yml)
[![NuGet](https://img.shields.io/nuget/v/StreamBus.svg)](https://www.nuget.org/packages/StreamBus)

MassTransit-style client for RabbitMQ streams on .NET 10. Workers register
buses with nested `AddStreamBus((context, cfg) => ...)` blocks, publish typed
messages without manual serialization, and consume through `ReceiveEndpoint`s
with first-class consumer-group identity. Global retry with per-endpoint
override hands exhausted messages to per-stream classic `{stream}_error`
queues by default.

**Configuration is explicit and fluent.** StreamBus does not read host
configuration, bind JSON settings, or discover connection settings. Every
option is a configurator method call in code, validated once at registration.

## Prerequisites

- .NET 10 SDK.
- RabbitMQ 4.x with streams enabled: stream port `5552`, AMQP `5672`
  (AMQP is only used when a queue dead-letter destination exists).
- Empty credentials are rejected. `guest`/`guest` is accepted for a local
  broker; remote deployments need a dedicated non-`guest` user.

## Install

```xml
<PackageReference Include="StreamBus" Version="0.2.0" />
```

## The 90% case

```csharp
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using StreamBus;

var builder = Host.CreateApplicationBuilder(args);

builder.Services.AddStreamBus((_, cfg) =>
{
    cfg.Host("localhost", "/", h =>
    {
        h.Credentials("guest", "guest");
        h.StreamPort(5552);
        h.AmqpPort(5672);
        h.Heartbeat(TimeSpan.FromSeconds(60));
        h.MonitorInterval(TimeSpan.FromSeconds(5));
    });

    cfg.Publish<OrderPlaced>("orders");

    cfg.ReceiveEndpoint("orders", e =>
    {
        e.Consumer<OrderConsumer>("billing");
        e.UseMessageRetry(r => r.Immediate(3));
    });
});

builder.Services.AddStreamBusHosting(h => h.BlockStartupUntilReady(TimeSpan.FromSeconds(30)));

await builder.Build().RunAsync();

public sealed record OrderPlaced(string OrderId);

public sealed class OrderConsumer(ILogger<OrderConsumer> log) : IConsumer<OrderPlaced>
{
    public Task Consume(ConsumeContext<OrderPlaced> context, CancellationToken cancellationToken)
    {
        log.LogInformation("Order {OrderId} at offset {Offset}", context.Message.OrderId, context.Offset);
        return Task.CompletedTask;
    }
}
```

## Features

Every capability links to its guide section:

- Buses: default + marker buses, explicit fluent configuration (`guide/config.md`)
- Connection: host/credentials/ports/heartbeat/monitor/pools (`guide/config.md`)
- Serialization: JSON options per bus (`guide/config.md`)
- Declarations: streams/super-streams + retention/placement knobs (`guide/topology.md`)
- Startup: blocking/non-blocking, readiness waits, health checks (`guide/hosting.md`)
- Typed publish: route table, keyed super publish, overrides, raw bytes (`guide/publish.md`)
- Endpoints: required groups, scale-out, SAC/offset/store knobs (`guide/consume.md`)
- Raw consume + manual offset get/set (`guide/consume.md`)
- Effectively-once pipelines + manual result publish (`guide/effectively-once.md`)
- Retry: global + per-endpoint schedules, escape hatch (`guide/errors.md`)
- Exhaustion: stream / classic+quorum queues (derived `{stream}_error` default) / owning callback (`guide/errors.md`)
- Replay (streams) and requeue (queues) (`guide/errors.md`)
- Topology derivation + broker adopt rule + conflicts (`guide/topology.md`)
- Multi-bus markers + shared consumer classes (`guide/multi-bus.md`)
- Message properties: message/correlation IDs and typed headers (`guide/publish.md`)
- Middleware: ordered publish/consume filters, explicit consume skip, telemetry (`guide/publish.md`, `guide/consume.md`)
- Observability: per-registration health, metrics, log events (`guide/observability.md`)

## Concepts

- **Bus registration** (`guide/config.md`): one `AddStreamBus` per bus,
  `AddStreamBus<TBus>` per extra bus. `Host` exactly once; streams declared
  up front; startup blocking is service-level via `AddStreamBusHosting`.
- **Explicit configuration** (`guide/config.md`): no configuration binder.
  Read environment variables or a vault in your own code if you want to, then
  pass the values to `Host`, `Publish`, `ReceiveEndpoint`, and the other
  configurator methods.
- **Typed publish** (`guide/publish.md`): `Publish<T>` for plain streams,
  `PublishSuper<T>` for super streams; the routing key travels with the
  `PublishSuperAsync` call (forgetting it is a compile error).
- **Endpoints** (`guide/consume.md`): the consumer group is required and
  verbatim - same group scales out, different groups are independent.
- **Multi-bus** (`guide/multi-bus.md`): marker types disambiguate buses;
  a shared consumer class needs a different group per bus.
- **Retry and errors** (`guide/errors.md`): global schedule, per-endpoint
  override, exhaustion to a stream, a classic/quorum queue (default
  `{stream}_error`), or an owning `OnExhausted` callback.

## Docs map

- `docs/guide/config.md` - connection, declarations, serializers, full option reference.
- `docs/guide/hosting.md` - startup blocking, readiness, health checks.
- `docs/guide/publish.md` - typed publish, super streams, escape hatches.
- `docs/guide/consume.md` - endpoints, groups, raw consume, offsets.
- `docs/guide/effectively-once.md` - read-process-write pipelines.
- `docs/guide/multi-bus.md` - marker buses and shared consumers.
- `docs/guide/errors.md` - retry layering, dead letters, requeue, exception table.
- `docs/guide/topology.md` - declarations, derivation, adopt rule.
- `docs/guide/observability.md` - health data, metrics, log events.
- `docs/llms.txt` - ground truths for automated consumers.
- `CHANGELOG.md` - 0.2.0 breaking changes and fixes.
