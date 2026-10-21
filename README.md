# StreamBus

[![Build](https://github.com/niravinfo/StreamBus/actions/workflows/build.yml/badge.svg)](https://github.com/niravinfo/StreamBus/actions/workflows/build.yml)
[![NuGet](https://img.shields.io/nuget/v/StreamBus.svg)](https://www.nuget.org/packages/StreamBus)

MassTransit-style client for RabbitMQ streams on .NET 10. Workers declare
buses with nested `AddStreamBus((context, cfg) => ...)` blocks, publish
typed messages without manual serialization, and consume through
`ReceiveEndpoint`s with first-class consumer-group identity. Global retry
with per-endpoint override hands exhausted messages to per-stream classic
`{stream}_error` queues by default.

## Prerequisites

- .NET 10 SDK.
- RabbitMQ 4.x with streams enabled: stream port `5552`, AMQP `5672`
  (AMQP is only used when a queue dead-letter destination exists).
- Default credentials `guest`/`guest` over `localhost` work against a
  local broker; remote deployments need a dedicated non-`guest` user
  (`STREAMBUS_USER` / `STREAMBUS_PASSWORD`).

## Install

```xml
<ProjectReference Include="path/to/src/StreamBus/StreamBus.csproj" />
```

## The 90% case

`appsettings.json`:

```json
{
  "StreamBus": {
    "Vhost": "/",
    "UserName": "guest",
    "Password": "guest",
    "Host": "localhost",
    "StreamPort": 5552,
    "AmqpPort": 5672,
    "HeartbeatSeconds": 60
  }
}
```

`Program.cs`:

```csharp
using StreamBus;

var builder = Host.CreateApplicationBuilder(args);

builder.Services.AddStreamBus(
    builder.Configuration.GetSection("StreamBus"),
    (context, cfg) =>
    {
        cfg.Publish<OrderEvent>("orders");
        cfg.ReceiveEndpoint("orders", e =>
        {
            e.Consumer<OrderConsumer>("order-processors");
        });
    });

builder.Services.AddStreamBusHosting(h => h.BlockStartupUntilReady(TimeSpan.FromSeconds(30)));
builder.Services.AddHostedService<CheckoutWorker>();

await builder.Build().RunAsync();

public sealed record OrderEvent(string OrderId, int Sequence);

public sealed class OrderConsumer(ILogger<OrderConsumer> log) : IConsumer<OrderEvent>
{
    public Task Consume(ConsumeContext<OrderEvent> ctx, CancellationToken ct)
    {
        log.LogInformation("Order {Id} at offset {Offset}", ctx.Message.OrderId, ctx.Offset);
        return Task.CompletedTask;
    }
}

public sealed class CheckoutWorker(IStreamBus bus) : BackgroundService
{
    protected override async Task ExecuteAsync(CancellationToken ct)
    {
        await bus.WaitUntilReadyAsync(ReadinessPolicy.UpTo(TimeSpan.FromSeconds(30)), ct);
        await bus.PublishAsync(new OrderEvent("o-1", 1), ct);
    }
}
```

## Features

Every capability links to its guide section:

- Buses: default + marker + `IConfigurationSection` registration (`guide/config.md`)
- Connection: host/credentials/ports/heartbeat/pools (`guide/config.md`)
- Serialization: JSON options per bus (`guide/config.md`)
- Declarations: streams/super-streams + retention/placement knobs (`guide/topology.md`)
- Startup: block/timeout/non-blocking, readiness waits, health checks (`guide/hosting.md`)
- Typed publish: route table, keyed super publish, overrides, raw bytes (`guide/publish.md`)
- Endpoints: required groups, scale-out, SAC/offset/store knobs (`guide/consume.md`)
- Raw consume + manual offset get/set (`guide/consume.md`)
- Effectively-once pipelines + manual result publish (`guide/effectively-once.md`)
- Retry: global + per-endpoint schedules, escape hatch (`guide/errors.md`)
- Exhaustion: stream / classic+quorum queues (derived `{stream}_error` default) / owning callback (`guide/errors.md`)
- Replay (streams) and requeue (queues) (`guide/errors.md`)
- Topology derivation + broker adopt rule + conflicts (`guide/topology.md`)
- Multi-bus markers + shared consumer classes (`guide/multi-bus.md`)
- Observability: health data, metrics, log events (`guide/observability.md`)

## Concepts

- **Bus registration** (`guide/config.md`): one `AddStreamBus` per bus,
  `AddStreamBus<TBus>` per extra bus. `Host` exactly once; streams declared
  up front; startup blocking is service-level via `AddStreamBusHosting`.
- **Typed publish** (`guide/publish.md`): `Publish<T>` for plain streams,
  `PublishSuper<T>` for super streams; the routing key travels with the
  `PublishSuperAsync` call (forgetting it is a compile error).
- **Endpoints** (`guide/consume.md`): the consumer group is required and
  verbatim — same group scales out, different groups are independent.
- **Multi-bus** (`guide/multi-bus.md`): marker types disambiguate buses;
  a shared consumer class needs a different group per bus.
- **Retry and errors** (`guide/errors.md`): global schedule, per-endpoint
  override, exhaustion to a stream, a classic/quorum queue (default
  `{stream}_error`), or an owning `OnExhausted` callback.

## Docs map

- `docs/guide/config.md` — connection, declarations, JSON binding, full key reference.
- `docs/guide/hosting.md` — startup blocking, readiness, health checks.
- `docs/guide/publish.md` — typed publish, super streams, escape hatches.
- `docs/guide/consume.md` — endpoints, groups, raw consume, offsets.
- `docs/guide/effectively-once.md` — read-process-write pipelines.
- `docs/guide/multi-bus.md` — marker buses and shared consumers.
- `docs/guide/errors.md` — retry layering, dead letters, requeue.
- `docs/guide/topology.md` — declarations, derivation, adopt rule.
- `docs/guide/observability.md` — health data, metrics, log events.
- `docs/llms.txt` — ground truths for automated consumers.
