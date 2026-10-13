# StreamBus User Guide

> Detailed, example-driven documentation for the StreamBus library
> (RabbitMQ Streams for .NET 10). Written for **humans and AI assistants** alike:
> every page states exact APIs, defaults, and copy-paste Worker Service examples.

## Who this is for

You run a .NET **Worker Service** (or any `IHost`), configure from
**`appsettings.json`**, and want to:

- publish messages to RabbitMQ Streams,
- consume them with typed `IConsumer<T>` classes resolved from DI,
- run **one or many buses** (one per vhost) in the same process.

## Important: how configuration works

StreamBus is **code-first**. There is **no** `AddStreamBus(IConfiguration)` overload.
You read `appsettings.json` yourself (with `IConfiguration` / options POCOs) and
pass the values into the `AddStreamBus(vhost, options => ...)` builder lambda.
The [Configuration Reference](configuration-reference.md) gives you a complete
`appsettings.json` schema plus the 30-line binding snippet you copy into
`Program.cs`.

## Map

| Start here | What you get |
|---|---|
| [Getting Started — Worker Service](getting-started-worker.md) | 5-minute end-to-end: install, `Program.cs`, publish + consume, run |
| [Configuration Reference](configuration-reference.md) | **Every setting, default, and validation rule.** `appsettings.json` schema, env vars, all builder methods |
| [Publishing](publishing.md) | `PublishAsync`, super streams, timeouts, serialization, what each exception means |
| [Consuming](consuming.md) | Typed `IConsumer<T>` (preferred) + raw `ConsumeAsync`, `ConsumeContext`, `ConsumeOptions` table |
| [Multiple Buses](multiple-buses.md) | Marker types, `IStreamBus<TBus>`, resolving buses/consumers, sharing rules, per-bus config |
| [Offsets, Retry & Errors](offsets-retry-errors.md) | `ConsumeOffset` × 5, references, SAC, retry modes, error stream + replay |
| [Effectively-Once & Topology](effectively-once-topology.md) | Read-process-write pipelines, `PublishResultAsync`, stream / super-stream declaration |
| [Health, Readiness & Observability](operations.md) | Startup blocking, health check, connection states, metrics, logging |

Machine-readable entry point for AI assistants: [`llms.txt`](../llms.txt).

## Conventions used in this guide

- `src/StreamBus/<File>.cs:<line>` pins every behavior to source.
- `bus` = `IStreamBus` (default bus) or `IStreamBus<TBus>` (named bus).
- `stream` = plain RabbitMQ stream; `super stream` = partitioned super stream.
- `reference` = broker consumer-group name (offset cursor). If you omit it,
  StreamBus derives a deterministic one (see [Consuming](consuming.md)).
- All snippets target `net10.0` with `ImplicitUsings` + `Nullable` enabled.
- Broker in examples: RabbitMQ with stream plugin on `localhost:5552`.
  **Never use the `guest` user** — create a dedicated non-guest user
  (see [Configuration Reference](configuration-reference.md)).

## Library at a glance

```csharp
// Program.cs (Worker Service) — the whole shape:
services.AddStreamBus("/", o => o
    .Credentials(user, password)          // required
    .Endpoint(host, port)                 // default 127.0.0.1:5552
    .Topology(t => t.DeclareStream("orders"))
    .Hosting(h => h.BlockStartupUntilReady(TimeSpan.FromSeconds(30))));

services.AddStreamBusConsumer<OrderEvent, OrderConsumer>("orders");

// publish anywhere IStreamBus is injected:
await bus.PublishAsync("orders", serializer.Serialize(order));

// consume with a DI-resolved class:
sealed class OrderConsumer : IConsumer<OrderEvent>
{
    public Task Consume(ConsumeContext<OrderEvent> ctx, CancellationToken ct) { ... }
}
```

Library: single `src/StreamBus` project, `net10.0`,
`RabbitMQ.Stream.Client 1.12.1` (see `src/StreamBus/StreamBus.csproj`,
`Directory.Packages.props`). Hosting is built in — `AddStreamBus` registers an
`IHostedService`, an `IStreamBusRegistry`, and a `"streambus"` health check.
