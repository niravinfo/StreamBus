# StreamBus v2 User-Centric API Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Redesign the StreamBus public API in MassTransit style so Workers configure buses with nested `AddStreamBus((context, cfg) => ...)` blocks, declare `ReceiveEndpoint`s with first-class consumer-group identity, publish typed messages without manual serialization, inherit global retry with per-endpoint override, and hand exhausted messages to per-stream classic `{stream}_error` queues — with breaking changes allowed.

**Architecture:** Keep the stream runtime (supervisor, registries, dispatch, retry, topology) untouched; replace the registration surface with configurator interfaces (`cfg.Host`, `cfg.Publish`, `cfg.ReceiveEndpoint`, `cfg.UseMessageRetry`), add a message-type route table for typed publish, move startup blocking to a service-level store, add a hand-rolled `IConfigurationSection` binder, derive startup topology from endpoint descriptors, and add a small AMQP transport (`RabbitMQ.Client`) used only for error queues and requeue.

**Tech Stack:** .NET 10, `RabbitMQ.Stream.Client 1.12.1` (streams), `RabbitMQ.Client` (new, error queues only), `Microsoft.Extensions.Hosting / Diagnostics.HealthChecks / Logging.Abstractions`.

**Spec:** This document, Part A (User Experience) is the spec — it shows exactly how users will use the library. Part B implements it. Executors read both.

## Global Constraints

- `TargetFramework=net10.0`, `Nullable=enable`, `ImplicitUsings=enable`, `TreatWarningsAsErrors=true`, `LangVersion=latest` (`Directory.Build.props`).
- Central package management only (`Directory.Packages.props`); no `Version=` on `PackageReference`; forward slashes in paths.
- No `.ConfigureAwait(false)` inside `[Fact]`s; use explicit `TestContext.Current.CancellationToken` in tests; `Assert.Single(values, pred)` overload where applicable.
- Integration tests need broker `localhost:5552` (streams) + `localhost:5672` (AMQP) with non-guest user/password from `STREAMBUS_USER`/`STREAMBUS_PASSWORD`; never use `guest`.
- Breaking changes allowed (pre-publish library): remove replaced APIs outright, no `[Obsolete]` shims.
- Every commit step below means: show changed files + exact command, wait for explicit human yes before running.

## Review Focus

- Same message type registered to two streams on one bus must fail at startup with a config error, not publish to a random one (Task 2).
- Super-stream typed publish without `RouteBy` must fail at registration, not misroute at runtime (Task 2).
- Manually overriding an undeclared stream at the call site must still surface `StreamBusStreamNotFoundException`, never auto-create (Task 2).
- Two services sharing one consumer class from a common library must be able to get different references; the derived default plus explicit group names must make same-vs-different service a conscious choice (Task 3).
- Exhausted messages must land in the per-stream `{stream}_error` classic queue with the full envelope in headers, and requeue must republish them to the source stream at-least-once (never silently drop, never duplicate-hide) (Task 5).

---

# Part A — User Experience (the spec, pseudocode first)

## A.1 Single-bus Worker (the 90% case)

`appsettings.json`:

```json
{
  "StreamBus": {
    "Vhost": "/",
    "UserName": "app",
    "Password": "s3cret",
    "Host": "127.0.0.1",
    "StreamPort": 5552,
    "AmqpPort": 5672,
    "Connection": { "ProducersPerConnection": 1, "ConsumersPerConnection": 1 },
    "Topology": {
      "Streams": [{ "Name": "audit", "MaxAgeDays": 30 }],
      "SuperStreams": [{ "Name": "orders-super", "Partitions": 3 }]
    }
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
        log.LogInformation("Order {Id} seq {Seq} at offset {Offset}", ctx.Message.OrderId, ctx.Message.Sequence, ctx.Offset);
        return Task.CompletedTask;
    }
}

public sealed class CheckoutWorker(IStreamBus bus) : BackgroundService
{
    protected override async Task ExecuteAsync(CancellationToken ct)
    {
        await bus.WaitUntilReadyAsync(ReadinessPolicy.UpTo(TimeSpan.FromSeconds(30)), ct);
        await bus.PublishAsync(new OrderEvent("o-1", 1), ct); // stream + serializer from registration
    }
}
```

What the user gets: no manual serialization, no per-call stream names, streams declared before first use, startup blocked globally until ready, failures handed to the `orders_error` classic queue by default.

## A.2 Code-first registration (no configuration section)

```csharp
services.AddStreamBus((context, cfg) =>
{
    cfg.Host("rabbitmq.internal", "/", h => h.Credentials("app", "s3cret"));

    cfg.Serializer(json => json.PropertyNamingPolicy = JsonNamingPolicy.CamelCase);
    cfg.DeclareStream("audit", k => k.MaxAge(TimeSpan.FromDays(30)));

    cfg.Publish<OrderEvent>("orders");
    cfg.ReceiveEndpoint("orders", e => e.Consumer<OrderConsumer>("order-processors"));
});
```

## A.3 Multiple buses (MassTransit multi-bus pattern)

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

public sealed class CheckoutWorker(IStreamBus<OrdersBus> orders, IStreamBus<BillingBus> billing) : BackgroundService
{
    protected override async Task ExecuteAsync(CancellationToken ct)
    {
        await orders.PublishAsync(new OrderEvent("o-1", 1), ct);
    }
}
```

## A.4 Super streams with routing (routing key always required)

```csharp
services.AddStreamBus((context, cfg) =>
{
    cfg.Host("localhost", "/", h => h.Credentials("app", "s3cret"));
    cfg.Publish<OrderEvent>("orders-super", p => p.RouteBy(e => e.OrderId)); // REQUIRED: broker hashes it
    cfg.ReceiveEndpoint("orders-super", e =>
    {
        e.Consumer<SuperOrderConsumer>("super-processors");
        e.SetSuperStream(partitions: 3); // declaration lives here → auto-declare works
    });
});

await bus.PublishAsync(new OrderEvent("o-1", 1), ct); // partitioned by OrderId
```

The consumer never routes — it reads all partitions. Producer picks the key, endpoint declares the partitions. The publish call takes no super/plain flag: the bus resolves super-ness from the declaration set at startup and fails fast (Task 6 table) if the key is missing or misplaced.

## A.5 Retry: global default, per-endpoint override, per-stream error queues

```csharp
services.AddStreamBus((context, cfg) =>
{
    cfg.Host("localhost", "/", h => h.Credentials("app", "s3cret"));

    // Global: every endpoint inherits this schedule unless overridden.
    cfg.UseMessageRetry(r => r.Intervals(
        TimeSpan.FromMilliseconds(200),
        TimeSpan.FromSeconds(2),
        TimeSpan.FromSeconds(10)));

    cfg.Publish<OrderEvent>("orders");

    cfg.ReceiveEndpoint("orders", e =>
    {
        e.Consumer<OrderConsumer>("order-processors");
        // default error destination: classic queue "orders_error" (derived, durable).
    });

    cfg.ReceiveEndpoint("audit", e =>
    {
        e.Consumer<AuditConsumer>("audit-log");
        e.UseMessageRetry(r => r.Immediate(5));                    // override: this endpoint only
        e.SetErrorQueue("audit_dlq", ErrorQueueType.Quorum);       // override: name + type
    });

    cfg.ReceiveEndpoint("metrics", e =>
    {
        e.Consumer<MetricsConsumer>("metrics-tail");
        e.UseMessageRetry(r => r.None());                          // CONFIG ERROR: nowhere to hand off
    });
});
```

Derivation rules (locked, tested in Task 4):

| Retry | Error queue | Effective mode |
|---|---|---|
| bounded schedule (`Immediate`/`Interval`/`Intervals`/`Exponential`) | derived/default or explicit | `SkipAfterDeadline`: hand off after the schedule, then advance |
| bounded schedule | none configured and derivation disabled | `Strict`: schedule runs, then repeat last delay forever (stall, order preserved) |
| unbounded (default / `UseRetry` with `MaxAttempts == null`) | none | `Strict`: today's default, retry forever |
| unbounded | any error queue set | config error at startup (handoff needs a bound) |
| `None()` | any error queue set | immediate handoff on first failure |
| `None()` | none | config error at startup (failure would have nowhere to go) |

Default error queue: name `{stream}_error`, type classic, durable. Explicit `SetErrorQueue(name?, type?)` overrides either part.

## A.6 Failure handling and requeue (queue-backed)

```csharp
// After deploying the fix, move error messages back into the source stream:
int requeued = await bus.RequeueErrorsAsync("orders", ct);
//Uses "orders_error" unless the endpoint overrode the name:
//  int requeued = await bus.RequeueErrorsAsync("audit", errorQueue: "audit_dlq", ct);
```

Flow: handler exhausts schedule → envelope (same fields as today's `ErrorEnvelope`: source stream/partition/offset, body, exception, attempts, ids, routing) published persistent to the classic/quorum queue with `streambus.error.*` headers → source offset advanced exactly once → `OnRetryExhausted` hook observed. Requeue `BasicGet`-loops the queue and republishes each body to the source stream (tail append, at-least-once), acking only after the stream confirm; malformed entries are acked with a warning and counted as skipped (documented, loud). Queues are declared durable at startup alongside streams.

## A.7 Overrides and escape hatches (rare, still supported)

```csharp
await bus.PublishAsync("orders-manual", new OrderEvent("o-1", 1), ct); // typed, explicit stream
await bus.PublishRawAsync("orders", rawBytes, new PublishOptions(), ct); // raw bytes (renamed)
await using var sub = await bus.ConsumeAsync("audit", new ConsumeOptions { Reference = "ops" }, handler, ct); // raw consume, unchanged
```

## A.8 Effectively-once (unchanged runtime, unambiguous bus by construction)

```csharp
await using var pipe = await bus.ConsumeEffectivelyOnceAsync("orders", "invoices",
    new EffectivelyOnceOptions(), async (ctx, ct) => resultBytes, ct);
```

No API change; only the bus it runs on is now unambiguous by construction.

---

# Part B — Build tasks

### Task 1: Configurator shell (`AddStreamBus((context, cfg))`, `Host`, declare, serializer)

**Files:**
- Create: `src/StreamBus/Configuration/StreamBusConfigurator.cs` (interfaces `IStreamBusConfigurator`, `IHostConfigurator`, `IStreamRegistrationContext`; implementations; `AddStreamBus` extensions incl. marker + section overloads)
- Modify: `src/StreamBus/Hosting.cs` (remove old `AddStreamBus(vhost, Action<StreamBusOptionsBuilder>)` overloads + `StreamBusOptionsBuilder`; keep hosted service, health check, stores until later tasks consume them; rewire `AddShared` to the configurator path)
- Modify: `src/StreamBus/Options.cs` (host fields gain `StreamPort`/`AmqpPort`; drop nothing else yet)
- Test: `tests/StreamBus.UnitTests/ConfiguratorTests.cs`

**Interfaces:**
- Consumes: `StreamBusConfigStore` (unchanged duplicate/conflict rules), `IStreamSerializer`.
- Produces: `services.AddStreamBus(Action<IStreamRegistrationContext, IStreamBusConfigurator> configure)`, `services.AddStreamBus<TBus>(...)`, `services.AddStreamBus(IConfigurationSection section, Action<IStreamRegistrationContext, IStreamBusConfigurator>? configure = null)` (+ marker overload; full binding lands in Task 8, this task only reserves the shape and binds `Vhost`). `IStreamBusConfigurator.Host(string host, string vhost, Action<IHostConfigurator> configure)`, `.Serializer(...)` (both overloads, as today), `.DeclareStream(name, knobs?)`, `.DeclareSuperStream(name, partitions, knobs?)`. `IHostConfigurator.Credentials(string user, string password)`, `.StreamPort(int = 5552)`, `.AmqpPort(int = 5672)`, `.ProducersPerConnection(int = 1)`, `.ConsumersPerConnection(int = 1)`. `IStreamRegistrationContext.Services` (`IServiceCollection`; provider-phase callbacks documented as future). `Publish`/`ReceiveEndpoint`/`UseMessageRetry` members are added by Tasks 2–4 on the same implementation class (partial or internal setters — Task 1 leaves `NotSupported`-free stubs throwing `StreamBusConfigurationException("not yet implemented")`? No placeholders allowed: instead Task 1 ships Host/declare/serializer only, and Tasks 2–4 extend the interface + implementation).

- [ ] **Step 1: Implement.** New `Configuration` folder; interfaces plus internal `StreamBusConfigurator : IStreamBusConfigurator` accumulating `StreamBusOptions` (extended with ports) + explicit declarations. `Host(...)` required exactly once per bus (second call throws `StreamBusConfigurationException`); missing `Host` at `Build()` throws naming the bus. Credentials validated as today (`guest` forbidden). `AddStreamBus` builds options, calls existing `AddShared` path, registers `IStreamBus`/`IStreamBus<TBus>` as today. Delete `StreamBusOptionsBuilder` and old overloads; fix all in-repo callers in this task.
- [ ] **Step 2: Verify.** `ConfiguratorTests`: missing `Host` throws; duplicate `Host` throws; `guest` rejected; defaults (`127.0.0.1`? No — host has no default now; `Host()` is required. Ports/pools keep defaults 5552/5672/1/1); marker + default coexist. Run `dotnet test tests/StreamBus.UnitTests --filter Configurator`. Full `dotnet build StreamBus.slnx` green.
- [ ] **Step 3: Commit (with human approval)**

```bash
git add src/StreamBus/Configuration/StreamBusConfigurator.cs src/StreamBus/Hosting.cs src/StreamBus/Options.cs tests/StreamBus.UnitTests/ConfiguratorTests.cs
git commit -m "Add MassTransit-style configurator shell with Host setup"
```

### Task 2: Typed publish (`cfg.Publish`, route table, `PublishAsync<T>`)

**Files:**
- Modify: `src/StreamBus/Configuration/StreamBusConfigurator.cs` (add `Publish<T>` + `PublishRegistrationBuilder<T>` with `RouteBy` only — no super flag; super-ness resolves from declarations in Task 6)
- Create: `src/StreamBus/PublishRouting.cs` (`PublishRegistration` record, `StreamBusPublishStore`, `StreamPublishRouter`)
- Modify: `src/StreamBus/Bus.cs` (add `PublishAsync<T>(T, ...)` + `PublishAsync<T>(string, T, ...)`; rename raw to `PublishRawAsync`)
- Modify: `src/StreamBus/Registry.cs` (pass serializer + router into bus constructors)
- Test: `tests/StreamBus.UnitTests/PublishRoutingTests.cs`

**Interfaces:**
- Consumes: `IStreamSerializer`, existing `StreamProducerRegistry` raw path, Task 1 configurator.
- Produces: `IStreamBusConfigurator.Publish<TMessage>(string stream, Action<PublishRegistrationBuilder<TMessage>>? configure = null)`, `IStreamBus.PublishAsync<T>(T message, PublishOptions? options = null, CancellationToken ct = default)`, `IStreamBus.PublishAsync<T>(string stream, T message, PublishOptions? options = null, CancellationToken ct = default)`, `IStreamBus.PublishRawAsync(string stream, ReadOnlyMemory<byte> body, PublishOptions? options = null, CancellationToken ct = default)`. Router `Resolve(busMarker, messageType)` throws on unknown type or multi-stream ambiguity.

- [ ] **Step 1: Implement.** `PublishRegistration{MessageType, Stream, IsSuperStream, RoutingKey Func<object,string?>?}`; builder records `RouteBy` only and never asks for a super flag — `IsSuperStream` stays false until Task 6 resolves it from the declaration set and enforces the routing table (super + missing key and plain + stray key both fail at startup). Store mirrors `StreamBusConsumerStore` duplicate guard (same type+stream). Bus generic overloads: resolve route → serialize with bus serializer → if the Task 6 resolution marked the registration super, require a routing key (missing/empty → `StreamBusConfigurationException`) → `PublishOptions with { IsSuperStream, RoutingKey }` → producer path. Constructor injection of serializer + router via `Registry.cs` creation sites.
- [ ] **Step 2: Verify.** `PublishRoutingTests`: resolve ok; unknown type message names the missing registration; two streams one type → ambiguity error listing both; super-ness enforcement lives in Task 6 (not here); routing function maps correctly; explicit-stream overload bypasses the table. Update in-repo raw-publish callers to `PublishRawAsync`. Run `dotnet test tests/StreamBus.UnitTests --filter PublishRouting`; `dotnet build StreamBus.slnx`.
- [ ] **Step 3: Commit (with human approval)**

```bash
git add src/StreamBus/Configuration/StreamBusConfigurator.cs src/StreamBus/PublishRouting.cs src/StreamBus/Bus.cs src/StreamBus/Registry.cs tests/StreamBus.UnitTests/PublishRoutingTests.cs
git commit -m "Add typed publish with registration-time stream routing"
```

### Task 3: `ReceiveEndpoint` with first-class consumer-group identity

**Files:**
- Modify: `src/StreamBus/Configuration/StreamBusConfigurator.cs` (add `ReceiveEndpoint(name, Action<IReceiveEndpointConfigurator>)`, endpoint implementation accumulating an endpoint descriptor)
- Modify: `src/StreamBus/Hosting.cs` (translate descriptors into `StreamBusConsumerStore` entries + `TryAddTransient<TConsumer>` at Add-time; delete old `AddStreamBusConsumer*` extensions; hosted-service validation unchanged)
- Test: `tests/StreamBus.UnitTests/ReceiveEndpointTests.cs`; rewrite `HostingTests`/`TypedConsumerTests` registrations

**Interfaces:**
- Consumes: `ConsumeOptionsBuilder`-equivalent knobs, `StreamBusConsumerStore.Add`, Task 1 configurator.
- Produces: `IStreamBusConfigurator.ReceiveEndpoint(string stream, Action<IReceiveEndpointConfigurator> configure)`, `IReceiveEndpointConfigurator.Consumer<TConsumer>(string? consumerGroup = null) where TConsumer : class` (exactly one call per endpoint; second throws), `.SuperStream(bool)`, `.Reference(string?)` (alternative spelling of the group; setting both to different values throws), `.SingleActiveConsumer(bool)`, `.Offset(ConsumeOffset)`, `.OffsetStore(...)`, `.Readiness(...)`, `.OnRetryExhausted(...)`, `.SetMaxAge/...` declaration knobs, `.SetSuperStream(int partitions)`, `.UseMessageRetry(...)` + `.SetErrorQueue(...)` (implemented by Task 4; this task stores the descriptors). Reference default: `streambus:<BusFullName>:<ConsumerFullName>:<stream>` (hashed over 255 B), identical to today's derivation.

- [ ] **Step 1: Implement.** `ReceiveEndpointDescriptor{Stream, MessageType?, ConsumerType, Reference?, IsSuperStream, SuperPartitions?, knobs..., RetryDescriptor?, ErrorQueueDescriptor?}`; `Consumer<T>` captures `typeof(T)` and requires it implement `IConsumer<>` for some message type (resolve the message type from the implemented interface at Add-time; missing → config error). Message type + consumer + stream flow into the existing typed-consumer startup path unchanged (handler, scope-per-message, serializer).
- [ ] **Step 2: Verify.** `ReceiveEndpointTests`: explicit group used verbatim; default derivation matches today's format; two `Consumer` calls throw; group + `Reference` conflict throws; same consumer type on two markers derives different references (scale-out/cross-service test); non-`IConsumer` type throws. Rewrite affected unit/integration registrations to the endpoint form. Run unit suite.
- [ ] **Step 3: Commit (with human approval)**

```bash
git add src/StreamBus/Configuration/StreamBusConfigurator.cs src/StreamBus/Hosting.cs tests/StreamBus.UnitTests/ReceiveEndpointTests.cs tests/StreamBus.UnitTests/HostingTests.cs tests/StreamBus.UnitTests/TypedConsumerTests.cs tests/StreamBus.IntegrationTests
git commit -m "Add ReceiveEndpoint with explicit consumer-group identity"
```

### Task 4: Retry layering (global default, endpoint override, error-queue options)

**Files:**
- Modify: `src/StreamBus/Configuration/StreamBusConfigurator.cs` (`UseMessageRetry` on bus + endpoint configurators, `SetErrorQueue` on endpoint)
- Create: `src/StreamBus/Configuration/RetryConfiguration.cs` (`IRetryConfigurator` with `Immediate(n)`/`Interval(n, delay)`/`Intervals(params)`/`Exponential(count, base, max, multiplier)`/`None()`; mapping to `RetryPolicy` + bounded flag; `ErrorQueueOptions{Name?, Type}`; `ErrorQueueType{Classic, Quorum}`; derivation function implementing the Part A.5 table)
- Modify: `src/StreamBus/Hosting.cs` (apply effective retry + error-queue descriptor into consumer startup)
- Test: `tests/StreamBus.UnitTests/RetryConfigurationTests.cs`

**Interfaces:**
- Consumes: `RetryPolicy`, `ErrorStreamOptions` (replaced for this path — stream-based error path removed in Task 5), endpoint/bus descriptors.
- Produces: `cfg.UseMessageRetry(Action<IRetryConfigurator>)`, `e.UseMessageRetry(...)` (override), `cfg.UseRetry(RetryPolicy)` / `e.UseRetry(...)` escape hatches, `e.SetErrorQueue(string? name = null, ErrorQueueType type = ErrorQueueType.Classic)`. Derivation: bounded+queue→`SkipAfterDeadline`; bounded+no queue→`Strict` repeat-last-delay; unbounded+no queue→`Strict` forever (default); unbounded+queue→config error; `None()`+queue→first-failure handoff; `None()`+no queue→config error. Default queue name `{stream}_error`, default type classic.

- [ ] **Step 1: Implement.** Schedule mapping: `Immediate(n)` → `MaxAttempts = n+1`, zero `DelayCalculator`; `Interval(n, d)` → `MaxAttempts = n+1`, constant calculator; `Intervals(ds)` → `MaxAttempts = ds.Length+1`, indexed calculator repeating last; `Exponential(count, b, max, m)` → knob mapping; `None()` → `MaxAttempts = 1`. Strict-with-bound repeat-last-delay behavior implemented in the dispatch loop: when `FailureMode.Strict` and attempts exceed `MaxAttempts`, keep waiting on the last delay instead of handing off (small, isolated change in `StreamPartitionConsumer` retry block, guarded by a flag on the internal config). Endpoint retry overrides global; absent endpoint retry inherits global; absent global keeps today's `Strict` unbounded default.
- [ ] **Step 2: Verify.** `RetryConfigurationTests`: every row of the Part A.5 table (modes + config errors), schedule calculators produce the exact delays, override beats global, default is Strict unbounded. Integration: one bounded+queue test asserting handoff lands in `{stream}_error`. Run unit suite.
- [ ] **Step 3: Commit (with human approval)**

```bash
git add src/StreamBus/Configuration/StreamBusConfigurator.cs src/StreamBus/Configuration/RetryConfiguration.cs src/StreamBus/Hosting.cs src/StreamBus/StreamPartitionConsumer.cs tests/StreamBus.UnitTests/RetryConfigurationTests.cs
git commit -m "Add layered retry with per-stream error queues"
```

### Task 5: AMQP error transport (classic queues, headers envelope, requeue)

**Files:**
- Modify: `Directory.Packages.props` + `src/StreamBus/StreamBus.csproj` (add `RabbitMQ.Client`, CPM-pinned, choose current stable v7.x at implementation time and record the version here)
- Create: `src/StreamBus/ErrorQueueTransport.cs` (`IErrorQueueConnector` supervised AMQP connection per vhost, queue declare `{durable, classic|quorum}`, header envelope mapping, publish persistent, `BasicGet` requeue loop)
- Modify: `src/StreamBus/ErrorPublisher.cs` (publish to queue instead of stream; confirm via AMQP confirms; commit source offset exactly once after confirm, same guarantee as today), `src/StreamBus/ErrorEnvelope.cs` (add `ToHeaders()`/`TryParseHeaders()`; keep record fields), `src/StreamBus/Bus.cs` (replace `ReplayErrorStreamAsync` with `RequeueErrorsAsync(string stream, string? errorQueue = null, CancellationToken ct = default)` returning requeued count), `src/StreamBus/ConsumerRegistry.cs` (remove stream-replay path + "error target must be plain stream" validation)
- Test: `tests/StreamBus.UnitTests/ErrorQueueTests.cs` (mapping, naming, derivation); integration requeue test (broker-gated)

**Interfaces:**
- Consumes: endpoint error-queue descriptors (Task 4), broker offset commit path (unchanged).
- Produces: `IStreamBus.RequeueErrorsAsync(string stream, string? errorQueue = null, CancellationToken ct = default) -> Task<int>` (queue defaults to the endpoint's configured name, else `{stream}_error`). Queue declare: durable, non-exclusive, non-auto-delete, `x-queue-type` explicit (`classic` default opt-in `quorum`). Headers `streambus.error.*` (same names/values as today's app-properties), persistent delivery, `message-id`/`correlation-id` mapped. Malformed entries: ack + warning + skipped count in logs (documented data-loss edge, loud by design).

- [ ] **Step 1: Implement.** Connector holds one `IConnection` per vhost (same host/credentials, `AmqpPort`), creates channels per operation (cheap) with a recreate-on-fault guard; publisher enables confirms and waits (bounded wait with `StreamBusTimeoutException` on unknown outcome, mirroring stream semantics). Declare error queues at startup for every endpoint with an effective error queue (same declare pipeline stage as streams). Requeue: `BasicGet` loop, idle termination after N consecutive empty polls (same 5s/200ms shape as today's replay), stream-republish via existing producer path with `publishingId = sourceOffset`? Source offsets are meaningless for at-least-once requeue into a stream tail — republish as fresh messages (document the duplicate semantics), ack only after stream confirm.
- [ ] **Step 2: Verify.** `ErrorQueueTests`: header round-trip, default naming, quorum arg mapping, `RequeueErrorsAsync` queue-name defaulting. Integration (broker): bounded consumer exhausts → message appears in `orders_error` with headers → `RequeueErrorsAsync` returns 1 → message redelivered on source stream. Run unit suite + broker test where env allows.
- [ ] **Step 3: Commit (with human approval)**

```bash
git add Directory.Packages.props src/StreamBus/StreamBus.csproj src/StreamBus/ErrorQueueTransport.cs src/StreamBus/ErrorPublisher.cs src/StreamBus/ErrorEnvelope.cs src/StreamBus/Bus.cs src/StreamBus/ConsumerRegistry.cs tests/StreamBus.UnitTests/ErrorQueueTests.cs tests/StreamBus.IntegrationTests
git commit -m "Move exhausted-message handling to classic error queues"
```

### Task 6: Registration-derived topology (endpoints + publishes + explicit)

**Files:**
- Modify: `src/StreamBus/Hosting.cs` (`StartAsync` declare-set), `src/StreamBus/TopologyRegistry.cs` (only if a seam is needed — prefer unchanged)
- Test: extend `tests/StreamBus.UnitTests/HostingTests.cs` + one integration derivation test

**Interfaces:**
- Consumes: endpoint descriptors (stream, super flag + partitions, knobs), publish registrations, explicit declarations, error-queue descriptors (queues declared via Task 5 transport, not here).
- Produces: startup behavior — declare-set = explicit declarations + consumer streams (plain auto-declared; super auto-declared with endpoint partitions) + publish streams (super publish without a matching endpoint still needs partitions: require explicit `DeclareSuperStream` in that case — config error naming the stream if missing) + error queues via AMQP declare. At startup, resolve each publish registration against the declaration set and enforce: super + `RouteBy` → hashed publish; super + missing `RouteBy` → config error naming type + stream; plain + stray `RouteBy` → config error; super publish with no declaration anywhere → config error naming the stream (partitions unknown). Explicit declarations win over derived (derived entry skipped when explicit covers the name); derived super/plain mismatch against explicit → `StreamConfigurationConflictException` naming both sources. Raw-only streams (no registration) keep today's behavior: no declare attempt, attach retries until the stream exists.

- [ ] **Step 1: Implement.** Build the declare-set per bus in `StartAsync` before consumer attach, reusing the infinite-retry declare pipeline. Super-publish-without-partitions rule as above.
- [ ] **Step 2: Verify.** Unit: declare-set contents (consumer+publish+explicit union), explicit-wins skip, conflict error names sources, super-publish-without-partitions throws naming the stream, and each row of the routing-enforcement table (super±key, plain+stray key). Integration: drop explicit `DeclareStream("orders")` from one typed test, still green via derivation. Run unit suite + that test.
- [ ] **Step 3: Commit (with human approval)**

```bash
git add src/StreamBus/Hosting.cs tests/StreamBus.UnitTests/HostingTests.cs tests/StreamBus.IntegrationTests
git commit -m "Derive startup topology from endpoint and publish registrations"
```

### Task 7: Global startup options, remove per-bus hosting

**Files:**
- Modify: `src/StreamBus/Options.cs` (delete `StreamBusHostOptions`; delete `Hosting` from `StreamBusOptions`)
- Modify: `src/StreamBus/Hosting.cs` (delete old host-options builder; add `StreamBusHostingOptions` record, single-use store, `AddStreamBusHosting`; rewire hosted service, default non-blocking)
- Modify: tests referencing per-bus hosting

**Interfaces:**
- Produces: `services.AddStreamBusHosting(Action<StreamBusHostingOptionsBuilder> configure)` (second call throws), `.BlockStartupUntilReady(TimeSpan? timeout = null)` (same validation as today).

- [ ] **Step 1: Implement.** Store mirrors the config-store pattern; hosted service reads global value; missing call = today's non-blocking default.
- [ ] **Step 2: Verify.** Update tests; add double-registration-throws + missing-means-non-blocking tests. Run unit suite.
- [ ] **Step 3: Commit (with human approval)**

```bash
git add src/StreamBus/Options.cs src/StreamBus/Hosting.cs tests/StreamBus.UnitTests tests/StreamBus.IntegrationTests
git commit -m "Move startup blocking to service-level hosting options"
```

### Task 8: Full `IConfigurationSection` binding for primitives

**Files:**
- Create: `src/StreamBus/ConfigurationBinding.cs` (hand-rolled binder)
- Modify: `src/StreamBus/Configuration/StreamBusConfigurator.cs` (apply bound values before the code lambda)
- Test: `tests/StreamBus.UnitTests/ConfigurationBindingTests.cs`

**Interfaces:**
- Consumes: Task 1 section overloads.
- Produces: binding keys under the section — `Vhost` (default `"/"`), `UserName`/`Password` (required), `Host` (default `127.0.0.1`), `StreamPort`/`AmqpPort` (defaults), `Connection:{Producers,Consumers}PerConnection`, `Topology:Streams[]:{Name,MaxAgeDays,MaxLengthBytes,MaxSegmentSizeBytes,LeaderLocator}`, `Topology:SuperStreams[]:{... + Partitions}`. All failures throw `StreamBusConfigurationException` naming section path + key, collected and reported together. Code lambda runs after binding (types, routes, endpoints, retry).

- [ ] **Step 1: Implement.** Internal binder over `GetChildren()` with strict parsing; apply via the same configurator methods code uses (single code path, no drift). Verify `Microsoft.Extensions.Hosting` already exposes `IConfigurationSection`; if not, add `Microsoft.Extensions.Configuration.Abstractions` to CPM first.
- [ ] **Step 2: Verify.** In-memory-configuration tests: full bind, missing user/password names the path, bad port/partitions/locator reported together, code lambda still adds endpoints after binding. Run filtered suite.
- [ ] **Step 3: Commit (with human approval)**

```bash
git add src/StreamBus/ConfigurationBinding.cs src/StreamBus/Configuration/StreamBusConfigurator.cs tests/StreamBus.UnitTests/ConfigurationBindingTests.cs
git commit -m "Bind bus primitives from IConfigurationSection"
```

### Task 9: Root README + guide refresh

**Files:**
- Create: `README.md` (root, ~120 lines: what-it-is, prerequisites, install, A.1 condensed, concepts, docs map)
- Modify: `docs/guide/*.md` (rewrite to v2 API), `docs/llms.txt` (new ground truths)

**Interfaces:** None (docs only). Every snippet must match the Task 1–8 API — verify by review against the new signatures plus `dotnet build StreamBus.slnx` plus grep for removed names (`AddStreamBusConsumer`, `PublishAsync(stream, bytes)`, per-bus `.Hosting(`, `ReplayErrorStreamAsync`) returning zero hits in `src`/`tests`.

- [ ] **Step 1: Implement.** README + five rewritten chapters (publish/consume/config/multi-bus/errors) + `llms.txt` updates (classic `_error` queues, reference identity, RouteBy, global+override retry).
- [ ] **Step 2: Verify.** Snippet-vs-signature check; build green; removed-name grep clean.
- [ ] **Step 3: Commit (with human approval)**

```bash
git add README.md docs/guide docs/llms.txt
git commit -m "Rewrite README and user guide for the v2 API"
```

### Task 10: Full suite + acceptance roll-up

**Files:**
- Modify: `tests/*` (remaining fallout), acceptance doc (append v2 amendment)

**Interfaces:** None.

- [ ] **Step 1: Implement.** Full unit suite green; broker integration suite where env allows; update acceptance rows whose API changed (publish, registration, hosting/multi-bus, error handling now queue-backed).
- [ ] **Step 2: Verify.** `dotnet test StreamBus.slnx` green modulo documented env-skips; removed-name grep clean in `src` + `tests`.
- [ ] **Step 3: Commit (with human approval)**

```bash
git add tests docs/2026-10-13-streambus-acceptance.md
git commit -m "Bring test suite and acceptance matrix onto the v2 API"
```

---

## Self-review (run against the spec above)

1. **Spec coverage:** A.1→Tasks 1+3+7+8; A.2→Task 1; A.3→Task 1 (markers); A.4→Tasks 2+3+6 (`RouteBy`, partitions on endpoint); A.5→Task 4 (table rows each tested); A.6→Task 5 (queue publish + requeue); A.7→Task 2 (overloads); A.8→no change. Gaps: direct error-queue consumers (`ReceiveEndpoint` on a queue) intentionally deferred — error queues are worked via requeue in v2, noted here so the omission is explicit.
2. **Placeholder scan:** every step names exact files, signatures, commands, expected outcomes; no TBD/TODO/similar-to.
3. **Type consistency:** `PublishRegistration{MessageType,Stream,IsSuperStream,RoutingKey}` (Task 2) feeds Tasks 2+6; endpoint descriptor fields (Task 3) feed Tasks 4+5+6; `ErrorQueueType{Classic,Quorum}` + `SetErrorQueue(name?, type?)` (Task 4) feed Task 5; `RequeueErrorsAsync` (Task 5) replaces `ReplayErrorStreamAsync` everywhere including Task 9's grep check.
4. **Review Focus:** five lines each pinned to an owning task's verification (ambiguity test T2, startup routing-enforcement tests T6, undeclared-override StreamNotFound T2, cross-service reference test T3, queue-headers + requeue at-least-once T5).
