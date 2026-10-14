# StreamBus v2 User-Centric API Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Redesign the StreamBus public API so Workers configure from `appsettings.json`, register consumers/publishers inside their bus, publish typed messages without manual serialization, and control startup globally — with breaking changes allowed.

**Architecture:** Keep the runtime (supervisor, registries, dispatch, retry, topology) untouched; replace only the registration surface (`Hosting.cs` builders/stores), add a message-type route table for typed publish, move startup blocking to a service-level store, add a hand-rolled `IConfigurationSection` binder, and derive the startup topology set from registrations.

**Tech Stack:** .NET 10, `RabbitMQ.Stream.Client 1.12.1`, `Microsoft.Extensions.Hosting / Diagnostics.HealthChecks / Logging.Abstractions` (no new packages unless Task 4 proves one needed).

**Spec:** This document, Part A (User Experience) is the spec — it shows exactly how users will use the library. Part B implements it. Executors read both.

## Global Constraints

- `TargetFramework=net10.0`, `Nullable=enable`, `ImplicitUsings=enable`, `TreatWarningsAsErrors=true`, `LangVersion=latest` (`Directory.Build.props`).
- Central package management only (`Directory.Packages.props`); no `Version=` on `PackageReference`; forward slashes in paths.
- No `.ConfigureAwait(false)` inside `[Fact]`s; use explicit `TestContext.Current.CancellationToken` in tests; `Assert.Single(values, pred)` overload where applicable.
- Integration tests need broker `localhost:5552` with non-guest user/password from `STREAMBUS_USER`/`STREAMBUS_PASSWORD`; never use `guest`.
- Breaking changes allowed (pre-publish library): remove replaced APIs outright, no `[Obsolete]` shims.
- Every commit step below means: show changed files + exact command, wait for explicit human yes before running.

## Review Focus

- Same message type registered to two streams on one bus must fail at startup with a config error, not publish to a random one (Task 1).
- Super-stream typed publish without a routing function must fail at registration, not misroute at runtime (Task 1).
- Manually overriding an undeclared stream at the call site must still surface `StreamBusStreamNotFoundException`, never auto-create (Task 1).
- Auto-declared error streams must carry a default retention so dead-letter streams cannot grow unbounded (Task 5).
- A consumer registered for a stream the bus never declares (raw-only path with no registration) must keep today's behavior: startup retry, no silent skip (Task 5).

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
    "Port": 5552,
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
    bus => bus
        .Publish<OrderEvent>("orders")
        .Consume<OrderEvent, OrderConsumer>("orders", c => c
            .Retry(new RetryPolicy { FailureMode = FailureMode.SkipAfterDeadline, MaxAttempts = 5 })
            .ErrorStream("orders-errors", maxAge: TimeSpan.FromDays(14))));

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

What the user gets: no manual serialization, no per-call stream names, streams declared before first use (union of registrations + explicit topology), startup blocked globally until ready.

## A.2 Code-first registration (no configuration section)

```csharp
services.AddStreamBus("/", bus => bus
    .Credentials("app", "s3cret")
    .Endpoint("rabbitmq.internal", 5552)
    .Serializer(json => json.PropertyNamingPolicy = JsonNamingPolicy.CamelCase)
    .Topology(t => t.DeclareStream("audit", k => k.MaxAge(TimeSpan.FromDays(30))))
    .Publish<OrderEvent>("orders")
    .Consume<OrderEvent, OrderConsumer>("orders"));
```

## A.3 Multiple buses

```csharp
services.AddStreamBus<OrdersBus>(config.GetSection("StreamBus:Orders"), bus => bus
    .Publish<OrderEvent>("orders")
    .Consume<OrderEvent, OrderConsumer>("orders"));

services.AddStreamBus<BillingBus>(config.GetSection("StreamBus:Billing"), bus => bus
    .Consume<InvoiceEvent, InvoiceConsumer>("invoices"));

public sealed class CheckoutWorker(IStreamBus<OrdersBus> orders, IStreamBus<BillingBus> billing) : BackgroundService
{
    protected override async Task ExecuteAsync(CancellationToken ct)
    {
        await orders.PublishAsync(new OrderEvent("o-1", 1), ct);
    }
}
```

## A.4 Super streams with routing

```csharp
services.AddStreamBus("/", bus => bus
    .Credentials("app", "s3cret")
    .Publish<OrderEvent>("orders-super", p => p.RouteBy(e => e.OrderId))
    .Consume<OrderEvent, OrderConsumer>("orders-super", c => c.SuperStream()));

await bus.PublishAsync(new OrderEvent("o-1", 1), ct); // partitioned by OrderId
```

## A.5 Overrides and escape hatches (rare, still supported)

```csharp
await bus.PublishAsync("orders-manual", new OrderEvent("o-1", 1), ct); // typed, explicit stream
await bus.PublishRawAsync("orders", rawBytes, new PublishOptions(), ct); // raw bytes (renamed, was PublishAsync)
await using var sub = await bus.ConsumeAsync("audit", new ConsumeOptions { Reference = "ops" }, handler, ct); // raw consume, unchanged
```

## A.6 Effectively-once (unchanged runtime, registration-scoped reference)

```csharp
await using var pipe = await bus.ConsumeEffectivelyOnceAsync("orders", "invoices",
    new EffectivelyOnceOptions(), async (ctx, ct) => resultBytes, ct);
```

No API change; only the bus it runs on is now unambiguous by construction.

---

# Part B — Build tasks

### Task 1: Typed publish (registration + route table + `PublishAsync<T>`)

**Files:**
- Modify: `src/StreamBus/Hosting.cs` (extend `StreamBusOptionsBuilder` with `Publish<T>` + `PublishRegistrationBuilder`; extend `AddShared` to flush publish registrations)
- Create: `src/StreamBus/PublishRouting.cs` (`PublishRegistration` record, `PublishRegistrationBuilder`, `StreamBusPublishStore`, `StreamPublishRouter`)
- Modify: `src/StreamBus/Bus.cs` (add `PublishAsync<T>(T, ...)` + `PublishAsync<T>(string, T, ...)` to `IStreamBus`; rename raw to `PublishRawAsync`)
- Modify: `src/StreamBus/ProducerRegistry.cs` (expose internal raw publish for router use; keep behavior)
- Modify: `src/StreamBus/Registry.cs` (pass serializer + router into `StreamBus`/`StreamBus<TBus>` constructors)
- Test: `tests/StreamBus.UnitTests/PublishRoutingTests.cs`

**Interfaces:**
- Consumes: `IStreamSerializer`, `PublishOptions`, `StreamBusConfigurationException`, existing `StreamProducerRegistry.PublishAsync`.
- Produces: `StreamBusPublishStore.Add(Type busMarker, PublishRegistration)`, `StreamPublishRouter.Resolve(Type busMarker, Type messageType) -> PublishRegistration` (throws `StreamBusConfigurationException` on unknown type or ambiguous duplicate), `IStreamBus.PublishAsync<T>(T message, PublishOptions? options = null, CancellationToken ct = default)`, `IStreamBus.PublishAsync<T>(string stream, T message, PublishOptions? options = null, CancellationToken ct = default)`, `IStreamBus.PublishRawAsync(string stream, ReadOnlyMemory<byte> body, PublishOptions? options = null, CancellationToken ct = default)`.

- [ ] **Step 1: Implement `PublishRouting.cs`**

```csharp
namespace StreamBus;

public sealed record PublishRegistration
{
    public required Type MessageType { get; init; }
    public required string Stream { get; init; }
    public bool IsSuperStream { get; init; }
    public Func<object, string?>? RoutingKey { get; init; }
}

public sealed class PublishRegistrationBuilder<TMessage>
{
    private bool _super;
    private Func<TMessage, string?>? _routeBy;
    public PublishRegistrationBuilder<TMessage> SuperStream(bool enabled = true) { _super = enabled; return this; }
    public PublishRegistrationBuilder<TMessage> RouteBy(Func<TMessage, string?> route)
    {
        _routeBy = route ?? throw new ArgumentNullException(nameof(route));
        return this;
    }
    internal PublishRegistration Build(string stream)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(stream);
        if (_super && _routeBy is null)
            throw new StreamBusConfigurationException($"Publish registration for {typeof(TMessage).FullName} targets super stream '{stream}' but no RouteBy is configured.");
        return new PublishRegistration
        {
            MessageType = typeof(TMessage),
            Stream = stream,
            IsSuperStream = _super,
            RoutingKey = _routeBy is null ? null : o => _routeBy((TMessage)o),
        };
    }
}

public sealed class StreamBusPublishStore
{
    private readonly List<(Type BusMarker, PublishRegistration Registration)> _registrations = new();
    public void Add(Type busMarker, PublishRegistration registration)
    {
        ArgumentNullException.ThrowIfNull(busMarker);
        ArgumentNullException.ThrowIfNull(registration);
        if (_registrations.Any(r => r.BusMarker == busMarker
            && r.Registration.MessageType == registration.MessageType
            && r.Registration.Stream == registration.Stream))
            throw new StreamBusConfigurationException($"Duplicate publish registration for {registration.MessageType.FullName} on stream '{registration.Stream}' (marker {busMarker.FullName}).");
        _registrations.Add((busMarker, registration));
    }
    public IReadOnlyList<(Type BusMarker, PublishRegistration Registration)> Registrations => _registrations;
}

internal sealed class StreamPublishRouter
{
    private readonly StreamBusPublishStore _store;
    public StreamPublishRouter(StreamBusPublishStore store) => _store = store;
    public PublishRegistration Resolve(Type busMarker, Type messageType)
    {
        var matches = _store.Registrations
            .Where(r => r.BusMarker == busMarker && r.Registration.MessageType == messageType)
            .Select(r => r.Registration).ToList();
        return matches.Count switch
        {
            0 => throw new StreamBusConfigurationException($"No publish registration for message {messageType.FullName} on bus {busMarker.FullName}. Register it with Publish<{messageType.Name}>(\"stream\") or pass the stream explicitly."),
            > 1 => throw new StreamBusConfigurationException($"Ambiguous publish registration for message {messageType.FullName} on bus {busMarker.FullName}: registered for {string.Join(", ", matches.Select(m => $"'{m.Stream}'"))}. Pass the stream explicitly."),
            _ => matches[0],
        };
    }
}
```

- [ ] **Step 2: Implement builder + bus surface.** Add to `StreamBusOptionsBuilder` a `List<(Type Message, Func<string, PublishRegistration>)>` (or non-generic factory taking stream late); add methods:

```csharp
public StreamBusOptionsBuilder Publish<TMessage>(string stream, Action<PublishRegistrationBuilder<TMessage>>? configure = null)
{
    ArgumentException.ThrowIfNullOrWhiteSpace(stream);
    var b = new PublishRegistrationBuilder<TMessage>();
    configure?.Invoke(b);
    _publishFactories.Add(s => b.Build(s)); // capture stream via closure instead: b.Build(stream)
    return this;
}
```

(Simplify: build eagerly with `stream` and store the finished `PublishRegistration` plus `typeof(TMessage)` in a list exposed via internal `PublishRegistrations` property.) In `AddStreamBus`/`AddStreamBus<TBus>`, after `Build()`, flush `options` publish list into `GetOrCreatePublishStore(services)` (mirror `GetOrCreateConsumerStore`). On `IStreamBus` add the two generic overloads; rename existing raw `PublishAsync` to `PublishRawAsync` (update both `StreamBus` classes to delegate). Generic implementation sketch (in `StreamBus`):

```csharp
public Task PublishAsync<T>(T message, PublishOptions? options = null, CancellationToken ct = default)
{
    ArgumentNullException.ThrowIfNull(message);
    var reg = _router.Resolve(typeof(DefaultBus), typeof(T));
    return PublishTypedAsync(reg, message, null, options, ct);
}
public Task PublishAsync<T>(string stream, T message, PublishOptions? options = null, CancellationToken ct = default)
{
    ArgumentNullException.ThrowIfNull(message);
    ArgumentException.ThrowIfNullOrWhiteSpace(stream);
    var body = _serializer.Serialize(message);
    return _producers.PublishAsync(stream, body, options ?? new PublishOptions(), ct);
}
private Task PublishTypedAsync<T>(PublishRegistration reg, T message, string? overrideStream, PublishOptions? options, CancellationToken ct)
{
    var stream = overrideStream ?? reg.Stream;
    var body = _serializer.Serialize(message);
    string? routing = reg.IsSuperStream ? reg.RoutingKey?.Invoke(message!) : null;
    if (reg.IsSuperStream && string.IsNullOrWhiteSpace(routing) && overrideStream is null)
        throw new StreamBusConfigurationException($"Publish of {typeof(T).FullName} to super stream '{stream}' produced no routing key.");
    var opts = (options ?? new PublishOptions()) with { IsSuperStream = reg.IsSuperStream, RoutingKey = routing };
    return _producers.PublishAsync(stream, body, opts, ct);
}
```

`StreamBus`/`StreamBus<TBus>` constructors gain `IStreamSerializer serializer, StreamPublishRouter router` parameters; `Registry.cs` creation sites (`Activator.CreateInstance` + direct) updated to pass them from the resolved options/store.

- [ ] **Step 3: Verify.** Add `tests/StreamBus.UnitTests/PublishRoutingTests.cs` with: resolve returns registration; unknown type throws; duplicate streams for one type throws ambiguous; super without RouteBy throws at `Build`; routing function maps object correctly. Run: `dotnet test tests/StreamBus.UnitTests --filter PublishRouting`. Also `dotnet build StreamBus.slnx`. Fix all callers of the renamed raw method inside `src` and tests in this task (keep the tree green).
- [ ] **Step 4: Commit (with human approval)**

```bash
git add src/StreamBus/Hosting.cs src/StreamBus/PublishRouting.cs src/StreamBus/Bus.cs src/StreamBus/ProducerRegistry.cs src/StreamBus/Registry.cs tests/StreamBus.UnitTests/PublishRoutingTests.cs
git commit -m "Add typed publish with registration-time stream routing"
```

### Task 2: Nest consumer registration inside the bus builder

**Files:**
- Modify: `src/StreamBus/Hosting.cs` (add `StreamBusOptionsBuilder.Consume<TMessage,TConsumer>`, remove `AddStreamBusConsumer*` extensions + `AddStreamBusConsumerCore`; flush consumer list in `AddStreamBus*`)
- Modify: `tests/StreamBus.UnitTests/HostingTests.cs`, `tests/StreamBus.UnitTests/TypedConsumerTests.cs` (rewrite registrations to nested form)
- Modify: `tests/StreamBus.IntegrationTests/*` (rewrite `AddStreamBusConsumer` call sites to nested form)

**Interfaces:**
- Consumes: `ConsumeOptionsBuilder`, `StreamBusConsumerStore.Add` (unchanged), `TypedConsumerRegistration`.
- Produces: `StreamBusOptionsBuilder.Consume<TMessage, TConsumer>(string stream, Action<ConsumeOptionsBuilder>? configure = null) where TConsumer : class, IConsumer<TMessage>` (returns builder for chaining). `StreamBusOptions` gains internal `IReadOnlyList<(Type MessageType, Type ConsumerType, string Stream, ConsumeOptions Options)> ConsumerRegistrations`.

- [ ] **Step 1: Implement.** On `StreamBusOptionsBuilder` add:

```csharp
private readonly List<(Type MessageType, Type ConsumerType, string Stream, ConsumeOptions Options)> _consumers = new();
public StreamBusOptionsBuilder Consume<TMessage, TConsumer>(string stream, Action<ConsumeOptionsBuilder>? configure = null)
    where TConsumer : class, IConsumer<TMessage>
{
    ArgumentException.ThrowIfNullOrWhiteSpace(stream);
    var b = new ConsumeOptionsBuilder();
    configure?.Invoke(b);
    _consumers.Add((typeof(TMessage), typeof(TConsumer), stream, b.Build()));
    return this;
}
internal IReadOnlyList<(Type MessageType, Type ConsumerType, string Stream, ConsumeOptions Options)> ConsumerRegistrations => _consumers;
```

In `AddStreamBus`/`AddStreamBus<TBus>`, after `AddShared`, resolve `GetOrCreateConsumerStore(services)` and `store.Add(busMarker, messageType, consumerType, stream, options)` per entry (keeps the duplicate guard in one place). Delete the four `AddStreamBusConsumer*` methods and `AddStreamBusConsumerCore`. `StreamBusHostedService` validation stays as-is (marker always has a bus now by construction, but keep the guard for hosts built without `AddStreamBus`).

- [ ] **Step 2: Verify.** Rewrite affected unit tests to nested form, e.g.:

```csharp
services.AddStreamBus("/", bus => bus
    .Credentials("app", "secret")
    .Consume<OrderEvent, OrderConsumer>("orders"));
```

Run `dotnet test tests/StreamBus.UnitTests`. Then rewrite integration call sites the same way and run integration tests that do not need topology changes (broker-gated ones run where env allows).
- [ ] **Step 3: Commit (with human approval)**

```bash
git add src/StreamBus/Hosting.cs tests/StreamBus.UnitTests/HostingTests.cs tests/StreamBus.UnitTests/TypedConsumerTests.cs tests/StreamBus.IntegrationTests
git commit -m "Nest consumer registration inside the bus builder"
```

### Task 3: Global startup options, remove per-bus hosting

**Files:**
- Modify: `src/StreamBus/Options.cs` (delete `StreamBusHostOptions`; delete `Hosting` from `StreamBusOptions`)
- Modify: `src/StreamBus/Hosting.cs` (delete `StreamBusHostOptionsBuilder` + `StreamBusOptionsBuilder.Hosting()`; add `StreamBusHostingOptions` record, `StreamBusHostingStore`, `AddStreamBusHosting` extension; rewire `StreamBusHostedService` to read the global store, default non-blocking)
- Modify: tests referencing `.Hosting(` (unit + integration)

**Interfaces:**
- Consumes: nothing new.
- Produces: `public sealed record StreamBusHostingOptions { bool BlockStartupUntilReady; TimeSpan? StartupTimeout; }`, `public static IServiceCollection AddStreamBusHosting(this IServiceCollection services, Action<StreamBusHostingOptionsBuilder> configure)` (second call throws `StreamBusConfigurationException`), `StreamBusHostingOptionsBuilder.BlockStartupUntilReady(TimeSpan? timeout = null)` (same validation as today).

- [ ] **Step 1: Implement.** New store mirrors `GetOrCreateStore` pattern but throws on second `AddStreamBusHosting` call. `StreamBusHostedService` constructor takes `StreamBusHostingStore?` (missing = defaults, so hosts without the call behave as today's non-blocking default). Replace every `options.Hosting.BlockStartupUntilReady / StartupTimeout` read in `StartAsync` with the global value.
- [ ] **Step 2: Verify.** Update tests: `.Hosting(h => h.BlockStartupUntilReady(...))` inside `AddStreamBus` becomes `services.AddStreamBusHosting(h => h.BlockStartupUntilReady(...))`; add a test that two `AddStreamBusHosting` calls throw and that missing call means non-blocking start. Run `dotnet test tests/StreamBus.UnitTests`.
- [ ] **Step 3: Commit (with human approval)**

```bash
git add src/StreamBus/Options.cs src/StreamBus/Hosting.cs tests/StreamBus.UnitTests tests/StreamBus.IntegrationTests
git commit -m "Move startup blocking to service-level hosting options"
```

### Task 4: `IConfigurationSection` binding overloads

**Files:**
- Modify: `src/StreamBus/Hosting.cs` (add `AddStreamBus(IServiceCollection, IConfigurationSection, Action<StreamBusOptionsBuilder>?)` + generic marker overload)
- Create: `src/StreamBus/ConfigurationBinding.cs` (hand-rolled binder, no new packages)
- Test: `tests/StreamBus.UnitTests/ConfigurationBindingTests.cs` (in-memory configuration)

**Interfaces:**
- Consumes: `Microsoft.Extensions.Configuration.IConfigurationSection` (verify the `Microsoft.Extensions.Hosting` reference already exposes it; if not, add `Microsoft.Extensions.Configuration.Abstractions` to `Directory.Packages.props` + `StreamBus.csproj` first).
- Produces: `AddStreamBus(this IServiceCollection, IConfigurationSection section, Action<StreamBusOptionsBuilder>? configure = null)`, `AddStreamBus<TBus>(this IServiceCollection, IConfigurationSection section, Action<StreamBusOptionsBuilder>? configure = null)`. Binding keys (all under the section): `Vhost` (default `"/"`), `UserName` (required), `Password` (required), `Host` (default `127.0.0.1`), `Port` (default `5552`), `Connection:ProducersPerConnection`, `Connection:ConsumersPerConnection`, `Topology:Streams[]:{Name,MaxAgeDays,MaxLengthBytes,MaxSegmentSizeBytes,LeaderLocator}`, `Topology:SuperStreams[]:{...same...,Partitions}`. Missing/invalid values throw `StreamBusConfigurationException` naming the section path and key. The code lambda runs after binding and may add credentials/serializers/consumers/publishers/extra topology.

- [ ] **Step 1: Implement `ConfigurationBinding.cs`.** Internal static binder reading `section.GetChildren()` with `int.TryParse`/`ulong.TryParse`/`Enum.TryParse<StreamLeaderLocator>`; collect errors and throw once listing all bad keys. Apply order: `Endpoint(host, port)` → `Credentials(user, pass)` → `Connection(...)` → `Topology(...)` (explicit declarations only; registrations add more in Task 5). Then invoke the code lambda on the same builder and continue through the existing `AddShared` path (refactor: private `AddBusCore(services, busType, Action<StreamBusOptionsBuilder>)` shared by the vhost and section overloads).
- [ ] **Step 2: Verify.** `ConfigurationBindingTests` with `ConfigurationBuilder().AddInMemoryCollection(...)`: full section binds all values; missing `UserName` throws naming the path; bad `Port`/`Partitions`/`LeaderLocator` throw; code lambda can still add `.Consume<,>`/`.Publish<>()` after binding. Run `dotnet test tests/StreamBus.UnitTests --filter ConfigurationBinding`.
- [ ] **Step 3: Commit (with human approval)**

```bash
git add src/StreamBus/Hosting.cs src/StreamBus/ConfigurationBinding.cs tests/StreamBus.UnitTests/ConfigurationBindingTests.cs
git commit -m "Add IConfigurationSection binding for bus primitives"
```

### Task 5: Registration-derived topology (bus declares the union)

**Files:**
- Modify: `src/StreamBus/Hosting.cs` (`StreamBusHostedService.StartAsync`: build declare-set = explicit topology + consumer streams + publish streams + error streams with default retention)
- Modify: `tests/StreamBus.UnitTests/HostingTests.cs` (declare-set tests), integration topology tests as needed

**Interfaces:**
- Consumes: `StreamBusConsumerStore.Registrations`, `StreamBusPublishStore.Registrations`, `IStreamTopology.EnsureStreamAsync/EnsureSuperStreamAsync`, existing `DeclareUntilReadyAsync` pipeline.
- Produces: startup behavior — every stream referenced by a consumer/publisher registration on a bus is ensured with that bus's topology handle before attach; error-stream registrations without explicit declaration are ensured with `MaxAge = 7 days` default; explicit declarations always win (a derived entry for an explicitly declared stream is skipped, so differing knobs can never conflict with themselves); a derived super/plain mismatch against an explicit declaration surfaces `StreamConfigurationConflictException` naming both sources.

- [ ] **Step 1: Implement.** In `StartAsync`, per bus entry, after supervisor start and before consumer attach, compute: for each consumer reg → `StreamDeclaration`/`SuperStreamDeclaration` from `Options.IsSuperStream` (partitions: consumers don't know partition counts — derive by querying? No: partition count for a derived super declaration is unknown, so instead resolve partitions via `QueryPartitionAsync` when the stream exists, and only declare-with-partitions when an explicit declaration exists. Concretely: if no explicit declaration covers the name, call `EnsureStreamAsync` for plain; for super without explicit declaration, attempt `EnsureSuperStreamAsync` only if partition count is known — it is not, so skip declaration and let the existing attach path (`StreamNotFoundException` + startup retry) handle a missing super stream, unchanged from today). Document this rule in code comments: plain streams auto-declare; super streams require explicit declaration (partition count is a provisioning decision). Error streams: plain `EnsureStreamAsync` with `MaxAge` default 7 days when not explicitly declared. Keep the infinite-retry declare pipeline for all of these.
- [ ] **Step 2: Verify.** Unit tests: declare-set contains consumer + publish + error streams; explicit declaration wins over derived; error default retention applied; super-consumer without explicit declaration does not attempt a bogus declare (assert via fake topology). Integration: remove explicit `DeclareStream("orders")` from one typed test and show it still passes via derivation. Run unit suite + that integration test.
- [ ] **Step 3: Commit (with human approval)**

```bash
git add src/StreamBus/Hosting.cs tests/StreamBus.UnitTests/HostingTests.cs tests/StreamBus.IntegrationTests
git commit -m "Derive startup topology from consumer and publisher registrations"
```

### Task 6: Root README + guide refresh

**Files:**
- Create: `README.md` (root)
- Modify: `docs/guide/*.md` (rewrite publish/consume/config/multiple-buses chapters to the v2 API; keep operations/advanced chapters, adjust names), `docs/llms.txt`

**Interfaces:** None (docs only). README structure: what-it-is + bullets; prerequisites (stream plugin, non-guest user); install; 30-second quickstart (Part A.1 condensed); 10-line concepts; docs map; requirements. Every snippet must compile against the Task 1–5 API — verify by copying each snippet into a scratch console project referencing the built library (or by review against the new signatures plus `dotnet build` of a snippet project if cheap).

- [ ] **Step 1: Implement.** Write root `README.md` (~120 lines) and update the five affected guide chapters + `llms.txt` ground truths.
- [ ] **Step 2: Verify.** Snippet check described above; `dotnet build StreamBus.slnx`; confirm no remaining references to removed APIs (`AddStreamBusConsumer`, `PublishAsync(stream, bytes)` raw name, per-bus `.Hosting(`) via grep.
- [ ] **Step 3: Commit (with human approval)**

```bash
git add README.md docs/guide docs/llms.txt
git commit -m "Rewrite README and user guide for the v2 API"
```

### Task 7: Full suite + acceptance roll-up

**Files:**
- Modify: `tests/*` (any remaining fallout), `docs/2026-10-13-streambus-acceptance.md` (append v2 amendment) or new `docs/2026-10-1X-streambus-v2-acceptance.md`

**Interfaces:** None.

- [ ] **Step 1: Implement.** Run full unit suite (`dotnet test tests/StreamBus.UnitTests`), fix fallout; run integration suite where broker env is available; update the acceptance matrix rows whose API changed (FR-1 publish, FR-2 consume registration, FR-8 hosting/multi-bus).
- [ ] **Step 2: Verify.** `dotnet test StreamBus.slnx` green (excluding documented env-skips); grep for removed API names returns zero hits in `src` and `tests`.
- [ ] **Step 3: Commit (with human approval)**

```bash
git add tests docs/2026-10-13-streambus-acceptance.md
git commit -m "Bring test suite and acceptance matrix onto the v2 API"
```

---

## Self-review (run against the spec above)

1. **Spec coverage:** A.1→Tasks 2+3+4+5; A.2→Tasks 2+3 (code-first overload kept); A.3→Tasks 2+4 (markers unchanged); A.4→Task 1 (`RouteBy`); A.5→Task 1 (overloads + `PublishRawAsync`); A.6→no change (runtime untouched). Gaps: none.
2. **Placeholder scan:** every step names exact files, signatures, commands, and expected outcomes; no TBD/TODO/similar-to.
3. **Type consistency:** `PublishRegistration{MessageType,Stream,IsSuperStream,RoutingKey}` is produced in Task 1 and consumed by Tasks 1 (router) and 5 (declare-set); `StreamBusOptionsBuilder.Consume/Publish` produced in Tasks 1–2 and consumed by Tasks 4–5; `AddStreamBusHosting` produced in Task 3, independent of others. `PublishOptions with {}` record semantics hold (`PublishOptions` is a record — verified in source).
4. **Review Focus:** each of the five lines names its owning task's verification (ambiguous-duplicate test, `Build`-time RouteBy test, undeclared-override behavior kept via unchanged `StreamBusStreamNotFoundException` path, error default-retention test, super-without-declaration no-bogus-declare test).
