# StreamBus Phase 9 (Typed Consumers + Serializer) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Implement MassTransit-style typed `IConsumer<T>` consumption with explicit stream binding, a pluggable serializer (JSON default), per-attempt DI scopes, and hosted startup in both modes — plus closing hosting-conformance gaps.

**Architecture:** The typed path is a thin layer over the proven plain path: `AddStreamBusConsumer` records (marker, message, consumer, stream, options) in a `StreamBusConsumerStore` and registers the consumer `TryAddTransient`; the hosted service resolves marker→vhost at startup and subscribes each registration through the existing `ConsumeAsync` with a wrapper handler that deserializes (failures become handler failures under the consumer's Retry/FailureMode), resolves a fresh consumer per attempt from a per-attempt scope, and invokes it. Startup mirrors the topology pattern (fail-fast sync resolution, background retry on NotReady/NotFound, blocking await bounded).

**Tech Stack:** .NET 10 (`net10.0`), C# latest, `RabbitMQ.Stream.Client` 1.12.1 (same pins — no new packages; `System.Text.Json` is inbox), xUnit v3, `StreamBus.slnx`, Central Package Management.

**Spec:** `docs/2026-10-08-streambus-design.md` (§9 API sketch, typed `IConsumer<T>`) and `docs/2026-10-08-streambus-requirements.md` (FR-2.1, FR-2.8). Hosting items (FR-8.9, FR-8.11) are already implemented — this phase pins conformance gaps only. No gate blocks this phase.

## Global Constraints

- `TargetFramework` exactly `net10.0`; `Nullable`/`ImplicitUsings` enable; `TreatWarningsAsErrors` true; `ManagePackageVersionsCentrally` true; `IsTestingPlatformApplication` false (all in `Directory.Build.props`, unchanged).
- `RabbitMQ.Stream.Client` exactly `1.12.1`; `Microsoft.Extensions.Hosting` / `Diagnostics.HealthChecks` / `Logging.Abstractions` exactly `10.0.12`; `xunit.v3` 4.0.2, `xunit.runner.visualstudio` 4.0.1, `Microsoft.NET.Test.Sdk` 18.10.1. No new packages (`System.Text.Json` ships inbox with .NET 10).
- Library root namespace `StreamBus`; never `RabbitMQ.Stream.*`.
- The typed path adds no new broker or offset mechanics: it delegates to `IStreamBus.ConsumeAsync`, so retry, error-stream, SAC, ordering, and offset rules are reused verbatim — never reimplemented.
- Effectively-once (`EffectivelyOnceConfig`) stays off the typed path in v1 (FR-6 uses the byte-level managed loop + `PublishResultAsync`).
- Integration tests require the local broker at `localhost:5552` with non-guest creds (`STREAMBUS_USER`/`STREAMBUS_PASSWORD`, defaulting to `app`/`password` in-test); never use `guest`.
- Every commit step requires explicit human approval before running; never push, never amend without permission.

## Review Focus

- Poison JSON under `Strict` stalls the partition forever with deserialization errors in the logs — never skipped, never silently dropped. Pinned by Task 4 Strict poison test.
- Two consumer types on one (bus, stream) must never share a reference (that would split one ordered stream across different handlers). Pinned by Task 3 distinctness test + Task 4 two-types test.
- Consumer instances resolve fresh per attempt (a retrying singleton with mutable state would carry cross-attempt pollution). Pinned by Task 3 resolution-count test.
- Serializer mismatch between publisher and consumer must fail loudly (error stream under Skip), never as silent misread. Pinned by Task 4 type-mismatch test + documentation.
- A consumer referencing a bus marker with no `AddStreamBus` registration must fail startup naming the marker, in both modes (resolution runs synchronously before backgrounding). Pinned by Task 5 unknown-marker test.

---

## File Structure

New files (no new packages):

- `src/StreamBus/Serialization.cs` — `IStreamSerializer`, `SystemTextJsonStreamSerializer` (shared stateless `Default`), `StreamBusDeserializationException`.
- `src/StreamBus/Consumer.cs` — `IConsumer<T>`, `ConsumeContext<T>`.

Modified files (existing suites must stay green):

- `src/StreamBus/ReferenceDerivation.cs` — `Derive(busType, consumerType, stream)` overload (plain two-arg overload untouched).
- `src/StreamBus/Options.cs` — `StreamBusOptions.Serializer` (defaults to the shared JSON default).
- `src/StreamBus/Hosting.cs` — `Serializer(...)` builder methods, `StreamBusHostOptionsBuilder` timeout validation, `ConsumeOptionsBuilder`, `TypedConsumerRegistration` + `StreamBusConsumerStore`, `AddStreamBusConsumer` ×2, `TypedConsumerHandler`, `StreamBusHostedService` typed startup (scope factory, sync resolution, background/blocking, transient classification, `_declareCts` → `_startupCts` rename).
- `tests/StreamBus.UnitTests/TypedConsumerTests.cs` (new, Task 3) + `tests/StreamBus.UnitTests/HostingTests.cs` (3 conformance tests, Tasks 3+5 reconfirmed below).
- `tests/StreamBus.IntegrationTests/TypedConsumerIntegrationTests.cs` (new, Task 4) + `tests/StreamBus.IntegrationTests/TypedStartupIntegrationTests.cs` (new, Task 5).

Explicitly out of Phase 9 (deferred, not stubs): typed *publish* (`PublishAsync` stays byte-based; publishers serialize with the same options by hand — documented); effectively-once typed handlers (byte-level FR-6 path only); `BindingsSuperStreamSpec` (Phase 8 scope stands); policy management (operator domain); per-message TTL/priority (do not exist).

Recorded rulings (spec as authority; bind all tasks): scope + consumer instance resolve fresh PER ATTEMPT (failure isolation beats scope economy); no startup resolve-probe (Add always registers via TryAddTransient, so a missing type is unreachable through public API — resolution failure surfaces as a loud handler error naming the type); no typed-publish API in v1; `TryAddTransient` respects user pre-registration (explicit lifetime wins); registration order is free (consumer-before-bus and bus-before-consumer both work — resolution happens at Start); `StreamConfigurationConflictException` hierarchy untouched (startup catch checks conflict then configuration separately); `StartupTimeout` must be positive when set (validated at Add time); same-vhost registrations may carry different topology lists (unioned, Phase 8 ruling stands).

---

### Task 1: Serialization + typed records + reference + serializer plumbing

**Files:**
- Create: `src/StreamBus/Serialization.cs`
- Create: `src/StreamBus/Consumer.cs`
- Modify: `src/StreamBus/ReferenceDerivation.cs` (add overload only)
- Modify: `src/StreamBus/Options.cs` (add `Serializer` property only)

**Interfaces:**
- Consumes: nothing new (pure records + mapping).
- Produces: `IStreamSerializer`/`SystemTextJsonStreamSerializer`/`StreamBusDeserializationException`, `IConsumer<T>`/`ConsumeContext<T>`, `Derive(busType, consumerType, stream)`, `StreamBusOptions.Serializer` for Tasks 2–5.

- [ ] **Step 1: Implement `Serialization.cs`**

```csharp
using System.Text.Json;

namespace StreamBus;

public interface IStreamSerializer
{
    byte[] Serialize<T>(T message);
    T Deserialize<T>(ReadOnlyMemory<byte> body);
}

public sealed class SystemTextJsonStreamSerializer : IStreamSerializer
{
    public static readonly SystemTextJsonStreamSerializer Default = new();

    private readonly JsonSerializerOptions _options;

    public SystemTextJsonStreamSerializer(JsonSerializerOptions? options = null)
        => _options = options ?? new JsonSerializerOptions();

    public byte[] Serialize<T>(T message) => JsonSerializer.SerializeToUtf8Bytes(message, _options);

    public T Deserialize<T>(ReadOnlyMemory<byte> body)
        => JsonSerializer.Deserialize<T>(body.Span) ?? throw new JsonException($"Deserialization of {typeof(T).FullName} returned null.");
}
```

```csharp
namespace StreamBus;

public sealed class StreamBusDeserializationException : Exception
{
    public StreamBusDeserializationException(string stream, int partition, ulong offset, Type messageType, Exception? inner)
        : base($"Could not deserialize the message at {stream} partition {partition} offset {offset} as {messageType.FullName}." + (inner is null ? " The payload deserialized to null." : ""), inner)
    {
        Stream = stream;
        Partition = partition;
        Offset = offset;
        MessageType = messageType;
    }

    public string Stream { get; }
    public int Partition { get; }
    public ulong Offset { get; }
    public Type MessageType { get; }
}
```

(Both types in one file: single responsibility is serialization. `Default` is stateless and shared — `JsonSerializerOptions` is thread-safe for concurrent serialization once constructed.)

- [ ] **Step 2: Implement `Consumer.cs`**

```csharp
namespace StreamBus;

public interface IConsumer<T>
{
    Task Consume(ConsumeContext<T> context, CancellationToken cancellationToken);
}

public sealed record ConsumeContext<T>
{
    public required T Message { get; init; }
    public required string Stream { get; init; }
    public required int Partition { get; init; }
    public required ulong Offset { get; init; }
    public required string? RoutingValue { get; init; }
    public required ReadOnlyMemory<byte> Body { get; init; }
    public required MessageMetadata Metadata { get; init; }
    public required CancellationToken CancellationToken { get; init; }
}
```

(Type parameter is `T` per the design sketch. `Body` stays raw so results can be republished byte-identical.)

- [ ] **Step 3: Extend `ReferenceDerivation.cs` (existing overload untouched)**

```csharp
    public static string Derive(Type busType, Type consumerType, string stream)
    {
        ArgumentNullException.ThrowIfNull(busType);
        ArgumentNullException.ThrowIfNull(consumerType);
        ArgumentException.ThrowIfNullOrWhiteSpace(stream);
        var full = $"streambus:{busType.FullName}:{consumerType.FullName}:{stream}";
        if (Encoding.UTF8.GetByteCount(full) <= MaxReferenceBytes)
            return full;
        var hash = Convert.ToHexString(SHA256.HashData(Encoding.UTF8.GetBytes(full)))[..32];
        return $"streambus-{hash}";
    }
```

(Two consumer types on one stream derive distinct references (FR-10.10); the plain two-arg overload is unchanged so delegate consumers keep their groups.)

- [ ] **Step 4: Extend `Options.cs` (one property)**

```csharp
    public IStreamSerializer Serializer { get; init; } = SystemTextJsonStreamSerializer.Default;
```

- [ ] **Step 5: Verify**

```powershell
dotnet build src/StreamBus/StreamBus.csproj -c Release
```

Expected: zero warnings/errors. Records are exercised by Tasks 2–4.

- [ ] **Step 6: Commit (with human approval)**

```powershell
git add src/StreamBus/Serialization.cs src/StreamBus/Consumer.cs src/StreamBus/ReferenceDerivation.cs src/StreamBus/Options.cs
git commit -m "feat: add typed consumer records, serializer, and per-consumer reference"
```

---

### Task 2: Registration, handler, hosted startup

**Files:**
- Modify: `src/StreamBus/Hosting.cs` (builders, store, `AddStreamBusConsumer` ×2, `TypedConsumerHandler`, hosted-service changes)

**Interfaces:**
- Consumes: Task 1 records, `IStreamBus.ConsumeAsync` (delegate path), `StreamBusDelays.ExponentialCapped` (5-arg), existing `StreamBusHostedService` start/stop shape.
- Produces: `AddStreamBusConsumer` surface, `IsStartupTransient`, started typed consumers for Tasks 3–5.

- [ ] **Step 1: Options builder additions in `Hosting.cs`**

```csharp
    private IStreamSerializer _serializer = SystemTextJsonStreamSerializer.Default;
    public StreamBusOptionsBuilder Serializer(IStreamSerializer serializer) { _serializer = serializer ?? throw new ArgumentNullException(nameof(serializer)); return this; }
    public StreamBusOptionsBuilder Serializer(Action<JsonSerializerOptions> configure)
    {
        ArgumentNullException.ThrowIfNull(configure);
        var options = new JsonSerializerOptions();
        configure(options);
        _serializer = new SystemTextJsonStreamSerializer(options);
        return this;
    }
```

(`_serializer` field next to the other builder fields; `Build()` gains `Serializer = _serializer,`. Add `using System.Text.Json;` — inbox, no package.)

Timeout validation in the existing host builder method:

```csharp
    public StreamBusHostOptionsBuilder BlockStartupUntilReady(TimeSpan? timeout = null)
    {
        if (timeout is { } t && t <= TimeSpan.Zero)
            throw new StreamBusConfigurationException("StartupTimeout must be positive when set.");
        _block = true; _timeout = timeout; return this;
    }
```

- [ ] **Step 2: `ConsumeOptionsBuilder` + store + registration (in `Hosting.cs`)**

```csharp
public sealed class ConsumeOptionsBuilder
{
    private bool _super;
    private string? _reference;
    private bool _sac = true;
    private ConsumeOrdering _ordering = ConsumeOrdering.Strict;
    private ConsumeOffset _offset = ConsumeOffset.ResumeOrFirst;
    private OffsetStoreOptions _store = new();
    private RetryPolicy _retry = new();
    private ErrorStreamOptions? _error;
    private Func<ErrorHandoff, Task>? _hook;
    private ReadinessPolicy _readiness = ReadinessPolicy.FailFastPolicy;

    public ConsumeOptionsBuilder SuperStream(bool enabled = true) { _super = enabled; return this; }
    public ConsumeOptionsBuilder Reference(string? reference) { _reference = reference; return this; }
    public ConsumeOptionsBuilder SingleActiveConsumer(bool enabled = true) { _sac = enabled; return this; }
    public ConsumeOptionsBuilder Ordering(ConsumeOrdering ordering) { _ordering = ordering; return this; }
    public ConsumeOptionsBuilder Offset(ConsumeOffset offset) { _offset = offset; return this; }
    public ConsumeOptionsBuilder OffsetStore(OffsetStoreOptions options) { _store = options ?? throw new ArgumentNullException(nameof(options)); return this; }
    public ConsumeOptionsBuilder Retry(RetryPolicy policy) { _retry = policy ?? throw new ArgumentNullException(nameof(policy)); return this; }
    public ConsumeOptionsBuilder ErrorStream(ErrorStreamOptions? options) { _error = options; return this; }
    public ConsumeOptionsBuilder OnRetryExhausted(Func<ErrorHandoff, Task>? hook) { _hook = hook; return this; }
    public ConsumeOptionsBuilder Readiness(ReadinessPolicy policy) { _readiness = policy ?? throw new ArgumentNullException(nameof(policy)); return this; }

    public ConsumeOptions Build() => new()
    {
        IsSuperStream = _super,
        Reference = _reference,
        SingleActiveConsumer = _sac,
        Ordering = _ordering,
        Offset = _offset,
        OffsetStore = _store,
        Retry = _retry,
        ErrorStream = _error,
        OnRetryExhausted = _hook,
        ReadinessPolicy = _readiness,
    };
}

public sealed record TypedConsumerRegistration
{
    public required Type BusMarker { get; init; }
    public required Type MessageType { get; init; }
    public required Type ConsumerType { get; init; }
    public required string Stream { get; init; }
    public required ConsumeOptions Options { get; init; }
}

public sealed class StreamBusConsumerStore
{
    private readonly List<TypedConsumerRegistration> _registrations = new();

    public void Add(Type busMarker, Type messageType, Type consumerType, string stream, ConsumeOptions options)
    {
        ArgumentNullException.ThrowIfNull(busMarker);
        ArgumentNullException.ThrowIfNull(messageType);
        ArgumentNullException.ThrowIfNull(consumerType);
        ArgumentException.ThrowIfNullOrWhiteSpace(stream);
        ArgumentNullException.ThrowIfNull(options);
        if (_registrations.Any(r => r.BusMarker == busMarker && r.MessageType == messageType && r.ConsumerType == consumerType && r.Stream == stream))
            throw new StreamBusConfigurationException($"Duplicate typed consumer registration for {consumerType.FullName} on stream '{stream}' (marker {busMarker.FullName}).");
        _registrations.Add(new TypedConsumerRegistration { BusMarker = busMarker, MessageType = messageType, ConsumerType = consumerType, Stream = stream, Options = options });
    }

    public IReadOnlyList<TypedConsumerRegistration> Registrations => _registrations;
}
```

`AddStreamBusConsumer` overloads (in `ServiceCollectionExtensions`; `TryAddTransient` respects user pre-registration):

```csharp
    public static IServiceCollection AddStreamBusConsumer<TMessage, TConsumer>(this IServiceCollection services, string stream, Action<ConsumeOptionsBuilder>? configure = null)
        where TConsumer : class, IConsumer<TMessage>
        => AddStreamBusConsumerCore<TMessage, TConsumer>(services, typeof(DefaultBus), stream, configure);

    public static IServiceCollection AddStreamBusConsumer<TBus, TMessage, TConsumer>(this IServiceCollection services, string stream, Action<ConsumeOptionsBuilder>? configure = null)
        where TConsumer : class, IConsumer<TMessage>
        => AddStreamBusConsumerCore<TMessage, TConsumer>(services, typeof(TBus), stream, configure);

    private static IServiceCollection AddStreamBusConsumerCore<TMessage, TConsumer>(IServiceCollection services, Type busMarker, string stream, Action<ConsumeOptionsBuilder>? configure)
        where TConsumer : class, IConsumer<TMessage>
    {
        ArgumentNullException.ThrowIfNull(services);
        ArgumentException.ThrowIfNullOrWhiteSpace(stream);
        services.TryAddTransient<TConsumer>();
        var builder = new ConsumeOptionsBuilder();
        configure?.Invoke(builder);
        GetOrCreateConsumerStore(services).Add(busMarker, typeof(TMessage), typeof(TConsumer), stream, builder.Build());
        return services;
    }

    private static StreamBusConsumerStore GetOrCreateConsumerStore(IServiceCollection services)
    {
        var existing = services.FirstOrDefault(d =>
            d.ServiceType == typeof(StreamBusConsumerStore) && d.ImplementationInstance is StreamBusConsumerStore);
        if (existing?.ImplementationInstance is StreamBusConsumerStore store) return store;
        var created = new StreamBusConsumerStore();
        services.AddSingleton<StreamBusConsumerStore>(created);
        return created;
    }
```

(`TryAddTransient` needs the existing `Microsoft.Extensions.DependencyInjection.Extensions` import ✓. `services.FirstOrDefault` needs `System.Linq` — check `Hosting.cs` imports; add `using System.Linq;` only if the build complains (the existing `GetOrCreateStore` already uses it, so it resolves today).)

- [ ] **Step 3: `TypedConsumerHandler` (in `Hosting.cs`)**

```csharp
internal static class TypedConsumerHandler
{
    public static Func<ConsumeContext, Task> Create(Type messageType, Type consumerType, IServiceScopeFactory scopes, IStreamSerializer serializer)
    {
        ArgumentNullException.ThrowIfNull(messageType);
        ArgumentNullException.ThrowIfNull(consumerType);
        ArgumentNullException.ThrowIfNull(scopes);
        ArgumentNullException.ThrowIfNull(serializer);
        var open = typeof(TypedConsumerHandler).GetMethod(nameof(CreateTyped), BindingFlags.NonPublic | BindingFlags.Static)
            ?? throw new StreamBusConfigurationException("Typed consumer invoker is missing (internal error).");
        MethodInfo closed;
        try
        {
            closed = open.MakeGenericMethod(messageType, consumerType);
        }
        catch (ArgumentException)
        {
            throw new StreamBusConfigurationException($"Consumer {consumerType.FullName} cannot consume {messageType.FullName} (must implement IConsumer<{messageType.Name}>).");
        }
        return (Func<ConsumeContext, Task>)closed.Invoke(null, new object[] { scopes, serializer })!;
    }

    private static Func<ConsumeContext, Task> CreateTyped<T, TConsumer>(IServiceScopeFactory scopes, IStreamSerializer serializer)
        where TConsumer : class, IConsumer<T>
        => async raw =>
        {
            T message;
            try
            {
                message = serializer.Deserialize<T>(raw.Body);
            }
            catch (Exception ex) when (ex is not OperationCanceledException)
            {
                throw new StreamBusDeserializationException(raw.Stream, raw.Partition, raw.Offset, typeof(T), ex);
            }
            if (ReferenceEquals(message, null))
                throw new StreamBusDeserializationException(raw.Stream, raw.Partition, raw.Offset, typeof(T), null);
            using var scope = scopes.CreateScope();
            var consumer = scope.ServiceProvider.GetRequiredService<TConsumer>();
            await consumer.Consume(new ConsumeContext<T>
            {
                Message = message,
                Stream = raw.Stream,
                Partition = raw.Partition,
                Offset = raw.Offset,
                RoutingValue = raw.RoutingValue,
                Body = raw.Body,
                Metadata = raw.Metadata,
                CancellationToken = raw.CancellationToken,
            }, raw.CancellationToken).ConfigureAwait(false);
        };
}
```

(Add `using System.Reflection;`. Per-attempt scope + fresh resolution happens here because the partition loop invokes this delegate once per attempt. `ReferenceEquals` avoids generic-nullability warnings.)

- [ ] **Step 4: Hosted-service typed startup**

Ctor gains two trailing optionals (existing 2–3-arg constructions keep compiling; DI honors defaults when unregistered):

```csharp
    private readonly ILogger<StreamBusHostedService> _logger; // existing from Phase 8
    private readonly IServiceScopeFactory? _scopes;
    private readonly StreamBusConsumerStore? _consumers;
    private CancellationTokenSource? _startupCts;
```

Rename `_declareCts` → `_startupCts` at all its sites (field, StartAsync create, StopAsync cancel) — it now covers topology + consumer background work.

In `StartAsync`, FIRST (before touching supervisors — pure fail-fast, no I/O):

```csharp
        var starters = new List<(TypedConsumerRegistration Reg, string Vhost, IStreamSerializer Serializer)>();
        if (_consumers is not null)
        {
            if (_scopes is null && _consumers.Registrations.Count > 0)
                throw new StreamBusConfigurationException("Typed consumers are registered but no IServiceScopeFactory is available (internal error).");
            foreach (var reg in _consumers.Registrations)
            {
                var match = _store.Registrations.FirstOrDefault(r => r.BusType == reg.BusMarker);
                if (match.BusType is null)
                    throw new StreamBusConfigurationException($"Typed consumer {reg.ConsumerType.FullName} on stream '{reg.Stream}' references marker {reg.BusMarker.FullName} with no AddStreamBus registration. Register the bus (Add-call order does not matter).");
                starters.Add((reg, match.Options.Vhost, match.Options.Serializer ?? SystemTextJsonStreamSerializer.Default));
            }
        }
```

Kick per-registration tasks next to the topology kick (same `startupCts` token), with kind-specific fault text:

```csharp
        var consumerTasks = new List<Task>();
        foreach (var (reg, vhost, serializer) in starters)
        {
            var task = StartConsumerUntilReadyAsync(reg, vhost, serializer, startupCts.Token);
            if (!blocking)
            {
                _ = task.ContinueWith(
                    t =>
                    {
                        if (t.Exception is { } ex)
                            _logger.LogCritical(ex, "Typed consumer {Consumer} on stream '{Stream}' failed permanently; it is NOT consuming.", reg.ConsumerType.FullName, reg.Stream);
                    },
                    CancellationToken.None,
                    TaskContinuationOptions.OnlyOnFaulted | TaskContinuationOptions.ExecuteSynchronously,
                    TaskScheduler.Default);
            }
            else
            {
                consumerTasks.Add(task);
            }
        }
```

(`blocking` is the existing local. `startupCts` is the renamed CTS.)

Blocking await: extend the existing declare-await block to both lists plus configuration surfacing:

```csharp
        var startupTasks = declareTasks.Concat(consumerTasks).ToList();
        if (startupTasks.Count > 0)
        {
            ...existing per-task conflict continuations on declareTasks (unchanged)...
            try
            {
                await Task.WhenAll(startupTasks).WaitAsync(cts.Token).ConfigureAwait(false);
            }
            catch (OperationCanceledException) when (!ct.IsCancellationRequested)
            {
                try { startupCts.Cancel(); }
                catch (ObjectDisposedException) { }
                var conflict = startupTasks
                    .SelectMany(t => t.Exception?.InnerExceptions.AsEnumerable() ?? Enumerable.Empty<Exception>())
                    .OfType<StreamConfigurationConflictException>()
                    .FirstOrDefault();
                if (conflict is not null) System.Runtime.ExceptionServices.ExceptionDispatchInfo.Capture(conflict).Throw();
                var config = startupTasks
                    .SelectMany(t => t.Exception?.InnerExceptions.AsEnumerable() ?? Enumerable.Empty<Exception>())
                    .OfType<StreamBusConfigurationException>()
                    .FirstOrDefault();
                if (config is not null) System.Runtime.ExceptionServices.ExceptionDispatchInfo.Capture(config).Throw();
                throw new StreamBusTimeoutException($"Timed out declaring topology and starting typed consumers within the startup timeout.");
            }
        }
```

(Restructure the existing `if (declareTasks.Count > 0)` block into this shape — conflict stays first (it is not a `StreamBusConfigurationException` subclass), then configuration (consumer validation errors surface immediately instead of timing out), then timeout. The message keeps the word "topology" so the Phase 8 timeout test still passes.)

Per-registration starter + transient classification:

```csharp
    internal static bool IsStartupTransient(Exception ex)
        => ex is StreamBusNotReadyException or StreamBusStreamNotFoundException;

    private async Task StartConsumerUntilReadyAsync(TypedConsumerRegistration reg, string vhost, IStreamSerializer serializer, CancellationToken ct)
    {
        var entry = _registry.GetOrCreateEntry(reg.BusMarker, vhost);
        var reference = reg.Options.Reference ?? ReferenceDerivation.Derive(reg.BusMarker, reg.ConsumerType, reg.Stream);
        ReferenceDerivation.Validate(reference);
        var options = reg.Options with { Reference = reference };
        var handler = TypedConsumerHandler.Create(reg.MessageType, reg.ConsumerType, _scopes!, serializer);
        var attempt = 0;
        while (true)
        {
            ct.ThrowIfCancellationRequested();
            try
            {
                await entry.Bus.ConsumeAsync(reg.Stream, options, handler, ct).ConfigureAwait(false);
                return;
            }
            catch (OperationCanceledException) when (ct.IsCancellationRequested)
            {
                throw;
            }
            catch (Exception ex) when (IsStartupTransient(ex))
            {
                attempt++;
                _logger.LogWarning(ex, "Typed consumer {Consumer} on stream '{Stream}' not started (attempt {Attempt}); retrying.", reg.ConsumerType.FullName, reg.Stream, attempt);
                await Task.Delay(StreamBusDelays.ExponentialCapped(attempt, TimeSpan.FromMilliseconds(200), TimeSpan.FromSeconds(30), 2.0, TimeSpan.FromSeconds(1)), ct).ConfigureAwait(false);
            }
        }
    }
```

(`_scopes!` is safe: StartAsync throws when registrations exist without a factory. `reg.Options with { Reference }` — record copy, original untouched.)

- [ ] **Step 5: Verify**

```powershell
dotnet build src/StreamBus/StreamBus.csproj -c Release
dotnet test tests/StreamBus.UnitTests/StreamBus.UnitTests.csproj -c Release
```

Expected: build clean; existing suite green (no typed registrations exist by default, so hosted behavior is unchanged).

- [ ] **Step 6: Commit (with human approval)**

```powershell
git add src/StreamBus/Hosting.cs
git commit -m "feat: add typed consumer registration with hosted startup"
```

---

### Task 3: Unit tests — serializer, mapping, store, wrapper, classification

**Files:**
- Create: `tests/StreamBus.UnitTests/TypedConsumerTests.cs`
- Modify: `tests/StreamBus.UnitTests/HostingTests.cs` (3 conformance tests — follow that file's existing ServiceCollection/stub patterns)

**Interfaces:**
- Consumes: Task 1–2 APIs via `InternalsVisibleTo` (`IsStartupTransient`, `TypedConsumerHandler`, `ReferenceDerivation`).
- Produces: pinned contracts for Tasks 4–5.

Style rules (analyzers are build errors): no `.ConfigureAwait(false)` inside `[Fact]`s; explicit `TestContext.Current.CancellationToken`; `using RabbitMQ.Stream.Client;` only where client types appear.

- [ ] **Step 1: Implement `TypedConsumerTests.cs`**

```csharp
using Microsoft.Extensions.DependencyInjection;

namespace StreamBus.UnitTests;

public sealed class TypedConsumerTests
{
    private sealed record OrderEvent(string OrderId, int Seq);

    private sealed class OrderConsumer : IConsumer<OrderEvent>
    {
        public readonly List<OrderEvent> Got = new();
        public Task Consume(ConsumeContext<OrderEvent> context, CancellationToken ct)
        {
            lock (Got) Got.Add(context.Message);
            return Task.CompletedTask;
        }
    }

    [Fact]
    public void Serializer_round_trips()
    {
        var serializer = SystemTextJsonStreamSerializer.Default;
        var original = new OrderEvent("o-1", 7);
        var back = serializer.Deserialize<OrderEvent>(serializer.Serialize(original));
        Assert.Equal(original, back);
    }

    [Fact]
    public void Serializer_throws_on_malformed()
    {
        Assert.ThrowsAny<System.Text.Json.JsonException>(() =>
            SystemTextJsonStreamSerializer.Default.Deserialize<OrderEvent>(System.Text.Encoding.UTF8.GetBytes("{bad json")));
    }

    [Fact]
    public void Builder_defaults_match_record_defaults()
    {
        Assert.Equal(new ConsumeOptions(), new ConsumeOptionsBuilder().Build());
    }

    [Fact]
    public void References_differ_per_consumer_type_but_stable()
    {
        var a = ReferenceDerivation.Derive(typeof(DefaultBus), typeof(OrderConsumer), "orders");
        var b = ReferenceDerivation.Derive(typeof(OrderConsumerTests_Other), typeof(OrderConsumerTests_Other), "orders");
        Assert.NotEqual(a, b);
        Assert.Equal(a, ReferenceDerivation.Derive(typeof(DefaultBus), typeof(OrderConsumer), "orders"));
        Assert.True(System.Text.Encoding.UTF8.GetByteCount(a) <= 255);
    }

    private sealed class OrderConsumerTests_Other : IConsumer<OrderEvent>
    {
        public Task Consume(ConsumeContext<OrderEvent> context, CancellationToken ct) => Task.CompletedTask;
    }

    [Fact]
    public void Long_names_hash_within_cap()
    {
        var name = ReferenceDerivation.Derive(typeof(DefaultBus), typeof(OrderConsumer), new string('x', 300));
        Assert.True(System.Text.Encoding.UTF8.GetByteCount(name) <= 255);
        Assert.StartsWith("streambus-", name);
    }

    [Fact]
    public void Store_rejects_duplicates_but_allows_variants()
    {
        var store = new StreamBusConsumerStore();
        var options = new ConsumeOptionsBuilder().Build();
        store.Add(typeof(DefaultBus), typeof(OrderEvent), typeof(OrderConsumer), "orders", options);
        Assert.Throws<StreamBusConfigurationException>(() =>
            store.Add(typeof(DefaultBus), typeof(OrderEvent), typeof(OrderConsumer), "orders", options));
        store.Add(typeof(DefaultBus), typeof(OrderEvent), typeof(OrderConsumer), "other-stream", options);
        store.Add(typeof(DefaultBus), typeof(OrderEvent), typeof(OrderConsumerTests_Other), "orders", options);
        Assert.Equal(3, store.Registrations.Count);
    }

    [Fact]
    public void Explicit_service_lifetime_wins()
    {
        var services = new ServiceCollection();
        var singleton = new OrderConsumer();
        services.AddSingleton<IConsumer<OrderEvent>>(singleton);
        services.AddSingleton(singleton);
        services.AddStreamBusConsumer<OrderEvent, OrderConsumer>("orders");
        var sp = services.BuildServiceProvider();
        Assert.Same(singleton, sp.GetRequiredService<OrderConsumer>());
    }

    private sealed class FlakyConsumer : IConsumer<OrderEvent>
    {
        public static int Constructions;
        private readonly Dictionary<int, int> _attempts;
        public FlakyConsumer(Dictionary<int, int> attempts)
        {
            Interlocked.Increment(ref Constructions);
            _attempts = attempts;
        }
        public Task Consume(ConsumeContext<OrderEvent> context, CancellationToken ct)
        {
            var seq = context.Message.Seq;
            int attempt;
            lock (_attempts)
            {
                attempt = _attempts.TryGetValue(seq, out var n) ? n + 1 : 1;
                _attempts[seq] = attempt;
            }
            if (seq == 2 && attempt < 3) throw new InvalidOperationException("flaky 2");
            return Task.CompletedTask;
        }
    }

    private static ConsumeContext RawOf(string body) => new()
    {
        Stream = "orders",
        Partition = 0,
        Offset = 0,
        RoutingValue = null,
        Body = System.Text.Encoding.UTF8.GetBytes(body),
        Metadata = new MessageMetadata(),
        CancellationToken = TestContext.Current.CancellationToken,
    };

    [Fact]
    public async Task Attempts_resolve_fresh_and_poison_translates()
    {
        var services = new ServiceCollection();
        var attempts = new Dictionary<int, int>();
        services.AddSingleton(attempts);
        services.AddTransient<FlakyConsumer>();
        await using var sp = services.BuildServiceProvider();
        var factory = sp.GetRequiredService<IServiceScopeFactory>();
        FlakyConsumer.Constructions = 0;
        var handler = TypedConsumerHandler.Create(typeof(OrderEvent), typeof(FlakyConsumer), factory, SystemTextJsonStreamSerializer.Default);
        var good = "{\"OrderId\":\"k\",\"Seq\":2}";
        await Assert.ThrowsAsync<InvalidOperationException>(() => handler(RawOf(good)));
        await Assert.ThrowsAsync<InvalidOperationException>(() => handler(RawOf(good)));
        await handler(RawOf(good));
        Assert.Equal(3, FlakyConsumer.Constructions);
        Assert.Equal(3, attempts[2]);
        var poison = await Assert.ThrowsAsync<StreamBusDeserializationException>(() => handler(RawOf("{bad json")));
        Assert.Equal("orders", poison.Stream);
        Assert.Equal(typeof(OrderEvent), poison.MessageType);
        var nil = await Assert.ThrowsAsync<StreamBusDeserializationException>(() => handler(RawOf("null")));
        Assert.Contains("null", nil.InnerException?.Message ?? nil.Message, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void Startup_transient_matrix()
    {
        Assert.True(StreamBusHostedService.IsStartupTransient(new StreamBusNotReadyException("x")));
        Assert.True(StreamBusHostedService.IsStartupTransient(new StreamBusStreamNotFoundException("x")));
        Assert.False(StreamBusHostedService.IsStartupTransient(new StreamBusConfigurationException("x")));
        Assert.False(StreamBusHostedService.IsStartupTransient(new StreamBusFatalException("x")));
        Assert.False(StreamBusHostedService.IsStartupTransient(new StreamBusPublishException("x")));
        Assert.False(StreamBusHostedService.IsStartupTransient(new InvalidOperationException("x")));
    }

    [Fact]
    public async Task Missing_scope_factory_fails_fast()
    {
        var store = new StreamBusConfigStore();
        store.Add(typeof(DefaultBus), new StreamBusOptions { Vhost = "/", UserName = "a", Password = "b" });
        var consumers = new StreamBusConsumerStore();
        consumers.Add(typeof(DefaultBus), typeof(OrderEvent), typeof(OrderConsumer), "orders", new ConsumeOptionsBuilder().Build());
        var registry = new StreamBusRegistry(
            _ => throw new InvalidOperationException("no resolve"),
            _ => throw new InvalidOperationException("no supervisor"),
            null);
        var hosted = new StreamBusHostedService(store, registry, null, null, consumers);
        var ex = await Assert.ThrowsAsync<StreamBusConfigurationException>(() => hosted.StartAsync(TestContext.Current.CancellationToken));
        Assert.Contains("IServiceScopeFactory", ex.Message);
    }

    [Fact]
    public void Non_positive_timeout_rejected_at_config()
    {
        Assert.Throws<StreamBusConfigurationException>(() => new StreamBusHostOptionsBuilder().BlockStartupUntilReady(TimeSpan.Zero));
        Assert.Throws<StreamBusConfigurationException>(() => new StreamBusHostOptionsBuilder().BlockStartupUntilReady(TimeSpan.FromSeconds(-1)));
    }
}
```

(Check `StreamBusRegistry` ctor shape before writing: `(Func<string, StreamBusOptions>, Func<StreamBusOptions, StreamBusConnectionSupervisor>, ILoggerFactory?)` — mirror an existing construction from `RegistryTests`/`HostingTests` (grep first; adapt lambdas). `StreamBusConfigStore.Add(Type, StreamBusOptions)` ✓ per Hosting.cs. `StreamBusNotReadyException` ctor: `(string)`? Check `PublishExceptions.cs` — `StreamBusStreamNotFoundException(stream, ex?)`, `StreamBusNotReadyException(message?)` — verify signatures via grep before finalizing (executor reconciles). `IServiceScopeFactory` needs `Microsoft.Extensions.DependencyInjection` using ✓ present. `FlakyConsumer` needs the attempts dict registered — `services.AddSingleton(attempts)` registers as `Dictionary<int,int>`; ctor takes that exact type ✓. `IServiceScopeFactory` resolvable from any provider ✓. `await using var sp` — ServiceProvider is IAsyncDisposable ✓.)

- [ ] **Step 2: Hosting conformance tests in `HostingTests.cs`** (follow that file's existing ServiceCollection patterns; 3 tests)

```csharp
    [Fact]
    public async Task Guest_at_startup_fails_with_actionable_message()
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddStreamBus("/", o => o.Credentials("guest", "guest"));
        await using var sp = services.BuildServiceProvider();
        var hosted = sp.GetServices<IHostedService>().OfType<StreamBusHostedService>().Single();
        var ex = await Assert.ThrowsAsync<StreamBusConfigurationException>(() => hosted.StartAsync(TestContext.Current.CancellationToken));
        Assert.Contains("guest", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void Same_vhost_union_declarations_allowed()
    {
        var store = new StreamBusConfigStore();
        var a = new StreamBusOptions { Vhost = "/", UserName = "u", Password = "p", Topology = new StreamTopologyDeclarations { Streams = new[] { new StreamDeclaration { Name = "a" } } } };
        var b = new StreamBusOptions { Vhost = "/", UserName = "u", Password = "p", Topology = new StreamTopologyDeclarations { SuperStreams = new[] { new SuperStreamDeclaration { Name = "b", Partitions = 2 } } } };
        store.Add(typeof(DefaultBus), a);
        store.Add(typeof(OrdersBus), b);
        Assert.Equal(2, store.Registrations.Count);
    }
```

(`OrdersBus` reuses the marker type `HostingTests.cs` already defines. `StreamTopologyDeclarations` record init with `IReadOnlyList` — `new[] {...}` works ✓.)

- [ ] **Step 3: Verify**

```powershell
dotnet test tests/StreamBus.UnitTests/StreamBus.UnitTests.csproj -c Release
```

Expected: all green (160 existing + new), output pristine.

- [ ] **Step 4: Commit (with human approval)**

```powershell
git add tests/StreamBus.UnitTests/TypedConsumerTests.cs tests/StreamBus.UnitTests/HostingTests.cs
git commit -m "test: add typed consumer, serializer, and hosting unit tests"
```

---

### Task 4: Typed-path integration

**Files:**
- Create: `tests/StreamBus.IntegrationTests/TypedConsumerIntegrationTests.cs`

**Interfaces:**
- Consumes: `AddStreamBusConsumer` surface (Task 2), `SystemTextJsonStreamSerializer.Default` for publishing matching bytes, `BusInstance`-style harness (extended with service-configuration hook).
- Produces: FR-2.1/FR-2.8 + ordering evidence; startup-path coverage in Task 5.

Harness (mirror `RetryIntegrationTests`/`TopologyIntegrationTests` idioms; the configuration hook is the only addition):

```csharp
using System.Text;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using RabbitMQ.Stream.Client;

namespace StreamBus.IntegrationTests;

public sealed class TypedConsumerIntegrationTests : IAsyncDisposable
{
    private static string User() => Environment.GetEnvironmentVariable("STREAMBUS_USER") ?? "app";
    private static string Password() => Environment.GetEnvironmentVariable("STREAMBUS_PASSWORD") ?? "password";
    private readonly List<string> _streams = new();
    private readonly List<string> _superStreams = new();

    private async Task<StreamSystem> ConnectAdminAsync()
        => await StreamSystem.Create(new StreamSystemConfig
        {
            UserName = User(),
            Password = Password(),
            VirtualHost = "/",
            Endpoints = new List<System.Net.EndPoint> { new System.Net.IPEndPoint(System.Net.IPAddress.Loopback, 5552) },
        });

    private static string Unique(string prefix) => $"{prefix}-{Guid.NewGuid():N}";

    public async ValueTask DisposeAsync()
    {
        var admin = await ConnectAdminAsync();
        try
        {
            foreach (var s in _streams) { try { await admin.DeleteStream(s); } catch { } }
            foreach (var s in _superStreams) { try { await admin.DeleteSuperStream(s); } catch { } }
        }
        finally { await admin.DisposeAsync(); }
    }

    private sealed record OrderEvent(string OrderId, int Seq);

    private sealed class BusInstance : IAsyncDisposable
    {
        public ServiceProvider Provider { get; private set; } = null!;
        public StreamBusHostedService Hosted { get; private set; } = null!;
        public IStreamBus Bus { get; private set; } = null!;
        public static async Task<BusInstance> StartAsync(Action<ServiceCollection>? configure = null, string vhost = "/")
        {
            var services = new ServiceCollection();
            services.AddLogging();
            services.AddStreamBus(vhost, o => o.Credentials(User(), Password()));
            configure?.Invoke(services);
            var sp = services.BuildServiceProvider();
            var hosted = sp.GetServices<IHostedService>().OfType<StreamBusHostedService>().Single();
            await hosted.StartAsync(TestContext.Current.CancellationToken);
            var bus = sp.GetRequiredService<IStreamBus>();
            Assert.Equal(BusReadiness.Ready, await bus.WaitUntilReadyAsync(ReadinessPolicy.UpTo(TimeSpan.FromSeconds(30)), TestContext.Current.CancellationToken));
            return new BusInstance { Provider = sp, Hosted = hosted, Bus = bus };
        }
        public async ValueTask DisposeAsync()
        {
            try { await Hosted.StopAsync(TestContext.Current.CancellationToken); }
            finally { await Provider.DisposeAsync(); }
        }
    }

    private static ReadOnlyMemory<byte> Body<T>(T message) => SystemTextJsonStreamSerializer.Default.Serialize(message);

    private static async Task WaitForAsync(Func<bool> done, TimeSpan timeout, string what)
    {
        var deadline = DateTime.UtcNow + timeout;
        while (!done())
        {
            if (DateTime.UtcNow >= deadline) Assert.Fail($"Timed out waiting for {what}.");
            await Task.Delay(100, TestContext.Current.CancellationToken);
        }
    }

    private sealed class OrderConsumer : IConsumer<OrderEvent>
    {
        private readonly List<(ConsumeContext<OrderEvent> Ctx, byte[] Raw)> _got;
        public OrderConsumer(List<(ConsumeContext<OrderEvent>, byte[])> got) => _got = got;
        public Task Consume(ConsumeContext<OrderEvent> context, CancellationToken ct)
        {
            lock (_got) _got.Add((context, context.Body.ToArray()));
            return Task.CompletedTask;
        }
    }
```

(Test asserts `SystemTextJsonStreamSerializer.Default.Serialize(ctx.Message)` equals `Raw` — byte-identical round trip through the broker.)

- [ ] **Step 1: Round-trip, super order, multi-bus, binding**

```csharp
    [Fact]
    public async Task Typed_round_trip_plain()
    {
        var ct = TestContext.Current.CancellationToken;
        var stream = Unique("typed-orders");
        _streams.Add(stream);
        var admin = await ConnectAdminAsync();
        await using var _ = admin;
        await admin.CreateStream(new StreamSpec(stream));
        var got = new List<(ConsumeContext<OrderEvent> Ctx, byte[] Raw)>();
        await using var inst = await BusInstance.StartAsync(s =>
            s.AddStreamBusConsumer<OrderEvent, OrderConsumer>(stream));
        await using var pub = await BusInstance.StartAsync();
        for (var seq = 0; seq < 3; seq++)
            await pub.Bus.PublishAsync(stream, Body(new OrderEvent("o", seq)), new PublishOptions(), ct);
        await pub.DisposeAsync();
        await WaitForAsync(() => { lock (got) return got.Count >= 3; }, TimeSpan.FromSeconds(30), "typed receipt");
        await Task.Delay(500, ct);
        lock (got)
        {
            Assert.Equal(3, got.Count);
            Assert.Equal(new[] { 0, 1, 2 }, got.Select(x => x.Ctx.Message.Seq).ToList());
            Assert.Equal(new[] { 0UL, 1UL, 2UL }, got.Select(x => x.Ctx.Offset).ToList());
            Assert.All(got, x => Assert.Equal(stream, x.Ctx.Stream));
            Assert.All(got, x => Assert.Equal(0, x.Ctx.Partition));
            Assert.All(got, x => Assert.Equal(SystemTextJsonStreamSerializer.Default.Serialize(x.Ctx.Message), x.Raw));
        }
    }
```

Wait — `OrderConsumer` needs the `got` list injected: `services.AddSingleton(got)` BEFORE `AddStreamBusConsumer` (TryAddTransient only registers the CONSUMER, not the list — the test registers `List<...>` as singleton itself; consumer ctor takes it ✓). The harness hook runs AFTER `AddStreamBus` — order: bus, then hook (consumer + list). AddStreamBusConsumer does TryAddTransient<OrderConsumer> (no list registration — test does `s.AddSingleton(got)` first). Specify in each test:

```csharp
        await using var inst = await BusInstance.StartAsync(s =>
        {
            s.AddSingleton(got);
            s.AddStreamBusConsumer<OrderEvent, OrderConsumer>(stream);
        });
```

(`AddSingleton(got)` infers `List<(ConsumeContext<OrderEvent>, byte[])>` ✓. Consumer ctor takes that exact type ✓.)

```csharp
    private sealed class BusA { }
    private sealed class BusB { }

    [Fact]
    public async Task Multi_bus_isolation()
    {
        var ct = TestContext.Current.CancellationToken;
        var streamA = Unique("typed-a");
        var streamB = Unique("typed-b");
        _streams.Add(streamA);
        _streams.Add(streamB);
        var admin = await ConnectAdminAsync();
        await using var _ = admin;
        await admin.CreateStream(new StreamSpec(streamA));
        await admin.CreateStream(new StreamSpec(streamB));
        var gotA = new List<(ConsumeContext<OrderEvent> Ctx, byte[] Raw)>();
        var gotB = new List<(ConsumeContext<OrderEvent> Ctx, byte[] Raw)>();
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddStreamBus<BusA>("/", o => o.Credentials(User(), Password()));
        services.AddStreamBus<BusB>("/", o => o.Credentials(User(), Password()));
        services.AddSingleton(gotA);
        // Two consumer TYPES (one per bus) so references stay distinct per FR-10.10.
        ...
    }
```
(One consumer type serves both buses — references differ by marker; capture shares one list with stream attribution.)

```csharp
    [Fact]
    public async Task Multi_bus_isolation()
    {
        // One consumer type serves both buses (references differ by marker);
        // capture shares one list and stream names attribute delivery per bus.
        var ct = TestContext.Current.CancellationToken;
        var streamA = Unique("typed-a");
        var streamB = Unique("typed-b");
        _streams.Add(streamA);
        _streams.Add(streamB);
        var admin = await ConnectAdminAsync();
        await using var _ = admin;
        await admin.CreateStream(new StreamSpec(streamA));
        await admin.CreateStream(new StreamSpec(streamB));
        var got = new List<(ConsumeContext<OrderEvent> Ctx, byte[] Raw)>();
        await using var inst = await BusInstance.StartAsync(s =>
        {
            s.AddStreamBus<BusA>("/", o => o.Credentials(User(), Password()));
            s.AddStreamBus<BusB>("/", o => o.Credentials(User(), Password()));
            s.AddSingleton(got);
            s.AddStreamBusConsumer<BusA, OrderEvent, OrderConsumer>(streamA);
            s.AddStreamBusConsumer<BusB, OrderEvent, OrderConsumer>(streamB);
        });
        await using var pub = await BusInstance.StartAsync();
        for (var seq = 0; seq < 3; seq++)
        {
            await pub.Bus.PublishAsync(streamA, Body(new OrderEvent($"a-{seq}", seq)), new PublishOptions(), ct);
            await pub.Bus.PublishAsync(streamB, Body(new OrderEvent($"b-{seq}", seq)), new PublishOptions(), ct);
        }
        await pub.DisposeAsync();
        await WaitForAsync(() => { lock (got) return got.Count >= 6; }, TimeSpan.FromSeconds(30), "multi-bus receipt");
        await Task.Delay(500, ct);
        lock (got)
        {
            Assert.Equal(6, got.Count);
            Assert.Equal(new[] { 0, 1, 2 }, got.Where(x => x.Ctx.Stream == streamA).Select(x => x.Ctx.Message.Seq).OrderBy(x => x).ToList());
            Assert.Equal(new[] { 0, 1, 2 }, got.Where(x => x.Ctx.Stream == streamB).Select(x => x.Ctx.Message.Seq).OrderBy(x => x).ToList());
            Assert.All(got.Where(x => x.Ctx.Stream == streamA), x => Assert.StartsWith("a-", x.Ctx.Message.OrderId));
            Assert.All(got.Where(x => x.Ctx.Stream == streamB), x => Assert.StartsWith("b-", x.Ctx.Message.OrderId));
        }
    }

    [Fact]
    public async Task Binding_is_explicit()
    {
        var ct = TestContext.Current.CancellationToken;
        var streamA = Unique("typed-a");
        var streamB = Unique("typed-b");
        _streams.Add(streamA);
        _streams.Add(streamB);
        var admin = await ConnectAdminAsync();
        await using var _ = admin;
        await admin.CreateStream(new StreamSpec(streamA));
        await admin.CreateStream(new StreamSpec(streamB));
        var got = new List<(ConsumeContext<OrderEvent> Ctx, byte[] Raw)>();
        await using var inst = await BusInstance.StartAsync(s =>
        {
            s.AddSingleton(got);
            s.AddStreamBusConsumer<OrderEvent, OrderConsumer>(streamA);
        });
        await using var pub = await BusInstance.StartAsync();
        await pub.Bus.PublishAsync(streamA, Body(new OrderEvent("a", 0)), new PublishOptions(), ct);
        await pub.Bus.PublishAsync(streamA, Body(new OrderEvent("a", 1)), new PublishOptions(), ct);
        await pub.Bus.PublishAsync(streamB, Body(new OrderEvent("b", 0)), new PublishOptions(), ct);
        await pub.Bus.PublishAsync(streamB, Body(new OrderEvent("b", 1)), new PublishOptions(), ct);
        await pub.DisposeAsync();
        await WaitForAsync(() => { lock (got) return got.Count >= 2; }, TimeSpan.FromSeconds(30), "bound-stream receipt");
        await Task.Delay(TimeSpan.FromSeconds(2), ct);
        lock (got)
        {
            Assert.Equal(2, got.Count);
            Assert.All(got, x => Assert.Equal(streamA, x.Ctx.Stream));
        }
    }

    [Fact]
    public async Task Typed_super_stream_ordered_per_key()
    {
        var ct = TestContext.Current.CancellationToken;
        var super = Unique("typed-super");
        _superStreams.Add(super);
        var admin = await ConnectAdminAsync();
        await using var _ = admin;
        await admin.CreateSuperStream(new PartitionsSuperStreamSpec(super, 3));
        var got = new List<(ConsumeContext<OrderEvent> Ctx, byte[] Raw)>();
        await using var inst = await BusInstance.StartAsync(s =>
        {
            s.AddSingleton(got);
            s.AddStreamBusConsumer<OrderEvent, OrderConsumer>(super, o => o.SuperStream());
        });
        await using var pub = await BusInstance.StartAsync();
        for (var seq = 0; seq < 12; seq++)
            await pub.Bus.PublishAsync(super, Body(new OrderEvent($"k{seq % 4}", seq)),
                new PublishOptions { IsSuperStream = true, RoutingKey = $"k{seq % 4}" }, ct);
        await pub.DisposeAsync();
        await WaitForAsync(() => { lock (got) return got.Count >= 12; }, TimeSpan.FromSeconds(60), "keyed receipt");
        await Task.Delay(500, ct);
        lock (got)
        {
            Assert.Equal(12, got.Count);
            foreach (var grp in got.GroupBy(x => x.Ctx.Message.OrderId))
                Assert.Equal(grp.Select(x => x.Ctx.Message.Seq).OrderBy(x => x).ToList(), grp.Select(x => x.Ctx.Message.Seq).ToList());
        }
    }
```

- [ ] **Step 2: Poison paths + retry order + mismatch loudness**

```csharp
    [Fact]
    public async Task Poison_skip_goes_to_error_stream()
    {
        var ct = TestContext.Current.CancellationToken;
        var stream = Unique("typed-src");
        var error = Unique("typed-src.error");
        _streams.Add(stream);
        _streams.Add(error);
        var admin = await ConnectAdminAsync();
        await using var _ = admin;
        await admin.CreateStream(new StreamSpec(stream));
        await admin.CreateStream(new StreamSpec(error) { MaxAge = TimeSpan.FromDays(7) });
```

```csharp
        var got = new List<(ConsumeContext<OrderEvent> Ctx, byte[] Raw)>();
        await using var inst = await BusInstance.StartAsync(s =>
        {
            s.AddSingleton(got);
            s.AddStreamBusConsumer<OrderEvent, OrderConsumer>(stream, o => o
                .Retry(new RetryPolicy { FailureMode = FailureMode.SkipAfterDeadline, MaxAttempts = 2, BaseDelay = TimeSpan.FromMilliseconds(10), MaxJitter = TimeSpan.Zero })
                .ErrorStream(new ErrorStreamOptions { Stream = error }));
        });
        await using var pub = await BusInstance.StartAsync();
        await pub.Bus.PublishAsync(stream, Encoding.UTF8.GetBytes("{bad json"), new PublishOptions(), ct);
        await pub.Bus.PublishAsync(stream, Body(new OrderEvent("ok", 1)), new PublishOptions(), ct);
        await pub.DisposeAsync();
        await WaitForAsync(() => { lock (got) return got.Count >= 1; }, TimeSpan.FromSeconds(30), "post-poison good message");
        Assert.Equal("ok", got[0].Ctx.Message.OrderId);
        using var reader = await inst.Bus.ConsumeAsync(error,
            new ConsumeOptions { Offset = ConsumeOffset.AlwaysFirst, Reference = Unique("read") },
            ctx => { lock (got) got.Add((null!, ctx.Body.ToArray())); return Task.CompletedTask; }, ct);
```


```csharp
        var errors = new List<byte[]>();
        using var reader = await inst.Bus.ConsumeAsync(error,
            new ConsumeOptions { Offset = ConsumeOffset.AlwaysFirst, Reference = Unique("read") },
            ctx => { lock (errors) errors.Add(ctx.Body.ToArray()); return Task.CompletedTask; }, ct);
        await WaitForAsync(() => { lock (errors) return errors.Count >= 1; }, TimeSpan.FromSeconds(30), "error handoff");
        Assert.Equal("{bad json", Encoding.UTF8.GetString(errors[0]));
    }
```

(Reader reference `Unique("read")` per read ✓. RetryPolicy/MaxJitter props per Phase 6 records ✓ (`BaseDelay`, `MaxJitter`, `MaxAttempts`, `FailureMode`).)

```csharp
    [Fact]
    public async Task Poison_strict_never_skips()
    {
        ...Strict default consumer; publish good(0), poison, good(1)...
        WaitFor first good; settle 5s; assert second good ABSENT (count stays 1) — bounded negative proving stall-not-skip.
    }

    [Fact]
    public async Task Retry_preserves_order_typed()
    {
        // DI-backed flaky consumer (attempts dict as singleton, like Task 3).
        var attempts = new Dictionary<int, int>();
        consumer class FlakyTypedConsumer(IConsumer<OrderEvent>) with dict ctor + throw-twice-on-seq-2 (define in file);
        services: AddSingleton(attempts) + AddStreamBusConsumer<OrderEvent, FlakyTypedConsumer>(stream, o => o.Retry(new RetryPolicy { BaseDelay = TimeSpan.FromMilliseconds(10), MaxJitter = TimeSpan.Zero }));
        publish 0..4; WaitFor 5 collected; assert ordered seqs + attempts[2] == 3.
    }
```

Collection for flaky: separate collecting consumer? Flaky CONSUMES (doesn't collect)... need results: make FlakyTypedConsumer ALSO record successes into a shared got list (second singleton). Two singletons (attempts dict + got list), consumer takes both. ✓ specify.

```csharp
    [Fact]
    public async Task Type_mismatch_is_loud()
    {
        // Same options both sides is a deployment obligation; a wire-type
        // mismatch must land in the error stream, never misread silently.
        ...Skip/MaxAttempts=1? MaxAttempts=1 means exactly one handler call then exhaustion (Phase 6 semantics ✓) + ErrorStream...
        publish raw {"OrderId":123,"Seq":1} (number for string → JsonException);
        assert error stream gets the exact bytes; assert typed consumer never received it (got stays empty — settle 2s).
    }
```

- [ ] **Step 3: Verify**

```powershell
dotnet test tests/StreamBus.IntegrationTests/StreamBus.IntegrationTests.csproj -c Release --filter "FullyQualifiedName~TypedConsumer"
```

Expected: all green against `localhost:5552`.

- [ ] **Step 4: Commit (with human approval)**

```powershell
git add tests/StreamBus.IntegrationTests/TypedConsumerIntegrationTests.cs
git commit -m "test: add typed consumer integration probes"
```

---

### Task 5: Startup integration + hosting conformance

**Files:**
- Create: `tests/StreamBus.IntegrationTests/TypedStartupIntegrationTests.cs`

**Interfaces:**
- Consumes: Task 2 startup paths, `PartitionsSuperStreamSpec`, `BusInstance`-with-hook pattern (copied into the new file — same shape as Task 4, plus topology config).
- Produces: startup/invalid/marker evidence + FR-8.9/8.11 conformance pins.

- [ ] **Step 1: Startup convergence, invalid options, unknown marker, two types**

```csharp
// (Same harness header as Task 4: User/Password/ConnectAdminAsync/Unique/DisposeAsync.
// BusInstance identical to Task 4's (AddStreamBus default + configure hook).
// Message + consumer types re-declared locally (test files do not share types).

    private sealed record OrderEvent(string OrderId, int Seq);

    private sealed class OrderConsumer : IConsumer<OrderEvent> { ...collecting... }

    private sealed class AuditConsumer : IConsumer<OrderEvent> { ...collecting into its own list type... }
```

(Both consumer classes share one `List<(string Who, int Seq)>` singleton, tagging entries with `nameof` for per-consumer attribution without extra service types.)

```csharp
    private sealed class OrderConsumer : IConsumer<OrderEvent>
    {
        private readonly List<(string Who, int Seq)> _got;
        public OrderConsumer(List<(string, int)> got) => _got = got;
        public Task Consume(ConsumeContext<OrderEvent> context, CancellationToken ct)
        {
            lock (_got) _got.Add((nameof(OrderConsumer), context.Message.Seq));
            return Task.CompletedTask;
        }
    }
    // AuditConsumer identical with nameof(AuditConsumer).
```

```csharp
    [Fact]
    public async Task Startup_with_topology_and_consumer_converges()
    {
        // Consumer registered BEFORE the bus (order independence) + stream declared
        // via Topology in the same config: everything converges after one Start.
        var ct = TestContext.Current.CancellationToken;
        var stream = Unique("typed-orders");
        _streams.Add(stream);
        var got = new List<(string Who, int Seq)>();
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddSingleton(got);
        services.AddStreamBusConsumer<OrderEvent, OrderConsumer>(stream);
        services.AddStreamBus("/", o =>
        {
            o.Credentials(User(), Password());
            o.Topology(t => t.DeclareStream(stream));
        });
        await using var sp = services.BuildServiceProvider();
        var hosted = sp.GetServices<IHostedService>().OfType<StreamBusHostedService>().Single();
        await hosted.StartAsync(ct);
        try
        {
            var bus = sp.GetRequiredService<IStreamBus>();
            Assert.Equal(BusReadiness.Ready, await bus.WaitUntilReadyAsync(ReadinessPolicy.UpTo(TimeSpan.FromSeconds(30)), ct));
            await bus.PublishAsync(stream, Body(new OrderEvent("o", 0)), new PublishOptions(), ct);
            await WaitForAsync(() => { lock (got) return got.Count >= 1; }, TimeSpan.FromSeconds(30), "typed receipt after startup");
            lock (got) Assert.Single(got);
        }
        finally
        {
            await hosted.StopAsync(ct);
        }
    }
```

(`Body<T>` helper + `WaitForAsync` copied in-file. `AddStreamBusConsumer` before `AddStreamBus` — order independence pinned by arrangement ✓.)

```csharp
    [Fact]
    public async Task Invalid_options_fail_blocking_startup()
    {
        var ct = ...; var stream = Unique(...); (admin creates it — else NotFound masks the config error! create via admin first.)
        ...services: AddStreamBus + AddStreamBusConsumer<OrderEvent, OrderConsumer>(stream, o => o.Retry(new RetryPolicy { FailureMode = FailureMode.SkipAfterDeadline, MaxAttempts = 2 })) // missing ErrorStream → configuration error
        + BlockStartupUntilReady(30s);
        StartAsync → ThrowsAsync<StreamBusConfigurationException> (message contains "ErrorStream");
        StopAsync (idempotent after failed start — disposes supervisors and cancels startup background work; consumer tasks faulted; Stop must not throw: assert no-throw via plain await).
    }

    private sealed class GhostBus { }

    [Fact]
    public async Task Unknown_marker_fails_startup_in_both_modes()
    {
        // Non-blocking: the marker resolution runs synchronously in StartAsync.
        ...services: AddStreamBus default + AddStreamBusConsumer<GhostBus, OrderEvent, OrderConsumer>(stream);
        non-blocking StartAsync → ThrowsAsync<StreamBusConfigurationException> (contains "GhostBus");
        // Blocking: same expectation through the bounded path.
        ...second services with BlockStartupUntilReady → same throw (fast, not after 30s — assert duration? no, just the type/message).
    }
```


```csharp
    [Fact]
    public async Task Two_consumer_types_each_receive_everything()
    {
        ...stream created; got shared List<(Who,Seq)>;
        services: AddStreamBus + AddSingleton(got) + AddStreamBusConsumer<OrderEvent, OrderConsumer>(stream) + AddStreamBusConsumer<OrderEvent, AuditConsumer>(stream);
        Start; publish 0..3; WaitFor 8; assert each Who has {0,1,2,3} (distinct references → no split).
    }
```

- [ ] **Step 2: Verify**

```powershell
dotnet test tests/StreamBus.IntegrationTests/StreamBus.IntegrationTests.csproj -c Release --filter "FullyQualifiedName~TypedStartup"
```

Expected: all green.

- [ ] **Step 3: Commit (with human approval)**

```powershell
git add tests/StreamBus.IntegrationTests/TypedStartupIntegrationTests.cs
git commit -m "test: add typed startup and hosting integration probes"
```

---

## Follow-ups for Phase 10+ (tracked, no code in this phase)

- Scope-per-attempt means a poison message under `Strict` holds one DI scope open indefinitely while retrying — by design (bounded resource: one scope per stalled partition), visible via `retry_blocked`.
- `ConsumeContext<T>.Message` for `T` with parameterized constructors deserializes per `System.Text.Json` rules (case-sensitive, no polymorphism by default) — migration to shared options is a deployment concern, documented in the mismatch test.
- Same (marker, message, consumer, stream) registered twice is rejected; every other combination is independent — including the same consumer class on two streams (two subscriptions).
- G-3 stays open (Phase 8); NFR-1–NFR-7 verify in Phase 10, not here.

---

## Self-Review

- Spec coverage: FR-2.1 (Tasks 2 registration both markers + explicit binding, 4 binding/multi-bus/startup tests, 5 startup/two-types tests), FR-2.8 (Tasks 1 serializer + null-guard, 3 malformed/null/resolution tests, 4 poison/mismatch/order tests), FR-8.9 (existing supervisor validation + Task 3 guest-startup pin — wait, I placed guest in HostingTests via Task 3 Step 2 ✓), FR-8.11 (existing modes + Task 3 timeout-validation pin + union pin), FR-10.10 (Tasks 1 three-arg Derive, 3 distinctness/cap tests, 5 two-types test).
- Placeholder scan: no TBD/TODO; every code step ships complete source; no "similar to Task N" (harness/helpers copied verbatim per file); the two `Assert.Skip`-free conditional paths need none (no environment-gated tests this phase — broker is the only external, required).
- Type consistency: `AddStreamBusConsumer<TMessage,TConsumer>` / `<TBus,TMessage,TConsumer>` (Tasks 2/3/4/5 match); `TypedConsumerHandler.Create(messageType, consumerType, scopes, serializer)` + private `CreateTyped<T,TConsumer>` (Tasks 2/3 match); `Derive(busType, consumerType, stream)` (Tasks 1/2/3 match); `ConsumeOptionsBuilder.*` setters → `Build()` (Tasks 2/3/4 match); `IsStartupTransient` (Tasks 2/3 match); `StreamBusOptions.Serializer` + both `Serializer(...)` overloads (Tasks 1/2 match); `IConsumer<T>.Consume(ConsumeContext<T>, CancellationToken)` (Tasks 1/2/3/4 match).
- Review Focus: each line pinned (T4 Strict test, T3 distinctness + T4 two-types, T3 resolution-count, T4 mismatch + docs note in Follow-ups, T5 marker test).
