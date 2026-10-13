# StreamBus Phase 4 (Producer Registry) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Implement Phase 4 producer registry: plain- and super-stream publishing with publisher confirms, producer reuse, per-call-site readiness, and the tracked Phase 3 follow-up of physical per-vhost `StreamSystem` sharing.

**Architecture:** The registry holds one `StreamBusConnectionSupervisor` per vhost (shared by all bus shells on that vhost) plus one producer cache per vhost. `PublishAsync` applies the call-site `ReadinessPolicy` first (fail-fast by default), then sends through a cached `IStreamProducerHandle` (one client producer per stream; one super-stream producer per super stream with per-call routing via a `ConditionalWeakTable`), bounding only the confirm wait with `PublishOptions.Timeout` and invalidating the cached handle on transient or timed-out sends.

**Tech Stack:** .NET 10 (`net10.0`), C# latest, `RabbitMQ.Stream.Client` 1.12.1 (same pins as Phase 3 — no new packages), xUnit v3, `StreamBus.slnx`, Central Package Management.

**Spec:** `docs/2026-10-08-streambus-design.md` (§2.1 one system per vhost, §2.2 pooling, §3 readiness, §8 no auto-create, §9 `PublishAsync`/`PublishOptions`) and `docs/2026-10-08-streambus-requirements.md` (FR-1.1–FR-1.5, FR-8.1, FR-8.6). Phase table: design §10 row 4.

## Global Constraints

- `TargetFramework` exactly `net10.0`; `Nullable`/`ImplicitUsings` enable; `TreatWarningsAsErrors` true; `ManagePackageVersionsCentrally` true; `IsTestingPlatformApplication` false (all in `Directory.Build.props`, unchanged).
- `RabbitMQ.Stream.Client` exactly `1.12.1`; `Microsoft.Extensions.Hosting` / `Diagnostics.HealthChecks` / `Logging.Abstractions` exactly `10.0.12`; `xunit.v3` 4.0.2, `xunit.runner.visualstudio` 4.0.1, `Microsoft.NET.Test.Sdk` 18.10.1.
- Library root namespace `StreamBus`; never `RabbitMQ.Stream.*`.
- Publishing NEVER auto-creates a stream (FR-1.3); the library contains no `CreateStream` call outside tests.
- `PublishOptions.Timeout` is the publisher-confirm timeout only, never the readiness wait (FR-8.6).
- `DeduplicationName` is deferred to Phase 7 (effectively-once stable producer names); it does not exist in this phase.
- Every commit step requires explicit human approval before running; never push, never amend without permission.

## Review Focus

- Publish to a missing stream fails with `StreamBusStreamNotFoundException` naming the stream AND the stream still does not exist afterwards (FR-1.2/FR-1.3). Pinned by Task 6 integration test asserting `StreamExists == false` after the failure.
- A failed publisher confirm raises `StreamBusPublishException` carrying stream + status — never swallowed (FR-1.4). Pinned by Task 5 unit test with a failing-confirmation stub.
- `Timeout` bounds only the confirm wait: a slow confirm yields `StreamBusTimeoutException` ("outcome unknown") while the bus stays `Ready`, independent of the readiness wait (FR-8.6). Pinned by Task 5 unit test with a late-confirming stub.
- N publishes to one stream create exactly one client producer (FR-1.5). Pinned by Task 5 counting-stub test.
- Same-stream publishes with different routing keys all flow through one super-stream producer and land on partitions consistent with their keys (FR-1.1 on super streams). Pinned by Task 6 integration test reading partitions back with test-only raw consumers.

---

## File Structure

New files (all under existing projects; no new packages):

- `src/StreamBus/PublishOptions.cs` — `PublishOptions` record (`ReadinessPolicy` default fail-fast, `IsSuperStream` default false, `RoutingKey` default null, `Timeout` default 10 s).
- `src/StreamBus/PublishExceptions.cs` — `StreamBusPublishException`, `StreamBusPublishTransientException`, `StreamBusStreamNotFoundException`, `StreamBusNotReadyException`.
- `src/StreamBus/ProducerHandles.cs` — `IStreamProducerHandle`, `StreamProducerHandle` (plain), `SuperStreamProducerHandle` (routing table + func), client `Message` construction.
- `src/StreamBus/ProducerRegistry.cs` — `StreamProducerRegistry` (per-vhost cache, readiness mapping, confirm-timeout wrapper, transient invalidation).
- `tests/StreamBus.UnitTests/PublishTests.cs` — broker-free tests via stubbed `IStreamSystemHandle`.
- `tests/StreamBus.IntegrationTests/PublishIntegrationTests.cs` — real-broker tests (stream provisioning/cleanup via test-only direct client use).

Modified Phase 3 files (same branch lineage; existing suites must stay green):

- `src/StreamBus/Connector.cs` — extend `IStreamSystemHandle` with `CreatePlainProducerAsync` / `CreateSuperStreamProducerAsync`; implement in `StreamSystemHandle`.
- `src/StreamBus/Registry.cs` — one supervisor per vhost shared by all bus shells on it; per-vhost producer registries; new internal accessor for the producer registry.
- `src/StreamBus/Hosting.cs` — `StartAsync` iterates distinct per-vhost supervisors (behavior identical, fewer instances).
- `src/StreamBus/Bus.cs` — add `PublishAsync` to `IStreamBus`, `IStreamBus<TBus>`, both implementations (delegate to the vhost's producer registry).
- `src/StreamBus/Options.cs` — no change (connection options reused for producer pool mapping).

Explicitly out of Phase 4 (deferred, not stubs): `PublishResultAsync` / `DeduplicationName` (Phase 7), `ConsumeAsync` / `IConsumer<T>` (Phase 5), retry/error stream (Phase 6), topology `Ensure*` (Phase 8), typed serializer (Phase 9).

Recorded rulings (spec as authority; bind all tasks): `PublishAsync(string stream, ReadOnlyMemory<byte> body, PublishOptions? options = null, CancellationToken ct = default)` returns `Task`; default policy is fail-fast (no hidden waits); default confirm timeout is 10 s; `IsSuperStream` is explicit per call (default false; `RoutingKey` required iff true, ignored iff false — no magic stream-type detection); null/empty stream and null options throw BCL `ArgumentException`/`ArgumentNullException` (idiomatic guard clauses); non-positive `Timeout` or missing super-stream `RoutingKey` throw `StreamBusConfigurationException` — all before any broker contact; non-ready `FailFast`/`TimedOut` throws `StreamBusNotReadyException`, non-ready `Fatal` throws `StreamBusFatalException`; send-side non-fatal client errors invalidate the cached handle and throw `StreamBusPublishTransientException`; confirm timeout invalidates the handle and throws `StreamBusTimeoutException` ("outcome unknown"); creation failures with `ResponseCode.StreamDoesNotExist` throw `StreamBusStreamNotFoundException`; other creation failures throw `StreamBusPublishException` wrapping the client error; failed/cancelled creations are never cached; caller cancellation propagates as `OperationCanceledException`.

As-probed 1.12.1 facts (Task 2, binding): no confirming `Send` exists — sends return `ValueTask`, confirms arrive via `ConfirmHandler` keyed by publishing id (bridged inside the handle by an internal per-producer tracker with unique ids from 1); the confirmation status member is `Code`, not `Status`; `Message` is a class with a `byte[]` constructor; `RawProducerConfig`/`RawSuperStreamProducerConfig` take the stream name in the ctor; `IProducer` dispose is synchronous. Later phases read `SendAsync` only — never a direct confirming send. Phase 7 note: the tracker generates ids internally today; effectively-once (publishing-id = source offset, `GetLastPublishingId` exists on `IProducer`) will need explicit-id support in the handle — not this phase.

---

### Task 1: Per-vhost supervisor sharing (tracked Phase 3 follow-up)

**Files:**
- Create: `src/StreamBus/AssemblyInfo.cs`
- Modify: `src/StreamBus/Registry.cs`
- Modify: `src/StreamBus/Hosting.cs`
- Test: `tests/StreamBus.UnitTests/RegistryTests.cs` (append inside the existing class; existing tests must pass unmodified)

**Interfaces:**
- Consumes: `StreamBusConnectionSupervisor` (StartAsync idempotent, State, WaitUntilReadyAsync), `StreamBusOptions`, `IStreamBusRegistry` + internal `GetOrCreateEntry` (Phase 3, unchanged signatures).
- Produces: one supervisor instance per vhost shared by all bus shells on it; `StreamBusRegistry` exposes per-vhost supervisor lookup for Task 3 (`internal StreamBusConnectionSupervisor SupervisorFor(string vhost)`).

- [ ] **Step 0: Create `src/StreamBus/AssemblyInfo.cs` (test visibility)**

```csharp
using System.Runtime.CompilerServices;

[assembly: InternalsVisibleTo("StreamBus.UnitTests")]
[assembly: InternalsVisibleTo("StreamBus.IntegrationTests")]
```

Internal registry/supervisor members are asserted from the unit-test assembly starting with this task's Step 3 — without this, nothing below compiles. Both test assemblies are named (integration included for future operator-hatch tests).

- [ ] **Step 1: Rework `Registry.cs` to per-vhost supervisors**

Replace the per-entry supervisor creation with a per-vhost supervisor map. Three exact changes (everything else in the file is untouched — note the real file uses explicit `IStreamBusRegistry.GetOrCreateEntry` forwarding to `GetOrCreateCore`, not a plain `GetOrCreateEntry` method):

(a) interface gains one member:

```csharp
internal BusEntry GetOrCreateEntry(Type busType, string vhost);
internal StreamBusConnectionSupervisor SupervisorFor(string vhost);
```

(b) new field plus core, placed beside `GetOrCreateCore`:

```csharp
private readonly ConcurrentDictionary<string, StreamBusConnectionSupervisor> _supervisors = new();

StreamBusConnectionSupervisor IStreamBusRegistry.SupervisorFor(string vhost)
    => SupervisorForCore(vhost);

internal StreamBusConnectionSupervisor SupervisorForCore(string vhost)
    => _supervisors.GetOrAdd(vhost, _ => _createSupervisor(_resolveOptions(vhost)));
```

(c) inside `GetOrCreateCore`'s `GetOrAdd` factory, replace

```csharp
var options = _resolveOptions(vhost);
var supervisor = _createSupervisor(options);
```

with

```csharp
var supervisor = SupervisorForCore(vhost);
```

`_resolveOptions`/`_createSupervisor` now run at most once per vhost. Unknown vhosts still surface `StreamBusConfigurationException` naming the vhost via `ForVhost` — unchanged.

- [ ] **Step 2: Adjust `Hosting.cs` `StartAsync` to distinct per-vhost supervisors**

Surgical change to the current `StartAsync` only (fields, constructor, `StopAsync` disposal, and the timeout/blocking/fatal block stay byte-identical; there is no `StartAll` method anymore and no `(StreamBusRegistry)` cast anywhere — do not reintroduce one). Replace the per-registration loop

```csharp
foreach (var (busType, options) in _store.Registrations)
{
    var entry = _registry.GetOrCreateEntry(busType, options.Vhost);
    await entry.Supervisor.StartAsync(ct).ConfigureAwait(false);
    lock (_gate) _started.Add(entry.Supervisor);
    supervisors.Add((options.Vhost, options.Hosting.StartupTimeout, entry.Supervisor));
}
```

with

```csharp
foreach (var (busType, options) in _store.Registrations)
    _registry.GetOrCreateEntry(busType, options.Vhost);
foreach (var vhost in _store.Registrations.Select(r => r.Options.Vhost).Distinct())
{
    var supervisor = _registry.SupervisorFor(vhost);
    await supervisor.StartAsync(ct).ConfigureAwait(false);
    lock (_gate) _started.Add(supervisor);
    var timeout = _store.Registrations.First(r => r.Options.Vhost == vhost).Options.Hosting.StartupTimeout;
    supervisors.Add((vhost, timeout, supervisor));
}
```

(`_registry.SupervisorFor` is the new internal interface member from Step 1 — same assembly, no cast.) Then delete the two-line comment `// One supervisor per bus+vhost today; no physical per-vhost sharing.` / `// Sharing a single StreamSystem per vhost is an explicit Phase 4 follow-up.` (sharing is now real). Everything after it (non-blocking early return, timeouts, linked CTS, wait loop, fatal throw) is untouched.

- [ ] **Step 3: Append sharing tests inside the existing `RegistryTests` class**

```csharp
[Fact]
public async Task Buses_on_one_vhost_share_one_supervisor()
{
    var registry = Create(out _);
    var a = registry.GetOrCreateBus("/");
    var b = registry.GetOrCreateBus<OrdersBus>("/");
    Assert.NotSame(a, (IStreamBus)b);
    var busRegistry = (IStreamBusRegistry)registry;
    var entryA = busRegistry.GetOrCreateEntry(typeof(DefaultBus), "/");
    var entryB = busRegistry.GetOrCreateEntry(typeof(OrdersBus), "/");
    Assert.Same(entryA.Supervisor, entryB.Supervisor);
    await Task.CompletedTask;
}

[Fact]
public void Different_vhosts_get_different_supervisors()
{
    var registry = Create(out _);
    registry.GetOrCreateBus("/");
    registry.GetOrCreateBus("other-vh");
    var busRegistry = (IStreamBusRegistry)registry;
    var s1 = busRegistry.SupervisorFor("/");
    var s2 = busRegistry.SupervisorFor("other-vh");
    Assert.NotSame(s1, s2);
}
```

Internal members are reachable through the `IStreamBusRegistry` interface thanks to Step 0's `InternalsVisibleTo` (explicit interface implementations are not visible through the concrete type — use the interface cast as shown). `OrdersBus` is the existing private nested marker in that file — reuse it, do not redeclare. The `Create(out _)` helper is the existing one (resolves `Vhost` with app/secret, `ScriptedConnector` never started) — reuse unchanged. `await Task.CompletedTask` keeps the async test warning-free under `TreatWarningsAsErrors`.

- [ ] **Step 4: Verify**

```powershell
dotnet test tests/StreamBus.UnitTests/StreamBus.UnitTests.csproj -c Release
```

Expected: all green including the 3 pre-existing registry tests unmodified (identity, coexistence, `DescribeStates`) plus the 2 new sharing tests.

- [ ] **Step 5: Commit (with human approval)**

```powershell
git add src/StreamBus/AssemblyInfo.cs src/StreamBus/Registry.cs src/StreamBus/Hosting.cs tests/StreamBus.UnitTests/RegistryTests.cs
git commit -m "feat: share one connection supervisor per vhost across buses"
```

---

### Task 2: Producer handles and client API probe

**Files:**
- Modify: `src/StreamBus/Connector.cs` (extend `IStreamSystemHandle`, implement in `StreamSystemHandle`)
- Create: `src/StreamBus/ProducerHandles.cs`
- Modify: `tests/StreamBus.UnitTests/ConnectionSupervisorTests.cs` (existing `StubHandle` gains the two new factory members)

**Interfaces:**
- Consumes: `StreamSystem` (`CreateRawProducer`, `CreateRawSuperStreamProducer`), `StreamBusConnectionOptions` (pool mapping already built), `StreamBusConfigurationException` (validation).
- Produces: `IStreamSystemHandle.CreatePlainProducerAsync(string, CancellationToken)` / `CreateSuperStreamProducerAsync(string, CancellationToken)` returning `IStreamProducerHandle`; `IStreamProducerHandle.SendAsync(ReadOnlyMemory<byte> body, string? routingKey, CancellationToken ct)` returning `Task` (throws `StreamBusPublishException` on failed confirm; raw client errors propagate).

- [ ] **Step 1: Probe the 1.12.1 producer API (no guessing)**

Run in PowerShell (read-only; finds the exact members to call):

```powershell
$dll = Get-ChildItem "$env:USERPROFILE/.nuget/packages/rabbitmq.stream.client/1.12.1/lib" -Recurse -Filter RabbitMQ.Stream.Client.dll | Select-Object -First 1 -ExpandProperty FullName
$asm = [Reflection.Assembly]::LoadFrom($dll)
$asm.GetType("RabbitMQ.Stream.Client.IProducer").GetMethods() | Select-Object Name, ReturnType, @{n="Params";e={($_.GetParameters() | ForEach-Object { "$($_.ParameterType.Name) $($_.Name)" }) -join ", "}}
$asm.GetType("RabbitMQ.Stream.Client.RawProducerConfig").GetProperties() | Select-Object Name, PropertyType
$asm.GetType("RabbitMQ.Stream.Client.ISuperStreamProducer").GetMethods() | Select-Object Name, ReturnType, @{n="Params";e={($_.GetParameters() | ForEach-Object { "$($_.ParameterType.Name) $($_.Name)" }) -join ", "}}
$asm.GetType("RabbitMQ.Stream.Client.RawSuperStreamProducerConfig").GetProperties() | Select-Object Name, PropertyType
$asm.GetType("RabbitMQ.Stream.Client.Message").GetProperties() | Select-Object Name, PropertyType
$asm.GetType("RabbitMQ.Stream.Client.Message").IsClass
$asm.GetType("RabbitMQ.Stream.Client.Message").GetConstructors() | Select-Object @{n="Params";e={($_.GetParameters() | ForEach-Object { "$($_.ParameterType.Name) $($_.Name)" }) -join ", "}}
$asm.GetType("RabbitMQ.Stream.Client.Confirmation").GetProperties() | Select-Object Name, PropertyType
$asm.GetType("RabbitMQ.Stream.Client.CreateProducerException").GetProperties() | Select-Object Name, PropertyType
$asm.GetType("RabbitMQ.Stream.Client.ResponseCode").GetEnumNames()
```

Record the output in the task report. Then write the code below against what the probe shows, with these bindings: use the confirming `Send` overload (the one returning `Task<Confirmation>`; prefer `(ulong publishingId, Message message)` when several confirming overloads exist, passing `0` — no dedup in this phase); set `MessagesBufferSize = 100` explicitly on both config types (safe at or above the client's minimum regardless of defaults); always set `Reference` explicitly (`$"streambus-pub-{stream}"` plain, `$"streambus-pub-{stream}"` super); do not set `ClientProvidedName` (leave the client default); construct `Message` with the probed `byte[]` constructor when one exists, else default-construct and set the `Data` property (`new Message { Data = body.ToArray() }` shape adapted to the probed member names — copy once, the client retains the buffer past the call); confirm mapping is `Status == ResponseCode.Ok` else throw `StreamBusPublishException($"Publish to stream '{stream}' was not confirmed (status {Status}).")`; `RawSuperStreamProducerConfig.Routing` reads the per-call routing value (next step).

- [ ] **Step 2: Extend `Connector.cs` with producer factories**

```csharp
public interface IStreamSystemHandle : IAsyncDisposable
{
    bool IsClosed { get; }
    Task<IStreamProducerHandle> CreatePlainProducerAsync(string stream, CancellationToken ct);
    Task<IStreamProducerHandle> CreateSuperStreamProducerAsync(string stream, CancellationToken ct);
}
```

`StreamSystemConnector` is untouched. `StreamSystemHandle` implements both by calling `StreamSystem.CreateRawProducer(new RawProducerConfig(stream) { MessagesBufferSize = 100, Reference = $"streambus-pub-{stream}" })` and `StreamSystem.CreateRawSuperStreamProducer(new RawSuperStreamProducerConfig(stream) { MessagesBufferSize = 100, Reference = $"streambus-pub-{stream}", Routing = ... })` respectively (adapt constructor/property shape to the probe: if the config types take the stream name as a constructor argument use it, else set the `Stream`/`SuperStream` property; if `Reference` is constructor-required pass it there). `CreateProducerException` from either call propagates unwrapped (the registry maps `StreamDoesNotExist` in Task 3). `ct` is honored before the create call (`ct.ThrowIfCancellationRequested()`); a cancelled create is never returned.

- [ ] **Step 3: Create `ProducerHandles.cs`**

```csharp
using System.Runtime.CompilerServices;
using RabbitMQ.Stream.Client;

namespace StreamBus;

public interface IStreamProducerHandle : IAsyncDisposable
{
    Task SendAsync(ReadOnlyMemory<byte> body, string? routingKey, CancellationToken ct);
}

internal sealed class StreamProducerHandle : IStreamProducerHandle
{
    private readonly IProducer _producer;
    private readonly string _stream;
    public StreamProducerHandle(IProducer producer, string stream) { _producer = producer; _stream = stream; }
    public async Task SendAsync(ReadOnlyMemory<byte> body, string? routingKey, CancellationToken ct)
    {
        ct.ThrowIfCancellationRequested();
        var confirmation = await _producer.Send(0, new Message(body.ToArray())).ConfigureAwait(false);
        if (confirmation.Status != ResponseCode.Ok)
            throw new StreamBusPublishException($"Publish to stream '{_stream}' was not confirmed (status {confirmation.Status}).");
    }
    public ValueTask DisposeAsync() => _producer.DisposeAsync();
}

internal sealed class SuperStreamProducerHandle : IStreamProducerHandle
{
    private static readonly ConditionalWeakTable<Message, string> RoutingTable = new();
    private readonly ISuperStreamProducer _producer;
    private readonly string _stream;
    public SuperStreamProducerHandle(ISuperStreamProducer producer, string stream) { _producer = producer; _stream = stream; }
    public static string RoutingFor(Message message)
        => RoutingTable.TryGetValue(message, out var routing)
            ? routing
            : throw new InvalidOperationException($"No routing value was stamped for the outgoing message to '{typeof(SuperStreamProducerHandle)}' (internal error).");
    public async Task SendAsync(ReadOnlyMemory<byte> body, string? routingKey, CancellationToken ct)
    {
        ct.ThrowIfCancellationRequested();
        var message = new Message(body.ToArray());
        RoutingTable.Add(message, routingKey!);
        var confirmation = await _producer.Send(0, message).ConfigureAwait(false);
        if (confirmation.Status != ResponseCode.Ok)
            throw new StreamBusPublishException($"Publish to stream '{_stream}' was not confirmed (status {confirmation.Status}).");
        RoutingTable.Remove(message);
    }
    public ValueTask DisposeAsync() => _producer.DisposeAsync();
}
```

Adaptations bound to the probe (apply exactly as needed, report which): if `Message` is a value type (`IsClass == false`), STOP and report NEEDS_CONTEXT with the full probe output (the controller will supply the routing variant for that case — do not invent a struct-based design); if the super `Send` overload differs (e.g. `Send(Message)` without publishing id), call the confirming overload the probe shows; if `Confirmation` exposes the status under a different member name than `Status`, use the probed name; if `IProducer`/`ISuperStreamProducer` dispose is synchronous (`IDisposable`), implement `DisposeAsync` via `Dispose()` + `ValueTask.CompletedTask`. `routingKey!` is safe: the registry guarantees non-null routing for super sends (Task 3) — the `ThrowIfNull`-equivalent is that guarantee plus this internal throw.

- [ ] **Step 4: Update the existing test stub for the widened interface**

`tests/StreamBus.UnitTests/ConnectionSupervisorTests.cs` contains `file sealed class StubHandle : IStreamSystemHandle` with only `IsClosed` + `DisposeAsync` — it stops compiling the moment Step 2 widens the interface. Append both factory methods (never called in those tests — the supervisors there reach `Ready` and stop):

```csharp
public Task<IStreamProducerHandle> CreatePlainProducerAsync(string stream, CancellationToken ct)
    => throw new NotImplementedException("ConnectionSupervisorTests never publishes.");
public Task<IStreamProducerHandle> CreateSuperStreamProducerAsync(string stream, CancellationToken ct)
    => throw new NotImplementedException("ConnectionSupervisorTests never publishes.");
```

No other existing file implements `IStreamSystemHandle` (`RegistryTests` returns a never-completing task, `HostingTests` touches no handles) — grep `IStreamSystemHandle` under `tests/` after editing to confirm all implementers compile.

- [ ] **Step 5: Verify**

```powershell
dotnet build src/StreamBus/StreamBus.csproj -c Release
```

Expected: zero warnings/errors. No tests in this task (handles are exercised through the registry in Tasks 3/5/6).

- [ ] **Step 6: Commit (with human approval)**

```powershell
git add src/StreamBus/Connector.cs src/StreamBus/ProducerHandles.cs tests/StreamBus.UnitTests/ConnectionSupervisorTests.cs
git commit -m "feat: add stream producer handles with confirm mapping"
```

---

### Task 3: Producer registry (cache, readiness, timeout, invalidation)

**Files:**
- Create: `src/StreamBus/PublishOptions.cs`
- Create: `src/StreamBus/PublishExceptions.cs`
- Create: `src/StreamBus/ProducerRegistry.cs`
- Modify: `src/StreamBus/ConnectionSupervisor.cs` (add `CurrentHandle`)

**Interfaces:**
- Consumes: `StreamBusConnectionSupervisor` (State, WaitUntilReadyAsync; gains `CurrentHandle`), `IStreamSystemHandle` factories (Task 2), `StreamBusConfigurationException`/`StreamBusFatalException` (Phase 3), `StreamBusPublishException` (this task's `PublishExceptions.cs`).
- Produces: `StreamProducerRegistry` with `Task PublishAsync(string stream, ReadOnlyMemory<byte> body, PublishOptions options, CancellationToken ct)` for Task 4; `PublishOptions`; the four exception types for Tasks 4–6; per-vhost instances held by `StreamBusRegistry` (Task 4 wires).

(`AssemblyInfo.cs` already exists from Task 1 — do not recreate it.)

- [ ] **Step 1: Implement `PublishOptions.cs`** (needed here — `ProducerRegistry` below takes it)

```csharp
namespace StreamBus;

public sealed record PublishOptions
{
    public ReadinessPolicy ReadinessPolicy { get; init; } = ReadinessPolicy.FailFastPolicy;
    public bool IsSuperStream { get; init; } = false;
    public string? RoutingKey { get; init; }
    public TimeSpan Timeout { get; init; } = TimeSpan.FromSeconds(10);
}
```

- [ ] **Step 2: Implement `PublishExceptions.cs`** (needed here — `ProducerRegistry` below throws all four)

First delete the minimal `StreamBusPublishException` declaration from `src/StreamBus/ProducerHandles.cs` (Task 2 placed an identical-shape placeholder there marked with a handoff comment because the type had no home yet — leaving both breaks the build with a duplicate-definition error). Then create the canonical definitions:

```csharp
namespace StreamBus;

public sealed class StreamBusPublishException : Exception
{
    public StreamBusPublishException(string message, Exception? inner = null)
        : base(message, inner) { }
}

public sealed class StreamBusPublishTransientException : Exception
{
    public StreamBusPublishTransientException(string message, Exception? inner = null)
        : base(message, inner) { }
}

public sealed class StreamBusStreamNotFoundException : Exception
{
    public string Stream { get; }
    public StreamBusStreamNotFoundException(string stream, Exception? inner = null)
        : base($"Stream '{stream}' does not exist (publishing does not auto-create streams).", inner)
    {
        Stream = stream;
    }
}

public sealed class StreamBusNotReadyException : Exception
{
    public StreamBusNotReadyException(string message)
        : base(message) { }
}
```

- [ ] **Step 3: Implement `ProducerRegistry.cs`**

```csharp
using System.Collections.Concurrent;
using RabbitMQ.Stream.Client;

namespace StreamBus;

internal sealed class StreamProducerRegistry
{
    private readonly StreamBusConnectionSupervisor _supervisor;
    private readonly string _vhost;
    private readonly ConcurrentDictionary<string, Task<IStreamProducerHandle>> _handles = new();

    public StreamProducerRegistry(StreamBusConnectionSupervisor supervisor, string vhost)
    {
        _supervisor = supervisor;
        _vhost = vhost;
    }

    public async Task PublishAsync(string stream, ReadOnlyMemory<byte> body, PublishOptions options, CancellationToken ct = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(stream);
        ArgumentNullException.ThrowIfNull(options);
        if (options.Timeout <= TimeSpan.Zero)
            throw new StreamBusConfigurationException("PublishOptions.Timeout must be positive.");
        if (options.IsSuperStream && string.IsNullOrWhiteSpace(options.RoutingKey))
            throw new StreamBusConfigurationException($"PublishOptions.RoutingKey is required for super stream '{stream}'.");
        var readiness = await _supervisor.WaitUntilReadyAsync(options.ReadinessPolicy, ct).ConfigureAwait(false);
        if (readiness == BusReadiness.Fatal)
            throw new StreamBusFatalException($"StreamBus for vhost '{_vhost}' is in Fatal state; publish to '{stream}' refused.");
        if (readiness != BusReadiness.Ready)
            throw new StreamBusNotReadyException($"StreamBus for vhost '{_vhost}' is not ready ({readiness}); publish to '{stream}' refused.");
        var handle = await GetOrCreateAsync(stream, options.IsSuperStream, ct).ConfigureAwait(false);
        using var timeoutCts = new CancellationTokenSource(options.Timeout);
        using var linked = CancellationTokenSource.CreateLinkedTokenSource(ct, timeoutCts.Token);
        try
        {
            await handle.SendAsync(body, options.RoutingKey, linked.Token).ConfigureAwait(false);
        }
        catch (StreamBusPublishException)
        {
            throw;
        }
        catch (OperationCanceledException) when (timeoutCts.IsCancellationRequested && !ct.IsCancellationRequested)
        {
            Invalidate(stream, options.IsSuperStream);
            throw new StreamBusTimeoutException($"Publish to stream '{stream}' was not confirmed within {options.Timeout} (outcome unknown).");
        }
        catch (Exception ex) when (!IsCallerCancel(ex, ct) && StreamBusErrorClassification.IsFatal(ex))
        {
            throw;
        }
        catch (Exception ex) when (!IsCallerCancel(ex, ct))
        {
            Invalidate(stream, options.IsSuperStream);
            throw new StreamBusPublishTransientException($"Publish to stream '{stream}' failed transiently; the cached producer was discarded.", ex);
        }
    }

    private static bool IsCallerCancel(Exception ex, CancellationToken ct)
        => ex is OperationCanceledException && ct.IsCancellationRequested;

    private static string Key(string stream, bool super) => super ? $"super:{stream}" : $"plain:{stream}";

    private void Invalidate(string stream, bool super)
        => _handles.TryRemove(Key(stream, super), out _);

    private async Task<IStreamProducerHandle> GetOrCreateAsync(string stream, bool super, CancellationToken ct)
    {
        var key = Key(stream, super);
        var task = _handles.GetOrAdd(key, _ => CreateAsync(stream, super, ct));
        try
        {
            return await task.ConfigureAwait(false);
        }
        catch
        {
            ((ICollection<KeyValuePair<string, Task<IStreamProducerHandle>>>)_handles).Remove(new KeyValuePair<string, Task<IStreamProducerHandle>>(key, task));
            throw;
        }
    }

    private async Task<IStreamProducerHandle> CreateAsync(string stream, bool super, CancellationToken ct)
    {
        var handle = _supervisor.CurrentHandle
            ?? throw new StreamBusNotReadyException($"StreamBus for vhost '{_vhost}' is not ready; publish to '{stream}' refused.");
        try
        {
            return super
                ? await handle.CreateSuperStreamProducerAsync(stream, ct).ConfigureAwait(false)
                : await handle.CreatePlainProducerAsync(stream, ct).ConfigureAwait(false);
        }
        catch (CreateProducerException ex) when (ex.ResponseCode == ResponseCode.StreamDoesNotExist)
        {
            throw new StreamBusStreamNotFoundException(stream, ex);
        }
        catch (CreateProducerException ex)
        {
            throw new StreamBusPublishException($"Could not create a producer for stream '{stream}' (status {ex.ResponseCode}).", ex);
        }
    }
}
```

The registry reads the live handle through a new trivial member on the supervisor (decided: option (b) — the readiness→exception mapping with vhost/stream context lives in exactly one place, the registry). The first catch passes the handle's own terminal mapping through unwrapped: a `StreamBusPublishException` from `SendAsync` already carries stream + status (Review Focus line 2 — it must surface as itself, never wrapped into transient). A failed confirm does NOT invalidate the cached handle (ruling: the producer is not known-broken by one NACK; a truly dead producer self-heals through the transient path on the next send, avoiding churn on message-specific rejections). Add to `StreamBusConnectionSupervisor` in the existing `ConnectionSupervisor.cs` (Modify, no new file):

```csharp
internal IStreamSystemHandle? CurrentHandle
{
    get { lock (_gate) return State == BusConnectionState.Ready ? _handle : null; }
}
```

And in `CreateAsync` above, the first line is:

```csharp
var handle = _supervisor.CurrentHandle
    ?? throw new StreamBusNotReadyException($"StreamBus for vhost '{_vhost}' is not ready; publish to '{stream}' refused.");
```

replacing the `_supervisorSystemHandleAsync(ct)` line (that method does not exist — do not create it). The null branch is defensive only: `PublishAsync` already returned `Ready` moments before on the same instance. `StreamBusPublishException` needs an `(string, Exception?)` constructor (this task's `PublishExceptions.cs` above provides it).

`((ICollection<KeyValuePair<string, Task<IStreamProducerHandle>>>)_handles).Remove(...)` removes only when the stored task reference-matches — `ConcurrentDictionary` has no key+value-match `TryRemove` overload, hence the interface form. Caller cancellation (`IsCallerCancel`) never invalidates and never wraps — the `OCE` propagates untouched.

- [ ] **Step 4: Verify**

```powershell
dotnet build src/StreamBus/StreamBus.csproj -c Release
```

Expected: zero warnings/errors. Behavior is pinned by Task 5 tests.

- [ ] **Step 5: Commit (with human approval)**

```powershell
git add src/StreamBus/PublishOptions.cs src/StreamBus/PublishExceptions.cs src/StreamBus/ProducerRegistry.cs src/StreamBus/ConnectionSupervisor.cs src/StreamBus/ProducerHandles.cs
git commit -m "feat: add producer registry with readiness and confirm timeout"
```

(`ProducerHandles.cs` carries the placeholder deletion — without it the committed tree holds a duplicate definition and does not build.)

---

### Task 4: Publish surface on the buses

**Files:**
- Modify: `src/StreamBus/Bus.cs` (add `PublishAsync` to both interfaces + both implementations)
- Modify: `src/StreamBus/Registry.cs` (per-vhost producer registries + internal accessor)

**Interfaces:**
- Consumes: `StreamProducerRegistry.PublishAsync`, `PublishOptions`, exception types (Task 3); `ReadinessPolicy`/`BusReadiness`/`BusConnectionState` (Phase 3).
- Produces: `IStreamBus.PublishAsync`, `IStreamBus<TBus>.PublishAsync` for Tasks 5–6.

- [ ] **Step 1: Extend `Bus.cs`**

Add to both `IStreamBus` and `IStreamBus<TBus>`:

```csharp
Task PublishAsync(string stream, ReadOnlyMemory<byte> body, PublishOptions? options = null, CancellationToken ct = default);
```

Both implementations gain a `StreamProducerRegistry` field (constructor-added third parameter) and delegate:

```csharp
// in both StreamBus and StreamBus<TBus> (field + ctor parameter added):
private readonly StreamProducerRegistry _producers;

public StreamBus(string vhost, StreamBusConnectionSupervisor supervisor, StreamProducerRegistry producers)
{
    Vhost = vhost;
    _supervisor = supervisor;
    _producers = producers;
}

public Task PublishAsync(string stream, ReadOnlyMemory<byte> body, PublishOptions? options = null, CancellationToken ct = default)
    => _producers.PublishAsync(stream, body, options ?? new PublishOptions(), ct);
```

(The `StreamBus<TBus>` constructor takes the identical third parameter.)

`Registry.cs` creates one `StreamProducerRegistry` per vhost alongside the supervisor. Exact shape (add beside the existing `_supervisors` map from Task 1):

```csharp
private readonly ConcurrentDictionary<string, StreamProducerRegistry> _producers = new();

internal StreamProducerRegistry ProducersFor(string vhost)
    => _producers.GetOrAdd(vhost, _ => new StreamProducerRegistry(SupervisorForCore(vhost), vhost));
```

And both bus constructions gain the registry as a third argument:

```csharp
IStreamBus bus = busType == typeof(DefaultBus)
    ? new StreamBus(vhost, supervisor, ProducersFor(vhost))
    : (IStreamBus)Activator.CreateInstance(typeof(StreamBus<>).MakeGenericType(busType), vhost, supervisor, ProducersFor(vhost))!;
```

`StreamBus`/`StreamBus<TBus>` constructors become `(string vhost, StreamBusConnectionSupervisor supervisor, StreamProducerRegistry producers)` storing `_producers` and delegating as above. `Registry.cs` is the only production caller of those constructors — no other call sites exist. `System.Collections.Concurrent` is already imported in `Registry.cs` (Phase 3).

- [ ] **Step 2: Verify**

```powershell
dotnet build src/StreamBus/StreamBus.csproj -c Release
dotnet test tests/StreamBus.UnitTests/StreamBus.UnitTests.csproj -c Release
```

Expected: build clean; existing unit suite still green (constructor changes are internal to the registry).

- [ ] **Step 3: Commit (with human approval)**

```powershell
git add src/StreamBus/Bus.cs src/StreamBus/Registry.cs
git commit -m "feat: add PublishAsync surface with options and exceptions"
```

---

### Task 5: Broker-free unit tests (reuse, confirms, errors, timeout)

**Files:**
- Create: `tests/StreamBus.UnitTests/PublishTests.cs`

**Interfaces:**
- Consumes: everything from Tasks 1–4; stub `IStreamSystemHandle` + stub `IStreamProducerHandle` defined in this file.
- Produces: unit pins for Review Focus lines 2–4, the error half of line 1 (missing-stream exception), and the super-stream routing-key validation; line 5's partition landing and line 1's stays-absent half are Task 6 integration.

- [ ] **Step 1: Implement `PublishTests.cs`**

```csharp
using RabbitMQ.Stream.Client;

namespace StreamBus.UnitTests;

sealed class StubProducerHandle : IStreamProducerHandle
{
    private readonly Func<ReadOnlyMemory<byte>, string?, CancellationToken, Task> _send;
    public int SendCount;
    public StubProducerHandle(Func<ReadOnlyMemory<byte>, string?, CancellationToken, Task>? send = null)
        => _send = send ?? ((_, _, _) => Task.CompletedTask);
    public Task SendAsync(ReadOnlyMemory<byte> body, string? routingKey, CancellationToken ct)
    {
        SendCount++;
        return _send(body, routingKey, ct);
    }
    public ValueTask DisposeAsync() => ValueTask.CompletedTask;
}

sealed class StubSystemHandle : IStreamSystemHandle
{
    public int PlainCreations;
    public int SuperCreations;
    public Func<string, CancellationToken, Task<IStreamProducerHandle>>? OnCreatePlain;
    public Func<string, CancellationToken, Task<IStreamProducerHandle>>? OnCreateSuper;
    public bool IsClosed => false;
    public Task<IStreamProducerHandle> CreatePlainProducerAsync(string stream, CancellationToken ct)
    {
        PlainCreations++;
        return OnCreatePlain is not null ? OnCreatePlain(stream, ct) : Task.FromResult<IStreamProducerHandle>(new StubProducerHandle());
    }
    public Task<IStreamProducerHandle> CreateSuperStreamProducerAsync(string stream, CancellationToken ct)
    {
        SuperCreations++;
        return OnCreateSuper is not null ? OnCreateSuper(stream, ct) : Task.FromResult<IStreamProducerHandle>(new StubProducerHandle());
    }
    public ValueTask DisposeAsync() => ValueTask.CompletedTask;
}

sealed class ReadyConnector : IStreamSystemConnector
{
    private readonly StubSystemHandle _handle;
    public ReadyConnector(StubSystemHandle handle) => _handle = handle;
    public Task<IStreamSystemHandle> ConnectAsync(StreamBusOptions options, CancellationToken ct)
        => Task.FromResult<IStreamSystemHandle>(_handle);
}

public sealed class PublishTests
{
    private static (StreamProducerRegistry Registry, StubSystemHandle System) CreateRegistry()
    {
        var system = new StubSystemHandle();
        var options = new StreamBusOptions { Vhost = "/", UserName = "app", Password = "secret" };
        var supervisor = new StreamBusConnectionSupervisor(options, new ReadyConnector(system));
        supervisor.StartAsync(CancellationToken.None).GetAwaiter().GetResult();
        var readiness = supervisor.WaitUntilReadyAsync(ReadinessPolicy.Indefinitely, new CancellationTokenSource(TimeSpan.FromSeconds(5)).Token).GetAwaiter().GetResult();
        Assert.Equal(BusReadiness.Ready, readiness);
        return (new StreamProducerRegistry(supervisor, "/"), system);
    }

    [Fact]
    public async Task N_publishes_reuse_one_producer()
    {
        var (registry, system) = CreateRegistry();
        for (var i = 0; i < 5; i++)
            await registry.PublishAsync("orders", new byte[] { (byte)i }, new PublishOptions());
        Assert.Equal(1, system.PlainCreations);
    }

    [Fact]
    public async Task Failed_confirm_raises_with_stream_and_status()
    {
        var (registry, system) = CreateRegistry();
        system.OnCreatePlain = (_, _) => Task.FromResult<IStreamProducerHandle>(
            new StubProducerHandle((_, _, _) => throw new StreamBusPublishException("Publish to stream 'orders' was not confirmed (status InternalError).")));
        var ex = await Assert.ThrowsAsync<StreamBusPublishException>(() =>
            registry.PublishAsync("orders", new byte[] { 1 }, new PublishOptions()));
        Assert.Contains("orders", ex.Message);
    }

    [Fact]
    public async Task Missing_stream_throws_not_found()
    {
        var system = new StubSystemHandle
        {
            OnCreatePlain = (_, _) => Task.FromException<IStreamProducerHandle>(
                new CreateProducerException("producer could not be created code: StreamDoesNotExist", ResponseCode.StreamDoesNotExist)),
        };
        var options = new StreamBusOptions { Vhost = "/", UserName = "app", Password = "secret" };
        var supervisor = new StreamBusConnectionSupervisor(options, new ReadyConnector(system));
        await supervisor.StartAsync(CancellationToken.None);
        var throwing = new StreamProducerRegistry(supervisor, "/");
        var ex = await Assert.ThrowsAsync<StreamBusStreamNotFoundException>(() =>
            throwing.PublishAsync("nope", new byte[] { 1 }, new PublishOptions()));
        Assert.Equal("nope", ex.Stream);
    }

    [Fact]
    public async Task Confirm_timeout_throws_and_bus_stays_ready()
    {
        var slow = new StubSystemHandle
        {
            OnCreatePlain = (_, _) => Task.FromResult<IStreamProducerHandle>(
                new StubProducerHandle(async (_, _, ct) => { await Task.Delay(60_000, ct); })),
        };
        var options = new StreamBusOptions { Vhost = "/", UserName = "app", Password = "secret" };
        var supervisor = new StreamBusConnectionSupervisor(options, new ReadyConnector(slow));
        await supervisor.StartAsync(CancellationToken.None);
        var reg = new StreamProducerRegistry(supervisor, "/");
        var ex = await Assert.ThrowsAsync<StreamBusTimeoutException>(() =>
            reg.PublishAsync("orders", new byte[] { 1 }, new PublishOptions { Timeout = TimeSpan.FromMilliseconds(200) }));
        Assert.Contains("outcome unknown", ex.Message);
        Assert.Equal(BusConnectionState.Ready, supervisor.State);
    }

    [Fact]
    public async Task Transient_send_invalidates_and_recreates()
    {
        var (registry, system) = CreateRegistry();
        var calls = 0;
        system.OnCreatePlain = (_, _) => Task.FromResult<IStreamProducerHandle>(
            new StubProducerHandle((_, _, _) => ++calls == 1
                ? throw new System.IO.IOException("reset")
                : Task.CompletedTask));
        await Assert.ThrowsAsync<StreamBusPublishTransientException>(() =>
            registry.PublishAsync("orders", new byte[] { 1 }, new PublishOptions()));
        await registry.PublishAsync("orders", new byte[] { 2 }, new PublishOptions());
        Assert.Equal(2, system.PlainCreations);
    }

    [Fact]
    public async Task Super_publish_requires_routing_key()
    {
        var (registry, _) = CreateRegistry();
        await Assert.ThrowsAsync<StreamBusConfigurationException>(() =>
            registry.PublishAsync("events", new byte[] { 1 }, new PublishOptions { IsSuperStream = true }));
    }

    [Fact]
    public async Task Not_ready_failfast_throws_not_ready()
    {
        var options = new StreamBusOptions { Vhost = "/", UserName = "app", Password = "secret", Host = "127.0.0.1", Port = 59999 };
        await using var supervisor = new StreamBusConnectionSupervisor(options, new StreamSystemConnector());
        await supervisor.StartAsync(CancellationToken.None);
        var registry = new StreamProducerRegistry(supervisor, "/");
        await Assert.ThrowsAsync<StreamBusNotReadyException>(() =>
            registry.PublishAsync("orders", new byte[] { 1 }, new PublishOptions()));
    }
}
```

Adaptation rules (apply, report): if `CreateProducerException` lacks a `(string, ResponseCode)` constructor, construct it the way the probe/Task 2 shows the client does (same rule as Phase 3's `GenericProtocolException`); if `ResponseCode.StreamDoesNotExist` is named differently, use the probed member; stub helpers are plain (not `file`-scoped) `sealed` classes — file-local types cannot appear in the helper's tuple return (CS9051); field stubs that no test assigns get a default lambda (CS0649); if the build flags further analyzer complaints (xUnit1051 tokens, undisposed locals, sync-over-async in helpers), fix in the standard forms (explicit `TestContext.Current.CancellationToken`, `await using`, async helpers) without changing assertions.

- [ ] **Step 2: Verify**

```powershell
dotnet test tests/StreamBus.UnitTests/StreamBus.UnitTests.csproj -c Release
```

Expected: all green (36 existing + 7 new), pristine output.

- [ ] **Step 3: Commit (with human approval)**

```powershell
git add tests/StreamBus.UnitTests/PublishTests.cs
git commit -m "test: add broker-free publish unit tests"
```

---

### Task 6: Integration probes + full Phase 4 gate

**Files:**
- Create: `tests/StreamBus.IntegrationTests/PublishIntegrationTests.cs`

**Interfaces:**
- Consumes: `IStreamBus.PublishAsync`, `PublishOptions`, all exception types; broker at 127.0.0.1:5552 with `STREAMBUS_USER`/`STREAMBUS_PASSWORD` (defaults app/password, created in Phase 3).
- Produces: the Phase 4 completion gate (FR-1.1–FR-1.5 against the real broker).

- [ ] **Step 1: Implement `PublishIntegrationTests.cs`**

```csharp
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using RabbitMQ.Stream.Client;

namespace StreamBus.IntegrationTests;

public sealed class PublishIntegrationTests : IAsyncDisposable
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
        }).ConfigureAwait(false);

    private static string Unique(string prefix) => $"{prefix}-{Guid.NewGuid():N}";

    public async ValueTask DisposeAsync()
    {
        var admin = await ConnectAdminAsync().ConfigureAwait(false);
        try
        {
            foreach (var s in _streams) { try { await admin.DeleteStream(s).ConfigureAwait(false); } catch { } }
            foreach (var s in _superStreams) { try { await admin.DeleteSuperStream(s).ConfigureAwait(false); } catch { } }
        }
        finally { await admin.DisposeAsync().ConfigureAwait(false); }
    }

    [Fact]
    public async Task Plain_round_trip_delivers_ordered_offsets()
    {
        var stream = Unique("pub-plain");
        var admin = await ConnectAdminAsync().ConfigureAwait(false);
        await using (admin.ConfigureAwait(false))
        {
            await admin.CreateStream(new StreamSpec(stream)).ConfigureAwait(false);
            _streams.Add(stream);
            var services = new ServiceCollection();
            services.AddLogging();
            services.AddStreamBus("/", o => o.Credentials(User(), Password()));
            await using var sp = services.BuildServiceProvider();
            var hosted = sp.GetServices<IHostedService>().OfType<StreamBusHostedService>().Single();
            await hosted.StartAsync(CancellationToken.None);
            try
            {
                var bus = sp.GetRequiredService<IStreamBus>();
                Assert.Equal(BusReadiness.Ready, await bus.WaitUntilReadyAsync(ReadinessPolicy.UpTo(TimeSpan.FromSeconds(30))));
                for (byte i = 0; i < 3; i++)
                    await bus.PublishAsync(stream, new ReadOnlyMemory<byte>(new[] { i }), new PublishOptions());
                var consumer = await admin.CreateRawConsumer(new RawConsumerConfig(stream)
                {
                    Reference = $"probe-{Guid.NewGuid():N}",
                    OffsetSpec = new OffsetTypeFirst(),
                }).ConfigureAwait(false);
                var received = new List<byte>();
                var done = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
                await consumer.Start(
                    (_, _, _, message) =>
                    {
                        lock (received) received.Add(message.Data.Span[0]);
                        if (received.Count == 3) done.TrySetResult();
                        return Task.CompletedTask;
                    }).ConfigureAwait(false);
                await done.Task.WaitAsync(TimeSpan.FromSeconds(30)).ConfigureAwait(false);
                await consumer.Close().ConfigureAwait(false);
                Assert.Equal(new byte[] { 0, 1, 2 }, received.ToArray());
            }
            finally { await hosted.StopAsync(CancellationToken.None); }
        }
    }

    [Fact]
    public async Task Missing_stream_fails_and_stays_absent()
    {
        var stream = Unique("pub-missing");
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddStreamBus("/", o => o.Credentials(User(), Password()));
        await using var sp = services.BuildServiceProvider();
        var hosted = sp.GetServices<IHostedService>().OfType<StreamBusHostedService>().Single();
        await hosted.StartAsync(CancellationToken.None);
        try
        {
            var bus = sp.GetRequiredService<IStreamBus>();
            Assert.Equal(BusReadiness.Ready, await bus.WaitUntilReadyAsync(ReadinessPolicy.UpTo(TimeSpan.FromSeconds(30))));
            var ex = await Assert.ThrowsAsync<StreamBusStreamNotFoundException>(() =>
                bus.PublishAsync(stream, new ReadOnlyMemory<byte>(new byte[] { 9 }), new PublishOptions()));
            Assert.Equal(stream, ex.Stream);
            var admin = await ConnectAdminAsync().ConfigureAwait(false);
            await using (admin.ConfigureAwait(false))
                Assert.False(await admin.StreamExists(stream).ConfigureAwait(false));
        }
        finally { await hosted.StopAsync(CancellationToken.None); }
    }

    [Fact]
    public async Task Super_stream_keys_land_on_stable_partitions()
    {
        var super = Unique("pub-super");
        var admin = await ConnectAdminAsync().ConfigureAwait(false);
        await using (admin.ConfigureAwait(false))
        {
            await admin.CreateSuperStream(new SuperStreamSpec(super, 3)).ConfigureAwait(false);
            _superStreams.Add(super);
            var services = new ServiceCollection();
            services.AddLogging();
            services.AddStreamBus("/", o => o.Credentials(User(), Password()));
            await using var sp = services.BuildServiceProvider();
            var hosted = sp.GetServices<IHostedService>().OfType<StreamBusHostedService>().Single();
            await hosted.StartAsync(CancellationToken.None);
            try
            {
                var bus = sp.GetRequiredService<IStreamBus>();
                Assert.Equal(BusReadiness.Ready, await bus.WaitUntilReadyAsync(ReadinessPolicy.UpTo(TimeSpan.FromSeconds(30))));
                foreach (var key in new[] { "alpha", "beta", "alpha", "gamma", "beta", "alpha" })
                    await bus.PublishAsync(super,
                        new ReadOnlyMemory<byte>(System.Text.Encoding.UTF8.GetBytes(key)),
                        new PublishOptions { IsSuperStream = true, RoutingKey = key });
                var partitions = await admin.QueryPartition(super).ConfigureAwait(false);
                Assert.Equal(3, partitions.Length);
                var perPartition = partitions.ToDictionary(p => p, _ => new List<string>());
                var total = 0;
                var done = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
                var closers = new List<Func<Task>>();
                foreach (var partition in partitions)
                {
                    var list = perPartition[partition];
                    var consumer = await admin.CreateRawConsumer(new RawConsumerConfig(partition)
                    {
                        Reference = $"probe-{Guid.NewGuid():N}",
                        OffsetSpec = new OffsetTypeFirst(),
                    }).ConfigureAwait(false);
                    closers.Add(() => consumer.Close());
                    await consumer.Start(
                        (_, _, _, message) =>
                        {
                            lock (list) list.Add(System.Text.Encoding.UTF8.GetString(message.Data.Span));
                            if (System.Threading.Interlocked.Increment(ref total) == 6) done.TrySetResult();
                            return Task.CompletedTask;
                        }).ConfigureAwait(false);
                }
                var winner = await Task.WhenAny(done.Task, Task.Delay(TimeSpan.FromSeconds(30))).ConfigureAwait(false);
                Assert.True(winner == done.Task, "Timed out waiting for all 6 super-stream messages.");
                foreach (var close in closers)
                    await close().ConfigureAwait(false);
                var all = perPartition.Values.SelectMany(v => v).OrderBy(x => x).ToList();
                Assert.Equal(6, all.Count);
                foreach (var key in new[] { "alpha", "beta", "gamma" })
                    Assert.Single(perPartition.Values.Where(v => v.Contains(key)));
            }
            finally { await hosted.StopAsync(CancellationToken.None); }
        }
    }
}
```

Adaptation rules (apply, report — same class as Phase 3's client-surface findings): if `StreamSpec`/`SuperStreamSpec` constructors, `RawConsumerConfig` members (`Reference`, `OffsetSpec`), consumer `Start`/`Close` shapes, `OffsetTypeFirst` construction, or `Message.Data` access differ in 1.12.1, adapt the test-only client calls to the probed signatures without changing any assertion (the `closers` list avoids naming the client's consumer interface, so a rename there needs no test-logic change); if the consumer has no `Close`/`Dispose` member, drop the closer registration (test process exit cleans up); if `CreateSuperStream` needs explicit binding keys, pass one key per partition.

As-built test-only deviations (verified green, assertions intact): `SuperStreamSpec` is abstract — use `PartitionsSuperStreamSpec(super, 3)`; raw consumers deliver via `RawConsumerConfig.MessageHandler` (3-arg lambda), there is no `Start` method; `Message.Data` is AMQP `Data` with `ReadOnlySequence<byte> Contents` (add `using System.Buffers;`); `IConsumer` disposal is sync `Dispose()`; `await using var admin` declarations (no `ConfigureAwait` on the system itself); no `.ConfigureAwait(false)` inside `[Fact]`s (xUnit1030), explicit `TestContext.Current.CancellationToken` (xUnit1051), `Assert.Single(values, pred)` overload (xUnit2031).

- [ ] **Step 2: Verify (full Phase 4 gate)**

```powershell
dotnet test StreamBus.slnx -c Release
Select-String -Path src/StreamBus/*.cs -Pattern "CreateStream\s*\(" | Where-Object { $_.Filename -notlike "*test*" }
```

Expected: unit suite green (43: 36 existing + 7 new) and all 3 integration probes green; the `Select-String` check returns nothing (no `CreateStream(` call exists under `src/` — FR-1.3 structural proof); broker version from the run environment recorded in the report (expect RabbitMQ 4.2.6; G-3's 3-node cluster explicitly not required).

- [ ] **Step 3: Commit (with human approval)**

```powershell
git add tests/StreamBus.IntegrationTests/PublishIntegrationTests.cs
git commit -m "test: add publish integration probes for FR-1"
```

---

## Follow-ups for Phase 5 (tracked, no code in this phase)

- Registry-level producer disposal on shutdown: failure-path eviction disposes (this phase), but shutdown relies on the client's pool close via supervisor disposal. Phase 5 may add an explicit registry hook.
- Reconnect-triggered producer invalidation: no supervisor reconnect behavior exists yet; a stale cached producer today self-heals through exactly one transient failure, which is acceptable but loud per reconnect.
- Phase 7 note (from Task 2 probe): the confirm tracker generates publishing ids internally (from 1); effectively-once (publishing-id = source offset) will need explicit-id support in the handle; `IProducer.GetLastPublishingId` exists for the resume-point query.

## Self-Review

- Spec coverage: FR-1.1 (Tasks 2–4 publish path + Task 6 round-trip/super probes), FR-1.2 (Tasks 3–4 missing mapping + Task 6 stays-absent probe), FR-1.3 (no `CreateStream` anywhere in `src/`, asserted by the `Select-String` check in Task 6 Step 2), FR-1.4 (Task 2 confirm mapping + Task 5 failing-confirm test), FR-1.5 (Task 5 reuse test), FR-8.1 sharing (Task 1 per-vhost supervisors), FR-8.6 (Tasks 3–4 timeout/readiness split + Task 5 timeout test). Design §9 surface present minus deliberately deferred `DeduplicationName` (ruled). G-1/G-2/G-3 untouched (later phases).
- Placeholder scan: no TBD/TODO; every code step ships complete source; the Task 2 probe branches are fully specified conditionals with exact commands, not open ends; no "similar to Task N".
- Type consistency: `PublishAsync(string, ReadOnlyMemory<byte>, PublishOptions?, CancellationToken)`, `PublishOptions` members, four exception names/shapes, `IStreamProducerHandle.SendAsync`, `StreamProducerRegistry.PublishAsync`, `SupervisorFor`/`ProducersFor` internal accessors used identically across Tasks 1–6. One deliberate rename to watch in review: Task 5's `CreateRegistry` helper vs Task 3's `SupervisorFor` — distinct names, no collision.
- Review Focus: each of the five lines names its pinning task and verification above.
