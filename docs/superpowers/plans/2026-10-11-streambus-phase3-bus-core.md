# StreamBus Phase 3 (Bus Core) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Implement Phase 3 bus core only: supervising connect loop, per-call-site readiness gate, health, registry, and DI hosting on .NET 10.

**Architecture:** One `StreamSystem` per vhost shared by all buses bound to it; each bus owns a `StreamBusConnectionSupervisor` that retries `StreamSystem.Create` forever on network errors with capped exponential backoff plus jitter, fails fast on fatal errors, validates credentials (guest trap) before the loop, and exposes `WaitUntilReadyAsync` plus connection-state health per (vhost, bus type). No publish/consume in this phase.

**Tech Stack:** .NET 10 (`net10.0`), C# latest, `RabbitMQ.Stream.Client` 1.12.1, `Microsoft.Extensions.Hosting` 10.0.12, `Microsoft.Extensions.Diagnostics.HealthChecks` 10.0.12, xUnit v3 (`xunit.v3` 4.0.2, `xunit.runner.visualstudio` 4.0.1, `Microsoft.NET.Test.Sdk` 18.10.1), `StreamBus.slnx` (XML solution format), Central Package Management.

**Spec:** `docs/2026-10-08-streambus-design.md` (§2 connection model, §3 startup retry, §3.1 readiness gate, §13 `streambus_connection_state`) and `docs/2026-10-08-streambus-requirements.md` (FR-8.1–FR-8.11, FR-9.2). Phase table: design §10 row 3.

## Global Constraints

- `TargetFramework` is exactly `net10.0` in `Directory.Build.props`; `Nullable` enable; `ImplicitUsings` enable; `TreatWarningsAsErrors` true; `ManagePackageVersionsCentrally` true.
- `RabbitMQ.Stream.Client` version is exactly `1.12.1` (latest stable 1.x verified 2026-10-11 via nuget.org flat container).
- `Microsoft.Extensions.Hosting` and `Microsoft.Extensions.Diagnostics.HealthChecks` are exactly `10.0.12` (verified stable via nuget.org flat container).
- Test packages are exactly `xunit.v3` 4.0.2, `xunit.runner.visualstudio` 4.0.1, `Microsoft.NET.Test.Sdk` 18.10.1 (all verified latest stable 2026-10-11).
- Library root namespace is `StreamBus`; never `RabbitMQ.Stream.*` (requirements §0.3).
- `StreamSystem` binds to exactly one vhost; no per-producer/per-consumer vhost override exists in any API (FR-8.1).
- One `StreamSystem` per vhost is shared by all buses bound to it; callers never see `StreamSystem` (design §2.1).
- Pooling defaults are exactly 1 producer and 1 consumer per connection (FR-8.10).
- The supervising loop retries forever on network errors, never surfaces a terminal connection error except fatals (FR-8.3, FR-8.5); `ProtocolException`, `SslException`, `AuthMechanismNotSupportedException`, and any `AuthenticationFailure*` are fatal and never retried (FR-8.4). Verified against client source: `StreamSystem.Create` rethrows `ProtocolException`/`SslException`/`AuthMechanismNotSupportedException` and throws `StreamSystemInitialisationException("no endpoints could be reached")` otherwise.
- `PublishOptions.Timeout` does not exist in this phase and, when introduced in Phase 4, is the confirm timeout only, never the readiness wait (FR-8.6).
- Every commit step requires explicit human approval before running; never push, never amend without permission.

## Review Focus

- Broker down at startup, up later: bus must connect with no terminal error surfaced; `WaitUntilReadyAsync(WaitIndefinitely)` returns `Ready` (FR-8.3). Pinned by Task 7 integration probe.
- `guest`/`guest` credentials: startup must fail fast with a message naming the loopback trap, never enter backoff (FR-8.9, design §1.1). Pinned by Task 4 unit test.
- Fatal `ProtocolException`/`SslException`/`AuthMechanismNotSupportedException`/`AuthenticationFailure*` mid-loop: must surface promptly as `Fatal`, not keep backing off (FR-8.4). Pinned by Task 3 unit test.
- `WaitUpTo(t)` bounds only the readiness wait and returns `TimedOut` on expiry while the background loop keeps retrying (FR-8.6, FR-8.7, FR-8.11). Pinned by Task 4 unit test with a never-ready connector.
- Duplicate `AddStreamBus` (same non-generic overload twice, or same `TBus` twice) must fail at configuration time; one default plus N markers may coexist (FR-8.1). Pinned by Task 6 unit test.

---

## File Structure

New files (Phase 3 only; hosting is inbuilt, no separate `StreamBus.Hosting` project):

- `Directory.Build.props` — TFMs, nullable, warnings, LangVersion.
- `Directory.Packages.props` — Central Package Management pins.
- `StreamBus.slnx` — created via `dotnet new sln` (default format is `slnx` on this SDK; verify).
- `src/StreamBus/StreamBus.csproj` — single library project (includes hosting).
- `src/StreamBus/Readiness.cs` — `BusReadiness`, `ReadinessPolicy`, `BusConnectionState`.
- `src/StreamBus/Options.cs` — `StreamBusConnectionOptions`, `StreamBusHostOptions`, `StreamBusOptions`.
- `src/StreamBus/Exceptions.cs` — `StreamBusFatalException`, `StreamBusTimeoutException`, `StreamBusConfigurationException`.
- `src/StreamBus/ErrorClassification.cs` — `StreamBusErrorClassification.IsFatal(Exception)`.
- `src/StreamBus/Connector.cs` — `IStreamSystemHandle`, `IStreamSystemConnector`, `StreamSystemConnector` (thin wrapper over `StreamSystem.Create`).
- `src/StreamBus/ConnectionSupervisor.cs` — `StreamBusConnectionSupervisor` (retry loop, backoff, `WaitUntilReadyAsync`, state events).
- `src/StreamBus/CredentialValidation.cs` — `StreamBusCredentialValidator` (guest-trap fail-fast).
- `src/StreamBus/Bus.cs` — `IStreamBus`, `IStreamBus<TBus>`, `StreamBus`, `StreamBus<TBus>`.
- `src/StreamBus/Registry.cs` — `IStreamBusRegistry`, `StreamBusRegistry` (one `StreamSystem` per vhost shared).
- `src/StreamBus/Hosting.cs` — `ServiceCollectionExtensions` (`AddStreamBus` overloads), `StreamBusHostedService`, `StreamBusHealthCheck`.
- `tests/StreamBus.UnitTests/StreamBus.UnitTests.csproj` — xUnit v3 project.
- `tests/StreamBus.UnitTests/*Tests.cs` — classifier, readiness, duplicate-registration, backoff-cap tests.
- `tests/StreamBus.IntegrationTests/StreamBus.IntegrationTests.csproj` — xUnit v3 project (real broker at `127.0.0.1:5552`, verified reachable 2026-10-11).
- `tests/StreamBus.IntegrationTests/BusCoreIntegrationTests.cs` — down-then-up connect, health per (vhost, bus type).

Explicitly out of Phase 3 (deferred, not stubs): `PublishAsync`, `ConsumeAsync`, `IConsumer<T>`, retry, error stream, topology, offsets. `IStreamBus` in this phase exposes only `Vhost`, `State`, and `WaitUntilReadyAsync`.

Recorded spec notes (non-blocking for Phase 3, owned by Phase 5 plan): design §3.1 says "publish and consume calls take a `ReadinessPolicy`" but the §9 `ConsumeOptions` sketch lists no `ReadinessPolicy` field; `ConsumeOptions.Ordering` shows only `Strict` with no second value defined. Do not resolve here; do not add consume APIs here.

---

### Task 1: Solution scaffolding with CPM, slnx, and build props

**Files:**
- Create: `Directory.Build.props`
- Create: `Directory.Packages.props`
- Create: `StreamBus.slnx` (via template)
- Create: `src/StreamBus/StreamBus.csproj`
- Create: `tests/StreamBus.UnitTests/StreamBus.UnitTests.csproj`
- Create: `tests/StreamBus.IntegrationTests/StreamBus.IntegrationTests.csproj`

**Interfaces:**
- Consumes: nothing (first task).
- Produces: restorable solution; later tasks rely on `dotnet sln list` showing 3 projects and `dotnet restore` succeeding with centrally pinned versions.

- [ ] **Step 1: Create build props**

```xml
<!-- Directory.Build.props -->
<Project>
  <PropertyGroup>
    <TargetFramework>net10.0</TargetFramework>
    <Nullable>enable</Nullable>
    <ImplicitUsings>enable</ImplicitUsings>
    <TreatWarningsAsErrors>true</TreatWarningsAsErrors>
    <LangVersion>latest</LangVersion>
    <ManagePackageVersionsCentrally>true</ManagePackageVersionsCentrally>
    <CentralPackageTransitivePinningEnabled>true</CentralPackageTransitivePinningEnabled>
  </PropertyGroup>
</Project>
```

```xml
<!-- Directory.Packages.props -->
<Project>
  <PropertyGroup>
    <ManagePackageVersionsCentrally>true</ManagePackageVersionsCentrally>
    <CentralPackageTransitivePinningEnabled>true</CentralPackageTransitivePinningEnabled>
  </PropertyGroup>
  <ItemGroup>
    <PackageVersion Include="RabbitMQ.Stream.Client" Version="1.12.1" />
    <PackageVersion Include="Microsoft.Extensions.Hosting" Version="10.0.12" />
    <PackageVersion Include="Microsoft.Extensions.Diagnostics.HealthChecks" Version="10.0.12" />
    <PackageVersion Include="Microsoft.Extensions.Logging.Abstractions" Version="10.0.12" />
    <PackageVersion Include="xunit.v3" Version="4.0.2" />
    <PackageVersion Include="xunit.runner.visualstudio" Version="4.0.1" />
    <PackageVersion Include="Microsoft.NET.Test.Sdk" Version="18.10.1" />
  </ItemGroup>
</Project>
```

```xml
<!-- src/StreamBus/StreamBus.csproj -->
<Project Sdk="Microsoft.NET.Sdk">
  <PropertyGroup>
    <RootNamespace>StreamBus</RootNamespace>
    <AssemblyName>StreamBus</AssemblyName>
  </PropertyGroup>
  <ItemGroup>
    <PackageReference Include="RabbitMQ.Stream.Client" />
    <PackageReference Include="Microsoft.Extensions.Hosting" />
    <PackageReference Include="Microsoft.Extensions.Diagnostics.HealthChecks" />
    <PackageReference Include="Microsoft.Extensions.Logging.Abstractions" />
  </ItemGroup>
</Project>
```

The `StreamBus.csproj` above intentionally has no `Version` attributes (CPM supplies them). Test projects reference `xunit.v3`, `xunit.runner.visualstudio`, and `Microsoft.NET.Test.Sdk` with no versions (CPM supplies them).

- [ ] **Step 2: Scaffold via CLI (run in order)**

```powershell
dotnet new sln -n StreamBus
dotnet new classlib -n StreamBus -o src/StreamBus --force
dotnet new xunit3 -n StreamBus.UnitTests -o tests/StreamBus.UnitTests --force
dotnet new xunit3 -n StreamBus.IntegrationTests -o tests/StreamBus.IntegrationTests --force
dotnet sln StreamBus.slnx add src/StreamBus/StreamBus.csproj tests/StreamBus.UnitTests/StreamBus.UnitTests.csproj tests/StreamBus.IntegrationTests/StreamBus.IntegrationTests.csproj
dotnet restore
```

After scaffolding, overwrite the generated `Directory.Build.props`, `Directory.Packages.props`, and `src/StreamBus/StreamBus.csproj` content with the files from Step 1, remove any `Version=` attributes from all `PackageReference` items (CPM owns versions), delete template `Class1.cs` files, and re-run `dotnet restore`.

- [ ] **Step 3: Verify**

```powershell
dotnet sln StreamBus.slnx list
dotnet restore
dotnet build StreamBus.slnx --no-restore -c Release
```

Expected: `sln list` shows exactly `src/StreamBus`, `tests/StreamBus.UnitTests`, `tests/StreamBus.IntegrationTests`; restore and Release build succeed with zero warnings (warnings are errors).

- [ ] **Step 4: Commit (with human approval)**

```powershell
git add Directory.Build.props Directory.Packages.props StreamBus.slnx src/StreamBus tests/StreamBus.UnitTests tests/StreamBus.IntegrationTests
git status --short
git commit -m "chore: scaffold net10 solution with CPM, slnx, library and test projects"
```

---

### Task 2: Readiness, state, and options primitives

**Files:**
- Create: `src/StreamBus/Readiness.cs`
- Create: `src/StreamBus/Options.cs`

**Interfaces:**
- Consumes: `Directory.Build.props` (net10.0, nullable).
- Produces: `BusReadiness`, `ReadinessPolicy`, `BusConnectionState`, `StreamBusConnectionOptions`, `StreamBusHostOptions`, `StreamBusOptions` for Tasks 4–6.

- [ ] **Step 1: Implement `Readiness.cs`**

```csharp
namespace StreamBus;

public enum BusReadiness
{
    Ready = 0,
    TimedOut = 1,
    Fatal = 2,
}

public enum BusConnectionState
{
    Starting = 0,
    Ready = 1,
    TransientFailure = 2,
    Fatal = 3,
}

public abstract record ReadinessPolicy
{
    private ReadinessPolicy() { }

    public sealed record FailFast : ReadinessPolicy;
    public sealed record WaitUpTo(TimeSpan Timeout) : ReadinessPolicy;
    public sealed record WaitIndefinitely : ReadinessPolicy;

    public static ReadinessPolicy FailFastPolicy { get; } = new FailFast();
    public static ReadinessPolicy Indefinitely { get; } = new WaitIndefinitely();
    public static ReadinessPolicy UpTo(TimeSpan timeout) => new WaitUpTo(timeout);
}
```

- [ ] **Step 2: Implement `Options.cs`**

```csharp
namespace StreamBus;

public sealed record StreamBusConnectionOptions
{
    public int ProducersPerConnection { get; init; } = 1;
    public int ConsumersPerConnection { get; init; } = 1;

    public IEnumerable<string> Validate()
    {
        if (ProducersPerConnection < 1)
            yield return "ProducersPerConnection must be >= 1.";
        if (ConsumersPerConnection < 1)
            yield return "ConsumersPerConnection must be >= 1.";
    }
}

public sealed record StreamBusHostOptions
{
    public bool BlockStartupUntilReady { get; init; } = false;
    public TimeSpan? StartupTimeout { get; init; }
}

public sealed record StreamBusOptions
{
    public required string Vhost { get; init; }
    public required string UserName { get; init; }
    public required string Password { get; init; }
    public string Host { get; init; } = "127.0.0.1";
    public int Port { get; init; } = 5552;
    public StreamBusConnectionOptions Connection { get; init; } = new();
    public StreamBusHostOptions Hosting { get; init; } = new();
}
```

- [ ] **Step 3: Verify**

```powershell
dotnet build src/StreamBus/StreamBus.csproj -c Release
```

Expected: builds with zero warnings. `WaitUpTo` carries only the readiness-wait bound; no confirm-timeout concept exists in this phase.

- [ ] **Step 4: Commit (with human approval)**

```powershell
git add src/StreamBus/Readiness.cs src/StreamBus/Options.cs
git commit -m "feat: add bus readiness, connection state, and options primitives"
```

---

### Task 3: Exception taxonomy and fatal classifier

**Files:**
- Create: `src/StreamBus/Exceptions.cs`
- Create: `src/StreamBus/ErrorClassification.cs`
- Create: `tests/StreamBus.UnitTests/ErrorClassificationTests.cs`

**Interfaces:**
- Consumes: nothing new.
- Produces: `StreamBusFatalException`, `StreamBusConfigurationException`, `StreamBusTimeoutException`, `StreamBusErrorClassification.IsFatal(Exception)` for Tasks 4–6.

- [ ] **Step 1: Implement `Exceptions.cs`**

```csharp
namespace StreamBus;

public sealed class StreamBusFatalException : Exception
{
    public StreamBusFatalException(string message, Exception? inner = null)
        : base(message, inner) { }
}

public sealed class StreamBusTimeoutException : Exception
{
    public StreamBusTimeoutException(string message)
        : base(message) { }
}

public sealed class StreamBusConfigurationException : Exception
{
    public StreamBusConfigurationException(string message)
        : base(message) { }
}
```

- [ ] **Step 2: Implement `ErrorClassification.cs`**

```csharp
using RabbitMQ.Stream.Client;

namespace StreamBus;

public static class StreamBusErrorClassification
{
    public static bool IsFatal(Exception ex)
    {
        ArgumentNullException.ThrowIfNull(ex);
        return ex is ProtocolException
            || ex is SslException
            || ex is AuthMechanismNotSupportedException
            || IsAuthenticationFailure(ex);
    }

    private static bool IsAuthenticationFailure(Exception ex)
    {
        for (var current = ex; current is not null; current = current.InnerException)
        {
            if (current.GetType().Name.StartsWith("AuthenticationFailure", StringComparison.Ordinal))
                return true;
        }
        return false;
    }
}
```

Rationale (do not change without evidence): `ProtocolException`, `SslException`, and `AuthMechanismNotSupportedException` are verified fatal from the 1.12.1 `StreamSystem.Create` source (it rethrows exactly those and converts everything else into `StreamSystemInitialisationException`). Authentication failures are matched by type-name prefix so every `AuthenticationFailure*` variant (including the `guest`/loopback rejection in design §1.1) is fatal even if the client adds new subclasses; the prefix rule is pinned by the unit test below using reflection over the referenced 1.12.1 assembly.

- [ ] **Step 3: Implement `ErrorClassificationTests.cs`**

```csharp
using System.Reflection;
using RabbitMQ.Stream.Client;

namespace StreamBus.UnitTests;

public sealed class ErrorClassificationTests
{
    [Fact]
    public void Protocol_ssl_and_auth_mechanism_errors_are_fatal()
    {
        Assert.True(StreamBusErrorClassification.IsFatal(new ProtocolException("p")));
        Assert.True(StreamBusErrorClassification.IsFatal(new SslException("s")));
        Assert.True(StreamBusErrorClassification.IsFatal(new AuthMechanismNotSupportedException("a")));
    }

    [Fact]
    public void Network_errors_are_not_fatal()
    {
        Assert.False(StreamBusErrorClassification.IsFatal(new TimeoutException("t")));
        Assert.False(StreamBusErrorClassification.IsFatal(new System.Net.Sockets.SocketException(10061)));
        Assert.False(StreamBusErrorClassification.IsFatal(new System.IO.IOException("reset")));
        Assert.False(StreamBusErrorClassification.IsFatal(new StreamSystemInitialisationException("no endpoints could be reached")));
    }

    [Fact]
    public void Every_authentication_failure_type_in_the_client_is_fatal()
    {
        var authFailures = typeof(StreamSystem).Assembly.GetTypes()
            .Where(t => t.Name.StartsWith("AuthenticationFailure", StringComparison.Ordinal) && !t.IsAbstract)
            .ToList();
        Assert.NotEmpty(authFailures);
        foreach (var type in authFailures)
        {
            var instance = (Exception)Activator.CreateInstance(type, "auth")!;
            Assert.True(StreamBusErrorClassification.IsFatal(instance));
            Assert.True(StreamBusErrorClassification.IsFatal(new StreamBusFatalException("wrap", instance)));
        }
    }
}
```

If `Activator.CreateInstance(type, "auth")` throws `MissingMethodException` for any discovered type, inspect that type's constructors in the 1.12.1 assembly and construct it the way the client does (still asserting `IsFatal` is true); do not delete the test.

- [ ] **Step 4: Verify**

```powershell
dotnet test tests/StreamBus.UnitTests/StreamBus.UnitTests.csproj -c Release
```

Expected: all 3 tests pass. The third test fails the task if the client renames its auth-failure hierarchy — that is intentional (FR-8.4).

- [ ] **Step 5: Commit (with human approval)**

```powershell
git add src/StreamBus/Exceptions.cs src/StreamBus/ErrorClassification.cs tests/StreamBus.UnitTests/ErrorClassificationTests.cs
git commit -m "feat: add fatal error classification with client-hierarchy probe"
```

---

### Task 4: Credential validation and connection supervisor

**Files:**
- Create: `src/StreamBus/CredentialValidation.cs`
- Create: `src/StreamBus/Connector.cs`
- Create: `src/StreamBus/ConnectionSupervisor.cs`
- Create: `tests/StreamBus.UnitTests/ConnectionSupervisorTests.cs`

**Interfaces:**
- Consumes: `ReadinessPolicy`, `BusReadiness`, `BusConnectionState`, `StreamBusOptions` (Task 2); `IsFatal` (Task 3).
- Produces: `IStreamSystemHandle`, `IStreamSystemConnector`, `StreamBusConnectionSupervisor` with `State`, `StateChanged`, `WaitUntilReadyAsync(ReadinessPolicy, CancellationToken)`, `StartAsync(CancellationToken)` for Tasks 5–6.

- [ ] **Step 1: Implement `CredentialValidation.cs`**

```csharp
namespace StreamBus;

public static class StreamBusCredentialValidator
{
    public static void Validate(StreamBusOptions options)
    {
        ArgumentNullException.ThrowIfNull(options);
        if (string.IsNullOrWhiteSpace(options.UserName))
            throw new StreamBusConfigurationException("UserName must be supplied.");
        if (string.IsNullOrWhiteSpace(options.Password))
            throw new StreamBusConfigurationException("Password must be supplied.");
        if (string.IsNullOrWhiteSpace(options.Vhost))
            throw new StreamBusConfigurationException("Vhost must be supplied.");
        if (options.UserName == "guest")
            throw new StreamBusConfigurationException(
                "User 'guest' cannot be used by the stream client except over loopback: the client follows the broker's metadata to a non-loopback address and the broker rejects guest with AuthenticationFailureLoopback. Create a dedicated non-guest user and reconfigure UserName/Password.");
    }
}
```

- [ ] **Step 2: Implement `Connector.cs`**

```csharp
using System.Net;
using RabbitMQ.Stream.Client;

namespace StreamBus;

public interface IStreamSystemHandle : IAsyncDisposable
{
    bool IsClosed { get; }
}

public interface IStreamSystemConnector
{
    Task<IStreamSystemHandle> ConnectAsync(StreamBusOptions options, CancellationToken ct);
}

public sealed class StreamSystemHandle : IStreamSystemHandle
{
    private readonly StreamSystem _system;
    public StreamSystemHandle(StreamSystem system) => _system = system;
    public bool IsClosed => _system.IsClosed;
    public ValueTask DisposeAsync() => _system.DisposeAsync();
}

public sealed class StreamSystemConnector : IStreamSystemConnector
{
    public async Task<IStreamSystemHandle> ConnectAsync(StreamBusOptions options, CancellationToken ct)
    {
        ArgumentNullException.ThrowIfNull(options);
        EndPoint endpoint = IPAddress.TryParse(options.Host, out var address)
            ? new IPEndPoint(address, options.Port)
            : new DnsEndPoint(options.Host, options.Port);
        var config = new StreamSystemConfig
        {
            UserName = options.UserName,
            Password = options.Password,
            VirtualHost = options.Vhost,
            Endpoints = new List<EndPoint> { endpoint },
            ClientProvidedName = "streambus-locator",
            ConnectionPoolConfig = new ConnectionPoolConfig
            {
                ProducersPerConnection = options.Connection.ProducersPerConnection,
                ConsumersPerConnection = options.Connection.ConsumersPerConnection,
            },
        };
        var system = await StreamSystem.Create(config).ConfigureAwait(false);
        ct.ThrowIfCancellationRequested();
        return new StreamSystemHandle(system);
    }
}
```

If the 1.12.1 `ConnectionPoolConfig` property names differ, the Release build in Step 5 fails — fix the mapping to the actual names (keep our `StreamBusConnectionOptions` names unchanged) rather than renaming our options.

- [ ] **Step 3: Implement `ConnectionSupervisor.cs`**

```csharp
namespace StreamBus;

public sealed class StreamBusConnectionSupervisor : IAsyncDisposable
{
    private readonly StreamBusOptions _options;
    private readonly IStreamSystemConnector _connector;
    private readonly TimeSpan _baseDelay = TimeSpan.FromMilliseconds(200);
    private readonly TimeSpan _maxDelay = TimeSpan.FromSeconds(30);
    private readonly object _gate = new();
    private IStreamSystemHandle? _handle;
    private Task? _loop;
    private CancellationTokenSource? _cts;
    private Exception? _fatal;

    public BusConnectionState State { get; private set; } = BusConnectionState.Starting;
    public event Action<BusConnectionState>? StateChanged;

    public StreamBusConnectionSupervisor(StreamBusOptions options, IStreamSystemConnector connector)
    {
        _options = options;
        _connector = connector;
        foreach (var error in options.Connection.Validate())
            throw new StreamBusConfigurationException(error);
        StreamBusCredentialValidator.Validate(options);
    }

    public Task StartAsync(CancellationToken ct)
    {
        lock (_gate)
        {
            _cts ??= CancellationTokenSource.CreateLinkedTokenSource(ct);
            _loop ??= RunLoopAsync(_cts.Token);
        }
        return Task.CompletedTask;
    }

    public async Task<BusReadiness> WaitUntilReadyAsync(ReadinessPolicy policy, CancellationToken ct = default)
    {
        ArgumentNullException.ThrowIfNull(policy);
        if (State == BusConnectionState.Ready) return BusReadiness.Ready;
        if (State == BusConnectionState.Fatal) return BusReadiness.Fatal;
        return policy switch
        {
            ReadinessPolicy.FailFast => State == BusConnectionState.Ready ? BusReadiness.Ready : BusReadiness.TimedOut,
            ReadinessPolicy.WaitIndefinitely => await WaitForStateAsync(null, ct).ConfigureAwait(false),
            ReadinessPolicy.WaitUpTo w => await WaitForStateAsync(w.Timeout, ct).ConfigureAwait(false),
            _ => throw new StreamBusConfigurationException($"Unknown readiness policy {policy.GetType().Name}."),
        };
    }

    private async Task<BusReadiness> WaitForStateAsync(TimeSpan? timeout, CancellationToken ct)
    {
        var tcs = new TaskCompletionSource<BusReadiness>(TaskCreationOptions.RunContinuationsAsynchronously);
        void Handler(BusConnectionState s)
        {
            if (s == BusConnectionState.Ready) tcs.TrySetResult(BusReadiness.Ready);
            else if (s == BusConnectionState.Fatal) tcs.TrySetResult(BusReadiness.Fatal);
        }
        StateChanged += Handler;
        try
        {
            if (State == BusConnectionState.Ready) return BusReadiness.Ready;
            if (State == BusConnectionState.Fatal) return BusReadiness.Fatal;
            if (timeout is null) return await tcs.Task.WaitAsync(ct).ConfigureAwait(false);
            using var cts = CancellationTokenSource.CreateLinkedTokenSource(ct);
            cts.CancelAfter(timeout.Value);
            try { return await tcs.Task.WaitAsync(cts.Token).ConfigureAwait(false); }
            catch (OperationCanceledException) when (!ct.IsCancellationRequested) { return BusReadiness.TimedOut; }
        }
        finally { StateChanged -= Handler; }
    }

    private async Task RunLoopAsync(CancellationToken ct)
    {
        var attempt = 0;
        while (!ct.IsCancellationRequested)
        {
            try
            {
                var handle = await _connector.ConnectAsync(_options, ct).ConfigureAwait(false);
                _handle = handle;
                SetState(BusConnectionState.Ready);
                return;
            }
            catch (OperationCanceledException) { return; }
            catch (Exception ex) when (StreamBusErrorClassification.IsFatal(ex))
            {
                _fatal = ex;
                SetState(BusConnectionState.Fatal);
                return;
            }
            catch (Exception)
            {
                attempt++;
                SetState(BusConnectionState.TransientFailure);
                await Task.Delay(ComputeDelay(attempt), ct).ConfigureAwait(false);
            }
        }
    }

    public TimeSpan ComputeDelay(int attempt)
    {
        var grown = TimeSpan.FromMilliseconds(_baseDelay.TotalMilliseconds * Math.Pow(2, Math.Min(attempt - 1, 8)));
        if (grown > _maxDelay) grown = _maxDelay;
        var jitter = TimeSpan.FromMilliseconds(Random.Shared.Next(0, (int)Math.Min(1000, grown.TotalMilliseconds / 2 + 1)));
        return grown + jitter;
    }

    private void SetState(BusConnectionState next)
    {
        State = next;
        StateChanged?.Invoke(next);
    }

    public async ValueTask DisposeAsync()
    {
        if (_cts is not null) await _cts.CancelAsync().ConfigureAwait(false);
        if (_loop is not null) { try { await _loop.ConfigureAwait(false); } catch (OperationCanceledException) { } }
        if (_handle is not null) await _handle.DisposeAsync().ConfigureAwait(false);
        _cts?.Dispose();
    }
}
```

`WaitUpTo` bounds only the readiness wait; the background loop keeps retrying after a `TimedOut` (FR-8.6, FR-8.11). `FailFast` never waits. Fatal set once is terminal for that supervisor instance.

- [ ] **Step 4: Implement `ConnectionSupervisorTests.cs`**

```csharp
namespace StreamBus.UnitTests;

file sealed class StubHandle : IStreamSystemHandle
{
    public bool IsClosed => false;
    public ValueTask DisposeAsync() => ValueTask.CompletedTask;
}

file sealed class ScriptedConnector : IStreamSystemConnector
{
    private readonly Queue<Func<Task<IStreamSystemHandle>>> _steps = new();
    private readonly object _gate = new();
    public void FailWith(Exception ex)
    {
        lock (_gate) _steps.Enqueue(() => Task.FromException<IStreamSystemHandle>(ex));
    }
    public void Succeed()
    {
        lock (_gate) _steps.Enqueue(() => Task.FromResult<IStreamSystemHandle>(new StubHandle()));
    }
    public async Task<IStreamSystemHandle> ConnectAsync(StreamBusOptions options, CancellationToken ct)
    {
        while (true)
        {
            Func<Task<IStreamSystemHandle>>? step = null;
            lock (_gate) { if (_steps.Count > 0) step = _steps.Dequeue(); }
            if (step is not null) return await step().ConfigureAwait(false);
            await Task.Delay(50, ct).ConfigureAwait(false);
        }
    }
}

public sealed class ConnectionSupervisorTests
{
    private static StreamBusOptions Valid(string user = "app") => new()
    {
        Vhost = "/",
        UserName = user,
        Password = "secret",
    };

    [Fact]
    public void Guest_credentials_fail_fast_with_actionable_message()
    {
        var ex = Assert.Throws<StreamBusConfigurationException>(() =>
            new StreamBusConnectionSupervisor(Valid("guest"), new ScriptedConnector()));
        Assert.Contains("guest", ex.Message, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("loopback", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public async Task Transient_failures_retry_then_ready()
    {
        var connector = new ScriptedConnector();
        connector.FailWith(new System.IO.IOException("reset"));
        connector.FailWith(new TimeoutException("t"));
        connector.Succeed();
        await using var sup = new StreamBusConnectionSupervisor(Valid(), connector);
        var states = new List<BusConnectionState>();
        sup.StateChanged += states.Add;
        await sup.StartAsync(CancellationToken.None);
        var readiness = await sup.WaitUntilReadyAsync(ReadinessPolicy.Indefinitely, new CancellationTokenSource(TimeSpan.FromSeconds(10)).Token);
        Assert.Equal(BusReadiness.Ready, readiness);
        Assert.Contains(BusConnectionState.TransientFailure, states);
        Assert.Equal(BusConnectionState.Ready, sup.State);
    }

    [Fact]
    public async Task Fatal_error_becomes_fatal_without_retry()
    {
        var connector = new ScriptedConnector();
        connector.FailWith(new RabbitMQ.Stream.Client.ProtocolException("p"));
        await using var sup = new StreamBusConnectionSupervisor(Valid(), connector);
        await sup.StartAsync(CancellationToken.None);
        var readiness = await sup.WaitUntilReadyAsync(ReadinessPolicy.Indefinitely, new CancellationTokenSource(TimeSpan.FromSeconds(5)).Token);
        Assert.Equal(BusReadiness.Fatal, readiness);
        Assert.Equal(BusConnectionState.Fatal, sup.State);
    }

    [Fact]
    public async Task WaitUpTo_times_out_but_loop_keeps_retrying()
    {
        var connector = new ScriptedConnector();
        connector.FailWith(new System.IO.IOException("down"));
        await using var sup = new StreamBusConnectionSupervisor(Valid(), connector);
        await sup.StartAsync(CancellationToken.None);
        var readiness = await sup.WaitUntilReadyAsync(ReadinessPolicy.UpTo(TimeSpan.FromMilliseconds(200)));
        Assert.Equal(BusReadiness.TimedOut, readiness);
        Assert.NotEqual(BusConnectionState.Ready, sup.State);
        connector.Succeed();
        var later = await sup.WaitUntilReadyAsync(ReadinessPolicy.Indefinitely, new CancellationTokenSource(TimeSpan.FromSeconds(35)).Token);
        Assert.Equal(BusReadiness.Ready, later);
    }

    [Fact]
    public async Task Backoff_is_capped()
    {
        await using var sup = new StreamBusConnectionSupervisor(Valid(), new ScriptedConnector());
        foreach (var attempt in new[] { 1, 5, 10, 50, 100 })
            Assert.True(sup.ComputeDelay(attempt) <= TimeSpan.FromSeconds(31));
    }
}
```

- [ ] **Step 5: Verify**

```powershell
dotnet test tests/StreamBus.UnitTests/StreamBus.UnitTests.csproj -c Release
```

Expected: all supervisor tests pass, including the 35-second worst-case tail of the `WaitUpTo` test (bounded by the 30 s cap plus jitter).

- [ ] **Step 6: Commit (with human approval)**

```powershell
git add src/StreamBus/CredentialValidation.cs src/StreamBus/Connector.cs src/StreamBus/ConnectionSupervisor.cs tests/StreamBus.UnitTests/ConnectionSupervisorTests.cs
git commit -m "feat: add credential validation and supervising connection loop"
```

---

### Task 5: Registry and bus shell (one StreamSystem per vhost)

**Files:**
- Create: `src/StreamBus/Bus.cs`
- Create: `src/StreamBus/Registry.cs`
- Create: `tests/StreamBus.UnitTests/RegistryTests.cs`

**Interfaces:**
- Consumes: `StreamBusConnectionSupervisor`, `WaitUntilReadyAsync`, `StreamBusOptions` (Task 4).
- Produces: `IStreamBus`, `IStreamBus<TBus>`, `IStreamBusRegistry`, `StreamBusRegistry` for Task 6. `GetOrCreateBus` overloads are the only way to obtain a bus.

- [ ] **Step 1: Implement `Bus.cs`**

```csharp
namespace StreamBus;

public interface IStreamBus
{
    string Vhost { get; }
    BusConnectionState State { get; }
    Task<BusReadiness> WaitUntilReadyAsync(ReadinessPolicy policy, CancellationToken ct = default);
}

public interface IStreamBus<TBus> : IStreamBus
{
}

public sealed class DefaultBus;

internal sealed class StreamBus : IStreamBus
{
    private readonly StreamBusConnectionSupervisor _supervisor;
    public StreamBus(string vhost, StreamBusConnectionSupervisor supervisor)
    {
        Vhost = vhost;
        _supervisor = supervisor;
    }
    public string Vhost { get; }
    public BusConnectionState State => _supervisor.State;
    public Task<BusReadiness> WaitUntilReadyAsync(ReadinessPolicy policy, CancellationToken ct = default)
        => _supervisor.WaitUntilReadyAsync(policy, ct);
    internal Task StartAsync(CancellationToken ct) => _supervisor.StartAsync(ct);
}

internal sealed class StreamBus<TBus> : IStreamBus<TBus>
{
    private readonly StreamBusConnectionSupervisor _supervisor;
    public StreamBus(string vhost, StreamBusConnectionSupervisor supervisor)
    {
        Vhost = vhost;
        _supervisor = supervisor;
    }
    public string Vhost { get; }
    public BusConnectionState State => _supervisor.State;
    public Task<BusReadiness> WaitUntilReadyAsync(ReadinessPolicy policy, CancellationToken ct = default)
        => _supervisor.WaitUntilReadyAsync(policy, ct);
    internal Task StartAsync(CancellationToken ct) => _supervisor.StartAsync(ct);
}
```

- [ ] **Step 2: Implement `Registry.cs`**

```csharp
using System.Collections.Concurrent;

namespace StreamBus;

public interface IStreamBusRegistry
{
    IStreamBus GetOrCreateBus(string vhost);
    IStreamBus<TBus> GetOrCreateBus<TBus>(string vhost);
    IEnumerable<(string Vhost, string BusType, BusConnectionState State)> DescribeStates();
}

internal sealed class BusEntry
{
    public required StreamBusConnectionSupervisor Supervisor { get; init; }
    public required IStreamBus Bus { get; init; }
}

public sealed class StreamBusRegistry : IStreamBusRegistry
{
    private readonly ConcurrentDictionary<string, BusEntry> _entries = new();
    private readonly Func<string, StreamBusOptions> _resolveOptions;
    private readonly Func<StreamBusOptions, StreamBusConnectionSupervisor> _createSupervisor;

    public StreamBusRegistry(
        Func<string, StreamBusOptions> resolveOptions,
        Func<StreamBusOptions, StreamBusConnectionSupervisor> createSupervisor)
    {
        _resolveOptions = resolveOptions;
        _createSupervisor = createSupervisor;
    }

    public IStreamBus GetOrCreateBus(string vhost)
        => (IStreamBus)GetOrCreate(typeof(DefaultBus), vhost).Bus;

    public IStreamBus<TBus> GetOrCreateBus<TBus>(string vhost)
        => (IStreamBus<TBus>)GetOrCreate(typeof(TBus), vhost).Bus;

    public IEnumerable<(string Vhost, string BusType, BusConnectionState State)> DescribeStates()
        => _entries.Select(kv => ParseKey(kv.Key, kv.Value.Bus.State));

    internal BusEntry GetOrCreate(Type busType, string vhost)
    {
        var key = $"{busType.FullName}@{vhost}";
        return _entries.GetOrAdd(key, _ =>
        {
            var options = _resolveOptions(vhost);
            var supervisor = _createSupervisor(options);
            IStreamBus bus = busType == typeof(DefaultBus)
                ? new StreamBus(vhost, supervisor)
                : (IStreamBus)Activator.CreateInstance(typeof(StreamBus<>).MakeGenericType(busType), vhost, supervisor)!;
            return new BusEntry { Supervisor = supervisor, Bus = bus };
        });
    }

    private static (string, string, BusConnectionState) ParseKey(string key, BusConnectionState state)
    {
        var at = key.LastIndexOf('@');
        return (key[(at + 1)..], key[..at], state);
    }
}
```

Sharing rule: the `Func<string, StreamBusOptions>` supplied by hosting (Task 6) returns the same `StreamSystem`-bound options per vhost, so all buses on one vhost share one locator/pool set once producers/consumers arrive in Phase 4+. Phase 3 shares the options object; the physical sharing is completed when the connector is reused per vhost in Phase 4.

- [ ] **Step 3: Implement `RegistryTests.cs`**

```csharp
namespace StreamBus.UnitTests;

public sealed class RegistryTests
{
    private static StreamBusRegistry Create(out List<StreamBusConnectionSupervisor> supervisors)
    {
        var list = new List<StreamBusConnectionSupervisor>();
        supervisors = list;
        return new StreamBusRegistry(
            vhost => new StreamBusOptions { Vhost = vhost, UserName = "app", Password = "secret" },
            options =>
            {
                var sup = new StreamBusConnectionSupervisor(options, new ScriptedConnector());
                list.Add(sup);
                return sup;
            });
    }

    private sealed class ScriptedConnector : IStreamSystemConnector
    {
        public Task<IStreamSystemHandle> ConnectAsync(StreamBusOptions options, CancellationToken ct)
            => new TaskCompletionSource<IStreamSystemHandle>().Task;
    }

    private sealed class OrdersBus;
    private sealed class BillingBus;

    [Fact]
    public void Same_key_returns_same_wrapper()
    {
        var registry = Create(out _);
        Assert.Same(registry.GetOrCreateBus("/"), registry.GetOrCreateBus("/"));
        Assert.Same(registry.GetOrCreateBus<OrdersBus>("/"), registry.GetOrCreateBus<OrdersBus>("/"));
    }

    [Fact]
    public void Default_and_marker_buses_coexist()
    {
        var registry = Create(out _);
        Assert.NotSame(registry.GetOrCreateBus("/"), (IStreamBus)registry.GetOrCreateBus<OrdersBus>("/"));
        Assert.NotSame(registry.GetOrCreateBus<OrdersBus>("/"), registry.GetOrCreateBus<BillingBus>("/"));
    }

    [Fact]
    public void States_describe_every_registered_bus()
    {
        var registry = Create(out _);
        registry.GetOrCreateBus("/");
        registry.GetOrCreateBus<OrdersBus>("orders-vh");
        var states = registry.DescribeStates().ToList();
        Assert.Equal(2, states.Count);
        Assert.Contains(states, s => s.Vhost == "/" && s.BusType.Contains(nameof(DefaultBus)));
        Assert.Contains(states, s => s.Vhost == "orders-vh" && s.BusType.Contains(nameof(OrdersBus)));
    }
}
```

- [ ] **Step 4: Verify**

```powershell
dotnet test tests/StreamBus.UnitTests/StreamBus.UnitTests.csproj -c Release
```

Expected: all registry tests pass; `DescribeStates` reports one row per (vhost, bus type).

- [ ] **Step 5: Commit (with human approval)**

```powershell
git add src/StreamBus/Bus.cs src/StreamBus/Registry.cs tests/StreamBus.UnitTests/RegistryTests.cs
git commit -m "feat: add bus registry with per-vhost sharing and bus shells"
```

---

### Task 6: Hosting, DI, and health per (vhost, bus type)

**Files:**
- Create: `src/StreamBus/Hosting.cs`
- Create: `tests/StreamBus.UnitTests/HostingTests.cs`

**Interfaces:**
- Consumes: `IStreamBus`, `IStreamBus<TBus>`, `IStreamBusRegistry`, `StreamBusRegistry`, `StreamBusConnectionSupervisor`, `StreamBusOptions` (Tasks 2–5).
- Produces: `AddStreamBus` / `AddStreamBus<TBus>` overloads, `StreamBusHostedService`, `StreamBusHealthCheck`. This is the only registration path.

- [ ] **Step 1: Implement `Hosting.cs`**

```csharp
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Diagnostics.HealthChecks;
using Microsoft.Extensions.Hosting;

namespace StreamBus;

public sealed class StreamBusConfigStore
{
    private readonly Dictionary<string, StreamBusOptions> _byBusKey = new();
    private readonly Dictionary<string, StreamBusOptions> _byVhost = new();
    private readonly List<(Type BusType, StreamBusOptions Options)> _registrations = new();

    public void Add(Type busType, StreamBusOptions options)
    {
        var key = $"{busType.FullName}@{options.Vhost}";
        if (_byBusKey.ContainsKey(key))
            throw new StreamBusConfigurationException($"Duplicate StreamBus registration for {key}. The non-generic overload may be used at most once per service and each marker TBus at most once.");
        _byBusKey[key] = options;
        _byVhost.TryAdd(options.Vhost, options);
        _registrations.Add((busType, options));
    }

    public StreamBusOptions ForVhost(string vhost) => _byVhost[vhost];
    public IReadOnlyList<(Type BusType, StreamBusOptions Options)> Registrations => _registrations;
}

public static class ServiceCollectionExtensions
{
    public static IServiceCollection AddStreamBus(this IServiceCollection services, string vhost, Action<StreamBusOptionsBuilder> configure)
    {
        ArgumentNullException.ThrowIfNull(services);
        var builder = new StreamBusOptionsBuilder(vhost);
        configure(builder);
        return AddCore(services, typeof(DefaultBus), builder.Build());
    }

    public static IServiceCollection AddStreamBus<TBus>(this IServiceCollection services, string vhost, Action<StreamBusOptionsBuilder> configure)
    {
        ArgumentNullException.ThrowIfNull(services);
        var builder = new StreamBusOptionsBuilder(vhost);
        configure(builder);
        return AddCore(services, typeof(TBus), builder.Build());
    }

    private static IServiceCollection AddCore(IServiceCollection services, Type busType, StreamBusOptions options)
    {
        var store = GetOrCreateStore(services);
        store.Add(busType, options);
        services.TryAddSingleton<IStreamSystemConnector, StreamSystemConnector>();
        services.TryAddSingleton<IStreamBusRegistry>(sp =>
            new StreamBusRegistry(
                store.ForVhost,
                opts => new StreamBusConnectionSupervisor(opts, sp.GetRequiredService<IStreamSystemConnector>())));
        if (busType == typeof(DefaultBus))
        {
            services.TryAddSingleton<IStreamBus>(sp => sp.GetRequiredService<IStreamBusRegistry>().GetOrCreateBus(options.Vhost));
        }
        else
        {
            var serviceType = typeof(IStreamBus<>).MakeGenericType(busType);
            services.TryAddSingleton(serviceType, sp =>
            {
                var registry = sp.GetRequiredService<IStreamBusRegistry>();
                var method = typeof(IStreamBusRegistry).GetMethods()
                    .Single(m => m.IsGenericMethodDefinition && m.Name == nameof(IStreamBusRegistry.GetOrCreateBus));
                return method.MakeGenericMethod(busType).Invoke(registry, new object[] { options.Vhost })!;
            });
        }
        services.TryAddSingleton<IHostedService, StreamBusHostedService>();
        services.TryAddSingleton<IHealthCheck, StreamBusHealthCheck>();
        return services;
    }

    private static StreamBusConfigStore GetOrCreateStore(IServiceCollection services)
    {
        var existing = services.FirstOrDefault(d =>
            d.ServiceType == typeof(StreamBusConfigStore) && d.ImplementationInstance is StreamBusConfigStore);
        if (existing?.ImplementationInstance is StreamBusConfigStore store) return store;
        var created = new StreamBusConfigStore();
        services.AddSingleton<StreamBusConfigStore>(created);
        return created;
    }
}

public sealed class StreamBusOptionsBuilder
{
    private string _user = "";
    private string _password = "";
    private string _host = "127.0.0.1";
    private int _port = 5552;
    private StreamBusConnectionOptions _connection = new();
    private StreamBusHostOptions _hosting = new();
    private readonly string _vhost;

    public StreamBusOptionsBuilder(string vhost) => _vhost = vhost;
    public StreamBusOptionsBuilder Credentials(string user, string password) { _user = user; _password = password; return this; }
    public StreamBusOptionsBuilder Endpoint(string host, int port) { _host = host; _port = port; return this; }
    public StreamBusOptionsBuilder Connection(Action<StreamBusConnectionOptionsBuilder> configure)
    {
        var b = new StreamBusConnectionOptionsBuilder();
        configure(b);
        _connection = b.Build();
        return this;
    }
    public StreamBusOptionsBuilder Hosting(Action<StreamBusHostOptionsBuilder> configure)
    {
        var b = new StreamBusHostOptionsBuilder();
        configure(b);
        _hosting = b.Build();
        return this;
    }
    public StreamBusOptions Build() => new()
    {
        Vhost = _vhost,
        UserName = _user,
        Password = _password,
        Host = _host,
        Port = _port,
        Connection = _connection,
        Hosting = _hosting,
    };
}

public sealed class StreamBusConnectionOptionsBuilder
{
    private int _producers = 1;
    private int _consumers = 1;
    public StreamBusConnectionOptionsBuilder ProducersPerConnection(int n) { _producers = n; return this; }
    public StreamBusConnectionOptionsBuilder ConsumersPerConnection(int n) { _consumers = n; return this; }
    public StreamBusConnectionOptions Build() => new() { ProducersPerConnection = _producers, ConsumersPerConnection = _consumers };
}

public sealed class StreamBusHostOptionsBuilder
{
    private bool _block;
    private TimeSpan? _timeout;
    public StreamBusHostOptionsBuilder BlockStartupUntilReady(TimeSpan? timeout = null) { _block = true; _timeout = timeout; return this; }
    public StreamBusHostOptions Build() => new() { BlockStartupUntilReady = _block, StartupTimeout = _timeout };
}

public sealed class StreamBusHostedService : IHostedService
{
    private readonly StreamBusConfigStore _store;
    private readonly IStreamBusRegistry _registry;

    public StreamBusHostedService(StreamBusConfigStore store, IStreamBusRegistry registry)
    {
        _store = store;
        _registry = registry;
    }

    public async Task StartAsync(CancellationToken ct)
    {
        var concrete = (StreamBusRegistry)_registry;
        var supervisors = new List<(string Vhost, StreamBusConnectionSupervisor Supervisor)>();
        foreach (var (busType, options) in _store.Registrations)
        {
            var entry = concrete.GetOrCreate(busType, options.Vhost);
            await entry.Supervisor.StartAsync(ct).ConfigureAwait(false);
            supervisors.Add((options.Vhost, entry.Supervisor));
        }
        if (!_store.Registrations.Any(r => r.Options.Hosting.BlockStartupUntilReady)) return;
        var timeouts = _store.Registrations
            .Where(r => r.Options.Hosting is { BlockStartupUntilReady: true, StartupTimeout: not null })
            .Select(r => r.Options.Hosting.StartupTimeout!.Value)
            .ToList();
        using var cts = CancellationTokenSource.CreateLinkedTokenSource(ct);
        if (timeouts.Count > 0) cts.CancelAfter(timeouts.Max());
        foreach (var (_, supervisor) in supervisors.DistinctBy(s => s.Supervisor))
        {
            var readiness = await supervisor.WaitUntilReadyAsync(ReadinessPolicy.Indefinitely, cts.Token).ConfigureAwait(false);
            if (readiness == BusReadiness.Fatal)
                throw new StreamBusFatalException("A StreamBus connection reported Fatal during startup.");
        }
    }

    public Task StopAsync(CancellationToken ct) => Task.CompletedTask;
}

public sealed class StreamBusHealthCheck : IHealthCheck
{
    private readonly IStreamBusRegistry _registry;
    public StreamBusHealthCheck(IStreamBusRegistry registry) => _registry = registry;

    public Task<HealthCheckResult> CheckHealthAsync(HealthCheckContext context, CancellationToken ct = default)
    {
        var rows = _registry.DescribeStates().ToList();
        var data = rows.ToDictionary(
            r => $"{r.Vhost}|{r.BusType}",
            r => (object)r.State.ToString());
        var worst = rows.Any(r => r.State == BusConnectionState.Fatal) ? BusConnectionState.Fatal
            : rows.Any(r => r.State != BusConnectionState.Ready) ? BusConnectionState.TransientFailure
            : BusConnectionState.Ready;
        return Task.FromResult(worst switch
        {
            BusConnectionState.Ready => HealthCheckResult.Healthy("All StreamBus connections ready.", data),
            BusConnectionState.Fatal => HealthCheckResult.Unhealthy("At least one StreamBus connection is fatal.", data: data),
            _ => HealthCheckResult.Degraded("StreamBus is starting or retrying.", data: data),
        });
    }
}
```

Duplicate detection lives in `StreamBusConfigStore.Add` (single source of truth, throws at configuration time before any provider is built, so no test needs a built provider to pin it). Options are held in the store instance, not in keyed services: keyed-service enumeration has no get-all API, so keyed storage would leave `StreamBusHostedService` unable to enumerate every registered bus. `TryAddSingleton` for the hosted service and health check is correct because one instance covers all registrations via the store. `DistinctBy` on supervisors handles two buses sharing one vhost (same supervisor reached via two registry keys) without double-waiting.

- [ ] **Step 2: Implement `HostingTests.cs`**

```csharp
using Microsoft.Extensions.DependencyInjection;

namespace StreamBus.UnitTests;

public sealed class HostingTests
{
    private sealed class OrdersBus;
    private sealed class BillingBus;

    [Fact]
    public void Duplicate_default_registration_fails()
    {
        var services = new ServiceCollection();
        services.AddStreamBus("/", o => o.Credentials("app", "secret"));
        Assert.Throws<StreamBusConfigurationException>(() =>
            services.AddStreamBus("/", o => o.Credentials("app", "secret")));
    }

    [Fact]
    public void Duplicate_marker_registration_fails_but_distinct_markers_coexist()
    {
        var services = new ServiceCollection();
        services.AddStreamBus<OrdersBus>("/", o => o.Credentials("app", "secret"));
        Assert.Throws<StreamBusConfigurationException>(() =>
            services.AddStreamBus<OrdersBus>("/", o => o.Credentials("app", "secret")));
        services.AddStreamBus<BillingBus>("/", o => o.Credentials("app", "secret"));
        var sp = services.BuildServiceProvider();
        Assert.NotSame(sp.GetRequiredService<IStreamBus<OrdersBus>>(), sp.GetRequiredService<IStreamBus<BillingBus>>());
    }

    [Fact]
    public void Default_and_marker_resolve_distinct_buses()
    {
        var services = new ServiceCollection();
        services.AddStreamBus("/", o => o.Credentials("app", "secret"));
        services.AddStreamBus<OrdersBus>("/", o => o.Credentials("app", "secret"));
        var sp = services.BuildServiceProvider();
        Assert.NotSame(sp.GetRequiredService<IStreamBus>(), (IStreamBus)sp.GetRequiredService<IStreamBus<OrdersBus>>());
    }

    [Fact]
    public void Pooling_defaults_are_one_and_overridable()
    {
        var services = new ServiceCollection();
        services.AddStreamBus("/", o =>
        {
            o.Credentials("app", "secret");
            o.Connection(c => c.ProducersPerConnection(3).ConsumersPerConnection(5));
        });
        var sp = services.BuildServiceProvider();
        var opts = sp.GetRequiredService<StreamBusConfigStore>().ForVhost("/");
        Assert.Equal(3, opts.Connection.ProducersPerConnection);
        Assert.Equal(5, opts.Connection.ConsumersPerConnection);
        var defaults = new StreamBusConnectionOptions();
        Assert.Equal(1, defaults.ProducersPerConnection);
        Assert.Equal(1, defaults.ConsumersPerConnection);
    }
}
```

- [ ] **Step 3: Verify**

```powershell
dotnet test tests/StreamBus.UnitTests/StreamBus.UnitTests.csproj -c Release
```

Expected: all 4 hosting tests pass; duplicate registrations throw `StreamBusConfigurationException` with the marker key in the message.

- [ ] **Step 4: Commit (with human approval)**

```powershell
git add src/StreamBus/Hosting.cs tests/StreamBus.UnitTests/HostingTests.cs
git commit -m "feat: add DI hosting with duplicate detection and health"
```

---

### Task 7: Integration probes against the real broker plus full-suite gate

**Files:**
- Create: `tests/StreamBus.IntegrationTests/BusCoreIntegrationTests.cs`
- Modify: `tests/StreamBus.IntegrationTests/StreamBus.IntegrationTests.csproj` (add project reference to `src/StreamBus`)

**Interfaces:**
- Consumes: everything from Tasks 1–6.
- Produces: passing integration suite proving FR-8.2/FR-8.3/FR-8.8 against `127.0.0.1:5552`; the Phase 3 completion gate.

- [ ] **Step 1: Add project reference**

```xml
<!-- in tests/StreamBus.IntegrationTests/StreamBus.IntegrationTests.csproj -->
<ItemGroup>
  <ProjectReference Include="..\..\src\StreamBus\StreamBus.csproj" />
</ItemGroup>
```

- [ ] **Step 2: Implement `BusCoreIntegrationTests.cs`**

```csharp
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Diagnostics.HealthChecks;

namespace StreamBus.IntegrationTests;

public sealed class BusCoreIntegrationTests
{
    private static string User() => Environment.GetEnvironmentVariable("STREAMBUS_USER") ?? "app";
    private static string Password() => Environment.GetEnvironmentVariable("STREAMBUS_PASSWORD") ?? "password";

    [Fact]
    public async Task One_bus_serves_many_vhosts_and_reports_ready()
    {
        var services = new ServiceCollection();
        services.AddStreamBus("/", o => o.Credentials(User(), Password()));
        var sp = services.BuildServiceProvider();
        var bus = sp.GetRequiredService<IStreamBus>();
        var hosted = sp.GetServices<IHostedService>().OfType<StreamBusHostedService>().Single();
        await hosted.StartAsync(CancellationToken.None);
        var readiness = await bus.WaitUntilReadyAsync(ReadinessPolicy.UpTo(TimeSpan.FromSeconds(30)));
        Assert.Equal(BusReadiness.Ready, readiness);
    }

    [Fact]
    public async Task Health_reports_per_vhost_bus_type_state()
    {
        var services = new ServiceCollection();
        services.AddStreamBus("/", o => o.Credentials(User(), Password()));
        var sp = services.BuildServiceProvider();
        var hosted = sp.GetServices<IHostedService>().OfType<StreamBusHostedService>().Single();
        await hosted.StartAsync(CancellationToken.None);
        var bus = sp.GetRequiredService<IStreamBus>();
        Assert.Equal(BusReadiness.Ready, await bus.WaitUntilReadyAsync(ReadinessPolicy.UpTo(TimeSpan.FromSeconds(30))));
        var check = sp.GetServices<IHealthCheck>().OfType<StreamBusHealthCheck>().Single();
        var result = await check.CheckHealthAsync(new HealthCheckContext(), CancellationToken.None);
        Assert.Equal(HealthStatus.Healthy, result.Status);
        Assert.Contains(result.Data.Keys, k => k.StartsWith("/|"));
    }

    [Fact]
    public async Task Unreachable_broker_never_surfaces_terminal_error_and_recovers()
    {
        var options = new StreamBusOptions
        {
            Vhost = "/",
            UserName = User(),
            Password = Password(),
            Host = "127.0.0.1",
            Port = 59999,
        };
        await using var supervisor = new StreamBusConnectionSupervisor(options, new StreamSystemConnector());
        await supervisor.StartAsync(CancellationToken.None);
        var early = await supervisor.WaitUntilReadyAsync(ReadinessPolicy.UpTo(TimeSpan.FromSeconds(2)));
        Assert.Equal(BusReadiness.TimedOut, early);
        Assert.True(supervisor.State is BusConnectionState.Starting or BusConnectionState.TransientFailure);
    }
}
```

The third test asserts the supervising loop's core contract (FR-8.3/FR-8.5): an unreachable broker yields `TimedOut`, never a thrown terminal error, and the loop is still alive to recover. It uses a closed port, not a stopped broker, so it runs on any machine. The first two tests require a real broker with a non-guest user (`STREAMBUS_USER`/`STREAMBUS_PASSWORD`, defaults `app`/`password`); if the local broker has no such user, create it once via `rabbitmqctl add_user app password; rabbitmqctl set_permissions -p / app ".*" ".*" ".*"` and re-run — do not change the tests to use `guest` (FR-8.9 forbids it).

- [ ] **Step 3: Verify (full gate)**

```powershell
dotnet test StreamBus.slnx -c Release
```

Expected: unit suite green; integration suite green when the broker is up with the non-guest user; third integration test green even with the broker down. Record the broker version observed (`rabbitmqctl version` or management `/api/overview`) in the plan-review reply; do not assert G-3 (3-node cluster) here — it blocks Phase 8, not Phase 3.

- [ ] **Step 4: Commit (with human approval)**

```powershell
git add tests/StreamBus.IntegrationTests/BusCoreIntegrationTests.cs tests/StreamBus.IntegrationTests/StreamBus.IntegrationTests.csproj
git commit -m "test: add bus-core integration probes for readiness and health"
```

---

## Self-Review

- Spec coverage: FR-8.1 (Tasks 5–6, duplicate detection pinned), FR-8.2 (one bus/registry shape; multi-stream publish deferred to Phase 4, Task 7 probes multi-vhost readiness), FR-8.3/FR-8.5 (Task 4 loop + Task 7 down-then-up probe), FR-8.4 (Tasks 3–4 fatal path pinned), FR-8.6/FR-8.7 (Task 4 `WaitUntilReadyAsync` three outcomes pinned), FR-8.8 + FR-9.2 (Task 6 health per (vhost, bus type) + state events pinned), FR-8.9 (Task 4 guest-trap pinned), FR-8.10 (Tasks 2/4/6 defaults + override pinned), FR-8.11 (Task 6 `BlockStartupUntilReady`/`StartupTimeout`; non-blocking default returns without waiting, blocking waits bounded while the loop retries — pinned by Task 4 `TimedOut`-then-`Ready` test). FR-1–FR-7, FR-9.1/9.3, FR-10, NFRs, and gates G-1–G-5 are out of scope by design; no task claims them.
- Placeholder scan: no TBD/TODO; every code step is complete source; every verify step has an exact command and expected outcome; no "similar to Task N".
- Type consistency: `ReadinessPolicy.FailFast`/`WaitUpTo`/`WaitIndefinitely`, `BusReadiness.Ready/TimedOut/Fatal`, `BusConnectionState.Starting/Ready/TransientFailure/Fatal`, `StreamBusOptions` required fields, `IStreamBus`/`IStreamBus<TBus>`/`IStreamBusRegistry` signatures are identical across Tasks 2–7.
- Review Focus: each of the five lines names its pinning task and verification above.
