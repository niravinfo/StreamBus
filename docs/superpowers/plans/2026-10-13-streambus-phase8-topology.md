# StreamBus Phase 8 (Topology with Serialized Declaration) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Implement serialized, cached, idempotent topology declaration (`IStreamTopology.EnsureStreamAsync` / `EnsureSuperStreamAsync`) with cross-process race tolerance and conflict detection, plus startup declaration in both hosting modes.

**Architecture:** A per-vhost `StreamTopologyRegistry` serializes declaration per stream name (semaphore pool), short-circuits repeats on a declaration fingerprint cache, and resolves every already-exists outcome through re-probe + compare (super partition counts are readable; plain-stream knobs are not — see rulings), never through broker code inspection. The hosted service unions all registrations' declarations per vhost and declares them in a retry-until-cancelled background task that blocking startup awaits. G-3's multi-node behavior is cluster-conditional and stays open on a single node.

**Tech Stack:** .NET 10 (`net10.0`), C# latest, `RabbitMQ.Stream.Client` 1.12.1 (same pins — no new packages), xUnit v3, `StreamBus.slnx`, Central Package Management. Test-only HTTP (built-in `HttpClient`, no package) for the management-API-conditional policy test.

**Spec:** `docs/2026-10-08-streambus-design.md` (§8 topology, §8.1 leader locator, §8.2 retention, §10 row 8) and `docs/2026-10-08-streambus-requirements.md` (FR-7.1–FR-7.5, G-3). Gate G-3 (3-node cluster) is an environment requirement: it stays OPEN on the single-node broker; its test is written cluster-conditional (skips) in Task 5.

## Global Constraints

- `TargetFramework` exactly `net10.0`; `Nullable`/`ImplicitUsings` enable; `TreatWarningsAsErrors` true; `ManagePackageVersionsCentrally` true; `IsTestingPlatformApplication` false (all in `Directory.Build.props`, unchanged).
- `RabbitMQ.Stream.Client` exactly `1.12.1`; `Microsoft.Extensions.Hosting` / `Diagnostics.HealthChecks` / `Logging.Abstractions` exactly `10.0.12`; `xunit.v3` 4.0.2, `xunit.runner.visualstudio` 4.0.1, `Microsoft.NET.Test.Sdk` 18.10.1.
- Library root namespace `StreamBus`; never `RabbitMQ.Stream.*`.
- This phase SANCTIONS stream creation in `src/` for the first time — and only for topology: a `src/` grep for `DeleteStream|DeleteSuperStream` must return nothing, and `CreateStream|CreateSuperStream` may appear only in `Connector.cs` (handle pass-through) and `TopologyRegistry.cs` (via the handle). The old blanket prohibition is superseded for creates; deletes stay forbidden (admin lifecycle is test-only).
- Conflict detection NEVER inspects broker response codes or parses exception messages: `CreateStreamException` derives `Exception` directly (no code property — probed), and spec arguments are unreadable via the stream protocol (`StreamInfo` carries only leader/replicas; `StreamStats` only offsets). Every already-exists outcome resolves through re-probe + compare.
- The library never deletes streams, never declares from the publish/consume paths (missing streams stay `StreamBusStreamNotFoundException`, never implicit create), and never touches replica placement (NFR-5).
- Integration tests require the local broker at `localhost:5552` with non-guest creds (`STREAMBUS_USER`/`STREAMBUS_PASSWORD`, defaulting to `app`/`password` in-test); never use `guest`.
- Every commit step requires explicit human approval before running; never push, never amend without permission.

## Review Focus

- Two processes declaring the same super-stream name with different partition counts must make the second throw a conflict naming expected vs actual counts — never silently adopt the existing partitions. Pinned by Task 5 super-partition-conflict test.
- Four concurrent same-spec declares (the simultaneous multi-instance startup case) must issue exactly one broker create and all succeed. Pinned by Task 4 single-create unit test + Task 5 all-succeed test.
- A restart declaring the identical spec must succeed without throwing, under either broker already-exists behavior. Pinned by Task 5 restart test (+ Task 4 repeat single-create test).
- Blocking startup whose declarations conflict with pre-existing streams must fail `StartAsync` loudly with the conflict — never hang, never silently continue undeclared. Pinned by Task 5 startup-conflict test.
- A knob-only difference observed pre-existing succeeds with a warning, while the same difference surfacing as a broker create-rejection throws a conflict (identical re-declares always succeed per probes, so a rejection means specs differed); order-dependence is inherent to protocol-unreadable knobs — keep declarations in sync across processes. Pinned by Task 5 knob test + Task 4 knob-conflict tests (fallback branch: both succeed+warn — see Task 2 Step 0).

---

## File Structure

New files (no new packages):

- `src/StreamBus/Topology.cs` — `StreamLeaderLocator` enum, `StreamDeclaration` + `SuperStreamDeclaration` + `StreamTopologyDeclarations` records, `IStreamTopology`, `TopologyMapping` (library → client specs), `TopologyValidation`, `TopologyDescribe` (expected/actual rendering), `StreamTopologyDeclarationsBuilder` + `StreamDeclarationKnobsBuilder`.
- `src/StreamBus/TopologyRegistry.cs` — `StreamTopologyRegistry : IStreamTopology` (per-name gates, fingerprint cache, create-with-race-tolerance, re-probe + compare, conflict construction).

Modified Phase 3–7 files (same branch lineage; existing suites must stay green):

- `src/StreamBus/Exceptions.cs` — add `StreamConfigurationConflictException` (design-named, with `Stream`/`Expected`/`Actual` properties).
- `src/StreamBus/Connector.cs` — `IStreamSystemHandle` gains `CreateStreamAsync` + `CreateSuperStreamAsync` (pass-through, no interpretation).
- `src/StreamBus/Options.cs` — `StreamBusOptions.Topology` (default empty).
- `src/StreamBus/Hosting.cs` — `StreamBusOptionsBuilder.Topology(...)`, `StreamBusHostedService` startup declaration (background + blocking-await, stop-cancel, optional logger).
- `src/StreamBus/Registry.cs` — `TopologyFor(vhost)` + pass into bus construction (+ `IStreamBusRegistry.TopologyFor` internal member, precedent: `SupervisorFor`).
- `src/StreamBus/Bus.cs` — `IStreamTopology Topology { get; }` on the interface + both implementations (ctor gains trailing topology param).

Tests:

- `tests/StreamBus.UnitTests/TopologyTests.cs` — mapping (incl. least-leaders default + Args emission per Task 1 table), validation matrix, builder validation, idempotency (repeat = zero further broker calls), 4-concurrent single-create, race/conflict/kind-mismatch/vanish matrix with scripted fakes.
- `tests/StreamBus.IntegrationTests/TopologyIntegrationTests.cs` — repeat + concurrent converge, super partition conflict (expected vs actual), kind mismatches, restart converges, startup both modes (+ startup conflict), retention usability, management-policy conditional, G-3 cluster conditional.

Explicitly out of Phase 8 (deferred, not stubs): `BindingsSuperStreamSpec` (partitions kind only in v1); `balanced` locator value (client 1.12.1 offers no member — `LeastLeaders` is the default and only modern choice); library-side policy management (policies are operator-owned; the library carries no policy API); stream deletion (never); typed `IConsumer<T>` (Phase 9); per-declaration timeout knobs (readiness is FailFast; the startup loop owns retry).

Recorded rulings (spec as authority; bind all tasks): conflict detection uses re-probe + compare exclusively — no code inspection (no code exists on the create path), no message parsing; super partition-count mismatch and super/plain kind mismatch throw `StreamConfigurationConflictException` with readable actuals; plain knob-only differences succeed with a warning (protocol-unreadable residual — specs must be kept in sync); same-spec N-instance startup always converges (unknown outcomes bias to compare-then-succeed, never to spurious throw); `SameConnection` ignores topology (connection identity only) and the hosted service unions every registration's declarations per vhost; locator defaults to `LeastLeaders` and is always pinned explicitly (FR-7.3); retention knobs exist for design-required cases (error streams, FR-5.2) while production tuning belongs in policies (FR-7.4, operator domain, conditional test only); G-3 stays open on a single node with its test written skip-guarded.

---

### Task 1: Live-broker probe + Args emission + management reachability

**Files:**
- Create: none (probe is throwaway, outside the repo; findings constrain Tasks 2–6).

**Interfaces:**
- Consumes: broker `localhost:5552` (non-guest), management plugin `http://127.0.0.1:15672` (same creds).
- Produces: verbatim behavior table (identical vs conflicting re-declare outcomes, partition naming, Args emission, StreamInfo content, management reachability) for Tasks 2–6.

- [ ] **Step 1: Run the live topology probe (throwaway console app, record everything)**

Create a throwaway app OUTSIDE the repo (as-probed corrections from Phase 7 already applied: `ImplicitUsings` on, `System.Buffers` using, plain `var` + explicit `.Dispose()` — client interfaces do not extend `IDisposable` — `new Message(...)` wraps on every send):

```powershell
$probe = "$env:TEMP/opencode/topo-probe"
Remove-Item -Recurse -Force $probe -ErrorAction SilentlyContinue
New-Item -ItemType Directory -Force $probe | Out-Null
```

`$probe/probe.csproj`:

```xml
<Project Sdk="Microsoft.NET.Sdk">
  <PropertyGroup>
    <OutputType>Exe</OutputType>
    <TargetFramework>net10.0</TargetFramework>
    <Nullable>enable</Nullable>
    <ImplicitUsings>enable</ImplicitUsings>
  </PropertyGroup>
  <ItemGroup>
    <PackageReference Include="RabbitMQ.Stream.Client" Version="1.12.1" />
    <PackageReference Include="Microsoft.Extensions.Logging.Abstractions" Version="10.0.12" />
  </ItemGroup>
</Project>
```

`$probe/Program.cs` (exact):

```csharp
using System.Buffers;
using System.Net;
using System.Text;
using Microsoft.Extensions.Logging.Abstractions;
using RabbitMQ.Stream.Client;

var user = Environment.GetEnvironmentVariable("STREAMBUS_USER") is { Length: > 0 } u ? u : "app";
var pass = Environment.GetEnvironmentVariable("STREAMBUS_PASSWORD") is { Length: > 0 } p ? p : "password";
var uid = Guid.NewGuid().ToString("N")[..8];
var plain = $"probe-topo-plain-{uid}";
var super = $"probe-topo-super-{uid}";

static string Ex(Exception ex) => ex.GetType().FullName + " | asCreateException=" + (ex as CreateException is null ? "<null>" : (ex as CreateException)!.ResponseCode.ToString()) + " | msg=" + ex.Message.Split('\n')[0];

await using var system = await StreamSystem.Create(new StreamSystemConfig
{
    UserName = user, Password = pass, VirtualHost = "/",
    Endpoints = new List<EndPoint> { new IPEndPoint(IPAddress.Loopback, 5552) },
});
try
{
    // (a) identical plain re-declare
    await system.CreateStream(new StreamSpec(plain));
    try { await system.CreateStream(new StreamSpec(plain)); Console.WriteLine("PLAIN-IDENTICAL: success"); }
    catch (Exception ex) { Console.WriteLine("PLAIN-IDENTICAL-THROWS: " + Ex(ex)); }
    // (b) conflicting plain re-declare (retention knob differs)
    try { await system.CreateStream(new StreamSpec(plain) { MaxAge = TimeSpan.FromDays(7) }); Console.WriteLine("PLAIN-KNOB-DIFF: success"); }
    catch (Exception ex) { Console.WriteLine("PLAIN-KNOB-DIFF-THROWS: " + Ex(ex)); }
    // (c) identical super re-declare
    await system.CreateSuperStream(new PartitionsSuperStreamSpec(super, 3));
    try { await system.CreateSuperStream(new PartitionsSuperStreamSpec(super, 3)); Console.WriteLine("SUPER-IDENTICAL: success"); }
    catch (Exception ex) { Console.WriteLine("SUPER-IDENTICAL-THROWS: " + Ex(ex)); }
    // (d) conflicting super re-declare (partitions differ)
    try { await system.CreateSuperStream(new PartitionsSuperStreamSpec(super, 5)); Console.WriteLine("SUPER-PART-DIFF: success"); }
    catch (Exception ex) { Console.WriteLine("SUPER-PART-DIFF-THROWS: " + Ex(ex)); }
    // (e) kind clashes
    try { await system.CreateSuperStream(new PartitionsSuperStreamSpec(plain, 2)); Console.WriteLine("SUPER-ON-PLAIN-NAME: success"); }
    catch (Exception ex) { Console.WriteLine("SUPER-ON-PLAIN-NAME-THROWS: " + Ex(ex)); }
    try { await system.CreateStream(new StreamSpec(super)); Console.WriteLine("PLAIN-ON-SUPER-NAME: success"); }
    catch (Exception ex) { Console.WriteLine("PLAIN-ON-SUPER-NAME-THROWS: " + Ex(ex)); }
    // (f) partition naming + info on single node
    Console.WriteLine("PARTITIONS: " + string.Join(",", await system.QueryPartition(super)));
    var info = await system.StreamInfo(plain);
    Console.WriteLine($"STREAMINFO: code={info.ResponseCode} replicas={info.Replicas.Count}");
    // (g) Args emission (no broker involved)
    static string Args(object spec) => string.Join(";", ((dynamic)spec).Args.Cast<System.Collections.DictionaryEntry>().Select(e => e.Key + "=" + e.Value));
    Console.WriteLine("ARGS-DEFAULT: " + Args(new StreamSpec("x")));
    Console.WriteLine("ARGS-KNOBBED: " + Args(new StreamSpec("x") { MaxAge = TimeSpan.FromDays(7), LeaderLocator = LeaderLocator.LeastLeaders }));
    Console.WriteLine("ARGS-SUPER: " + Args(new PartitionsSuperStreamSpec("x", 3)));
    // (h) management reachability (operator-domain probe for the FR-7.4 test)
    try
    {
        using var http = new HttpClient { Timeout = TimeSpan.FromSeconds(5) };
        var bytes = Encoding.UTF8.GetBytes($"{user}:{pass}");
        http.DefaultRequestHeaders.Authorization = new System.Net.Http.Headers.AuthenticationHeaderValue("Basic", Convert.ToBase64String(bytes));
        var resp = await http.GetAsync("http://127.0.0.1:15672/api/overview");
        Console.WriteLine($"MANAGEMENT: {(int)resp.StatusCode}");
    }
    catch (Exception ex) { Console.WriteLine("MANAGEMENT-UNREACHABLE: " + ex.GetType().Name); }
}
finally
{
    try { await system.DeleteStream(plain); } catch { }
    try { await system.DeleteSuperStream(super); } catch { }
}
```

```powershell
dotnet run --project $probe/probe.csproj -c Release
Remove-Item -Recurse -Force $probe
```

The task report must record verbatim: every `PLAIN-*`/`SUPER-*` line, `PARTITIONS`, `STREAMINFO`, all three `ARGS-*` lines, and the `MANAGEMENT*` line. Expected shape (design §8 + client 1.12.1): identical re-declares either succeed or throw catchable already-exists; conflicting re-declares throw something catchable (any type — the Task 2 design never inspects it); `Args` shows which keys a default vs knobbed spec emits (drives the FR-7.4 spec-purity test); single-node `replicas=0` (drives the G-3 skip). Any outcome is compatible with Task 2 — record, do not branch.

- [ ] **Step 2: Verify (no repo changes in this task)**

```powershell
git status --short
```

Expected: only the pre-existing untracked plan doc. This task produces evidence only.

- [ ] **Step 3: No commit (evidence lives in the task report)**

---

### Task 2: Records, mapping, registry with re-probe + compare

**Files:**
- Create: `src/StreamBus/Topology.cs`
- Create: `src/StreamBus/TopologyRegistry.cs`
- Modify: `src/StreamBus/Exceptions.cs` (add `StreamConfigurationConflictException`)
- Modify: `src/StreamBus/Connector.cs` (`CreateStreamAsync` + `CreateSuperStreamAsync` on the interface + implementation)
- Modify: every existing `IStreamSystemHandle` stub under `tests/` (grep first; add both members throwing `NotImplementedException("<TestClass> never declares topology.")`).

**Interfaces:**
- Consumes: `IStreamSystemHandle.StreamExistsAsync`/`SuperStreamExistsAsync`/`QueryPartitionAsync`, `StreamBusConnectionSupervisor` readiness, `ReferenceDerivation`-style byte cap (names: broker limit applies — validate non-empty; over-long names fail at the broker with a catchable create error → rethrow path, documented), `StreamBusDelays` (not needed here — no retry in Ensure; the startup loop in Task 3 owns retry).
- Produces: `IStreamTopology`, `StreamTopologyRegistry` (incl. `GetOrCreate` semantics for Task 3 wiring), declaration records + mapping + validation + describe for Tasks 3–6.

- [ ] **Step 0: Concurrent-identical-create probe (decides the knob-conflict branch)**

The Task 1 probe covered sequential re-declares. This step covers the true race (in-process concurrency is serialized by our gate, so only cross-process concurrency can hit it). Throwaway app, same `$probe` pattern as Task 1 (ImplicitUsings on, explicit Dispose — no sends here, only declares):

```csharp
// (same connection boilerplate as Task 1: user/pass, uid, system)
var cplain = $"probe-topo-race-plain-{uid}";
var csuper = $"probe-topo-race-super-{uid}";
try
{
    var plains = Enumerable.Range(0, 4).Select(async i =>
    {
        try { await system.CreateStream(new StreamSpec(cplain)); return $"PLAIN-{i}: success"; }
        catch (Exception ex) { return $"PLAIN-{i}-THROWS: " + ex.GetType().Name + " | " + ex.Message.Split('\n')[0]; }
    }).ToList();
    var supers = Enumerable.Range(0, 4).Select(async i =>
    {
        try { await system.CreateSuperStream(new PartitionsSuperStreamSpec(csuper, 2)); return $"SUPER-{i}: success"; }
        catch (Exception ex) { return $"SUPER-{i}-THROWS: " + ex.GetType().Name + " | " + ex.Message.Split('\n')[0]; }
    }).ToList();
    foreach (var line in await Task.WhenAll(plains)) Console.WriteLine(line);
    foreach (var line in await Task.WhenAll(supers)) Console.WriteLine(line);
}
finally
{
    try { await system.DeleteStream(cplain); } catch { }
    try { await system.DeleteSuperStream(csuper); } catch { }
    foreach (var p in new[] { 0, 1 }) { try { await system.DeleteStream($"{cplain}-{p}"); } catch { } }
}
```

Record all 8 lines verbatim in the task report, then take exactly one branch (record which):

- PRIMARY (expected: all 8 succeed — identical never throws even concurrently): implement Steps 1–7 as written. A create-throw meeting a readable match is a knob conflict (Task 4 knob tests assert throws).
- FALLBACK (any identical create throws already-exists-ish): drop the createError→conflict sub-branches in both Compares below (a readable match with a create error becomes success-with-warning, same as pre-existing); keep partition-mismatch, kind-mismatch/coexistence, and absent-rethrow branches unchanged. Task 4's two knob-conflict tests then assert success instead of throws (same scripted setup, plain `await ...` with no throw, plus `Assert.True` on the stream existing); everything else in Task 4 is identical under both branches.

- [ ] **Step 1: Add the conflict exception to `Exceptions.cs`**

```csharp
public sealed class StreamConfigurationConflictException : Exception
{
    public StreamConfigurationConflictException(string stream, string expected, string actual)
        : base($"Stream '{stream}' already exists with a conflicting spec (expected {expected}; actual {actual}). Streams are immutable: delete and recreate the stream, or align the declaration.")
    {
        Stream = stream;
        Expected = expected;
        Actual = actual;
    }

    public string Stream { get; }
    public string Expected { get; }
    public string Actual { get; }
}
```

- [ ] **Step 2: Implement `Topology.cs`**

```csharp
namespace StreamBus;

public enum StreamLeaderLocator
{
    LeastLeaders = 0,
    ClientLocal = 1,
    Random = 2,
}

public sealed record StreamDeclaration
{
    public required string Name { get; init; }
    public TimeSpan? MaxAge { get; init; }
    public ulong? MaxLengthBytes { get; init; }
    public int? MaxSegmentSizeBytes { get; init; }
    public StreamLeaderLocator LeaderLocator { get; init; } = StreamLeaderLocator.LeastLeaders;
}

public sealed record SuperStreamDeclaration
{
    public required string Name { get; init; }
    public required int Partitions { get; init; }
    public TimeSpan? MaxAge { get; init; }
    public ulong? MaxLengthBytes { get; init; }
    public int? MaxSegmentSizeBytes { get; init; }
    public StreamLeaderLocator LeaderLocator { get; init; } = StreamLeaderLocator.LeastLeaders;
}

public sealed record StreamTopologyDeclarations
{
    public IReadOnlyList<StreamDeclaration> Streams { get; init; } = Array.Empty<StreamDeclaration>();
    public IReadOnlyList<SuperStreamDeclaration> SuperStreams { get; init; } = Array.Empty<SuperStreamDeclaration>();
}

public interface IStreamTopology
{
    string Vhost { get; }
    Task EnsureStreamAsync(StreamDeclaration declaration, CancellationToken ct = default);
    Task EnsureSuperStreamAsync(SuperStreamDeclaration declaration, CancellationToken ct = default);
}

internal static class TopologyMapping
{
    public static RabbitMQ.Stream.Client.StreamSpec ToStreamSpec(StreamDeclaration declaration)
    {
        ArgumentNullException.ThrowIfNull(declaration);
        var spec = new RabbitMQ.Stream.Client.StreamSpec(declaration.Name);
        if (declaration.MaxAge is { } age) spec.MaxAge = age;
        if (declaration.MaxLengthBytes is { } length) spec.MaxLengthBytes = length;
        if (declaration.MaxSegmentSizeBytes is { } segment) spec.MaxSegmentSizeBytes = segment;
        spec.LeaderLocator = Map(declaration.LeaderLocator);
        return spec;
    }

    public static RabbitMQ.Stream.Client.PartitionsSuperStreamSpec ToSuperStreamSpec(SuperStreamDeclaration declaration)
    {
        ArgumentNullException.ThrowIfNull(declaration);
        var spec = new RabbitMQ.Stream.Client.PartitionsSuperStreamSpec(declaration.Name, declaration.Partitions);
        if (declaration.MaxAge is { } age) spec.MaxAge = age;
        if (declaration.MaxLengthBytes is { } length) spec.MaxLengthBytes = length;
        if (declaration.MaxSegmentSizeBytes is { } segment) spec.MaxSegmentSizeBytes = segment;
        spec.LeaderLocator = Map(declaration.LeaderLocator);
        return spec;
    }

    public static RabbitMQ.Stream.Client.LeaderLocator Map(StreamLeaderLocator locator) => locator switch
    {
        StreamLeaderLocator.LeastLeaders => RabbitMQ.Stream.Client.LeaderLocator.LeastLeaders,
        StreamLeaderLocator.ClientLocal => RabbitMQ.Stream.Client.LeaderLocator.ClientLocal,
        StreamLeaderLocator.Random => RabbitMQ.Stream.Client.LeaderLocator.Random,
        _ => throw new StreamBusConfigurationException($"Unknown stream leader locator {locator}."),
    };
}

internal static class TopologyValidation
{
    public static void ValidateStream(StreamDeclaration declaration)
    {
        ArgumentNullException.ThrowIfNull(declaration);
        ArgumentException.ThrowIfNullOrWhiteSpace(declaration.Name);
        ValidateKnobs(declaration.MaxAge, declaration.MaxLengthBytes, declaration.MaxSegmentSizeBytes, declaration.LeaderLocator, $"StreamDeclaration '{declaration.Name}'");
    }

    public static void ValidateSuper(SuperStreamDeclaration declaration)
    {
        ArgumentNullException.ThrowIfNull(declaration);
        ArgumentException.ThrowIfNullOrWhiteSpace(declaration.Name);
        if (declaration.Partitions < 1)
            throw new StreamBusConfigurationException($"SuperStreamDeclaration '{declaration.Name}' Partitions must be >= 1.");
        ValidateKnobs(declaration.MaxAge, declaration.MaxLengthBytes, declaration.MaxSegmentSizeBytes, declaration.LeaderLocator, $"SuperStreamDeclaration '{declaration.Name}'");
    }

    private static void ValidateKnobs(TimeSpan? maxAge, ulong? maxLengthBytes, int? maxSegmentSizeBytes, StreamLeaderLocator locator, string what)
    {
        if (!Enum.IsDefined(locator))
            throw new StreamBusConfigurationException($"{what} LeaderLocator has an undefined value.");
        if (maxAge is { } age && age <= TimeSpan.Zero)
            throw new StreamBusConfigurationException($"{what} MaxAge must be positive when set.");
        if (maxLengthBytes is { } length && length < 1)
            throw new StreamBusConfigurationException($"{what} MaxLengthBytes must be >= 1 when set.");
        if (maxSegmentSizeBytes is { } segment && segment < 1)
            throw new StreamBusConfigurationException($"{what} MaxSegmentSizeBytes must be >= 1 when set.");
    }
}

internal static class TopologyDescribe
{
    public static string Expected(SuperStreamDeclaration declaration)
        => $"super stream '{declaration.Name}' with {declaration.Partitions} partitions ({Knobs(declaration.MaxAge, declaration.MaxLengthBytes, declaration.MaxSegmentSizeBytes, declaration.LeaderLocator)})";

    public static string Expected(StreamDeclaration declaration)
        => $"plain stream '{declaration.Name}' ({Knobs(declaration.MaxAge, declaration.MaxLengthBytes, declaration.MaxSegmentSizeBytes, declaration.LeaderLocator)})";

    private static string Knobs(TimeSpan? maxAge, ulong? maxLengthBytes, int? maxSegmentSizeBytes, StreamLeaderLocator locator)
        => $"max-age={maxAge?.ToString() ?? "unset"}, max-length-bytes={maxLengthBytes?.ToString() ?? "unset"}, max-segment-size-bytes={maxSegmentSizeBytes?.ToString() ?? "unset"}, leader-locator={locator}";
}
```

(`balanced` is deliberately unoffered: client 1.12.1 has no such member. `MaxAge` uses `TimeSpan?`: null leaves the client default untouched — the Task 1 Args table pins what defaults emit.)

- [ ] **Step 3: Extend `Connector.cs`**

`IStreamSystemHandle` additions (pass-through only — the registry interprets every outcome):

```csharp
Task CreateStreamAsync(RabbitMQ.Stream.Client.StreamSpec spec, CancellationToken ct);
Task CreateSuperStreamAsync(RabbitMQ.Stream.Client.PartitionsSuperStreamSpec spec, CancellationToken ct);
```

Implementations (next to the producer creators; `RabbitMQ.Stream.Client` is already imported):

```csharp
public async Task CreateStreamAsync(RabbitMQ.Stream.Client.StreamSpec spec, CancellationToken ct)
{
    ArgumentNullException.ThrowIfNull(spec);
    ct.ThrowIfCancellationRequested();
    await _system.CreateStream(spec).ConfigureAwait(false);
}

public async Task CreateSuperStreamAsync(RabbitMQ.Stream.Client.PartitionsSuperStreamSpec spec, CancellationToken ct)
{
    ArgumentNullException.ThrowIfNull(spec);
    ct.ThrowIfCancellationRequested();
    await _system.CreateSuperStream(spec).ConfigureAwait(false);
}
```

- [ ] **Step 4: Implement `TopologyRegistry.cs`**

```csharp
using System.Collections.Concurrent;
using System.Runtime.ExceptionServices;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

namespace StreamBus;

internal sealed class StreamTopologyRegistry : IStreamTopology
{
    private readonly StreamBusConnectionSupervisor _supervisor;
    private readonly string _vhost;
    private readonly ILogger<StreamTopologyRegistry> _logger;
    private readonly ConcurrentDictionary<string, SemaphoreSlim> _gates = new();
    private readonly ConcurrentDictionary<string, string> _declared = new();

    public StreamTopologyRegistry(StreamBusConnectionSupervisor supervisor, string vhost, ILogger<StreamTopologyRegistry>? logger = null)
    {
        ArgumentNullException.ThrowIfNull(supervisor);
        ArgumentNullException.ThrowIfNull(vhost);
        _supervisor = supervisor;
        _vhost = vhost;
        _logger = logger ?? NullLogger<StreamTopologyRegistry>.Instance;
    }

    public string Vhost => _vhost;

    public async Task EnsureStreamAsync(StreamDeclaration declaration, CancellationToken ct = default)
    {
        ArgumentNullException.ThrowIfNull(declaration);
        TopologyValidation.ValidateStream(declaration);
        var handle = await ReadyHandleAsync(declaration.Name, ct).ConfigureAwait(false);
        var gate = _gates.GetOrAdd(declaration.Name, static _ => new SemaphoreSlim(1, 1));
        await gate.WaitAsync(ct).ConfigureAwait(false);
        try
        {
            var fingerprint = Fingerprint.Plain(declaration);
            if (_declared.TryGetValue(declaration.Name, out var known) && known == fingerprint)
                return;
            // Kind occupancy is decided in ComparePlainAsync (single compare path covers
            // pre-existing and raced occupancy identically). The create gate checks
            // BOTH kinds: the broker allows plain-over-super coexistence (Ruling R6),
            // so attempting the create on a super-occupied name would silently enter
            // ambiguous broker state instead of throwing the kind conflict.
            Exception? createError = null;
            if (!await handle.StreamExistsAsync(declaration.Name, ct).ConfigureAwait(false)
                && !await handle.SuperStreamExistsAsync(declaration.Name, ct).ConfigureAwait(false))
            {
                try
                {
                    await handle.CreateStreamAsync(TopologyMapping.ToStreamSpec(declaration), ct).ConfigureAwait(false);
                    _declared[declaration.Name] = fingerprint;
                    return;
                }
                catch (OperationCanceledException) when (ct.IsCancellationRequested)
                {
                    throw;
                }
                catch (Exception ex)
                {
                    createError = ex;
                }
            }
            var terminal = await ComparePlainAsync(handle, declaration, createError, ct).ConfigureAwait(false);
            if (terminal is not null) ExceptionDispatchInfo.Capture(terminal).Throw();
            if (createError is not null)
                _logger.LogInformation(createError, "Stream '{Stream}' was declared concurrently with an identical-enough spec; treating as success.", declaration.Name);
            _declared[declaration.Name] = fingerprint;
        }
        finally
        {
            gate.Release();
        }
    }

    public async Task EnsureSuperStreamAsync(SuperStreamDeclaration declaration, CancellationToken ct = default)
    {
        ArgumentNullException.ThrowIfNull(declaration);
        TopologyValidation.ValidateSuper(declaration);
        var handle = await ReadyHandleAsync(declaration.Name, ct).ConfigureAwait(false);
        var gate = _gates.GetOrAdd(declaration.Name, static _ => new SemaphoreSlim(1, 1));
        await gate.WaitAsync(ct).ConfigureAwait(false);
        try
        {
            var fingerprint = Fingerprint.Super(declaration);
            if (_declared.TryGetValue(declaration.Name, out var known) && known == fingerprint)
                return;
            Exception? createError = null;
            if (!await handle.SuperStreamExistsAsync(declaration.Name, ct).ConfigureAwait(false)
                && !await handle.StreamExistsAsync(declaration.Name, ct).ConfigureAwait(false))
            {
                try
                {
                    await handle.CreateSuperStreamAsync(TopologyMapping.ToSuperStreamSpec(declaration), ct).ConfigureAwait(false);
                    _declared[declaration.Name] = fingerprint;
                    return;
                }
                catch (OperationCanceledException) when (ct.IsCancellationRequested)
                {
                    throw;
                }
                catch (Exception ex)
                {
                    createError = ex;
                }
            }
            var terminal = await CompareSuperAsync(handle, declaration, createError, ct).ConfigureAwait(false);
            if (terminal is not null) ExceptionDispatchInfo.Capture(terminal).Throw();
            if (createError is not null)
                _logger.LogInformation(createError, "Super stream '{Stream}' was declared concurrently with an identical-enough spec; treating as success.", declaration.Name);
            _declared[declaration.Name] = fingerprint;
        }
        finally
        {
            gate.Release();
        }
    }

    // Re-probe + compare. Plain-stream argument compatibility is NOT readable via
    // the stream protocol, so the broker's create outcome is the only knob signal:
    // an identical re-declare always succeeds (Tasks 1–2 probes), hence a create
    // failure meeting an existing plain stream means the specs differed. A
    // pre-existing plain stream with no create attempt is success-with-warning
    // (order-dependent residual — keep declarations in sync across processes).
    private async Task<Exception?> ComparePlainAsync(IStreamSystemHandle handle, StreamDeclaration declaration, Exception? createError, CancellationToken ct)
    {
        if (await handle.SuperStreamExistsAsync(declaration.Name, ct).ConfigureAwait(false))
        {
            var partitions = await handle.QueryPartitionAsync(declaration.Name, ct).ConfigureAwait(false);
            return new StreamConfigurationConflictException(declaration.Name,
                TopologyDescribe.Expected(declaration),
                $"super stream with {partitions.Length} partitions");
        }
        if (await handle.StreamExistsAsync(declaration.Name, ct).ConfigureAwait(false))
        {
            if (createError is not null)
                return new StreamConfigurationConflictException(declaration.Name,
                    TopologyDescribe.Expected(declaration),
                    "plain stream with differing arguments (argument detail is not readable via the stream protocol)");
            _logger.LogWarning("Stream '{Stream}' already exists; argument-level compatibility is not readable via the stream protocol — treating as success. Keep declarations in sync across processes.", declaration.Name);
            return null;
        }
        if (createError is not null) return createError;
        return new StreamBusStreamNotFoundException(declaration.Name);
    }

    private async Task<Exception?> CompareSuperAsync(IStreamSystemHandle handle, SuperStreamDeclaration declaration, Exception? createError, CancellationToken ct)
    {
        if (await handle.SuperStreamExistsAsync(declaration.Name, ct).ConfigureAwait(false))
        {
            var partitions = await handle.QueryPartitionAsync(declaration.Name, ct).ConfigureAwait(false);
            if (partitions.Length != declaration.Partitions)
                return new StreamConfigurationConflictException(declaration.Name,
                    TopologyDescribe.Expected(declaration),
                    $"super stream with {partitions.Length} partitions");
            if (await handle.StreamExistsAsync(declaration.Name, ct).ConfigureAwait(false))
                return new StreamConfigurationConflictException(declaration.Name,
                    TopologyDescribe.Expected(declaration),
                    $"super stream with {partitions.Length} partitions coexisting with a plain stream of the same name");
            if (createError is not null)
                return new StreamConfigurationConflictException(declaration.Name,
                    TopologyDescribe.Expected(declaration),
                    $"super stream with {partitions.Length} partitions and differing arguments (argument detail is not readable via the stream protocol)");
            return null;
        }
        if (await handle.StreamExistsAsync(declaration.Name, ct).ConfigureAwait(false))
            return new StreamConfigurationConflictException(declaration.Name,
                TopologyDescribe.Expected(declaration),
                "plain stream");
        if (createError is not null) return createError;
        return new StreamBusStreamNotFoundException(declaration.Name);
    }

    private async Task<IStreamSystemHandle> ReadyHandleAsync(string stream, CancellationToken ct)
    {
        var readiness = await _supervisor.WaitUntilReadyAsync(ReadinessPolicy.FailFastPolicy, ct).ConfigureAwait(false);
        if (readiness == BusReadiness.Fatal)
            throw new StreamBusFatalException($"StreamBus for vhost '{_vhost}' is in Fatal state; topology declaration for '{stream}' refused.");
        if (readiness != BusReadiness.Ready)
            throw new StreamBusNotReadyException($"StreamBus for vhost '{_vhost}' is not ready ({readiness}); topology declaration for '{stream}' refused.");
        return _supervisor.CurrentHandle
            ?? throw new StreamBusNotReadyException($"StreamBus for vhost '{_vhost}' is not ready; topology declaration for '{stream}' refused.");
    }

    private static class Fingerprint
    {
        // Unit separator (U+001F, escaped explicitly — never a literal): stream
        // names cannot smuggle field boundaries through it.
        private const string Sep = "";
        public static string Plain(StreamDeclaration d)
            => string.Join(Sep, "plain", d.Name, d.MaxAge?.Ticks.ToString() ?? "-", d.MaxLengthBytes?.ToString() ?? "-", d.MaxSegmentSizeBytes?.ToString() ?? "-", d.LeaderLocator.ToString());

        public static string Super(SuperStreamDeclaration d)
            => string.Join(Sep, "super", d.Name, d.Partitions.ToString(), d.MaxAge?.Ticks.ToString() ?? "-", d.MaxLengthBytes?.ToString() ?? "-", d.MaxSegmentSizeBytes?.ToString() ?? "-", d.LeaderLocator.ToString());
    }
}
```

(Separator is U+001F unit separator — stream names cannot smuggle field boundaries through it. The pre-check + compare double-probe is deliberate: the pre-check skips the create attempt on the hot restart path; correctness lives in Compare, which re-reads fresh state after any race.)

- [ ] **Step 5: Update every existing test stub**

Grep `IStreamSystemHandle` under `tests/`; add to every implementer:
- `CreateStreamAsync` → `throw new NotImplementedException("<TestClass> never declares topology.")`.
- `CreateSuperStreamAsync` → `throw new NotImplementedException("<TestClass> never declares topology.")`.
(Task 4's fakes are self-contained scriptable doubles — these stubs only need to compile here.)

- [ ] **Step 6: Verify**

```powershell
dotnet build src/StreamBus/StreamBus.csproj -c Release
dotnet test tests/StreamBus.UnitTests/StreamBus.UnitTests.csproj -c Release
```

Expected: build clean; existing suite green (additions are additive; no caller uses the registry yet).

- [ ] **Step 7: Commit (with human approval)**

```powershell
git add src/StreamBus/Topology.cs src/StreamBus/TopologyRegistry.cs src/StreamBus/Exceptions.cs src/StreamBus/Connector.cs tests/
git commit -m "feat: add serialized topology registry with compare-based conflicts"
```

---

### Task 3: Bus surface, options, startup declaration

**Files:**
- Modify: `src/StreamBus/Options.cs` (`StreamBusOptions.Topology` + builders)
- Modify: `src/StreamBus/Bus.cs` (`IStreamTopology Topology { get; }` + both implementations, trailing ctor param)
- Modify: `src/StreamBus/Registry.cs` (`TopologyFor` + pass into bus construction + interface member)
- Modify: `src/StreamBus/Hosting.cs` (builder `Topology(...)`, hosted-service declaration with both modes)
- Modify: every `new StreamBus(` / `new StreamBus<` under `tests/` (grep first; pass a topology registry from the test's supervisor).

**Interfaces:**
- Consumes: `IStreamTopology`/`StreamTopologyRegistry`/`StreamTopologyDeclarations` (Task 2), `StreamBusDelays.ExponentialCapped` (5-arg), existing `StreamBusHostedService` start/stop shape.
- Produces: `bus.Topology`, configured startup declarations, both hosting modes for Tasks 4–6.

- [ ] **Step 1: Extend `Options.cs`**

```csharp
public sealed record StreamBusOptions
{
    public required string Vhost { get; init; }
    public required string UserName { get; init; }
    public required string Password { get; init; }
    public string Host { get; init; } = "127.0.0.1";
    public int Port { get; init; } = 5552;
    public StreamBusConnectionOptions Connection { get; init; } = new();
    public StreamBusHostOptions Hosting { get; init; } = new();
    public StreamTopologyDeclarations Topology { get; init; } = new();
}
```

(Only the last line is new. `SameConnection` in `Hosting.cs` compares connection fields explicitly, so topology is excluded from connection identity by construction — no change there. Registrations sharing a vhost may carry different declaration lists; the hosted service unions them.)

- [ ] **Step 2: Builders in `Hosting.cs` (topology section)**

```csharp
public sealed class StreamTopologyDeclarationsBuilder
{
    private readonly List<StreamDeclaration> _streams = new();
    private readonly List<SuperStreamDeclaration> _supers = new();

    public StreamTopologyDeclarationsBuilder DeclareStream(string name, Action<StreamDeclarationKnobsBuilder>? configure = null)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(name);
        var knobs = new StreamDeclarationKnobsBuilder();
        configure?.Invoke(knobs);
        _streams.Add(knobs.BuildStream(name));
        return this;
    }

    public StreamTopologyDeclarationsBuilder DeclareSuperStream(string name, int partitions, Action<StreamDeclarationKnobsBuilder>? configure = null)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(name);
        var knobs = new StreamDeclarationKnobsBuilder();
        configure?.Invoke(knobs);
        _supers.Add(knobs.BuildSuper(name, partitions));
        return this;
    }

    public StreamTopologyDeclarations Build()
    {
        var built = new StreamTopologyDeclarations { Streams = _streams.ToList(), SuperStreams = _supers.ToList() };
        foreach (var s in built.Streams) TopologyValidation.ValidateStream(s);
        foreach (var s in built.SuperStreams) TopologyValidation.ValidateSuper(s);
        return built;
    }
}

public sealed class StreamDeclarationKnobsBuilder
{
    private TimeSpan? _maxAge;
    private ulong? _maxLengthBytes;
    private int? _maxSegmentSizeBytes;
    private StreamLeaderLocator _locator = StreamLeaderLocator.LeastLeaders;

    public StreamDeclarationKnobsBuilder MaxAge(TimeSpan maxAge) { _maxAge = maxAge; return this; }
    public StreamDeclarationKnobsBuilder MaxLengthBytes(ulong maxLengthBytes) { _maxLengthBytes = maxLengthBytes; return this; }
    public StreamDeclarationKnobsBuilder MaxSegmentSizeBytes(int maxSegmentSizeBytes) { _maxSegmentSizeBytes = maxSegmentSizeBytes; return this; }
    public StreamDeclarationKnobsBuilder LeaderLocator(StreamLeaderLocator locator) { _locator = locator; return this; }

    internal StreamDeclaration BuildStream(string name) => new()
    {
        Name = name, MaxAge = _maxAge, MaxLengthBytes = _maxLengthBytes,
        MaxSegmentSizeBytes = _maxSegmentSizeBytes, LeaderLocator = _locator,
    };

    internal SuperStreamDeclaration BuildSuper(string name, int partitions) => new()
    {
        Name = name, Partitions = partitions, MaxAge = _maxAge, MaxLengthBytes = _maxLengthBytes,
        MaxSegmentSizeBytes = _maxSegmentSizeBytes, LeaderLocator = _locator,
    };
}
```

`StreamBusOptionsBuilder` gains the section + build line:

```csharp
private StreamTopologyDeclarations _topology = new();
public StreamBusOptionsBuilder Topology(Action<StreamTopologyDeclarationsBuilder> configure)
{
    ArgumentNullException.ThrowIfNull(configure);
    var b = new StreamTopologyDeclarationsBuilder();
    configure(b);
    _topology = b.Build();
    return this;
}
```

and `Build()` gains `Topology = _topology,`. (`Build()` validates eagerly: misconfigured declarations fail at `AddStreamBus` time with `StreamBusConfigurationException`.)

- [ ] **Step 3: Hosted-service startup declaration**

`StreamBusHostedService` gains an optional logger (existing direct constructions `new StreamBusHostedService(store, registry)` keep compiling; DI resolves the default):

```csharp
private readonly ILogger<StreamBusHostedService> _logger;
private CancellationTokenSource? _declareCts;
```
```csharp
public StreamBusHostedService(StreamBusConfigStore store, IStreamBusRegistry registry, ILogger<StreamBusHostedService>? logger = null)
{
    _store = store;
    _registry = registry;
    _logger = logger ?? NullLogger<StreamBusHostedService>.Instance;
}
```

(`using Microsoft.Extensions.Logging;` + `using Microsoft.Extensions.Logging.Abstractions;` — check current imports; `Hosting.cs` already imports Logging, add Abstractions.)

In `StartAsync`, after the supervisor-start loop and before the readiness gate, kick per-vhost declaration:

```csharp
CancellationTokenSource declareCts;
lock (_gate)
{
    _declareCts?.Dispose();
    _declareCts = CancellationTokenSource.CreateLinkedTokenSource(ct);
    declareCts = _declareCts;
}
var blocking = _store.Registrations.Any(r => r.Options.Hosting.BlockStartupUntilReady);
var declareTasks = new List<Task>();
foreach (var vhost in _store.Registrations.Select(r => r.Options.Vhost).Distinct())
{
    var streams = _store.Registrations.Where(r => r.Options.Vhost == vhost).SelectMany(r => r.Options.Topology.Streams).ToList();
    var supers = _store.Registrations.Where(r => r.Options.Vhost == vhost).SelectMany(r => r.Options.Topology.SuperStreams).ToList();
    if (streams.Count == 0 && supers.Count == 0) continue;
    var task = DeclareUntilReadyAsync(vhost, streams, supers, declareCts.Token);
    if (!blocking)
    {
        _ = task.ContinueWith(
            t =>
            {
                if (t.Exception is { } ex)
                    _logger.LogCritical(ex, "Topology declaration for vhost '{Vhost}' failed permanently; streams were NOT declared.", vhost);
            },
            CancellationToken.None,
            TaskContinuationOptions.OnlyOnFaulted | TaskContinuationOptions.ExecuteSynchronously,
            TaskScheduler.Default);
    }
    else
    {
        declareTasks.Add(task);
    }
}
```

After the existing readiness gate (before `StartAsync` returns), when blocking:

```csharp
if (declareTasks.Count > 0)
{
    // Fail fast on conflict: a faulted declaration carrying a conflict cancels
    // its siblings so the blocking wait surfaces the conflict instead of idling
    // behind another vhost's transient retries until the timeout.
    foreach (var task in declareTasks)
    {
        _ = task.ContinueWith(
            t =>
            {
                if (t.Exception?.InnerExceptions.Any(e => e is StreamConfigurationConflictException) == true)
                {
                    try { declareCts.Cancel(); }
                    catch (ObjectDisposedException) { }
                }
            },
            CancellationToken.None,
            TaskContinuationOptions.OnlyOnFaulted | TaskContinuationOptions.ExecuteSynchronously,
            TaskScheduler.Default);
    }
    try
    {
        await Task.WhenAll(declareTasks).WaitAsync(cts.Token).ConfigureAwait(false);
    }
    catch (OperationCanceledException) when (!ct.IsCancellationRequested)
    {
        // Startup already failed: abandon declaration retries (else they run away
        // and a late conflict fault goes unobserved), then report the conflict
        // when one faulted, or the timeout otherwise.
        try { declareCts.Cancel(); }
        catch (ObjectDisposedException) { }
        var conflict = declareTasks
            .SelectMany(t => t.Exception?.InnerExceptions.AsEnumerable() ?? Enumerable.Empty<Exception>())
            .OfType<StreamConfigurationConflictException>()
            .FirstOrDefault();
        if (conflict is not null) System.Runtime.ExceptionServices.ExceptionDispatchInfo.Capture(conflict).Throw();
        throw new StreamBusTimeoutException($"Timed out declaring topology within the startup timeout.");
    }
}
```

(`cts` is the existing linked timeout source in the blocking path — currently `using var cts` is created only inside the `if` for blocking... check: the existing code creates `cts` AFTER the early return, so it exists wherever declare-await runs. If no registration blocks, `declareTasks` is empty (non-blocking branch) and no await happens. Exact placement: insert the declare-await immediately after the readiness `foreach`, still inside the blocking region.)

Core loop (new private method; retry on transient, loud stop on conflict/fatal):

```csharp
private async Task DeclareUntilReadyAsync(string vhost, List<StreamDeclaration> streams, List<SuperStreamDeclaration> supers, CancellationToken ct)
{
    var topology = _registry.TopologyFor(vhost);
    var attempt = 0;
    while (true)
    {
        ct.ThrowIfCancellationRequested();
        try
        {
            foreach (var s in streams)
                await topology.EnsureStreamAsync(s, ct).ConfigureAwait(false);
            foreach (var s in supers)
                await topology.EnsureSuperStreamAsync(s, ct).ConfigureAwait(false);
            return;
        }
        catch (OperationCanceledException) when (ct.IsCancellationRequested)
        {
            throw;
        }
        catch (StreamConfigurationConflictException)
        {
            throw;
        }
        catch (StreamBusFatalException)
        {
            throw;
        }
        catch (Exception ex)
        {
            attempt++;
            _logger.LogWarning(ex, "Topology declaration for vhost '{Vhost}' failed (attempt {Attempt}); retrying.", vhost, attempt);
            await Task.Delay(StreamBusDelays.ExponentialCapped(attempt, TimeSpan.FromMilliseconds(200), TimeSpan.FromSeconds(30), 2.0, TimeSpan.FromSeconds(1)), ct).ConfigureAwait(false);
        }
    }
}
```

`StopAsync` additions (cancel declarations alongside supervisors):

```csharp
CancellationTokenSource? declareCts;
lock (_gate)
{
    declareCts = _declareCts;
    _declareCts = null;
}
if (declareCts is not null)
{
    try { declareCts.Cancel(); }
    finally { declareCts.Dispose(); }
}
```

(Place before the supervisor disposal loop. In-flight declaration attempts observe the cancel via their `ct` — both `Ensure*` (FailFast wait + `WaitAsync(ct)`) and the backoff `Task.Delay(..., ct)` are cancel-aware.)

- [ ] **Step 4: `Registry.cs` + `Bus.cs` wiring**

`StreamBusRegistry` gains:

```csharp
private readonly ConcurrentDictionary<string, StreamTopologyRegistry> _topologies = new();

internal StreamTopologyRegistry TopologyFor(string vhost)
    => _topologies.GetOrAdd(vhost, _ => new StreamTopologyRegistry(SupervisorForCore(vhost), vhost, _loggerFactory.CreateLogger<StreamTopologyRegistry>()));
```

Check `StreamBusRegistry`'s logger field name first (`_loggerFactory` exists — `ConsumersFor` uses it; mirror that line). `IStreamBusRegistry` gains `internal StreamTopologyRegistry TopologyFor(string vhost);` (precedent: internal members on the public interface).

`GetOrCreateCore` passes `TopologyFor(vhost)` into both `StreamBus` ctors (new trailing param). `IStreamBus` gains:

```csharp
/// <summary>
/// Declares streams and super streams idempotently (FR-7). Startup
/// declarations configured via <c>Topology(...)</c> are declared automatically;
/// these methods cover explicit/operator-driven declaration anytime.
/// </summary>
IStreamTopology Topology { get; }
```

Both implementations store the field and expose it. (`IStreamBus<TBus>` inherits; no redeclare.)

- [ ] **Step 5: Verify**

```powershell
dotnet build src/StreamBus/StreamBus.csproj -c Release
dotnet test tests/StreamBus.UnitTests/StreamBus.UnitTests.csproj -c Release
```

Expected: build clean; existing suite green. Grep `new StreamBus(` under `src/` + `tests/` first and update every direct construction with the new trailing arg.

- [ ] **Step 6: Commit (with human approval)**

```powershell
git add src/StreamBus/Options.cs src/StreamBus/Hosting.cs src/StreamBus/Registry.cs src/StreamBus/Bus.cs tests/
git commit -m "feat: wire topology surface with startup declaration"
```

---

### Task 4: Unit tests — mapping, validation, serialization, conflicts

**Files:**
- Create: `tests/StreamBus.UnitTests/TopologyTests.cs`

**Interfaces:**
- Consumes: everything from Tasks 2–3 via `InternalsVisibleTo`.
- Produces: pinned contracts for Task 5 (Args table, conflict matrix, single-create).

Style rules (analyzers are build errors): no `.ConfigureAwait(false)` inside `[Fact]`s; use `TestContext.Current.CancellationToken` explicitly; `using RabbitMQ.Stream.Client;` for client types; test namespace resolves `StreamBus.*` without a using.

- [ ] **Step 1: Implement `TopologyTests.cs`**

```csharp
using RabbitMQ.Stream.Client;

namespace StreamBus.UnitTests;

public sealed class TopologyTests
{
    [Fact]
    public void Locator_defaults_to_least_leaders()
    {
        Assert.Equal(StreamLeaderLocator.LeastLeaders, new StreamDeclaration { Name = "s" }.LeaderLocator);
        Assert.Equal(StreamLeaderLocator.LeastLeaders, new SuperStreamDeclaration { Name = "s", Partitions = 3 }.LeaderLocator);
        Assert.Equal(LeaderLocator.LeastLeaders, TopologyMapping.Map(StreamLeaderLocator.LeastLeaders));
    }

    [Fact]
    public void Locator_maps_all_values()
    {
        Assert.Equal(LeaderLocator.ClientLocal, TopologyMapping.Map(StreamLeaderLocator.ClientLocal));
        Assert.Equal(LeaderLocator.Random, TopologyMapping.Map(StreamLeaderLocator.Random));
    }

    [Fact]
    public void Default_spec_emits_no_retention_args()
    {
        // Args emission table (Task 1 probe values asserted here — update both if the client changes them):
        var spec = TopologyMapping.ToStreamSpec(new StreamDeclaration { Name = "s" });
        Assert.DoesNotContain("max-age", spec.Args.Keys);
        Assert.DoesNotContain("max-length-bytes", spec.Args.Keys);
        Assert.DoesNotContain("stream-max-segment-size-bytes", spec.Args.Keys);
        var knobbed = TopologyMapping.ToStreamSpec(new StreamDeclaration { Name = "s", MaxAge = TimeSpan.FromDays(7) });
        Assert.True(knobbed.Args.ContainsKey("max-age"));
        Assert.Equal("604800s", knobbed.Args["max-age"]);
    }

    [Fact]
    public void Knobs_pass_through_and_partitions_hold()
    {
        // PartitionsSuperStreamSpec knob properties are set-only, so passthrough
        // is observed through the emitted Args (same table as above).
        var spec = TopologyMapping.ToSuperStreamSpec(new SuperStreamDeclaration { Name = "ss", Partitions = 4, MaxAge = TimeSpan.FromHours(1), MaxLengthBytes = 1024, MaxSegmentSizeBytes = 4096, LeaderLocator = StreamLeaderLocator.Random });
        Assert.Equal("ss", spec.Name);
        Assert.Equal(4, spec.Partitions);
        Assert.Equal("3600s", spec.Args["max-age"]);
        Assert.Equal("1024", spec.Args["max-length-bytes"]);
        Assert.Equal("4096", spec.Args["stream-max-segment-size-bytes"]);
    }

    [Fact]
    public void Validation_rejects_bad_specs()
    {
        Assert.Throws<ArgumentException>(() => TopologyValidation.ValidateStream(new StreamDeclaration { Name = "  " }));
        Assert.Throws<StreamBusConfigurationException>(() => TopologyValidation.ValidateSuper(new SuperStreamDeclaration { Name = "s", Partitions = 0 }));
        Assert.Throws<StreamBusConfigurationException>(() => TopologyValidation.ValidateStream(new StreamDeclaration { Name = "s", MaxAge = TimeSpan.Zero }));
        Assert.Throws<StreamBusConfigurationException>(() => TopologyValidation.ValidateStream(new StreamDeclaration { Name = "s", MaxLengthBytes = 0 }));
        Assert.Throws<StreamBusConfigurationException>(() => TopologyValidation.ValidateStream(new StreamDeclaration { Name = "s", MaxSegmentSizeBytes = 0 }));
    }

    [Fact]
    public void Builder_validates_at_build_time()
    {
        var ex = Assert.Throws<StreamBusConfigurationException>(() =>
            new StreamBusOptionsBuilder("/").Topology(t => t.DeclareSuperStream("ss", 0)).Build());
        Assert.Contains("Partitions", ex.Message);
        var options = new StreamBusOptionsBuilder("/").Credentials("a", "b")
            .Topology(t => t.DeclareStream("s", k => k.MaxAge(TimeSpan.FromDays(7))).DeclareSuperStream("ss", 3))
            .Build();
        Assert.Single(options.Topology.Streams);
        Assert.Single(options.Topology.SuperStreams);
        Assert.Equal(TimeSpan.FromDays(7), options.Topology.Streams[0].MaxAge);
    }

    private sealed class ScriptSystem : IStreamSystemHandle
    {
        public readonly List<string> CreatedStreams = new();
        public readonly List<string> CreatedSupers = new();
        public int ExistsProbes;
        public Func<string, CancellationToken, Task>? OnCreateStream;
        public Func<string, CancellationToken, Task>? OnCreateSuper;
        public Func<string, bool> StreamExists = _ => false;
        public Func<string, bool> SuperExists = _ => false;
        public Func<string, string[]> Partitions = _ => Array.Empty<string>();
        public bool IsClosed => false;
        public Task CreateStreamAsync(StreamSpec spec, CancellationToken ct)
        {
            lock (CreatedStreams) CreatedStreams.Add(spec.Name);
            if (OnCreateStream is not null) return OnCreateStream(spec.Name, ct);
            return Task.CompletedTask;
        }
        public Task CreateSuperStreamAsync(PartitionsSuperStreamSpec spec, CancellationToken ct)
        {
            lock (CreatedSupers) CreatedSupers.Add(spec.Name);
            if (OnCreateSuper is not null) return OnCreateSuper(spec.Name, ct);
            return Task.CompletedTask;
        }
        public Task<IStreamProducerHandle> CreatePlainProducerAsync(string stream, CancellationToken ct) => throw new NotImplementedException("Topology tests declare only.");
        public Task<IStreamProducerHandle> CreatePlainProducerWithReferenceAsync(string stream, string reference, CancellationToken ct) => throw new NotImplementedException("Topology tests declare only.");
        public Task<IStreamProducerHandle> CreateSuperStreamProducerAsync(string stream, CancellationToken ct) => throw new NotImplementedException("Topology tests declare only.");
        public Task<IStreamProducerHandle> CreateSuperStreamProducerWithReferenceAsync(string stream, string reference, CancellationToken ct) => throw new NotImplementedException("Topology tests declare only.");
        public Task<RawConsumer> CreateRawConsumerAsync(RawConsumerConfig config, CancellationToken ct) => throw new NotImplementedException("Topology tests never attach.");
        public Task<ulong?> TryQueryOffsetAsync(string reference, string stream, CancellationToken ct) => Task.FromResult<ulong?>(null);
        public Task<ulong> QuerySequenceAsync(string reference, string stream, CancellationToken ct) => Task.FromResult(0UL);
        public Task StoreSystemOffsetAsync(string reference, string stream, ulong offset, CancellationToken ct) => Task.CompletedTask;
        public Task<bool> StreamExistsAsync(string stream, CancellationToken ct) { Interlocked.Increment(ref ExistsProbes); return Task.FromResult(StreamExists(stream)); }
        public Task<bool> SuperStreamExistsAsync(string superStream, CancellationToken ct) { Interlocked.Increment(ref ExistsProbes); return Task.FromResult(SuperExists(superStream)); }
        public Task<string[]> QueryPartitionAsync(string superStream, CancellationToken ct) => Task.FromResult(Partitions(superStream));
        public ValueTask DisposeAsync() => ValueTask.CompletedTask;
    }

    private sealed class ReadyConnector : IStreamSystemConnector
    {
        private readonly ScriptSystem _handle;
        public ReadyConnector(ScriptSystem handle) => _handle = handle;
        public Task<IStreamSystemHandle> ConnectAsync(StreamBusOptions options, CancellationToken ct) => Task.FromResult<IStreamSystemHandle>(_handle);
    }

    private static (StreamTopologyRegistry Registry, ScriptSystem System) CreateRegistry()
    {
        var system = new ScriptSystem();
        var options = new StreamBusOptions { Vhost = "/", UserName = "app", Password = "secret" };
        var supervisor = new StreamBusConnectionSupervisor(options, new ReadyConnector(system));
        supervisor.StartAsync(CancellationToken.None).GetAwaiter().GetResult();
        var readiness = supervisor.WaitUntilReadyAsync(ReadinessPolicy.Indefinitely, new CancellationTokenSource(TimeSpan.FromSeconds(5)).Token).GetAwaiter().GetResult();
        Assert.Equal(BusReadiness.Ready, readiness);
        return (new StreamTopologyRegistry(supervisor, "/"), system);
    }

    [Fact]
    public async Task Repeat_same_spec_makes_no_further_broker_calls()
    {
        var (registry, system) = CreateRegistry();
        var ct = TestContext.Current.CancellationToken;
        var decl = new SuperStreamDeclaration { Name = "ss", Partitions = 3 };
        await registry.EnsureSuperStreamAsync(decl, ct);
        await registry.EnsureSuperStreamAsync(decl, ct);
        Assert.Single(system.CreatedSupers);
        var probesAfterFirst = system.ExistsProbes;
        await registry.EnsureSuperStreamAsync(decl, ct);
        Assert.Equal(probesAfterFirst, system.ExistsProbes);
    }

    [Fact]
    public async Task Four_concurrent_same_spec_issue_single_create()
    {
        var (registry, system) = CreateRegistry();
        var ct = TestContext.Current.CancellationToken;
        var decl = new SuperStreamDeclaration { Name = "ss", Partitions = 3 };
        await Task.WhenAll(Enumerable.Range(0, 4).Select(_ => registry.EnsureSuperStreamAsync(decl, ct)));
        Assert.Single(system.CreatedSupers);
    }

    [Fact]
    public async Task Preexisting_identical_spec_needs_no_create()
    {
        var (registry, system) = CreateRegistry();
        var ct = TestContext.Current.CancellationToken;
        system.SuperExists = _ => true;
        system.Partitions = _ => new[] { "ss-0", "ss-1", "ss-2" };
        await registry.EnsureSuperStreamAsync(new SuperStreamDeclaration { Name = "ss", Partitions = 3 }, ct);
        Assert.Empty(system.CreatedSupers);
    }

    [Fact]
    public async Task Raced_identical_create_succeeds()
    {
        var (registry, system) = CreateRegistry();
        var ct = TestContext.Current.CancellationToken;
        // Pre-checks see absent; the broker accepts the identical create (Tasks 1–2 probes).
        await registry.EnsureSuperStreamAsync(new SuperStreamDeclaration { Name = "ss", Partitions = 3 }, ct);
        Assert.Single(system.CreatedSupers);
    }

    [Fact]
    public async Task Create_rejection_with_matching_partitions_is_knob_conflict()
    {
        var (registry, system) = CreateRegistry();
        var ct = TestContext.Current.CancellationToken;
        // The registry's pre-check only attempts the create while the super looks
        // absent, so the first existence probe answers absent and later probes see
        // the raced-into-existence super with matching partitions.
        var superProbes = 0;
        system.SuperExists = _ => Interlocked.Increment(ref superProbes) > 1;
        system.OnCreateSuper = (_, _) => throw new InvalidOperationException("broker: PreconditionFailed");
        system.Partitions = _ => new[] { "ss-0", "ss-1", "ss-2" };
        var ex = await Assert.ThrowsAsync<StreamConfigurationConflictException>(() =>
            registry.EnsureSuperStreamAsync(new SuperStreamDeclaration { Name = "ss", Partitions = 3, MaxAge = TimeSpan.FromDays(1) }, ct));
        Assert.Contains("differing arguments", ex.Actual);
    }

    [Fact]
    public async Task Create_rejection_with_existing_plain_is_conflict()
    {
        var (registry, system) = CreateRegistry();
        var ct = TestContext.Current.CancellationToken;
        // Same absent-then-present sequencing as the super knob-conflict test: the
        // pre-check attempts the create (which the broker rejects), and the
        // re-probe then finds the plain stream, proving the specs differed.
        var streamProbes = 0;
        system.StreamExists = _ => Interlocked.Increment(ref streamProbes) > 1;
        system.OnCreateStream = (_, _) => throw new InvalidOperationException("broker: PreconditionFailed");
        var ex = await Assert.ThrowsAsync<StreamConfigurationConflictException>(() =>
            registry.EnsureStreamAsync(new StreamDeclaration { Name = "s", MaxAge = TimeSpan.FromDays(1) }, ct));
        Assert.Contains("differing arguments", ex.Actual);
    }

    [Fact]
    public async Task Coexisting_plain_and_super_conflicts()
    {
        var (registry, system) = CreateRegistry();
        var ct = TestContext.Current.CancellationToken;
        system.SuperExists = _ => true;
        system.StreamExists = _ => true;
        system.Partitions = _ => new[] { "ss-0", "ss-1", "ss-2" };
        var ex = await Assert.ThrowsAsync<StreamConfigurationConflictException>(() =>
            registry.EnsureSuperStreamAsync(new SuperStreamDeclaration { Name = "ss", Partitions = 3 }, ct));
        Assert.Contains("coexisting", ex.Actual);
    }

    [Fact]
    public async Task Partition_mismatch_throws_with_expected_and_actual()
    {
        var (registry, system) = CreateRegistry();
        var ct = TestContext.Current.CancellationToken;
        system.OnCreateSuper = (_, _) => throw new InvalidOperationException("broker: PreconditionFailed");
        system.SuperExists = _ => true;
        system.Partitions = _ => new[] { "ss-0", "ss-1", "ss-2" };
        var ex = await Assert.ThrowsAsync<StreamConfigurationConflictException>(() =>
            registry.EnsureSuperStreamAsync(new SuperStreamDeclaration { Name = "ss", Partitions = 5 }, ct));
        Assert.Contains("5", ex.Message);
        Assert.Contains("3", ex.Message);
        Assert.Contains("5", ex.Expected);
        Assert.Contains("3", ex.Actual);
    }

    [Fact]
    public async Task Kind_mismatch_throws()
    {
        var (registry, system) = CreateRegistry();
        var ct = TestContext.Current.CancellationToken;
        system.StreamExists = _ => true;
        var ex = await Assert.ThrowsAsync<StreamConfigurationConflictException>(() =>
            registry.EnsureSuperStreamAsync(new SuperStreamDeclaration { Name = "s", Partitions = 2 }, ct));
        Assert.Contains("plain stream", ex.Actual);
        // A plain Ensure over an occupying super must reach the compare path: the
        // create attempt has to fail (a successful create is success, per the
        // Task 1 PLAIN-ON-SUPER-NAME probe), so the rejection is scripted here.
        system.StreamExists = _ => false;
        system.SuperExists = _ => true;
        system.Partitions = _ => new[] { "s-0" };
        system.OnCreateStream = (_, _) => throw new InvalidOperationException("broker: PreconditionFailed");
        var ex2 = await Assert.ThrowsAsync<StreamConfigurationConflictException>(() =>
            registry.EnsureStreamAsync(new StreamDeclaration { Name = "s" }, ct));
        Assert.Contains("1", ex2.Actual);
    }

    [Fact]
    public async Task Create_failure_with_absent_stream_rethrows_original()
    {
        var (registry, system) = CreateRegistry();
        var ct = TestContext.Current.CancellationToken;
        system.OnCreateStream = (_, _) => throw new InvalidOperationException("broker: nope");
        var ex = await Assert.ThrowsAsync<InvalidOperationException>(() =>
            registry.EnsureStreamAsync(new StreamDeclaration { Name = "s" }, ct));
        Assert.Contains("nope", ex.Message);
    }

    [Fact]
    public async Task Preexisting_plain_stream_needs_no_create()
    {
        var (registry, system) = CreateRegistry();
        var ct = TestContext.Current.CancellationToken;
        system.StreamExists = _ => true;
        await registry.EnsureStreamAsync(new StreamDeclaration { Name = "s" }, ct);
        Assert.Empty(system.CreatedStreams);
    }
}
```

Two more tests live in `tests/StreamBus.UnitTests/HostingTests.cs` (reusing that file's existing `StubHandle`/`SuccessConnector`/marker patterns — adapt member names to what that file defines):

```csharp
    [Fact]
    public async Task Startup_declares_union_of_registrations()
    {
        var ct = TestContext.Current.CancellationToken;
        var handle = new StubHandle();
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddStreamBus("/", o =>
        {
            o.Credentials("app", "secret");
            o.Topology(t => t.DeclareStream("stream-a"));
        });
        services.AddStreamBus<OrdersBus>("/", o =>
        {
            o.Credentials("app", "secret");
            o.Topology(t => t.DeclareStream("stream-b"));
        });
        services.AddSingleton<IStreamSystemConnector>(new SuccessConnector(handle));
        await using var sp = services.BuildServiceProvider();
        var hosted = sp.GetServices<IHostedService>().OfType<StreamBusHostedService>().Single();
        await hosted.StartAsync(ct);
        try
        {
            var deadline = DateTime.UtcNow + TimeSpan.FromSeconds(5);
            while (true)
            {
                List<string> snapshot;
                lock (handle.CreatedStreams) snapshot = handle.CreatedStreams.ToList();
                if (snapshot.Contains("stream-a") && snapshot.Contains("stream-b")) break;
                if (DateTime.UtcNow >= deadline) break;
                await Task.Delay(TimeSpan.FromMilliseconds(50), ct);
            }
            List<string> created;
            lock (handle.CreatedStreams) created = handle.CreatedStreams.ToList();
            Assert.Contains("stream-a", created);
            Assert.Contains("stream-b", created);
        }
        finally
        {
            await hosted.StopAsync(CancellationToken.None);
        }
    }

    [Fact]
    public async Task Blocking_startup_timeout_names_topology()
    {
        var ct = TestContext.Current.CancellationToken;
        var handle = new StubHandle();
        handle.StreamExistsFunc = async (_, ct2) => { await Task.Delay(Timeout.Infinite, ct2); return false; };
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddStreamBus("/", o =>
        {
            o.Credentials("app", "secret");
            o.Topology(t => t.DeclareStream("hung-stream"));
            o.Hosting(h => h.BlockStartupUntilReady(TimeSpan.FromMilliseconds(200)));
        });
        services.AddSingleton<IStreamSystemConnector>(new SuccessConnector(handle));
        await using var sp = services.BuildServiceProvider();
        var hosted = sp.GetServices<IHostedService>().OfType<StreamBusHostedService>().Single();
        try
        {
            var ex = await Assert.ThrowsAsync<StreamBusTimeoutException>(() => hosted.StartAsync(ct));
            Assert.Contains("topology", ex.Message, StringComparison.Ordinal);
        }
        finally
        {
            await hosted.StopAsync(CancellationToken.None);
        }
    }
```

(`Assert.Single(system.CreatedSupers)` on a `List<string>` uses the collection overload ✓. Arg key names follow the recorded Task 1 table (`max-age`, `max-length-bytes`, `stream-max-segment-size-bytes` — no `x-` prefix). `Interlocked` needs no using (`System.Threading` is implicit). `Array.Empty<string>()` needs `System` (implicit ✓).)

- [ ] **Step 2: Verify**

```powershell
dotnet test tests/StreamBus.UnitTests/StreamBus.UnitTests.csproj -c Release
```

Expected: all green, output pristine.

- [ ] **Step 3: Commit (with human approval)**

```powershell
git add tests/StreamBus.UnitTests/TopologyTests.cs
git commit -m "test: add topology mapping, validation, and conflict unit tests"
```

---

### Task 5: Integration tests — convergence, conflicts, startup, retention

**Files:**
- Create: `tests/StreamBus.IntegrationTests/TopologyIntegrationTests.cs`

**Interfaces:**
- Consumes: `bus.Topology`, `AddStreamBus` `Topology(...)` config, admin `StreamSystem`, `PartitionsSuperStreamSpec`, `BusInstance`-style harness (extended with topology config).
- Produces: FR-7.1/7.2/7.4/7.5 evidence; G-3 test present but skip-guarded (gate stays open on single node).

Skip mechanism (applies to the two conditional tests): `Assert.Skip(reason)` (xUnit v3 runtime skip). If the compiler rejects it in xUnit 4.0.2, `return;` with a `// skipped: <reason>` comment instead — same file, no other change.

- [ ] **Step 1: Harness + idempotency + concurrency + restart**

```csharp
using System.Net;
using System.Net.Http.Headers;
using System.Text;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using RabbitMQ.Stream.Client;

namespace StreamBus.IntegrationTests;

public sealed class TopologyIntegrationTests : IAsyncDisposable
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

    private sealed class BusInstance : IAsyncDisposable
    {
        public ServiceProvider Provider { get; private set; } = null!;
        public StreamBusHostedService Hosted { get; private set; } = null!;
        public IStreamBus Bus { get; private set; } = null!;
        public static async Task<BusInstance> StartAsync(string vhost = "/", Action<StreamBusOptionsBuilder>? configure = null)
        {
            var services = new ServiceCollection();
            services.AddLogging();
            services.AddStreamBus(vhost, o => { o.Credentials(User(), Password()); configure?.Invoke(o); });
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

    private static ReadOnlyMemory<byte> Body(string s) => Encoding.UTF8.GetBytes(s);

    private static async Task WaitForAsync(Func<bool> done, TimeSpan timeout, string what)
    {
        var deadline = DateTime.UtcNow + timeout;
        while (!done())
        {
            if (DateTime.UtcNow >= deadline) Assert.Fail($"Timed out waiting for {what}.");
            await Task.Delay(100, TestContext.Current.CancellationToken);
        }
    }

    [Fact]
    public async Task Repeat_and_concurrent_declares_converge()
    {
        var ct = TestContext.Current.CancellationToken;
        var plain = Unique("topo-plain");
        var super = Unique("topo-super");
        _streams.Add(plain);
        _superStreams.Add(super);
        await using var inst = await BusInstance.StartAsync();
        await inst.Bus.Topology.EnsureStreamAsync(new StreamDeclaration { Name = plain }, ct);
        await inst.Bus.Topology.EnsureStreamAsync(new StreamDeclaration { Name = plain }, ct);
        var sdecl = new SuperStreamDeclaration { Name = super, Partitions = 3 };
        await Task.WhenAll(Enumerable.Range(0, 4).Select(_ => inst.Bus.Topology.EnsureSuperStreamAsync(sdecl, ct)));
        var admin = await ConnectAdminAsync();
        await using var _ = admin;
        Assert.True(await admin.StreamExists(plain));
        Assert.True(await admin.SuperStreamExists(super));
        Assert.Equal(3, (await admin.QueryPartition(super)).Length);
        await inst.Bus.PublishAsync(plain, Body("k:0"), new PublishOptions(), ct);
        await inst.Bus.PublishAsync(super, Body("k:0"), new PublishOptions { IsSuperStream = true, RoutingKey = "k" }, ct);
    }
```

- [ ] **Step 2: Conflicts name expected vs actual**

```csharp
    [Fact]
    public async Task Super_partition_conflict_names_expected_and_actual()
    {
        var ct = TestContext.Current.CancellationToken;
        var super = Unique("topo-super");
        _superStreams.Add(super);
        await using var inst = await BusInstance.StartAsync();
        await inst.Bus.Topology.EnsureSuperStreamAsync(new SuperStreamDeclaration { Name = super, Partitions = 3 }, ct);
        var ex = await Assert.ThrowsAsync<StreamConfigurationConflictException>(() =>
            inst.Bus.Topology.EnsureSuperStreamAsync(new SuperStreamDeclaration { Name = super, Partitions = 5 }, ct));
        Assert.Contains("5", ex.Message);
        Assert.Contains("3", ex.Message);
        Assert.Contains("5", ex.Expected);
        Assert.Contains("3", ex.Actual);
    }

    [Fact]
    public async Task Kind_mismatch_conflicts()
    {
        var ct = TestContext.Current.CancellationToken;
        var plain = Unique("topo-plain");
        var super = Unique("topo-super");
        _streams.Add(plain);
        _superStreams.Add(super);
        await using var inst = await BusInstance.StartAsync();
        await inst.Bus.Topology.EnsureStreamAsync(new StreamDeclaration { Name = plain }, ct);
        var ex = await Assert.ThrowsAsync<StreamConfigurationConflictException>(() =>
            inst.Bus.Topology.EnsureSuperStreamAsync(new SuperStreamDeclaration { Name = plain, Partitions = 2 }, ct));
        Assert.Contains("plain stream", ex.Actual);
        await inst.Bus.Topology.EnsureSuperStreamAsync(new SuperStreamDeclaration { Name = super, Partitions = 2 }, ct);
        var ex2 = await Assert.ThrowsAsync<StreamConfigurationConflictException>(() =>
            inst.Bus.Topology.EnsureStreamAsync(new StreamDeclaration { Name = super }, ct));
        Assert.Contains("2", ex2.Actual);
    }

    [Fact]
    public async Task Restart_with_identical_spec_succeeds()
    {
        var ct = TestContext.Current.CancellationToken;
        var plain = Unique("topo-plain");
        var super = Unique("topo-super");
        _streams.Add(plain);
        _superStreams.Add(super);
        await using var first = await BusInstance.StartAsync();
        await first.Bus.Topology.EnsureStreamAsync(new StreamDeclaration { Name = plain, MaxAge = TimeSpan.FromDays(7) }, ct);
        await first.Bus.Topology.EnsureSuperStreamAsync(new SuperStreamDeclaration { Name = super, Partitions = 2 }, ct);
        await first.DisposeAsync();
        await using var second = await BusInstance.StartAsync();
        await second.Bus.Topology.EnsureStreamAsync(new StreamDeclaration { Name = plain, MaxAge = TimeSpan.FromDays(7) }, ct);
        await second.Bus.Topology.EnsureSuperStreamAsync(new SuperStreamDeclaration { Name = super, Partitions = 2 }, ct);
        var admin = await ConnectAdminAsync();
        await using var _ = admin;
        Assert.True(await admin.StreamExists(plain));
        Assert.Equal(2, (await admin.QueryPartition(super)).Length);
    }

    [Fact]
    public async Task Plain_knob_redclare_succeeds_with_warning()
    {
        // Plain-stream argument compatibility is not readable via the stream
        // protocol (Review Focus 5): identical-name knob differences converge
        // instead of throwing. Keep declarations in sync across processes.
        var ct = TestContext.Current.CancellationToken;
        var plain = Unique("topo-plain");
        _streams.Add(plain);
        await using var inst = await BusInstance.StartAsync();
        await inst.Bus.Topology.EnsureStreamAsync(new StreamDeclaration { Name = plain }, ct);
        await inst.Bus.Topology.EnsureStreamAsync(new StreamDeclaration { Name = plain, MaxAge = TimeSpan.FromDays(7) }, ct);
        var admin = await ConnectAdminAsync();
        await using var _ = admin;
        Assert.True(await admin.StreamExists(plain));
    }
```

- [ ] **Step 3: Startup declaration in both modes, plus startup conflict**

```csharp
    [Fact]
    public async Task Nonblocking_startup_declares_in_background()
    {
        var ct = TestContext.Current.CancellationToken;
        var plain = Unique("topo-plain");
        var super = Unique("topo-super");
        _streams.Add(plain);
        _superStreams.Add(super);
        await using var inst = await BusInstance.StartAsync("/", o => o.Topology(t => t
            .DeclareStream(plain)
            .DeclareSuperStream(super, 2)));
        var admin = await ConnectAdminAsync();
        await using var _ = admin;
        await WaitForAsync(() => admin.StreamExists(plain).GetAwaiter().GetResult()
            && admin.SuperStreamExists(super).GetAwaiter().GetResult(), TimeSpan.FromSeconds(30), "startup declaration");
        Assert.Equal(2, (await admin.QueryPartition(super)).Length);
    }

    [Fact]
    public async Task Blocking_startup_declares_before_return()
    {
        var ct = TestContext.Current.CancellationToken;
        var plain = Unique("topo-plain");
        var super = Unique("topo-super");
        _streams.Add(plain);
        _superStreams.Add(super);
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddStreamBus("/", o =>
        {
            o.Credentials(User(), Password());
            o.Topology(t => t.DeclareStream(plain).DeclareSuperStream(super, 2));
            o.Hosting(h => h.BlockStartupUntilReady(TimeSpan.FromSeconds(30)));
        });
        await using var sp = services.BuildServiceProvider();
        var hosted = sp.GetServices<IHostedService>().OfType<StreamBusHostedService>().Single();
        await hosted.StartAsync(ct);
        try
        {
            var admin = await ConnectAdminAsync();
            await using var _ = admin;
            Assert.True(await admin.StreamExists(plain));
            Assert.Equal(2, (await admin.QueryPartition(super)).Length);
        }
        finally
        {
            await hosted.StopAsync(ct);
        }
    }

    [Fact]
    public async Task Blocking_startup_with_conflict_fails_loudly()
    {
        var ct = TestContext.Current.CancellationToken;
        var super = Unique("topo-super");
        _superStreams.Add(super);
        var admin = await ConnectAdminAsync();
        await using var _ = admin;
        await admin.CreateSuperStream(new PartitionsSuperStreamSpec(super, 3));
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddStreamBus("/", o =>
        {
            o.Credentials(User(), Password());
            o.Topology(t => t.DeclareSuperStream(super, 5));
            o.Hosting(h => h.BlockStartupUntilReady(TimeSpan.FromSeconds(30)));
        });
        await using var sp = services.BuildServiceProvider();
        var hosted = sp.GetServices<IHostedService>().OfType<StreamBusHostedService>().Single();
        var ex = await Assert.ThrowsAsync<StreamConfigurationConflictException>(() => hosted.StartAsync(ct));
        Assert.Contains("5", ex.Message);
        Assert.Contains("3", ex.Message);
        await hosted.StopAsync(ct);
    }
```

(`admin.StreamExists(...).GetAwaiter().GetResult()` inside the poll predicate keeps the `Func<bool>` shape — the polled call is idempotent and fast. `o.Hosting(h => ...)` follows the existing builder (`BlockStartupUntilReady(TimeSpan?)` ✓).)

- [ ] **Step 4: Retention usability + conditional policy + conditional G-3**

```csharp
    [Fact]
    public async Task Retention_declaration_is_usable()
    {
        // Effective retention is not readable via the stream protocol; this pins
        // that a retention-carrying declaration succeeds and the stream works.
        var ct = TestContext.Current.CancellationToken;
        var error = Unique("topo-error");
        _streams.Add(error);
        await using var inst = await BusInstance.StartAsync();
        await inst.Bus.Topology.EnsureStreamAsync(new StreamDeclaration { Name = error, MaxAge = TimeSpan.FromDays(7) }, ct);
        await inst.Bus.PublishAsync(error, Body("k:0"), new PublishOptions(), ct);
        var got = new List<string>();
        using var c = await inst.Bus.ConsumeAsync(error,
            new ConsumeOptions { Offset = ConsumeOffset.AlwaysFirst, Reference = Unique("read") },
            ctx => { lock (got) got.Add(Encoding.UTF8.GetString(ctx.Body.Span)); return Task.CompletedTask; }, ct);
        await WaitForAsync(() => { lock (got) return got.Count >= 1; }, TimeSpan.FromSeconds(30), "retained message");
    }

    private static async Task<bool> ManagementReachableAsync()
    {
        try
        {
            using var http = new HttpClient { Timeout = TimeSpan.FromSeconds(5) };
            http.DefaultRequestHeaders.Authorization = new AuthenticationHeaderValue("Basic",
                Convert.ToBase64String(Encoding.UTF8.GetBytes($"{User()}:{Password()}")));
            using var resp = await http.GetAsync("http://127.0.0.1:15672/api/overview", TestContext.Current.CancellationToken);
            return resp.IsSuccessStatusCode;
        }
        catch
        {
            return false;
        }
    }

    [Fact]
    public async Task Policy_applied_tuning_is_effective()
    {
        // FR-7.4 operator path: production tuning belongs in policies, not queue
        // arguments. Runs only where the management plugin is reachable.
        if (!await ManagementReachableAsync()) Assert.Skip("management API not usable with stream credentials (unreachable or unauthorized); FR-7.4 operator path not exercised here.");
        var ct = TestContext.Current.CancellationToken;
        var stream = Unique("topo-pol");
        _streams.Add(stream);
        await using var inst = await BusInstance.StartAsync();
        await inst.Bus.Topology.EnsureStreamAsync(new StreamDeclaration { Name = stream }, ct);
        var policy = Unique("pol");
        using var http = new HttpClient { Timeout = TimeSpan.FromSeconds(10) };
        http.DefaultRequestHeaders.Authorization = new AuthenticationHeaderValue("Basic",
            Convert.ToBase64String(Encoding.UTF8.GetBytes($"{User()}:{Password()}")));
        var put = await http.PutAsync(
            $"http://127.0.0.1:15672/api/policies/%2F/{policy}",
            new StringContent(
                $"{{\"pattern\":\"^{stream}$\",\"definition\":{{\"max-age\":3600}},\"priority\":0}}",
                Encoding.UTF8, "application/json"), ct);
        Assert.True(put.IsSuccessStatusCode, $"PUT policy failed: {(int)put.StatusCode}");
        try
        {
            using var get = await http.GetAsync($"http://127.0.0.1:15672/api/queues/%2F/{stream}", ct);
            var payload = await get.Content.ReadAsStringAsync(ct);
            Assert.Contains("max-age", payload);
        }
        finally
        {
            await http.DeleteAsync($"http://127.0.0.1:15672/api/policies/%2F/{policy}", ct);
        }
    }

    [Fact]
    public async Task Leader_placement_gate_G3()
    {
        // G-3: leader-locator and replica placement need a 3-node cluster. This
        // test proves the gate is WIRED (declaration + StreamInfo observation) and
        // skips on a single node instead of asserting unmeasured behavior.
        var ct = TestContext.Current.CancellationToken;
        var super = Unique("topo-super");
        _superStreams.Add(super);
        await using var inst = await BusInstance.StartAsync();
        await inst.Bus.Topology.EnsureSuperStreamAsync(new SuperStreamDeclaration { Name = super, Partitions = 3 }, ct);
        var admin = await ConnectAdminAsync();
        await using var _ = admin;
        var info = await admin.StreamInfo(super);
        // Single-node brokers report replicas=1 here (Task 1 probe) — placement is
        // only observable with real replicas, so gate on > 1, not > 0.
        if (info.Replicas.Count <= 1) Assert.Skip("fewer than 2 replicas; G-3 requires a 3-node cluster.");
        Assert.NotNull(info.Leader);
    }
```

(`AuthenticationHeaderValue` needs `System.Net.Http.Headers` — the file imports it ✓. `Convert`/`Encoding` need `System`/`System.Text` (implicit/file-level ✓ — `System.Text` via the `using System.Text;` line ✓). The policy `max-age` value form (`3600` vs `"3600s"`) follows the broker's policy-definition convention; if the PUT shape is rejected the test fails loudly with the status — the executor does not guess alternatives, it reports the broker's answer.)

- [ ] **Step 5: Verify**

```powershell
dotnet test tests/StreamBus.IntegrationTests/StreamBus.IntegrationTests.csproj -c Release --filter "FullyQualifiedName~Topology"
```

Expected: all new tests green (policy + G-3 either run or Skip — a Skip is a pass for gating purposes only if its skip reason names the environment; both do). Then the full suites:

```powershell
dotnet test tests/StreamBus.UnitTests/StreamBus.UnitTests.csproj -c Release
dotnet test tests/StreamBus.IntegrationTests/StreamBus.IntegrationTests.csproj -c Release
```

Expected: everything green (Phases 3–7 unbroken).

- [ ] **Step 6: Commit (with human approval)**

```powershell
git add tests/StreamBus.IntegrationTests/TopologyIntegrationTests.cs
git commit -m "test: add topology convergence, conflict, and gate integration probes"
```

---

## Follow-ups for Phase 9+ (tracked, no code in this phase)

- G-3 stays OPEN until a 3-node cluster run turns the skip into asserts (leader spread per `least-leaders`, replica consumers). Nothing in this phase may claim placement behavior from single-node evidence.
- Plain-stream knob-only conflicts are protocol-invisible: the success-with-warning residual stands until/unless the broker exposes argument read-back. Keep cross-process specs in sync (same-spec startup is the enforced pattern).
- `BindingsSuperStreamSpec` (routing-based super streams) is out of v1; only the partitions kind is declarable.
- The `balanced` locator alias is unoffered (client 1.12.1 has no member); `LeastLeaders` is its documented successor.
- Policy lifecycle (create/update/delete, pattern governance) is operator-owned; the library will never manage policies.
- The per-name gate pool grows with distinct declared names and is never evicted (bounded by the deployment's stream count — same ceiling as the producer/consumer registries).

---

## Self-Review

- Spec coverage: FR-7.1 (Tasks 2 cache+fingerprint + 4 repeat test + 5 repeat/concurrent tests), FR-7.2 (Tasks 2 race-tolerant create + compare + conflict type, 4 race/conflict/kind tests, 5 conflict/kind/restart tests, 3 union-startup), FR-7.3 (Tasks 2 default+mapping, 4 locator tests, 5 declare-success, 5 G-3 skip-guarded), FR-7.4 (Tasks 2 retention knobs + no policy API, 4 Args-emission test, 5 policy-conditional + docs), FR-7.5 (Tasks 3 startup both modes + union, 5 startup tests), FR-3.3 immutability (Tasks 2 partition compare + 4/5 conflict tests), G-3 (Task 5 conditional test; gate open — stated in header, follow-ups, and test skip reason).
- Placeholder scan: no TBD/TODO; every code step ships complete source; Task 2 Step 0 branches on the concurrent-identical outcome with both branches fully specified (primary refined rule, fallback match-tolerated); all other probe outcomes need no branching (uniform compare); no "similar to Task N" (patterns are copied verbatim where reused: readiness gate, fault continuation, harness); the two `Assert.Skip` sites carry the exact early-return fallback.
- Type consistency: `EnsureStreamAsync(StreamDeclaration, ct)` / `EnsureSuperStreamAsync(SuperStreamDeclaration, ct)` (Tasks 2/3/5/6 match); `CreateStreamAsync(StreamSpec, ct)` / `CreateSuperStreamAsync(PartitionsSuperStreamSpec, ct)` handle methods (Tasks 2/4 match); `TopologyFor(vhost)` (Tasks 3/5 match); `bus.Topology` (Tasks 3/5/6 match); `DeclareStream(name, knobs?)` / `DeclareSuperStream(name, partitions, knobs?)` builder (Tasks 3/4/6 match); `StreamConfigurationConflictException(stream, expected, actual)` + `Stream`/`Expected`/`Actual` (Tasks 2/4/6 match); `Fingerprint` internal to Task 2 only.
- Review Focus: each line pinned (6c, 5-single-create + 6b, 6e, startup-conflict test, 6 knob test + warning).
