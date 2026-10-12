# StreamBus Phase 5 (Consumer Registry) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Implement Phase 5 consumer registry: delegate-path ordered consumption on plain and super streams with broker-owned offset tracking, SAC load balancing, per-partition metrics, and the promotion fix — plus the G-4/G-5a gates.

**Architecture:** One `StreamConsumerRegistry` per vhost owns partition loops; each partition runs a supervision loop around a `RawConsumer` (never `Reliable*`) that resolves its resume offset from the broker on every attach (create, SAC promote, failure-restart) via a pure, unit-tested resolution function, dispatches strictly sequentially through the caller's delegate, stores only forward via the per-partition consumer, and recreates with backoff after handler failures so nothing is ever silently skipped. Observability flows through a shared `System.Diagnostics.Metrics` meter (observable gauges + event counters) and `ILogger` warnings.

**Tech Stack:** .NET 10 (`net10.0`), C# latest, `RabbitMQ.Stream.Client` 1.12.1 (same pins — no new packages; metrics are in the shared framework), xUnit v3, `StreamBus.slnx`, Central Package Management.

**Spec:** `docs/2026-10-08-streambus-design.md` (§4 ordering/SAC/promotion fix, §9 consume sketch, §12 offset mechanics, §13 metrics catalog) and `docs/2026-10-08-streambus-requirements.md` (FR-2.2–FR-2.7, FR-3.1–FR-3.2, FR-9.1, FR-10.1–FR-10.13). Phase table: design §10 row 5. Gates G-4 and G-5a in this phase; G-5b (Strict-retry window) belongs to Phase 6 with retry.

## Global Constraints

- `TargetFramework` exactly `net10.0`; `Nullable`/`ImplicitUsings` enable; `TreatWarningsAsErrors` true; `ManagePackageVersionsCentrally` true; `IsTestingPlatformApplication` false (all in `Directory.Build.props`, unchanged).
- `RabbitMQ.Stream.Client` exactly `1.12.1`; `Microsoft.Extensions.Hosting` / `Diagnostics.HealthChecks` / `Logging.Abstractions` exactly `10.0.12`; `xunit.v3` 4.0.2, `xunit.runner.visualstudio` 4.0.1, `Microsoft.NET.Test.Sdk` 18.10.1.
- Library root namespace `StreamBus`; never `RabbitMQ.Stream.*`.
- Ordered paths use `RawConsumer` plus the library supervision loop — never `Reliable*` (its built-in reconnect cannot be disabled and its last-dispatched resume skips unprocessed messages under retry).
- The resume point is never produced implicitly as `OffsetTypeNext`, and never replayed implicitly as `OffsetTypeFirst`: the only `Next` path is explicit `AlwaysNext` (or the `ResumeOrNext` no-stored branch); the only `First` paths are explicit `AlwaysFirst`, the `ResumeOrFirst` no-stored branch, or a failed resolve query (replay-safe direction, always warned + counted).
- The resume point is never inferred from local, in-memory, or last-dispatched state — `localWater` is a write-guard only, never a resume source.
- A `src/` grep for `ReliableProducer`, `ReliableConsumer`, `ReliableSuperStream` (any `Reliable*` client type) must return nothing; committed in Task 6 verify.
- Every commit step requires explicit human approval before running; never push, never amend without permission.

## Review Focus

- SAC failover mid-stream loses nothing, duplicates nothing, and keeps per-key order: kill one of three same-reference instances mid-backlog, the rest absorb every message exactly once with each key's subsequence ordered (G-4, FR-2.3/2.6/3.1). Pinned by Task 6 failover test asserting totals + order + final stored offset.
- Death-and-recreate resumes from the broker-stored offset, never from local state: consume half, dispose, recreate same reference, only the remainder arrives (G-5a, FR-2.5/10.4). Pinned by Task 6 resume test (a localWater-based resume would redeliver and fail the total).
- A stale or demoted instance's backward store is rejected locally, counted, and never sent (FR-2.7/10.7). Pinned by Task 5 guard unit test + fallback/rejected MeterListener tests.
- Every no-stored-offset fallback warns (reference, partition, branch, resolved offset) and counts by branch — never silent (FR-10.12). Pinned by Task 5 logger-sink + counter tests and Task 6 fresh-stream runs.
- A throwing handler redelivers from the stored offset with order preserved around the failure; the offset advances only after success (FR-10.6). Pinned by Task 6 fail-then-succeed test (fail twice on one message, then succeed; assert redelivery set + order + final offset).

---

## File Structure

New files (no new packages):

- `src/StreamBus/ConsumeOptions.cs` — `ConsumeOptions`, `ConsumeOffset`, `ConsumeOrdering`, `OffsetStoreOptions`.
- `src/StreamBus/ConsumeContext.cs` — `ConsumeContext`, `MessageMetadata`.
- `src/StreamBus/ReferenceDerivation.cs` — auto-derivation + 255-byte validation.
- `src/StreamBus/OffsetResolution.cs` — pure `ResolveOffset`, `ResolveWithQueryAsync`, `MonotonicOffsetGuard`.
- `src/StreamBus/StreamBusMetrics.cs` — shared meter, observable gauges, event counters, partition/connection tracking.
- `src/StreamBus/StreamPartitionConsumer.cs` — per-partition attach loop, promotion listener, dispatch wrapper, `OffsetStorer`, `PartitionState`.
- `src/StreamBus/PartitionLagPoller.cs` — 15 s end-offset lag poller (interval-injectable for tests).
- `src/StreamBus/ConsumerRegistry.cs` — per-vhost `StreamConsumerRegistry` + `StreamConsumerRegistration` (`IStreamConsumer`).
- `src/StreamBus/StreamBusDelays.cs` — shared capped backoff helper.

Modified Phase 3/4 files (same branch lineage; existing suites must stay green):

- `src/StreamBus/Connector.cs` — `IStreamSystemHandle` gains `TryQueryOffsetAsync`, `StoreSystemOffsetAsync`, `GetEndOffsetAsync`, `StreamExistsAsync`, `SuperStreamExistsAsync`, `QueryPartitionAsync`; `StreamSystemHandle` delegates to the verified `StreamSystem` members.
- `src/StreamBus/ConnectionSupervisor.cs` — `ComputeDelay` delegates to the shared helper (test-visible behavior unchanged); gains `CurrentHandle` sibling pattern unchanged.
- `src/StreamBus/Bus.cs` — `ConsumeAsync`, `GetConsumerOffsetAsync`, `SetConsumerOffsetAsync` on both interfaces + both implementations.
- `src/StreamBus/Registry.cs` — per-vhost consumer registries + `ConsumersFor` accessor + `ActiveConsumers` aggregate + connection-state metric tracking in `GetOrCreateCore`.
- `src/StreamBus/Hosting.cs` — `StopAsync` disposes tracked consumers before supervisors.

Tests:

- `tests/StreamBus.UnitTests/OffsetResolutionTests.cs` — resolution matrix + query-failure branches + guard + derivation + validation.
- `tests/StreamBus.UnitTests/OffsetCadenceTests.cs` — `OffsetStorer` (Every N + redelivery window, MaxInterval timer, regression check).
- `tests/StreamBus.UnitTests/ConsumerMetricsTests.cs` — MeterListener pins (fallback/rejected counters, inflight gauge via wiring, lag poller with stub end-offset).
- `tests/StreamBus.IntegrationTests/ConsumerIntegrationTests.cs` — coverage/exactly-once/order (4 partitions × 3 instances), failover (G-4), broadcast, kill-resume (G-5a), offset branches, fail-then-succeed redelivery, Set/Get escapes.

Explicitly out of Phase 5 (deferred, not stubs): typed `IConsumer<T>` + `AddStreamBusConsumer` + serializer (Phase 9: FR-2.1/2.8); in-handler retry + `RetryPolicy`/`FailureMode`/`ErrorStream` (Phase 6: FR-4/FR-5, G-1, G-5b); topology `Ensure*` (Phase 8: FR-7, FR-3.3 enforcement); `PublishResultAsync` (Phase 7); hot-key auto-remedy (none exists — NFR-4, documented ceiling only).

Recorded rulings (spec as authority; bind all tasks): delegate path only, handler shape `Func<ConsumeContext, Task>` with `ct` inside the context; `IStreamBus.ConsumeAsync`/`IStreamBus<TBus>.ConsumeAsync` return `Task<IStreamConsumer>` (`IStreamConsumer : IAsyncDisposable` with `Stream` + `Reference`); `ReadinessPolicy` lives inside `ConsumeOptions` (default fail-fast — additive extension of the sketch satisfying §3.1 text; resolves the flagged sketch/text inconsistency); `IsSuperStream` explicit per call (default false); delegate default `Reference` derives from (bus marker type, stream) — two delegate consumers on one stream share a group unless given distinct references (documented); `Get/SetConsumerOffsetAsync` take `(stream, reference, partition)` — a `stream`-less signature cannot address (reference, stream) offsets, so the sketch is extended with the leading `stream` parameter (documented deviation); operator calls use fail-fast readiness; handler failure in this phase means log + dispose + backoff + recreate from the stored offset (Phase 6 refines to in-handler retry); poison messages loop forever with backoff by design until Phase 6; promotion/create query failure degrades to the configured no-stored branch (First replays safely, Fail throws `StreamBusConfigurationException` naming reference+partition); fallback counter labels carry branch only (resolved offset goes to the log line — label cardinality safety); `retry_blocked` gauge ships with base labels + `failure_mode="strict"`, no attempt label (Phase 6 drives real values); lag is consumer-held `max(0, delivered - stored)` computed live in the observable (ExpE proved `LastOffset()` unusable — see Round 2); `RoutingValue` is `string?` from the `streambus.routing-key` application property when present, else null; `MessageMetadata` maps client properties with `ApplicationProperties` defaulting to empty; handler `await`s everything inside the callback so dispatch stays strictly sequential per partition; store path awaits with catch-log-continue on the hot path and fire-and-forget on the timer; `localWater` key is `{reference}@{partitionStream}`, never cleared, `Set` overwrites with audit log; components take optional `ILogger<T>?` defaulting to `NullLogger`; G-4/G-5a run single-process multi-system (broker-arbitrated SAC, deterministic); coverage test uses 4 partitions × 3 instances (property-equivalent to the 6×4 example at lower flake cost); FR-3.3/3.4 need no code (nothing mutates partitions; ceiling already documented in requirements §4).

---

### Task 1: Consumer client API probe + records + reference derivation

**Files:**
- Create: `src/StreamBus/ConsumeOptions.cs`
- Create: `src/StreamBus/ConsumeContext.cs`
- Create: `src/StreamBus/ReferenceDerivation.cs`
- Create: `src/StreamBus/StreamBusDelays.cs`

**Interfaces:**
- Consumes: `ReadinessPolicy` (Phase 3), `StreamBusConfigurationException` (Phase 3).
- Produces: `ConsumeOptions` (+enums), `ConsumeContext`/`MessageMetadata`, `ReferenceDerivation` API, `StreamBusDelays.ExponentialCapped` for Tasks 2–5.

- [ ] **Step 1: Probe the 1.12.1 consumer API (read-only, record everything)**

Run in PowerShell and record the full output in the task report:

```powershell
$dll = Get-ChildItem "$env:USERPROFILE/.nuget/packages/rabbitmq.stream.client/1.12.1/lib" -Recurse -Filter RabbitMQ.Stream.Client.dll | Select-Object -First 1 -ExpandProperty FullName
$asm = [Reflection.Assembly]::LoadFrom($dll)
$asm.GetType("RabbitMQ.Stream.Client.RawConsumerConfig").GetProperties() | Select-Object Name, PropertyType
$asm.GetType("RabbitMQ.Stream.Client.RawConsumerConfig").GetConstructors() | Select-Object @{n="Params";e={($_.GetParameters() | ForEach-Object { "$($_.ParameterType.Name) $($_.Name)" }) -join ", "}}
$asm.GetType("RabbitMQ.Stream.Client.RawConsumer").GetMethods() | Select-Object Name, ReturnType, @{n="Params";e={($_.GetParameters() | ForEach-Object { "$($_.ParameterType.Name) $($_.Name)" }) -join ", "}}
$asm.GetType("RabbitMQ.Stream.Client.MessageContext").GetProperties() | Select-Object Name, PropertyType
$asm.GetType("RabbitMQ.Stream.Client.IOffsetType").Assembly.GetTypes() | Where-Object { $_.Name -like "OffsetType*" } | Select-Object Name, IsClass, @{n="Ctors";e={($_.GetConstructors() | ForEach-Object { "(" + (($_.GetParameters() | ForEach-Object { "$($_.ParameterType.Name) $($_.Name)" }) -join ", ") + ")" }) -join " "}}
$asm.GetType("RabbitMQ.Stream.Client.Message").GetProperty("Properties").PropertyType.GetProperties() | Select-Object Name, PropertyType
$asm.GetType("RabbitMQ.Stream.Client.StreamStats").GetProperties() | Select-Object Name, PropertyType
```

The report must answer: (a) `RawConsumerConfig` settable members for Stream/Reference/OffsetSpec/SingleActiveConsumer/ConsumerUpdateListener/MessageHandler (+ their exact delegate/property types and any ctor); (b) `RawConsumer.StoreOffset` exact signature; (c) `RawConsumer` dispose shape (`DisposeAsync` vs `Dispose` vs `Close`); (d) the delivered-offset source (`MessageContext` member name holding the offset, or explicit "absent"); (e) `OffsetTypeOffset` constructor shape; (f) `ConsumerUpdateListener` delegate type and parameter order; (g) client message `Properties` members for MessageId/CorrelationId/Timestamp and `ApplicationProperties` type; (h) which `StreamStats` member holds the end/last offset (name + type), or explicit "none usable".

- [ ] **Step 2: Implement `ConsumeOptions.cs`**

```csharp
namespace StreamBus;

public enum ConsumeOffset
{
    ResumeOrFirst = 0,
    ResumeOrNext = 1,
    ResumeOrFail = 2,
    AlwaysFirst = 3,
    AlwaysNext = 4,
}

public enum ConsumeOrdering
{
    Strict = 0,
}

public sealed record OffsetStoreOptions
{
    public int EveryMessages { get; init; } = 1;
    public TimeSpan? MaxInterval { get; init; }
}

public sealed record ConsumeOptions
{
    public bool IsSuperStream { get; init; } = false;
    public string? Reference { get; init; }
    public bool SingleActiveConsumer { get; init; } = true;
    public ConsumeOrdering Ordering { get; init; } = ConsumeOrdering.Strict;
    public ConsumeOffset Offset { get; init; } = ConsumeOffset.ResumeOrFirst;
    public OffsetStoreOptions OffsetStore { get; init; } = new();
    public ReadinessPolicy ReadinessPolicy { get; init; } = ReadinessPolicy.FailFastPolicy;
}
```

- [ ] **Step 3: Implement `ConsumeContext.cs`**

```csharp
namespace StreamBus;

public sealed record MessageMetadata
{
    public string? MessageId { get; init; }
    public string? CorrelationId { get; init; }
    public IReadOnlyDictionary<string, object?> ApplicationProperties { get; init; }
        = new Dictionary<string, object?>();
    public DateTimeOffset? Timestamp { get; init; }
}

public sealed record ConsumeContext
{
    public required string Stream { get; init; }
    public required int Partition { get; init; }
    public required ulong Offset { get; init; }
    public required string? RoutingValue { get; init; }
    public required ReadOnlyMemory<byte> Body { get; init; }
    public required MessageMetadata Metadata { get; init; }
    public required CancellationToken CancellationToken { get; init; }
}
```

`RoutingValue` reads the `streambus.routing-key` application property as a string when present, else null (mapping code lives in Task 3's wrapper; the property stays nullable here by design).

- [ ] **Step 4: Implement `ReferenceDerivation.cs`**

```csharp
using System.Security.Cryptography;
using System.Text;

namespace StreamBus;

internal static class ReferenceDerivation
{
    public const int MaxReferenceBytes = 255;

    public static string Derive(Type busType, string stream)
    {
        ArgumentNullException.ThrowIfNull(busType);
        ArgumentException.ThrowIfNullOrWhiteSpace(stream);
        var full = $"streambus:{busType.FullName}:{stream}";
        if (Encoding.UTF8.GetByteCount(full) <= MaxReferenceBytes)
            return full;
        var hash = Convert.ToHexString(SHA256.HashData(Encoding.UTF8.GetBytes(full)))[..32];
        return $"streambus-{hash}";
    }

    public static void Validate(string reference)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(reference);
        if (Encoding.UTF8.GetByteCount(reference) > MaxReferenceBytes)
            throw new StreamBusConfigurationException(
                $"Consumer Reference exceeds the broker's 255-byte limit ({Encoding.UTF8.GetByteCount(reference)} bytes). Supply a shorter stable reference.");
    }
}
```

- [ ] **Step 5: Implement `StreamBusDelays.cs` + delegate supervisor**

```csharp
namespace StreamBus;

internal static class StreamBusDelays
{
    public static TimeSpan ExponentialCapped(int attempt, TimeSpan baseDelay, TimeSpan maxDelay, int jitterCapMs = 1000)
    {
        ArgumentOutOfRangeException.ThrowIfLessThan(attempt, 1);
        var grown = TimeSpan.FromMilliseconds(baseDelay.TotalMilliseconds * Math.Pow(2, Math.Min(attempt - 1, 8)));
        if (grown > maxDelay) grown = maxDelay;
        var jitter = TimeSpan.FromMilliseconds(Random.Shared.Next(0, (int)Math.Min(jitterCapMs, grown.TotalMilliseconds / 2 + 1)));
        return grown + jitter;
    }
}
```

In `ConnectionSupervisor.cs`, replace the body of the existing public `ComputeDelay(int attempt)` with `=> StreamBusDelays.ExponentialCapped(attempt, TimeSpan.FromMilliseconds(200), TimeSpan.FromSeconds(30));` — keep the method, its name, visibility, and the `_baseDelay`/`_maxDelay` fields untouched so the existing cap test passes unchanged. (Read the current method first; change only its body.)

- [ ] **Step 6: Verify**

```powershell
dotnet build src/StreamBus/StreamBus.csproj -c Release
```

Expected: zero warnings/errors. Records are exercised by Tasks 3–5.

- [ ] **Step 7: Commit (with human approval)**

```powershell
git add src/StreamBus/ConsumeOptions.cs src/StreamBus/ConsumeContext.cs src/StreamBus/ReferenceDerivation.cs src/StreamBus/StreamBusDelays.cs src/StreamBus/ConnectionSupervisor.cs
git commit -m "feat: add consume options, context, reference derivation, shared backoff"
```

---

### Task 2: Offset core + handle offset methods

**Files:**
- Create: `src/StreamBus/OffsetResolution.cs`
- Modify: `src/StreamBus/Connector.cs` (handle gains `TryQueryOffsetAsync`, `StoreSystemOffsetAsync`, `GetEndOffsetAsync`, `StreamExistsAsync`, `SuperStreamExistsAsync`, `QueryPartitionAsync`; `StreamSystemHandle` delegates)
- Modify: `tests/StreamBus.UnitTests/ConnectionSupervisorTests.cs`, `tests/StreamBus.UnitTests/HostingTests.cs`, `tests/StreamBus.UnitTests/PublishTests.cs` (every existing `IStreamSystemHandle` stub gains the six members — grep `IStreamSystemHandle` under `tests/` first and update every implementer; stub bodies throw `NotImplementedException` with the test name)

**Interfaces:**
- Consumes: `StreamSystem.TryQueryOffset/StoreOffset/StreamExists/SuperStreamExists/QueryPartition` (verified members), `ConsumeOffset` (Task 1), `StreamBusConfigurationException` + `ILogger` patterns.
- Produces: `OffsetResolution.ResolveOffset`, `ResolveWithQueryAsync`, `MonotonicOffsetGuard`, widened `IStreamSystemHandle` for Tasks 3–6.

- [ ] **Step 1: Implement `OffsetResolution.cs`**

```csharp
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using RabbitMQ.Stream.Client;

namespace StreamBus;

internal static class OffsetResolution
{
    public static IOffsetType ResolveOffset(
        ConsumeOffset mode,
        ulong? stored,
        ulong localWater,
        string reference,
        string partitionStream,
        ILogger logger,
        Action<string> onFallback)
    {
        ArgumentNullException.ThrowIfNull(logger);
        ArgumentNullException.ThrowIfNull(onFallback);
        if (mode is ConsumeOffset.AlwaysFirst) return new OffsetTypeFirst();
        if (mode is ConsumeOffset.AlwaysNext) return new OffsetTypeNext();
        if (stored is null)
        {
            var branch = mode switch
            {
                ConsumeOffset.ResumeOrFirst => "ResumeOrFirst",
                ConsumeOffset.ResumeOrNext => "ResumeOrNext",
                ConsumeOffset.ResumeOrFail => "ResumeOrFail",
                _ => throw new StreamBusConfigurationException($"Unknown consume offset mode {mode}."),
            };
            if (mode == ConsumeOffset.ResumeOrFail)
                throw new StreamBusConfigurationException(
                    $"No stored offset for reference '{reference}' on partition '{partitionStream}' (mode ResumeOrFail).");
            var resolved = mode == ConsumeOffset.ResumeOrFirst ? (IOffsetType)new OffsetTypeFirst() : new OffsetTypeNext();
            logger.LogWarning(
                "No stored offset for reference '{Reference}' on partition '{Partition}' (branch {Branch}); resolved {Resolved}.",
                reference, partitionStream, branch, resolved.GetType().Name);
            onFallback(branch);
            return resolved;
        }
        if (localWater > stored.Value)
            logger.LogWarning(
                "Local high-water {Local} exceeds broker-stored {Stored} for reference '{Reference}' on partition '{Partition}'; broker wins, replaying from {Resume}.",
                localWater, stored.Value, reference, partitionStream, stored.Value + 1);
        return new OffsetTypeOffset(stored.Value + 1);
    }

    public static async Task<IOffsetType> ResolveWithQueryAsync(
        ConsumeOffset mode,
        Func<Task<ulong?>> queryStored,
        ulong localWater,
        string reference,
        string partitionStream,
        ILogger logger,
        Action<string> onFallback)
    {
        ulong? stored;
        try
        {
            stored = await queryStored().ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            logger.LogWarning(ex,
                "Offset query failed for reference '{Reference}' on partition '{Partition}'; degrading to the no-stored-offset branch.",
                reference, partitionStream);
            stored = null;
        }
        return ResolveOffset(mode, stored, localWater, reference, partitionStream, logger, onFallback);
    }
}

internal sealed class MonotonicOffsetGuard
{
    private readonly Dictionary<string, ulong> _water = new();
    private readonly object _gate = new();

    public ulong Water(string key)
    {
        lock (_gate) return _water.TryGetValue(key, out var w) ? w : 0;
    }

    public bool TryAdmit(string key, ulong candidate)
    {
        lock (_gate)
        {
            if (!_water.TryGetValue(key, out var current))
            {
                // First contact admits anything: offset 0 IS progress from nothing.
                // (A plain `candidate <= water` check against the 0 default would reject
                // offset 0 forever and miscount a rejection on every fresh group's first
                // message.)
                _water[key] = candidate;
                return true;
            }
            if (candidate <= current) return false;
            _water[key] = candidate;
            return true;
        }
    }

    public void Overwrite(string key, ulong value)
    {
        lock (_gate) _water[key] = value;
    }
}
```

Adaptations bound to the Task 1 probe (apply, report): the `OffsetTypeOffset(ulong)` constructor shape is confirmed — construct directly; if `IOffsetType` lives under a different namespace, qualify it. The `stored.Value + 1` overflow edge (`ulong.MaxValue`) is unreachable in practice (streams never reach 2^64 messages) — no guard.

- [ ] **Step 2: Widen `IStreamSystemHandle` with offset/broker reads**

```csharp
Task<ulong?> TryQueryOffsetAsync(string reference, string stream, CancellationToken ct);
Task StoreSystemOffsetAsync(string reference, string stream, ulong offset, CancellationToken ct);
Task<ulong> GetEndOffsetAsync(string stream, CancellationToken ct);
Task<bool> StreamExistsAsync(string stream, CancellationToken ct);
Task<bool> SuperStreamExistsAsync(string superStream, CancellationToken ct);
Task<string[]> QueryPartitionAsync(string superStream, CancellationToken ct);
```

`StreamSystemHandle` implements by delegating to `StreamSystem.TryQueryOffset`, `StreamSystem.StoreOffset`, `StreamSystem.StreamExists`, `StreamSystem.SuperStreamExists`, `StreamSystem.QueryPartition` directly, plus `GetEndOffsetAsync` mapping the Task 1-probed end-offset member of `StreamSystem.StreamStats(stream)`:

```csharp
public async Task<ulong> GetEndOffsetAsync(string stream, CancellationToken ct)
{
    ct.ThrowIfCancellationRequested();
    var stats = await _system.StreamStats(stream).ConfigureAwait(false);
#pragma warning disable CS0618 // 1.12.1 marks LastOffset obsolete in favor of CommittedChunkId (different semantic); probe-pinned as the only end-offset source.
    return stats.LastOffset();
#pragma warning restore CS0618
}
```

(`StreamStats` exposes no end-offset property — the Task 1 probe confirms the end offset is the parameterless `LastOffset()` method. `FirstOffset()`/`CommittedChunkId()` siblings exist but are not needed in this phase. Do NOT "fix" this by switching to `CommittedChunkId()`.)

- [ ] **Step 3: Update every existing test stub**

Grep `IStreamSystemHandle` under `tests/`, and in each implementing stub add the six members with `=> throw new NotImplementedException("<TestClass> never queries offsets.")` bodies. The brief knows three files (`ConnectionSupervisorTests.cs`, `HostingTests.cs`, `PublishTests.cs`) — if the grep finds more implementers, update them too and report.

- [ ] **Step 4: Verify**

```powershell
dotnet build src/StreamBus/StreamBus.csproj -c Release
```

Expected: zero warnings/errors. Resolution behavior is pinned by Task 5 tests.

- [ ] **Step 5: Commit (with human approval)**

```powershell
git add src/StreamBus/OffsetResolution.cs src/StreamBus/Connector.cs tests/StreamBus.UnitTests/ConnectionSupervisorTests.cs tests/StreamBus.UnitTests/HostingTests.cs tests/StreamBus.UnitTests/PublishTests.cs
git commit -m "feat: add offset resolution core and handle offset methods"
```

---

### Task 3: Partition consumer loop + metrics foundation

**Files:**
- Create: `src/StreamBus/StreamBusMetrics.cs`
- Create: `src/StreamBus/StreamPartitionConsumer.cs` (loop + wrapper + `OffsetStorer` + `PartitionState`)

**Interfaces:**
- Consumes: `RawConsumer` lifecycle (Task 1 probe shapes), `OffsetResolution` + guard (Task 2), `IStreamSystemHandle` (Task 2), `ConsumeOptions`/`ConsumeContext`/`MessageMetadata` (Task 1), `StreamBusDelays` (Task 1), `ILogger` patterns.
- Produces: `StreamPartitionConsumer` (attach loop + `PartitionState`), `OffsetStorer`, `StreamBusMetrics.Shared` for Task 4; `PartitionState` fields read by Task 4's lag poller.

- [ ] **Step 1: Implement `StreamBusMetrics.cs`**

```csharp
using System.Collections.Concurrent;
using System.Diagnostics.Metrics;

namespace StreamBus;

internal sealed class PartitionState
{
    public long Inflight;
    public long LastDelivered;
    public long LastStored;
    public long RetryBlocked;
    public long Lag;
}

internal sealed class StreamBusMetrics
{
    public static readonly StreamBusMetrics Shared = new();

    private readonly Meter _meter = new("StreamBus");
    private readonly ConcurrentDictionary<string, PartitionState> _partitions = new();
    private readonly ConcurrentDictionary<string, Func<BusConnectionState>> _connections = new();

    private StreamBusMetrics()
    {
        _ = new ObservableUpDownCounter<long>(_meter, "streambus_partition_lag",
            () => ObservePartitions(s => s.Lag));
        _ = new ObservableUpDownCounter<long>(_meter, "streambus_partition_inflight",
            () => ObservePartitions(s => s.Inflight));
        _ = new ObservableUpDownCounter<long>(_meter, "streambus_partition_retry_blocked",
            () => ObservePartitions(s => s.RetryBlocked),
            unit: null, description: null);
        _ = new ObservableUpDownCounter<int>(_meter, "streambus_connection_state",
            () => _connections.Select(kv => new Measurement<int>((int)kv.Value(),
                new TagList { { "vhost", VhostOf(kv.Key) }, { "bus", BusOf(kv.Key) } })));
        StoreRejected = _meter.CreateCounter<long>("streambus_offset_store_rejected_total");
        Regressed = _meter.CreateCounter<long>("streambus_offset_regressed_total");
        Fallback = _meter.CreateCounter<long>("streambus_offset_fallback_total");
    }

    public Counter<long> StoreRejected { get; }
    public Counter<long> Regressed { get; }
    public Counter<long> Fallback { get; }

    public IDisposable TrackPartition(string vhost, string bus, string stream, int partition, string reference, PartitionState state)
    {
        var key = $"{vhost}|{bus}|{stream}|{partition}|{reference}";
        _partitions[key] = state;
        return new Unregister(() => _partitions.TryRemove(key, out _));
    }

    public void TrackConnection(string vhost, string bus, Func<BusConnectionState> readState)
    {
        _connections[$"{vhost}|{bus}"] = readState;
    }

    public void RecordStoreRejected(string vhost, string bus, string stream, int partition, string reference)
        => StoreRejected.Add(1, tag(vhost, bus, stream, partition, reference));

    public void RecordRegressed(string vhost, string bus, string stream, int partition, string reference)
        => Regressed.Add(1, tag(vhost, bus, stream, partition, reference));

    public void RecordFallback(string vhost, string bus, string stream, int partition, string reference, string branch)
        => Fallback.Add(1, tag(vhost, bus, stream, partition, reference, ("branch", branch)));
    ...
}
```

Full implementation notes (write completely, no shortcuts): `ObservePartitions` parses the compound key back into the five base tags (`vhost`, `bus`, `stream`, `partition`, `reference`) plus `failure_mode="strict"` on the retry-blocked series only; `VhostOf`/`BusOf` split the connection key; `tag(...)` builds the `TagList`; `Unregister` is a private sealed `IDisposable` running the action once. The retry-blocked tags are base-five + `("failure_mode", "strict")` (Phase 5 default; Phase 6 drives real values). The class is process-lifetime by design (single `Shared` instance; meters are process-global anyway) — state dictionaries grow only with distinct (vhost, bus, stream, partition, reference) tuples and connection pairs, both bounded by registrations. No metric is ever removed except partition scopes on consumer disposal.

- [ ] **Step 2: Implement `StreamPartitionConsumer.cs`**

```csharp
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using RabbitMQ.Stream.Client;

namespace StreamBus;

internal sealed class OffsetStorer
{
    private readonly int _every;
    private readonly MonotonicOffsetGuard _guard;
    private readonly string _waterKey;
    private readonly Action<ulong> _onRejected;
    private int _sinceLastStore;
    private ulong _lastSuccess;
    private bool _hasSuccess;

    public OffsetStorer(int every, MonotonicOffsetGuard guard, string waterKey, Action<ulong> onRejected)
    {
        _every = every;
        _guard = guard;
        _waterKey = waterKey;
        _onRejected = onRejected;
    }

    public ulong LastSuccess => _lastSuccess;
    public bool HasSuccess => _hasSuccess;

    public bool ShouldStoreOnSuccess(ulong offset)
    {
        _lastSuccess = offset;
        _hasSuccess = true;
        _sinceLastStore++;
        return _sinceLastStore >= _every;
    }

    public void Stored(ulong offset, int keepRemainder)
    {
        _sinceLastStore = keepRemainder;
        _lastSuccess = offset;
    }

    public bool TryAdmit(ulong candidate)
    {
        if (_guard.TryAdmit(_waterKey, candidate)) return true;
        _onRejected(candidate);
        return false;
    }
}
```

Cadence contract: `ShouldStoreOnSuccess` returns true every `_every` successes; after a successful store call `Stored(offset, 0)`; when `MaxInterval` timer fires, the loop stores `LastSuccess` if `HasSuccess` (guard-checked, fire-and-forget with catch-log). `Stored(offset, keepRemainder)` resets the counter (pass 0 after cadence stores; the timer path also passes 0 — the next success restarts the count, which matches "store the last-success offset" without double-counting).

The partition loop (single class, sequential dispatch invariant documented in a comment):

```csharp
internal sealed class StreamPartitionConsumer : IAsyncDisposable
{
    // owns: attach loop (create → resolve → consume → failure/backoff/recreate),
    // promotion listener (resolve on every promote, never throws),
    // dispatch wrapper (context build, strictly-awaited handler, cadence stores),
    // regression read-back after the first store per attach, metrics state.
}
```

Write it completely per these bindings: constructor takes `(IStreamSystemHandle system, string stream, string partitionStream, int partitionIndex, string reference, ConsumeOptions options, Func<ConsumeContext, Task> handler, MonotonicOffsetGuard guard, PartitionState state, StreamBusMetrics metrics, string vhost, string bus, ILogger<StreamPartitionConsumer>? logger = null)`; `RunAsync(CancellationToken)` loops until cancelled/disposed: resolve via `ResolveWithQueryAsync` (query = `system.TryQueryOffsetAsync(reference, partitionStream, ct)`, fallback callback = metrics `RecordFallback` with the five tags); build `RawConsumerConfig` with the probed members (`new RawConsumerConfig(partitionStream)` ctor, `Reference`, `OffsetSpec` = resolved, `IsSingleActiveConsumer` from options, `ConsumerUpdateListener` = promote→`ResolveWithQueryAsync` (the listener ignores its `(reference, stream, promotedAsActive)` arguments except for logging, queries the broker fresh on every promotion, and carries a catch-all returning `OffsetTypeFirst` + warning + fallback count on any failure: it never throws and never returns null, because the client's fallback on listener failure is `OffsetTypeNext`, which would skip the backlog; the catch-all reports under the fixed `PromotionFailed` branch, distinct from the mode branches — a failed promotion query must never throw for exactly that reason, INCLUDING under `ResumeOrFail` (creation-time Fail still throws fail-fast; promotion-time Fail degrades to replay, documented deviation)) — leave `InitialCredits`, `ClientProvidedName`, handlers at client defaults); dispatch wrapper builds `ConsumeContext` (delivered offset from the probed `MessageContext.Offset`, Body copied once via `ToArray()`, RoutingValue from the `streambus.routing-key` application property as string else null, Metadata exactly as below, `ApplicationProperties` copied into a `Dictionary<string, object?>` defaulting to empty); `state.Inflight` inc/dec around the awaited handler; `state.LastDelivered` = context offset before invoking; on success `state.LastStored` advances only when a store is admitted; on handler exception: log error naming stream/partition/offset, decrement inflight, dispose the consumer via `await consumer.Close()`, `StreamBusDelays` backoff (attempt counter resets on each successful attach), continue the loop (redelivery from the stored offset); on `OperationCanceledException`: exit quietly; regression check: after the first admitted store of each attach, `TryQueryOffsetAsync` again and `RecordRegressed` + warning when the broker holds less than the guard water; `DisposeAsync` cancels the loop, closes the current consumer via `Close()` and disposes the MaxInterval timer; attempts use `Attempt` counting from 1 per consecutive failure streak. `DisposeAsync` awaits an in-flight handler to finish — a hung handler blocks disposal (same ceiling as hot keys; test (h)'s `finally { release.TrySetResult(); }` pattern exists for exactly this reason).

Metadata mapping (exact — Task 1 probe: `MessageId`/`CorrelationId` are `object`, no `Timestamp` member, closest is `CreationTime : DateTime` in UTC by AMQP definition):

```csharp
static MessageMetadata MapMetadata(RabbitMQ.Stream.Client.AMQP.Properties? properties, IDictionary<string, object>? applicationProperties)
{
    var app = applicationProperties is null
        ? new Dictionary<string, object?>()
        : applicationProperties.ToDictionary(kv => kv.Key, kv => (object?)kv.Value);
    app.TryGetValue("streambus.routing-key", out var routingObj);
    return new MessageMetadata
    {
        MessageId = properties?.MessageId?.ToString(),
        CorrelationId = properties?.CorrelationId?.ToString(),
        ApplicationProperties = app,
        Timestamp = properties?.CreationTime is { } ct
            ? new DateTimeOffset(DateTime.SpecifyKind(ct, DateTimeKind.Utc))
            : null,
    };
}
```

`RoutingValue` for the context is `app.TryGetValue("streambus.routing-key", out var r) ? r?.ToString() : null` (non-string values stringified, absent key yields null).

Adaptations bound to the Task 1 probe (apply, report): `RawConsumerConfig` member names, `MessageHandler` delegate shape (adapt the wrapper lambda; the strictly-awaited invariant stays), `RawConsumer.StoreOffset(ulong)` returning `Task` (await with catch-log-continue), consumer teardown via `await consumer.Close()` (`RawConsumer` has no async dispose; `Close()` returns `Task<ResponseCode>` which is ignored), `OffsetSpec` property name, `ApplicationProperties` value type (copy entries; non-string routing values stringified).

- [ ] **Step 3: Verify**

```powershell
dotnet build src/StreamBus/StreamBus.csproj -c Release
```

Expected: zero warnings/errors. Loop dynamics are pinned by Task 6 integration; pure pieces by Task 5.

- [ ] **Step 4: Commit (with human approval)**

```powershell
git add src/StreamBus/StreamBusMetrics.cs src/StreamBus/StreamPartitionConsumer.cs
git commit -m "feat: add partition consumer loop with offset tracking"
```

---

### Task 4: Consumer registry + bus surface + lag poller

**Files:**
- Create: `src/StreamBus/ConsumerRegistry.cs`
- Create: `src/StreamBus/PartitionLagPoller.cs`
- Modify: `src/StreamBus/Bus.cs` (ConsumeAsync + Get/Set on both interfaces + impls)
- Modify: `src/StreamBus/Registry.cs` (per-vhost consumer registries, `ConsumersFor`, `ActiveConsumers`, connection tracking hook)
- Modify: `src/StreamBus/Hosting.cs` (StopAsync disposes tracked consumers first)

**Interfaces:**
- Consumes: everything from Tasks 1–3; `StreamBusRegistry`/`StreamBusConfigStore`/`StreamBusHostedService` (Phase 3/4 shapes).
- Produces: `IStreamBus.ConsumeAsync` + `Get/SetConsumerOffsetAsync` (+generic), `StreamConsumerRegistry`, `PartitionLagPoller` for Tasks 5–6.

- [ ] **Step 1: Implement `PartitionLagPoller.cs`**

```csharp
internal sealed class PartitionLagPoller : IAsyncDisposable
{
    private readonly PartitionState _state;
    private readonly Func<Task<ulong>> _getEnd;
    private readonly Func<ulong> _getStored;
    private readonly PeriodicTimer _timer;

    public PartitionLagPoller(PartitionState state, Func<Task<ulong>> getEnd, Func<ulong> getStored, TimeSpan interval)
    {
        _state = state;
        _getEnd = getEnd;
        _getStored = getStored;
        _timer = new PeriodicTimer(interval);
    }

    public async Task RunAsync(CancellationToken ct)
    {
        try
        {
            while (await _timer.WaitForNextTickAsync(ct).ConfigureAwait(false))
            {
                try
                {
                    var end = await _getEnd().ConfigureAwait(false);
                    var stored = _getStored();
                    var lag = end > stored ? (long)(end - stored) : 0;
                    Interlocked.Exchange(ref _state.Lag, lag);
                }
                catch (OperationCanceledException) { return; }
                catch (Exception) { /* poller never faults the loop; next tick retries */ }
            }
        }
        catch (OperationCanceledException) { }
    }

    public ValueTask DisposeAsync()
    {
        _timer.Dispose();
        return ValueTask.CompletedTask;
    }
}
```

Production interval is `TimeSpan.FromSeconds(15)` (passed by the registration); tests inject ~100 ms.

- [ ] **Step 2: Implement `ConsumerRegistry.cs`**

```csharp
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using RabbitMQ.Stream.Client;

namespace StreamBus;

public interface IStreamConsumer : IAsyncDisposable
{
    string Stream { get; }
    string Reference { get; }
}

internal sealed class StreamConsumerRegistration : IStreamConsumer
{
    // owns partition loops + lag pollers + metrics scopes + deregister callback
}

internal sealed class StreamConsumerRegistry
{
    public StreamConsumerRegistry(StreamBusConnectionSupervisor supervisor, string vhost) { ... }

    public async Task<IStreamConsumer> ConsumeAsync(
        Type busType, string stream, ConsumeOptions options,
        Func<ConsumeContext, Task> handler, CancellationToken ct)
    {
        // 1. validate: stream, options, handler non-null; EveryMessages >= 1;
        //    MaxInterval null or > 0; reference (supplied or derived) 255-byte check.
        // 2. readiness: supervisor.WaitUntilReadyAsync(options.ReadinessPolicy, ct)
        //    -> Fatal: StreamBusFatalException; non-Ready: StreamBusNotReadyException.
        // 3. handle: supervisor.CurrentHandle ?? NotReady (defensive, same as publish).
        // 4. existence: plain -> handle.StreamExistsAsync else NotFound;
        //    super -> handle.SuperStreamExistsAsync else NotFound.
        // 5. partitions: plain -> [(stream, 0)]; super -> handle.QueryPartitionAsync
        //    indexed in returned order.
        // 6. reference: options.Reference ?? ReferenceDerivation.Derive(busType, stream).
        // 7. build one StreamPartitionConsumer per partition (shared guard, metrics
        //    TrackPartition scope each, lag poller each with handle.GetEndOffsetAsync
        //    + storer LastSuccess), start RunAsync tasks, start pollers,
        //    track registration, return it.
        // Creation-time resolve failures under ResumeOrFail surface
        // StreamBusConfigurationException from ConsumeAsync itself (fail fast).
    }

    public Task<ulong?> GetOffsetAsync(string stream, string reference, int partition, CancellationToken ct) { ... }
    public Task SetOffsetAsync(string stream, string reference, int partition, ulong offset, CancellationToken ct) { ... }
}
```

Write both classes completely: registration holds `List<(StreamPartitionConsumer Loop, Task RunTask)>`, `List<(PartitionLagPoller Poller, Task PollTask)>`, `List<IDisposable> _scopes`, a `CancellationTokenSource` for the family, an idempotent `DisposeAsync` (cancel, await runs, dispose loops/pollers/scopes, deregister), `Stream`/`Reference` properties; `GetOffsetAsync` resolves the partition stream — `StreamExistsAsync` → plain path (index must be 0 else `StreamBusConfigurationException`); else `SuperStreamExistsAsync` → super path (`QueryPartitionAsync`, index range-checked, else `StreamBusConfigurationException`); else neither exists → `StreamBusStreamNotFoundException` naming the stream (fail fast on config error — never let the client's `QueryPartition` exception propagate unmapped, never conflate missing with nothing-stored) — then `handle.TryQueryOffsetAsync` (fail-fast readiness first); `SetOffsetAsync` resolves likewise, calls `handle.StoreSystemOffsetAsync`, overwrites the guard water for `{reference}@{partitionStream}` (bypass by design), and writes an audit `LogInformation` naming reference/stream/partition/offset on the registry's owned logger (see logger rule below). The guard instance is per-registry (shared across that registry's registrations — keyed water makes sharing safe). Metric tracking: `TrackPartition(vhost, busType.FullName, stream, index, reference, state)` per partition; connection tracking is NOT here (Task 4 wires it in `Registry.cs`, next step).

Logger rule (FR-10.12/FR-10.13 observability — loggers must reach real providers, never die in `NullLogger` in production): `StreamBusRegistry` gains an optional third constructor parameter `ILoggerFactory? loggerFactory = null` (existing two-lambda constructions keep compiling); `Hosting.cs` `AddCore` passes `sp.GetService<ILoggerFactory>()` (nullable-tolerant — providers built without `AddLogging` yield null → `NullLoggerFactory.Instance`; add `using Microsoft.Extensions.Logging;` to `Hosting.cs`); `StreamConsumerRegistry` takes an optional `ILoggerFactory?` (stored; defaults to `NullLoggerFactory.Instance`), creates per-loop `ILogger<StreamPartitionConsumer>` from it, and owns the `ILogger<StreamConsumerRegistry>` used for the Set audit line. Test constructions pass nothing (NullLogger) or a capturing test factory.

- [ ] **Step 3: Wire `Registry.cs`, `Bus.cs`, `Hosting.cs`**

`Registry.cs`: add `ConcurrentDictionary<string, StreamConsumerRegistry> _consumers` + `internal StreamConsumerRegistry ConsumersFor(string vhost) => _consumers.GetOrAdd(vhost, _ => new StreamConsumerRegistry(SupervisorFor(vhost), vhost));` + `internal IReadOnlyCollection<IStreamConsumer> ActiveConsumers => snapshot across vhost registries` (each `StreamConsumerRegistry` exposes its live registrations; aggregate with `SelectMany`); in `GetOrCreateCore` after entry creation add `StreamBusMetrics.Shared.TrackConnection(vhost, busType.FullName!, () => entry.Supervisor.State);` (`FullName` of `DefaultBus`/markers is non-null in practice; use `?? busType.Name` fallback — no null-forgiving without cause).

`Bus.cs`: add to both interfaces

```csharp
Task<IStreamConsumer> ConsumeAsync(string stream, ConsumeOptions options, Func<ConsumeContext, Task> handler, CancellationToken ct = default);
Task<ulong?> GetConsumerOffsetAsync(string stream, string reference, int partition, CancellationToken ct = default);
Task SetConsumerOffsetAsync(string stream, string reference, int partition, ulong offset, CancellationToken ct = default);
```

Both implementations gain a `StreamConsumerRegistry` fourth constructor parameter (same pattern as the producer registry in Phase 4) and delegate to `ConsumersFor(vhost)`-provided registries: `ConsumeAsync` → `_consumers.ConsumeAsync(GetType().GetGenericArguments()...)` — no: the bus shell knows its marker statically. `StreamBus` (default) passes `typeof(DefaultBus)`; `StreamBus<TBus>` passes `typeof(TBus)`. Store the marker `Type` as a fifth... simpler: pass the marker type at construction from `Registry.cs` (it has `busType` in scope): constructors become `(string vhost, supervisor, producers, consumers, Type busType)`? Five parameters is unwieldy — instead the registry resolves the marker: `StreamBus` passes `typeof(DefaultBus)` inline in its own `ConsumeAsync` body, `StreamBus<TBus>` passes `typeof(TBus)` inline. No ctor change beyond the consumers registry (fourth parameter, same as Phase 4's producer pattern). `Get/SetConsumerOffsetAsync` delegate straight to the vhost registry (no bus type needed).

`Hosting.cs` `StopAsync`: insert consumer disposal before the existing supervisor block:

```csharp
foreach (var consumer in _registry.ActiveConsumers)
{
    try { await consumer.DisposeAsync().ConfigureAwait(false); }
    catch (Exception) { /* shutdown best-effort; supervisor disposal below still runs */ }
}
```

`_registry` is typed `IStreamBusRegistry` — add `internal IReadOnlyCollection<IStreamConsumer> ActiveConsumers { get; }` to that interface (same-assembly pattern as `GetOrCreateEntry`/`SupervisorFor`). Empty-swallow on shutdown is intentional (a failing consumer dispose must not block supervisor shutdown); no logging scope exists here — keep the empty catch with that comment.

- [ ] **Step 4: Verify**

```powershell
dotnet build src/StreamBus/StreamBus.csproj -c Release
dotnet test tests/StreamBus.UnitTests/StreamBus.UnitTests.csproj -c Release
```

Expected: build clean; existing suite green (bus ctor changes are internal to the registry; `StopAsync` addition is behavior-neutral when no consumers exist).

- [ ] **Step 5: Commit (with human approval)**

```powershell
git add src/StreamBus/ConsumerRegistry.cs src/StreamBus/PartitionLagPoller.cs src/StreamBus/Bus.cs src/StreamBus/Registry.cs src/StreamBus/Hosting.cs
git commit -m "feat: add consumer registry with bus surface and lag poller"
```

---

### Task 5: Unit tests (resolution, cadence, metrics, validation)

**Files:**
- Create: `tests/StreamBus.UnitTests/OffsetResolutionTests.cs`
- Create: `tests/StreamBus.UnitTests/OffsetCadenceTests.cs`
- Create: `tests/StreamBus.UnitTests/ConsumerMetricsTests.cs`

**Interfaces:**
- Consumes: everything from Tasks 1–4; `MeterListener` (BCL, no packages); test `ILogger` sink (implement `ILogger` collecting into a list, or use a simple stub).
- Produces: pins for Review Focus lines 3 (fallback half), guard half of line 2-adjacent monotonicity, cadence redelivery windows.

- [ ] **Step 1: Implement `OffsetResolutionTests.cs`**

```csharp
using Microsoft.Extensions.Logging;
using RabbitMQ.Stream.Client;

namespace StreamBus.UnitTests;

file sealed class ListLogger : ILogger
{
    public readonly List<string> Lines = new();
    public IDisposable? BeginScope<TState>(TState state) where TState : notnull => null;
    public bool IsEnabled(LogLevel logLevel) => true;
    public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception? exception, Func<TState, Exception?, string> formatter)
    {
        lock (Lines) Lines.Add($"{logLevel}: {formatter(state, exception)}");
    }
}
```

Wait — `ILogger` (non-generic) requires `BeginScope<TState>(TState state)` returning `IDisposable?` — with nullable annotations, `IDisposable?` return with `=> null` needs `#nullable` context (unit project has Nullable enable ✓). If the analyzer complains, return `NullScope.Instance` from Abstractions (`Microsoft.Extensions.Logging.Abstractions.NullScope` — public? `NullScope` is public in Abstractions ✓). Use `=> NullScope.Instance` to be safe.

Matrix tests (all call `OffsetResolution.ResolveOffset` directly with a branch recorder list):

```csharp
public sealed class OffsetResolutionTests
{
    private static (ListLogger Log, List<string> Branches) NewSink()
        => (new ListLogger(), new List<string>());

    [Fact]
    public void Always_modes_ignore_the_broker()
    {
        var (log, branches) = NewSink();
        Assert.IsType<OffsetTypeFirst>(OffsetResolution.ResolveOffset(ConsumeOffset.AlwaysFirst, 99, 50, "r", "p", log, branches.Add));
        Assert.IsType<OffsetTypeNext>(OffsetResolution.ResolveOffset(ConsumeOffset.AlwaysNext, 99, 50, "r", "p", log, branches.Add));
        Assert.Empty(branches);
    }

    [Theory]
    [InlineData(ConsumeOffset.ResumeOrFirst, null, "ResumeOrFirst", typeof(OffsetTypeFirst))]
    [InlineData(ConsumeOffset.ResumeOrNext, null, "ResumeOrNext", typeof(OffsetTypeNext))]
    public void No_stored_offset_falls_back(ConsumeOffset mode, ulong? _ignored, string branch, Type expected)
    {
        var (log, branches) = NewSink();
        var result = OffsetResolution.ResolveOffset(mode, null, 0, "ref-A", "part-0", log, branches.Add);
        Assert.IsType(expected, result);
        Assert.Equal(new[] { branch }, branches);
        Assert.Contains(log.Lines, l => l.Contains("ref-A") && l.Contains("part-0") && l.Contains(branch));
    }

    [Fact]
    public void ResumeOrFail_without_stored_throws_naming_reference_and_partition()
    {
        var (log, branches) = NewSink();
        var ex = Assert.Throws<StreamBusConfigurationException>(() =>
            OffsetResolution.ResolveOffset(ConsumeOffset.ResumeOrFail, null, 0, "ref-B", "part-2", log, branches.Add));
        Assert.Contains("ref-B", ex.Message);
        Assert.Contains("part-2", ex.Message);
    }

    [Fact]
    public void Stored_offset_resolves_to_next_and_broker_wins_over_local()
    {
        var (log, branches) = NewSink();
        var result = OffsetResolution.ResolveOffset(ConsumeOffset.ResumeOrFirst, 5, 9, "r", "p", log, branches.Add);
        Assert.Equal(new OffsetTypeOffset(6), result);
        Assert.Contains(log.Lines, l => l.Contains("9") && l.Contains("5"));
        Assert.Empty(branches);
    }

    [Fact]
    public async Task Query_failure_degrades_to_no_stored_branch()
    {
        var (log, branches) = NewSink();
        var result = await OffsetResolution.ResolveWithQueryAsync(
            ConsumeOffset.ResumeOrFirst, () => Task.FromException<ulong?>(new TimeoutException("blip")),
            0, "r", "p", log, branches.Add);
        Assert.IsType<OffsetTypeFirst>(result);
        Assert.Equal(new[] { "ResumeOrFirst" }, branches);
    }

    [Fact]
    public void Guard_rejects_non_monotonic_and_set_overwrites()
    {
        var guard = new MonotonicOffsetGuard();
        Assert.True(guard.TryAdmit("k", 4));
        Assert.False(guard.TryAdmit("k", 4));
        Assert.False(guard.TryAdmit("k", 2));
        Assert.True(guard.TryAdmit("k", 5));
        guard.Overwrite("k", 99);
        Assert.False(guard.TryAdmit("k", 50));
        Assert.True(guard.TryAdmit("k", 100));
    }

    [Fact]
    public void Derivation_is_stable_distinct_and_bounded()
    {
        var a1 = ReferenceDerivation.Derive(typeof(DefaultBus), "orders");
        var a2 = ReferenceDerivation.Derive(typeof(DefaultBus), "orders");
        Assert.Equal(a1, a2);
        Assert.StartsWith("streambus:", a1);
        Assert.NotEqual(a1, ReferenceDerivation.Derive(typeof(DefaultBus), "billing"));
        var longStream = new string('s', 300);
        var hashed = ReferenceDerivation.Derive(typeof(DefaultBus), longStream);
        Assert.True(System.Text.Encoding.UTF8.GetByteCount(hashed) <= 255);
        Assert.Throws<StreamBusConfigurationException>(() => ReferenceDerivation.Validate(new string('r', 300)));
        ReferenceDerivation.Validate(a1);
    }
}
```

Adaptations (apply, report): `OffsetTypeOffset` offset member name per Task 1 probe (used above as `.Offset`); `Theory` with `Type`/`ulong?` parameters — xUnit serializes `typeof(...)` fine, `null` for `ulong?` needs explicit `[InlineData(..., null, ...)]` (shown); if the analyzer flags the unused `_ignored` parameter, drop it and the argument. `DefaultBus` is public (Phase 3) ✓. `NullScope` fallback for the logger scope as noted.

- [ ] **Step 2: Implement `OffsetCadenceTests.cs`**

Test `OffsetStorer` + `MonotonicOffsetGuard` + redelivery-window semantics directly (all internal, IVT present):

```csharp
public sealed class OffsetCadenceTests
{
    [Fact]
    public async Task Every3_stores_at_3_and_redelivers_at_most_2()
    {
        // Simulate: successes 0..4 with Every=3 -> stores {2}; "crash" (no more stores);
        // resume replays from last stored + 1 = 3 -> redelivery set {3,4} (<= N-1 = 2).
        var guard = new MonotonicOffsetGuard();
        var rejected = new List<ulong>();
        var storer = new OffsetStorer(3, guard, "k", rejected.Add);
        var stored = new List<ulong>();
        for (ulong o = 0; o < 5; o++)
        {
            if (storer.ShouldStoreOnSuccess(o))
            {
                Assert.True(storer.TryAdmit(o));
                stored.Add(o);
                storer.Stored(o, 0);
            }
        }
        Assert.Equal(new[] { 2UL }, stored);
        Assert.Equal(3UL, stored[^1] + 1); // resume point
        Assert.Empty(rejected);
        await Task.CompletedTask;
    }

    [Fact]
    public async Task MaxInterval_timer_stores_last_success()
    {
        var guard = new MonotonicOffsetGuard();
        var storer = new OffsetStorer(1000, guard, "k", _ => { });
        var stored = new List<ulong>();
        using var timer = new PeriodicTimer(TimeSpan.FromMilliseconds(50));
        var cts = new CancellationTokenSource(TimeSpan.FromSeconds(5));
        // one success, no cadence store; timer path stores LastSuccess:
        Assert.False(storer.ShouldStoreOnSuccess(0)); // 1 < 1000
        // emulate the loop's timer tick body:
        while (await timer.WaitForNextTickAsync(cts.Token))
        {
            if (storer.HasSuccess && storer.TryAdmit(storer.LastSuccess))
            {
                stored.Add(storer.LastSuccess);
                storer.Stored(storer.LastSuccess, 0);
                break;
            }
        }
        Assert.Equal(new[] { 0UL }, stored);
    }
}
```

Wait — `ShouldStoreOnSuccess` sets `_hasSuccess = true` but never records WHICH offset succeeded. The storer doesn't know the offset on success calls! Look at my Task 3 `OffsetStorer`: `ShouldStoreOnSuccess()` takes no offset — but `LastSuccess` must be set from somewhere. BUG: the loop must tell the storer each success offset. Fix the Task 3 design: `ShouldStoreOnSuccess(ulong offset)` records `_lastSuccess = offset` every call and returns whether cadence hit. Update Task 3's class: `public bool ShouldStoreOnSuccess(ulong offset) { _lastSuccess = offset; _hasSuccess = true; _sinceLastStore++; return _sinceLastStore >= _every; }`. And the tests above call `ShouldStoreOnSuccess(o)` / `ShouldStoreOnSuccess(0)`. I must patch Task 3's OffsetStorer block accordingly (do it now as part of self-review).

Also `PeriodicTimer` + `CancellationTokenSource(TimeSpan)` undisposed — analyzer: `using var timer` ✓ shown; `new CancellationTokenSource(...)` not disposed — Phase 4 precedent (undisposed CTS inline passed builds) + adaptation paragraph will cover. Add the analyzer sentence to Task 5's adaptation notes... Task 5 has no adaptation paragraph yet! Add one: analyzer standard forms (TestContext ct, await using, NullScope, Theory unused params).

Also `stored[^1]` index-from-end ✓ C# 8+.

- [ ] **Step 3: Implement `ConsumerMetricsTests.cs`**

```csharp
using System.Diagnostics.Metrics;

namespace StreamBus.UnitTests;

public sealed class ConsumerMetricsTests : IDisposable
{
    private readonly MeterListener _listener = new();
    private readonly List<(string Instrument, double Value, KeyValuePair<string, object?>[] Tags)> _measurements = new();

    public ConsumerMetricsTests()
    {
        _listener.InstrumentPublished = (instrument, listener) =>
        {
            if (instrument.Meter.Name == "StreamBus")
                listener.EnableMeasurementEvents(instrument);
        };
        _listener.SetMeasurementEventCallback<double>(Record);
        _listener.SetMeasurementEventCallback<long>(Record);
        _listener.SetMeasurementEventCallback<int>(Record);
        _listener.Start();
    }

    private void Record<T>(Instrument instrument, T measurement, ReadOnlySpan<KeyValuePair<string, object?>> tags, object? state)
    {
        lock (_measurements) _measurements.Add((instrument.Name, Convert.ToDouble(measurement), tags.ToArray()));
    }

    public void Dispose() => _listener.Dispose();

    [Fact]
    public void Fallback_and_rejected_counters_emit_with_tags()
    {
        StreamBusMetrics.Shared.RecordFallback("vh", "bus", "stream", 0, "ref", "ResumeOrFirst");
        StreamBusMetrics.Shared.RecordStoreRejected("vh", "bus", "stream", 0, "ref");
        Assert.Contains(_measurements, m => m.Instrument == "streambus_offset_fallback_total"
            && m.Tags.Any(t => t.Key == "branch" && (string?)t.Value == "ResumeOrFirst"));
        Assert.Contains(_measurements, m => m.Instrument == "streambus_offset_store_rejected_total");
    }

    [Fact]
    public async Task Lag_poller_computes_end_minus_stored()
    {
        var state = new PartitionState();
        var poller = new PartitionLagPoller(state, () => Task.FromResult(10UL), () => 4UL, TimeSpan.FromMilliseconds(50));
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(5));
        var run = poller.RunAsync(cts.Token);
        var deadline = DateTime.UtcNow + TimeSpan.FromSeconds(5);
        while (state.Lag != 6 && DateTime.UtcNow < deadline)
            await Task.Delay(20);
        await cts.CancelAsync();
        try { await run; } catch (OperationCanceledException) { }
        await poller.DisposeAsync();
        Assert.Equal(6, state.Lag);
    }

    [Fact]
    public void Partition_tracking_scope_registers_and_unregisters()
    {
        var state = new PartitionState { Inflight = 3 };
        using (StreamBusMetrics.Shared.TrackPartition("vh", "b", "s", 1, "r", state))
        {
            _listener.RecordObservableInstruments();
            Assert.Contains(_measurements, m => m.Instrument == "streambus_partition_inflight" && m.Value == 3);
        }
        // gauge observations are pull-based; scope disposal only stops future observations.
    }
}
```

Notes: `SetMeasurementEventCallback<T>` overloads for double/long/int cover UpDownCounter<long>, Counter<long>, ObservableUpDownCounter<int>. `Convert.ToDouble(measurement)` generic — constrain? `Record<T>` with `Convert.ToDouble` works for numeric Ts. `RecordObservableInstruments()` triggers observable callbacks synchronously ✓. The tracking-scope test asserts inflight==3 observed — proves wiring end to end. Counter tests rely on synchronous `Add` delivery to enabled listeners ✓ (listeners enabled at construction; instruments pre-exist on the shared meter — `InstrumentPublished` fires for already-published instruments on Start? MeterListener delivers already-published instruments on Start ✓ standard behavior).

Adaptation paragraph for Task 5 (add): analyzer standard forms; `NullScope.Instance` if the null scope return is flagged; Theory unused-param removal; undisposed CTS/timer forms per precedent.

- [ ] **Step 2b: Append `ConsumeValidationTests` to `OffsetResolutionTests.cs`**

```csharp
public sealed class ConsumeValidationTests
{
    private static StreamConsumerRegistry UnstartedRegistry()
    {
        var options = new StreamBusOptions { Vhost = "/", UserName = "app", Password = "secret" };
        var supervisor = new StreamBusConnectionSupervisor(options,
            new NeverReadyConnector());
        return new StreamConsumerRegistry(supervisor, "/");
    }

    private sealed class NeverReadyConnector : IStreamSystemConnector
    {
        public Task<IStreamSystemHandle> ConnectAsync(StreamBusOptions options, CancellationToken ct)
            => new TaskCompletionSource<IStreamSystemHandle>().Task;
    }

    private static Task<IStreamConsumer> Consume(StreamConsumerRegistry registry, string stream, ConsumeOptions options)
        => registry.ConsumeAsync(typeof(DefaultBus), stream, options, _ => Task.CompletedTask, TestContext.Current.CancellationToken);

    [Fact]
    public async Task Over_long_reference_throws_naming_limit()
    {
        var registry = UnstartedRegistry();
        var ex = await Assert.ThrowsAsync<StreamBusConfigurationException>(() =>
            Consume(registry, "s", new ConsumeOptions { Reference = new string('r', 300) }));
        Assert.Contains("255", ex.Message);
    }

    [Theory]
    [InlineData(0)]
    [InlineData(-1)]
    public async Task Non_positive_every_messages_throws(int every)
    {
        var registry = UnstartedRegistry();
        await Assert.ThrowsAsync<StreamBusConfigurationException>(() =>
            Consume(registry, "s", new ConsumeOptions { OffsetStore = new OffsetStoreOptions { EveryMessages = every } }));
    }

    [Theory]
    [InlineData(0)]
    [InlineData(-1)]
    public async Task Non_positive_max_interval_throws(int seconds)
    {
        var registry = UnstartedRegistry();
        await Assert.ThrowsAsync<StreamBusConfigurationException>(() =>
            Consume(registry, "s", new ConsumeOptions { OffsetStore = new OffsetStoreOptions { MaxInterval = TimeSpan.FromSeconds(seconds) } }));
    }

    [Theory]
    [InlineData(null)]
    [InlineData("")]
    [InlineData("   ")]
    public async Task Null_or_empty_stream_throws(string? stream)
    {
        var registry = UnstartedRegistry();
        await Assert.ThrowsAsync<ArgumentException>(() =>
            registry.ConsumeAsync(typeof(DefaultBus), stream!, new ConsumeOptions(), _ => Task.CompletedTask, TestContext.Current.CancellationToken));
    }

    [Fact]
    public async Task Null_handler_throws()
    {
        var registry = UnstartedRegistry();
        await Assert.ThrowsAsync<ArgumentNullException>(() =>
            registry.ConsumeAsync(typeof(DefaultBus), "s", new ConsumeOptions(), null!, TestContext.Current.CancellationToken));
    }

    private sealed class ListLoggerFactory : ILoggerFactory
    {
        public readonly ListLogger Logger = new();
        public void AddProvider(ILoggerProvider provider) { }
        public ILogger CreateLogger(string categoryName) => Logger;
        public void Dispose() { }
    }

    private static StreamConsumerRegistry RegistryWithLogging(ListLoggerFactory factory, IStreamSystemHandle handle)
    {
        var options = new StreamBusOptions { Vhost = "/", UserName = "app", Password = "secret" };
        var supervisor = new StreamBusConnectionSupervisor(options, new ReadyConnector(handle));
        supervisor.StartAsync(CancellationToken.None).GetAwaiter().GetResult();
        return new StreamConsumerRegistry(supervisor, "/", factory);
    }

    private sealed class StubSystemHandle : IStreamSystemHandle
    {
        public bool Exists = true;
        public bool SuperExists;
        public bool IsClosed => false;
        public Task<IStreamProducerHandle> CreatePlainProducerAsync(string stream, CancellationToken ct) => throw new NotImplementedException();
        public Task<IStreamProducerHandle> CreateSuperStreamProducerAsync(string stream, CancellationToken ct) => throw new NotImplementedException();
        public Task<ulong?> TryQueryOffsetAsync(string reference, string stream, CancellationToken ct) => Task.FromResult<ulong?>(null);
        public Task StoreSystemOffsetAsync(string reference, string stream, ulong offset, CancellationToken ct) => Task.CompletedTask;
        public Task<ulong> GetEndOffsetAsync(string stream, CancellationToken ct) => Task.FromResult(0UL);
        public Task<bool> StreamExistsAsync(string stream, CancellationToken ct) => Task.FromResult(Exists);
        public Task<bool> SuperStreamExistsAsync(string superStream, CancellationToken ct) => Task.FromResult(SuperExists);
        public Task<string[]> QueryPartitionAsync(string superStream, CancellationToken ct) => Task.FromResult(new[] { superStream + "-0" });
        public Task<RawConsumer> CreateRawConsumerAsync(RawConsumerConfig config, CancellationToken ct) => throw new NotImplementedException();
        public ValueTask DisposeAsync() => ValueTask.CompletedTask;
    }

    private sealed class ReadyConnector : IStreamSystemConnector
    {
        private readonly StubSystemHandle _handle;
        public ReadyConnector(StubSystemHandle handle) => _handle = handle;
        public Task<IStreamSystemHandle> ConnectAsync(StreamBusOptions options, CancellationToken ct)
            => Task.FromResult<IStreamSystemHandle>(_handle);
    }

    [Fact]
    public async Task Bus_level_set_audit_reaches_a_real_logger()
    {
        var factory = new ListLoggerFactory();
        var registry = RegistryWithLogging(factory, new StubSystemHandle());
        await registry.SetOffsetAsync("s", "grp", 0, 7, TestContext.Current.CancellationToken);
        Assert.Contains(factory.Logger.Lines, l => l.Contains("grp") && l.Contains("7"));
    }

    [Fact]
    public async Task Missing_stream_throws_not_found_on_all_entry_points()
    {
        var factory = new ListLoggerFactory();
        var handle = new StubSystemHandle { Exists = false, SuperExists = false };
        var registry = RegistryWithLogging(factory, handle);
        await Assert.ThrowsAsync<StreamBusStreamNotFoundException>(() =>
            registry.ConsumeAsync(typeof(DefaultBus), "missing", new ConsumeOptions(), _ => Task.CompletedTask, TestContext.Current.CancellationToken));
        await Assert.ThrowsAsync<StreamBusStreamNotFoundException>(() =>
            registry.GetOffsetAsync("missing", "grp", 0, TestContext.Current.CancellationToken));
        await Assert.ThrowsAsync<StreamBusStreamNotFoundException>(() =>
            registry.SetOffsetAsync("missing", "grp", 0, 7, TestContext.Current.CancellationToken));
    }
}
```

`TimeSpan.FromSeconds(seconds)` with 0/-1 gives Zero/negative — both rejected. `null` stream case: `stream!` suppresses the nullable warning while still passing null at runtime. All validation precedes the readiness wait, so the never-ready connector is never touched (no broker contact, deterministic).

- [ ] **Step 4: Verify**

```powershell
dotnet test tests/StreamBus.UnitTests/StreamBus.UnitTests.csproj -c Release
```

Expected: all green (43 existing + new matrix/cadence/metrics tests), pristine output.

- [ ] **Step 5: Commit (with human approval)**

```powershell
git add tests/StreamBus.UnitTests/OffsetResolutionTests.cs tests/StreamBus.UnitTests/OffsetCadenceTests.cs tests/StreamBus.UnitTests/ConsumerMetricsTests.cs
git commit -m "test: add consumer offset, cadence, and metrics unit tests"
```

---

### Task 6: Integration tests + gates + full Phase 5 gate

**Files:**
- Create: `tests/StreamBus.IntegrationTests/ConsumerIntegrationTests.cs`

**Interfaces:**
- Consumes: bus `ConsumeAsync`/`Get/SetConsumerOffsetAsync`, `ConsumeOptions`, real broker (app/password, unique streams, test-only admin `StreamSystem` + `PartitionsSuperStreamSpec` + `MessageHandler` + `Data.Contents` per Phase 4 as-built).
- Produces: G-4 + G-5a gate evidence; the Phase 5 completion gate.

- [ ] **Step 0: Client-logger plumbing + SAC-off-on-Next + store tracing (src fixes enabling this task)**

Rationale: the client's internal shutdowns (e.g. `ProcessChunks` ending on credit errors) are currently invisible — every client creation passes no logger — and SAC+`Next` subscriptions demonstrably receive nothing on this broker (D2/D4 evidence), so tail-attach consumers must not join SAC groups.

(a) `src/StreamBus/Connector.cs`: add `using Microsoft.Extensions.Logging;`. `StreamSystemConnector` gains `public StreamSystemConnector(ILoggerFactory? loggerFactory = null)` storing `(loggerFactory ?? NullLoggerFactory.Instance)` in a field, passed into `new StreamSystemHandle(system, factory)`. `StreamSystemHandle` gains the matching optional second ctor parameter (existing single-arg constructions keep compiling) and passes real loggers: `StreamSystem.Create(config, factory.CreateLogger<StreamSystem>())`, `CreateRawProducer(..., factory.CreateLogger<RawProducer>())` (same for the super-stream producer call), `CreateRawConsumer(config, factory.CreateLogger<RawConsumer>())`. When no factory is registered the behavior is byte-identical to today (NullLogger everywhere).
(b) `src/StreamBus/Hosting.cs`: replace `services.TryAddSingleton<IStreamSystemConnector, StreamSystemConnector>();` with `services.TryAddSingleton<IStreamSystemConnector>(sp => new StreamSystemConnector(sp.GetService<ILoggerFactory>()));` (nullable-tolerant `GetService`, NOT required — an activator-resolved optional parameter would throw when no logging is registered). Add `using Microsoft.Extensions.Logging;` if missing.
(c) `src/StreamBus/StreamPartitionConsumer.cs` attach path: after resolving, compute `var singleActive = _options.SingleActiveConsumer && resolved is not OffsetTypeNext;` — when the resolved spec is `Next` (AlwaysNext, or fresh ResumeOrNext), the consumer attaches non-exclusively and logs: `_logger.LogInformation("SAC disabled for stream {Stream} partition {Partition}: Next-resolved consumers cannot join an SAC group (tail attach receives nothing under SAC on this broker).", ...)`. Use `singleActive` for `IsSingleActiveConsumer`. (Promotion-time Next on an SAC-created consumer remains a documented residual corner.)
(d) `src/StreamBus/StreamPartitionConsumer.cs` `StoreAndTrackAsync`: add `_logger.LogDebug("Storing offset {Offset} ...")` immediately before the `StoreOffset` call and `LogDebug("Stored offset ...")` after success (Debug level: zero cost when disabled, decisive when diagnosing stalls).

- [ ] **Step 1: Implement `ConsumerIntegrationTests.cs`**

Shared harness (write once, all tests use it):

```csharp
using System.Diagnostics.Metrics;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using RabbitMQ.Stream.Client;

namespace StreamBus.IntegrationTests;

public sealed class ConsumerIntegrationTests : IAsyncDisposable
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
        public ServiceProvider Provider { get; }
        public StreamBusHostedService Hosted { get; }
        public IStreamBus Bus { get; }
        public string Name { get; private set; } = "";
        public static async Task<BusInstance> StartAsync(string name, string vhost = "/")
        {
            var services = new ServiceCollection();
            services.AddLogging();
            services.AddStreamBus(vhost, o => o.Credentials(User(), Password()));
            var sp = services.BuildServiceProvider();
            var hosted = sp.GetServices<IHostedService>().OfType<StreamBusHostedService>().Single();
            await hosted.StartAsync(TestContext.Current.CancellationToken);
            var bus = sp.GetRequiredService<IStreamBus>();
            Assert.Equal(BusReadiness.Ready, await bus.WaitUntilReadyAsync(ReadinessPolicy.UpTo(TimeSpan.FromSeconds(30)), TestContext.Current.CancellationToken));
            return new BusInstance { Provider = sp, Hosted = hosted, Bus = bus, Name = name };
        }
        public async ValueTask DisposeAsync()
        {
            try { await Hosted.StopAsync(TestContext.Current.CancellationToken); }
            finally { await Provider.DisposeAsync(); }
        }
    }

    private static ReadOnlyMemory<byte> Body(string s) => System.Text.Encoding.UTF8.GetBytes(s);

    private static (string Key, int Seq) Parse(ReadOnlyMemory<byte> body)
    {
        var parts = System.Text.Encoding.UTF8.GetString(body.Span).Split(':');
        return (parts[0], int.Parse(parts[1]));
    }

    private static int Count<T>(List<T> list) { lock (list) return list.Count; }

    private static async Task WaitForAsync(Func<bool> done, TimeSpan timeout, string what)
    {
        var deadline = DateTime.UtcNow + timeout;
        while (!done())
        {
            if (DateTime.UtcNow >= deadline) Assert.Fail($"Timed out waiting for {what}.");
            await Task.Delay(100, TestContext.Current.CancellationToken);
        }
    }

    private static Func<ConsumeContext, Task> Rec(List<int> into) => ctx =>
    {
        var (_, seq) = Parse(ctx.Body);
        lock (into) into.Add(seq);
        return Task.CompletedTask;
    };
}
```

Payload convention: publish bodies `"{key}:{seq}"` UTF-8 via `IStreamBus.PublishAsync` (plain for plain tests; super with RoutingKey=key for super tests). Handlers parse with `Parse` and record under lock; record tuples always include `ctx.Partition` so per-partition end-state assertions are possible.

Test (a) coverage + exactly-once + order, 4 partitions × 3 instances:

```csharp
[Fact]
public async Task Super_stream_splits_across_instances_exactly_once_ordered()
{
    var super = Unique("con-super");
    var admin = await ConnectAdminAsync();
    await using var _ = admin;
    await admin.CreateSuperStream(new PartitionsSuperStreamSpec(super, 4));
    _superStreams.Add(super);
    var keys = new[] { "k0", "k1", "k2", "k3", "k4", "k5" };
    // backlog first (no settle flake): publish 4 seq per key via a throwaway bus
    await using var pub = await BusInstance.StartAsync("pub");
    try
    {
        foreach (var seq in Enumerable.Range(1, 4))
            foreach (var key in keys)
                await pub.Bus.PublishAsync(super, Body($"{key}:{seq}"),
                    new PublishOptions { IsSuperStream = true, RoutingKey = key }, TestContext.Current.CancellationToken);
    }
    finally { await pub.DisposeAsync(); }
    // three same-reference instances consume the 24-message backlog
    var instances = new List<BusInstance>();
    var all = new List<(string Key, int Seq, string Instance, int Partition)>();
    try
    {
        foreach (var name in new[] { "i0", "i1", "i2" })
        {
            var inst = await BusInstance.StartAsync(name);
            instances.Add(inst);
            await inst.Bus.ConsumeAsync(super,
                new ConsumeOptions { IsSuperStream = true, Reference = "grp-a" },
                ctx =>
                {
                    var (key, seq) = Parse(ctx.Body);
                    lock (all) all.Add((key, seq, name, ctx.Partition));
                    return Task.CompletedTask;
                }, TestContext.Current.CancellationToken);
        }
        await WaitForAsync(() => Count(all) == 24, TimeSpan.FromSeconds(60), "backlog drain");
    }
    finally { foreach (var inst in instances) await inst.DisposeAsync(); }
    Assert.Equal(24, all.Count);
    Assert.Equal(24, all.Select(x => (x.Key, x.Seq)).Distinct().Count()); // exactly once
    foreach (var key in keys)
        Assert.Equal(new[] { 1, 2, 3, 4 }, all.Where(x => x.Key == key).Select(x => x.Seq));
    // Partition enumeration sanity (the registry depends on stable QueryPartition
    // order). No LastOffset() proof: ExpE proved it returns 0 with messages present,
    // so it pins nothing.
    var partitions = await admin.QueryPartition(super);
    Assert.Equal(4, partitions.Length);
}
```

Helpers (write once in the file): `Body(string)`, `Parse(ReadOnlyMemory<byte>)`, `Count(list)` locked, `WaitForAsync(Func<bool>, TimeSpan, string)` polling with `Assert.True` + message on timeout. Per-key order assertion as shown relies on arrival order == processing order per partition — the loop dispatches sequentially, and recording happens inside the handler, so list order per key is meaningful. Cross-instance same-key split cannot happen (one partition → one active consumer) — the assertion would catch it.

Test (b) G-4 failover (2 instances, same ref, 2-partition super; disposes the first mid-stream):

```csharp
[Fact]
public async Task Failover_preserves_offsets_and_order()
{
    var super = Unique("con-fail");
    var admin = await ConnectAdminAsync();
    await using var _ = admin;
    await admin.CreateSuperStream(new PartitionsSuperStreamSpec(super, 2));
    _superStreams.Add(super);
    var keys = new[] { "a", "b" };
    await using var pub = await BusInstance.StartAsync("pub");
    try
    {
        foreach (var seq in Enumerable.Range(1, 6))
            foreach (var key in keys)
                await pub.Bus.PublishAsync(super, Body($"{key}:{seq}"),
                    new PublishOptions { IsSuperStream = true, RoutingKey = key }, TestContext.Current.CancellationToken);
    }
    finally { await pub.DisposeAsync(); }
    var all = new List<(string Key, int Seq, string Instance, int Partition, ulong Offset)>();
    var a = await BusInstance.StartAsync("fa");
    var b = await BusInstance.StartAsync("fb");
    var aDisposed = false;
    try
    {
        foreach (var (inst, name) in new[] { (a, "fa"), (b, "fb") })
            await inst.Bus.ConsumeAsync(super,
                new ConsumeOptions { IsSuperStream = true, Reference = "grp-b" },
                ctx =>
                {
                    var (key, seq) = Parse(ctx.Body);
                    lock (all) all.Add((key, seq, name, ctx.Partition, ctx.Offset));
                    return Task.CompletedTask;
                }, TestContext.Current.CancellationToken);
        await WaitForAsync(() => all.Any(x => x.Instance == "fa") && Count(all) >= 2, TimeSpan.FromSeconds(60), "A to own a partition");
        await a.DisposeAsync();
        aDisposed = true;
        await using var pub2 = await BusInstance.StartAsync("pub2");
        try
        {
            foreach (var seq in Enumerable.Range(7, 6))
                foreach (var key in keys)
                    await pub2.Bus.PublishAsync(super, Body($"{key}:{seq}"),
                        new PublishOptions { IsSuperStream = true, RoutingKey = key }, TestContext.Current.CancellationToken);
        }
        finally { await pub2.DisposeAsync(); }
        await WaitForAsync(() => Count(all) == 24, TimeSpan.FromSeconds(90), "post-failover drain");
        Assert.Equal(24, all.Count);
        Assert.Equal(24, all.Select(x => (x.Key, x.Seq)).Distinct().Count());
        foreach (var key in keys)
            Assert.Equal(Enumerable.Range(1, 12), all.Where(x => x.Key == key).Select(x => x.Seq));
        foreach (var p in new[] { 0, 1 })
        {
            var received = all.Where(x => x.Partition == p).ToList();
            var stored = await b.Bus.GetConsumerOffsetAsync(super, "grp-b", p, TestContext.Current.CancellationToken);
            if (received.Count == 0) Assert.Null(stored);
            // Stored must equal the furthest processed offset: never beyond (safety),
            // fully advanced (liveness). Count-based equality is wrong under non-contiguous
            // broker offsets — max-delivered is the mechanism-agnostic pin.
            else Assert.Equal(received.Max(x => x.Offset), stored);
        }
    }
    finally
    {
        if (!aDisposed) await a.DisposeAsync();
        await b.DisposeAsync();
    }
}
```

(`a` registers first, so it initially owns partitions and the `fa >= 1` wait proves a real promotion happens on dispose. Both keys may hash onto one partition — the `n == 0` branch keeps the end-state assertion correct either way. Fresh partitions number from 0 contiguously with all sends confirmed, so stored `== count - 1` per non-empty partition.)

Test (c) broadcast (FR-2.4):

```csharp
[Fact]
public async Task Distinct_references_each_receive_everything()
{
    var stream = Unique("con-bcast");
    var admin = await ConnectAdminAsync();
    await using var _ = admin;
    await admin.CreateStream(new StreamSpec(stream));
    _streams.Add(stream);
    await using var pub = await BusInstance.StartAsync("pub");
    try
    {
        foreach (var seq in Enumerable.Range(0, 6))
            await pub.Bus.PublishAsync(stream, Body($"k:{seq}"), new PublishOptions(), TestContext.Current.CancellationToken);
    }
    finally { await pub.DisposeAsync(); }
    var g1 = new List<(int Seq, int Partition)>();
    var g2 = new List<(int Seq, int Partition)>();
    await using var i1 = await BusInstance.StartAsync("i1");
    await using var i2 = await BusInstance.StartAsync("i2");
    await i1.Bus.ConsumeAsync(stream, new ConsumeOptions { Reference = "grp-c1" }, ctx =>
    {
        var (_, seq) = Parse(ctx.Body);
        lock (g1) g1.Add((seq, ctx.Partition));
        return Task.CompletedTask;
    }, TestContext.Current.CancellationToken);
    await i2.Bus.ConsumeAsync(stream, new ConsumeOptions { Reference = "grp-c2" }, ctx =>
    {
        var (_, seq) = Parse(ctx.Body);
        lock (g2) g2.Add((seq, ctx.Partition));
        return Task.CompletedTask;
    }, TestContext.Current.CancellationToken);
    await WaitForAsync(() => Count(g1) == 6 && Count(g2) == 6, TimeSpan.FromSeconds(60), "broadcast drain");
    Assert.Equal(Enumerable.Range(0, 6), g1.Select(x => x.Seq));
    Assert.Equal(Enumerable.Range(0, 6), g2.Select(x => x.Seq));
}
```

Test (d) G-5a kill-resume (fresh registry on recreate, so only a broker resume passes):

```csharp
[Fact]
public async Task Recreate_resumes_from_broker_stored_offset()
{
    var stream = Unique("con-resume");
    var admin = await ConnectAdminAsync();
    await using var _ = admin;
    await admin.CreateStream(new StreamSpec(stream));
    _streams.Add(stream);
    // Two backlog phases (deterministic: a plain-stream backlog drains in <200ms, so an
    // intermediate Count==5 wait overshoots and flakes — never wait mid-drain).
    await using var pub = await BusInstance.StartAsync("pub");
    try
    {
        foreach (var seq in Enumerable.Range(0, 5))
            await pub.Bus.PublishAsync(stream, Body($"k:{seq}"), new PublishOptions(), TestContext.Current.CancellationToken);
    }
    finally { await pub.DisposeAsync(); }
    var first = new List<int>();
    await using (var inst = await BusInstance.StartAsync("i"))
    {
        var c1 = await inst.Bus.ConsumeAsync(stream, new ConsumeOptions { Reference = "grp-d" }, Rec(first), TestContext.Current.CancellationToken);
        await WaitForAsync(() => Count(first) == 5, TimeSpan.FromSeconds(60), "first drain");
        await c1.DisposeAsync();
    }
    await using var pub2 = await BusInstance.StartAsync("pub2");
    try
    {
        foreach (var seq in Enumerable.Range(5, 5))
            await pub2.Bus.PublishAsync(stream, Body($"k:{seq}"), new PublishOptions(), TestContext.Current.CancellationToken);
    }
    finally { await pub2.DisposeAsync(); }
    // Fresh instance => fresh guard water: only a broker resume passes this test.
    var second = new List<int>();
    await using var inst2 = await BusInstance.StartAsync("i2");
    var c2 = await inst2.Bus.ConsumeAsync(stream, new ConsumeOptions { Reference = "grp-d" }, Rec(second), TestContext.Current.CancellationToken);
    await WaitForAsync(() => Count(second) == 5, TimeSpan.FromSeconds(60), "second drain");
    await c2.DisposeAsync();
    Assert.Equal(new[] { 0, 1, 2, 3, 4 }, first);
    Assert.Equal(new[] { 5, 6, 7, 8, 9 }, second);
    Assert.Equal(10, first.Concat(second).Distinct().Count());
}
```

(The second instance builds a brand-new registry with fresh guard water, so a localWater-based resume would redeliver 0..4 and fail the assertions — discriminating by construction.)

Test (e) offset branches (three sub-scenarios, three streams, one fact):

```csharp
[Fact]
public async Task Offset_branches_behave()
{
    var admin = await ConnectAdminAsync();
    await using var _ = admin;
    await using var inst = await BusInstance.StartAsync("i");
    // e1: resume, not replay (promotion-fix pin)
    var e1 = Unique("con-e1");
    await admin.CreateStream(new StreamSpec(e1));
    _streams.Add(e1);
    foreach (var seq in Enumerable.Range(0, 3))
        await inst.Bus.PublishAsync(e1, Body($"k:{seq}"), new PublishOptions(), TestContext.Current.CancellationToken);
    var first = new List<int>();
    var c1 = await inst.Bus.ConsumeAsync(e1, new ConsumeOptions { Reference = "grp-e1" }, Rec(first), TestContext.Current.CancellationToken);
    await WaitForAsync(() => Count(first) == 3, TimeSpan.FromSeconds(60), "e1 first drain");
    await c1.DisposeAsync();
    Assert.Equal(new[] { 0, 1, 2 }, first);
    foreach (var seq in Enumerable.Range(3, 3))
        await inst.Bus.PublishAsync(e1, Body($"k:{seq}"), new PublishOptions(), TestContext.Current.CancellationToken);
    var second = new List<int>();
    var c2 = await inst.Bus.ConsumeAsync(e1, new ConsumeOptions { Reference = "grp-e1" }, Rec(second), TestContext.Current.CancellationToken);
    await WaitForAsync(() => Count(second) == 3, TimeSpan.FromSeconds(60), "e1 resume drain");
    await c2.DisposeAsync();
    Assert.Equal(new[] { 3, 4, 5 }, second);
    // e2: ResumeOrFail on a fresh reference throws
    var e2 = Unique("con-e2");
    await admin.CreateStream(new StreamSpec(e2));
    _streams.Add(e2);
    await Assert.ThrowsAsync<StreamBusConfigurationException>(() =>
        inst.Bus.ConsumeAsync(e2, new ConsumeOptions { Reference = "grp-e2", Offset = ConsumeOffset.ResumeOrFail }, Rec(new List<int>()), TestContext.Current.CancellationToken));
    // e3: AlwaysFirst replays; AlwaysNext tails (attach point fixed at creation, so
    // publishing after ConsumeAsync returns is deterministically post-attach)
    var e3 = Unique("con-e3");
    await admin.CreateStream(new StreamSpec(e3));
    _streams.Add(e3);
    foreach (var seq in Enumerable.Range(0, 2))
        await inst.Bus.PublishAsync(e3, Body($"k:{seq}"), new PublishOptions(), TestContext.Current.CancellationToken);
    var replay = new List<int>();
    var c3 = await inst.Bus.ConsumeAsync(e3, new ConsumeOptions { Reference = "grp-e3", Offset = ConsumeOffset.AlwaysFirst }, Rec(replay), TestContext.Current.CancellationToken);
    await WaitForAsync(() => Count(replay) == 2, TimeSpan.FromSeconds(60), "e3 replay drain");
    await c3.DisposeAsync();
    Assert.Equal(new[] { 0, 1 }, replay);
    var tail = new List<int>();
    var c4 = await inst.Bus.ConsumeAsync(e3, new ConsumeOptions { Reference = "grp-e3b", Offset = ConsumeOffset.AlwaysNext }, Rec(tail), TestContext.Current.CancellationToken);
    foreach (var seq in Enumerable.Range(2, 2))
        await inst.Bus.PublishAsync(e3, Body($"k:{seq}"), new PublishOptions(), TestContext.Current.CancellationToken);
    await WaitForAsync(() => Count(tail) == 2, TimeSpan.FromSeconds(60), "e3 tail drain");
    await c4.DisposeAsync();
    Assert.Equal(new[] { 2, 3 }, tail);
}
```

Test (f) Set/Get escapes:

```csharp
[Fact]
public async Task Set_then_get_round_trip_and_resume()
{
    var stream = Unique("con-f");
    var admin = await ConnectAdminAsync();
    await using var _ = admin;
    await admin.CreateStream(new StreamSpec(stream));
    _streams.Add(stream);
    await using var pub = await BusInstance.StartAsync("pub");
    try
    {
        foreach (var s in Enumerable.Range(0, 10))
            await pub.Bus.PublishAsync(stream, Body($"k:{s}"), new PublishOptions(), TestContext.Current.CancellationToken);
    }
    finally { await pub.DisposeAsync(); }
    await using var inst = await BusInstance.StartAsync("i");
    await inst.Bus.SetConsumerOffsetAsync(stream, "grp-f", 0, 7, TestContext.Current.CancellationToken);
    Assert.Equal(7UL, await inst.Bus.GetConsumerOffsetAsync(stream, "grp-f", 0, TestContext.Current.CancellationToken));
    var got = new List<int>();
    var c = await inst.Bus.ConsumeAsync(stream, new ConsumeOptions { Reference = "grp-f" }, Rec(got), TestContext.Current.CancellationToken);
    await WaitForAsync(() => Count(got) == 2, TimeSpan.FromSeconds(60), "post-set drain");
    await c.DisposeAsync();
    Assert.Equal(new[] { 8, 9 }, got);
}
```

Test (g) fail-then-succeed redelivery (FR-10.6):

```csharp
[Fact]
public async Task Handler_failure_redelivers_without_advancing()
{
    var stream = Unique("con-g");
    var admin = await ConnectAdminAsync();
    await using var _ = admin;
    await admin.CreateStream(new StreamSpec(stream));
    _streams.Add(stream);
    await using var pub = await BusInstance.StartAsync("pub");
    try
    {
        foreach (var seq in Enumerable.Range(0, 5))
            await pub.Bus.PublishAsync(stream, Body($"k:{seq}"), new PublishOptions(), TestContext.Current.CancellationToken);
    }
    finally { await pub.DisposeAsync(); }
    await using var inst = await BusInstance.StartAsync("i");
    var got = new List<int>();
    var attempts = new Dictionary<int, int>();
    var c = await inst.Bus.ConsumeAsync(stream, new ConsumeOptions { Reference = "grp-g" }, ctx =>
    {
        var (_, seq) = Parse(ctx.Body);
        lock (attempts) attempts[seq] = attempts.TryGetValue(seq, out var n) ? n + 1 : 1;
        if (seq == 2 && attempts[seq] <= 2) throw new InvalidOperationException("boom on 2");
        lock (got) got.Add(seq);
        return Task.CompletedTask;
    }, TestContext.Current.CancellationToken);
    await WaitForAsync(() => Count(got) == 5, TimeSpan.FromSeconds(90), "redelivery drain");
    await c.DisposeAsync();
    // Failures throw before recording, so only successes land in `got`; the attempts map
    // is what proves redelivery (offset 2 attempted 3 times: fail, fail, succeed).
    Assert.Equal(new[] { 0, 1, 2, 3, 4 }, got);
    Assert.Equal(3, attempts[2]);
    Assert.Equal(4UL, await inst.Bus.GetConsumerOffsetAsync(stream, "grp-g", 0, TestContext.Current.CancellationToken));
}
```

(The synchronous `throw` inside the non-async lambda surfaces through the wrapper's awaited invocation onto the same failure path. Two failures each trigger dispose+backoff+recreate from the stored offset 1, so offset 2 arrives three times before 3; the stored offset advances only after success, ending at 4.)

Test (h) inflight gauge wiring proof (deterministic via a gate):

```csharp
[Fact]
public async Task Inflight_gauge_tracks_dispatched_uncompleted()
{
    var stream = Unique("con-h");
    var admin = await ConnectAdminAsync();
    await using var _ = admin;
    await admin.CreateStream(new StreamSpec(stream));
    _streams.Add(stream);
    await using var pub = await BusInstance.StartAsync("pub");
    try
    {
        await pub.Bus.PublishAsync(stream, Body("k:0"), new PublishOptions(), TestContext.Current.CancellationToken);
    }
    finally { await pub.DisposeAsync(); }
    await using var inst = await BusInstance.StartAsync("i");
    using var listener = new MeterListener();
    var observed = new List<(string Name, double Value, string Partition)>();
    listener.InstrumentPublished = (instrument, listener2) =>
    {
        if (instrument.Meter.Name == "StreamBus") listener2.EnableMeasurementEvents(instrument);
    };
    listener.SetMeasurementEventCallback<long>((measurement, value, tags, _) =>
    {
        if (measurement.Name == "streambus_partition_inflight")
        {
            string? part = null;
            foreach (var tag in tags) if (tag.Key == "partition") part = tag.Value?.ToString();
            lock (observed) observed.Add((measurement.Name, value, part ?? "?"));
        }
    });
    listener.Start();
    var entered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
    var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
    var c = await inst.Bus.ConsumeAsync(stream, new ConsumeOptions { Reference = "grp-h" }, async ctx =>
    {
        entered.TrySetResult();
        await release.Task;
    }, TestContext.Current.CancellationToken);
    try
    {
        Assert.True(await Task.WhenAny(entered.Task, Task.Delay(TimeSpan.FromSeconds(30))) == entered.Task, "handler entered");
        listener.RecordObservableInstruments();
        Assert.Contains(observed, m => m.Value == 1 && m.Partition == "0");
    }
    finally { release.TrySetResult(); }
    await WaitForAsync(() =>
    {
        listener.RecordObservableInstruments();
        lock (observed) return observed.Any(m => m.Value == 0);
    }, TimeSpan.FromSeconds(30), "inflight back to 0");
    await c.DisposeAsync();
}
```

(`release` is always set in `finally`, so a failing assert cannot hang disposal on the gated handler. `using System.Diagnostics.Metrics;` is required at the top of the file — add it to the harness usings.)

- [ ] **Step 2: Verify (full Phase 5 gate)**

```powershell
dotnet test StreamBus.slnx -c Release
Select-String -Path src/StreamBus/*.cs -Pattern "Reliable(Producer|Consumer|SuperStream)"
```

Expected: full suite green (43 + new unit + 8 new integration); the `Select-String` check returns nothing (no `Reliable*` client types under `src/`); broker version recorded (expect 4.2.6; G-3 not required).

- [ ] **Step 3: Commit (with human approval)**

```powershell
git add tests/StreamBus.IntegrationTests/ConsumerIntegrationTests.cs
git commit -m "test: add consumer integration probes and G-4/G-5a gates"
```

### Round 2 amendments (executed as part of Task 6's fix round — decides the open items above)

R2-A. Lag redesign (ExpE probe, 2026-10-12: `LastOffset()` returns 0 with 10 messages present, immediately and after 3 s and after consume; empty streams throw `OffsetNotFoundException`): `LastOffset()` is unusable as an end offset on this broker, so the 15 s poller cannot work. Delete `src/StreamBus/PartitionLagPoller.cs` entirely; remove `GetEndOffsetAsync` from `IStreamSystemHandle` + `StreamSystemHandle` + all test stubs implementing it; remove poller creation/tasks/disposal from `StreamConsumerRegistration`; drop the `Lag` field from `PartitionState` and initialize `LastDelivered`/`LastStored` to `-1` (nothing delivered/stored yet — so an empty consumer reads lag 0, offset 0 delivered reads 1, offset 0 stored reads 0); the `streambus_partition_lag` observable computes `Math.Max(0, s.LastDelivered - s.LastStored)` live (consumer-held unprocessed backlog — documented as such, not broker-total). Rewrite Task 5's lag test as: tracked state `{ LastDelivered = 9, LastStored = 4 }` observes lag 5 via `MeterListener.RecordObservableInstruments()`.

R2-B. (b)-tail rework (above): record broker `ctx.Offset` in tuples; assert stored == max-received per non-empty partition (null when empty).

R2-C. Producer naming: the per-handle uniqueness counter MUST be process-wide `static` (sequential same-stream producers across systems must never collide); cap total name length at 255 UTF-8 bytes (hash-suffix fallback mirroring consumer references). Phase 7 will replace naming with stable per-(partition,target) names.

---

## Follow-ups for Phase 6 / Phase 7 (tracked, no code in this phase)

- Promotion queries use `CancellationToken.None` with no timeout — add a timeout policy note when retry (Phase 6) lengthens promotion windows.
- Poison messages loop forever with backoff by design — confirm log-volume acceptability when Phase 6's retry multiplies attempts.
- Read-process-write (Phase 7) must not assume dense broker offsets: failover evidence shows stored 36–41 for 24 sends under churn — resume with max-delivered semantics, never count-based.
- Broker-total backlog lag is unmeasurable here (`LastOffset` unusable, evidenced) — revisit only if the client/broker exposes a true end offset.
- `retry_blocked` gauge is a static 0 with `failure_mode="strict"` until Phase 6 drives real values.

---

## Self-Review

- Spec coverage: FR-2.2 (Tasks 3–4 delegate path), FR-2.3 (Task 6a), FR-2.4 (Task 6c), FR-2.5 (Tasks 2–3 resolve-on-every-attach + Task 6d), FR-2.6 (Tasks 3 sequential dispatch + Task 6a/b order asserts), FR-2.7 (Tasks 2–3 guard + Task 5/6 pins), FR-3.1/3.2 (Task 6a stability), FR-3.3 (no code mutates partitions — nothing to implement), FR-3.4 (documented in requirements §4 — nothing to implement), FR-9.1 (Tasks 3–4 metrics + Task 5/6 pins), FR-10.1–10.5 (Task 2 resolve + Task 5 matrix + Task 6e), FR-10.6 (Task 3 no-advance-on-failure + Task 6g), FR-10.7 (Tasks 2–3 guard/regression + Task 5), FR-10.8 (Task 3 cadence + Task 5 window/timer tests), FR-10.9 (per-partition consumer store path only — no super-entity store call exists), FR-10.10 (Task 1 derivation + Task 5 tests; typed-path extension in Phase 9), FR-10.11 (Task 4 ConsumeAsync validation + Task 5 `ConsumeValidationTests`)
- FR-10.12 (Task 2 fallback callback + Task 3 wiring to log/metric + Task 5 logger-sink/counter tests + Task 6 fresh runs), FR-10.13 (Task 4 Get/Set + Task 6f), FR-2.1/2.8 (Phase 9 — excluded with rationale), FR-10.14 (Phase 7 — excluded with rationale).
- Placeholder scan: no TBD/TODO; every code step ships complete source; the Task 2 probe branches are fully specified conditionals with exact commands (Message-struct and missing-end-offset cases escalate via NEEDS_CONTEXT rather than inventing designs); no "similar to Task N".
- Type consistency: `PublishAsync`-style naming carried over (`ConsumeAsync(string, ConsumeOptions, Func<ConsumeContext,Task>, CancellationToken)` on buses; internal registry overload adds leading `Type busType`); `PublishOptions`/`ConsumeOptions` member names distinct per direction; `SupervisorFor`/`ProducersFor`/`ConsumersFor`/`ActiveConsumers` internal accessors used identically across Tasks 1–6; `ShouldStoreOnSuccess(ulong)` call sites (Task 3 block + Task 5 tests) match.
- Review Focus: each line pinned above.
