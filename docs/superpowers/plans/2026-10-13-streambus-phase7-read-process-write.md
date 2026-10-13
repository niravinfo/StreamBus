# StreamBus Phase 7 (Effectively-Once Read-Process-Write) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Implement effectively-once read-process-write: `PublishResultAsync` (publishing-id = source offset, stable producer names, retry-forever) plus a managed `ConsumeEffectivelyOnceAsync` cycle (no source offset stores, resume from the target's last publishing id on every attach), with the G-2 end-to-end gate.

**Architecture:** The result path reuses the Phase 6 layering: a stable-name `ResultProducerRegistry` (cached producers keyed by derived name, single confirmed attempt, last-id resume queries) under a `ResultPublisher` orchestration (retry-forever, injectable delay, unit-testable without a broker). The partition loop gains an effectively-once branch that reuses the Strict-unbounded attempt loop and the attach/promotion machinery but publishes the handler's returned bytes with id = source offset and never stores; resume is injected through the existing `stored + 1` rule by feeding it the target's last publishing id, so every attach (create, promote, reconnect) recomputes from the target. Replay bias is structural: any resume doubt resolves earlier (dedup absorbs reprocessing), never later.

**Tech Stack:** .NET 10 (`net10.0`), C# latest, `RabbitMQ.Stream.Client` 1.12.1 (same pins — no new packages), xUnit v3, `StreamBus.slnx`, Central Package Management.

**Spec:** `docs/2026-10-08-streambus-design.md` (§7 effectively-once, §9 result-path sketch, §10 row 7, §12 offset mechanics) and `docs/2026-10-08-streambus-requirements.md` (FR-6.1–FR-6.8, FR-10.14, G-2; G-5 pattern for kill-windows). Phase table: design §10 row 7. G-2 blocks this phase.

## Global Constraints

- `TargetFramework` exactly `net10.0`; `Nullable`/`ImplicitUsings` enable; `TreatWarningsAsErrors` true; `ManagePackageVersionsCentrally` true; `IsTestingPlatformApplication` false (all in `Directory.Build.props`, unchanged).
- `RabbitMQ.Stream.Client` exactly `1.12.1`; `Microsoft.Extensions.Hosting` / `Diagnostics.HealthChecks` / `Logging.Abstractions` exactly `10.0.12`; `xunit.v3` 4.0.2, `xunit.runner.visualstudio` 4.0.1, `Microsoft.NET.Test.Sdk` 18.10.1.
- Library root namespace `StreamBus`; never `RabbitMQ.Stream.*`.
- Ordered paths use `RawConsumer` plus the library supervision loop — never `Reliable*`.
- The resume point is never produced implicitly as `OffsetTypeNext`, and never replayed implicitly as `OffsetTypeFirst` (Phase 5 invariant, unchanged). EO first-run `First` comes only from the explicit never-published branch.
- The resume point is never inferred from local, in-memory, or last-dispatched state — `localWater` stays a write-guard only (EO writes nothing, so the guard stays at 0 on EO references).
- The library never creates, deletes, or alters streams or super streams (topology `Ensure*` is Phase 8): source and target must pre-exist; missing streams are `StreamBusStreamNotFoundException`, never an implicit create.
- Result publishes use caller-visible stable names with explicit ids — never the unique-name `PublishAsync` path (whose ids restart at 1 per handle and would corrupt dedup).
- A `src/` grep for `ReliableProducer`, `ReliableConsumer`, `ReliableSuperStream` (any `Reliable*` client type) must return nothing; a `src/` grep for `CreateStream|CreateSuperStream|DeleteStream|DeleteSuperStream` must return nothing (admin lifecycle is test-only); committed in Task 4 verify.
- Integration tests require the local broker at `localhost:5552` with non-guest creds (`STREAMBUS_USER`/`STREAMBUS_PASSWORD`, defaulting to `app`/`password` in-test); never use `guest`.
- Every commit step requires explicit human approval before running; never push, never amend without permission.

## Review Focus

- A re-publish under an already-used (producer name, publishing id) with *different* bytes must keep the first write, not corrupt the target (FR-6.1/FR-6.3 dedup intent). Pinned by Task 6b (poison-republish: count stable, originals intact).
- A kill at any point — including between target-confirm and any bookkeeping (there is none on the EO path) — must never duplicate or skip on restart. Pinned by Task 6c/6g exact-content asserts.
- `QuerySequence` for a never-used (reference, stream) must map to first-run `First`, never to a skip of offset 0. Pinned by Task 1 probe record + Task 2 never-published branch + Task 5 first-run unit test + Task 6a from-empty run.
- Concurrent manual `PublishResultAsync` calls under one producer name (caller bug; FR-6.3 forbids it) must surface loudly via the pending-collision internal error, never interleave silently. Pinned by Task 5 concurrent-publish unit test; the managed path is sequential by construction (dispatch gate + SAC).
- A super-stream target with unroutable results (null routing, unrouted source, no fallback) must stall loudly via `FailAttach` (redeliver, never skip), never drop. Pinned by Task 3 missing-routing branch + Task 6f (attempts grow, zero target writes).

---

## File Structure

New files (no new packages):

- `src/StreamBus/EffectivelyOnce.cs` — `EffectivelyOnceOptions` + `PublishResultOptions` (public), `ResultWork` + `EffectivelyOnceConfig` (internal), `ResultNaming` (stable producer name + consumer reference + id derivation), `ResultResume` (max + 1 math), `EffectivelyOnceValidation` (all EO/publish-result config checks, unit-testable without a broker).
- `src/StreamBus/ResultProducerRegistry.cs` — `ResultProducerRegistry` (stable-name cache with 255-name cap, single-attempt `PublishResultAsync`, bias-to-replay `GetResumeOffsetAsync`).
- `src/StreamBus/ResultPublisher.cs` — `ResultPublisher` (`PublishOnceAsync` + retry-forever `PublishForeverAsync` with injectable delay).

Modified Phase 3/4/5/6 files (same branch lineage; existing suites must stay green):

- `src/StreamBus/Connector.cs` — `IStreamSystemHandle` gains `CreateSuperStreamProducerWithReferenceAsync` + `QuerySequenceAsync`; `StreamSystemHandle` implements both.
- `src/StreamBus/ProducerHandles.cs` — `IStreamProducerHandle` gains `PublishResultAsync(body, publishingId, routingKey, ct)`; plain handle ignores routing (commented), super handle stamps it (required, backstop config error).
- `src/StreamBus/ConsumeOptions.cs` — gains `internal EffectivelyOnceConfig? EffectivelyOnce` (set only by `ConsumeEffectivelyOnceAsync`; null = normal path).
- `src/StreamBus/StreamPartitionConsumer.cs` — EO ctor params (appended after `logger`, all call sites compile unchanged), `QueryStoredAsync` branch used on create + promote, EO dispatch branch with compute-retry + publish-forever and no store, store/timer guards, missing-routing `FailAttach`.
- `src/StreamBus/ConsumerRegistry.cs` — 4th... 5th ctor param `ResultProducerRegistry results` (update `Registry.ConsumersFor` + every test construction found by grep), `ConsumeEffectivelyOnceAsync`, `PublishResultAsync` (validation + existence + forever loop).
- `src/StreamBus/Registry.cs` — `ResultProducersFor(vhost)` + pass-through into `ConsumersFor`.
- `src/StreamBus/Bus.cs` — `PublishResultAsync` + `ConsumeEffectivelyOnceAsync` on `IStreamBus` (+ both implementations; generic inherits the interface).

Tests:

- `tests/StreamBus.UnitTests/EffectivelyOnceTests.cs` — naming, validation matrix (no broker), resume math, 255-cap, concurrent-collision.
- `tests/StreamBus.UnitTests/ResultPublisherTests.cs` — forever-orchestration (fail-twice-then-confirm, cancel-mid, negative-delay clamp, throwing-delay fallback).
- `tests/StreamBus.IntegrationTests/EffectivelyOnceIntegrationTests.cs` — plain/super happy paths, FR-6.1 id assert, FR-10.14 no-store, dedup poison-republish, kill-mid-run, retry-order, missing-routing stall, SAC split, G-2 full cycle (FR-6.8).

Explicitly out of Phase 7 (deferred, not stubs): typed `IConsumer<T>` + serializer (Phase 9: FR-2.1/2.8); topology `Ensure*` (Phase 8: FR-7); nullable/filter results (v1 handler returns bytes — exactly one write per step); EO + `SkipAfterDeadline`/`ErrorStream` (contradicts no-store; rejected); hot-key auto-remedy (none exists — NFR-4, metrics only); new metrics series (existing gauges reused).

Recorded rulings (spec as authority; bind all tasks): handler returns `ReadOnlyMemory<byte>` (non-nullable — exactly one write enforced by types; filtering is a non-goal, use a marker body); manual `PublishResultAsync` is the primitive (enforces id = offset, retries forever, no `Timeout` by design) while `ConsumeEffectivelyOnceAsync` is the managed cycle (owns resume + no-store + ordering) — a manual publish combined with a storing consumer is at-least-once, documented on the method; resume always biases earlier (any query doubt → `First`/lower max; dedup absorbs); SAC is hardcoded `true` for EO (FR-6.3, no knob); super-target routing defaults to the source `RoutingValue` per message with an overridable `TargetRouting` func (manual path uses static `RoutingKey`); first-run and every query failure resolve to `First`; the 255-name cap is exact single-threaded and best-effort under racing first-use (broker enforces the real limit); `SetConsumerOffsetAsync` on an EO reference only touches the guard water and never the resume (target is the source of truth — XML-comment); `TargetRouting` throwing propagates to the dispatch safety net (attach recycle + redeliver, never a skip).

---

### Task 1: Live-broker probe + EO records + naming + validation

**Files:**
- Create: `src/StreamBus/EffectivelyOnce.cs`
- Modify: none (probe is throwaway, outside the repo).

**Interfaces:**
- Consumes: `ReferenceDerivation.MaxReferenceBytes`, `StreamBusConfigurationException`, `StreamBusPublishException`, `RetryPolicy`/`FailureMode`, `ReadinessPolicy`, `ConsumeContext`, `StreamBusDelays.ExponentialCapped` (later tasks).
- Produces: `EffectivelyOnceOptions`, `PublishResultOptions`, `ResultWork`, `EffectivelyOnceConfig`, `ResultNaming.*`, `ResultResume.ResumeOffset`, `EffectivelyOnceValidation.*` for Tasks 2–6.

- [ ] **Step 1: Run the live dedup/sequence probe (throwaway console app, record everything)**

The load-bearing unknowns are broker-side and cannot be answered by reflection: (a) `QuerySequence` for a never-used (reference, stream); (b) `QuerySequence` after explicit-id publishes; (c) re-publish under a used (name, id) with different bytes; (d) super-producer explicit ids + per-partition `QuerySequence`. Create a throwaway app OUTSIDE the repo and delete it after:

```powershell
$probe = "$env:TEMP/opencode/eo-probe"
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
using Microsoft.Extensions.Logging.Abstractions;
using RabbitMQ.Stream.Client;

var user = Environment.GetEnvironmentVariable("STREAMBUS_USER") is { Length: > 0 } u ? u : "app";
var pass = Environment.GetEnvironmentVariable("STREAMBUS_PASSWORD") is { Length: > 0 } p ? p : "password";
var uid = Guid.NewGuid().ToString("N")[..8];
var plain = $"probe-eo-plain-{uid}";
var super = $"probe-eo-super-{uid}";
var name = $"probe-eo-name-{uid}";

await using var system = await StreamSystem.Create(new StreamSystemConfig
{
    UserName = user, Password = pass, VirtualHost = "/",
    Endpoints = new List<EndPoint> { new IPEndPoint(IPAddress.Loopback, 5552) },
});
try
{
    await system.CreateStream(new StreamSpec(plain));
    await system.CreateSuperStream(new PartitionsSuperStreamSpec(super, 2));

    // (a) unknown reference
    try
    {
        var q = await system.QuerySequence("probe-never-used-" + uid, plain);
        Console.WriteLine($"UNKNOWN-REF-RESULT: {q}");
    }
    catch (Exception ex)
    {
        Console.WriteLine($"UNKNOWN-REF-THROWS: {ex.GetType().FullName}: {ex.Message.Split('\n')[0]}");
    }

    // (b) explicit ids then query
    var confirm = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
    var confirmed = 0;
    // (Client interfaces declare Dispose() without extending IDisposable,
    // so plain var + explicit Dispose — verified against 1.12.1.)
    var producer = await system.CreateRawProducer(
        new RawProducerConfig(plain) { Reference = name, ConfirmHandler = _ => { if (Interlocked.Increment(ref confirmed) == 3) confirm.TrySetResult(); } },
        NullLogger<RawProducer>.Instance);
    await producer.Send(0, new Message(new byte[] { 1 }));
    await producer.Send(1, new Message(new byte[] { 2 }));
    await producer.Send(2, new Message(new byte[] { 3 }));
    await confirm.Task.WaitAsync(TimeSpan.FromSeconds(15));
    Console.WriteLine($"LAST-ID-AFTER-0-1-2: {await system.QuerySequence(name, plain)}");

    // (c) re-publish same ids, different bodies
    // NOTE: same producer instance, same ids — broker dedup applies per (name, id).
    await producer.Send(0, new Message(new byte[] { 9 }));
    await producer.Send(1, new Message(new byte[] { 9 }));
    await producer.Send(2, new Message(new byte[] { 9 }));
    await Task.Delay(TimeSpan.FromSeconds(3));
    Console.WriteLine($"LAST-ID-AFTER-REPLAY: {await system.QuerySequence(name, plain)}");
    var bodies = new List<byte>();
    var consumer = await system.CreateRawConsumer(
        new RawConsumerConfig(plain) { Reference = "probe-read-" + uid, OffsetSpec = new OffsetTypeFirst(), MessageHandler = (_, _, m) => { lock (bodies) bodies.Add(m.Data.Contents.ToArray()[0]); return Task.CompletedTask; } },
        NullLogger<RawConsumer>.Instance);
    await Task.Delay(TimeSpan.FromSeconds(3));
    await consumer.Close();
    consumer.Dispose();
    Console.WriteLine($"BODIES-AFTER-REPLAY: {string.Join(",", bodies.Order())} (expect 1,2,3 if first-write-wins)");
    producer.Dispose();

    // (d) super producer explicit ids + per-partition query
    var sname = $"probe-eo-sname-{uid}";
    var sproducer = await system.CreateRawSuperStreamProducer(
        new RawSuperStreamProducerConfig(super) { Reference = sname, Routing = _ => "k", ConfirmHandler = _ => { } },
        NullLogger<RawProducer>.Instance);
    await sproducer.Send(10, new Message(new byte[] { 7 }));
    await sproducer.Send(11, new Message(new byte[] { 7 }));
    await Task.Delay(TimeSpan.FromSeconds(3));
    foreach (var part in await system.QueryPartition(super))
        try { Console.WriteLine($"SUPER-PART {part}: QuerySequence = {await system.QuerySequence(sname, part)}"); }
        catch (Exception ex) { Console.WriteLine($"SUPER-PART {part}: throws {ex.GetType().Name}"); }
    sproducer.Dispose();
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

The task report must record verbatim: the `UNKNOWN-REF-*` line, `LAST-ID-AFTER-0-1-2`, `BODIES-AFTER-REPLAY`, and every `SUPER-PART` line. Measured 2026-10-13 (recorded in the Task 1 report): unknown returns `0` (does NOT throw); last-id `2`; bodies `1,2,3` (first-write-wins); touched super partition returns its last id, untouched returns `0`. Ruling R1 applies: `QuerySequence` 0 maps to never-published (null) → first-run `First` (replay-biased, dedup-absorbed); the Task 2 skip-partition branch keys on the mapped null, not on throws.

- [ ] **Step 2: Implement `EffectivelyOnce.cs`**

```csharp
using System.Security.Cryptography;
using System.Text;

namespace StreamBus;

public sealed record EffectivelyOnceOptions
{
    public bool SourceIsSuperStream { get; init; } = false;
    public bool TargetIsSuperStream { get; init; } = false;
    public string? Reference { get; init; }
    public Func<ConsumeContext, string?>? TargetRouting { get; init; }
    public RetryPolicy Retry { get; init; } = new();
    public ReadinessPolicy ReadinessPolicy { get; init; } = ReadinessPolicy.FailFastPolicy;
}

public sealed record PublishResultOptions
{
    public required string ProducerName { get; init; }
    public bool IsSuperStream { get; init; } = false;
    public string? RoutingKey { get; init; }
    public TimeSpan BaseDelay { get; init; } = TimeSpan.FromMilliseconds(200);
    public double Multiplier { get; init; } = 2.0;
    public TimeSpan MaxDelay { get; init; } = TimeSpan.FromSeconds(30);
    public TimeSpan MaxJitter { get; init; } = TimeSpan.FromSeconds(1);
}

internal sealed record ResultWork
{
    public required string TargetStream { get; init; }
    public required bool TargetSuper { get; init; }
    public required ReadOnlyMemory<byte> Body { get; init; }
    public required ulong SourceOffset { get; init; }
    public required string? RoutingKey { get; init; }
    public required string ProducerName { get; init; }
}

internal sealed record EffectivelyOnceConfig
{
    public required string TargetStream { get; init; }
    public required bool TargetIsSuperStream { get; init; }
    public required Func<ConsumeContext, string?> TargetRouting { get; init; }
    public required Func<ConsumeContext, Task<ReadOnlyMemory<byte>>> ResultHandler { get; init; }
    // Resume source of truth, injected by the registry (Ruling R2: a func, not the
    // Task-2 registry type, so this file compiles standalone in Task 1).
    // Args: (targetStream, targetSuper, producerName, ct) -> last publishing id, or null.
    public required Func<string, bool, string, CancellationToken, Task<ulong?>> ResumeQuery { get; init; }
}

internal static class ResultNaming
{
    public static string ProducerName(string sourcePartitionStream, string targetStream)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(sourcePartitionStream);
        ArgumentException.ThrowIfNullOrWhiteSpace(targetStream);
        var full = $"streambus-eo-{sourcePartitionStream}-to-{targetStream}";
        if (Encoding.UTF8.GetByteCount(full) <= ReferenceDerivation.MaxReferenceBytes)
            return full;
        var hash = Convert.ToHexString(SHA256.HashData(Encoding.UTF8.GetBytes(full)))[..32];
        return $"streambus-eo-{hash}";
    }

    public static string ConsumerReference(Type busType, string sourceStream, string targetStream)
    {
        ArgumentNullException.ThrowIfNull(busType);
        ArgumentException.ThrowIfNullOrWhiteSpace(sourceStream);
        ArgumentException.ThrowIfNullOrWhiteSpace(targetStream);
        var full = $"streambus-eo:{busType.FullName}:{sourceStream}->{targetStream}";
        if (Encoding.UTF8.GetByteCount(full) <= ReferenceDerivation.MaxReferenceBytes)
            return full;
        var hash = Convert.ToHexString(SHA256.HashData(Encoding.UTF8.GetBytes(full)))[..32];
        return $"streambus-eo-{hash}";
    }

    public static ulong PublishingId(ulong sourceOffset) => sourceOffset;
}

internal static class ResultResume
{
    // Returns the greatest known last publishing id — the resume BASE, not the
    // resume point (Ruling R4): the shared stored+1 rule in OffsetResolution
    // derives lastId+1 exactly once. Both GetResumeOffsetAsync branches return
    // this base, so a super-target resume can never double-advance past an
    // unprocessed source offset (FR-6.5). Null when nothing is known.
    public static ulong? ResumeOffset(IEnumerable<ulong?> lastIds)
    {
        ArgumentNullException.ThrowIfNull(lastIds);
        ulong? max = null;
        foreach (var id in lastIds)
        {
            if (id is null) continue;
            max = max is null ? id.Value : Math.Max(max.Value, id.Value);
        }
        return max;
    }
}

internal static class EffectivelyOnceValidation
{
    public static void ValidateConsume(RetryPolicy retry, string prefix = "EffectivelyOnceOptions.Retry")
    {
        ArgumentNullException.ThrowIfNull(retry);
        if (retry.FailureMode != FailureMode.Strict)
            throw new StreamBusConfigurationException($"{prefix}.FailureMode must be Strict for effectively-once (SkipAfterDeadline has no offset to advance; the source stores nothing).");
        if (retry.MaxAttempts is not null)
            throw new StreamBusConfigurationException($"{prefix}.MaxAttempts must be null for effectively-once (unbounded retry; the publish is retried until confirmed).");
        ValidateKnobs(retry.BaseDelay, retry.MaxDelay, retry.Multiplier, retry.MaxJitter, prefix);
    }

    public static void ValidatePublishResult(PublishResultOptions options)
    {
        ArgumentNullException.ThrowIfNull(options);
        ArgumentException.ThrowIfNullOrWhiteSpace(options.ProducerName);
        if (Encoding.UTF8.GetByteCount(options.ProducerName) > ReferenceDerivation.MaxReferenceBytes)
            throw new StreamBusConfigurationException("PublishResultOptions.ProducerName exceeds the broker's 255-byte limit.");
        if (options.IsSuperStream && string.IsNullOrWhiteSpace(options.RoutingKey))
            throw new StreamBusConfigurationException("PublishResultOptions.RoutingKey is required for a super-stream result target.");
        ValidateKnobs(options.BaseDelay, options.MaxDelay, options.Multiplier, options.MaxJitter, "PublishResultOptions");
    }

    public static void ValidateKnobs(TimeSpan baseDelay, TimeSpan maxDelay, double multiplier, TimeSpan maxJitter, string prefix)
    {
        if (baseDelay <= TimeSpan.Zero)
            throw new StreamBusConfigurationException($"{prefix}.BaseDelay must be positive.");
        if (maxDelay <= TimeSpan.Zero)
            throw new StreamBusConfigurationException($"{prefix}.MaxDelay must be positive.");
        if (multiplier < 1.0 || !double.IsFinite(multiplier))
            throw new StreamBusConfigurationException($"{prefix}.Multiplier must be a finite number >= 1.");
        if (maxJitter < TimeSpan.Zero)
            throw new StreamBusConfigurationException($"{prefix}.MaxJitter must be non-negative.");
    }
}
```

- [ ] **Step 3: Verify**

```powershell
dotnet build src/StreamBus/StreamBus.csproj -c Release
```

Expected: zero warnings/errors. Records are exercised by Tasks 2–5.

- [ ] **Step 4: Commit (with human approval)**

```powershell
git add src/StreamBus/EffectivelyOnce.cs
git commit -m "feat: add effectively-once options, naming, and resume records"
```

---

### Task 2: Result transport — handle path, connector, stable registry, forever-publisher

**Files:**
- Create: `src/StreamBus/ResultProducerRegistry.cs`
- Create: `src/StreamBus/ResultPublisher.cs`
- Modify: `src/StreamBus/ProducerHandles.cs` (`PublishResultAsync` on the interface + both handles)
- Modify: `src/StreamBus/Connector.cs` (`CreateSuperStreamProducerWithReferenceAsync` + `QuerySequenceAsync`)
- Modify: every existing `IStreamSystemHandle` / `IStreamProducerHandle` stub under `tests/` (grep first; add the two handle members + one producer member, each throwing `NotImplementedException("<TestClass> ...")`).

**Interfaces:**
- Consumes: `ResultNaming`/`ResultResume`/`PublishResultOptions`/`ResultWork` (Task 1), `ConfirmationTracker.PublishWithIdAsync`, `ReadinessPolicy.FailFastPolicy`, `ISuperStreamProducer : IProducer` (probed: inheritance holds, so no tracker overload is needed).
- Produces: `ResultProducerRegistry.PublishResultAsync` (single confirmed attempt) + `GetOrCreateAsync` + `GetResumeOffsetAsync`, `ResultPublisher.PublishOnceAsync` + `PublishForeverAsync` for Tasks 3–6.

- [ ] **Step 1: Add `PublishResultAsync` to `ProducerHandles.cs`**

Interface addition:

```csharp
Task PublishResultAsync(ReadOnlyMemory<byte> body, ulong publishingId, string? routingKey, CancellationToken ct);
```

`StreamProducerHandle` implementation (routing is ignored on plain targets — a super source feeding a plain target still carries a source routing value; it has nowhere to go):

```csharp
public async Task PublishResultAsync(ReadOnlyMemory<byte> body, ulong publishingId, string? routingKey, CancellationToken ct)
{
    ct.ThrowIfCancellationRequested();
    await _confirmations.PublishWithIdAsync(_producer, _stream, publishingId, new Message(body.ToArray()), ct).ConfigureAwait(false);
}
```

`SuperStreamProducerHandle` implementation (routing is required — the registries validate upfront, so this throw is a defensive backstop, never a primary path):

```csharp
public async Task PublishResultAsync(ReadOnlyMemory<byte> body, ulong publishingId, string? routingKey, CancellationToken ct)
{
    ct.ThrowIfCancellationRequested();
    if (string.IsNullOrWhiteSpace(routingKey))
        throw new StreamBusConfigurationException($"Result publish to super stream '{_stream}' requires a routing key (internal error).");
    var message = new Message(body.ToArray());
    RoutingTable.Add(message, routingKey);
    try
    {
        await _confirmations.PublishWithIdAsync(_producer, _stream, publishingId, message, ct).ConfigureAwait(false);
    }
    finally
    {
        RoutingTable.Remove(message);
    }
}
```

(`PublishWithIdAsync` takes `IProducer`; `ISuperStreamProducer` extends it — no overload needed.)

- [ ] **Step 2: Extend `Connector.cs`**

`IStreamSystemHandle` additions:

```csharp
Task<IStreamProducerHandle> CreateSuperStreamProducerWithReferenceAsync(string stream, string reference, CancellationToken ct);
Task<ulong> QuerySequenceAsync(string reference, string stream, CancellationToken ct);
```

`StreamSystemHandle` implementations (place next to the plain-with-reference method):

```csharp
public async Task<IStreamProducerHandle> CreateSuperStreamProducerWithReferenceAsync(string stream, string reference, CancellationToken ct)
{
    ArgumentException.ThrowIfNullOrWhiteSpace(reference);
    if (Encoding.UTF8.GetByteCount(reference) > ReferenceDerivation.MaxReferenceBytes)
        throw new StreamBusConfigurationException($"Producer reference exceeds the broker's 255-byte limit.");
    ct.ThrowIfCancellationRequested();
    var confirmations = new ConfirmationTracker();
    // Stable name (not the unique-name path): recreating under the same name is
    // safe — dedup is broker-side per (name, id) and the fresh tracker holds no
    // stale in-flight ids. The same routing table as the unique-name path applies.
    var producer = await _system.CreateRawSuperStreamProducer(new RawSuperStreamProducerConfig(stream)
    {
        MessagesBufferSize = 100,
        Reference = reference,
        Routing = SuperStreamProducerHandle.RoutingFor,
        ConfirmHandler = confirmation => confirmations.Complete(confirmation.Item2),
    }, _factory.CreateLogger<RawProducer>()).ConfigureAwait(false);
    return new SuperStreamProducerHandle(producer, stream, confirmations);
}

public async Task<ulong> QuerySequenceAsync(string reference, string stream, CancellationToken ct)
{
    ArgumentException.ThrowIfNullOrWhiteSpace(reference);
    ArgumentException.ThrowIfNullOrWhiteSpace(stream);
    ct.ThrowIfCancellationRequested();
    return await _system.QuerySequence(reference, stream).ConfigureAwait(false);
}
```

(`System.Text` for `Encoding` is already imported in `Connector.cs`. Unknown references throw here — the registry maps every failure to never-published; see Task 1 probe.)

- [ ] **Step 3: Implement `ResultProducerRegistry.cs`**

```csharp
using System.Collections.Concurrent;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using RabbitMQ.Stream.Client;

namespace StreamBus;

internal sealed class ResultProducerRegistry
{
    internal const int MaxProducerNamesPerStream = 255;

    private readonly StreamBusConnectionSupervisor _supervisor;
    private readonly string _vhost;
    private readonly ILogger<ResultProducerRegistry> _logger;
    private readonly ConcurrentDictionary<string, Task<IStreamProducerHandle>> _handles = new();

    public ResultProducerRegistry(StreamBusConnectionSupervisor supervisor, string vhost, ILogger<ResultProducerRegistry>? logger = null)
    {
        ArgumentNullException.ThrowIfNull(supervisor);
        ArgumentNullException.ThrowIfNull(vhost);
        _supervisor = supervisor;
        _vhost = vhost;
        _logger = logger ?? NullLogger<ResultProducerRegistry>.Instance;
    }

    public async Task PublishResultAsync(string targetStream, bool targetSuper, ReadOnlyMemory<byte> body, ulong sourceOffset, string? routingKey, string producerName, CancellationToken ct = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(targetStream);
        ArgumentException.ThrowIfNullOrWhiteSpace(producerName);
        var readiness = await _supervisor.WaitUntilReadyAsync(ReadinessPolicy.FailFastPolicy, ct).ConfigureAwait(false);
        if (readiness == BusReadiness.Fatal)
            throw new StreamBusFatalException($"StreamBus for vhost '{_vhost}' is in Fatal state; result publish to '{targetStream}' refused.");
        if (readiness != BusReadiness.Ready)
            throw new StreamBusNotReadyException($"StreamBus for vhost '{_vhost}' is not ready ({readiness}); result publish to '{targetStream}' refused.");
        var (entry, handle) = await GetOrCreateAsync(targetStream, targetSuper, producerName, ct).ConfigureAwait(false);
        var key = Key(targetStream, targetSuper, producerName);
        try
        {
            await handle.PublishResultAsync(body, ResultNaming.PublishingId(sourceOffset), routingKey, ct).ConfigureAwait(false);
        }
        catch (OperationCanceledException)
        {
            throw;
        }
        catch (StreamBusPublishException)
        {
            // Negative confirm: the handle is healthy, the message was refused.
            // Keep the cached producer (no invalidate); the caller retries.
            throw;
        }
        catch (StreamBusConfigurationException)
        {
            throw;
        }
        catch (Exception ex) when (!IsCallerCancel(ex, ct))
        {
            await InvalidateAsync(key, entry, handle).ConfigureAwait(false);
            throw new StreamBusPublishTransientException($"Result publish to stream '{targetStream}' failed transiently; the cached stable producer was discarded (the same name is recreated and publishing id {sourceOffset} is safe to retry).", ex);
        }
    }

    public async Task<IStreamProducerHandle> GetOrCreateAsync(string targetStream, bool targetSuper, string producerName, CancellationToken ct)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(targetStream);
        ArgumentException.ThrowIfNullOrWhiteSpace(producerName);
        var key = Key(targetStream, targetSuper, producerName);
        // The factory must not capture the caller's ct (same rationale as the
        // unique-name registry). The 255-name cap is exact single-threaded and
        // best-effort under racing first-use; the broker enforces the real limit.
        var task = _handles.GetOrAdd(key, _ =>
        {
            if (NamesFor(targetStream, targetSuper) >= MaxProducerNamesPerStream)
                throw new StreamBusConfigurationException($"Effectively-once target '{targetStream}' already has {MaxProducerNamesPerStream} stable producer names (broker dedup limit); add a target stream instead of more source partitions.");
            return CreateAsync(targetStream, targetSuper, producerName, CancellationToken.None);
        });
        try
        {
            return await task.WaitAsync(ct).ConfigureAwait(false);
        }
        catch
        {
            if (task.IsFaulted || task.IsCanceled)
                ((ICollection<KeyValuePair<string, Task<IStreamProducerHandle>>>)_handles).Remove(new KeyValuePair<string, Task<IStreamProducerHandle>>(key, task));
            throw;
        }
    }

    public async Task<ulong?> GetResumeOffsetAsync(string targetStream, bool targetSuper, string producerName, CancellationToken ct = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(targetStream);
        ArgumentException.ThrowIfNullOrWhiteSpace(producerName);
        // Returns the last publishing id, or null when nothing was ever published.
        // Every doubt resolves to null (replay from first): reprocessing under a
        // stable (name, id) is absorbed by broker dedup, while an overstated resume
        // would skip (FR-6.5). A missing handle (startup race) also replays; the
        // attach loop backs off and re-resolves.
        var handle = _supervisor.CurrentHandle;
        if (handle is null) return null;
        try
        {
            if (!targetSuper)
                return await QueryOneAsync(handle, producerName, targetStream, ct).ConfigureAwait(false);
            string[] partitions;
            try
            {
                partitions = await handle.QueryPartitionAsync(targetStream, ct).ConfigureAwait(false);
            }
            catch (OperationCanceledException) when (ct.IsCancellationRequested)
            {
                throw;
            }
            catch (Exception ex)
            {
                _logger.LogDebug(ex, "Effectively-once resume partition query failed for target '{Target}'; resolving to first offset.", targetStream);
                return null;
            }
            var known = new List<ulong?>(partitions.Length);
            foreach (var partition in partitions)
            {
                var last = await QueryOneAsync(handle, producerName, partition, ct).ConfigureAwait(false);
                if (last is not null) known.Add(last);
            }
            return ResultResume.ResumeOffset(known);
        }
        catch (OperationCanceledException) when (ct.IsCancellationRequested)
        {
            throw;
        }
        catch (Exception ex)
        {
            _logger.LogDebug(ex, "Effectively-once resume query failed for producer '{Producer}' on target '{Target}'; resolving to first offset.", producerName, targetStream);
            return null;
        }
    }

    private async Task<ulong?> QueryOneAsync(IStreamSystemHandle handle, string producerName, string stream, CancellationToken ct)
    {
        try
        {
            var last = await handle.QuerySequenceAsync(producerName, stream, ct).ConfigureAwait(false);
            // Ruling R1: the broker returns 0 both for never-published and for
            // "only id 0 published" (Task 1 probe). Map 0 to unknown: the resume
            // then resolves to First (replay, dedup-absorbed) instead of
            // skipping offset 0. Never moves the resume later — safe direction.
            if (last == 0) return null;
            return last;
        }
        catch (OperationCanceledException) when (ct.IsCancellationRequested)
        {
            throw;
        }
        catch (Exception ex)
        {
            _logger.LogDebug(ex, "Effectively-once resume query found nothing for producer '{Producer}' on '{Stream}'; treating as never-published.", producerName, stream);
            return null;
        }
    }

    private static bool IsCallerCancel(Exception ex, CancellationToken ct)
        => ex is OperationCanceledException && ct.IsCancellationRequested;

    private static string Key(string target, bool super, string name) => super ? $"result:super:{target}:{name}" : $"result:plain:{target}:{name}";

    private int NamesFor(string target, bool super)
    {
        var prefix = super ? $"result:super:{target}:" : $"result:plain:{target}:";
        var count = 0;
        foreach (var key in _handles.Keys)
            if (key.StartsWith(prefix, StringComparison.Ordinal)) count++;
        return count;
    }

    private async Task InvalidateAsync(string key, Task<IStreamProducerHandle> expected, IStreamProducerHandle handleToDispose)
    {
        var removed = ((ICollection<KeyValuePair<string, Task<IStreamProducerHandle>>>)_handles)
            .Remove(new KeyValuePair<string, Task<IStreamProducerHandle>>(key, expected));
        if (!removed)
            return;
        try
        {
            await handleToDispose.DisposeAsync().ConfigureAwait(false);
        }
        catch
        {
        }
    }

    private async Task<IStreamProducerHandle> CreateAsync(string targetStream, bool targetSuper, string producerName, CancellationToken ct)
    {
        var handle = _supervisor.CurrentHandle
            ?? throw new StreamBusNotReadyException($"StreamBus for vhost '{_vhost}' is not ready; result publish to '{targetStream}' refused.");
        try
        {
            return targetSuper
                ? await handle.CreateSuperStreamProducerWithReferenceAsync(targetStream, producerName, ct).ConfigureAwait(false)
                : await handle.CreatePlainProducerWithReferenceAsync(targetStream, producerName, ct).ConfigureAwait(false);
        }
        catch (CreateProducerException ex) when (ex.ResponseCode == ResponseCode.StreamDoesNotExist)
        {
            throw new StreamBusStreamNotFoundException(targetStream, ex);
        }
        catch (CreateProducerException ex)
        {
            throw new StreamBusPublishException($"Could not create a result producer for stream '{targetStream}' (status {ex.ResponseCode}).", ex);
        }
    }
}
```

- [ ] **Step 4: Implement `ResultPublisher.cs`**

```csharp
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

namespace StreamBus;

internal sealed class ResultPublisher
{
    private readonly ResultProducerRegistry _results;
    private readonly ILogger<ResultPublisher> _logger;

    public ResultPublisher(ResultProducerRegistry results, ILogger<ResultPublisher>? logger = null)
    {
        ArgumentNullException.ThrowIfNull(results);
        _results = results;
        _logger = logger ?? NullLogger<ResultPublisher>.Instance;
    }

    public Task PublishOnceAsync(ResultWork work, CancellationToken ct)
    {
        ArgumentNullException.ThrowIfNull(work);
        return _results.PublishResultAsync(work.TargetStream, work.TargetSuper, work.Body, work.SourceOffset, work.RoutingKey, work.ProducerName, ct);
    }

    // Retry-forever (FR-6.5): a confirmed publish until it lands or ct fires.
    // There is deliberately no timeout and no silent skip. Injectable delay keeps
    // this unit-testable without a broker.
    public async Task PublishForeverAsync(ResultWork work, Func<int, TimeSpan> delayFor, CancellationToken ct)
    {
        ArgumentNullException.ThrowIfNull(work);
        ArgumentNullException.ThrowIfNull(delayFor);
        var attempt = 0;
        while (true)
        {
            ct.ThrowIfCancellationRequested();
            attempt++;
            try
            {
                await PublishOnceAsync(work, ct).ConfigureAwait(false);
                return;
            }
            catch (OperationCanceledException) when (ct.IsCancellationRequested)
            {
                throw;
            }
            catch (Exception ex)
            {
                if (RetryDelays.ShouldLogRetryWarning(attempt))
                    _logger.LogWarning(ex,
                        "Effectively-once publish failed for target {Target} source offset {Offset} (attempt {Attempt}); retrying without advancing.",
                        work.TargetStream, work.SourceOffset, attempt);
                else
                    _logger.LogDebug(ex,
                        "Effectively-once publish failed for target {Target} source offset {Offset} (attempt {Attempt}); retrying without advancing.",
                        work.TargetStream, work.SourceOffset, attempt);
                TimeSpan delay;
                try
                {
                    delay = delayFor(attempt);
                }
                catch (Exception dex)
                {
                    _logger.LogWarning(dex, "Effectively-once delay function threw for attempt {Attempt}; using zero delay.", attempt);
                    delay = TimeSpan.Zero;
                }
                if (delay < TimeSpan.Zero) delay = TimeSpan.Zero;
                await Task.Delay(delay, ct).ConfigureAwait(false);
            }
        }
    }
}
```

- [ ] **Step 5: Update every existing test stub**

Grep `IStreamSystemHandle` and `IStreamProducerHandle` under `tests/`; add to every implementer:
- `CreateSuperStreamProducerWithReferenceAsync` → `throw new NotImplementedException("<TestClass> never creates stable super producers.")` (with the actual class name).
- `QuerySequenceAsync` → `throw new NotImplementedException("<TestClass> never queries publishing sequences.")`.
- Fake `IStreamProducerHandle.PublishResultAsync` → `throw new NotImplementedException("<TestClass> never publishes results.")`.
(Task 5's fakes record instead; these stubs only need to compile here.)

- [ ] **Step 6: Verify**

```powershell
dotnet build src/StreamBus/StreamBus.csproj -c Release
dotnet test tests/StreamBus.UnitTests/StreamBus.UnitTests.csproj -c Release
```

Expected: build clean; existing suite green (interface additions are additive; `PublishAsync`/`PublishEnvelopeAsync` untouched).

- [ ] **Step 7: Commit (with human approval)**

```powershell
git add src/StreamBus/ProducerHandles.cs src/StreamBus/Connector.cs src/StreamBus/ResultProducerRegistry.cs src/StreamBus/ResultPublisher.cs tests/
git commit -m "feat: add stable result transport with last-id resume queries"
```

---

### Task 3: EO dispatch branch + managed consume wiring + validation

**Files:**
- Modify: `src/StreamBus/ConsumeOptions.cs` (add `internal EffectivelyOnceConfig? EffectivelyOnce`)
- Modify: `src/StreamBus/StreamPartitionConsumer.cs` (EO fields/ctor, `QueryStoredAsync`, EO dispatch branch, store/timer guards)
- Modify: `src/StreamBus/ConsumerRegistry.cs` (5th ctor param `ResultProducerRegistry results`, `ConsumeEffectivelyOnceAsync`, `PublishResultAsync`)
- Modify: `src/StreamBus/Registry.cs` (`ResultProducersFor` + pass into `ConsumersFor`)
- Modify: every `new StreamConsumerRegistry(` under `tests/` (grep first; pass a `new ResultProducerRegistry(supervisor, vhost)` built from the test's existing supervisor).

**Interfaces:**
- Consumes: `EffectivelyOnceConfig`/`EffectivelyOnceOptions`/`EffectivelyOnceValidation`/`ResultNaming` (Task 1), `ResultProducerRegistry`/`ResultPublisher` (Task 2), `OffsetResolution`/`MonotonicOffsetGuard`/`PartitionState`/`StreamBusMetrics`/`RetryDelays` (Phase 5/6).
- Produces: EO dispatch with exact no-store points, validated `ConsumeEffectivelyOnceAsync`, registry `PublishResultAsync` for Task 4.

- [ ] **Step 1: Extend `ConsumeOptions.cs`**

Add (internal — only `ConsumeEffectivelyOnceAsync` sets it; a `ConsumeAsync` handler returns no result, so exposing it publicly would be a type-level mismatch):

```csharp
internal EffectivelyOnceConfig? EffectivelyOnce { get; init; }
```

- [ ] **Step 2: Branch `StreamPartitionConsumer`**

Fields (next to `_errors`):

```csharp
private readonly EffectivelyOnceConfig? _eo;
private readonly ResultProducerRegistry? _results;
```

Constructor: append AFTER `logger` (existing positional callers stay valid):

```csharp
ErrorPublisher errorPublisher,
ILogger<StreamPartitionConsumer>? logger = null,
EffectivelyOnceConfig? effectivelyOnce = null,
ResultProducerRegistry? results = null)
```

Body additions (with the other null-checks/assigns):

```csharp
if (effectivelyOnce is not null && results is null)
    throw new ArgumentException("Effectively-once requires a result producer registry.", nameof(results));
if (effectivelyOnce is null && results is not null)
    throw new ArgumentException("A result producer registry without effectively-once config is inconsistent.", nameof(effectivelyOnce));
_eo = effectivelyOnce;
_results = results;
```

New helper (place next to `OnPromotedCoreAsync`):

```csharp
// Effectively-once resume (FR-6.7/FR-10.14): the source stores nothing, so the
// resume point is the target's last publishing id (the shared stored+1 rule
// turns it into lastId+1) on every attach — create, promote, reconnect alike.
private Task<ulong?> QueryStoredAsync(CancellationToken ct)
    => _eo is null
        ? _system.TryQueryOffsetAsync(_reference, _partitionStream, ct)
        : _eo.ResumeQuery(_eo.TargetStream, _eo.TargetIsSuperStream, ResultNaming.ProducerName(_partitionStream, _eo.TargetStream), ct);
```

Replace the two resolve sites:
- RunAsync: `() => _system.TryQueryOffsetAsync(_reference, _partitionStream, loopCt),` → `() => QueryStoredAsync(loopCt),`
- `OnPromotedCoreAsync`: `() => _system.TryQueryOffsetAsync(_reference, _partitionStream, CancellationToken.None),` → `() => QueryStoredAsync(CancellationToken.None),`

Dispatch branch — old:

```csharp
            Interlocked.Increment(ref _state.Inflight);
            try
            {
                // In-handler retry (FR-4.1): the attempt loop runs under the already-held
```

new (insert the EO branch as the first statement inside that `try`):

```csharp
            Interlocked.Increment(ref _state.Inflight);
            try
            {
                if (_eo is not null)
                {
                    await DispatchEffectivelyOnceAsync(offset, consumeContext, attach).ConfigureAwait(false);
                    return;
                }
                // In-handler retry (FR-4.1): the attempt loop runs under the already-held
```

New method (place after `ExhaustAsync`):

```csharp
// Effectively-once dispatch (FR-6): Strict-unbounded compute retry (EO
// validation forces FailureMode.Strict + null MaxAttempts, so this loop never
// exhausts), then publish-forever with id = source offset (FR-6.1/FR-6.5),
// then return WITHOUT storing (FR-10.14 — the publishing id is the resume
// point). Runs under the dispatch gate, so one partition publishes under one
// stable name sequentially (FR-6.3). A single finally owns the inflight
// decrement + retry_blocked reset for every exit path below.
private async Task DispatchEffectivelyOnceAsync(ulong offset, ConsumeContext consumeContext, AttachState attach)
{
    try
    {
        ReadOnlyMemory<byte> result;
        var attempts = 0;
        while (true)
        {
            attempts++;
            try
            {
                result = await _eo!.ResultHandler(consumeContext).ConfigureAwait(false);
                break;
            }
            catch (OperationCanceledException) when (attach.LoopToken.IsCancellationRequested)
            {
                return;
            }
            catch (Exception ex)
            {
                if (RetryDelays.ShouldLogRetryWarning(attempts))
                    _logger.LogWarning(ex,
                        "Effectively-once handler failed for stream {Stream} partition {Partition} offset {Offset} (attempt {Attempt}); retrying in-handler.",
                        _stream, _partitionIndex, offset, attempts);
                else
                    _logger.LogDebug(ex,
                        "Effectively-once handler failed for stream {Stream} partition {Partition} offset {Offset} (attempt {Attempt}); retrying in-handler.",
                        _stream, _partitionIndex, offset, attempts);
                Interlocked.Exchange(ref _state.RetryBlocked, 1);
                var delay = RetryDelays.ResolveDelay(_options.Retry, attempts, ex, _logger);
                try
                {
                    await Task.Delay(delay, attach.LoopToken).ConfigureAwait(false);
                }
                catch (OperationCanceledException) when (attach.LoopToken.IsCancellationRequested)
                {
                    return;
                }
            }
        }
        Interlocked.Exchange(ref _state.RetryBlocked, 0);
        // A throwing TargetRouting propagates to the dispatch safety net (attach
        // recycle + redeliver, never a skip).
        var routing = _eo!.TargetRouting(consumeContext);
        if (_eo.TargetIsSuperStream && routing is null)
        {
            FailAttach(attach, offset,
                new StreamBusConfigurationException(
                    $"Effectively-once target routing resolved null for super target '{_eo.TargetStream}' (source offset {offset}); supply EffectivelyOnceOptions.TargetRouting or a routed source."),
                "Effectively-once target routing missing");
            return;
        }
        var producerName = ResultNaming.ProducerName(_partitionStream, _eo.TargetStream);
        var publishAttempts = 0;
        while (true)
        {
            attach.LoopToken.ThrowIfCancellationRequested();
            publishAttempts++;
            try
            {
                await _results!.PublishResultAsync(_eo.TargetStream, _eo.TargetIsSuperStream, result, offset, routing, producerName, attach.LoopToken).ConfigureAwait(false);
                return;
            }
            catch (OperationCanceledException) when (attach.LoopToken.IsCancellationRequested)
            {
                return;
            }
            catch (Exception ex)
            {
                if (RetryDelays.ShouldLogRetryWarning(publishAttempts))
                    _logger.LogWarning(ex,
                        "Effectively-once publish failed for target {Target} source offset {Offset} (attempt {Attempt}); retrying without advancing.",
                        _eo.TargetStream, offset, publishAttempts);
                else
                    _logger.LogDebug(ex,
                        "Effectively-once publish failed for target {Target} source offset {Offset} (attempt {Attempt}); retrying without advancing.",
                        _eo.TargetStream, offset, publishAttempts);
                var delay = RetryDelays.ResolveDelay(_options.Retry, publishAttempts, ex, _logger);
                try
                {
                    await Task.Delay(delay, attach.LoopToken).ConfigureAwait(false);
                }
                catch (OperationCanceledException) when (attach.LoopToken.IsCancellationRequested)
                {
                    return;
                }
            }
        }
    }
    finally
    {
        Interlocked.Decrement(ref _state.Inflight);
        Interlocked.Exchange(ref _state.RetryBlocked, 0);
    }
}
```

Store/timer guards — old:

```csharp
            if (_storer.ShouldStoreOnSuccess(offset))
```

new:

```csharp
            if (_eo is null && _storer.ShouldStoreOnSuccess(offset))
```

old:

```csharp
                if (_options.OffsetStore.MaxInterval is { } interval && interval > TimeSpan.Zero)
```

new:

```csharp
                if (_eo is null && _options.OffsetStore.MaxInterval is { } interval && interval > TimeSpan.Zero)
```

(Validation in `ConsumeEffectivelyOnceAsync` already forces default `OffsetStore`; the guards are belt-and-suspenders so a future caller cannot silently store. The regression read-back lives only in the store path, so EO never triggers it.)

- [ ] **Step 3: `ConsumerRegistry` wiring**

Ctor gains a required trailing parameter (legal after the optional `loggerFactory`, and it forces every call site to be updated explicitly):

```csharp
public StreamConsumerRegistry(StreamBusConnectionSupervisor supervisor, string vhost, StreamProducerRegistry producers, ILoggerFactory? loggerFactory, ResultProducerRegistry results)
```

with `ArgumentNullException.ThrowIfNull(results)` and `_results = results;`.

`ConsumeEffectivelyOnceAsync` (place after `ConsumeAsync`):

```csharp
public async Task<IStreamConsumer> ConsumeEffectivelyOnceAsync(
    Type busType, string sourceStream, string targetStream, EffectivelyOnceOptions options,
    Func<ConsumeContext, Task<ReadOnlyMemory<byte>>> handler, CancellationToken ct = default)
{
    ArgumentNullException.ThrowIfNull(busType);
    ArgumentException.ThrowIfNullOrWhiteSpace(sourceStream);
    ArgumentException.ThrowIfNullOrWhiteSpace(targetStream);
    ArgumentNullException.ThrowIfNull(options);
    ArgumentNullException.ThrowIfNull(handler);
    ArgumentNullException.ThrowIfNull(options.Retry);
    ArgumentNullException.ThrowIfNull(options.ReadinessPolicy);
    EffectivelyOnceValidation.ValidateConsume(options.Retry);
    var reference = options.Reference ?? ResultNaming.ConsumerReference(busType, sourceStream, targetStream);
    ReferenceDerivation.Validate(reference);
    if (options.Reference is not null) ReferenceDerivation.Validate(options.Reference);

    var readiness = await _supervisor.WaitUntilReadyAsync(options.ReadinessPolicy, ct).ConfigureAwait(false);
    if (readiness == BusReadiness.Fatal)
    {
        throw new StreamBusFatalException($"StreamBus for vhost '{_vhost}' is in Fatal state; effectively-once consume of '{sourceStream}' refused.");
    }
    if (readiness != BusReadiness.Ready)
    {
        throw new StreamBusNotReadyException($"StreamBus for vhost '{_vhost}' is not ready ({readiness}); effectively-once consume of '{sourceStream}' refused.");
    }
    var handle = _supervisor.CurrentHandle
        ?? throw new StreamBusNotReadyException($"StreamBus for vhost '{_vhost}' is not ready; effectively-once consume of '{sourceStream}' refused.");

    string[] sourcePartitions;
    if (options.SourceIsSuperStream)
    {
        if (!await handle.SuperStreamExistsAsync(sourceStream, ct).ConfigureAwait(false))
            throw new StreamBusStreamNotFoundException(sourceStream);
        sourcePartitions = await handle.QueryPartitionAsync(sourceStream, ct).ConfigureAwait(false);
    }
    else
    {
        if (await handle.SuperStreamExistsAsync(sourceStream, ct).ConfigureAwait(false))
            throw new StreamBusConfigurationException($"Effectively-once source '{sourceStream}' is a super stream but SourceIsSuperStream is false.");
        if (!await handle.StreamExistsAsync(sourceStream, ct).ConfigureAwait(false))
            throw new StreamBusStreamNotFoundException(sourceStream);
        sourcePartitions = new[] { sourceStream };
    }
    if (options.TargetIsSuperStream)
    {
        if (!await handle.SuperStreamExistsAsync(targetStream, ct).ConfigureAwait(false))
            throw new StreamBusStreamNotFoundException(targetStream);
    }
    else
    {
        if (await handle.SuperStreamExistsAsync(targetStream, ct).ConfigureAwait(false))
            throw new StreamBusConfigurationException($"Effectively-once target '{targetStream}' is a super stream but TargetIsSuperStream is false.");
        if (!await handle.StreamExistsAsync(targetStream, ct).ConfigureAwait(false))
            throw new StreamBusStreamNotFoundException(targetStream);
    }

    var busName = busType.FullName ?? busType.Name;
    var family = new CancellationTokenSource();
    var errors = new ErrorPublisher(_producers, _loggerFactory.CreateLogger<ErrorPublisher>());
    var targetRouting = options.TargetRouting ?? (static ctx => ctx.RoutingValue);
    var loops = new List<(StreamPartitionConsumer Loop, Task RunTask)>(sourcePartitions.Length);
    var scopes = new List<IDisposable>(sourcePartitions.Length);
    try
    {
        for (var index = 0; index < sourcePartitions.Length; index++)
        {
            var partitionStream = sourcePartitions[index];
            var state = new PartitionState();
            scopes.Add(StreamBusMetrics.Shared.TrackPartition(_vhost, busName, sourceStream, index, reference, state));
            var consumeOptions = new ConsumeOptions
            {
                IsSuperStream = options.SourceIsSuperStream,
                Reference = reference,
                SingleActiveConsumer = true,
                Offset = ConsumeOffset.ResumeOrFirst,
                Retry = options.Retry,
                ReadinessPolicy = options.ReadinessPolicy,
                EffectivelyOnce = new EffectivelyOnceConfig
                {
                    TargetStream = targetStream,
                    TargetIsSuperStream = options.TargetIsSuperStream,
                    TargetRouting = targetRouting,
                    ResultHandler = handler,
                    ResumeQuery = _results.GetResumeOffsetAsync,
                },
            };
            var loop = new StreamPartitionConsumer(
                handle, sourceStream, partitionStream, index, reference, consumeOptions,
                static _ => Task.CompletedTask, // unreached in EO mode (the branch above calls ResultHandler)
                _guard, state, StreamBusMetrics.Shared, _vhost, busName, errors,
                _loggerFactory.CreateLogger<StreamPartitionConsumer>(), consumeOptions.EffectivelyOnce, _results);
            loops.Add((loop, loop.RunAsync(family.Token)));
        }
        // Same attach contract as ConsumeAsync: every partition's first broker
        // subscription is confirmed before the call returns.
        await Task.WhenAll(loops.Select(l => l.Loop.FirstAttachAsync)).WaitAsync(ct).ConfigureAwait(false);
    }
    catch
    {
        family.Cancel();
        try
        {
            await Task.WhenAll(loops.Select(l => l.RunTask)).ConfigureAwait(false);
        }
        catch (Exception)
        {
        }
        foreach (var (loop, _) in loops)
        {
            await loop.DisposeAsync().ConfigureAwait(false);
        }
        foreach (var scope in scopes)
        {
            scope.Dispose();
        }
        family.Dispose();
        throw;
    }

    var registration = new StreamConsumerRegistration(sourceStream, reference, loops, scopes, family, Remove);
    lock (_gate)
    {
        _live.Add(registration);
    }
    return registration;
}
```

(`static _ => Task.CompletedTask` matches `Func<ConsumeContext, Task>`; the discard names the context. `static ctx => ctx.RoutingValue` matches `Func<ConsumeContext, string?>`.)

Registry `PublishResultAsync` (place after `ReplayAsync`):

```csharp
public async Task PublishResultAsync(string targetStream, ReadOnlyMemory<byte> result, ulong sourceOffset, PublishResultOptions? options, CancellationToken ct = default)
{
    ArgumentException.ThrowIfNullOrWhiteSpace(targetStream);
    ArgumentNullException.ThrowIfNull(options);
    EffectivelyOnceValidation.ValidatePublishResult(options);
    await EnsureReadyAsync(targetStream, ct).ConfigureAwait(false);
    var handle = _supervisor.CurrentHandle
        ?? throw new StreamBusNotReadyException($"StreamBus for vhost '{_vhost}' is not ready; result publish to '{targetStream}' refused.");
    if (options.IsSuperStream)
    {
        if (!await handle.SuperStreamExistsAsync(targetStream, ct).ConfigureAwait(false))
            throw new StreamBusStreamNotFoundException(targetStream);
    }
    else
    {
        if (await handle.SuperStreamExistsAsync(targetStream, ct).ConfigureAwait(false))
            throw new StreamBusConfigurationException($"Result target '{targetStream}' is a super stream but PublishResultOptions.IsSuperStream is false.");
        if (!await handle.StreamExistsAsync(targetStream, ct).ConfigureAwait(false))
            throw new StreamBusStreamNotFoundException(targetStream);
    }
    var publisher = new ResultPublisher(_results, _loggerFactory.CreateLogger<ResultPublisher>());
    await publisher.PublishForeverAsync(
        new ResultWork
        {
            TargetStream = targetStream,
            TargetSuper = options.IsSuperStream,
            Body = result,
            SourceOffset = sourceOffset,
            RoutingKey = options.RoutingKey,
            ProducerName = options.ProducerName,
        },
        attempt => StreamBusDelays.ExponentialCapped(attempt, options.BaseDelay, options.MaxDelay, options.Multiplier, options.MaxJitter),
        ct).ConfigureAwait(false);
}
```

(`EnsureReadyAsync` is the existing FailFast helper. `StreamBusDelays` is in-namespace; add no usings. `System.Text` for the `Encoding` in validation lives in `EffectivelyOnce.cs` itself.)

- [ ] **Step 4: `Registry.cs` wiring**

```csharp
private readonly ConcurrentDictionary<string, ResultProducerRegistry> _results = new();

internal ResultProducerRegistry ResultProducersFor(string vhost)
    => _results.GetOrAdd(vhost, _ => new ResultProducerRegistry(SupervisorForCore(vhost), vhost));

internal StreamConsumerRegistry ConsumersFor(string vhost)
    => _consumers.GetOrAdd(vhost, _ => new StreamConsumerRegistry(SupervisorForCore(vhost), vhost, ProducersFor(vhost), _loggerFactory, ResultProducersFor(vhost)));
```

(replacing the existing `ConsumersFor` line verbatim.)

- [ ] **Step 5: Verify**

```powershell
dotnet build src/StreamBus/StreamBus.csproj -c Release
dotnet test tests/StreamBus.UnitTests/StreamBus.UnitTests.csproj -c Release
```

Expected: build clean; existing suite green — the default `EffectivelyOnce = null` keeps every existing consumer construction on the Phase 6 path. Fix every `new StreamConsumerRegistry(` under `tests/` found by grep (pass the test's supervisor into `new ResultProducerRegistry(...)`).

- [ ] **Step 6: Commit (with human approval)**

```powershell
git add src/StreamBus/ConsumeOptions.cs src/StreamBus/StreamPartitionConsumer.cs src/StreamBus/ConsumerRegistry.cs src/StreamBus/Registry.cs tests/
git commit -m "feat: add effectively-once dispatch with no-store resume-from-target"
```

---

### Task 4: Bus surface

**Files:**
- Modify: `src/StreamBus/Bus.cs` (`PublishResultAsync` + `ConsumeEffectivelyOnceAsync` on the interface + both implementations)

**Interfaces:**
- Consumes: `StreamConsumerRegistry.ConsumeEffectivelyOnceAsync` + `PublishResultAsync` (Task 3), `EffectivelyOnceOptions`/`PublishResultOptions` (Task 1).
- Produces: public `IStreamBus` result surface for Tasks 5–6.

- [ ] **Step 1: Extend `Bus.cs`**

Interface additions (`IStreamBus`; `IStreamBus<TBus>` inherits them, no redeclare):

```csharp
/// <summary>
/// Publishes a processing result with publishing-id = <paramref name="sourceOffset"/>
/// under <c>options.ProducerName</c>, retrying until confirmed or cancelled. There is
/// deliberately no confirm timeout: a result is never skipped silently (FR-6.5).
/// Concurrent use of one producer name is forbidden (FR-6.3) — the managed
/// <see cref="ConsumeEffectivelyOnceAsync"/> path serializes per name; manual
/// callers must do the same. A manual publish combined with a storing consumer
/// is at-least-once; effectively-once holds for the managed cycle only.
/// </summary>
Task PublishResultAsync(string targetStream, ReadOnlyMemory<byte> result, ulong sourceOffset, PublishResultOptions? options = null, CancellationToken ct = default);
/// <summary>
/// Consumes <paramref name="sourceStream"/> without storing source offsets
/// (FR-10.14) and publishes each handler result to <paramref name="targetStream"/>
/// with publishing-id = source offset. The resume point is recomputed from the
/// target's last publishing id on every attach; operator <c>SetConsumerOffset</c>
/// calls on this reference do not affect the resume.
/// </summary>
Task<IStreamConsumer> ConsumeEffectivelyOnceAsync(string sourceStream, string targetStream, EffectivelyOnceOptions options, Func<ConsumeContext, Task<ReadOnlyMemory<byte>>> handler, CancellationToken ct = default);
```

Both implementations delegate (`StreamBus` shown; `StreamBus<TBus>` identical with `typeof(TBus)`):

```csharp
public Task PublishResultAsync(string targetStream, ReadOnlyMemory<byte> result, ulong sourceOffset, PublishResultOptions? options = null, CancellationToken ct = default)
    => _consumers.PublishResultAsync(targetStream, result, sourceOffset, options, ct);
public Task<IStreamConsumer> ConsumeEffectivelyOnceAsync(string sourceStream, string targetStream, EffectivelyOnceOptions options, Func<ConsumeContext, Task<ReadOnlyMemory<byte>>> handler, CancellationToken ct = default)
    => _consumers.ConsumeEffectivelyOnceAsync(typeof(DefaultBus), sourceStream, targetStream, options, handler, ct);
```

(`PublishResultOptions? options = null` with a `required ProducerName` forces callers to pass options explicitly — there is no meaningless default. The registry null-checks.)

- [ ] **Step 2: Verify**

```powershell
dotnet build src/StreamBus/StreamBus.csproj -c Release
dotnet test tests/StreamBus.UnitTests/StreamBus.UnitTests.csproj -c Release
grep -r "ReliableProducer\|ReliableConsumer\|ReliableSuperStream" src/ --include="*.cs" | wc -l
grep -r "CreateStream\|CreateSuperStream\|DeleteStream\|DeleteSuperStream" src/ --include="*.cs" | wc -l
```

Expected: build clean, suite green, both greps print `0`.

- [ ] **Step 3: Commit (with human approval)**

```powershell
git add src/StreamBus/Bus.cs
git commit -m "feat: expose publish-result and effectively-once consume on the bus"
```

---

### Task 5: Unit tests — naming, validation, resume, orchestration, cap, collision

**Files:**
- Create: `tests/StreamBus.UnitTests/EffectivelyOnceTests.cs`
- Create: `tests/StreamBus.UnitTests/ResultPublisherTests.cs`

**Interfaces:**
- Consumes: everything from Tasks 1–2 via `InternalsVisibleTo` (assert: `AssemblyInfo.cs` already lists both test assemblies).
- Produces: pinned contracts for Task 6 (names, first-run branch, forever-retry, 255 cap).

Style rules (analyzers are build errors): no `.ConfigureAwait(false)` inside `[Fact]`s; use `TestContext.Current.CancellationToken` explicitly; `Assert.Single(values, pred)` overload where applicable.

- [ ] **Step 1: Implement `EffectivelyOnceTests.cs`**

```csharp
namespace StreamBus.UnitTests;

public sealed class EffectivelyOnceTests
{
    [Fact]
    public void Producer_name_is_stable_and_meaningful()
    {
        var a = ResultNaming.ProducerName("orders-0", "orders.done");
        Assert.Equal(a, ResultNaming.ProducerName("orders-0", "orders.done"));
        Assert.Contains("orders-0", a);
        Assert.Contains("orders.done", a);
    }

    [Fact]
    public void Producer_name_differs_per_partition_and_target()
    {
        Assert.NotEqual(ResultNaming.ProducerName("s-0", "t"), ResultNaming.ProducerName("s-1", "t"));
        Assert.NotEqual(ResultNaming.ProducerName("s-0", "t1"), ResultNaming.ProducerName("s-0", "t2"));
    }

    [Fact]
    public void Producer_name_never_exceeds_255_bytes()
    {
        var longStream = new string('x', 300);
        var name = ResultNaming.ProducerName(longStream, longStream);
        Assert.True(System.Text.Encoding.UTF8.GetByteCount(name) <= 255);
        Assert.StartsWith("streambus-eo-", name);
        Assert.Equal(name, ResultNaming.ProducerName(longStream, longStream));
    }

    [Fact]
    public void Consumer_reference_is_stable_per_group()
    {
        var a = ResultNaming.ConsumerReference(typeof(DefaultBus), "src", "dst");
        Assert.Equal(a, ResultNaming.ConsumerReference(typeof(DefaultBus), "src", "dst"));
        Assert.NotEqual(a, ResultNaming.ConsumerReference(typeof(DefaultBus), "src", "other"));
    }

    [Fact]
    public void Publishing_id_equals_source_offset() => Assert.Equal(41UL, ResultNaming.PublishingId(41));

    [Fact]
    public void Consume_rejects_skip_mode()
    {
        var ex = Assert.Throws<StreamBusConfigurationException>(() =>
            EffectivelyOnceValidation.ValidateConsume(new RetryPolicy { FailureMode = FailureMode.SkipAfterDeadline, MaxAttempts = 3 }));
        Assert.Contains("Strict", ex.Message);
    }

    [Fact]
    public void Consume_rejects_bounded_strict()
    {
        Assert.Throws<StreamBusConfigurationException>(() =>
            EffectivelyOnceValidation.ValidateConsume(new RetryPolicy { FailureMode = FailureMode.Strict, MaxAttempts = 3 }));
    }

    [Fact]
    public void Consume_accepts_default_strict_unbounded()
    {
        EffectivelyOnceValidation.ValidateConsume(new RetryPolicy());
    }

    [Fact]
    public void Consume_rejects_non_positive_base_delay()
    {
        var ex = Assert.Throws<StreamBusConfigurationException>(() =>
            EffectivelyOnceValidation.ValidateConsume(new RetryPolicy { BaseDelay = TimeSpan.Zero }));
        Assert.Contains("BaseDelay", ex.Message);
    }

    [Fact]
    public void Publish_result_rejects_missing_name()
    {
        Assert.ThrowsAny<Exception>(() =>
            EffectivelyOnceValidation.ValidatePublishResult(new PublishResultOptions { ProducerName = "  " }));
    }

    [Fact]
    public void Publish_result_rejects_overlong_name()
    {
        Assert.Throws<StreamBusConfigurationException>(() =>
            EffectivelyOnceValidation.ValidatePublishResult(new PublishResultOptions { ProducerName = new string('y', 300) }));
    }

    [Fact]
    public void Publish_result_rejects_super_without_routing()
    {
        var ex = Assert.Throws<StreamBusConfigurationException>(() =>
            EffectivelyOnceValidation.ValidatePublishResult(new PublishResultOptions { ProducerName = "n", IsSuperStream = true }));
        Assert.Contains("RoutingKey", ex.Message);
    }

    [Fact]
    public void Resume_is_max_last_id()
    {
        Assert.Equal(7UL, ResultResume.ResumeOffset(new ulong?[] { 3, 7, 5 }));
    }

    [Fact]
    public void Resume_skips_unknown_partitions()
    {
        Assert.Equal(3UL, ResultResume.ResumeOffset(new ulong?[] { null, 3 }));
    }

    [Fact]
    public void Resume_is_null_when_nothing_published()
    {
        Assert.Null(ResultResume.ResumeOffset(Array.Empty<ulong?>()));
        Assert.Null(ResultResume.ResumeOffset(new ulong?[] { null }));
    }

    private sealed class StubHandle : IStreamProducerHandle
    {
        public readonly List<(ulong Id, string? Routing)> Published = new();
        public Task SendAsync(ReadOnlyMemory<byte> body, string? routingKey, CancellationToken ct) => throw new NotImplementedException("EO tests publish results only.");
        public Task SendEnvelopeAsync(ReadOnlyMemory<byte> body, string? routingKey, IReadOnlyDictionary<string, object?> app, string? mid, string? cid, CancellationToken ct) => throw new NotImplementedException("EO tests publish results only.");
        public Task PublishReplayAsync(ReadOnlyMemory<byte> body, ulong publishingId, CancellationToken ct) => throw new NotImplementedException("EO tests publish results only.");
        public Task PublishResultAsync(ReadOnlyMemory<byte> body, ulong publishingId, string? routingKey, CancellationToken ct)
        {
            lock (Published) Published.Add((publishingId, routingKey));
            return Task.CompletedTask;
        }
        public ValueTask DisposeAsync() => ValueTask.CompletedTask;
    }

    private sealed class StubSystem : IStreamSystemHandle
    {
        public readonly Dictionary<string, StubHandle> Created = new();
        public readonly Dictionary<string, ulong> Sequences = new();
        public string[] Partitions = new[] { "p" };
        public bool IsClosed => false;
        public Task<IStreamProducerHandle> CreatePlainProducerAsync(string stream, CancellationToken ct) => throw new NotImplementedException("EO tests use stable names.");
        public Task<IStreamProducerHandle> CreatePlainProducerWithReferenceAsync(string stream, string reference, CancellationToken ct)
        {
            var h = new StubHandle();
            Created[$"plain:{stream}:{reference}"] = h;
            return Task.FromResult<IStreamProducerHandle>(h);
        }
        public Task<IStreamProducerHandle> CreateSuperStreamProducerAsync(string stream, CancellationToken ct) => throw new NotImplementedException("EO tests use stable names.");
        public Task<IStreamProducerHandle> CreateSuperStreamProducerWithReferenceAsync(string stream, string reference, CancellationToken ct)
        {
            var h = new StubHandle();
            Created[$"super:{stream}:{reference}"] = h;
            return Task.FromResult<IStreamProducerHandle>(h);
        }
        public Task<ulong?> TryQueryOffsetAsync(string reference, string stream, CancellationToken ct) => Task.FromResult<ulong?>(null);
        public Task StoreSystemOffsetAsync(string reference, string stream, ulong offset, CancellationToken ct) => Task.CompletedTask;
        public Task<bool> StreamExistsAsync(string stream, CancellationToken ct) => Task.FromResult(true);
        public Task<bool> SuperStreamExistsAsync(string superStream, CancellationToken ct) => Task.FromResult(false);
        public Task<string[]> QueryPartitionAsync(string superStream, CancellationToken ct) => Task.FromResult(Partitions);
        public Task<ulong> QuerySequenceAsync(string reference, string stream, CancellationToken ct)
            => Sequences.TryGetValue($"{reference}@{stream}", out var v)
                ? Task.FromResult(v)
                : Task.FromException<ulong>(new InvalidOperationException($"no sequence for {reference}@{stream}"));
        public Task<RabbitMQ.Stream.Client.RawConsumer> CreateRawConsumerAsync(RabbitMQ.Stream.Client.RawConsumerConfig config, CancellationToken ct) => throw new NotImplementedException("EO unit tests never attach.");
        public ValueTask DisposeAsync() => ValueTask.CompletedTask;
    }

    private static (ResultProducerRegistry Registry, StubSystem System) CreateRegistry()
    {
        var system = new StubSystem();
        var options = new StreamBusOptions { Vhost = "/", UserName = "app", Password = "secret" };
        var supervisor = new StreamBusConnectionSupervisor(options, new ReadyConnector(system));
        supervisor.StartAsync(CancellationToken.None).GetAwaiter().GetResult();
        var readiness = supervisor.WaitUntilReadyAsync(ReadinessPolicy.Indefinitely, new CancellationTokenSource(TimeSpan.FromSeconds(5)).Token).GetAwaiter().GetResult();
        Assert.Equal(BusReadiness.Ready, readiness);
        return (new ResultProducerRegistry(supervisor, "/"), system);
    }

    private sealed class ReadyConnector : IStreamSystemConnector
    {
        private readonly StubSystem _handle;
        public ReadyConnector(StubSystem handle) => _handle = handle;
        public Task<IStreamSystemHandle> ConnectAsync(StreamBusOptions options, CancellationToken ct) => Task.FromResult<IStreamSystemHandle>(_handle);
    }

    [Fact]
    public async Task Stable_producer_is_reused_and_carries_source_offset_as_id()
    {
        var (registry, system) = CreateRegistry();
        var ct = TestContext.Current.CancellationToken;
        await registry.PublishResultAsync("dst", new byte[] { 1 }, 7, null, "grp", ct);
        await registry.PublishResultAsync("dst", new byte[] { 2 }, 8, null, "grp", ct);
        var handle = Assert.Single(system.Created.Values);
        Assert.Equal(new[] { 7UL, 8UL }, handle.Published.Select(p => p.Id).ToList());
    }

    [Fact]
    public async Task Duplicate_in_flight_id_fails_loudly()
    {
        // FR-6.3 forbids concurrent publishing under one name. The managed path is
        // sequential by construction (dispatch gate + SAC); this pins the loud
        // failure for manual misuse at the level where it lives: the pending-id
        // table in ConfirmationTracker rejects the second same-id publish.
        // First probe the exact IProducer surface (property types are unknown),
        // then implement every member (blocking Send(ulong, Message),
        // NotImplementedException elsewhere):
        //
        // ```powershell
        // $dll = Get-ChildItem "$env:USERPROFILE/.nuget/packages/rabbitmq.stream.client/1.12.1/lib" -Recurse -Filter RabbitMQ.Stream.Client.dll | Select-Object -First 1 -ExpandProperty FullName
        // $asm = [Reflection.Assembly]::LoadFrom($dll)
        // $asm.GetType("RabbitMQ.Stream.Client.IProducer").GetMethods() | Select-Object Name
        // $asm.GetType("RabbitMQ.Stream.Client.IProducer").GetProperties() | Select-Object Name, PropertyType
        // ```
        var tracker = new ConfirmationTracker();
        var gate = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var producer = new GateProducer(gate.Task);
        using var timeout = new CancellationTokenSource();
        var first = tracker.PublishWithIdAsync(producer, "dst", 5, new RabbitMQ.Stream.Client.Message(new byte[] { 1 }), timeout.Token);
        await Task.Delay(200, TestContext.Current.CancellationToken);
        var ex = await Assert.ThrowsAsync<StreamBusPublishException>(() =>
            tracker.PublishWithIdAsync(producer, "dst", 5, new RabbitMQ.Stream.Client.Message(new byte[] { 2 }), timeout.Token));
        Assert.Contains("already in flight", ex.Message);
        gate.TrySetResult();
        // Nothing confirms the first publish (no ConfirmHandler wired in the
        // fake), so cancel its token: the tracker's ct-registration fires and
        // the await propagates OperationCanceledException instead of hanging.
        timeout.Cancel();
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => first);
    }
```

    `GateProducer` (exact members per the probe above; only `Send(ulong, Message)` does work):

```csharp
    private sealed class GateProducer : RabbitMQ.Stream.Client.IProducer
    {
        private readonly Task _gate;
        public GateProducer(Task gate) => _gate = gate;
        public async ValueTask Send(ulong publishingId, RabbitMQ.Stream.Client.Message message) => await _gate;
        // ... every remaining probed member throws NotImplementedException ...
    }
```

And the 255-cap test (stub-level, deterministic single-threaded):

```csharp
    [Fact]
    public async Task Producer_name_cap_is_enforced_per_target()
    {
        var (registry, _) = CreateRegistry();
        var ct = TestContext.Current.CancellationToken;
        for (var i = 0; i < ResultProducerRegistry.MaxProducerNamesPerStream; i++)
            await registry.GetOrCreateAsync("dst", false, $"name-{i}", ct);
        var ex = await Assert.ThrowsAsync<StreamBusConfigurationException>(() =>
            registry.GetOrCreateAsync("dst", false, "name-overflow", ct));
        Assert.Contains("255", ex.Message);
        // A different target is unaffected.
        await registry.GetOrCreateAsync("other", false, "name-overflow", ct);
    }
```

(256 stub creations: fast, no broker.)

- [ ] **Step 2: Implement `ResultPublisherTests.cs`**

```csharp
namespace StreamBus.UnitTests;

public sealed class ResultPublisherTests
{
    private sealed class FlakyHandle : IStreamProducerHandle
    {
        private int _failuresLeft;
        public int Calls;
        public FlakyHandle(int failures) => _failuresLeft = failures;
        public Task SendAsync(ReadOnlyMemory<byte> body, string? routingKey, CancellationToken ct) => throw new NotImplementedException();
        public Task SendEnvelopeAsync(ReadOnlyMemory<byte> body, string? routingKey, IReadOnlyDictionary<string, object?> app, string? mid, string? cid, CancellationToken ct) => throw new NotImplementedException();
        public Task PublishReplayAsync(ReadOnlyMemory<byte> body, ulong publishingId, CancellationToken ct) => throw new NotImplementedException();
        public Task PublishResultAsync(ReadOnlyMemory<byte> body, ulong publishingId, string? routingKey, CancellationToken ct)
        {
            Calls++;
            if (Interlocked.Decrement(ref _failuresLeft) >= 0)
                throw new StreamBusPublishTransientException("flaky");
            return Task.CompletedTask;
        }
        public ValueTask DisposeAsync() => ValueTask.CompletedTask;
    }

    private sealed class FlakySystem : IStreamSystemHandle
    {
        private readonly FlakyHandle _handle;
        public FlakySystem(int failures) => _handle = new FlakyHandle(failures);
        public int Calls => _handle.Calls;
        public bool IsClosed => false;
        public Task<IStreamProducerHandle> CreatePlainProducerAsync(string stream, CancellationToken ct) => throw new NotImplementedException();
        public Task<IStreamProducerHandle> CreatePlainProducerWithReferenceAsync(string stream, string reference, CancellationToken ct) => Task.FromResult<IStreamProducerHandle>(_handle);
        public Task<IStreamProducerHandle> CreateSuperStreamProducerAsync(string stream, CancellationToken ct) => throw new NotImplementedException();
        public Task<IStreamProducerHandle> CreateSuperStreamProducerWithReferenceAsync(string stream, string reference, CancellationToken ct) => Task.FromResult<IStreamProducerHandle>(_handle);
        public Task<ulong?> TryQueryOffsetAsync(string reference, string stream, CancellationToken ct) => Task.FromResult<ulong?>(null);
        public Task StoreSystemOffsetAsync(string reference, string stream, ulong offset, CancellationToken ct) => Task.CompletedTask;
        public Task<bool> StreamExistsAsync(string stream, CancellationToken ct) => Task.FromResult(true);
        public Task<bool> SuperStreamExistsAsync(string superStream, CancellationToken ct) => Task.FromResult(false);
        public Task<string[]> QueryPartitionAsync(string superStream, CancellationToken ct) => Task.FromResult(new[] { superStream });
        public Task<ulong> QuerySequenceAsync(string reference, string stream, CancellationToken ct) => Task.FromResult(0UL);
        public Task<RabbitMQ.Stream.Client.RawConsumer> CreateRawConsumerAsync(RabbitMQ.Stream.Client.RawConsumerConfig config, CancellationToken ct) => throw new NotImplementedException();
        public ValueTask DisposeAsync() => ValueTask.CompletedTask;
    }

    private sealed class ReadyConnector : IStreamSystemConnector
    {
        private readonly FlakySystem _handle;
        public ReadyConnector(FlakySystem handle) => _handle = handle;
        public Task<IStreamSystemHandle> ConnectAsync(StreamBusOptions options, CancellationToken ct) => Task.FromResult<IStreamSystemHandle>(_handle);
    }

    private static (ResultPublisher Publisher, FlakySystem System) Create(int failures)
    {
        var system = new FlakySystem(failures);
        var options = new StreamBusOptions { Vhost = "/", UserName = "app", Password = "secret" };
        var supervisor = new StreamBusConnectionSupervisor(options, new ReadyConnector(system));
        supervisor.StartAsync(CancellationToken.None).GetAwaiter().GetResult();
        return (new ResultPublisher(new ResultProducerRegistry(supervisor, "/")), system);
    }

    private static ResultWork Work() => new()
    {
        TargetStream = "dst", TargetSuper = false, Body = new byte[] { 1 },
        SourceOffset = 9, RoutingKey = null, ProducerName = "grp",
    };

    [Fact]
    public async Task Fail_twice_then_confirms_three_publishes_one_return()
    {
        var (publisher, system) = Create(failures: 2);
        await publisher.PublishForeverAsync(Work(), _ => TimeSpan.Zero, TestContext.Current.CancellationToken);
        Assert.Equal(3, system.Calls);
    }

    [Fact]
    public async Task Cancel_mid_handoff_throws_and_stops()
    {
        var (publisher, system) = Create(failures: int.MaxValue);
        using var cts = new CancellationTokenSource(TimeSpan.FromMilliseconds(200));
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() =>
            publisher.PublishForeverAsync(Work(), _ => TimeSpan.FromMilliseconds(50), cts.Token));
        Assert.True(system.Calls >= 1);
    }

    [Fact]
    public async Task Negative_delay_clamps_to_zero()
    {
        var (publisher, system) = Create(failures: 1);
        await publisher.PublishForeverAsync(Work(), _ => TimeSpan.FromMilliseconds(-5), TestContext.Current.CancellationToken);
        Assert.Equal(2, system.Calls);
    }

    [Fact]
    public async Task Throwing_delay_falls_back_to_zero()
    {
        var (publisher, system) = Create(failures: 1);
        await publisher.PublishForeverAsync(Work(), _ => throw new InvalidOperationException("hostile"), TestContext.Current.CancellationToken);
        Assert.Equal(2, system.Calls);
    }
}
```

Resume queries (GetResumeOffsetAsync) are pinned in EffectivelyOnceTests.cs against the sequence-programmable StubSystem above (Sequences map keyed name@stream, Partitions array):

```csharp
    [Fact]
    public async Task Resume_returns_last_plus_one()
    {
        var (registry, system) = CreateRegistry();
        system.Sequences["grp@dst"] = 12;
        Assert.Equal(12UL, await registry.GetResumeOffsetAsync("dst", false, "grp", TestContext.Current.CancellationToken));
    }

    [Fact]
    public async Task Resume_is_null_when_never_published()
    {
        var (registry, _) = CreateRegistry();
        Assert.Null(await registry.GetResumeOffsetAsync("dst", false, "unknown", TestContext.Current.CancellationToken));
    }

    [Fact]
    public async Task Resume_takes_max_across_super_partitions_and_skips_unknown()
    {
        var (registry, system) = CreateRegistry();
        system.Partitions = new[] { "t-0", "t-1", "t-2" };
        system.Sequences["grp@t-0"] = 4;
        system.Sequences["grp@t-2"] = 9;
        Assert.Equal(9UL, await registry.GetResumeOffsetAsync("t", true, "grp", TestContext.Current.CancellationToken));
    }

    [Fact]
    public async Task Resume_is_null_when_no_partition_known()
    {
        var (registry, system) = CreateRegistry();
        system.Partitions = new[] { "t-0", "t-1" };
        Assert.Null(await registry.GetResumeOffsetAsync("t", true, "grp", TestContext.Current.CancellationToken));
    }
```
(Test files live in the StreamBus.UnitTests namespace, so StreamBus.* source types resolve without a using; add using RabbitMQ.Stream.Client for client types and fully qualify System.Text.Encoding.)

- [ ] **Step 3: Verify**

```powershell
dotnet test tests/StreamBus.UnitTests/StreamBus.UnitTests.csproj -c Release
```

Expected: all green, including the new files. The `GateProducer` fake from the reflection step must implement the full probed `IProducer` surface.

- [ ] **Step 4: Commit (with human approval)**

```powershell
git add tests/StreamBus.UnitTests/EffectivelyOnceTests.cs tests/StreamBus.UnitTests/ResultPublisherTests.cs
git commit -m "test: add effectively-once naming, resume, and forever-publish unit tests"
```

---

### Task 6: Integration tests + G-2 gate

**Files:**
- Create: `tests/StreamBus.IntegrationTests/EffectivelyOnceIntegrationTests.cs`

**Interfaces:**
- Consumes: bus surface (Task 4), `ResultNaming` (internals visible), admin `StreamSystem` (`QuerySequence`, `CreateStream`, `PartitionsSuperStreamSpec`), `BusInstance` harness pattern from `RetryIntegrationTests`.
- Produces: FR-6.1–FR-6.7 + FR-10.14 evidence, G-2 (FR-6.8) gate.

- [ ] **Step 1: Harness + plain happy path (FR-6.1, FR-6.5-order, FR-10.14)**

Full file header (mirror `RetryIntegrationTests` idioms exactly):

```csharp
using System.Text;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using RabbitMQ.Stream.Client;

namespace StreamBus.IntegrationTests;

public sealed class EffectivelyOnceIntegrationTests : IAsyncDisposable
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

    private static ReadOnlyMemory<byte> Body(string s) => Encoding.UTF8.GetBytes(s);

    private static (string Key, int Seq) Parse(ReadOnlyMemory<byte> body)
    {
        var parts = Encoding.UTF8.GetString(body.Span).Split(':');
        return (parts[0], int.Parse(parts[1]));
    }

    private static async Task WaitForAsync(Func<bool> done, TimeSpan timeout, string what)
    {
        var deadline = DateTime.UtcNow + timeout;
        while (!done())
        {
            if (DateTime.UtcNow >= deadline) Assert.Fail($"Timed out waiting for {what}.");
            await Task.Delay(100, TestContext.Current.CancellationToken);
        }
    }

    private static Task<ReadOnlyMemory<byte>> Echo(ConsumeContext ctx)
    {
        var (key, seq) = Parse(ctx.Body);
        return Task.FromResult(Body($"{key}:{seq}"));
    }

    private async Task<List<(string Key, int Seq)>> ReadTargetAsync(IStreamBus bus, string target, bool isSuper, int expected, TimeSpan timeout)
    {
        var got = new List<(string Key, int Seq)>();
        using var c = await bus.ConsumeAsync(target,
            new ConsumeOptions { Offset = ConsumeOffset.AlwaysFirst, Reference = Unique("read"), IsSuperStream = isSuper },
            ctx => { lock (got) got.Add(Parse(ctx.Body)); return Task.CompletedTask; },
            TestContext.Current.CancellationToken);
        await WaitForAsync(() => { lock (got) return got.Count >= expected; }, timeout, $"target {target} to reach {expected}");
        await Task.Delay(500, TestContext.Current.CancellationToken);
        lock (got) return got.ToList();
    }

    [Fact]
    public async Task Plain_to_plain_maps_publishing_id_to_source_offset()
    {
        var ct = TestContext.Current.CancellationToken;
        var source = Unique("eo-src");
        var target = Unique("eo-dst");
        var admin = await ConnectAdminAsync();
        await using var _ = admin;
        await admin.CreateStream(new StreamSpec(source));
        _streams.Add(source);
        await admin.CreateStream(new StreamSpec(target));
        _streams.Add(target);
        await using var pub = await BusInstance.StartAsync("pub");
        for (var seq = 0; seq < 5; seq++)
            await pub.Bus.PublishAsync(source, Body($"k:{seq}"), new PublishOptions(), ct);
        await pub.DisposeAsync();
        await using var eo = await BusInstance.StartAsync("eo");
        using var consumer = await eo.Bus.ConsumeEffectivelyOnceAsync(source, target, new EffectivelyOnceOptions(), Echo, ct);
        var got = await ReadTargetAsync(eo.Bus, target, isSuper: false, expected: 5, TimeSpan.FromSeconds(30));
        Assert.Equal(5, got.Count);
        Assert.Equal(Enumerable.Range(0, 5).Select(i => ("k", i)).ToList(), got);
        var producerName = ResultNaming.ProducerName(source, target);
        Assert.Equal(4UL, await admin.QuerySequence(producerName, target));
        var eoRef = ResultNaming.ConsumerReference(typeof(DefaultBus), source, target);
        Assert.Null(await eo.Bus.GetConsumerOffsetAsync(source, eoRef, 0, ct));
    }
}
```

(`Assert.Equal` on two `List<(string,int)>` compares element-wise ✓. `Echo` as a static method group converts to `Func<ConsumeContext, Task<ReadOnlyMemory<byte>>>` ✓. Unused `consumer`/`c` variables: `using var` discards — analyzers may flag unused? Existing tests use `var c = await ...` similarly (line 109: `var c = await inst.Bus.ConsumeAsync`) ✓ same idiom.)

- [ ] **Step 2: Dedup poison-republish (FR-6.1/FR-6.3 intent — Review Focus 1)**

```csharp
    [Fact]
    public async Task Republish_same_ids_keeps_first_write()
    {
        var ct = TestContext.Current.CancellationToken;
        var source = Unique("eo-src");
        var target = Unique("eo-dst");
        var admin = await ConnectAdminAsync();
        await using var _ = admin;
        await admin.CreateStream(new StreamSpec(source));
        _streams.Add(source);
        await admin.CreateStream(new StreamSpec(target));
        _streams.Add(target);
        await using var pub = await BusInstance.StartAsync("pub");
        for (var seq = 0; seq < 3; seq++)
            await pub.Bus.PublishAsync(source, Body($"k:{seq}"), new PublishOptions(), ct);
        await pub.DisposeAsync();
        await using var eo = await BusInstance.StartAsync("eo");
        using var consumer = await eo.Bus.ConsumeEffectivelyOnceAsync(source, target, new EffectivelyOnceOptions(), Echo, ct);
        Assert.Equal(3, (await ReadTargetAsync(eo.Bus, target, false, 3, TimeSpan.FromSeconds(30))).Count);
        var producerName = ResultNaming.ProducerName(source, target);
        for (var seq = 0; seq < 3; seq++)
            await eo.Bus.PublishResultAsync(target, Body($"POISON:{seq}"), (ulong)seq,
                new PublishResultOptions { ProducerName = producerName }, ct);
        await Task.Delay(TimeSpan.FromSeconds(2), ct);
        var got = await ReadTargetAsync(eo.Bus, target, false, 3, TimeSpan.FromSeconds(30));
        Assert.Equal(3, got.Count);
        Assert.DoesNotContain(got, x => x.Key == "POISON");
        Assert.Equal(Enumerable.Range(0, 3).Select(i => ("k", i)).ToList(), got);
    }
```

(`Assert.DoesNotContain` with predicate — xUnit v3 supports the predicate overload ✓.)

- [ ] **Step 3: Kill-mid-run resume (G-5 pattern, Review Focus 2)**

```csharp
    [Fact]
    public async Task Kill_mid_run_resumes_without_dupes_or_gaps()
    {
        var ct = TestContext.Current.CancellationToken;
        var source = Unique("eo-src");
        var target = Unique("eo-dst");
        var admin = await ConnectAdminAsync();
        await using var _ = admin;
        await admin.CreateStream(new StreamSpec(source));
        _streams.Add(source);
        await admin.CreateStream(new StreamSpec(target));
        _streams.Add(target);
        await using var pub = await BusInstance.StartAsync("pub");
        for (var seq = 0; seq < 10; seq++)
            await pub.Bus.PublishAsync(source, Body($"k:{seq}"), new PublishOptions(), ct);
        await pub.DisposeAsync();
        var gate = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var seen = 0;
        await using var eo1 = await BusInstance.StartAsync("eo1");
        using var c1 = await eo1.Bus.ConsumeEffectivelyOnceAsync(source, target, new EffectivelyOnceOptions(),
            async ctx =>
            {
                if (Interlocked.Increment(ref seen) == 4) gate.TrySetResult();
                await Task.Delay(50, ctx.CancellationToken);
                var (key, seq) = Parse(ctx.Body);
                return Body($"{key}:{seq}");
            }, ct);
        Assert.True(await gate.Task.WaitAsync(TimeSpan.FromSeconds(30), ct));
        // Crash-like kill: the bus (not the consumer) is disposed, so in-flight
        // dispatches die without any bookkeeping to reconcile. Redelivery plus
        // (name, id) dedup must converge to exactly-once.
        await eo1.DisposeAsync();
        await using var eo2 = await BusInstance.StartAsync("eo2");
        using var c2 = await eo2.Bus.ConsumeEffectivelyOnceAsync(source, target, new EffectivelyOnceOptions(), Echo, ct);
        var got = await ReadTargetAsync(eo2.Bus, target, false, 10, TimeSpan.FromSeconds(30));
        Assert.Equal(10, got.Count);
        Assert.Equal(Enumerable.Range(0, 10).Select(i => ("k", i)).ToList(), got);
    }
```

- [ ] **Step 4: Retry preserves order (FR-4.7 analog on the EO path)**

```csharp
    [Fact]
    public async Task Handler_retry_preserves_partition_order()
    {
        var ct = TestContext.Current.CancellationToken;
        var source = Unique("eo-src");
        var target = Unique("eo-dst");
        var admin = await ConnectAdminAsync();
        await using var _ = admin;
        await admin.CreateStream(new StreamSpec(source));
        _streams.Add(source);
        await admin.CreateStream(new StreamSpec(target));
        _streams.Add(target);
        await using var pub = await BusInstance.StartAsync("pub");
        for (var seq = 0; seq < 5; seq++)
            await pub.Bus.PublishAsync(source, Body($"k:{seq}"), new PublishOptions(), ct);
        await pub.DisposeAsync();
        await using var eo = await BusInstance.StartAsync("eo");
        var attempts = new Dictionary<int, int>();
        using var consumer = await eo.Bus.ConsumeEffectivelyOnceAsync(source, target,
            new EffectivelyOnceOptions { Retry = new RetryPolicy { BaseDelay = TimeSpan.FromMilliseconds(10), MaxJitter = TimeSpan.Zero } },
            ctx =>
            {
                var (key, seq) = Parse(ctx.Body);
                lock (attempts) attempts[seq] = attempts.TryGetValue(seq, out var n) ? n + 1 : 1;
                if (seq == 2 && attempts[seq] < 3) throw new InvalidOperationException("flaky 2");
                return Task.FromResult(Body($"{key}:{seq}"));
            }, ct);
        var got = await ReadTargetAsync(eo.Bus, target, false, 5, TimeSpan.FromSeconds(30));
        Assert.Equal(Enumerable.Range(0, 5).Select(i => ("k", i)).ToList(), got);
        Assert.Equal(3, attempts[2]);
    }
```

- [ ] **Step 5: Super source + super target with restart (FR-6.3/FR-6.6)**

```csharp
    [Fact]
    public async Task Super_source_to_plain_target_keeps_per_key_order()
    {
        var ct = TestContext.Current.CancellationToken;
        var source = Unique("eo-ssrc");
        var target = Unique("eo-dst");
        var admin = await ConnectAdminAsync();
        await using var _ = admin;
        await admin.CreateSuperStream(new PartitionsSuperStreamSpec(source, 3));
        _superStreams.Add(source);
        await admin.CreateStream(new StreamSpec(target));
        _streams.Add(target);
        await using var pub = await BusInstance.StartAsync("pub");
        for (var seq = 0; seq < 12; seq++)
            await pub.Bus.PublishAsync(source, Body($"k{seq % 4}:{seq}"),
                new PublishOptions { IsSuperStream = true, RoutingKey = $"k{seq % 4}" }, ct);
        await pub.DisposeAsync();
        await using var eo = await BusInstance.StartAsync("eo");
        using var consumer = await eo.Bus.ConsumeEffectivelyOnceAsync(source, target,
            new EffectivelyOnceOptions { SourceIsSuperStream = true }, Echo, ct);
        var got = await ReadTargetAsync(eo.Bus, target, false, 12, TimeSpan.FromSeconds(60));
        Assert.Equal(12, got.Count);
        foreach (var grp in got.GroupBy(x => x.Key))
            Assert.Equal(grp.Select(x => x.Seq).OrderBy(x => x).ToList(), grp.Select(x => x.Seq).ToList());
    }

    [Fact]
    public async Task Plain_to_super_target_resumes_from_max()
    {
        var ct = TestContext.Current.CancellationToken;
        var source = Unique("eo-src");
        var target = Unique("eo-stgt");
        var admin = await ConnectAdminAsync();
        await using var _ = admin;
        await admin.CreateStream(new StreamSpec(source));
        _streams.Add(source);
        await admin.CreateSuperStream(new PartitionsSuperStreamSpec(target, 2));
        _superStreams.Add(target);
        await using var pub = await BusInstance.StartAsync("pub");
        for (var seq = 0; seq < 6; seq++)
            await pub.Bus.PublishAsync(source, Body($"k{seq % 2}:{seq}"), new PublishOptions(), ct);
        await pub.DisposeAsync();
        var eoOptions = new EffectivelyOnceOptions
        {
            TargetIsSuperStream = true,
            TargetRouting = ctx => Parse(ctx.Body).Key,
        };
        await using var eo1 = await BusInstance.StartAsync("eo1");
        using var c1 = await eo1.Bus.ConsumeEffectivelyOnceAsync(source, target, eoOptions, Echo, ct);
        Assert.Equal(6, (await ReadTargetAsync(eo1.Bus, target, true, 6, TimeSpan.FromSeconds(60))).Count);
        await eo1.DisposeAsync();
        // Second generation publishes more, then resumes: only the new offsets flow.
        await using var pub2 = await BusInstance.StartAsync("pub2");
        for (var seq = 6; seq < 8; seq++)
            await pub2.Bus.PublishAsync(source, Body($"k{seq % 2}:{seq}"), new PublishOptions(), ct);
        await pub2.DisposeAsync();
        await using var eo2 = await BusInstance.StartAsync("eo2");
        using var c2 = await eo2.Bus.ConsumeEffectivelyOnceAsync(source, target, eoOptions, Echo, ct);
        var got = await ReadTargetAsync(eo2.Bus, target, true, 8, TimeSpan.FromSeconds(60));
        Assert.Equal(8, got.Count);
        Assert.Equal(8, got.Select(x => (x.Key, x.Seq)).Distinct().Count());
    }
```

- [ ] **Step 6: Missing routing stalls loudly (Review Focus 5) + SAC split (FR-6.3)**

```csharp
    [Fact]
    public async Task Super_target_without_routing_redelivers_and_writes_nothing()
    {
        var ct = TestContext.Current.CancellationToken;
        var source = Unique("eo-src");
        var target = Unique("eo-stgt");
        var admin = await ConnectAdminAsync();
        await using var _ = admin;
        await admin.CreateStream(new StreamSpec(source));
        _streams.Add(source);
        await admin.CreateSuperStream(new PartitionsSuperStreamSpec(target, 2));
        _superStreams.Add(target);
        await using var pub = await BusInstance.StartAsync("pub");
        await pub.Bus.PublishAsync(source, Body("k:0"), new PublishOptions(), ct);
        await pub.DisposeAsync();
        await using var eo = await BusInstance.StartAsync("eo");
        var entries = 0;
        using var consumer = await eo.Bus.ConsumeEffectivelyOnceAsync(source, target,
            new EffectivelyOnceOptions
            {
                TargetIsSuperStream = true,
                Retry = new RetryPolicy { BaseDelay = TimeSpan.FromMilliseconds(10), MaxJitter = TimeSpan.Zero },
            },
            ctx => { Interlocked.Increment(ref entries); return Echo(ctx); }, ct);
        // No routing anywhere (unrouted source + default TargetRouting): the step
        // must redeliver (entries grow) and must never write the target.
        await WaitForAsync(() => Volatile.Read(ref entries) >= 3, TimeSpan.FromSeconds(30), "redeliveries");
        var got = new List<(string Key, int Seq)>();
        using var reader = await eo.Bus.ConsumeAsync(target,
            new ConsumeOptions { Offset = ConsumeOffset.AlwaysFirst, Reference = Unique("read"), IsSuperStream = true },
            ctx => { lock (got) got.Add(Parse(ctx.Body)); return Task.CompletedTask; }, ct);
        await Task.Delay(TimeSpan.FromSeconds(2), ct);
        lock (got) Assert.Empty(got);
    }

    [Fact]
    public async Task Two_instances_share_one_group_without_corruption()
    {
        var ct = TestContext.Current.CancellationToken;
        var source = Unique("eo-ssrc");
        var target = Unique("eo-dst");
        var admin = await ConnectAdminAsync();
        await using var _ = admin;
        await admin.CreateSuperStream(new PartitionsSuperStreamSpec(source, 2));
        _superStreams.Add(source);
        await admin.CreateStream(new StreamSpec(target));
        _streams.Add(target);
        await using var pub = await BusInstance.StartAsync("pub");
        for (var seq = 0; seq < 8; seq++)
            await pub.Bus.PublishAsync(source, Body($"k{seq % 2}:{seq}"),
                new PublishOptions { IsSuperStream = true, RoutingKey = $"k{seq % 2}" }, ct);
        await pub.DisposeAsync();
        await using var a = await BusInstance.StartAsync("a");
        await using var b = await BusInstance.StartAsync("b");
        // Same derived reference (same bus type + streams) = one SAC group:
        // partitions split across instances, every message exactly once.
        using var ca = await a.Bus.ConsumeEffectivelyOnceAsync(source, target,
            new EffectivelyOnceOptions { SourceIsSuperStream = true }, Echo, ct);
        using var cb = await b.Bus.ConsumeEffectivelyOnceAsync(source, target,
            new EffectivelyOnceOptions { SourceIsSuperStream = true }, Echo, ct);
        var got = await ReadTargetAsync(a.Bus, target, false, 8, TimeSpan.FromSeconds(60));
        Assert.Equal(8, got.Count);
        Assert.Equal(8, got.Select(x => (x.Key, x.Seq)).Distinct().Count());
    }
```

- [ ] **Step 7: G-2 full cycle (FR-6.8 gate — super→super with flakiness + kill)**

```csharp
    [Fact]
    public async Task Full_cycle_super_to_super_with_flakes_and_kill_G2()
    {
        var ct = TestContext.Current.CancellationToken;
        var source = Unique("eo-ssrc");
        var target = Unique("eo-stgt");
        var admin = await ConnectAdminAsync();
        await using var _ = admin;
        await admin.CreateSuperStream(new PartitionsSuperStreamSpec(source, 3));
        _superStreams.Add(source);
        await admin.CreateSuperStream(new PartitionsSuperStreamSpec(target, 2));
        _superStreams.Add(target);
        await using var pub = await BusInstance.StartAsync("pub");
        for (var seq = 0; seq < 20; seq++)
            await pub.Bus.PublishAsync(source, Body($"k{seq % 5}:{seq}"),
                new PublishOptions { IsSuperStream = true, RoutingKey = $"k{seq % 5}" }, ct);
        await pub.DisposeAsync();
        var eoOptions = new EffectivelyOnceOptions
        {
            SourceIsSuperStream = true,
            TargetIsSuperStream = true,
            TargetRouting = ctx => Parse(ctx.Body).Key,
            Retry = new RetryPolicy { BaseDelay = TimeSpan.FromMilliseconds(10), MaxJitter = TimeSpan.Zero },
        };
        Task<ReadOnlyMemory<byte>> Flaky(ConsumeContext ctx)
        {
            var (key, seq) = Parse(ctx.Body);
            if (seq % 5 == 0 && Interlocked.Increment(ref _flakes) % 2 == 1)
                throw new InvalidOperationException($"flake {seq}");
            return Task.FromResult(Body($"{key}:{seq}"));
        }
        var gate = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var seen = 0;
        await using var eo1 = await BusInstance.StartAsync("eo1");
        using var c1 = await eo1.Bus.ConsumeEffectivelyOnceAsync(source, target, eoOptions,
            async ctx =>
            {
                if (Interlocked.Increment(ref seen) == 8) gate.TrySetResult();
                return await Flaky(ctx);
            }, ct);
        Assert.True(await gate.Task.WaitAsync(TimeSpan.FromSeconds(60), ct));
        await eo1.DisposeAsync();
        await using var eo2 = await BusInstance.StartAsync("eo2");
        using var c2 = await eo2.Bus.ConsumeEffectivelyOnceAsync(source, target, eoOptions, Flaky, ct);
        var got = await ReadTargetAsync(eo2.Bus, target, true, 20, TimeSpan.FromSeconds(90));
        Assert.Equal(20, got.Count);
        Assert.Equal(20, got.Select(x => (x.Key, x.Seq)).Distinct().Count());
        foreach (var grp in got.GroupBy(x => x.Key))
            Assert.Equal(grp.Select(x => x.Seq).OrderBy(x => x).ToList(), grp.Select(x => x.Seq).ToList());
        // FR-6.6 spot-check: every target partition holds a sequence for at least
        // one stable producer (resume-from-max had something to read).
        var parts = await admin.QueryPartition(target);
        Assert.Equal(2, parts.Length);
    }

    private int _flakes;
```

(`_flakes` is an instance field shared across the eo1/eo2 handlers, so flakes continue on restart (Strict retries absorb them). xUnit creates a new test-class instance per [Fact], so there is no cross-test state.)

(`Flaky` is a non-async local function: a synchronous throw propagates synchronously through the call inside the dispatch try and is caught by the retry loop. The async wrapper lambda awaits it normally.)

The `seen` gate at 8 of 20 is loose by design: restart converges regardless of the exact kill point, and even a post-completion kill keeps the asserts valid.

- [ ] **Step 8: Verify**

```powershell
dotnet test tests/StreamBus.IntegrationTests/StreamBus.IntegrationTests.csproj -c Release --filter "FullyQualifiedName~EffectivelyOnce"
```

Expected: all new tests green against `localhost:5552`. Then the full suites:

```powershell
dotnet test tests/StreamBus.UnitTests/StreamBus.UnitTests.csproj -c Release
dotnet test tests/StreamBus.IntegrationTests/StreamBus.IntegrationTests.csproj -c Release
```

Expected: everything green (Phases 3–6 unbroken). Broker-down retries are FailFast at the call site here (readiness waits bound the startup instead).

- [ ] **Step 9: Commit (with human approval)**

```powershell
git add tests/StreamBus.IntegrationTests/EffectivelyOnceIntegrationTests.cs
git commit -m "test: add effectively-once integration probes with G-2 gate"
```

---

## Follow-ups for Phase 8+ (tracked, no code in this phase)

- `SetConsumerOffsetAsync` on an EO reference is an accepted no-op for resume (guard-water only); revisit only if operators need to rewind an EO source (would require clearing target sequences — broker has no such API).
- The 255-name cap is best-effort under racing first-use; exactness would need a creation semaphore (rejected: first-use races are startup-only and the broker enforces the limit).
- `retry_blocked` on EO partitions reuses the `failure_mode="strict"` tag (EO is always Strict) — no new label.
- Resume cost on super targets is one `QuerySequence` per partition per attach; negligible against attach frequency.
- Nullable/filter results (zero-write steps) stay out of v1; the handler type enforces exactly one write.

---

## Self-Review

- Spec coverage: FR-6.1 (Tasks 2 id plumbing + 5 id assert + 6a/6b), FR-6.2 (Task 3 single publish per step + handler-returns-bytes type rule), FR-6.3 (Task 3 hardcoded SAC + sequential gate; Task 6 SAC-split test), FR-6.4 (Task 1 naming + Task 5 cap/meaningfulness tests), FR-6.5 (Tasks 2/3 retry-forever + Task 6 kill/restart asserts), FR-6.6 (Task 2 max-across-partitions + Task 5/6 super-target tests), FR-6.7 (Task 3 no-store branch + resume injection), FR-6.8/G-2 (Task 6 G-2 test), FR-10.14 (Task 3 no-store + Task 6a null-offset assert).
- FR-2.8/Phase 9, FR-7/Phase 8 untouched by design (listed out-of-scope).
- Placeholder scan: no TBD/TODO; every code step ships complete source; probe-dependent branch is a single explicit conditional with a STOP-and-ask (Task 1 unknown-ref shape); no "similar to Task N" (helpers are copied verbatim where reused: `InvalidateAsync`, `IsCallerCancel`, harness).
- Type consistency: `PublishResultAsync(body, publishingId, routingKey, ct)` handle method (Tasks 2/3/5 match); `ResultProducerRegistry.PublishResultAsync(targetStream, targetSuper, body, sourceOffset, routingKey, producerName, ct)` (Tasks 2/3 match); `GetResumeOffsetAsync(targetStream, targetSuper, producerName, ct) -> ulong?` = LAST id (Tasks 2/3 match; +1 applied by the shared stored+1 rule); `PublishForeverAsync(work, delayFor, ct)` (Tasks 2/4 match); `QuerySequenceAsync(reference, stream, ct) -> ulong` (Tasks 2/3 match); `ResultNaming.ProducerName(partitionStream, target)` + `ConsumerReference(busType, source, target)` + `PublishingId(offset)` (Tasks 1–3/5/6 match); `EffectivelyOnceConfig` fields (Tasks 1/3 match); `ConsumeEffectivelyOnceAsync(busType, source, target, options, handler, ct)` + `PublishResultAsync(target, result, sourceOffset, options, ct)` registry/bus signatures (Tasks 3/4/6 match).
- Review Focus: each line pinned above (Task 6b, Task 6c/6g, Task 1 + 2 + 5 + 6a, Task 5 collision test, Task 3 + 6f).
