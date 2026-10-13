# StreamBus Phase 6 (Retry + Error Stream) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Implement Phase 6 in-handler retry with `RetryPolicy`/`FailureMode`, the hand-published error stream with envelope + idempotent replay, and starvation visibility — plus the G-1 and G-5b gates.

**Architecture:** Retry lives strictly inside the partition dispatch loop (the loop already `await`s the handler sequentially under a dispatch gate, so sleeping between attempts stalls exactly one partition and preserves order by construction); exhaustion under `SkipAfterDeadline` hands the envelope to a dedicated `ErrorPublisher` (confirmed publish first, single advance-store second, observe-only hook third) while `Strict` retries unboundedly without ever advancing; replay is a call-scoped `AlwaysFirst` consumer plus stable-named producers publishing with `publishingId = sourceOffset` so broker dedup makes repeat replays idempotent. Observability reuses the Phase 5 meter (driving `retry_blocked` 1/0 live) with attempt detail in logs only.

**Tech Stack:** .NET 10 (`net10.0`), C# latest, `RabbitMQ.Stream.Client` 1.12.1 (same pins — no new packages), xUnit v3, `StreamBus.slnx`, Central Package Management.

**Spec:** `docs/2026-10-08-streambus-design.md` (§5 retry, §6 error stream, §11.1 per-consumer modes, §11.2 G-1, §12 write rule, §13 metrics catalog) and `docs/2026-10-08-streambus-requirements.md` (FR-4.1–FR-4.7, FR-5.1–FR-5.4, FR-9.3, NFR-4, G-1; G-5b is the Strict-retry-window half of G-5, split coined by the Phase 5 plan). Phase table: design §10 row 6. G-1 blocks this phase; G-5b belongs to retry (Phase 5 pinned G-5a only).

## Global Constraints

- `TargetFramework` exactly `net10.0`; `Nullable`/`ImplicitUsings` enable; `TreatWarningsAsErrors` true; `ManagePackageVersionsCentrally` true; `IsTestingPlatformApplication` false (all in `Directory.Build.props`, unchanged).
- `RabbitMQ.Stream.Client` exactly `1.12.1`; `Microsoft.Extensions.Hosting` / `Diagnostics.HealthChecks` / `Logging.Abstractions` exactly `10.0.12`; `xunit.v3` 4.0.2, `xunit.runner.visualstudio` 4.0.1, `Microsoft.NET.Test.Sdk` 18.10.1.
- Library root namespace `StreamBus`; never `RabbitMQ.Stream.*`.
- Ordered paths use `RawConsumer` plus the library supervision loop — never `Reliable*` (its handler-exception swallowing silently skips messages; FR-4.2).
- Retry happens **inside** the handler path only (FR-4.1): no wrapper outside the dispatch loop may catch-and-retry around it. The attempt loop lives in `StreamPartitionConsumer.DispatchAsync`, under the already-held dispatch gate.
- The resume point is never produced implicitly as `OffsetTypeNext`, and never replayed implicitly as `OffsetTypeFirst` (Phase 5 invariant, unchanged). Replay's `AlwaysFirst` is an explicit operator-driven read, not a resume.
- The resume point is never inferred from local, in-memory, or last-dispatched state — `localWater` is a write-guard only, never a resume source (Phase 5 invariant, unchanged).
- The library never creates, deletes, or alters streams or super streams (topology `Ensure*` is Phase 8): error streams must pre-exist; missing error stream is `StreamBusStreamNotFoundException`, never an implicit create.
- A `src/` grep for `ReliableProducer`, `ReliableConsumer`, `ReliableSuperStream` (any `Reliable*` client type) must return nothing; a `src/` grep for `CreateStream|CreateSuperStream|DeleteStream|DeleteSuperStream` must return nothing (admin lifecycle is test-only); committed in Task 6 verify.
- Every commit step requires explicit human approval before running; never push, never amend without permission.

## Review Focus

- A `SkipAfterDeadline` handoff whose error-stream publish fails must never advance past the message and never drop it: publish retries until confirmed or cancelled, the advance-store runs only after confirmation (FR-4.5). Pinned by Task 5 handoff-orchestration unit tests (fail-twice publisher: 3 publishes, 1 commit, commit-after-publish ordering; cancel-mid-handoff: zero commits).
- A poison message under `Strict` never advances and is never skipped: infinite in-handler retry, kill-mid-retry redelivers the same offset first, order preserved (G-5b). Pinned by Task 6 kill-during-retry test with a handler-entry gate (deterministic, never timing-based).
- Replaying the same error stream twice stores each message exactly once (FR-5.3). Pinned by Task 6 double-replay test asserting source counts.
- A throwing user `DelayCalculator` must not break dispatch: fall back to the exponential knobs with a warning (no task's happy path covers a hostile calculator). Pinned by Task 5 calculator-throws unit test.
- Attempt numbering is exact: `MaxAttempts` counts the initial attempt, `MaxAttempts = 1` means exactly one handler call then exhaustion (FR-4.3). Pinned by Task 5 delay/numbering unit tests + Task 6 `MaxAttempts = 1` integration variant.

---

## File Structure

New files (no new packages):

- `src/StreamBus/RetryPolicy.cs` — `FailureMode` enum, `RetryPolicy` + `ErrorStreamOptions` + `ErrorHandoff` records, `RetryDelays` (resolve + log-sampling).
- `src/StreamBus/ErrorEnvelope.cs` — `ErrorEnvelope` record, app-property envelope build/`TryParse`, `ReplayNaming` (stable producer name + publishing-id derivation).
- `src/StreamBus/ErrorPublisher.cs` — `ErrorPublisher` (confirmed handoff publish + `HandOffAsync` orchestration with injectable transport for tests).

Modified Phase 3/4/5 files (same branch lineage; existing suites must stay green):

- `src/StreamBus/ConsumeOptions.cs` — gains `Retry`, `ErrorStream`, `OnRetryExhausted` (defaults preserve Phase 5 behavior exactly).
- `src/StreamBus/StreamBusDelays.cs` — gains multiplier/jitter overload (existing overload body untouched).
- `src/StreamBus/ProducerHandles.cs` — `IStreamProducerHandle` gains `SendEnvelopeAsync`; `ConfirmationTracker` gains `PublishWithIdAsync`.
- `src/StreamBus/ProducerRegistry.cs` — gains `PublishEnvelopeAsync` (cached-handle + invalidate pattern, same as `PublishAsync`).
- `src/StreamBus/Connector.cs` — `IStreamSystemHandle` gains `CreatePlainProducerWithReferenceAsync`; `StreamSystemHandle` refactors to a shared core.
- `src/StreamBus/StreamPartitionConsumer.cs` — in-handler attempt loop, handoff path, `retry_blocked` drive, store-commit refactor, attach-start gauge reset.
- `src/StreamBus/ConsumerRegistry.cs` — combo/knob validation, error-stream existence check, `ErrorPublisher` wiring, `ReplayAsync`.
- `src/StreamBus/Registry.cs` — `ConsumersFor` passes `ProducersFor(vhost)` into the consumer registry.
- `src/StreamBus/Bus.cs` — `ReplayErrorStreamAsync` on both interfaces + both implementations.

Tests:

- `tests/StreamBus.UnitTests/RetryPolicyTests.cs` — validation matrix (no broker: validation precedes readiness), delay numbering/cap/override/calculator-throws, log sampling.
- `tests/StreamBus.UnitTests/ErrorEnvelopeTests.cs` — envelope round-trip, `TryParse` rejections, replay naming/id derivation.
- `tests/StreamBus.UnitTests/ErrorHandoffTests.cs` — `HandOffAsync` orchestration (retry-then-commit, ordering, cancel-no-commit).
- `tests/StreamBus.IntegrationTests/RetryIntegrationTests.cs` — Skip handoff + envelope + advance-once, `MaxAttempts = 1`, Strict G-5b kill-during-retry, G-1 load + failover with retry, double-replay idempotency, shared-vs-per-stream forms, mode coexistence, retry_blocked/lag visibility (FR-9.3).

Explicitly out of Phase 6 (deferred, not stubs): super-stream error destinations (v1 error streams are plain only — config error otherwise); stable FR-6 result-path producer naming (Phase 7; replay-scoped stable names here do not preempt it); typed `IConsumer<T>` + serializer (Phase 9: FR-2.1/2.8); topology `Ensure*` (Phase 8: FR-7); `PublishResultAsync` (Phase 7); hot-key auto-remedy (none exists — NFR-4, metrics only).

Recorded rulings (spec as authority; bind all tasks): `Retry`/`ErrorStream`/`OnRetryExhausted` on `ConsumeOptions` are additive (defaults = Phase 5 behavior: Strict unbounded, no error stream, no hook); error stream must pre-exist (operator creates it with `x-max-age`; library probes existence fail-fast, never creates); error destinations are plain streams in v1 (a super-stream error destination is `StreamBusConfigurationException`); handoff publishes reuse the existing unique-name cached-producer path (each handoff is a distinct message; dedup safety comes free — documented deviation from §6's "stable producer names" letter, whose idempotency intent applies to replay, where stable names ARE used); replay publishes to the recorded partition stream (plain `targetStream` override allowed; super override rejected in v1); replay reference is fixed per error stream (`streambus-replay:{sha}`) with `AlwaysFirst` (idempotent republish makes re-reads safe; one stored value per error stream is bounded broker garbage, documented); replay termination is idle-timeout (no usable broker end offset — Phase 5 Round 2 proved `LastOffset()` unusable); `retry_blocked` carries value 1/0 with the existing `failure_mode="strict"` tag and NO attempt label (label-cardinality ruling carried from Phase 5; attempt detail goes to logs); `OnRetryExhausted` is awaited but observe-only (throw → warn + continue; cancel → abandon with fault-logging continuation, same pattern as the Phase 5 timer fire-and-forget); handoff publish failure retries until confirmed or cancelled and NEVER advances (at-least-once over at-most-once, always); malformed error messages and missing replay targets are skip + warn + advance-past (a garbage message must not stall replay forever); negative/zero calculator results clamp to zero with a warning; poison-Strict log sampling is Warning for attempts 1–5 and every 100th, Debug otherwise (pure `ShouldLogRetryWarning`, unit-tested); promotion interplay needs no code change (demoted-instance stores are same-or-older values → duplicates possible, skips impossible; G-1 pins it empirically); the Phase 5 "promotion query timeout" follow-up is closed by this plan with no change (the listener stays O(1) resolve-only; retry sleep never runs on the promotion path).

---

### Task 1: Client write-path probe + retry records + delay helper

**Files:**
- Create: `src/StreamBus/RetryPolicy.cs`
- Modify: `src/StreamBus/StreamBusDelays.cs` (add overload only)

**Interfaces:**
- Consumes: `StreamBusConfigurationException`, `ILogger` patterns, existing `StreamBusDelays.ExponentialCapped` (body untouched).
- Produces: `FailureMode`, `RetryPolicy`, `ErrorStreamOptions`, `ErrorHandoff`, `RetryDelays.ResolveDelay` / `ShouldLogRetryWarning`, generalized delay overload for Tasks 2–4.

- [ ] **Step 1: Probe the 1.12.1 write path (read-only, record everything)**

Run in PowerShell and record the full output in the task report:

```powershell
$dll = Get-ChildItem "$env:USERPROFILE/.nuget/packages/rabbitmq.stream.client/1.12.1/lib" -Recurse -Filter RabbitMQ.Stream.Client.dll | Select-Object -First 1 -ExpandProperty FullName
$asm = [Reflection.Assembly]::LoadFrom($dll)
$asm.GetType("RabbitMQ.Stream.Client.Message").GetConstructors() | Select-Object @{n="Params";e={($_.GetParameters() | ForEach-Object { "$($_.ParameterType.Name) $($_.Name)" }) -join ", "}}
$asm.GetType("RabbitMQ.Stream.Client.Message").GetProperties() | Select-Object Name, PropertyType, CanWrite
$asm.GetType("RabbitMQ.Stream.Client.AMQP.Properties").GetProperties() | Select-Object Name, PropertyType, CanWrite
$asm.GetType("RabbitMQ.Stream.Client.StreamSpec").GetConstructors() | Select-Object @{n="Params";e={($_.GetParameters() | ForEach-Object { "$($_.ParameterType.Name) $($_.Name)" }) -join ", "}}
$asm.GetType("RabbitMQ.Stream.Client.StreamSpec").GetProperties() | Select-Object Name, PropertyType, CanWrite
```

The report must answer: (a) `Message` constructor shapes (byte[]/ReadOnlyMemory overloads); (b) whether `Message.Properties` and `Message.ApplicationProperties` are settable and their exact types; (c) `AMQP.Properties` settable members for MessageId/CorrelationId/CreationTime (+ exact CLR types); (d) the `StreamSpec` shape for declaring `x-max-age` retention from integration-test setup (property/ctor-arg name + type), or explicit "no retention member — test setup uses server policy/args dict shape X"; (e) how to read back effective retention if any (`StreamInfo`/`StreamStats` member or explicit "none").

- [ ] **Step 2: Implement `RetryPolicy.cs`**

```csharp
using Microsoft.Extensions.Logging;

namespace StreamBus;

public enum FailureMode
{
    Strict = 0,
    SkipAfterDeadline = 1,
}

public sealed record RetryPolicy
{
    public FailureMode FailureMode { get; init; } = FailureMode.Strict;
    public int? MaxAttempts { get; init; }
    public TimeSpan BaseDelay { get; init; } = TimeSpan.FromMilliseconds(200);
    public double Multiplier { get; init; } = 2.0;
    public TimeSpan MaxDelay { get; init; } = TimeSpan.FromSeconds(30);
    public TimeSpan MaxJitter { get; init; } = TimeSpan.FromSeconds(1);
    public Func<int, Exception, TimeSpan>? DelayCalculator { get; init; }
}

public sealed record ErrorStreamOptions
{
    public required string Stream { get; init; }
    public string? RoutingValue { get; init; }
}

public sealed record ErrorHandoff
{
    public required string ErrorStream { get; init; }
    public required string SourceStream { get; init; }
    public required int SourcePartition { get; init; }
    public required ulong SourceOffset { get; init; }
    public required int Attempts { get; init; }
    public required DateTimeOffset FailedAt { get; init; }
    public required Exception Exception { get; init; }
}

internal static class RetryDelays
{
    public static TimeSpan ResolveDelay(RetryPolicy policy, int attempt, Exception ex, ILogger logger)
    {
        ArgumentNullException.ThrowIfNull(policy);
        ArgumentNullException.ThrowIfNull(ex);
        ArgumentNullException.ThrowIfNull(logger);
        ArgumentOutOfRangeException.ThrowIfLessThan(attempt, 1);
        if (policy.DelayCalculator is { } calc)
        {
            try
            {
                return Clamp(calc(attempt, ex), attempt, logger);
            }
            catch (Exception cex)
            {
                logger.LogWarning(cex,
                    "Retry DelayCalculator threw for attempt {Attempt}; falling back to exponential knobs.",
                    attempt);
            }
        }
        return StreamBusDelays.ExponentialCapped(
            attempt, policy.BaseDelay, policy.MaxDelay, policy.Multiplier, policy.MaxJitter);
    }

    public static bool ShouldLogRetryWarning(int attempt)
        => attempt <= 5 || attempt % 100 == 0;

    private static TimeSpan Clamp(TimeSpan computed, int attempt, ILogger logger)
    {
        if (computed >= TimeSpan.Zero) return computed;
        logger.LogWarning(
            "Retry DelayCalculator returned negative {Delay} for attempt {Attempt}; clamped to zero.",
            computed, attempt);
        return TimeSpan.Zero;
    }
}
```

- [ ] **Step 3: Generalize `StreamBusDelays` (existing overload untouched)**

Read `src/StreamBus/StreamBusDelays.cs` first; leave the existing `ExponentialCapped(int, TimeSpan, TimeSpan, int)` body byte-identical (the supervisor cap test pins it). Add:

```csharp
public static TimeSpan ExponentialCapped(int attempt, TimeSpan baseDelay, TimeSpan maxDelay, double multiplier, TimeSpan maxJitter)
{
    ArgumentOutOfRangeException.ThrowIfLessThan(attempt, 1);
    ArgumentOutOfRangeException.ThrowIfLessThanOrEqual(multiplier, 1.0 - double.Epsilon); // multiplier < 1 rejected; == 1 allowed (constant delay)
    var grown = TimeSpan.FromMilliseconds(baseDelay.TotalMilliseconds * Math.Pow(multiplier, Math.Min(attempt - 1, 16)));
    if (grown > maxDelay) grown = maxDelay;
    if (grown < TimeSpan.Zero) grown = TimeSpan.Zero;
    var cap = TimeSpan.FromMilliseconds(Math.Min(maxJitter.TotalMilliseconds, grown.TotalMilliseconds / 2));
    var jitter = cap <= TimeSpan.Zero ? TimeSpan.Zero : TimeSpan.FromMilliseconds(Random.Shared.NextDouble() * cap.TotalMilliseconds);
    return grown + jitter;
}
```

Multiplier guard semantics: `< 1` throws (`ThrowIfLessThanOrEqual(multiplier, 1.0 - epsilon)` rejects anything meaningfully below 1 while allowing exactly 1.0). `Math.Min(attempt - 1, 16)` caps the exponent (16 doublings ≈ 65k× — overflow-safe for sane knobs; the `maxDelay` cap binds it anyway).

- [ ] **Step 4: Verify**

```powershell
dotnet build src/StreamBus/StreamBus.csproj -c Release
```

Expected: zero warnings/errors. Records are exercised by Tasks 3–5.

- [ ] **Step 5: Commit (with human approval)**

```powershell
git add src/StreamBus/RetryPolicy.cs src/StreamBus/StreamBusDelays.cs
git commit -m "feat: add retry policy records and generalized backoff"
```

---

### Task 2: Envelope + producer write paths + error publisher transport

**Files:**
- Create: `src/StreamBus/ErrorEnvelope.cs`
- Create: `src/StreamBus/ErrorPublisher.cs`
- Modify: `src/StreamBus/ProducerHandles.cs` (`SendEnvelopeAsync` + `PublishReplayAsync` + `PublishWithIdAsync`)
- Modify: `src/StreamBus/ProducerRegistry.cs` (`PublishEnvelopeAsync`)
- Modify: `src/StreamBus/Connector.cs` (`CreatePlainProducerWithReferenceAsync` + shared core)
- Modify: every existing `IStreamSystemHandle` stub under `tests/` (grep first; Phase 5 brief knew three files plus the validation stub — update every implementer found, new member throws `NotImplementedException("<TestClass> never creates producers.")`)

**Interfaces:**
- Consumes: `RetryPolicy`/`ErrorStreamOptions` (Task 1), client `Message` write shapes (Task 1 probe — adapt, report), `ConfirmationTracker`, `IStreamSystemHandle`.
- Produces: `ErrorEnvelope` (build/`TryParse`), `ReplayNaming`, `ErrorPublisher.PublishHandoffAsync` + `HandOffAsync`, envelope send path, explicit-id publish path, named-producer creation for Tasks 3–5.

- [ ] **Step 1: Implement `ErrorEnvelope.cs`**

Envelope keys live under `streambus.error.`; every value is stored as a string (invariant culture for numbers, round-trip `"o"` for timestamps) so parsing never depends on broker type mapping:

```csharp
using System.Globalization;

namespace StreamBus;

public sealed record ErrorEnvelope
{
    public required string ErrorStream { get; init; }
    public required string SourceStream { get; init; }
    public required int SourcePartition { get; init; }
    public required string SourcePartitionStream { get; init; }
    public required ulong SourceOffset { get; init; }
    public required byte[] Body { get; init; }
    public required string ExceptionType { get; init; }
    public required string ExceptionMessage { get; init; }
    public required string ExceptionStack { get; init; }
    public required int Attempts { get; init; }
    public required DateTimeOffset FailedAt { get; init; }
    public required string? MessageId { get; init; }
    public required string? CorrelationId { get; init; }
    public required string? RoutingValue { get; init; }
    public required string ErrorRoutingValue { get; init; }

    private const string P = "streambus.error.";

    public Dictionary<string, object?> ToApplicationProperties() => new()
    {
        [P + "source-stream"] = SourceStream,
        [P + "source-partition"] = SourcePartition.ToString(CultureInfo.InvariantCulture),
        [P + "source-partition-stream"] = SourcePartitionStream,
        [P + "source-offset"] = SourceOffset.ToString(CultureInfo.InvariantCulture),
        [P + "exception-type"] = ExceptionType,
        [P + "exception-message"] = ExceptionMessage,
        [P + "exception-stack"] = ExceptionStack,
        [P + "attempts"] = Attempts.ToString(CultureInfo.InvariantCulture),
        [P + "failed-at"] = FailedAt.ToString("o", CultureInfo.InvariantCulture),
        [P + "message-id"] = MessageId,
        [P + "correlation-id"] = CorrelationId,
        [P + "routing-value"] = RoutingValue,
        [P + "error-routing-value"] = ErrorRoutingValue,
    };

    public static bool TryParse(string errorStream, byte[] body, IReadOnlyDictionary<string, object?> app, out ErrorEnvelope? envelope)
    {
        envelope = null;
        if (string.IsNullOrWhiteSpace(errorStream) || body is null || app is null) return false;
        string? Get(string name) => app.TryGetValue(P + name, out var v) ? v?.ToString() : null;
        var sourceStream = Get("source-stream");
        var partitionStream = Get("source-partition-stream");
        var exceptionType = Get("exception-type");
        if (string.IsNullOrWhiteSpace(sourceStream)
            || string.IsNullOrWhiteSpace(partitionStream)
            || string.IsNullOrWhiteSpace(exceptionType)
            || Get("source-partition") is not { } partRaw || !int.TryParse(partRaw, NumberStyles.Integer, CultureInfo.InvariantCulture, out var partition)
            || Get("source-offset") is not { } offsetRaw || !ulong.TryParse(offsetRaw, NumberStyles.Integer, CultureInfo.InvariantCulture, out var offset)
            || Get("attempts") is not { } attemptsRaw || !int.TryParse(attemptsRaw, NumberStyles.Integer, CultureInfo.InvariantCulture, out var attempts)
            || Get("failed-at") is not { } failedRaw || !DateTimeOffset.TryParse(failedRaw, CultureInfo.InvariantCulture, DateTimeStyles.RoundtripKind, out var failedAt)
            || Get("error-routing-value") is not { } routing || string.IsNullOrWhiteSpace(routing))
            return false;
        envelope = new ErrorEnvelope
        {
            ErrorStream = errorStream,
            SourceStream = sourceStream,
            SourcePartition = partition,
            SourcePartitionStream = partitionStream,
            SourceOffset = offset,
            Body = body,
            ExceptionType = exceptionType,
            ExceptionMessage = Get("exception-message") ?? string.Empty,
            ExceptionStack = Get("exception-stack") ?? string.Empty,
            Attempts = attempts,
            FailedAt = failedAt,
            MessageId = Get("message-id"),
            CorrelationId = Get("correlation-id"),
            RoutingValue = Get("routing-value"),
            ErrorRoutingValue = routing,
        };
        return true;
    }
}

internal static class ReplayNaming
{
    public static string ProducerName(string partitionStream)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(partitionStream);
        var hash = Convert.ToHexString(System.Security.Cryptography.SHA256.HashData(
            System.Text.Encoding.UTF8.GetBytes(partitionStream)))[..32];
        return $"streambus-replay-{hash}";
    }

    public static string Reference(string errorStream)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(errorStream);
        var hash = Convert.ToHexString(System.Security.Cryptography.SHA256.HashData(
            System.Text.Encoding.UTF8.GetBytes(errorStream)))[..32];
        return $"streambus-replay-{hash}";
    }

    public static ulong PublishingId(ulong sourceOffset) => sourceOffset;
}
```

`TryParse` returning false (never throwing) is what lets replay skip + warn + advance past garbage. Null-valued optional entries (`message-id` etc.) are fine — `Get` yields null and they stay null. Missing *required* keys fail the parse.

- [ ] **Step 2: Producer write paths (`ProducerHandles.cs`)**

Add to `ConfirmationTracker`:

```csharp
public async Task PublishWithIdAsync(IProducer producer, string stream, ulong publishingId, Message message, CancellationToken ct)
{
    ct.ThrowIfCancellationRequested();
    var confirmation = new TaskCompletionSource<ResponseCode>(TaskCreationOptions.RunContinuationsAsynchronously);
    if (!_pending.TryAdd(publishingId, confirmation))
        throw new StreamBusPublishException($"Replay publishing id {publishingId} is already in flight for stream '{stream}' (internal error).");
    try
    {
        await producer.Send(publishingId, message).ConfigureAwait(false);
    }
    catch
    {
        _pending.TryRemove(publishingId, out _);
        throw;
    }
    using (ct.Register(static state => ((TaskCompletionSource<ResponseCode>)state!).TrySetCanceled(), confirmation))
    {
        ResponseCode code;
        try
        {
            code = await confirmation.Task.ConfigureAwait(false);
        }
        catch (OperationCanceledException) when (ct.IsCancellationRequested)
        {
            _pending.TryRemove(publishingId, out _);
            throw;
        }
        if (code != ResponseCode.Ok)
            throw new StreamBusPublishException($"Publish to stream '{stream}' was not confirmed (status {code}).");
    }
}
```

(Mirrors `PublishAsync` exactly, except the caller supplies the id and a collision in the pending table is an internal error — replay ids are distinct per message by construction.)

Add to `IStreamProducerHandle`:

```csharp
Task SendEnvelopeAsync(ReadOnlyMemory<byte> body, string? routingKey, IReadOnlyDictionary<string, object?> applicationProperties, string? messageId, string? correlationId, CancellationToken ct);
Task PublishReplayAsync(ReadOnlyMemory<byte> body, ulong publishingId, CancellationToken ct);
```

`StreamProducerHandle` implements `SendEnvelopeAsync` by building the client `Message` per the Task 1 probe (body + `Properties` MessageId/CorrelationId when non-null + `ApplicationProperties` copied into the client's dictionary type) and sending via `_confirmations.PublishAsync`, and `PublishReplayAsync` via `_confirmations.PublishWithIdAsync(_producer, _stream, publishingId, new Message(body.ToArray()), ct)`. `SuperStreamProducerHandle` implements both with `throw new StreamBusConfigurationException("Error streams and replay targets are plain streams; this path is not supported.")` — the registries never route error/replay publishes to a super handle (Tasks 3–4 validate plain-only), so these are defensive backstops, never primary paths.

Adaptations bound to the Task 1 probe (apply, report): `Message` construction shape (ctor args vs object-initializer setters for `Properties`/`ApplicationProperties`); the exact CLR type of the client's application-properties dictionary (copy entries); `Properties` MessageId/CorrelationId member types (assign strings, `.ToString()` if object-typed — the consume path already reads them as object).

- [ ] **Step 3: `ProducerRegistry.PublishEnvelopeAsync` + `Connector` named-producer creation**

Registry (mirrors `PublishAsync` minus timeout; handoff retries are the caller's loop):

```csharp
public async Task PublishEnvelopeAsync(string stream, ReadOnlyMemory<byte> body, string? routingKey, IReadOnlyDictionary<string, object?> applicationProperties, string? messageId, string? correlationId, CancellationToken ct = default)
{
    ArgumentException.ThrowIfNullOrWhiteSpace(stream);
    ArgumentNullException.ThrowIfNull(applicationProperties);
    var readiness = await _supervisor.WaitUntilReadyAsync(ReadinessPolicy.FailFastPolicy, ct).ConfigureAwait(false);
    if (readiness == BusReadiness.Fatal)
        throw new StreamBusFatalException($"StreamBus for vhost '{_vhost}' is in Fatal state; publish to '{stream}' refused.");
    if (readiness != BusReadiness.Ready)
        throw new StreamBusNotReadyException($"StreamBus for vhost '{_vhost}' is not ready ({readiness}); publish to '{stream}' refused.");
    var (entry, handle) = await GetOrCreateAsync(stream, super: false, ct).ConfigureAwait(false);
    var key = Key(stream, super: false);
    try
    {
        await handle.SendEnvelopeAsync(body, routingKey, applicationProperties, messageId, correlationId, ct).ConfigureAwait(false);
    }
    catch (OperationCanceledException)
    {
        throw;
    }
    catch (StreamBusPublishException)
    {
        throw;
    }
    catch (Exception ex) when (!IsCallerCancel(ex, ct))
    {
        await InvalidateAsync(key, entry, handle).ConfigureAwait(false);
        throw new StreamBusPublishTransientException($"Publish to stream '{stream}' failed transiently; the cached producer was discarded.", ex);
    }
}
```

`Connector.cs`: add to `IStreamSystemHandle`:

```csharp
Task<IStreamProducerHandle> CreatePlainProducerWithReferenceAsync(string stream, string reference, CancellationToken ct);
```

`StreamSystemHandle` refactors `CreatePlainProducerAsync` to call a shared core with `ProducerReference(stream)`, and the new method calls the same core with the supplied reference (validated: null/whitespace → `ArgumentException`; >255 UTF-8 bytes → `StreamBusConfigurationException` — a replay-scoped name is derived safely, so this is defensive only):

```csharp
public Task<IStreamProducerHandle> CreatePlainProducerAsync(string stream, CancellationToken ct)
    => CreatePlainProducerCoreAsync(stream, ProducerReference(stream), ct);

public Task<IStreamProducerHandle> CreatePlainProducerWithReferenceAsync(string stream, string reference, CancellationToken ct)
{
    ArgumentException.ThrowIfNullOrWhiteSpace(reference);
    if (Encoding.UTF8.GetByteCount(reference) > ReferenceDerivation.MaxReferenceBytes)
        throw new StreamBusConfigurationException($"Producer reference exceeds the broker's 255-byte limit.");
    return CreatePlainProducerCoreAsync(stream, reference, ct);
}

private async Task<IStreamProducerHandle> CreatePlainProducerCoreAsync(string stream, string reference, CancellationToken ct)
{
    ct.ThrowIfCancellationRequested();
    var confirmations = new ConfirmationTracker();
    var producer = await _system.CreateRawProducer(new RawProducerConfig(stream)
    {
        MessagesBufferSize = 100,
        Reference = reference,
        ConfirmHandler = confirmations.Complete,
    }, _factory.CreateLogger<RawProducer>()).ConfigureAwait(false);
    return new StreamProducerHandle(producer, stream, confirmations);
}
```

Keep the unique-name comment on `ProducerReference` (move it with the method, don't duplicate).

- [ ] **Step 4: Implement `ErrorPublisher.cs`**

```csharp
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

namespace StreamBus;

internal sealed class ErrorPublisher
{
    private readonly StreamProducerRegistry _producers;
    private readonly ILogger<ErrorPublisher> _logger;

    public ErrorPublisher(StreamProducerRegistry producers, ILogger<ErrorPublisher>? logger = null)
    {
        ArgumentNullException.ThrowIfNull(producers);
        _producers = producers;
        _logger = logger ?? NullLogger<ErrorPublisher>.Instance;
    }

    public Task PublishHandoffAsync(string errorStream, ErrorEnvelope envelope, CancellationToken ct)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(errorStream);
        ArgumentNullException.ThrowIfNull(envelope);
        return _producers.PublishEnvelopeAsync(
            errorStream, envelope.Body, routingKey: null,
            envelope.ToApplicationProperties(), envelope.MessageId, envelope.CorrelationId, ct);
    }

    // Orchestration with injectable transport (unit-testable without a broker):
    // retry the confirmed publish until it lands or ct fires, then run the
    // advance-store exactly once. Never advances past an unconfirmed handoff.
    public async Task HandOffAsync(
        ErrorEnvelope envelope,
        Func<ErrorEnvelope, CancellationToken, Task> publish,
        Func<Task> commitStore,
        Func<int, Exception, TimeSpan> delayFor,
        CancellationToken ct)
    {
        ArgumentNullException.ThrowIfNull(envelope);
        ArgumentNullException.ThrowIfNull(publish);
        ArgumentNullException.ThrowIfNull(commitStore);
        ArgumentNullException.ThrowIfNull(delayFor);
        var attempt = 0;
        while (true)
        {
            ct.ThrowIfCancellationRequested();
            attempt++;
            try
            {
                await publish(envelope, ct).ConfigureAwait(false);
                break;
            }
            catch (OperationCanceledException) when (ct.IsCancellationRequested)
            {
                throw;
            }
            catch (Exception ex)
            {
                _logger.LogWarning(ex,
                    "Error-stream handoff publish failed for {Source} offset {Offset} (attempt {Attempt}); retrying without advancing.",
                    envelope.SourcePartitionStream, envelope.SourceOffset, attempt);
                var delay = delayFor(attempt, ex);
                if (delay < TimeSpan.Zero) delay = TimeSpan.Zero;
                await Task.Delay(delay, ct).ConfigureAwait(false);
            }
        }
        await commitStore().ConfigureAwait(false);
    }
}
```

(The routing discriminator travels inside the envelope (`error-routing-value`); the outgoing message needs no broker routing key because error streams are plain. `routingKey: null` is deliberate — comment it as such.)

- [ ] **Step 5: Update every existing test stub**

Grep `IStreamSystemHandle` under `tests/`; add both new members (`SendEnvelopeAsync` is on the handle interface — existing fake *handles* gain it too; grep `IStreamProducerHandle` as well):
- `CreatePlainProducerWithReferenceAsync` → `throw new NotImplementedException("<TestClass> never creates producers.")`.
- Fake `IStreamProducerHandle.SendEnvelopeAsync` → record args into a list where the test needs it, else `NotImplementedException`. Task 5's handoff tests bypass transports via `HandOffAsync` funcs, so stubs only need to compile here.

- [ ] **Step 6: Verify**

```powershell
dotnet build src/StreamBus/StreamBus.csproj -c Release
dotnet test tests/StreamBus.UnitTests/StreamBus.UnitTests.csproj -c Release
```

Expected: build clean; existing suite green (interface additions are additive; `PublishAsync` untouched).

- [ ] **Step 7: Commit (with human approval)**

```powershell
git add src/StreamBus/ErrorEnvelope.cs src/StreamBus/ErrorPublisher.cs src/StreamBus/ProducerHandles.cs src/StreamBus/ProducerRegistry.cs src/StreamBus/Connector.cs tests/
git commit -m "feat: add error envelope, handoff publisher, and producer write paths"
```

---

### Task 3: In-handler retry loop + validation + error-stream wiring

**Files:**
- Modify: `src/StreamBus/ConsumeOptions.cs` (add `Retry`, `ErrorStream`, `OnRetryExhausted`)
- Modify: `src/StreamBus/StreamPartitionConsumer.cs` (attempt loop, handoff path, `retry_blocked`, commit refactor)
- Modify: `src/StreamBus/ConsumerRegistry.cs` (validation, existence check, `ErrorPublisher` wiring)

**Interfaces:**
- Consumes: `RetryPolicy`/`RetryDelays`/`ErrorHandoff` (Task 1), `ErrorEnvelope`/`ErrorPublisher` (Task 2), `OffsetStorer`/`MonotonicOffsetGuard`/`PartitionState`/`StreamBusMetrics` (Phase 5).
- Produces: retrying dispatch with exact store points, validated `ConsumeAsync`, per-partition `retry_blocked` values for Tasks 5–6.

- [ ] **Step 1: Extend `ConsumeOptions.cs`**

```csharp
public sealed record ConsumeOptions
{
    public bool IsSuperStream { get; init; } = false;
    public string? Reference { get; init; }
    public bool SingleActiveConsumer { get; init; } = true;
    public ConsumeOrdering Ordering { get; init; } = ConsumeOrdering.Strict;
    public ConsumeOffset Offset { get; init; } = ConsumeOffset.ResumeOrFirst;
    public OffsetStoreOptions OffsetStore { get; init; } = new();
    public RetryPolicy Retry { get; init; } = new();
    public ErrorStreamOptions? ErrorStream { get; init; }
    public Func<ErrorHandoff, Task>? OnRetryExhausted { get; init; }
    public ReadinessPolicy ReadinessPolicy { get; init; } = ReadinessPolicy.FailFastPolicy;
}
```

Defaults preserve Phase 5 behavior exactly (Strict unbounded, no error stream, no hook).

- [ ] **Step 2: Rework `StreamPartitionConsumer.DispatchAsync` with the attempt loop**

Constructor gains a required `ErrorPublisher errorPublisher` parameter (stored; null-checked). Add a `PromotionFallbackBranch`-style constant for the hook-abandon debug line if needed (inline text is fine).

Replace the handler-invoke section (current: inc inflight → await handler → dec → cadence store) with:

```csharp
Interlocked.Increment(ref _state.Inflight);
try
{
    var attempts = 0;
    while (true)
    {
        attempts++;
        try
        {
            await _handler(consumeContext).ConfigureAwait(false);
            break;
        }
        catch (OperationCanceledException) when (attach.LoopToken.IsCancellationRequested)
        {
            Interlocked.Decrement(ref _state.Inflight);
            Interlocked.Exchange(ref _state.RetryBlocked, 0);
            return;
        }
        catch (Exception ex)
        {
            var strict = _options.Retry.FailureMode == FailureMode.Strict;
            var exhausted = !strict && attempts >= (_options.Retry.MaxAttempts ?? int.MaxValue);
            if (RetryDelays.ShouldLogRetryWarning(attempts))
                _logger.LogWarning(ex,
                    "Consume handler failed for stream {Stream} partition {Partition} offset {Offset} (attempt {Attempt}, mode {Mode}); retrying in-handler.",
                    _stream, _partitionIndex, offset, attempts, _options.Retry.FailureMode);
            else
                _logger.LogDebug(ex,
                    "Consume handler failed for stream {Stream} partition {Partition} offset {Offset} (attempt {Attempt}, mode {Mode}); retrying in-handler.",
                    _stream, _partitionIndex, offset, attempts, _options.Retry.FailureMode);
            if (exhausted)
            {
                Interlocked.Decrement(ref _state.Inflight);
                Interlocked.Exchange(ref _state.RetryBlocked, 0);
                await ExhaustAsync(consumer, offset, consumeContext, attempts, ex, attach).ConfigureAwait(false);
                return;
            }
            Interlocked.Exchange(ref _state.RetryBlocked, 1);
            var delay = RetryDelays.ResolveDelay(_options.Retry, attempts, ex, _logger);
            try
            {
                await Task.Delay(delay, attach.LoopToken).ConfigureAwait(false);
            }
            catch (OperationCanceledException) when (attach.LoopToken.IsCancellationRequested)
            {
                Interlocked.Decrement(ref _state.Inflight);
                Interlocked.Exchange(ref _state.RetryBlocked, 0);
                return;
            }
        }
    }
}
catch (OperationCanceledException) when (attach.LoopToken.IsCancellationRequested)
{
    Interlocked.Decrement(ref _state.Inflight);
    Interlocked.Exchange(ref _state.RetryBlocked, 0);
    return;
}
Interlocked.Decrement(ref _state.Inflight);
Interlocked.Exchange(ref _state.RetryBlocked, 0);

if (_storer.ShouldStoreOnSuccess(offset))
{
    await StoreAndTrackAsync(consumer, offset, attach, attach.LoopToken).ConfigureAwait(false);
}
```

Notes (implement exactly): `attempts` counts completed handler attempts (initial = 1), matching `MaxAttempts` semantics and the `DelayCalculator` 1-based retry number (`ResolveDelay(policy, attempts, ...)` — the delay before retry N+1 uses attempt number N, per §5 "1 = delay before the first retry after the initial failure"). The `?? int.MaxValue` is defensive-only (validation guarantees non-null under Skip). Cancellation at any point exits quietly with inflight decremented and `retry_blocked` reset — no store, no advance, redelivery by design. The pre-existing outer `catch (Exception)` safety net and `FailAttach` paths stay untouched (context-build errors still fail the attach; they are not handler failures).

`ExhaustAsync` (new private method — handoff then hook, never throws into dispatch):

```csharp
private async Task ExhaustAsync(RawConsumer consumer, ulong offset, ConsumeContext consumeContext, int attempts, Exception lastError, AttachState attach)
{
    var errorStream = _options.ErrorStream!.Stream;
    var failedAt = DateTimeOffset.UtcNow;
    var envelope = new ErrorEnvelope
    {
        ErrorStream = errorStream,
        SourceStream = _stream,
        SourcePartition = _partitionIndex,
        SourcePartitionStream = _partitionStream,
        SourceOffset = offset,
        Body = consumeContext.Body.ToArray(),
        ExceptionType = lastError.GetType().FullName ?? lastError.GetType().Name,
        ExceptionMessage = lastError.Message,
        ExceptionStack = lastError.ToString(),
        Attempts = attempts,
        FailedAt = failedAt,
        MessageId = consumeContext.Metadata.MessageId,
        CorrelationId = consumeContext.Metadata.CorrelationId,
        RoutingValue = consumeContext.RoutingValue,
        ErrorRoutingValue = _options.ErrorStream.RoutingValue ?? _stream,
    };
    _logger.LogWarning(lastError,
        "Retry budget exhausted for stream {Stream} partition {Partition} offset {Offset} after {Attempts} attempts; handing off to error stream {ErrorStream}.",
        _stream, _partitionIndex, offset, attempts, errorStream);
    await _errors.HandOffAsync(
        envelope,
        (env, ct) => _errors.PublishHandoffAsync(errorStream, env, ct),
        () => CommitStoreAsync(consumer, offset, attach, attach.LoopToken),
        (n, ex) => RetryDelays.ResolveDelay(_options.Retry, n, ex, _logger),
        attach.LoopToken).ConfigureAwait(false);
    if (_options.OnRetryExhausted is { } hook)
    {
        var handoff = new ErrorHandoff
        {
            ErrorStream = errorStream,
            SourceStream = _stream,
            SourcePartition = _partitionIndex,
            SourceOffset = offset,
            Attempts = attempts,
            FailedAt = failedAt,
            Exception = lastError,
        };
        var hookTask = hook(handoff);
        try
        {
            await hookTask.WaitAsync(attach.LoopToken).ConfigureAwait(false);
        }
        catch (OperationCanceledException) when (attach.LoopToken.IsCancellationRequested)
        {
            _ = hookTask.ContinueWith(
                t =>
                {
                    if (t.Exception is { } hex)
                        _logger.LogWarning(hex,
                            "OnRetryExhausted hook faulted after abandon for stream {Stream} partition {Partition} reference {Reference}.",
                            _stream, _partitionIndex, _reference);
                },
                CancellationToken.None,
                TaskContinuationOptions.OnlyOnFaulted | TaskContinuationOptions.ExecuteSynchronously,
                TaskScheduler.Default);
            _logger.LogDebug(
                "OnRetryExhausted hook abandoned on shutdown for stream {Stream} partition {Partition}.",
                _stream, _partitionIndex);
        }
        catch (Exception hex)
        {
            _logger.LogWarning(hex,
                "OnRetryExhausted hook failed for stream {Stream} partition {Partition}; handoff outcome unchanged.",
                _stream, _partitionIndex);
        }
    }
}
```

Refactor `StoreAndTrackAsync`: extract the post-admission body into `CommitStoreAsync(RawConsumer consumer, ulong offset, AttachState attach, CancellationToken ct)` (raw `StoreOffset` + catch-log-continue + `_storer.Stored(offset)` + `LastStored` exchange + first-store regression check). `StoreAndTrackAsync` becomes TryAdmit-gate + `CommitStoreAsync`. The handoff commit reuses `CommitStoreAsync` directly (admission already happened inside `HandOffAsync`'s publish ordering — admission precedes the publish: see ordering note below).

Admission ordering in the handoff path (implement exactly): `HandOffAsync`'s `commitStore` callback is `() => CommitStoreAsync(...)`, which performs NO admission. Admission for the handoff happens once, before `HandOffAsync` is called: wrap the `ExhaustAsync` body start with the guard check — if `!_storer.TryAdmit(offset)` (rejected metric + debug already inside), return without publishing (a stale/demoted instance must not emit duplicate error messages; the active instance redelivers and hands off itself). Insert immediately after the warning log, before building the envelope:

```csharp
if (!_storer.TryAdmit(offset))
{
    return; // stale view: the active instance owns this offset's handoff.
}
```

And in `RunAsync`, reset `Interlocked.Exchange(ref _state.RetryBlocked, 0)` right after a consumer attaches successfully (next to `failures = 0`), so a recreated attach never inherits a stuck 1 from a failed attach that died mid-retry.

Handoff cadence note (comment in code): the handoff commit calls `_storer.Stored(offset)`, resetting the cadence counter — the handoff offset is always stored exactly once regardless of `EveryMessages` (FR-4.5 "advances exactly once"), and the `IsStored` timer dedup then skips it.

- [ ] **Step 3: `ConsumerRegistry` validation + wiring**

`StreamConsumerRegistry` gains a required fourth constructor parameter `StreamProducerRegistry producers` (stored; null-checked) — `Registry.cs` `ConsumersFor` becomes `new StreamConsumerRegistry(SupervisorForCore(vhost), vhost, _loggerFactory, ProducersFor(vhost))`. Per-`ConsumeAsync` call, build `new ErrorPublisher(producers, _loggerFactory.CreateLogger<ErrorPublisher>())` and pass it to each `StreamPartitionConsumer`.

Sync validation block in `ConsumeAsync` (alongside the existing checks, still before the readiness wait):

```csharp
ArgumentNullException.ThrowIfNull(options.Retry);
var retry = options.Retry;
if (retry.MaxAttempts is { } max && max < 1)
    throw new StreamBusConfigurationException("ConsumeOptions.Retry.MaxAttempts must be >= 1 when set.");
if (retry.FailureMode == FailureMode.Strict && retry.MaxAttempts is not null)
    throw new StreamBusConfigurationException("ConsumeOptions.Retry.MaxAttempts must be null with FailureMode.Strict (unbounded retry).");
if (retry.FailureMode == FailureMode.SkipAfterDeadline && retry.MaxAttempts is null)
    throw new StreamBusConfigurationException("ConsumeOptions.Retry.MaxAttempts is required with FailureMode.SkipAfterDeadline.");
if (retry.BaseDelay <= TimeSpan.Zero)
    throw new StreamBusConfigurationException("ConsumeOptions.Retry.BaseDelay must be positive.");
if (retry.MaxDelay <= TimeSpan.Zero)
    throw new StreamBusConfigurationException("ConsumeOptions.Retry.MaxDelay must be positive.");
if (retry.Multiplier < 1.0)
    throw new StreamBusConfigurationException("ConsumeOptions.Retry.Multiplier must be >= 1.");
if (retry.MaxJitter < TimeSpan.Zero)
    throw new StreamBusConfigurationException("ConsumeOptions.Retry.MaxJitter must be non-negative.");
if (retry.FailureMode == FailureMode.Strict && options.ErrorStream is not null)
    throw new StreamBusConfigurationException("ConsumeOptions.ErrorStream must not be configured with FailureMode.Strict.");
if (retry.FailureMode == FailureMode.SkipAfterDeadline && options.ErrorStream is null)
    throw new StreamBusConfigurationException("ConsumeOptions.ErrorStream is required with FailureMode.SkipAfterDeadline.");
if (options.ErrorStream is { } es)
{
    ArgumentException.ThrowIfNullOrWhiteSpace(es.Stream);
}
```

After the readiness wait + handle acquisition, alongside the source-stream existence check:

```csharp
if (options.ErrorStream is { } error)
{
    if (await handle.SuperStreamExistsAsync(error.Stream, ct).ConfigureAwait(false))
        throw new StreamBusConfigurationException($"Error stream '{error.Stream}' must be a plain stream in v1 (super-stream error destinations are not supported).");
    if (!await handle.StreamExistsAsync(error.Stream, ct).ConfigureAwait(false))
        throw new StreamBusStreamNotFoundException(error.Stream);
}
```

(Order: super-probe first so a super stream reports the precise config error rather than NotFound; mirrors `ResolvePartitionStreamAsync` ordering.)

- [ ] **Step 4: Verify**

```powershell
dotnet build src/StreamBus/StreamBus.csproj -c Release
dotnet test tests/StreamBus.UnitTests/StreamBus.UnitTests.csproj -c Release
```

Expected: build clean; existing suite green — in particular the Phase 5 fail-then-succeed integration is untouched by this task, and the default `Retry = new()` keeps all existing unit constructions valid (Strict + null MaxAttempts + null ErrorStream passes the new checks).

- [ ] **Step 5: Commit (with human approval)**

```powershell
git add src/StreamBus/ConsumeOptions.cs src/StreamBus/StreamPartitionConsumer.cs src/StreamBus/ConsumerRegistry.cs src/StreamBus/Registry.cs
git commit -m "feat: add in-handler retry loop with strict/skip exhaustion"
```

---

### Task 4: Replay bus surface + registry implementation

**Files:**
- Modify: `src/StreamBus/Bus.cs` (`ReplayErrorStreamAsync` on both interfaces + both implementations)
- Modify: `src/StreamBus/ConsumerRegistry.cs` (`ReplayAsync`)

**Interfaces:**
- Consumes: `ErrorEnvelope.TryParse`, `ReplayNaming`, `StreamPartitionConsumer` (transient use), `CreatePlainProducerWithReferenceAsync`, `ConfirmationTracker.PublishWithIdAsync`, fail-fast readiness pattern.
- Produces: `IStreamBus.ReplayErrorStreamAsync` (+generic) for Task 6.

- [ ] **Step 1: Implement `StreamConsumerRegistry.ReplayAsync`**

```csharp
public async Task<int> ReplayAsync(string errorStream, string? targetStream, CancellationToken ct = default)
{
    ArgumentException.ThrowIfNullOrWhiteSpace(errorStream);
    if (targetStream is not null) ArgumentException.ThrowIfNullOrWhiteSpace(targetStream);
    await EnsureReadyAsync(errorStream, ct).ConfigureAwait(false);
    var handle = _supervisor.CurrentHandle
        ?? throw new StreamBusNotReadyException($"StreamBus for vhost '{_vhost}' is not ready; replay of '{errorStream}' refused.");
    if (await handle.SuperStreamExistsAsync(errorStream, ct).ConfigureAwait(false))
        throw new StreamBusConfigurationException($"Error stream '{errorStream}' must be a plain stream in v1.");
    if (!await handle.StreamExistsAsync(errorStream, ct).ConfigureAwait(false))
        throw new StreamBusStreamNotFoundException(errorStream);
    string? targetPartitionProbe = null;
    if (targetStream is not null)
    {
        if (await handle.SuperStreamExistsAsync(targetStream, ct).ConfigureAwait(false))
            throw new StreamBusConfigurationException($"Replay target '{targetStream}' must be a plain stream in v1.");
        if (!await handle.StreamExistsAsync(targetStream, ct).ConfigureAwait(false))
            throw new StreamBusStreamNotFoundException(targetStream);
        targetPartitionProbe = targetStream;
    }

    var reference = ReplayNaming.Reference(errorStream);
    var replayOptions = new ConsumeOptions { Offset = ConsumeOffset.AlwaysFirst };
    var guard = new MonotonicOffsetGuard(); // replay-scoped: never touches consumer watermarks.
    var state = new PartitionState(); // untracked: replay progress surfaces via the return count + logs, not the meter.
    var errors = new ErrorPublisher(_producers, _loggerFactory.CreateLogger<ErrorPublisher>());
    var replayed = 0;
    var skipped = 0;
    long lastTicks = DateTimeOffset.UtcNow.UtcTicks; // cross-thread via Interlocked below.
    var producers = new Dictionary<string, IStreamProducerHandle>();
    static void DisposeProducers(Dictionary<string, IStreamProducerHandle> table)
    {
        foreach (var (_, entry) in table)
        {
            try { entry.DisposeAsync().GetAwaiter().GetResult(); }
            catch { }
        }
        table.Clear();
    }
    // Transport (locked): replay publishes explicit ids through the interface —
    // Task 2 adds `Task PublishReplayAsync(ReadOnlyMemory<byte> body, ulong publishingId, CancellationToken ct)`
    // to `IStreamProducerHandle` (`StreamProducerHandle` sends via
    // `ConfirmationTracker.PublishWithIdAsync`; the super handle throws plain-only).
    // The stable replay name makes (name, id) deterministic per target partition stream.
    using var family = new CancellationTokenSource();
    try
    {
        var loop = new StreamPartitionConsumer(
            handle, errorStream, errorStream, 0, reference, replayOptions,
            async ctx =>
            {
                if (!ErrorEnvelope.TryParse(errorStream, ctx.Body.ToArray(), ctx.Metadata.ApplicationProperties, out var envelope) || envelope is null)
                {
                    Interlocked.Increment(ref skipped);
                    _logger.LogWarning("Replay skipping malformed message on error stream '{Error}' at offset {Offset}.",
                        errorStream, ctx.Offset);
                    return;
                }
                var partitionStream = targetPartitionProbe ?? envelope.SourcePartitionStream;
                if (targetPartitionProbe is null
                    && !await handle.StreamExistsAsync(partitionStream, ct).ConfigureAwait(false))
                {
                    Interlocked.Increment(ref skipped);
                    _logger.LogWarning("Replay skipping message for vanished partition stream '{Partition}' (error offset {Offset}).",
                        partitionStream, ctx.Offset);
                    return;
                }
                if (!producers.TryGetValue(partitionStream, out var producer))
                {
                    producer = await handle.CreatePlainProducerWithReferenceAsync(
                        partitionStream, ReplayNaming.ProducerName(partitionStream), ct).ConfigureAwait(false);
                    producers[partitionStream] = producer;
                }
                await producer.PublishReplayAsync(envelope.Body, ReplayNaming.PublishingId(envelope.SourceOffset), ct).ConfigureAwait(false);
                Interlocked.Increment(ref replayed);
                Interlocked.Exchange(ref lastTicks, DateTimeOffset.UtcNow.UtcTicks);
            },
            guard, state, StreamBusMetrics.Shared, _vhost, "replay", errors,
            _loggerFactory.CreateLogger<StreamPartitionConsumer>());
        var run = loop.RunAsync(family.Token);
        try
        {
            await loop.FirstAttachAsync.WaitAsync(ct).ConfigureAwait(false);
            var idle = TimeSpan.FromSeconds(5);
            while (true)
            {
                ct.ThrowIfCancellationRequested();
                var since = DateTimeOffset.UtcNow - new DateTimeOffset(Interlocked.Read(ref lastTicks), TimeSpan.Zero);
                if (since >= idle && Volatile.Read(ref state.Inflight) == 0) break;
                await Task.Delay(TimeSpan.FromMilliseconds(200), ct).ConfigureAwait(false);
            }
        }
        finally
        {
            family.Cancel();
            try { await run.ConfigureAwait(false); }
            catch (OperationCanceledException) { }
            await loop.DisposeAsync().ConfigureAwait(false);
        }
        _logger.LogInformation("Replayed {Replayed} messages from error stream '{Error}' ({Skipped} skipped).",
            replayed, errorStream, skipped);
        return replayed;
    }
    finally { DisposeProducers(producers); family.Dispose(); }
}
```

The replay consumer uses the default Strict policy: a failing replay publish retries in-handler (stalling the error-stream partition, preserving replay order). `skipped` paths return normally, advancing past garbage/vanished targets by design.

- [ ] **Step 2: `Bus.cs` surface**

Both interfaces gain:

```csharp
Task<int> ReplayErrorStreamAsync(string errorStream, string? targetStream = null, CancellationToken ct = default);
```

Both implementations delegate: `=> _consumers.ReplayAsync(errorStream, targetStream, ct);` (no bus type needed — replay is vhost-scoped like Get/Set offsets).

- [ ] **Step 3: Verify**

```powershell
dotnet build src/StreamBus/StreamBus.csproj -c Release
dotnet test tests/StreamBus.UnitTests/StreamBus.UnitTests.csproj -c Release
```

Expected: build clean; suite green (surface is additive; replay untested until Task 6).

- [ ] **Step 4: Commit (with human approval)**

```powershell
git add src/StreamBus/Bus.cs src/StreamBus/ConsumerRegistry.cs
git commit -m "feat: add idempotent error-stream replay"
```

---

### Task 5: Unit tests (policy, delays, envelope, handoff)

**Files:**
- Create: `tests/StreamBus.UnitTests/RetryPolicyTests.cs`
- Create: `tests/StreamBus.UnitTests/ErrorEnvelopeTests.cs`
- Create: `tests/StreamBus.UnitTests/ErrorHandoffTests.cs`

**Interfaces:**
- Consumes: everything from Tasks 1–4; `NeverReadyConnector` validation-precedes-readiness pattern (Phase 5 `ConsumeValidationTests`); test `ListLogger` sink (Phase 5 `OffsetResolutionTests` — `NullScope.Instance` scope form).
- Produces: pins for Review Focus lines 1 (handoff half), 4, 5.

- [ ] **Step 1: Implement `RetryPolicyTests.cs`**

```csharp
namespace StreamBus.UnitTests;

public sealed class RetryPolicyTests
{
    private static StreamConsumerRegistry UnstartedRegistry()
    {
        var options = new StreamBusOptions { Vhost = "/", UserName = "app", Password = "secret" };
        var supervisor = new StreamBusConnectionSupervisor(options, new NeverReadyConnector());
        var producers = new StreamProducerRegistry(supervisor, "/");
        return new StreamConsumerRegistry(supervisor, "/", null, producers);
    }

    private sealed class NeverReadyConnector : IStreamSystemConnector
    {
        public Task<IStreamSystemHandle> ConnectAsync(StreamBusOptions options, CancellationToken ct)
            => new TaskCompletionSource<IStreamSystemHandle>().Task;
    }

    private static Task<IStreamConsumer> Consume(ConsumeOptions options)
    {
        var registry = UnstartedRegistry();
        return registry.ConsumeAsync(typeof(DefaultBus), "s", options, _ => Task.CompletedTask, TestContext.Current.CancellationToken);
    }

    [Fact]
    public async Task Strict_with_max_attempts_throws()
    {
        var ex = await Assert.ThrowsAsync<StreamBusConfigurationException>(() =>
            Consume(new ConsumeOptions { Retry = new RetryPolicy { MaxAttempts = 3 } }));
        Assert.Contains("Strict", ex.Message);
    }

    [Fact]
    public async Task Strict_with_error_stream_throws()
    {
        var ex = await Assert.ThrowsAsync<StreamBusConfigurationException>(() =>
            Consume(new ConsumeOptions { ErrorStream = new ErrorStreamOptions { Stream = "e" } }));
        Assert.Contains("Strict", ex.Message);
    }

    [Fact]
    public async Task Skip_without_max_attempts_throws()
    {
        var ex = await Assert.ThrowsAsync<StreamBusConfigurationException>(() =>
            Consume(new ConsumeOptions
            {
                Retry = new RetryPolicy { FailureMode = FailureMode.SkipAfterDeadline },
                ErrorStream = new ErrorStreamOptions { Stream = "e" },
            }));
        Assert.Contains("MaxAttempts", ex.Message);
    }

    [Fact]
    public async Task Skip_without_error_stream_throws()
    {
        var ex = await Assert.ThrowsAsync<StreamBusConfigurationException>(() =>
            Consume(new ConsumeOptions
            {
                Retry = new RetryPolicy { FailureMode = FailureMode.SkipAfterDeadline, MaxAttempts = 3 },
            }));
        Assert.Contains("ErrorStream", ex.Message);
    }

    [Theory]
    [InlineData(0)]
    [InlineData(-2)]
    public async Task Non_positive_max_attempts_throws(int max)
    {
        await Assert.ThrowsAsync<StreamBusConfigurationException>(() =>
            Consume(new ConsumeOptions
            {
                Retry = new RetryPolicy { FailureMode = FailureMode.SkipAfterDeadline, MaxAttempts = max },
                ErrorStream = new ErrorStreamOptions { Stream = "e" },
            }));
    }

    [Theory]
    [InlineData(0)]
    [InlineData(-1)]
    public async Task Non_positive_base_delay_throws(int ms)
    {
        await Assert.ThrowsAsync<StreamBusConfigurationException>(() =>
            Consume(new ConsumeOptions { Retry = new RetryPolicy { BaseDelay = TimeSpan.FromMilliseconds(ms) } }));
    }

    [Fact]
    public async Task Sub_one_multiplier_throws()
    {
        await Assert.ThrowsAsync<StreamBusConfigurationException>(() =>
            Consume(new ConsumeOptions { Retry = new RetryPolicy { Multiplier = 0.5 } }));
    }

    [Fact]
    public void Attempt_1_resolves_to_base_delay_without_jitter()
    {
        var log = new ListLogger();
        var policy = new RetryPolicy { BaseDelay = TimeSpan.FromMilliseconds(200), Multiplier = 2.0, MaxDelay = TimeSpan.FromSeconds(30), MaxJitter = TimeSpan.Zero };
        Assert.Equal(TimeSpan.FromMilliseconds(200), RetryDelays.ResolveDelay(policy, 1, new InvalidOperationException(), log));
        Assert.Equal(TimeSpan.FromMilliseconds(400), RetryDelays.ResolveDelay(policy, 2, new InvalidOperationException(), log));
        Assert.Equal(TimeSpan.FromMilliseconds(800), RetryDelays.ResolveDelay(policy, 3, new InvalidOperationException(), log));
    }

    [Fact]
    public void Delay_caps_at_max_delay_and_jitter_stays_bounded()
    {
        var log = new ListLogger();
        var policy = new RetryPolicy { BaseDelay = TimeSpan.FromSeconds(1), Multiplier = 10.0, MaxDelay = TimeSpan.FromSeconds(5), MaxJitter = TimeSpan.FromSeconds(1) };
        for (var attempt = 1; attempt <= 6; attempt++)
        {
            var delay = RetryDelays.ResolveDelay(policy, attempt, new InvalidOperationException(), log);
            Assert.True(delay >= TimeSpan.FromSeconds(5) && delay <= TimeSpan.FromSeconds(6));
        }
    }

    [Fact]
    public void Delay_calculator_overrides_knobs_with_one_based_attempts()
    {
        var log = new ListLogger();
        var seen = new List<int>();
        var policy = new RetryPolicy { DelayCalculator = (attempt, _) => { seen.Add(attempt); return TimeSpan.FromMilliseconds(attempt * 10); } };
        Assert.Equal(TimeSpan.FromMilliseconds(10), RetryDelays.ResolveDelay(policy, 1, new InvalidOperationException(), log));
        Assert.Equal(TimeSpan.FromMilliseconds(20), RetryDelays.ResolveDelay(policy, 2, new InvalidOperationException(), log));
        Assert.Equal(new[] { 1, 2 }, seen);
    }

    [Fact]
    public void Throwing_calculator_falls_back_and_warns()
    {
        var log = new ListLogger();
        var policy = new RetryPolicy
        {
            BaseDelay = TimeSpan.FromMilliseconds(200), Multiplier = 2.0,
            MaxDelay = TimeSpan.FromSeconds(30), MaxJitter = TimeSpan.Zero,
            DelayCalculator = (_, _) => throw new InvalidOperationException("boom"),
        };
        Assert.Equal(TimeSpan.FromMilliseconds(200), RetryDelays.ResolveDelay(policy, 1, new InvalidOperationException(), log));
        Assert.Contains(log.Lines, l => l.Contains("DelayCalculator"));
    }

    [Fact]
    public void Negative_calculator_result_clamps_to_zero()
    {
        var log = new ListLogger();
        var policy = new RetryPolicy { DelayCalculator = (_, _) => TimeSpan.FromMilliseconds(-5) };
        Assert.Equal(TimeSpan.Zero, RetryDelays.ResolveDelay(policy, 1, new InvalidOperationException(), log));
    }

    [Theory]
    [InlineData(1, true)]
    [InlineData(5, true)]
    [InlineData(6, false)]
    [InlineData(99, false)]
    [InlineData(100, true)]
    [InlineData(200, true)]
    public void Log_sampling_warns_early_then_every_hundredth(int attempt, bool warn)
    {
        Assert.Equal(warn, RetryDelays.ShouldLogRetryWarning(attempt));
    }
}
```

`ListLogger`: copy the Phase 5 `OffsetResolutionTests` sink verbatim (same file-local shape, `NullScope.Instance` scope). `StreamConsumerRegistry` 4-arg constructor: verify the current signature first — Task 3 adds `producers` as the fourth parameter (`supervisor, vhost, loggerFactory?, producers`); the test passes `null` factory + a real `StreamProducerRegistry` on a never-ready supervisor (validation throws before touching either). If Task 3 ordered parameters differently, match it and report. All validation precedes the readiness wait, so no broker contact (deterministic).

`TimeSpan.Zero` jitter bounds: `maxJitter = 0` → cap zero → zero jitter → exact knob values (assertable). `MaxJitter` negative-validation test: add `[Theory] -1` → config throw (mirror base-delay test; include it).

- [ ] **Step 2: Implement `ErrorEnvelopeTests.cs`**

```csharp
namespace StreamBus.UnitTests;

public sealed class ErrorEnvelopeTests
{
    private static ErrorEnvelope Sample() => new()
    {
        ErrorStream = "orders.error",
        SourceStream = "orders",
        SourcePartition = 2,
        SourcePartitionStream = "orders-2",
        SourceOffset = 41,
        Body = new byte[] { 1, 2, 3 },
        ExceptionType = "System.InvalidOperationException",
        ExceptionMessage = "boom",
        ExceptionStack = "stack",
        Attempts = 3,
        FailedAt = new DateTimeOffset(2026, 10, 13, 12, 0, 0, TimeSpan.Zero),
        MessageId = "m-1",
        CorrelationId = "c-1",
        RoutingValue = "k1",
        ErrorRoutingValue = "orders",
    };

    [Fact]
    public void Round_trip_preserves_every_field_and_body()
    {
        var sample = Sample();
        Assert.True(ErrorEnvelope.TryParse(sample.ErrorStream, sample.Body, sample.ToApplicationProperties(), out var parsed));
        Assert.NotNull(parsed);
        Assert.Equal(sample, parsed);
    }

    [Theory]
    [InlineData("source-stream")]
    [InlineData("source-partition-stream")]
    [InlineData("exception-type")]
    [InlineData("source-offset")]
    [InlineData("attempts")]
    [InlineData("error-routing-value")]
    public void Missing_required_key_fails_parse(string key)
    {
        var app = Sample().ToApplicationProperties();
        app.Remove("streambus.error." + key);
        Assert.False(ErrorEnvelope.TryParse("e", new byte[] { 9 }, app, out _));
    }

    [Fact]
    public void Garbage_values_fail_parse_without_throwing()
    {
        var app = Sample().ToApplicationProperties();
        app["streambus.error.source-offset"] = "not-a-number";
        Assert.False(ErrorEnvelope.TryParse("e", new byte[] { 9 }, app, out _));
    }

    [Fact]
    public void Replay_naming_is_stable_bounded_and_id_is_the_offset()
    {
        var a = ReplayNaming.ProducerName("orders-2");
        Assert.Equal(a, ReplayNaming.ProducerName("orders-2"));
        Assert.NotEqual(a, ReplayNaming.ProducerName("orders-3"));
        Assert.True(System.Text.Encoding.UTF8.GetByteCount(a) <= 255);
        Assert.Equal(41UL, ReplayNaming.PublishingId(41));
        var r = ReplayNaming.Reference("orders.error");
        Assert.Equal(r, ReplayNaming.Reference("orders.error"));
    }
}
```

`Assert.Equal(sample, parsed)` — record equality includes `Body` byte[] by reference! Records compare array properties with `EqualityComparer<byte[]>.Default` (reference equality) — `ToApplicationProperties` does NOT round-trip the body (body travels outside app props; `TryParse` takes `body` and assigns the same reference passed in). Here the same `sample.Body` reference is passed, so equality holds. (If this ever flakes, compare `parsed!.Body` with `Assert.Equal(sample.Body, parsed.Body)` — xUnit compares enumerables element-wise. Keep the record assert; the reference is identical by construction.)

- [ ] **Step 3: Implement `ErrorHandoffTests.cs`**

```csharp
namespace StreamBus.UnitTests;

public sealed class ErrorHandoffTests
{
    private static ErrorEnvelope Sample() => new()
    {
        ErrorStream = "e", SourceStream = "s", SourcePartition = 0, SourcePartitionStream = "s",
        SourceOffset = 7, Body = new byte[] { 1 }, ExceptionType = "X", ExceptionMessage = "m",
        ExceptionStack = "st", Attempts = 3, FailedAt = DateTimeOffset.UtcNow,
        MessageId = null, CorrelationId = null, RoutingValue = null, ErrorRoutingValue = "s",
    };

    [Fact]
    public async Task Publish_failure_retries_then_commits_once_after_confirmation()
    {
        var publisher = new ErrorPublisher(new StreamProducerRegistry(
            new StreamBusConnectionSupervisor(
                new StreamBusOptions { Vhost = "/", UserName = "a", Password = "b" },
                new NeverReadyConnector()), "/"));
        var publishes = 0;
        var commits = 0;
        var order = new List<string>();
        await publisher.HandOffAsync(Sample(),
            (_, _) =>
            {
                publishes++;
                order.Add($"publish#{publishes}");
                return publishes < 3
                    ? Task.FromException(new TimeoutException("blip"))
                    : Task.CompletedTask;
            },
            () => { commits++; order.Add("commit"); return Task.CompletedTask; },
            (_, _) => TimeSpan.Zero,
            TestContext.Current.CancellationToken);
        Assert.Equal(3, publishes);
        Assert.Equal(1, commits);
        Assert.Equal(new[] { "publish#1", "publish#2", "publish#3", "commit" }, order);
    }

    [Fact]
    public async Task Cancel_mid_handoff_commits_nothing()
    {
        var publisher = new ErrorPublisher(new StreamProducerRegistry(
            new StreamBusConnectionSupervisor(
                new StreamBusOptions { Vhost = "/", UserName = "a", Password = "b" },
                new NeverReadyConnector()), "/"));
        using var cts = new CancellationTokenSource();
        cts.Cancel();
        var commits = 0;
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() =>
            publisher.HandOffAsync(Sample(),
                (_, _) => Task.FromException(new TimeoutException("blip")),
                () => { commits++; return Task.CompletedTask; },
                (_, _) => TimeSpan.FromMilliseconds(50),
                cts.Token));
        Assert.Equal(0, commits);
    }
}
```

`NeverReadyConnector`: same three-line shape as Task 5 Step 1 (duplicate it — no cross-file sharing per the no-placeholders rule; each test file is self-contained). `ErrorPublisher` ctor takes `(producers, logger?)` — registry never touched (no readiness wait inside `HandOffAsync`), so construction is broker-free. `ThrowsAnyAsync` covers `OperationCanceledException` + `TaskCanceledException`.

- [ ] **Step 4: Verify**

```powershell
dotnet test tests/StreamBus.UnitTests/StreamBus.UnitTests.csproj -c Release
```

Expected: all green (78 existing + new policy/envelope/handoff tests), pristine output.

- [ ] **Step 5: Commit (with human approval)**

```powershell
git add tests/StreamBus.UnitTests/RetryPolicyTests.cs tests/StreamBus.UnitTests/ErrorEnvelopeTests.cs tests/StreamBus.UnitTests/ErrorHandoffTests.cs
git commit -m "test: add retry policy, envelope, and handoff unit tests"
```

Adaptation paragraph for Task 5 (analyzers are build errors): no `.ConfigureAwait(false)` inside `[Fact]`s; `TestContext.Current.CancellationToken` for every async test; `using var cts` disposal; `NullScope.Instance` logger scope; `Assert.Single(values, pred)` overload where a single-match assert reads better; forward slashes in any new `ProjectReference` (none expected).

---

### Task 6: Integration tests + G-1/G-5b gates + full Phase 6 gate

**Files:**
- Create: `tests/StreamBus.IntegrationTests/RetryIntegrationTests.cs`

**Interfaces:**
- Consumes: bus `ConsumeAsync`/`ReplayErrorStreamAsync`, `ConsumeOptions` with `Retry`/`ErrorStream`, real broker (app/password, unique streams, test-only admin `StreamSystem` + `StreamSpec` retention args per the Task 1 probe, `MessageHandler` + `Data.Contents` per Phase 4/5 as-built).
- Produces: G-1 + G-5b gate evidence; the Phase 6 completion gate.

- [ ] **Step 1: Implement `RetryIntegrationTests.cs`**

Shared harness: copy the Phase 5 `ConsumerIntegrationTests` harness verbatim into the new file (`ConnectAdminAsync`, `Unique`, `DisposeAsync` with `_streams`/`_superStreams`, `BusInstance`, `Body`, `Parse`, `Count`, `WaitForAsync`, `Rec`) and add:

```csharp
private static StreamSpec ErrorSpec(string stream)
{
    // Retention args per the Task 1 probe (x-max-age mandatory, FR-5.2).
    // Exact member/ctor shape comes from the probe report; the call reads:
    // await admin.CreateStream(ErrorSpec(error)); _streams.Add(error);
    ...
}
```

Error-envelope reader for assertions:

```csharp
private static (byte[] Body, Dictionary<string, string?> App) ReadError(
    MessageContext ctx, Message msg)
{
    var app = msg.ApplicationProperties is null
        ? new Dictionary<string, string?>()
        : msg.ApplicationProperties.ToDictionary(kv => kv.Key, kv => kv.Value?.ToString());
    return (msg.Data.Contents.ToArray(), app);
}
```

(Consume the error stream with a plain `AlwaysFirst` fresh reference in-test and capture raw `(ctx, msg)` pairs — use a test-only admin `RawConsumer` (same pattern as Phase 4/5 admin usage) OR a bus consumer recording `ctx.Body` + `ctx.Metadata.ApplicationProperties`. Prefer the bus consumer: `ctx.Metadata.ApplicationProperties` already carries the envelope dict. No raw admin consumer needed.)

Test (a) Skip handoff + envelope + advance-once (FR-4.5, FR-5.1):

```csharp
[Fact]
public async Task Skip_handoff_publishes_envelope_and_advances_once()
{
    var stream = Unique("retry-src");
    var error = Unique("retry-src.error");
    var admin = await ConnectAdminAsync();
    await using var _ = admin;
    await admin.CreateStream(new StreamSpec(stream));
    _streams.Add(stream);
    await admin.CreateStream(ErrorSpec(error));
    _streams.Add(error);
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
    var c = await inst.Bus.ConsumeAsync(stream,
        new ConsumeOptions
        {
            Reference = "grp-r1",
            Retry = new RetryPolicy { FailureMode = FailureMode.SkipAfterDeadline, MaxAttempts = 3 },
            ErrorStream = new ErrorStreamOptions { Stream = error },
        },
        ctx =>
        {
            var (_, seq) = Parse(ctx.Body);
            lock (attempts) attempts[seq] = attempts.TryGetValue(seq, out var n) ? n + 1 : 1;
            if (seq == 2) throw new InvalidOperationException("poison 2");
            lock (got) got.Add(seq);
            return Task.CompletedTask;
        }, TestContext.Current.CancellationToken);
    await WaitForAsync(() => Count(got) == 4, TimeSpan.FromSeconds(90), "post-handoff drain");
    await c.DisposeAsync();
    Assert.Equal(new[] { 0, 1, 3, 4 }, got);
    Assert.Equal(3, attempts[2]);
    Assert.All(attempts.Where(kv => kv.Key != 2), kv => Assert.Equal(1, kv.Value));
    Assert.Equal(4UL, await inst.Bus.GetConsumerOffsetAsync(stream, "grp-r1", 0, TestContext.Current.CancellationToken));
    // envelope: consume the error stream, exactly one message, all FR-5.1 fields
    var errors = new List<(byte[] Body, IReadOnlyDictionary<string, object?> App)>();
    var ec = await inst.Bus.ConsumeAsync(error,
        new ConsumeOptions { Reference = "grp-r1-err", Offset = ConsumeOffset.AlwaysFirst },
        ctx => { lock (errors) errors.Add((ctx.Body.ToArray(), ctx.Metadata.ApplicationProperties)); return Task.CompletedTask; },
        TestContext.Current.CancellationToken);
    await WaitForAsync(() => Count(errors) == 1, TimeSpan.FromSeconds(60), "error drain");
    await ec.DisposeAsync();
    var (eBody, eApp) = errors[0];
    Assert.Equal("k:2", System.Text.Encoding.UTF8.GetString(eBody));
    string? G(string k) => eApp.TryGetValue("streambus.error." + k, out var v) ? v?.ToString() : null;
    Assert.Equal(stream, G("source-stream"));
    Assert.Equal("0", G("source-partition"));
    Assert.Equal("2", G("source-offset"));
    Assert.Equal("3", G("attempts"));
    Assert.Contains("InvalidOperationException", G("exception-type") ?? "");
    Assert.Equal("poison 2", G("exception-message"));
    Assert.False(string.IsNullOrWhiteSpace(G("exception-stack")));
    Assert.False(string.IsNullOrWhiteSpace(G("failed-at")));
    Assert.Equal(stream, G("error-routing-value"));
}
```

Replace the awkward attempts line with a clean pin: `Assert.All(attempts.Where(kv => kv.Key != 2), kv => Assert.Equal(1, kv.Value));` (`Assert.All` exists in xUnit v3 ✓). MaxAttempts=1 variant: second fact, same shape with `MaxAttempts = 1`, asserting `attempts[2] == 1`, `got == {0,1,3,4}`, offset 4, envelope attempts "1". (Two facts sharing private helpers `RunSkipScenario(int maxAttempts)` returning `(got, attempts, errors)` — write the helper once, both facts call it. Helper code must be complete, not "same as above".)

Test (b) G-5b kill-during-retry (Strict window):

```csharp
[Fact]
public async Task Strict_retry_window_survives_kill_without_skip()
{
    var stream = Unique("retry-g5b");
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
    var entered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
    var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
    var first = new List<int>();
    await using (var inst = await BusInstance.StartAsync("i"))
    {
        var c1 = await inst.Bus.ConsumeAsync(stream, new ConsumeOptions { Reference = "grp-r2" }, ctx =>
        {
            var (_, seq) = Parse(ctx.Body);
            if (seq == 1 && !entered.Task.IsCompleted)
            {
                entered.TrySetResult();
                release.Task.GetAwaiter().GetResult(); // block inside the handler: the retry/dispatch window is now held open
            }
            lock (first) first.Add(seq);
            return Task.CompletedTask;
        }, TestContext.Current.CancellationToken);
        Assert.True(await Task.WhenAny(entered.Task, Task.Delay(TimeSpan.FromSeconds(30))) == entered.Task, "handler entered on 1");
        await c1.DisposeAsync(); // kill mid-dispatch (hangs until release? NO — see note)
        release.TrySetResult();
    }
    ...
}
```

PROBLEM (resolve — the kill must land while the partition is stalled, but `DisposeAsync` awaits the in-flight dispatch by design, so disposing while the handler blocks deadlocks the test). Resolution: the block must be retry-sleep, not handler-block. Deterministic shape: handler fails seq 1 twice with attempts recorded, third attempt blocks on the gate; test waits for `entered` (third attempt entered = two failures done = Strict retry loop proven active), then disposes the CONSUMER REGISTRATION via a second path that cancels first: `await c1.DisposeAsync()` cancels the family token → retry/dispatch sees cancellation → handler's `release.Task` wait must ALSO observe cancellation or disposal hangs. So the test handler waits with cancellation: capture `ctx.CancellationToken` (the attach loop token, cancelled on dispose) and `await Task.Delay(-1, ctx.CancellationToken)`-style wait instead of a raw gate:

```csharp
var c1 = await inst.Bus.ConsumeAsync(stream, new ConsumeOptions { Reference = "grp-r2" }, async ctx =>
{
    var (_, seq) = Parse(ctx.Body);
    var n = attempts-map increment;
    if (seq == 1 && n == 3) { entered.TrySetResult(); await Task.Delay(Timeout.Infinite, ctx.CancellationToken); }
    if (seq == 1 && n < 3) throw new InvalidOperationException("flaky 1");
    lock (got) got.Add(seq);
}, ...);
Assert entered; // third attempt entered: two Strict retries completed
await c1.DisposeAsync(); // cancels attach token -> infinite wait aborts -> disposal completes
```

then recreate on a FRESH instance (fresh guard — only a broker resume passes), handler records into `second`, succeeding always: expect `second == {1,2,3,4}` (redelivery starts at un-stored 1: 0 was stored with Every=1 default), `first` contains `{0,1,1}` (0 success, 1 fail, 1 fail — third attempt never recorded success), combined per-order assertions + final offset 4. This pins G-5b: the kill landed mid-retry-window (after 2 failures, inside the 3rd attempt) and nothing was skipped. (No `GetAwaiter().GetResult()` blocking inside handlers anywhere — always await with the context token.)

Test (c) G-1 load + failover with retry (FR-4.7):

```csharp
[Fact]
public async Task Retry_preserves_order_under_load_and_failover()
{
    var super = Unique("retry-g1");
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
    var attempts = new Dictionary<(string Key, int Seq), int>();
    Func<string, Func<ConsumeContext, Task>> handler = name => ctx =>
    {
        var (key, seq) = Parse(ctx.Body);
        lock (attempts) attempts[(key, seq)] = attempts.TryGetValue((key, seq), out var n) ? n + 1 : 1;
        if (attempts[(key, seq)] == 1) throw new InvalidOperationException($"first-attempt flake {key}:{seq}");
        lock (all) all.Add((key, seq, name, ctx.Partition, ctx.Offset));
        return Task.CompletedTask;
    };
    var a = await BusInstance.StartAsync("ga");
    var b = await BusInstance.StartAsync("gb");
    var aDisposed = false;
    try
    {
        await a.Bus.ConsumeAsync(super,
            new ConsumeOptions { IsSuperStream = true, Reference = "grp-r3" }, handler("ga"), TestContext.Current.CancellationToken);
        await b.Bus.ConsumeAsync(super,
            new ConsumeOptions { IsSuperStream = true, Reference = "grp-r3" }, handler("gb"), TestContext.Current.CancellationToken);
        await WaitForAsync(() => all.Any(x => x.Instance == "ga") && Count(all) >= 2, TimeSpan.FromSeconds(60), "A to own a partition");
        // concurrent load + failover mid-stream
        var load = Task.Run(async () =>
        {
            await using var lp = await BusInstance.StartAsync("load");
            try
            {
                foreach (var seq in Enumerable.Range(7, 6))
                    foreach (var key in keys)
                        await lp.Bus.PublishAsync(super, Body($"{key}:{seq}"),
                            new PublishOptions { IsSuperStream = true, RoutingKey = key }, TestContext.Current.CancellationToken);
            }
            finally { await lp.DisposeAsync(); }
        });
        await a.DisposeAsync();
        aDisposed = true;
        await load;
        await WaitForAsync(() => Count(all) == 24, TimeSpan.FromSeconds(120), "post-failover drain");
        Assert.Equal(24, all.Count);
        Assert.Equal(24, all.Select(x => (x.Key, x.Seq)).Distinct().Count());
        foreach (var key in keys)
            Assert.Equal(Enumerable.Range(1, 12), all.Where(x => x.Key == key).Select(x => x.Seq));
        foreach (var p in new[] { 0, 1 })
        {
            var received = all.Where(x => x.Partition == p).ToList();
            var stored = await b.Bus.GetConsumerOffsetAsync(super, "grp-r3", p, TestContext.Current.CancellationToken);
            if (received.Count == 0) Assert.Null(stored);
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

(Every message fails its first attempt (Strict unbounded default backoff) then succeeds — retry is in every message's path while load publishes concurrently and ownership fails over. Tail assertions mirror Phase 5 test (b): stored == max-received per non-empty partition, null when empty. `attempts` lock covers the read-then-throw check — the check-then-act is under the same lock in the snippet above? The snippet reads `attempts[(key,seq)]` outside the lock on the second line — fix: do the whole read/increment/throw-decision inside ONE lock, recording the decision to a local, then throwing outside the lock (never throw while holding a lock... actually throwing inside a `lock` is safe in C# (finally releases), but keep it clean: compute `fail` bool inside lock, `if (fail) throw` after.)

Test (d) double-replay idempotency (FR-5.3):

```csharp
[Fact]
public async Task Replay_twice_stores_once()
{
    // ... source + error streams, 3 messages, Skip policy with always-fail on seq 1 ...
    // after handoff: await bus.ReplayErrorStreamAsync(error) == 1
    // read source with AlwaysFirst fresh ref -> bodies {k:0,k:1,k:2,k:1} (one replayed copy)
    // await bus.ReplayErrorStreamAsync(error) == 1 again
    // re-read source -> STILL exactly {k:0,k:1,k:2,k:1} (broker dedup on stable name + id)
    // malformed message: publish raw garbage to error stream, replay returns 1 (garbage skipped, no throw)
}
```

Source re-reads need fresh references per read (`grp-rp1`, `grp-rp2`) with `AlwaysFirst`. Count assertions exact. The garbage message: `await inst.Bus.PublishAsync(error, Body("not-an-envelope"), ...)` then replay returns 1 and source unchanged. (Garbage advances the replay consumer past itself — assert replay still returns 1 both before... order: replay#1 (1) → read → publish garbage → replay#2 returns 1 (replays the real one again, deduped; garbage skipped) → read unchanged. Precise and deterministic.)

Test (e) shared vs per-stream forms (FR-5.4): two sources `s1`, `s2` sharing one error stream; `s1` consumer uses `ErrorStreamOptions { Stream = shared }` (routing defaults to source name), `s2` uses `{ Stream = shared, RoutingValue = "custom-2" }`; poison one message each; assert error stream holds both envelopes with `error-routing-value` == `s1-name` and `"custom-2"` respectively.

Test (f) mode coexistence (FR-4.6): stream A Strict flaky-then-succeed (assert full ordered drain), stream B Skip always-fail on one message (assert handoff + advance). One fact, two consumers, same instance, different references.

Test (g) retry_blocked + lag visibility (FR-9.3): plain stream, 1 message, Strict; handler enters (gate TCS) then waits on `ctx.CancellationToken`-infinite delay; MeterListener (Phase 5 test (h) shape) observes `streambus_partition_retry_blocked == 1` with `partition == "0"` and `streambus_partition_lag > 0`; dispose consumer (cancels wait); observe back to 0. `release` set in finally (same anti-hang shape as Phase 5 (h)).

- [ ] **Step 2: Verify (full Phase 6 gate)**

```powershell
dotnet test StreamBus.slnx -c Release
Select-String -Path src/StreamBus/*.cs -Pattern "Reliable(Producer|Consumer|SuperStream)"
Select-String -Path src/StreamBus/*.cs -Pattern "CreateStream|CreateSuperStream|DeleteStream|DeleteSuperStream"
```

Expected: full suite green (78 + new unit + 8 Phase 5 consumer + 7 new retry integration + rest); both `Select-String` checks return nothing; broker version recorded (expect 4.2.6; G-3 not required).

- [ ] **Step 3: Commit (with human approval)**

```powershell
git add tests/StreamBus.IntegrationTests/RetryIntegrationTests.cs
git commit -m "test: add retry, handoff, and replay integration probes with G-1/G-5b gates"
```

---

## Follow-ups for Phase 7+ (tracked, no code in this phase)

- Stable FR-6 result-path producer naming stays Phase 7; replay-scoped stable names (`streambus-replay-{sha}`) are independent and documented here.
- `OnRetryExhausted` abandon-on-cancel leaves the hook task running with a fault-logging continuation (same ceiling as hung handlers — NFR-4 family).
- `DelayCalculator` returning huge delays stalls the partition that long (caller's choice; clamped only when negative).
- Replay idle timeout (5 s) is a heuristic on a broker with no end offset; revisit only if the client exposes a true end offset (same unmeasurable-end constraint as Phase 5 lag).
- Broker-total backlog lag remains unmeasurable (`LastOffset` unusable, evidenced Phase 5) — FR-9.3 visibility is consumer-held lag + retry_blocked only.
- Super-stream error destinations and super-stream replay targets are v1 config errors — revisit if a later phase needs them.
- `retry_blocked` keeps the static `failure_mode="strict"` tag until a later phase drives real modes (Phase 5 follow-up closed: values are now live 1/0).

---

## Self-Review

- Spec coverage: FR-4.1 (Task 3 loop placement + FR-4.2 RawConsumer invariant in constraints), FR-4.2 (constraints + existing paths), FR-4.3 (Tasks 1 knobs + Task 5 numbering/cap/override tests), FR-4.4 (Tasks 1+3 Strict + Task 5 combo tests + Task 6b), FR-4.5 (Tasks 2–3 handoff + Task 5 orchestration + Task 6a advance-once), FR-4.6 (Task 6f), FR-4.7/G-1 (Task 6c), FR-5.1 (Tasks 2 envelope + Task 6a field asserts), FR-5.2 (Task 1 probe + Task 6 `ErrorSpec` retention creation; effective-retention read-back per probe or explicit "none"), FR-5.3 (Tasks 2+4 replay + Task 6d double-replay), FR-5.4 (Task 6e both forms), FR-9.3 (Task 6g blocked+lag pins), FR-9.1 retry_blocked values (Tasks 3 drive + 6g), G-5b (Task 6b).
- FR-2.8/Phase 9, FR-6/Phase 7, FR-7/Phase 8 untouched by design (listed out-of-scope).
- Placeholder scan: no TBD/TODO; every code step ships complete source; probe-dependent branches are exact conditionals with commands (Task 1 a–e, Task 2 adaptation paragraph, Task 4 transport locked in Task 2); the Task 6a awkward line is replaced by the `Assert.All` form; no "similar to Task N" (harness copy is explicit: "copy verbatim", each new helper complete).
- Type consistency: `RetryPolicy`/`ErrorStreamOptions`/`ErrorHandoff`/`ErrorEnvelope`/`ReplayNaming`/`ErrorPublisher`/`RetryDelays` names identical across Tasks 1–6; `HandOffAsync(envelope, publish, commitStore, delayFor, ct)` call sites (Task 3 `ExhaustAsync` + Task 5 tests) match; `PublishReplayAsync(body, publishingId, ct)` handle method (Task 2/4) matches; `ReplayAsync(errorStream, targetStream, ct)` registry method matches `Bus` delegation; `ShouldStoreOnSuccess(ulong)`/`Stored(offset)`/`IsStored` call sites unchanged from Phase 5; `StreamConsumerRegistry` 4th ctor param `producers` used identically in Tasks 3–5.
- Review Focus: each line pinned above (Task 5 ×3, Task 6 ×2).
