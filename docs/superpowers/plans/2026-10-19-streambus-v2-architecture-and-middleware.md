# StreamBus v2 — Architecture Remediation & Middleware Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Close the liveness loop (detect and recover from connection/consumer death and lost confirms), consolidate the drifted API surface, and add user-composable publish/consume middleware (any number of filters, executed in registration order) with a header carrier — per spec, minus configuration-binding scope (B8 dropped).

**Architecture:** RabbitMQ.Stream.Client is kept as-is; resilience is owned in-library via a supervising connection monitor + client death callbacks + per-handle corruption invalidation. The publish and consume facades collapse into one generic core; ordered user pipelines compose around serialize/send and around the outer per-message handler delegate. Configuration remains explicit and code-first — options are passed through configurator method calls, with no `IConfiguration`/`appsettings.json` binding. Every remediation lands with regression tests before the next milestone starts.

**Tech Stack:** .NET 10 (net10.0), RabbitMQ.Stream.Client 1.12.1 (pinned), RabbitMQ.Client 7.2.2 (dead-letter classic queues only), Microsoft.Extensions.Hosting 10.0.12, xunit.v3

**Spec:** `docs/superpowers/specs/2026-10-18-streambus-v2-architecture-and-middleware-design.md` — the plan argues from the spec; executors read both. Acceptance IDs V2-A.x/V2-B.x/V2-C.x/V2-D.x and gates V2-G1..G7 are defined there.

## Global Constraints

- Target framework `net10.0`; `TreatWarningsAsErrors` stays ON for every task; central package management stays; do NOT bump `RabbitMQ.Stream.Client` / `RabbitMQ.Client` versions in this plan.
- Branch: `v2` (created in Task 0). One branch, frequent commits, one task per commit.
- Preserve verbatim (spec §4): (1) strict per-partition handler ordering, at most one in-flight handler per partition; (2) offset advances only after handler success (cadence rules) or **confirmed** error-destination publish, never before, with the monotonic-window guard rejecting regression; (3) never skip before hand-off confirms; exhaustion is confirmed-then-commit; (4) effectively-once: publishing id = source offset, resume = target's last confirmed publishing id recomputed each attach, source never stores offsets, `QuerySequence == 0` resolves to replay-from-first under dedup; (5) unique process-wide producer names (broker dedup trap); replay/EO names stable per (source-partition → target); (6) SAC promotion listener never throws (client fallback for a throwing listener = scroll forward = skipped backlog); (7) `ConsumeAsync` returns only after the broker confirms the subscription.
- Public API adds only what the spec defines; every removed/changed public member goes into CHANGELOG.md; version becomes 0.2.0.
- **Configuration is explicit — no `IConfiguration` surface (ADR: supersedes spec B8).** Callers configure the library by calling methods: `AddStreamBus((context, cfg) => cfg.Host(host, vhost, h => ...))`. Do NOT add `IConfiguration`/`IConfigurationSection` overloads, `string sectionName` overloads, `Bind(...)` methods, section-key parsing, unknown-key warnings, or per-field code-vs-section precedence. Every knob (including `MonitorInterval`, `HandoffConfirmTimeout`, `ErrorQueueOptions.ConfirmTimeout`) must be reachable from a configurator method; nothing may be settable only from a config file.
- **Pipelines are additive and ordered (C2/C3 contract).** Users may register any number of publish filters and any number of consume filters. Order of execution is exactly registration order; first registered = outermost wrapper = runs first on the way in and last on the way out. No filtering, reordering, deduplication or cap (beyond rejecting an accidental duplicate registration of the same filter type). Filters compose; each one may call `next` exactly once.
- Line anchors in this plan reflect the tree as of 2026-10-18. If a symbol name or line drifted, locate by symbol (`grep -n "<symbol>" src/StreamBus`) and apply the edit to the located member — the change itself is fully specified everywhere.
- Every task's verify gate BEFORE its commit: `dotnet build StreamBus.slnx` reports **0 warnings, 0 errors** and `dotnet test tests/StreamBus.UnitTests` is fully green (the suite takes ~4s locally). Integration tests need a broker on `localhost:5552` (env `STREAMBUS_USER`/`STREAMBUS_PASSWORD`).
- New test naming follows the repo's lowercase-snake test-method convention (see existing `RetryIntegrationTests`).

## Review Focus

The five failure modes from the spec that are most likely to bite a real user, each with the verification that pins it:

1. **Idle connection death stalls consumers silently (green-but-dead)** — pinned by unit `ConsumerDeathTests` callback tests (Task 2), integration `ConsumerDeathTests.Connection_closed_while_idle_reattaches_within_bounds` (Task 19), and `BrokerRestartTests` (Task 19). A reasonable person expects a dead consumer connection to detach within ~5s and reattach from the stored offset.
2. **Hand-off confirm wait wedges the partition dispatch gate forever** — pinned by `ConfirmationCloseTests.Handoff_returns_transient_after_timeout_and_message_stays_replayable` (Task 1). Expect a lost confirm on the error hand-off to surface as a retryable transient within `ErrorHandoffTimeout`.
3. **Replay republish silently swallowed by broker dedup (cross-source name collision)** — pinned by `ReplayNamingTests.Replay_names_are_unique_per_source_stream` (Task 10). Expect replay after naming change to never drop envelopes.
4. **Pipeline semantics drift (ordering, skip, exception classification)** — pinned by `PipelineSemanticsTests` ordering/skip/classification matrix (Task 16). Task 17 re-asserts skip-in-EO rejection through integration. Expect filters to run registration-order; skip to advance offset exactly once; skip to be rejected at config time under EO.
5. **Docs/README factual drift again** — pinned by DocsSnippets compile project (Task 13) and llms.txt grep checks (Task 13). Expect the README "90% case" to compile against shipped API verbatim.

---

# Milestones

- **M1 — Liveness mechanics** (Tasks 1–3, gate V2-G1)
- **M2 — Supervision & truth** (Tasks 4–7, gate V2-G2)
- **M3 — Surface consolidation** (Tasks 8–11, gate V2-G3)
- **M4 — Docs honesty** (Task 13, gate V2-G4)
- **M5 — Properties + publish pipeline** (Tasks 14–15, gate V2-G5)
- **M6 — Consume pipeline + EO + telemetry** (Tasks 16–17, gate V2-G6)
- **M7 — CI & chaos** (Tasks 18–19, gate V2-G7)

# Task 0: Branch & baseline

**Files:** none (repo state)

**Interfaces:** Produces: branch `v2` at current HEAD; green baseline proof.

- [ ] **Step 0.1: Create branch**

Run: `git checkout -b v2` (from `D:\Personal\Projects\StreamBus`; ensure the working tree is clean first: `git status --porcelain` must be empty except `docs/superpowers/specs/*` and `docs/superpowers/plans/*`, which get committed in this task).

- [ ] **Step 0.2: Commit spec + plan**

```bash
git add docs/superpowers/specs/2026-10-18-streambus-v2-architecture-and-middleware-design.md
git add docs/superpowers/plans/2026-10-19-streambus-v2-architecture-and-middleware.md
git commit -m "docs(v2): remediation + middleware design and plan"
```

- [ ] **Step 0.3: Baseline proof**

Run: `dotnet build StreamBus.slnx` then `dotnet test tests/StreamBus.UnitTests`
Expected: 0 warnings; all unit tests pass (247 as of 2026-10-18; record actual count in the PR description for M1).

# Task 1 (A3): Confirm-path hardening — close-callback cancellation + bounded hand-off

**Files:**
- Modify: `src/StreamBus/ProducerHandles.cs` (`ConfirmationTracker`)
- Modify: `src/StreamBus/Connector.cs` (three `RawProducerConfig` creation sites, ~lines 80–85, 94–100, 114–121)
- Modify: `src/StreamBus/ProducerRegistry.cs` (`PublishEnvelopeAsync`, ~lines 59–87)
- Modify: `src/StreamBus/ConsumeOptions.cs` (new `HandoffConfirmTimeout` member)
- Modify: `src/StreamBus/Configuration/StreamBusConfigurator.cs` (endpoint setter)
- Modify: `src/StreamBus/ErrorPublisher.cs` (thread timeout through `HandOffAsync` → `PublishEnvelopeAsync`)
- Test: `tests/StreamBus.UnitTests/ConfirmationCloseTests.cs` (new)

**Interfaces:**
- Consumes: `ConfirmationTracker.PublishAsync/PublishWithIdAsync` (existing); `RawProducerConfig` (client type).
- Produces: `ConfirmationTracker.OnConnectionLost()` (idempotent, call-any-thread, no-throw); `IStreamProducerHandle`-level transient semantics unchanged (callers already treat `StreamBusTransientException` as invalidate-and-retry); `ConsumeOptions.HandoffConfirmTimeout` (`TimeSpan`, default 10s); `IReceiveEndpointConfigurator.ErrorHandoffTimeout(TimeSpan)`.

- [ ] **Step 1: ConfirmationTracker gains a close hook**

In `ProducerHandles.cs`, add to `ConfirmationTracker` (align member names to the existing private fields for pending tasks/ids — locate via `grep -n "class ConfirmationTracker"`; keep the existing confirm/failure paths untouched):

```csharp
/// <summary>Publisher connection dropped before confirms arrived. The client never
/// invokes ConfirmHandler for the lost publishing ids; resolve every pending
/// waiter as transient so awaiters stop waiting and the registry invalidates
/// the (now-closed) handle. Idempotent; safe from any thread; must not throw.</summary>
public void OnConnectionLost()
{
    var ex = new StreamBusTransientException(
        "Producer connection closed before confirmation; publish outcome unknown.");
    // Cancel every pending waiter with `ex`, clear the pending table, and
    // release any waiters registered via ct so they observe the resolution.
    CancelAll(ex); // reuse the existing CancelAll(Exception) if present; otherwise implement it here:
}
```

If `CancelAll(Exception)` does not exist (it may currently only support success/completion), write it as: under the tracker's existing lock, `TrySetException(ex)` on every task completion source in the pending table, clear the table, and leave publishing-id allocation to the next handle (trackers are per-handle).

- [ ] **Step 2: Wire the hook into every RawProducerConfig**

In `Connector.cs`, all three producer config sites gain:

```csharp
ConnectionClosedHandler = _ => tracker.OnConnectionLost(),
```

where `tracker` is the `ConfirmationTracker` instance already bound as the site's `ConfirmHandler` target (if a site builds configs before the tracker exists, restructure minimally so the tracker is constructed first and both handlers close over it). Note: `ConnectionClosedHandler` is `Action<string>` on the client's `RawProducerConfig`.

- [ ] **Step 3: Bound the hand-off confirm wait**

In `ProducerRegistry.PublishEnvelopeAsync` (the no-timeout path identified in review):

```csharp
// BEFORE: await handle.SendEnvelopeAsync(envelope, ct);  // unbounded — wedged dispatch gate
// AFTER:
using var attemptCts = CancellationTokenSource.CreateLinkedTokenSource(ct);
attemptCts.CancelAfter(confirmTimeout);
try
{
    await handle.SendEnvelopeAsync(envelope, attemptCts.Token).ConfigureAwait(false);
}
catch (OperationCanceledException) when (!ct.IsCancellationRequested)
{
    await InvalidateAsync(handle).ConfigureAwait(false);
    throw new StreamBusTransientException(
        $"Error hand-off publish was not confirmed within {confirmTimeout}; outcome unknown. A retry will re-publish the envelope.");
}
```

`confirmTimeout` is a new parameter threaded from `ErrorPublisher.HandOffAsync(...)` — signature becomes `HandOffAsync(StreamProducerHandle publisher, StreamBusErrorEnvelope envelope, ...,Func<Exception,bool> isFatal, TimeSpan confirmTimeout, CancellationToken ct)`. Inside `HandOffAsync`'s existing retry loop, this exception classifies as transient (it already does — `StreamBusTransientException`) and the loop re-attempts; per-attempt logging text gains `timeout={confirmTimeout.TotalSeconds}s`.

- [ ] **Step 4: Surface the knob**

`ConsumeOptions` gains `public TimeSpan HandoffConfirmTimeout { get; init; } = TimeSpan.FromSeconds(10);`
`IReceiveEndpointConfigurator` gains `void ErrorHandoffTimeout(TimeSpan timeout);` (positive, clamped to ≥ 1s by the configurator; sets the endpoint descriptor's copy of `HandoffConfirmTimeout` used when deriving `ConsumeOptions` — follow the same derivation path the endpoint's retry options take).
`ErrorPublisher.HandOffAsync` callers in `StreamPartitionConsumer.ExhaustAsync` pass `_options.HandoffConfirmTimeout`.

- [ ] **Step 5: Tests (`tests/StreamBus.UnitTests/ConfirmationCloseTests.cs`)**

```csharp
public class ConfirmationCloseTests
{
    [Fact]
    public async Task Connection_lost_mid_wait_fails_all_pending_confirmations_transiently() { ... }

    [Fact]
    public async Task Confirmation_idempotent_when_connection_lost_repeatedly() { ... }

    [Fact]
    public async Task Handoff_returns_transient_after_timeout_and_message_stays_replayable() { ... }
}
```

Test 1 construction: build a `ConfirmationTracker` (it must be publicly constructible for tests already — existing `PublishTests` constructs it; follow that fixture style), call `PublishWithIdAsync(...)` with a fake producer whose `Send` succeeds but never confirms, invoke `tracker.OnConnectionLost()`, and assert the returned task throws `StreamBusTransientException` with message containing "outcome unknown"; assert a racing waiter resolves rather than hanging (use a 5s `WaitAsync` timeout in the test; if the test would hang the assertion fails — no flakiness invitation).
Test 3: call `registry.PublishEnvelopeAsync` against a scripted `IStreamProducerHandle` whose `SendEnvelopeAsync` never completes when given a token that was NOT cancelled by the caller (use the scripted-handle pattern from `ConnectionSupervisorTests`/`PublishTests` — hand-rolled fakes, unexercised methods throw). Assert `StreamBusTransientException`, and that invalidation ran (existing registry invalidation observable pattern).

- [ ] **Step 6: Verify**

Run: `dotnet test tests/StreamBus.UnitTests --filter "FullyQualifiedName~ConfirmationCloseTests"` then full suite + build.
Expected: all green; the earlier `Strict_retry_window_survives_kill_without_skip` integration test still passes against a broker (run once manually if broker available).

- [ ] **Step 7: Commit**

```bash
git add src/StreamBus/ProducerHandles.cs src/StreamBus/Connector.cs src/StreamBus/ProducerRegistry.cs src/StreamBus/ConsumeOptions.cs src/StreamBus/Configuration/StreamBusConfigurator.cs src/StreamBus/ErrorPublisher.cs src/StreamBus/StreamPartitionConsumer.cs tests/StreamBus.UnitTests/ConfirmationCloseTests.cs
git commit -m "fix(resilience): close-callback cancels pending confirms; bounded error-hand-off waits (A3)"
```

# Task 2 (A1): Consumer liveness — death callbacks + watchdog

**Files:**
- Modify: `src/StreamBus/StreamPartitionConsumer.cs` (`RawConsumerConfig` assembly ~lines 214–229; attach park loop ~lines 253–265; disposal paths)
- Modify: `src/StreamBus/Connector.cs` ONLY IF the creation seam wraps the returned consumer — locate via `grep -n "CreateRawConsumerAsync"`; if the seam returns the client's `RawConsumer` directly, no change here.
- Test: `tests/StreamBus.UnitTests/ConsumerDeathTests.cs` (new)

**Interfaces:**
- Consumes: existing `attach` failure machinery (`FailAttach`, transient classification, backoff loop) — nothing new needed from it.
- Produces: `ConnectionClosedHandler`/`MetadataHandler` on the RAW consumer config route to `FailAttach` with a transient exception; the parked attach loop wakes every 5s and, if the consumer handle reports closed, fails the attach the same way. Behavior contract: a closed consumer subscription detaches ≤ ~5s (callback path: immediately) and recreates on the next attempt, resuming from the stored offset.

- [ ] **Step 1: Track the current consumer handle for probing**

In `StreamPartitionConsumer`, add a tiny holder so the config callbacks (built before the consumer exists) can observe the instance:

```csharp
// Configuration callbacks are built before CreateRawConsumerAsync returns;
// the holder lets them see the instance without resurrecting races on `consumer`.
private sealed class ConsumerHolder
{
    private volatile RawConsumer? _current;
    public RawConsumer? Current => _current;
    public void Set(RawConsumer c) => _current = c;
    public void Clear() => _current = null;
}

private readonly ConsumerHolder _consumerHandle = new();
```

Set `Set(consumer)` right after `CreateRawConsumerAsync` returns (next to the existing `_firstAttach.TrySetResult()`), and `Clear()` in the same `finally`/pre-close position where `consumer` is closed/disposed today. If `CreateRawConsumerAsync` returns a wrapper rather than `RawConsumer`, expose `IsClosed` on that wrapper and use its type in the holder instead.

- [ ] **Step 2: Wire the two death callbacks**

In the `RawConsumerConfig` initializer add:

```csharp
ConnectionClosedHandler = reason =>
{
    // Client marks the consumer closed and delivers nothing further; the
    // only recovery is our attach loop closing and recreating. Transient by
    // definition: the stream name, reference and offsets are unaffected.
    _consumerHandle.Clear();
    attach.FailAttach(
        new StreamBusTransientException($"Consumer subscription closed by the stream client ({reason})."),
        "consumer_connection_closed");
    return Task.CompletedTask; // adjust to the client's delegate signature (may be void/Func<string,Task>)
},
MetadataHandler = metadata =>
{
    // Metadata updates can invalidate the subscription (e.g., leader change);
    // the client closes the consumer when that happens. A probe on the
    // current handle keeps this behavior uniform and avoids double-close
    // races from FailAttach on a still-open subscription.
    if (_consumerHandle.Current?.IsClosed ?? false)
    {
        _consumerHandle.Clear();
        attach.FailAttach(
            new StreamBusTransientException($"Stream metadata change closed the subscription ({metadata})."),
            "consumer_metadata_change");
    }
    // MUST NOT THROW — the client's fallback for a throwing listener is.
    // OffsetNext = skip the backlog (see invariant 6).
    return Task.CompletedTask;
},
```

The `FailAttach` call sites must match the existing signature (locate via `grep -n "FailAttach"`; if it takes only an exception, drop the reason string and embed the reason in the message — observability of the reason is required by V2-A1 either via the second arg or the message).

- [ ] **Step 3: Press the park loop with a watchdog**

Replace the current parked wait (the line `await attach.Failure.Task.WaitAsync(loopCt).ConfigureAwait(false);` — locate exactly; it may sit inside a larger structure) with:

```csharp
// Park = wake on attach failure OR a 5s liveness watchdog. The watchdog
// exists for the case where the client lost the connection without firing
// the callbacks (defensive; callbacks are the primary signal).
while (true)
{
    try
    {
        await attach.Failure.Task.WaitAsync(TimeSpan.FromSeconds(5), loopCt).ConfigureAwait(false);
        break; // attach failed -> existing close/backoff/recreate path runs
    }
    catch (TimeoutException)
    {
        var current = _consumerHandle.Current;
        if (current is not null && current.IsClosed)
        {
            _consumerHandle.Clear();
            attach.FailAttach(
                new StreamBusTransientException("Consumer handle reports closed while parked (watchdog)."),
                "consumer_watchdog");
            break;
        }
    }
    // loop continues: next WaitAsync(5s) — no allocation churn beyond WaitAsync
}
```

(`WaitAsync(TimeSpan, CancellationToken)` throws `TimeoutException` on elapse — cancellation still propagates as `OperationCanceledException` and is handled by the existing loop structure.)

- [ ] **Step 4: Tests (`tests/StreamBus.UnitTests/ConsumerDeathTests.cs`)**

Follow the scripted-fake pattern from `AcceptanceTests.cs` (fakes target the library's own seams; unexercised methods throw).

```csharp
public class ConsumerDeathTests
{
    [Fact]
    public async Task Connection_closed_callback_fails_attach_and_loop_recreates() { ... } // fake handle: raise closed -> assert FailAttach path, next attach attempt still made

    [Fact]
    public async Task Metadata_change_on_closed_handle_fails_attach_exactly_once() { ... }

    [Fact]
    public async Task Park_watchdog_detects_closed_handle_within_6s() { ... } // real clock allowed: park 5s budget; assert < 6.5s wall

    [Fact]
    public async Task Watchdog_does_not_fail_attach_while_handle_is_open() { ... } // parked 12s, closed never set -> attach still pending, no failure signals
}
```

Fake seam notes: the fake `IStreamSystemHandle.CreateRawConsumerAsync` returns a scripted consumer exposing controllable `IsClosed` (extend the existing scripted consumer fake with `bool IsClosed { get; set; }` and a closure-raising method; if the config callbacks take the client's delegate types, the fake must accept and store the `RawConsumerConfig` so tests can invoke its stored `ConnectionClosedHandler`/`MetadataHandler` — this is how the callback paths get exercised without the real broker).

- [ ] **Step 5: Verify**

Run: `dotnet test tests/StreamBus.UnitTests --filter "FullyQualifiedName~ConsumerDeathTests"`, then full suite + build. If a broker is available run `dotnet test tests/StreamBus.IntegrationTests --filter "FullyQualifiedName~ConsumerIntegrationTests"` — must stay green (existing resume-from-stored-offset tests now also implicitly cover recreate-after-callback-close once Task 19's death test lands; do not add integration coverage yet).

- [ ] **Step 6: Commit**

```bash
git add src/StreamBus/StreamPartitionConsumer.cs src/StreamBus/Connector.cs tests/StreamBus.UnitTests/ConsumerDeathTests.cs
git commit -m "fix(resilience): consumer death callbacks + watchdog fail attach and recreate (A1)"
```

# Task 3 (A2): Supervisor becomes a real monitor

**Files:**
- Modify: `src/StreamBus/ConnectionSupervisor.cs` (`RunLoopAsync` lines 89–114, `_handle`/`CurrentHandle`)
- Modify: `src/StreamBus/Options.cs` (`StreamBusConnectionOptions.MonitorInterval`)
- Modify: `src/StreamBus/Configuration/StreamBusConfigurator.cs` (`HostConfigurator.MonitorInterval`)
- Test: `tests/StreamBus.UnitTests/ConnectionSupervisorTests.cs` (extend)

**Interfaces:**
- Consumes: `IStreamSystemHandle.IsClosed` (exists, currently unreferenced), `ComputeDelay(attempt)` (exists), `SetState` + `StateChanged` (exist).
- Produces: `StreamBusConnectionOptions.MonitorInterval` (`TimeSpan`, default 5s, validated > 0); `CurrentHandle` return type unchanged; state events now re-fire `TransientFailure → Ready` cycles after first success.

- [ ] **Step 1: Replace RunLoopAsync**

```csharp
private async Task RunLoopAsync(CancellationToken ct)
{
    var attempt = 0;
    while (!ct.IsCancellationRequested)
    {
        try
        {
            var handle = await _connector.ConnectAsync(_options, ct).ConfigureAwait(false);
            SwapHandle(handle); // atomic: Volatile.Write under the hood
            attempt = 0;        // a successful connect clears the backoff streak
            SetState(BusConnectionState.Ready);
            await MonitorAsync(handle, ct).ConfigureAwait(false);
            // MonitorAsync returns only when the handle closed or we're stopping:
            if (!ct.IsCancellationRequested) SetState(BusConnectionState.TransientFailure);
        }
        catch (OperationCanceledException) { return; }
        catch (Exception ex) when (StreamBusErrorClassification.IsFatal(ex))
        {
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

private async Task MonitorAsync(IStreamSystemHandle handle, CancellationToken ct)
{
    var interval = _options.MonitorInterval;
    while (!ct.IsCancellationRequested)
    {
        await Task.Delay(interval, ct).ConfigureAwait(false);
        if (handle.IsClosed) return;
    }
}
```

Keep the existing FailFast switch arms deleted-in-review (dead code at former lines 56–57) out of the new method.

- [ ] **Step 2: Atomic handle swap**

```csharp
// field
private volatile IStreamSystemHandle? _currentHandle; // replaces _handle if _handle was plain

public IStreamSystemHandle CurrentHandle
    => _currentHandle ?? throw new StreamBusTransientException("Connection system handle is not available yet.");

private void SwapHandle(IStreamSystemHandle handle) => _currentHandle = handle;
```

If `CurrentHandle` currently has a different throwing behavior on null, preserve its exception type (callers may already expect it) — the observability requirement is only that consumers never see a partially-initialized handle. Also remove the now-dead `Ready/Fatal` switch arms flagged in the review (they were unreachable).

- [ ] **Step 3: Options + configurator knob**

`StreamBusConnectionOptions` gains `public TimeSpan MonitorInterval { get; init; } = TimeSpan.FromSeconds(5);` with validation in its existing `Validate()` (positive, ≤ 60s). `HostConfigurator` gains `void MonitorInterval(TimeSpan interval);` (same bound). This is the ONLY configuration path for the knob — there is no section binding (see Global Constraints); do not add one.

- [ ] **Step 4: Tests (extend `ConnectionSupervisorTests.cs`)**

```csharp
[Fact]
public async Task Supervisor_reconnects_after_system_handle_closes() // fake handle IsClosed flips true after connect; script connector: second ConnectAsync returns healthy; assert states Ready->TransientFailure->Ready + CurrentHandle switched

[Fact]
public async Task Backoff_streak_resets_after_successful_reconnect() // connector script: ok, closed, fail x2, ok; assert the second connect's delay == base (attempt reset)

[Fact]
public async Task Monitor_interval_configurable_and_valid() // options validation: zero/negative/over-60s rejects
```

The scripted `IStreamSystemConnector`/`IStreamSystemHandle` fakes from this file already support fail-then-success scripts; extend the handle fake with `bool IsClosed { get; set; }` (it already has the member — review noted it exists and is unused).

- [ ] **Step 5: Verify**

Run: `dotnet test tests/StreamBus.UnitTests --filter "FullyQualifiedName~ConnectionSupervisorTests"` then full suite + build. Integration sanity (broker available): `BusCoreIntegrationTests` still green — startup path is unchanged in blocking mode.

- [ ] **Step 6: Commit**

```bash
git add src/StreamBus/ConnectionSupervisor.cs src/StreamBus/Options.cs src/StreamBus/Configuration/StreamBusConfigurator.cs tests/StreamBus.UnitTests/ConnectionSupervisorTests.cs
git commit -m "feat(resilience): supervisor monitors post-connect state and reconnects on system-handle close (A2)"
```

# Task 4 (A5): Producer registry slot pattern + non-blocking disposal

**Files:**
- Modify: `src/StreamBus/ProducerRegistry.cs` (`GetOrCreateAsync` ~lines 111–129, `InvalidateAsync` ~lines 96–109)
- Modify: `src/StreamBus/ProducerHandles.cs` (`IStreamProducerHandle` disposal semantics / `DisposeAsync`)
- Test: `tests/StreamBus.UnitTests/PublishTests.cs` (extend: concurrent-create test)

**Interfaces:**
- Produces: registry create/validate behavior identical outward; timeouts now dispose off-thread (≤3s cap); a concurrent duplicate factory can never orphan a producer.

- [ ] **Step 1: Slots replace raw task caching**

```csharp
private sealed class ProducerSlot
{
    public readonly object Gate = new();
    public TaskCompletionSource<StreamProducerHandle>? Success;
    public bool Failed; // last attempt outcome for diagnostics
}

private async Task<StreamProducerHandle> CreateOrWaitAsync(
    string key, Func<Task<StreamProducerHandle>> factory)
{
    while (true)
    {
        var slot = _slots.GetOrAdd(key, _ => new ProducerSlot());
        bool isOwner;
        TaskCompletionSource<StreamProducerHandle>? mine = null;
        lock (slot.Gate)
        {
            if (slot.Success is not null)
            {
                TaskCompletionSource<StreamProducerHandle> done = slot.Success;
                return await done.Task.ConfigureAwait(false); // completed fast-path (value is final)
            }
            if (slot.Pending is null)
            {
                mine = new TaskCompletionSource<StreamProducerHandle>(TaskCreationOptions.RunContinuationsAsynchronously);
                slot.Pending = mine;
                isOwner = true;
            }
            else isOwner = false;
        }
        if (!isOwner)
        {
            Task<StreamProducerHandle> pending;
            lock (slot.Gate) pending = slot.Pending!.Task;
            return await pending.WaitAsync(CancellationToken.None).ConfigureAwait(false);
        }
        try
        {
            var created = await factory().ConfigureAwait(false);
            lock (slot.Gate)
            {
                // store the created handle in the registry's existing cache
                // structure (manager wrapping unchanged from current code):
                slot.Success = mine;
                slot.Success.TrySetResult(created);
                slot.Pending = null;
            }
            return created;
        }
        catch (Exception ex)
        {
            lock (slot.Gate)
            {
                slot.Pending?.TrySetException(ex);
                slot.Pending = null;
                slot.Failed = true;
            }
            // remove ONLY if this slot is still the current one for the key —
            // a racing success must not be discarded.
            _slots.TryRemove(new KeyValuePair<string, ProducerSlot>(key, slot));
            throw;
        }
    }
}
```

(Adapt field names to the current cache — the current code stores `Task<StreamProducerHandle>` values; replace with `_slots` of `ProducerSlot`; `InvalidateAsync` keeps its value-equality removal semantics but now removes/updates `ProducerSlot` and disposes the handle off-thread per Step 2. Preserve the existing behavior pinned by `cancelled_waiter_does_not_evict_healthy_producer`.) NOTE: `ProducerSlot` carries BOTH members — `Pending` (in-flight TCS, null when idle) and `Success` (completed TCS, non-null once a handle exists). The sample shows the full control flow including the completed fast-path; `InvalidateAsync` sets `Success = null` and disposes the handle off-thread (Step 2).

- [ ] **Step 2: Off-thread bounded dispose**

In the handle's dispose (or registry `InvalidateAsync`), replace the synchronous `_producer.Dispose()` call:

```csharp
private static async ValueTask CloseOffThreadAsync(RawProducer producer, ILogger log)
{
    try
    {
        await Task.Run(() => producer.Dispose()).WaitAsync(TimeSpan.FromSeconds(3)).ConfigureAwait(false);
    }
    catch (Exception ex)
    {
        log.LogWarning(ex, "Raw producer close did not complete within 3s; abandoning (broker will reap the idle publisher).");
    }
}
```

Client `RawProducer.Dispose()` is synchronous (blocks up to ~3s via internal `MidWait`) — that's why it must leave the hot path.

- [ ] **Step 3: Tests (extend `PublishTests.cs`)**

```csharp
[Fact]
public async Task Concurrent_first_publish_creates_exactly_one_producer() // dual concurrent GetOrCreate path: await both, assert same reference — scripted connector counts CreateRawProducer calls == 1

[Fact]
public async Task Dispose_never_blocks_caller_more_than_3s() // scripted producer whose Dispose blocks forever: invalidate must not hang (WaitAsync cap), logger warned
```

- [ ] **Step 4: Verify + commit**

Full suite green (the existing `cancelled_waiter_does_not_evict_healthy_producer` and the timeout-then-recreate tests MUST remain green unchanged).

```bash
git add src/StreamBus/ProducerRegistry.cs src/StreamBus/ProducerHandles.cs tests/StreamBus.UnitTests/PublishTests.cs
git commit -m "fix(resilience): producer cache slot pattern; off-thread bounded dispose (A5)"
```

# Task 5 (A6): HealthAggregator — health tells the truth

**Files:**
- Create: `src/StreamBus/HealthAggregator.cs`
- Modify: `src/StreamBus/ConsumerRegistry.cs` (registration lifecycle events)
- Modify: `src/StreamBus/StreamPartitionConsumer.cs` (attach state promotion callbacks)
- Modify: `src/StreamBus/Hosting.cs` (wire aggregator; `IHealthCheck` implementation; blocking-startup consumer gate flag)
- Modify: `src/StreamBus/Registry.cs` (expose per-vhost aggregator accessor)
- Test: `tests/StreamBus.UnitTests/HealthAggregatorTests.cs` (new)

**Interfaces:**
- Produces: internal `StreamBusHealthAggregator` (per-vhost singleton, shared by all buses on that vhost); `IHealthCheck` reads only from it; `AddStreamBusHosting(h => h.BlockStartupUntilReady(...))` extended by `h.BlockUntilConsumersAttached(true|false)` (default **false** in this task; flipping the default arrives with the docs milestone per ADR-6 break list — record in CHANGELOG now).

- [ ] **Step 1: The aggregator**

```csharp
namespace StreamBus;

internal enum BusRegistrationState { Attaching, Attached, FailedRetrying, Stopped }

internal sealed record BusRegistrationHealth(
    string Vhost,
    string Registration,       // typed: endpoint name; raw: "raw:{stream}" [+ ":2" on duplicates]; declares: "declare:{stream}"
    string Kind,               // "consumer" | "declare"
    BusRegistrationState State,
    string? Reason,
    DateTimeOffset Since);

internal sealed class StreamBusHealthAggregator
{
    private readonly ConcurrentDictionary<string, BusRegistrationHealth> _rows = new();
    private readonly object _lock = new();
    private BusConnectionState _connection = BusConnectionState.Starting;

    public void SetConnection(BusConnectionState state) { lock (_lock) _connection = state; }

    public void UpsertRegistration(string vhost, string registration, string kind,
        BusRegistrationState state, string? reason = null)
    {
        var key = $"{vhost}|{registration}";
        _rows.AddOrUpdate(key,
            _ => new BusRegistrationHealth(vhost, registration, kind, state, reason, DateTimeOffset.UtcNow),
            (_, existing) =>
                existing.State == state && existing.Reason == reason
                    ? existing // no churn: keep the original Since
                    : existing with { State = state, Reason = reason, Since = DateTimeOffset.UtcNow });
    }

    public (BusConnectionState Connection, IReadOnlyList<BusRegistrationHealth> Registrations) Snapshot()
    {
        lock (_lock) return (_connection, _rows.Values.ToArray());
    }

    internal enum HealthStatus { Healthy, Degraded, Unhealthy }
    public HealthStatus Compute()
    {
        // declare rows in Attaching are startup noise, not degradation
        var (connection, rows) = Snapshot();
        var consumers = rows.Where(r => r.Kind == "consumer").ToList();
        var declares  = rows.Where(r => r.Kind == "declare").ToList();

        bool declareExhausted = declares.Any(d => d.State == BusRegistrationState.FailedRetrying);
        bool consumersAllDead = consumers.Count > 0
            && consumers.All(c => c.State is BusRegistrationState.FailedRetrying or BusRegistrationState.Stopped);
        bool anyConsumerRetrying = consumers.Any(c => c.State == BusRegistrationState.FailedRetrying);

        if (connection == BusConnectionState.Fatal || declareExhausted || consumersAllDead)
            return HealthStatus.Unhealthy;
        if (anyConsumerRetrying)
            return HealthStatus.Degraded;
        return HealthStatus.Healthy;
    }
}
```

One aggregator lives per vhost (keyed in the existing registry cache next to supervisors); per-(vhost,busType) *views* filter rows by the bus's registrations for health/readiness projections.

- [ ] **Step 2: Wire lifecycle**

`ConsumerRegistry` receives the aggregator (ctor param, internal). Points (locate each by symbol):
- Partition consumer construction/registration → `UpsertRegistration(..., Attaching)`.
- After successful attach (`_firstAttach.TrySetResult()` side) → `Attached`.
- Every `FailAttach` (existing single place) → `FailedRetrying` with reason string (the reason label introduced in Task 2: `consumer_connection_closed` / `consumer_metadata_change` / `consumer_watchdog` / message-derived).
- Loop disposal (final `finally`) → `Stopped` (keep-remote `Stopped` is NOT degraded).
- Declares in Hosting: running → `Attaching` row kind surfaced via registration name `declare:{stream}`; success deletes the row; exhausted → `FailedRetrying` row with the final error message.

- [ ] **Step 3: HealthCheck rewrite** (`Hosting.cs:713–730`)

```csharp
public Task<HealthCheckResult> CheckHealthAsync(HealthCheckContext context, CancellationToken cancellationToken = default)
{
    var snap = _aggregator.Compute(); // aggregated per the vhost this registration belongs to
    return Task.FromResult(snap.Status switch // Healthy/Degraded/Unhealthy per Step 1
    {
        ... HealthCheckResult(Healthy, description: summary string incl. per-row one-line details) ...
    });
}
```

The health description string lists non-Attached rows with `registration/reason/since` — this is what makes the check actionable in monitoring.

- [ ] **Step 4: Readiness/UX for blocking startup** — `BlockUntilConsumersAttached(bool)` on the hosting configurator, defaulting false for this milestone; when true the startup block (existing `CancelAfter` budgeted wait) additionally waits for all consumer rows `Attached` (reuse the existing delegate-driven readiness plumbing; failure path rethrows the last failure reason via `StreamBusStartupBlockedException` — NEW exception, member of the exception base added in Task 8? NO — Task 8 has not run yet at this point; use `StreamBusException`-independent definition now: `public sealed class StreamBusStartupBlockedException : Exception` direct with a note to re-base in Task 11 when the base exists.)

- [ ] **Step 5: Tests (`HealthAggregatorTests.cs`)**

```csharp
[Fact] public async Task Consumer_failed_registration_maps_to_degraded() // force registration FailedRetrying via script
[Fact] public async Task All_failed_after_startup_maps_to_unhealthy()
[Fact] public async Task Declare_failure_row_marks_degraded_until_exhausted_then_unhealthy()
[Fact] public async Task Raw_consumers_are_first_class_registrations() // "raw:{stream}" rows appear + participate in mapping
[Fact] public async Task Suppression_after_stopped_is_healthy() // all rows Stopped after dispose => healthy
```

- [ ] **Step 6: Verify + commit**

Full suite + build green; run health check manually via `AcceptanceTests`-style script (existing health check test file covers registration; update it to assert the new Description fields non-empty when degraded).

```bash
git add src/StreamBus/HealthAggregator.cs src/StreamBus/ConsumerRegistry.cs src/StreamBus/StreamPartitionConsumer.cs src/StreamBus/Hosting.cs src/StreamBus/Registry.cs tests/StreamBus.UnitTests/HealthAggregatorTests.cs
git commit -m "feat(health): per-vhost health aggregation incl. consumers; health check tells the truth (A6)"
```

# Task 6 (A7): Shutdown/startup classification & registry hygiene

**Files:**
- Modify: `src/StreamBus/Hosting.cs` (readiness wait OCE path ~lines 465–469 + blocking-startup catch; stop-swallow path ~line 684; supervisor removal)
- Modify: `src/StreamBus/Registry.cs` (`RemoveSupervisor` addition)
- Test: `tests/StreamBus.UnitTests/HostingTests.cs` (extend)

- [ ] **Step 1: OCE ≠ timeout** — in the blocking-startup readiness wait, add a catch ahead of the current `catch (OCE) when (!ct.IsCancellationRequested)` arm:

```csharp
catch (OperationCanceledException) when (_hostStopping)
{
    // Host is stopping: cancelling the STARTUP wait is NOT a "Timed out" event.
    throw; // rethrow OCE; the hosted-service contract stops the application
}
```

Match the actual token names: the startup wait is driven by the shared `_startupCts` (created per `StartAsync` with the `CancelAfter` budget) and, secondarily, by the host-provided stop token — `_hostStopping` is a local captured before the wait: the hosted service's stop token `IsCancellationRequested`. Budget expiry (the `CancelAfter` token fired while the host is NOT stopping) keeps mapping to `StreamBusTimeoutException("Timed out waiting … within {effective}.")` exactly as today.

- [ ] **Step 2: Log, don't swallow, on stop** — replace the empty catch at the dispose sites in `StopAsync` with `catch (Exception ex) { _logger.LogWarning(ex, "Consumer/supervisor dispose did not complete cleanly during StopAsync."); }` and after the finalizes, call the new `_registry.RemoveSupervisor(vhost)` (below) so disposed supervisors don't get reused: registry gains

```csharp
internal void RemoveSupervisor(string vhost) => _supervisors.TryRemove(vhost, out _);
```

- [ ] **Step 3: Tests** — extend `HostingTests.cs`:

```csharp
[Fact] public async Task Stop_during_blocking_startup_rethrows_OCE_not_timeout() // fake slow connect; cancel stop token mid-block; assert OperationCanceledException
[Fact] public async Task Stop_logs_dispose_failures_instead_of_swallowing() // scripted supervisor whose DisposeAsync throws: assert log emitted ( ILogger fake or capture via provider)
```

- [ ] **Step 4: Verify + commit**

```bash
git add src/StreamBus/Hosting.cs src/StreamBus/Registry.cs tests/StreamBus.UnitTests/HostingTests.cs
git commit -m "fix(hosting): stop-vs-startup cancellation classified correctly; dispose failures logged (A7)"
```

# Task 7 (A8): ErrorQueueTransport — channel reuse + single timeout knob

**Files:**
- Modify: `src/StreamBus/ErrorQueueTransport.cs` (`PublishConfirmedAsync` ~lines 124–190, `ConfirmTimeout` ~line 44, `_gate` disposal ~line 289)
- Test: `tests/StreamBus.UnitTests/ErrorQueueTransportTests.cs` (new file — use in-memory scripted `IConnection`/`IChannel` per RabbitMQ.Client 7.x abstraction surfaces; if the transport's AMQP calls are not seam-wrapped, first extract the minimal `IPublishChannel` seam JUST for tests: methods `T PublishProperties`, `WaitForConfirmsAsync`, `CloseAsync`)

**Interfaces:**
- Produces: `ErrorQueueOptions.ConfirmTimeout` (TimeSpan, default 10s, surfaced on configurator `DeadLetterToQueue(... → q => q.ConfirmTimeout(...))`); a burst of N dead-letter publishes now uses one channel; disposal no longer `ObjectDisposedException`s racing summary-s.

- [ ] **Step 1: One channel per burst** — `DrainAsync`'s republish loop and any multi-envelope dead-letter run now create one channel and reuse it:

```csharp
// BEFORE (per envelope): await using var channel = await connection.CreateChannelAsync(...); await PublishConfirmedAsync(channel, ...) each
// AFTER:
await using var channel = await connection.CreateChannelAsync(CreateChannelOptions(pooling), ct);
foreach (var envelope in batch)
    await PublishConfirmedAsync(channel, envelope, confirmTimeout, ct); // method signature now takes the channel
```

Per-message delivery guarantees unchanged: confirm wait + mandatory/return/nack handlers (unwire in finally exactly as today). `DeclareAsync` for `BasicProperties`-declare sets the envelope properties unchanged.

- [ ] **Step 2: ConfirmTimeout knob** — `_confirmTimeout` static is replaced by `ErrorQueueOptions.ConfirmTimeout` (default 10s), flowing the same route the queue options already take (the `.DeadLetterToQueue(...)` endpoint path and the shared queue-options record).

- [ ] **Step 3: Disposal race** — `_gate` naive access wrapped: `GetConnectionAsync` waits wrapped in try/catch `ObjectDisposedException` → rethrow as `StreamBusTransientException("Error queue transport is stopping.")` (never `ObjectDisposedException` to callers).

- [ ] **Step 4: Tests** — scripted `IPublishChannel` seam tests: channel created once across 2 000-envelope burst drain (spy counter); mid-burst channel shutdown → remainder requeued (the existing at-least-once guarantee, now asserted through the new seam); ConfirmTimeout honored vs publish timeout default.

- [ ] **Step 5: Verify + commit**

```bash
git add src/StreamBus/ErrorQueueTransport.cs tests/StreamBus.UnitTests/ErrorQueueTransportTests.cs
git commit -m "feat(errors): channel reuse for dead-letter bursts; uniform ConfirmTimeout (A8)"
```
# Task 8 (B1–B4): Bus core collapse, one retry loop, one validation boundary, one namer

**Files:**
- Modify: `src/StreamBus/Bus.cs` (replace both classes)
- Create: `src/StreamBus/StreamRuntimeLoop.cs`
- Create: `src/StreamBus/StreamBusNaming.cs`
- Modify: `src/StreamBus/Hosting.cs` (route the three retry loops onto the helper)
- Modify: `src/StreamBus/ConnectionSupervisor.cs` (review-side connect loop keeps its custom shape — see below)
- Modify: validation sites per Step 3 list
- Test: behavior-only verification (existing suites are the contract)

**Interfaces:**
- Produces: `StreamBusCore<TBus>` (internal, single implementation); shims `StreamBus : StreamBusCore<DefaultBus>` and `StreamBus<TBus> : StreamBusCore<TBus>` (3 lines each); public API textually unchanged; `StreamRuntimeLoop.RetryUntilAsync<T>(...)`; `StreamBusNaming` (publish references, replay names, EO names; 255-byte rule).

- [ ] **Step 1: Write StreamBusCore<TBus>**

Replace the content of the two classes in `Bus.cs` with ONE generic core (bodies are the current `StreamBus` bodies verbatim with `typeof(DefaultBus)` → `typeof(TBus)`):

```csharp
internal abstract class StreamBusCore<TBus> : IStreamBus
{
    // field set + ctor copied from the current StreamBus class verbatim
    // (supervisor, producers, consumers, topology, serializer, router,
    //  errorQueues, findDeclaration).

    // IStreamBus members copied verbatim from the current classes with ONE change:
    // every `_router.Resolve(typeof(DefaultBus), ...)` becomes `_router.Resolve(typeof(TBus), ...)`
    // and every `_consumers.ConsumeAsync(typeof(DefaultBus), ...)` becomes
    // `_consumers.ConsumeAsync(typeof(TBus), ...)`.
    // All options-normalization (`with { IsSuperStream = false, RoutingKey = null }`),
    // misregistration error texts, and StartAsync stay byte-identical to today.
}

internal sealed class StreamBus : StreamBusCore<DefaultBus>, IStreamBus
{
    public StreamBus(string vhost, StreamBusConnectionSupervisor supervisor, StreamProducerRegistry producers,
        StreamConsumerRegistry consumers, IStreamTopology topology, IStreamSerializer serializer,
        StreamPublishRouter router, ErrorQueueTransport errorQueues, Func<string, DeclaredStream?>? findDeclaration = null)
        : base(vhost, supervisor, producers, consumers, topology, serializer, router, errorQueues, findDeclaration) { }
}

internal sealed class StreamBus<TBus> : StreamBusCore<TBus>, IStreamBus<TBus>
{
    public StreamBus(string vhost, StreamBusConnectionSupervisor supervisor, StreamProducerRegistry producers,
        StreamConsumerRegistry consumers, IStreamTopology topology, IStreamSerializer serializer,
        StreamPublishRouter router, ErrorQueueTransport errorQueues, Func<string, DeclaredStream?>? findDeclaration = null)
        : base(vhost, supervisor, producers, consumers, topology, serializer, router, errorQueues, findDeclaration) { }
}
```

Delete `DefaultBus`-duplication comments if any; keep `public sealed class DefaultBus;` marker line untouched.

- [ ] **Step 2: The retry-loop helper**

```csharp
namespace StreamBus;

internal enum StreamBusRetryKind { Transient, Fatal }

internal static class StreamRuntimeLoop
{
    /// <summary>One skeleton for every bounded start/declare retry: attempt,
    /// classify, delay with exponential-capped backoff, rethrow non-transients.</summary>
    public static async Task<T> RetryUntilAsync<T>(
        Func<CancellationToken, Task<T>> attempt,
        Func<Exception, StreamBusRetryKind> classify,
        TimeSpan baseDelay,
        TimeSpan maxDelay,
        int maxAttempts,
        ILogger logger,
        string operation,
        CancellationToken ct)
    {
        var currentAttempt = 0;
        while (true)
        {
            try { return await attempt(ct).ConfigureAwait(false); }
            catch (OperationCanceledException) { throw; }
            catch (Exception ex)
            {
                if (classify(ex) == StreamBusRetryKind.Fatal) throw;
                currentAttempt++;
                if (currentAttempt >= maxAttempts) throw;
                logger.LogWarning(ex, "{Operation} attempt {Attempt} failed; retrying.", operation, currentAttempt);
                await Task.Delay(StreamBusDelays.ExponentialCapped(currentAttempt, baseDelay, maxDelay), ct).ConfigureAwait(false);
            }
        }
    }
}
```

Swap the three Hosting loops (`DeclareDerivedUntilReadyAsync`, `DeclareQueuesUntilReadyAsync`, `StartConsumerUntilReadyAsync`) onto it: each keeps its local conflict/fatal classification lambda and its existing budget/context (the `ContinueWith`-cancel-sibling conflict plumbing in the declare path is preserved — only the retry skeleton changes). The supervisor's `RunLoopAsync` (Task 3) keeps its custom loop: it runs indefinitely without maxAttempts and mutates state events inline, which the helper does not model — documented as intentional.

- [ ] **Step 3: One validation boundary**

Move every range/member validation into the owning record's `Validate()`: `StreamBusConnectionOptions.Validate()`, `StreamOptionsValidation.Validate()`, and a new `StreamBusOptions.Validate()` (ports, ranges, heartbeat, pool limits, retry bounds). Then DELETE the duplicated checks from: `HostConfigurator` setters, `StreamBusConfigurator.Build()`, `ConnectionSupervisor` ctor, `Connector.ConnectAsync` pool re-check, `ReceiveEndpointConfigurator` setters, `TopologyValidation.ValidateKnobs` — each calls the owning record's `Validate()` (configurator-level also calls it once in `Build()` — the single call site, not re-implementation). All existing error-message texts are preserved verbatim (asserted by existing tests; if a test pinned a message produced at configurator-set time that now comes from `Validate()`, update the test to the same text).

- [ ] **Step 4: StreamBusNaming**

```csharp
namespace StreamBus;

internal static class StreamBusNaming
{
    public const int MaxName = 255;

    /// Stable publisher reference for explicit-dedup producers (replay).
    public static string ReplayProducerName(string sourceStream, string sourcePartitionStream)
    {
        // "streambus-replay-" + sha256(sourcePartitionStream)[..16] + "-" + sourcePartitionStream
        // (kept under 255 bytes; existing ReplayNaming recipe migrated verbatim)
    }

    /// Unique-per-handle publish refs (dedup safety): consumed via Connector's
    /// existing counter — migrate the hashing recipe here, keep format.
    public static string PublishReference(string streamName, long sequence) { ... }

    /// Effectively-once producer name per (source partition stream -> target).
    public static string EffectivelyOnceProducerName(string sourcePartitionStream, string targetStream) { ... }

    private static string Sha16(string input) { /* SHA256, first 16 hex chars, UTF8 */ }

    public static string Enforce(string name)
        => name.Length <= MaxName ? name
            : throw new StreamBusException($"Stable producer name would exceed {MaxName} bytes: {TruncateForMessage(name)}");
}
```

Migrate the three recipe sites: `Connector.cs` (PublishReference), `ErrorEnvelope.cs` (`ReplayNaming`, both usages), `EffectivelyOnce.cs` (naming + 255 rule), keeping every produced string byte-identical to today (pinned by `EffectivelyOnceTests` naming tests — extend them to cover `ReplayProducerName`).

- [ ] **Step 5: Verify**

Full suite + build: ALL existing behavior tests green with NO expectation changes except: any test asserting a validation message's *origin* stays identical. `grep -n "typeof(DefaultBus)" src/StreamBus/Bus.cs` returns at most one hit (inside the shim's generic-instantiation path if kept at all).

- [ ] **Step 6: Commit**

```bash
git add src/StreamBus/Bus.cs src/StreamBus/StreamRuntimeLoop.cs src/StreamBus/StreamBusNaming.cs src/StreamBus/Hosting.cs src/StreamBus/ConnectionSupervisor.cs
git add -A src/StreamBus/Configuration tests/StreamBus.UnitTests
git commit -m "refactor(core): single generic bus core; one retry skeleton; one validation boundary; one namer (B1-B4)"
```

# Task 9 (B5–B6): Handle split, store hygiene, factory hosting service

**Files:**
- Modify: `src/StreamBus/ProducerHandles.cs` (`IStreamProducerHandle` split)
- Modify: `src/StreamBus/Connector.cs`, `src/StreamBus/ResultProducerRegistry.cs`, `src/StreamBus/ProducerRegistry.cs` (implementations track the split)
- Modify: `src/StreamBus/Hosting.cs` (stores internalized; `StreamBusRegistrationView`; `StreamBusHostedService` factory)
- Modify: `src/StreamBus/PublishRouting.cs` → renamed `PublishRegistration.cs` (type `StreamBusPublishStore` keeps its name; file rename only)

**Interfaces:**
- Produces: `IStreamPlainProducerHandle { SendAsync(ReadOnlyMemory<byte>, PublishOptions, ct); PublishReplayAsync(...) }`, `IStreamSuperProducerHandle : IStreamPlainProducerHandle { PublishWithIdAsync(ulong, ReadOnlyMemory<byte>, ct); PublishResultAsync(...); QuerySequenceAsync(...) }` — the super-only members leave the plain surface; the silently-ignored routingKey parameter disappears from plain; super-side `SendEnvelopeAsync` throw-path disappears.
- Produces: `StreamBusRegistrationView` (record: `Vhost, Host, StreamPort, AmqpPort, HeartbeatSeconds` — no `Password`).

- [ ] **Step 1: Interface split**

```csharp
public interface IStreamPlainProducerHandle : IAsyncDisposable
{
    Task SendAsync(ReadOnlyMemory<byte> body, PublishOptions options, CancellationToken ct);
    Task<int> PublishReplayAsync(string errorStream, string? targetStream, CancellationToken ct);
}

public interface IStreamSuperProducerHandle : IStreamPlainProducerHandle
{
    Task PublishWithIdAsync(ulong publishingId, ReadOnlyMemory<byte> body, CancellationToken ct);
    Task PublishResultAsync(ReadOnlyMemory<byte> body, ulong publishingId, string? routingKey, CancellationToken ct); // copy the EXACT current super-handle signature (symbol-locate if drifted)
    Task<ulong?> QuerySequenceAsync(/* copy the EXACT current parameter list from the super-handle implementation — do NOT invent */);
}
```

Registry caches the union under the existing keyed slots; callers that need super members down-cast (currently: `ResultProducerRegistry`, `ConsumerRegistry.ReplayAsync`, `ErrorPublisher`) — each gets the narrowed type from the registry API instead (`GetOrCreateAsync` gains `Func<StreamProducerHandle, THandle>` accessor where needed), so `PublishRouting`/`Connector` casting moves to ONE site. The per-handle static routing table (per spec B5) arrives in Task 8's registry rework if it did not land there — verify `grep -n "ConditionalWeakTable" src/StreamBus/ProducerHandles.cs` returns zero after this task; if a residual exists here, migrate the CWT into the handle instance field now.

- [ ] **Step 2: Store hygiene**

Registration stores (`StreamBusConfigStore`, `StreamBusConsumerStore`, `StreamBusEndpointStore`, `StreamBusPublishStore`, `StreamBusHostingStore`): `Add(...)` becomes `internal`; public read accessors unchanged. `StreamBusConfigStore.Registrations` becomes `IReadOnlyList<StreamBusRegistrationView>` built by projection (no `Password` exposure). `StreamBusHostedService` obtains an `internal` constructor requiring all dependencies; the public DI path uses an internal factory method registered in `Hosting.AddCore` (the eight-nullable-parameters ctor and its two "internal error" throws disappear).

- [ ] **Step 3: Verify + commit**

Build + full suite green; `grep -rn "Password" src/StreamBus/ProducerHandles.cs src/StreamBus/Hosting.cs src/StreamBus/PublishRegistration.cs` shows only the internal options record, not the view; external compile probe: a scratch console referencing the built DLL and calling `StreamBusConfigStore` `Add` must fail to compile (moved to internal) — run the probe once manually.

```bash
git add src/StreamBus tests/StreamBus.UnitTests
git commit -m "refactor(surface): handle interface split; internal stores; credential-free registration view (B5-B6)"
```

# Task 10 (B7+B9+B14): Dead surface removal, typed keys, replay naming

**Files:**
- Delete members from: `src/StreamBus/Hosting.cs` (`StreamBusConnectionOptionsBuilder`, `ConsumeOptionsBuilder`), `src/StreamBus/ConsumeOptions.cs` (`ConsumeOrdering`, `ConsumeOptions.Ordering`)
- Modify: `src/StreamBus/Registry.cs` (typed keys), `src/StreamBus/ErrorEnvelope.cs` + `src/StreamBus/ConsumerRegistry.cs` (replay naming per source)
- Test: `tests/StreamBus.UnitTests/TypedConsumerTests.cs` (update — removes `ConsumeOptionsBuilder` usage), new `ReplayNamingTests.cs`

- [ ] **Step 1: Delete dead surface** — remove the three members + the orphaned `AddStreamBusConsumer` hints in tests; CHANGELOG break list entries added (Task 13 refuses none of them).

- [ ] **Step 2: Typed registry keys** — `_supervisors`/bus entry dictionary key becomes `(Type BusType, string Vhost)` (value-tuple); `DescribeStates` drops the `@`-split parse entirely. Update all `.Keys`/`.TryGetValue` sites.

- [ ] **Step 3: Replay naming per source** — `StreamBusNaming.ReplayProducerName` (Task 8) now takes BOTH `sourceStream` AND `sourcePartitionStream`; `ConsumerRegistry.ReplayAsync` passes `envelope.SourceStream` so two sources replaying from one shared error stream to an explicit target never share a name (broker dedup id spaces stay disjoint). Update `ReplayNaming` callers in `ErrorEnvelope.cs` — after Task 8 the caller list is one place.

- [ ] **Step 4: Tests (`ReplayNamingTests.cs`)**

```csharp
[Theory] public void Replay_names_are_unique_per_source_stream(...) // two sources, same partition id space, different names; each ≤ 255 bytes
[Fact]   public void Typed_registry_keys_survive_vhost_with_at_sign() // "(t, 'v@h')" lookup equality
```

- [ ] **Step 5: Verify + commit**

```bash
git add -A src tests
git commit -m "refactor(surface): delete dead knobs; typed registry keys; replay names unique per source (B7+B9+B14)"
```

# Task 11 (B10–B13+B15): Exception base, declaration cache, topology merge, parallel start, raw guard

**Files:**
- Modify: `src/StreamBus/Exceptions.cs`, `PublishExceptions.cs`, `Serialization.cs` (base type + rebase)
- Modify: `src/StreamBus/Registry.cs` (declaration cache wiring), `src/StreamBus/Bus.cs` (use cached finder)
- Modify: `src/StreamBus/TopologyRegistry.cs` (generic merge + Source enum), `src/StreamBus/DeclareSet.cs` (Source enum)
- Modify: `src/StreamBus/Hosting.cs` (parallel supervisor start)
- Modify: `src/StreamBus/ConsumerRegistry.cs` (raw Strict+destination guard)

- [ ] **Step 1: `StreamBusException`**

```csharp
public abstract class StreamBusException(string message, Exception? inner = null) : Exception(message, inner);
```

Rebase ALL nine sealed types onto it (configuration, transient, timeout, not-ready, fatal, stream-not-found, publish, deserialization, publish-store — plus `StreamBusStartupBlockedException` from Task 5). `Abstractions.md` table lands in Task 13. Verify: `grep -c "StreamBusException" src/StreamBus/*.cs` ≥ 10.

- [ ] **Step 2: Declaration cache** — `Registry.cs` builds the per-marker declaration map ONCE at bus creation (it is configuration-immutable after `AddStreamBus`), stores it on the bus entry; `Bus.cs` `_findDeclaration` becomes a cached dictionary lookup instead of a per-call `DeclareSetBuilder.Build`. Behavior preserved: explicit `DeclareSetBuilder.ValidateCallTarget` still fires on the cached entry.

- [ ] **Step 3: TopologyRegistry generic merge + Source enum** — merge `EnsureStreamAsync`/`EnsureSuperStreamAsync` into `EnsureAsync<TSpec>(TSpec spec, Func<...> create, Func<...> compare)` with two thin wrappers; `DeclaredStream.Source` becomes `internal enum DeclaredStreamSource { Endpoint, Publish, DeadLetter }` + `string Description` projection; the string literals disappear from `Merge`.

- [ ] **Step 4: Parallel supervisor start** — `Hosting.StartAsync` per-vhost supervisor starts via `Task.WhenAll` with per-task error capture preserving today's classification (the first fatal wins, transient logs continue). The existing `ContinueWith` conflict-cancel semantics for declare tasks unchanged.

- [ ] **Step 5: Raw Strict guard** — in `ConsumerRegistry.ConsumeAsync` (raw path), reject `Retry mode == Strict` with a hand-off destination present: `throw new StreamBusConfigurationException("Retry policy Strict never exhausts; a hand-off destination (ErrorStream/DeadLetter) cannot fire. Remove the destination or use a bounded retry schedule.")`.

- [ ] **Step 6: Verify + commit** — full suite green (text-pinned tests updated where messages anchored to old internals).

```bash
git add -A src tests
git commit -m "refactor(core): exception base; cached declarations; topology merge; parallel vhost starts; raw-mode guards (B10-B15)"
```

# Task 12 (B8): REMOVED — no IConfiguration binding

**Status:** dropped from v2. Configuration is explicit only.

**Files:** none.

**Interfaces:** none. Do NOT produce `AddStreamBus(IConfigurationSection, ...)`, `AddStreamBus(string sectionName, ...)`, `StreamBusConnectionOptions.Bind(...)`, configurator nullable sentinel tracking, section-key parsing, unknown-key warnings, or code-vs-section precedence.

**Why:** the maintainer decision is that callers pass everything by calling methods — hosts, ports, credentials, pools, timeouts, monitor interval, pipeline registrations. A config file is not part of the v2 surface, so no public API, no tests, and no docs may reference one. The existing `AddStreamBus(configure)` / `AddStreamBus<TBus>(configure)` overloads remain the only entry points.

**Enforcement (add to Task 13's grep gate):** `grep -rn "IConfiguration" src/StreamBus` returns zero hits.

**Numbering note:** later tasks keep their existing numbers so every cross-reference in this plan stays valid. This gap is intentional — do not renumber.

# Task 13 (D3+B16+B17): Docs truth, version, CHANGELOG, docs-compile project

**Files:**
- Create: `CHANGELOG.md`, `tests/StreamBus.DocsSnippets/StreamBus.DocsSnippets.csproj`, `tests/StreamBus.DocsSnippets/Program.Readme.cs`
- Modify: `README.md`, `docs/llms.txt`, all `docs/guide/*.md` deltas (errors § timeout note, hosting § health semantics, publish/consume § — leave pipeline guide sections for Tasks 15–17 adds), `src/StreamBus/StreamBus.csproj` (`<VersionPrefix>0.2.0</VersionPrefix>` + `<PackageId>StreamBus</PackageId>` if missing), `src/StreamBus/Abstractions.md` exception table if the repo keeps one (else fold into docs/guide/errors.md)
- Modify: `Directory.Build.props`, `docs/guide/consume.md`

- [ ] **Step 1: README rewrite** — the "90% case" becomes the explicit/fluent path, with **no `IConfiguration`, no `Builder.Configuration.GetSection(...)`, no `appsettings.json` snippet anywhere**:

```csharp
services.AddStreamBus((context, cfg) =>
{
    cfg.Host("localhost", "/", h =>
    {
        h.Credentials("guest", "guest");
        h.StreamPort(5552);
        h.AmqpPort(5672);
        h.Heartbeat(TimeSpan.FromSeconds(60));
        h.MonitorInterval(TimeSpan.FromSeconds(5));
    });

    cfg.Publish<OrderPlaced>("orders");

    cfg.ReceiveEndpoint("orders", e =>
    {
        e.Consumer<OrderConsumer>("billing");
        e.UseMessageRetry(r => r.Immediate(3));
    });
});

services.AddStreamBusHosting(h => h.BlockStartupUntilReady(TimeSpan.FromSeconds(30)));
```

Read the real `IHostConfigurator` / `IReceiveEndpointConfigurator` members before writing (`h.Credentials(user, password)`, `h.StreamPort/AmqpPort/Heartbeat`, `e.Consumer<TConsumer>("consumerGroup")`) — do not invent overloads. Every snippet copied into `tests/StreamBus.DocsSnippets/Program.Readme.cs` and compiled by the solution.

- [ ] **Step 2: DocsSnippets project**

```xml
<Project Sdk="Microsoft.NET.Sdk">
  <PropertyGroup>
    <OutputType>Exe</OutputType>
    <IsPackable>false</IsPackable>
    <AssemblyName>StreamBus.DocsSnippets</AssemblyName>
  </PropertyGroup>
  <ItemGroup>
    <ProjectReference Include="..\..\src\StreamBus\StreamBus.csproj" />
  </ItemGroup>
</Project>
```

`Program.Readme.cs` mirrors the README example verbatim (files named after the doc section). CI (Task 18) builds the whole solution, so snippets compile-checked forever. A top comment in `Program.Readme.cs` states the sync rule: README snippets MUST be reproduced here; a CI failure means the README drifted.

- [ ] **Step 3: llms.txt regeneration** — rewrite from code-ground truths: **configuration is explicit/fluent only — state plainly that there is no `IConfiguration`/`appsettings.json` binding and that every option is a configurator method call**; empty credentials rejected / `guest` accepted for local; `DeadLetterToStream/DeadLetterToQueue` + `DeadLetterQueueType`; pipeline hooks (`UsePublishFilter` / `UseConsumeFilter`, registration order, N filters — see Tasks 15–17); monitor interval documented; health-check semantics documented.

- [ ] **Step 4: CHANGELOG.md** — `## 0.2.0` heading, `### Breaking` (stores internal, Password projection removed, orphaned builders deleted, hosting `BlockUntilConsumersAttached` introduced), `### Added` (monitor interval, health aggregation, hand-off timeout, message properties/header carrier, publish + consume pipelines), `### Fixed` (A1–A8 short list with acceptance IDs). Do NOT list a configuration binder — B8 is out of scope and no `IConfiguration` surface ships.

- [ ] **Step 5: Analyzer discipline on** — in `Directory.Build.props` add `<AnalysisLevel>latest-recommended</AnalysisLevel>`; build the solution; if any CA diagnostic fires, fix the code (or add a documented `dotnet_diagnostic.<rule>.severity = suggestion|none` line with a one-line reason in the props file). Acceptance (V2-B16): `dotnet build StreamBus.slnx` reports 0 warnings WITH analyzers active — run once and paste the summary into the PR description.

- [ ] **Step 6: Cross-process caveat (B17)** — `docs/guide/consume.md` gains, in the consumer-groups section: "Same reference, `SingleActiveConsumer = false`, two processes: both receive deliveries (at-least-once doubles, never skips). Stores interleave; a stale process may regress the broker-side offset — never run two writers on one reference outside SAC." Acceptance (V2-B17): the guide text exists and quotes `SingleActiveConsumer` verbatim.

- [ ] **Step 7: Verify + commit**

```bash
dotnet build StreamBus.slnx  # DocsSnippets must compile = README compiles
grep -rn "IConfiguration" src/StreamBus   # MUST return zero hits (B8 dropped)
git add -A
git commit -m "docs(v2): README/llms truth pass, docs-compile snippets, CHANGELOG 0.2.0 (D3 + B16 + B17)"
```

# Task 14 (C1): Message properties carrier

**Files:**
- Create: `src/StreamBus/MessageProperties.cs`
- Modify: `src/StreamBus/PublishOptions.cs`, `src/StreamBus/ConsumeContext.cs` (typed accessors), and the producer send site where the client `Message` is constructed (locate `new Message(` in `ProducerHandles.cs`/`Connector.cs`)
- Test: `tests/StreamBus.UnitTests/HeaderCarrierTests.cs`, integration call in `ConsumerIntegrationTests.cs`

**Interfaces:**
- Produces: `StreamMessageProperties { MessageId?, CorrelationId?, Headers }`; `PublishOptions.Properties`; `MessageMetadata.GetString/GetInt/GetLong/GetBool/GetJson<T>(key)`; legal value set: `string | bool | int | long` (write-time throw otherwise).

- [ ] **Step 1: Types + guard**

```csharp
namespace StreamBus;

public sealed record StreamMessageProperties
{
    public string? MessageId { get; init; }
    public string? CorrelationId { get; init; }
    public IReadOnlyDictionary<string, object?> Headers { get; init; } = new Dictionary<string, object?>();

    internal static void ValidateValue(string key, object? value)
    {
        if (value is string or bool or int or long or null) return;
        throw new ArgumentException(
            $"Header '{key}' has unsupported type {value?.GetType().Name ?? "null"}; " +
            "legal header values are string, bool, int, long.", nameof(value));
    }
}
```

`PublishOptions` gains `public StreamMessageProperties? Properties { get; init; }` (validated at call time; `with`-normalization in `BusCore` keeps `RoutingKey/IsSuperStream` overrides from clobbering it).

- [ ] **Step 2: Render on publish** — at the client `Message` construction site:

```csharp
if (options?.Properties is { } p)
{
    if (p.MessageId is not null) message.Properties.MessageId = p.MessageId;
    if (p.CorrelationId is not null) message.Properties.CorrelationId = p.CorrelationId;
    var appProps = new Dictionary<string, object?>();
    foreach (var (k, v) in p.Headers) { StreamMessageProperties.ValidateValue(k, v); appProps[k] = v; }
    message.Properties.ApplicationProperties = appProps;
}
```

(Adapt exactly to the client's `MessageProperties` surface used at the site; empty `Properties` → publish exactly as today.)

- [ ] **Step 3: Read on consume** — extend `MessageMetadata`:

```csharp
public string? GetString(string key)        => ApplicationProperties.TryGetValue(key, out var v) ? v as string : null;
public int?    GetInt(string key)           => TryNumeric(key, out long l) && l is >= int.MinValue and <= int.MaxValue ? (int)l : null;
public long?   GetLong(string key)          => TryNumeric(key, out long l) ? l : null;
public bool?   GetBool(string key)          => ApplicationProperties.TryGetValue(key, out var v) && v is bool b ? b : null;
public T?      GetJson<T>(string key)       => /* bytes of GetString → IStreamSerializer.Deserialize<T> (JsonException → StreamBusDeserializationException? NO: throw ArgumentException naming key) */;
private bool TryNumeric(string key, out long value) { /* handles int, long, and (string-numeric? NO — strings are not numbers) */ }
```

(`GetJson<T>` errors throw `ArgumentException` naming the key + inner JsonException — deserialization exceptions remain for BODY decoding only.)

- [ ] **Step 4: Tests** — unit: legal/illegal value sets (illegal throws naming type+key), roundtrip via a scripted handle, accessors incl. `GetJson<T>`; integration: publish with headers → consume on plain stream, assert all five accessors (add `Properties_survive_to_consume_side` to `ConsumerIntegrationTests.cs`); super-stream properties survival asserted here too (spec C1 acceptance).

- [ ] **Step 5: Verify + commit**

```bash
git add -A src tests
git commit -m "feat(publish): message properties carrier with guarded value set (C1)"
```

# Task 15 (C2): Publish pipeline

**Files:**
- Create: `src/StreamBus/PublishPipeline.cs`
- Modify: `src/StreamBus/Bus.cs` (invoke pipeline in `PublishAsync`/`PublishSuperAsync` after resolve, before serialize), `src/StreamBus/Configuration/StreamBusConfigurator.cs` (`UsePublishFilter`), `src/StreamBus/PublishRegistration.cs` (filter store on the publish registration)
- Test: `tests/StreamBus.UnitTests/PublishPipelineTests.cs`, integration `PublishPipelineIntegrationTests.cs`

**Interfaces:**
- Produces:

```csharp
public interface IStreamPublishFilter
{
    Task OnPublish(StreamPublishFilterContext context, PublishDelegate next, CancellationToken ct);
}
public delegate Task PublishDelegate(StreamPublishFilterContext context);

public sealed class StreamPublishFilterContext
{
    public required string Stream { get; init; }              // resolved
    public required string? RoutingKey { get; init; }         // super only; filters may NOT override (see below)
    public required object Message { get; init; }             // pre-serialization
    public required IStreamHeaderWriter Headers { get; init; } // writes → PublishOptions.Properties
    public required StreamBusRegistrationView Bus { get; init; }
    public required IServiceProvider Services { get; init; }  // per-publish scope
}

public interface IStreamHeaderWriter
{
    void Set(string key, object? value);   // guards the C1 legal set
    bool TryGet(string key, out object? value);
    bool Remove(string key);
}
```

- [ ] **Step 1: Store & composition** — **additive, ordered, repeatable**: a bus may register any number of publish filters (no cap), and execution order is exactly registration order.

```csharp
// On IStreamBusConfigurator (all four are additive; calling one N times registers N filters)
void UsePublishFilter<TFilter>() where TFilter : class, IStreamPublishFilter;   // resolved once from root provider
void UsePublishFilter(Type filterType);                                          // non-generic equivalent
void UsePublishFilter(Func<IServiceProvider, IStreamPublishFilter> factory);     // custom construction
void UsePublishFilter(IStreamPublishFilter instance);                            // caller-owned singleton
```

- The publish-store carries the **ordered list**; `BusCore` composes it once per bus. `list[0]` is the outermost wrapper: it runs **first** on the way in and **last** on the way out (standard middleware nesting — document this with the two-filter diagram in `docs/guide/publish.md`).
- Filters are constructed ONCE per bus from the root provider at bus creation and MUST be thread-safe; per-publish scoped services come from `context.Services`, never from the filter's constructor.
- Registering the same filter **type** twice on the same bus throws `StreamBusConfigurationException("Publish filter '{type}' is already registered on this bus; duplicate registration would run it twice.")` — duplicate is always a mistake, never an intended pipeline.
- Registration after the bus is built/started throws `StreamBusConfigurationException` (config is immutable at runtime, same rule as topology).
- `PublishRawAsync` does not run filters (documented explicitly — raw means raw).

- [ ] **Step 2: Execution** — in `StreamBusCore`:

```csharp
var reg = _router.Resolve(typeof(TBus), typeof(T));
// kind-misregistration checks unchanged...
await using var publishScope = _scopeFactory.CreateAsyncScope();
var context = new StreamPublishFilterContext
{
    Stream = reg.Stream,
    RoutingKey = routingKey,
    Message = message,
    Headers = new HeaderWriter(),
    Bus = _busView,
    Services = publishScope.ServiceProvider,
};
await _publishPipeline.RunAsync(context, serializeAndSendAsync, ct).ConfigureAwait(false);
// publishScope disposes after the run completes: per-publish lifetime.
```

where `serializeAndSendAsync` is the existing body (serialize → `_producers.PublishAsync(reg.Stream, body, effective, ct)`), passed AS the terminal `next` delegate; the `Headers` writer materializes into the `PublishOptions.Properties.Headers` at send time (compose with any call-level `options.Properties` — call-level wins on key conflict, documented). Filters that misbehave (not calling `next` more than once) throw `StreamBusConfigurationException("Publish filter {name} invoked next {n} times; exactly one continuation is allowed.")` — publish-side double-invocation is a programming error, never silent.

- [ ] **Step 3: Exception classification + contract enforcement** — a filter exception classifies through the SAME path as send-time exceptions (existing transient/fatal classification), because the pipeline is invoked inside the same await chain.

There is **no publish short-circuit**, and this must be enforced, not assumed: the runner counts how many times the terminal delegate was invoked.
- `0` invocations → throw `StreamBusConfigurationException("Publish filter '{name}' returned without calling next; the message would have been silently dropped.")` — a forgotten `next` is a bug, never a silent no-op.
- `> 1` invocations → throw `StreamBusConfigurationException("Publish filter {name} invoked next {n} times; exactly one continuation is allowed.")`.
- Exactly `1` → normal completion.

Both guards live in one place (`PublishPipeline.RunAsync`) so no filter can bypass the contract.

- [ ] **Step 4: Tests** — order across **three** filters (record entry/exit sequence, assert first-registered-first-in/last-out), N-filter composition, duplicate-type rejection at config time, zero-`next` rejection, double-`next` rejection, header enrichment visible in a scripted Handle's rendered `PublishOptions.Properties`, filter exception → `StreamBusTransientException`-classified path, super routing-untouched assert, `PublishRawAsync` NOT running filters; integration: header-filter E2E (`PublishPipelineIntegrationTests.cs`: filter adds tenant → consume side reads it via Task 14 accessors).

- [ ] **Step 5: Verify + commit**

```bash
git add -A src tests
git commit -m "feat(publish): ordered publish pipeline with header enrichment (C2)"
```

# Task 16 (C3): Consume pipeline

**Files:**
- Create: `src/StreamBus/ConsumePipeline.cs`
- Modify: `src/StreamBus/StreamPartitionConsumer.cs` (dispatch wraps the outer delegate), `src/StreamBus/Configuration/StreamBusConfigurator.cs` (`UseConsumeFilter`, `AllowPipelineSkip`), `src/StreamBus/ErrorEnvelope.cs` (OutputHeaders onto envelope + destination properties), `src/StreamBus/ConsumeOptions.cs` (internal reject flags)
- Test: `tests/StreamBus.UnitTests/PipelineSemanticsTests.cs` (new, the big matrix), integration additions

**Interfaces:**
- Produces (as specced §8-C3):

```csharp
public interface IStreamConsumeFilter
{
    bool CanSkip { get; }   // default false; interface default-implemented member
    Task OnConsume(StreamConsumeFilterContext context, ConsumeDelegate next, CancellationToken ct);
}
public delegate Task ConsumeDelegate(StreamConsumeFilterContext context);

public sealed class StreamConsumeFilterContext
{
    public required ConsumeContext Message { get; init; }
    public required IStreamHeaderWriter OutputHeaders { get; init; }  // forwarded on exhaustion
    public required IServiceProvider Services { get; init; }          // PER-MESSAGE scope shared with the typed consumer
    public bool SkipHandler { get; set; }
    public T? GetPayload<T>();
    public void SetPayload<T>(T value); // cross-filter composition, keyed per type
}
```

- [ ] **Step 1: Registration & composition** — **additive, ordered, repeatable**, at two scopes:

```csharp
// IStreamBusConfigurator — bus-wide, applies to every consume endpoint on the bus
void UseConsumeFilter<TFilter>() where TFilter : class, IStreamConsumeFilter;
void UseConsumeFilter(Type filterType);
void UseConsumeFilter(Func<IServiceProvider, IStreamConsumeFilter> factory);
void UseConsumeFilter(IStreamConsumeFilter instance);

// IReceiveEndpointConfigurator — endpoint-specific, appended after the bus-wide filters
void UseConsumeFilter<TFilter>() where TFilter : class, IStreamConsumeFilter;
void UseConsumeFilter(Type filterType);
void UseConsumeFilter(Func<IServiceProvider, IStreamConsumeFilter> factory);
void UseConsumeFilter(IStreamConsumeFilter instance);
```

- Final order = **bus-wide filters in registration order, then endpoint filters in registration order**. `list[0]` is the outermost wrapper: first in, last out. (Rationale, documented: cross-cutting concerns registered on the bus — tracing, tenant extraction — always wrap endpoint-specific logic.)
- No cap on filter count. The composed list is built once per attach and reused for every delivery on that endpoint.
- Filters are singletons per bus (constructed once from the root provider) and MUST be thread-safe; per-message scoped services come from `context.Services`.
- Registering the same filter **type** twice within the same scope throws `StreamBusConfigurationException("Consume filter '{type}' is already registered on this bus/endpoint; duplicate registration would run it twice.")`.
- Registration after start throws `StreamBusConfigurationException` (configuration is immutable at runtime).

- [ ] **Step 2: Scope sharing** — the per-message `IServiceScopeFactory.CreateScope()` used for typed consumer resolution today becomes the pipeline's `context.Services` scope: ONE scope per delivered message, created in `Hosting.TypedConsumerHandler`'s invoke path (locate the `CreateScope` in `Hosting.cs:299–300` and hoist to a shared holder consumed by both the pipeline and the consumer resolution).

- [ ] **Step 3: SPC integration (outer wrap)** — in `DispatchAsync`, the "run handler" step becomes "run pipeline whose terminal `next` invokes the (possibly typed) handler":

```csharp
var pipeline = _pipelines; // ordered, built per attach from bus-wide + endpoint filters
await pipeline.RunAsync(pipelineContext /* built with Message + OutputHeaders + Services + payloads */,
    ctx =>
    {
        if (ctx.SkipHandler) { /* do nothing — Skip path below */ return Task.CompletedTask; }
        return handler(ctx.Message, ct); // the existing handler invocation, unchanged
    },
    ct).ConfigureAwait(false);
if (pipelineContext.SkipHandler) { /* treat as processed: return success without destination/handoff */ }
```

Retry/exhaustion/offset cadence are UNTOUCHED machinery: a pipeline exception replaces the handler exception in the same `catch` — classification chain identical. `ErrorPublisher` gains `OutputHeaders` routing: when exhaustion publishes the envelope, envelope headers are the C1-legal projection of `OutputHeaders` on top of existing envelope fields (existing `source-stream/...` keys preserved; name collisions → `OutputHeaders` skipped for the colliding key with a warning naming both keys).

**Contract enforcement (consume side differs from publish — never drop a message):** the runner counts terminal-handler invocations and the dispatch acts on that count:

| Count / state | Action |
|---|---|
| `1` | normal success |
| `0` **and** `SkipHandler == true` | intentional skip — advance per skip semantics |
| `0` **and** `SkipHandler == false` | throw `StreamBusTransientException("Consume filter '{name}' returned without calling next and without setting SkipHandler; the message would be dropped while the offset advances.")` → retried, then exhausted to the error destination. Never silently advanced. |
| `>1` | throw `StreamBusTransientException("Consume filter '{name}' invoked next {n} times; a message must be handled exactly once.")` |

Rationale for the asymmetry with publish: a publish-side contract violation throws straight to the caller because nothing was sent yet; a consume-side violation must not return "success", because success advances the offset and the message is unrecoverable.

The runner records a per-layer invocation count, so the message names the **specific** filter that failed the contract rather than the outermost one — the offending registration is identifiable from the log alone.

- [ ] **Step 4: Configuration-time rejection matrix**

```csharp
// Effectively-once pipeline + any filter with CanSkip == true -> config exception naming the filter:
throw new StreamBusConfigurationException(
    $"Consume filter '{filterType.Name}' can skip messages (CanSkip), but effectively-once pipelines must never skip: a skipped source message would publish no result and replay forever.");
// Typed endpoint + any CanSkip filter without e.AllowPipelineSkip() -> config exception naming both.
```

- [ ] **Step 5: Tests (`PipelineSemanticsTests.cs`)** — the matrix: ordering across **three** filters at one scope and across bus+endpoint scopes (entry/exit sequence recorded, first-in/last-out asserted), duplicate-type rejection, zero-`next` without `SkipHandler` → transient (offset NOT advanced), double-`next` → transient, exception→retry→exhaustion with OutputHeaders on envelope, skip→advance-without-destination (no error publish, offset stored per cadence), skip NOT possible in EO (config rejection), typed endpoint without AllowPipelineSkip (config rejection), scope-sharing assert (payload set by filter read by consumer — the flagship tenant E2E), OutputHeaders key-collision warning path.

- [ ] **Step 6: Verify + commit**

```bash
git add -A src tests
git commit -m "feat(consume): consume pipeline wrapping outer delegate; shared scope; skip semantics (C3)"
```

# Task 17 (C4+C5+C6): Telemetry spans, in-box samples, EO results, pipeline metrics

**Files:**
- Create: `src/StreamBus/StreamBusActivity.cs`, `examples/StreamBus.Examples.Producer/TenantFilter.cs`, `examples/StreamBus.Examples.Consumer/TenantFilter.cs` (+ guide edits)
- Modify: `src/StreamBus/EffectivelyOnce.cs` + `StreamPartitionConsumer.cs` (EO OutputHeaders on result publish), `src/StreamBus/StreamBusMetrics.cs` (new instruments), `docs/guide/publish.md|consume.md|observability.md`
- Test: `tests/StreamBus.UnitTests/ActivityTests.cs`, EO additions, MeterListener additions

- [ ] **Step 1: ActivitySource** — `static readonly ActivitySource Source = new("StreamBus", "2.0.0");` spans: `streambus.publish` (tags: `streambus.stream`, `streambus.size`, `streambus.routing_key?`), `streambus.consume` (tags: `streambus.stream`, `streambus.partition`, `streambus.offset`, exception event on classification). Always constructed; sampled only if a listener subscribes. Wire in `StreamBusCore` publish methods + `DispatchAsync` (start span in try/finally; consume span links to `CorrelationId` when present via `Activity LinksSourceContext` if correlation carries W3C parent — parse defensively, never throw).

- [ ] **Step 2: Helpers are EXAMPLES** (per adopted default 4): `TenantFilter.cs` in both example projects (publish side sets `x-tenant` from ambient; consume side reads `x-tenant` into a payload for downstream scoping) PLUS `CorrelationIdFilter` in producer example. `docs/guide/publish.md` + `consume.md` gain short "Pipelines" sections referencing these files.

- [ ] **Step 3: EO integration** — `_resultTarget` publishes carry the consume-context `OutputHeaders` (same C1 projection as Task 16); skip-in-EO remains config-rejected (Task 16 matrix pinned). Integration: EO pipeline test with an annotating filter — headers present at target; resume behavior unchanged (existing 9 EO tests all still green).

- [ ] **Step 4: Pipeline metrics** — `StreamBusMetrics` gains:

```csharp
private readonly Histogram<double> _pipelineDuration;  // "streambus_pipeline_duration"
// tags: kind = publish|consume
// seconds; recorded in the pipeline runner's stopwatch around the whole chain (filters + next)
```

Recorded in both pipelines at a single choke point; MeterListener unit tests pin the tag sets. Update `docs/guide/observability.md` instrument contract (8th + 9th instruments).

- [ ] **Step 5: Verify + commit**

```bash
dotnet test tests/StreamBus.UnitTests
git add -A src tests examples docs
git commit -m "feat(telemetry): ActivitySource spans + sample filters + EO header flow + pipeline duration metric (C4-C6)"
```

# Task 18 (D1): CI — GitHub Actions with brokered integration

**Files:**
- Create: `.github/workflows/ci.yml`, `ci/rabbitmq/rabbitmq.conf`, `ci/rabbitmq/enabled_plugins`
- Modify: `tests/StreamBus.IntegrationTests` shared fixture (skip-lane env)

- [ ] **Step 1: Workflow**

```yaml
name: ci
on:
  push:
    branches: [v2, main]
  pull_request:

jobs:
  unit:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-dotnet@v4
        with:
          dotnet-version: "10.0.x"
      - run: dotnet build StreamBus.slnx
      - run: dotnet test tests/StreamBus.UnitTests --logger "console;verbosity=normal"

  integration:
    runs-on: ubuntu-latest
    services:
      rabbitmq:
        image: rabbitmq:4-management
        ports: ["5552:5552", "5672:5672", "15672:15672"]
        volumes:
          - ${{ github.workspace }}/ci/rabbitmq/rabbitmq.conf:/etc/rabbitmq/rabbitmq.conf:ro
          - ${{ github.workspace }}/ci/rabbitmq/enabled_plugins:/etc/rabbitmq/enabled_plugins:ro
        env:
          RABBITMQ_DEFAULT_USER: streambus
          RABBITMQ_DEFAULT_PASS: streambus
        options: >-
          --health-cmd "rabbitmq-diagnostics -q check_port_connectivity"
          --health-interval 10s
          --health-timeout 5s
          --health-retries 20
    env:
      STREAMBUS_USER: streambus
      STREAMBUS_PASSWORD: streambus
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-dotnet@v4
        with:
          dotnet-version: "10.0.x"
      - run: dotnet test tests/StreamBus.IntegrationTests --logger "console;verbosity=normal"
```

`ci/rabbitmq/rabbitmq.conf`:

```
stream.listeners.tcp.default = 5552
management.tcp.port = 15672
vm_memory_high_watermark.relative = 0.9
```

`ci/rabbitmq/enabled_plugins`:

```
[rabbitmq_stream,rabbitmq_stream_management,rabbitmq_management].
```

- [ ] **Step 2: Skip lane** — in the integration fixture's shared setup (locate the existing `BusInstance`/`StartAsync` base — the file where `Hard-asserts BusReadiness.Ready` lives), prepend:

```csharp
if (Environment.GetEnvironmentVariable("STREAMBUS_SKIP_INTEGRATION") == "1")
{
    Assert.Skip("Integration lane disabled (STREAMBUS_SKIP_INTEGRATION=1).");
}
```

- [ ] **Step 3: Verify + commit** — push branch once to validate the workflow runs (the repo must be on a host with Actions; if the repo host is not GitHub-compatible, keep the YAML + conf as the canonical shape and note the swappable platform in the PR).

```bash
git add .github ci tests/StreamBus.IntegrationTests
git commit -m "ci: unit + brokered integration workflow with streams config (D1)"
```

# Task 19 (D2): Chaos integration — the three production failure modes

**Files:**
- Create: `tests/StreamBus.IntegrationTests/ConsumerDeathTests.cs`, `tests/StreamBus.IntegrationTests/BrokerRestartTests.cs`
- Modify: integration management-API helper (reuse/extend `TopologyIntegrationTests` helpers — locate them by symbol)

- [ ] **Step 1: Forced subscription close while idle** — `ConsumerDeathTests.Connection_closed_while_idle_reattaches_within_bounds`: start a consumer (no messages flowing), wait attached; list `/api/connections` and `DELETE` each connection whose `user_agent` contains "Stream" (stream client connections only — never the management client's own); then publish 3 messages and assert: handler observed them ≤ ~10s (callback path: near-immediate; watchdog path ≤ 5s + backoff), `GetConsumerOffsetAsync` reaches 3, NO skipped messages.

- [ ] **Step 2: Metadata/leader-change close** — `ConsumerDeathTests.Metadata_event_close_reattaches_and_promotes_correctly` (SAC stream; force via management: close the connection owning the active consumer; assert promotion to the remaining active instance and continued consumption — this asserts the SAC-promotion listener discipline live).

- [ ] **Step 3: Full connection kill** — `BrokerRestartTests.Connections_kill_does_not_skip_or_gap`: publisher + consumer active under load (5 messages/queue per second × 8s), kill ALL stream connections mid-run; assert: consumer resumes from stored offset with gaps = ZERO (no lost message positions), publish-side errors classified transient during the outage window, health transitions consumed (Task 5) reflect Degraded then Healthy.

- [ ] **Step 4: Verify + commit** — run against the local broker, then rely on CI job from Task 18 for repeat runs.

```bash
git add tests/StreamBus.IntegrationTests
git commit -m "test(integration): forced-close, metadata-close, and connection-kill chaos coverage (D2)"
```

# Task 20: Traceability matrix + definition of done (final gate)

**Files:**
- Modify: `docs/superpowers/specs/2026-10-18-streambus-v2-architecture-and-middleware-design.md` (append the matrix under §10 as a landed-markers table), `CHANGELOG.md` (final release notes)

- [ ] **Step 1: The matrix**

| Review finding | Spec item | Acceptance ID | Owning task | Test evidence |
|---|---|---|---|---|
| One-shot supervisor / stale Ready | A2 | V2-A2 | Task 3 | `ConnectionSupervisorTests.Supervisor_reconnects_after_system_handle_closes` + `BrokerRestartTests` |
| Unwired consumer death callbacks | A1 | V2-A1 | Task 2 | `ConsumerDeathTests` (unit) + `ConsumerDeathTests.Connection_closed_while_idle_reattaches_within_bounds` (int) |
| Unbounded hand-off confirm (wedge) | A3/A4 | V2-A3 | Task 1 | `ConfirmationCloseTests.Handoff_returns_transient_after_timeout_and_message_stays_replayable` + hang-regression unit |
| Health blind to consumer/declare state | A6 | V2-A6 | Task 5 | `HealthAggregatorTests` matrix + integration degraded/unhealthy windows |
| Registry factory race / blocking dispose | A5 | V2-A5 | Task 4 | `PublishTests` new tests + existing eviction tests green |
| Shutdown misclassified as timeout | A7 | V2-A7 | Task 6 | `HostingTests.Stop_during_blocking_startup_rethrows_OCE_not_timeout` |
| Channel-per-message dead-letter burst | A8 | V2-A8 | Task 7 | `ErrorQueueTransportTests` burst + mid-burst-shutdown |
| Bus twins / drift debt | B1–B4 | V2-G3 | Task 8 | full suites green unchanged; LOC delta negative |
| Two-transport handle / public stores / password leak | B5–B6 | V2-B5/B6 | Task 9 | compile probes + grep gates |
| Dead knobs, `@`-keys, replay dedup loss | B7+B9+B14 | V2-B7.1 | Task 10 | `ReplayNamingTests` + greps |
| Exception hierarchy, per-publish declare rebuild, topology dupes, serial starts, raw Strict | B10–B15 | V2-B10.1 | Task 11 | suite + `grep` gates |
| README does not compile / `llms.txt` lies | D3 | V2-G4 | Task 13 | DocsSnippets compile + llms grep gates |
| Configuration binding (proposed, **dropped** — explicit config only) | B8 (dropped) | — | Task 12 (removed) | `grep -rn "IConfiguration" src/StreamBus` = 0 hits |
| No header carrier / no pipelines | C1–C3 | V2-C1..C3 | Tasks 14–16 | `HeaderCarrierTests`, `PublishPipelineTests`, `PipelineSemanticsTests` |
| Pipeline registration is additive + ordered (N filters, registration order, contract guards) | C2/C3 | V2-C2.1/C3.1 | Tasks 15–16 | 3-filter ordering tests, duplicate-type rejection, zero/double-`next` guards, bus+endpoint scope ordering |
| EO + filter composition / telemetry silent | C4–C6 | V2-C4..C6 | Tasks 17 | Activity/Meter tests + EO integration |
| Analyzers off / lint debt | B16 | V2-B16 | Task 13 | build gate: 0 diagnostics at latest-recommended |
| Cross-process reference caveat undocumented | B17 | V2-B17 | Task 13 | guide consume section text (quoted rule) |
| No CI / green requires memory | D1 | V2-G7 | Task 18 | workflow run on `v2` |
| Chaos modes untested | D2 | V2-D2 | Task 19 | the three chaos tests |

- [ ] **Step 2: Definition of done** — (a) 0 warnings incl. analyzers; (b) 247+ unit tests green + all integration green on CI service broker; (c) README + every guide snippet compiles via DocsSnippets, and no doc contains an `IConfiguration`/`appsettings.json` example; (c2) `grep -rn "IConfiguration" src/StreamBus` returns zero hits; (c3) publish and consume endpoints each accept N filters and execute them strictly in registration order (proven by the three-filter ordering tests, including bus-wide-then-endpoint scope order); (d) `llms.txt` matches grepped code facts; (e) LOC delta on `src/StreamBus` ≤ 0 relative to `main` merge-base; (f) both pipelines + health + chaos evidenced in CHANGELOG with acceptance IDs; (g) ADRs ADR-1..ADR-6 exist under `docs/adr/` with final (not draft) wording.

- [ ] **Step 3: Commit**

```bash
git add -A
git commit -m "docs(v2): traceability matrix + definition of done (final)"
```
