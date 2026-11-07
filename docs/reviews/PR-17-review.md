# Code Review — PR #17 (`nirav/feature/SteamBusV2_Fixes` vs `origin/main`)

**Totals:** 8 Critical · 23 Important · 27 Minor — 58 items

- All findings verified against the actual code on the branch, compared directly against `origin/main`
  (not the PR target branch, per the single-commit merge plan).
- Item IDs: `INF-` = infrastructure/hosting/errors, `CON-` = consume path, `PUB-` = publish path,
  `FLT-` = typed filters/middleware, `DOC-` = docs/packaging/naming/test hygiene.
- Verification notes: the "typed consume context payload loss" claim from an earlier round was
  **refuted** (the typed context is created before the pipeline runs each attempt, so all filters
  share one instance) and is therefore not included. The two claimed `_state.Inflight` leak paths
  were also refuted, but the structural fragility is kept as CON-3.
- **INF-4 REFUTED (2026-11-01):** `StreamBusConnectionSupervisor.StartAsync` is fire-and-forget by
  contract (`ConnectionSupervisor.cs:32-40`) — it returns `Task.CompletedTask` and `RunLoopAsync`
  swallows every failure (transient → backoff/retry, fatal → `State = Fatal`). No exception can
  reach `StartSupervisorAsync`'s catch block, so `startErrors` was always empty, the claimed
  "failed-start supervisor never enters `_started`" path is unreachable, and every supervisor is
  in `_started` before the hosted `StartAsync` can possibly throw. The dead start-failure
  scaffolding (unreachable fatal-throw, transient-warning log, `IsFatalStartFailure`) was removed
  and the fire-and-forget contract documented in `Hosting.cs`; ownership is pinned by
  `HostingTests.StopAsync_disposes_and_deregisters_supervisors_that_never_connected`.

## Suggested execution order

1. **Before merge (Critical):** INF-1, INF-3, CON-1, CON-2, PUB-1, FLT-1. (INF-4 refuted — see header note.)
2. **Same PR, second commit (Important correctness):** PUB-2, PUB-3, PUB-4, PUB-5, CON-3, CON-4,
   CON-6, CON-7, CON-11, CON-13, INF-2, INF-5, INF-6, INF-7, FLT-2, FLT-3, FLT-4, FLT-5.
   (CON-5 refuted as IMPORTANT — doc-only, see its entry.)
3. **Polish batch:** DOC-1, DOC-2, DOC-3, DOC-5, DOC-7, DOC-8, DOC-12, and all minors.

---

# 🔴 CRITICAL

---

**[INF-1] Confirm channel: publish TCS completed by any ack/nack event, no delivery-tag match; cancellation path escapes without channel reset**
- **Severity:** CRITICAL
- **File:** src\StreamBus\ErrorQueueTransport.cs (lines 120–129, 150–162; caller 332–350)
- **Problem:** The cached publish channel is reused across publishes, but each `PublishAsync` call subscribes fresh `BasicAcks/Nacks` handlers that complete a per-call TCS on the *first* event, with no check that the event's `DeliveryTag` belongs to this publish. A duplicated, broker-replayed, or late ack from a previous publish on the same channel can resolve the next publish's TCS — reporting a dead-letter as confirmed when it wasn't (an at-least-once violation). Additionally, when the caller's token cancels, `OperationCanceledException` exits without resetting the outcome-unknown channel — the exact hazard the timeout comment warns about — leaving a stray in-flight delivery whose late ack can hit the next publish's handlers.
```csharp
// ErrorQueueTransport.cs:126-129 + 150-162
AsyncEventHandler<BasicAckEventArgs> onAck = (_, _) => { Complete(null); return Task.CompletedTask; };
...
catch (OperationCanceledException) when (!ct.IsCancellationRequested)
{
    throw new StreamBusTimeoutException($"Publish to error queue '{queue}' was not confirmed within {confirmTimeout} (outcome unknown).");
}

// caller, ErrorQueueTransport.cs:332-350 — no OperationCanceledException arm
try
{
    await channel.PublishAsync(queue.Name, envelope, confirmTimeout, ct).ConfigureAwait(false);
}
catch (StreamBusTimeoutException)
{
    // The confirm outcome is unknown; a late ack must not be
    // mistaken for the next publish on this channel.
    await ResetPublishChannelAsync().ConfigureAwait(false);
    throw;
}
```
- **Solution:** (1) Capture the delivery tag returned by `BasicPublishAsync` and only complete the TCS on a matching tag; treat unmatched ack/nack events as unknown-outcome and reset. (2) Add an `OperationCanceledException` arm in `ErrorQueueTransport.PublishAsync` that resets the publish channel before rethrowing.
```csharp
// AmqpPublishChannel.PublishAsync
var tag = await _channel.BasicPublishAsync(
    exchange: string.Empty, routingKey: queue, mandatory: true,
    basicProperties: properties, body: envelope.Body,
    cancellationToken: linked.Token).ConfigureAwait(false);
AsyncEventHandler<BasicAckEventArgs> onAck = (_, e) =>
{
    if (e.DeliveryTag == tag) Complete(null);
    return Task.CompletedTask;
};

// ErrorQueueTransport.PublishAsync
catch (OperationCanceledException)
{
    // Outcome unknown: a late ack must not be mistaken for the next publish.
    await ResetPublishChannelAsync().ConfigureAwait(false);
    throw;
}
```

---

**[INF-3] Per-vhost `ErrorQueueTransport` instances (with live AMQP connections) are never disposed**
- **Severity:** CRITICAL
- **File:** src\StreamBus\Registry.cs (lines 85, 89–93); src\StreamBus\Hosting.cs (lines 755–785)
- **Problem:** `StreamBusRegistry` lazily creates an `ErrorQueueTransport` per vhost (each owning an AMQP `IConnection`), but neither the registry (no `IAsyncDisposable`) nor `StreamBusHostedService.StopAsync` ever disposes them. `StopAsync` disposes only consumers and supervisors. Every vhost that ever touched a queue dead-letter or `DrainAsync` leaves an open AMQP connection and cached channel at shutdown; the graceful `CloseAsync(200, ...)` path is unreachable in normal host shutdown.
```csharp
// Registry.cs:89-93
internal ErrorQueueTransport ErrorQueuesFor(string vhost)
    => _errorQueues.GetOrAdd(vhost, _ => new ErrorQueueTransport(
        _resolveOptions(vhost),
        null,
        _loggerFactory.CreateLogger<ErrorQueueTransport>()));

// Hosting.cs:779-784 — StopAsync ends here; no transport disposal
foreach (var (vhost, supervisor) in copy)
{
    try { await supervisor.DisposeAsync().ConfigureAwait(false); }
    catch (Exception ex) { _logger.LogWarning(ex, "..."); }
    finally { _registry.RemoveSupervisor(vhost); }
}
```
- **Solution:** Make `StreamBusRegistry` implement `IAsyncDisposable` that drains and disposes `_errorQueues`, and have `StreamBusHostedService.StopAsync` dispose the registry after the supervisor loop.
```csharp
// StreamBusRegistry
internal sealed class StreamBusRegistry : IStreamBusRegistry, IAsyncDisposable
{
    public async ValueTask DisposeAsync()
    {
        foreach (var (_, transport) in _errorQueues)
        {
            try { await transport.DisposeAsync().ConfigureAwait(false); }
            catch { }
        }
        _errorQueues.Clear();
    }
}

// StreamBusHostedService.StopAsync, after the supervisor loop:
await _registry.DisposeAsync().ConfigureAwait(false);
```

---

**[INF-4] ~~Failed-start supervisors are never added to `_started` — never disposed or deregistered on StopAsync~~ — **REFUTED**
- **Severity:** ~~CRITICAL~~ → none (refuted; minor cleanup applied)
- **File:** src\StreamBus\Hosting.cs (lines 736–753, 755–785)
- **Refutation:** The finding requires `supervisor.StartAsync(ct)` to be able to throw. It cannot:
  `StreamBusConnectionSupervisor.StartAsync` (`ConnectionSupervisor.cs:32-40`) only links a CTS,
  kicks off `RunLoopAsync` as a background task (which catches **all** exceptions internally —
  OCE exits, fatal sets `State = Fatal`, transient backs off and retries), and returns
  `Task.CompletedTask`. Therefore the `catch (Exception ex) { return ex; }` in
  `StartSupervisorAsync` was unreachable, `startErrors` was always all-null, and every supervisor
  lands in `_started` before the hosted `StartAsync` can throw on any later path (e.g.
  `BlockStartupUntilReady` fatal). The premise "on failure the supervisor never enters `_started`"
  does not occur.
- **Resolution:** The misleading dead scaffolding was removed (see header note): `StartSupervisor`
  is now synchronous, `IsFatalStartFailure`/fatal-throw/warning-lot lines are gone, the
  fire-and-forget contract is documented at the call site, and
  `HostingTests.StopAsync_disposes_and_deregisters_supervisors_that_never_connected` pins the
  ownership invariant (StopAsync disposes + deregisters supervisors that never connected).
- No original review text removed below for traceability:

**[INF-4-ORIGINAL] Failed-start supervisors are never added to `_started` — never disposed or deregistered on StopAsync**
- **Severity:** CRITICAL
- **File:** src\StreamBus\Hosting.cs (lines 736–753, 755–785)
- **Problem:** `StartSupervisorAsync` catches the start exception and returns it, so on failure the supervisor never enters `_started`. The supervisor's `RunLoopAsync` keeps retrying in the background by design, but `StopAsync` iterates only `_started`, so its `DisposeAsync` is never called and `_registry.RemoveSupervisor(vhost)` never runs. Cleanup only works by the incidental fact that `Host` passes a shutdown-linked token — an implementation detail the library shouldn't rely on.
```csharp
// Hosting.cs:736-753
private async Task<Exception?> StartSupervisorAsync(
    string vhost, StreamBusConnectionSupervisor supervisor, CancellationToken ct)
{
    try
    {
        await supervisor.StartAsync(ct).ConfigureAwait(false);
        lock (_gate) _started.Add((vhost, supervisor));
        return null;
    }
    catch (OperationCanceledException) when (ct.IsCancellationRequested) { throw; }
    catch (Exception ex) { return ex; }   // supervisor NOT recorded
}
```
- **Solution:** Track every supervisor created during `StartAsync` regardless of first-attempt outcome (a background retry loop is still an owned resource), and dispose/deregister all of them in `StopAsync`.
```csharp
// StartAsync, when building supervisors:
var supervisor = _registry.SupervisorFor(vhost);
lock (_gate) _started.Add((vhost, supervisor));   // record before start
startTasks.Add(StartSupervisorAsync(vhost, supervisor, ct));
```

---

**[CON-1] Foreign-token `OperationCanceledException` is retried as a generic failure; unbounded default retry wedges the partition forever**
- **Severity:** CRITICAL
- **File:** src\StreamBus\StreamPartitionConsumer.cs (lines 543–548) + src\StreamBus\RetryPolicy.cs (lines 15–24)
- **Problem:** The dispatch retry loop only exits quietly on `OperationCanceledException` when *the loop token* is cancelled. A handler that throws OCE from its own token (per-delivery token, DB timeout wrapped as OCE, HTTP `TaskCanceledException`) falls into the generic catch and is retried. The default `RetryPolicy` has `MaxAttempts = null` (unbounded) and registry validation permits unbounded retry with no handoff destination, so this case retries forever inside the dispatch gate: back-pressure freezes credit for that partition and the message never dead-letters. In-handler retry is also invisible to health monitoring.
```csharp
catch (OperationCanceledException) when (attach.LoopToken.IsCancellationRequested)
{
    Interlocked.Decrement(ref _state.Inflight);
    Interlocked.Exchange(ref _state.RetryBlocked, 0);
    return;
}
```
```csharp
// RetryPolicy.cs — default is unbounded
public int? MaxAttempts { get; init; }
public TimeSpan BaseDelay { get; init; } = TimeSpan.FromMilliseconds(200);
```
- **Solution:** (1) Classify OCE: only treat as shutdown when `attach.LoopToken.IsCancellationRequested`; otherwise count it toward `MaxAttempts` exactly like any handler failure, with a visible recycle. (2) Document on `RetryPolicy.MaxAttempts` that `null` means "the partition halts on any persistent handler failure" (see also CON-5).
```csharp
catch (OperationCanceledException) when (attach.LoopToken.IsCancellationRequested)
{
    ... return; // shutdown only
}
catch (OperationCanceledException ex) // foreign token: a real handler failure
{
    Interlocked.Decrement(ref _state.Inflight);
    FailAttach(attach, offset, ex, "Consume handler cancelled without shutdown");
    return;
}
```

---

**[CON-2] `OffsetStorer` mutates fields on the dispatch thread while the timer thread reads them — no synchronization, tearing possible on 32-bit**
- **Severity:** CRITICAL
- **File:** src\StreamBus\StreamPartitionConsumer.cs (lines 35–82, 945–955, 1085–1116)
- **Problem:** `ShouldStoreOnSuccess`/`Stored` mutate `_lastSuccess`, `_sinceLastStore`, `_lastStored` on the dispatch path; `TickAsync` reads `HasSuccess`/`LastSuccess`/`IsStored` on the timer task with no lock, no `Volatile`, and `ulong` fields that can tear on 32-bit targets. A stale `LastSuccess` snapshot can cause the timer to admit and store an offset for a *stale/possibly-closed* handle, advancing the watermark so the current dispatch's cadence store of the same offset is then rejected — the store silently no-ops.
```csharp
// fields (35-44): mutated on dispatch path only
private int _sinceLastStore;
private ulong _lastSuccess;
private bool _hasSuccess;
private ulong _lastStored;
private bool _hasStored;
```
```csharp
// TickAsync (1091-1099): timer thread
if (!_storer.HasSuccess) { continue; }
var offset = _storer.LastSuccess;
if (_storer.IsStored(offset)) { continue; }
```
- **Solution:** Guard all `OffsetStorer` mutation and reads with the existing `_storeGate` (or a dedicated lock): `ShouldStoreOnSuccess`, `Stored`, `IsStored`, and `LastSuccess`/`HasSuccess` access take the gate, and `TickAsync` snapshots `LastSuccess` under the same lock before calling `StoreAndTrackAsync`.
```csharp
public ulong LastSuccess { get { lock (_gate) return _lastSuccess; } }
public bool ShouldStoreOnSuccess(ulong offset) { lock (_gate) { ... } }
// TickAsync:
ulong offset;
lock (_gate)
{
    if (!_storer.HasSuccess) continue;
    offset = _storer.LastSuccess;
}
```

---

**[PUB-1] Filter `Set` loses to call-level header values in `Materialize` — VALID, fixed (2026-11-01)**
- **Severity:** CRITICAL → resolved
- **File:** src\StreamBus\Headers.cs (lines 185–198; seeded at src\StreamBus\Bus.cs line 169)
- **Problem:** `StreamSendHeaders` is constructed with the caller's `PublishOptions.Properties.Headers` as its initial values, so a filter's `Set` correctly overrides them in `_values`. But `Materialize` then re-merges `callLevel.Headers` *over* `_values`, so any header the caller also supplied silently defeats the filter's `Set`. `Remove` intentionally wins over call-level values (per its doc comment), so the two mutations have opposite, contradictory precedence. A middleware filter that overrides a user-supplied `x-tenant` header is silently ignored — filters must be the last writer in a pipeline.
```csharp
var merged = new Dictionary<string, object?>(_values, StringComparer.Ordinal);
if (callLevel is not null)
{
    foreach (var (key, value) in callLevel.Headers)
        merged[key] = value;
}
```
- **Solution:** Delete the `callLevel` merge loop — `_values` already contains those entries because the bag is seeded from `effective.Properties?.Headers` at `Bus.cs:169`. Keep the `_removed` loop so a filter `Remove` still wins. Add a doc comment on `Set` stating that filter mutations take precedence over call-site headers, consistent with `Remove`.

- **Verification & resolution (2026-11-01):** Confirmed against MassTransit's pipeline: it uses one mutable header
  bag, no re-merge; caller initializer *data* lands on the context before middleware, and configured filters are the
  last writer (only a per-call *delegate* — a concept StreamBus doesn't have — outranks middleware). The StreamBus
  fix applied the same model:
  1. Deleted the `callLevel` re-merge loop in `Materialize` (`Headers.cs`) — filters are now the last writer.
  2. `Reset()` restored to the *seeded* state instead of clearing everything: the old re-merge loop was silently
     what re-seeded call-level headers after `Reset()` between consume retry attempts
     (`StreamPartitionConsumer.cs:562, 845`). `StreamSendHeaders` now keeps an `_initial` snapshot and `Reset()`
     restores it, so each retry attempt starts from the delivery's original headers.
  3. Docs updated: `IStreamSendHeaders.Set` now states filter mutations take precedence over call-site values
     (symmetric with `Remove`'s existing doc); `Reset()` documents the restore-to-seed contract.
  4. Tests updated (TDD — watched both fail before the fix): the two "call level wins" pinning tests were inverted
     to assert filter-wins (`HeadersTests.Materialize_lets_filter_set_win_over_call_level_headers`,
     `PublishPipelineTests.Header_writer_merges_call_level_properties_and_filter_set_wins`), and
     `Send_headers_support_set_get_remove` now pins restore-to-seed on `Reset()`. Full suite green:
     UnitTests 441/441, IntegrationTests 70/70 (2 pre-existing skips).
```csharp
internal StreamMessageProperties Materialize(StreamMessageProperties? callLevel)
{
    // _values was seeded from the call-level headers at construction, so filter
    // Set/Remove mutations already reflect the correct precedence: filters win.
    var merged = new Dictionary<string, object?>(_values, StringComparer.Ordinal);

    foreach (var key in _removed)
        merged.Remove(key);

    return (callLevel ?? new StreamMessageProperties()) with { Headers = merged };
}
```

---

**[FLT-1] Typed filter machinery is entirely reflection-based with no AOT/trimming story**
- **Severity:** CRITICAL
- **File:** src\StreamBus\FilterAdapters.cs (lines 23–35); src\StreamBus\ConsumePipeline.cs (lines 251–259); src\StreamBus\TypedMessageDeserializer.cs (lines 37–46)
- **Problem:** Every typed filter/context/deserialization path uses `MakeGenericType` + `Activator.CreateInstance`, `MakeGenericMethod` on a `NonPublic` method found by name, and `Expression.Compile`. There are no `[RequiresUnreferencedCode]`/`[DynamicallyAccessedMembers]` annotations and the csproj sets no AOT/trimming lane. Under a trimmed or NativeAOT publish these calls fail at runtime — silently, one message-type-cache-miss after deployment — and the `!` on `GetMethod` turns a simple rename of `CreateTyped` into a delivery-time crash on every bus.
```csharp
// FilterAdapters.cs 23-25
return (IStreamPublishFilter)Activator.CreateInstance(
    typeof(TypedPublishFilterAdapter<>).MakeGenericType(messageType),
    filter)!;

// ConsumePipeline.cs 253-258
var method = typeof(TypedConsumeContext).GetMethod(
    nameof(CreateTyped),
    BindingFlags.NonPublic | BindingFlags.Static)!;
return (Func<ConsumeContext, object, ConsumeContext>)method
    .MakeGenericMethod(messageType)
    .CreateDelegate(typeof(Func<ConsumeContext, object, ConsumeContext>));

// TypedMessageDeserializer.cs 39-41
var closed = typeof(IStreamSerializer)
    .GetMethod(nameof(IStreamSerializer.Deserialize))!
    .MakeGenericMethod(messageType);
```
- **Solution:** (1) Add compile-time-closable registration overloads so the common path needs no runtime closing: `UseConsumeFilter<TFilter, TMessage>()` constructs `TypedConsumeFilterAdapter<TMessage>` directly at the call site (the ASP.NET Core `UseMiddleware<T>` pattern). (2) Replace the string-based `GetMethod` with a static generic cache class that is closed via `MakeGenericType` once, with annotations. (3) Annotate the remaining dynamic paths with `[RequiresUnreferencedCode]` and document that type-name-based registration is not trimming-safe.
```csharp
// New public API, closed at the call site — no MakeGenericType
public void UseConsumeFilter<TFilter, TMessage>()
    where TFilter : class, IStreamConsumeFilter<TMessage>
{
    AddConsumeFilter(ConsumeFilterRegistration.ForInstance(
        TypedConsumeFilterAdapter<TMessage>.Wrap(this)));
}

// Typed context factory via generic cache class, not GetMethod by name
internal static class TypedConsumeContextFactory<T>
{
    public static ConsumeContext Create(ConsumeContext delivery, object message)
        => new ConsumeContext<T> { /* ...copy fields... */ Message = (T)message };
}
```

- **Verification & resolution (2026-11-01):** Partially valid, with severity
  corrected from CRITICAL to IMPORTANT. The AOT/trimming claim is real but the
  library makes no AOT promise, so it is not merge-blocking; the rename
  fragility is a genuine latent bug and was fixed.
  1. **Confirmed genuine:** `TypedConsumeContext.CreateFactory` resolved a
     private method by `nameof(CreateTyped)` and `TypedMessageDeserializer`
     resolved `IStreamSerializer.Deserialize` by name, both with `!`; a rename
     would compile and crash on the first typed delivery. `Expression.Compile`
     added per-type startup cost and cannot run under NativeAOT. `Hosting.cs`
     had the same name-based pattern for typed consumer invokers.
  2. **Refuted as CRITICAL:** no doc, csproj property, or README claim promises
     trimming or NativeAOT support, so an AOT failure is an unsupported scenario
     rather than a broken contract. The AOT work is a capability upgrade, not a
     correctness emergency.
  3. **Applied fix:** every name-based `GetMethod` / `MakeGenericMethod` /
     `CreateDelegate` / `Expression.Compile` call was replaced by
     `RuntimeGeneric`, a single annotated `MakeGenericType` +
     `Activator.CreateInstance` helper used by generic factory classes
     (`TypedMessageReader<T>`, `TypedConsumeContextFactory<T>`,
     `TypedConsumerInvoker<T,TConsumer>`, typed filter adapters). The typed
     deserializer now calls `IStreamSerializer.Deserialize<T>` directly.
  4. **New API:** `UsePublishFilter<TFilter, TMessage>()` and
     `UseConsumeFilter<TFilter, TMessage>()` (bus and endpoint) construct the
     typed adapter at the call site (`PublishFilterRegistration.ForTyped` /
     `ConsumeFilterRegistration.ForTyped`), so the common typed path performs no
     runtime generic closing. The existing single-type-parameter and `Type`
     overloads remain for interface-derived and open-generic registration and
     now carry `[RequiresUnreferencedCode]` / `[RequiresDynamicCode]`, with
     `[DynamicallyAccessedMembers]` on the type inputs.
  5. **Docs/changelog:** the publish and consume guides and `docs/llms.txt`
     state the trimming-safe path; CHANGELOG records the new overloads and the
     removal of name-based reflection. Tests added in
     `TypedFilterRegistrationTests` pin the new overloads and the compile-time
     adapters (UnitTests 448/448; the brokered integration suite could not run
     in this environment because no RabbitMQ broker is listening).
  6. **Review follow-up (2026-11-01):** a fresh `csharp-reviewer` pass found no
     critical/high defects. Its medium/low findings were addressed:
     `[DynamicallyAccessedMembers]` now propagates into
     `FilterTypeInspector.Classify`, so the IL2070 warning at
     `FilterTypeInspector.cs` is gone under `IsTrimmable`; the same filter type
     may be registered twice only when the registrations are pinned to
     disjoint message types (true duplicates still fail); `WrapTyped` rejects a
     message type the compile-time adapter was not pinned to; the dead
     `CreateTyped` helpers were removed; the docs no longer imply end-to-end
     trimming/NativeAOT support; and the CHANGELOG lists the new annotations
     and interface members under `### Breaking`. UnitTests 458/458.

---

# 🟠 IMPORTANT

---

**[PUB-2] Publish filters resolved once from the root provider create captive dependencies — VALID, fixed (2026-11-01)**
- **Severity:** IMPORTANT
- **File:** src\StreamBus\PublishPipelineCache.cs (lines 34–49 and 76–82)
- **Problem:** The cache constructor eagerly resolves every universal filter from `_rootProvider` and caches the instance for the bus's lifetime; typed filters are likewise resolved from `_rootProvider!`. If a user registers a scoped (or transient) filter type, it is silently promoted to singleton: any scoped service it captures (e.g., a `DbContext`) lives for the whole application. Meanwhile `Bus.cs:168` creates a per-publish DI scope and hands it to filters via `context.Services`, advertising scoped semantics that the cache's own resolution contradicts. Nothing validates the lifetime.
```csharp
var filter = registration.CreateUniversal(rootProvider);
```
```csharp
var instance = _typedInstances.GetOrAdd(
    closedFilterType,
    type => registration.ResolveTyped(type, _rootProvider!));
```
- **Solution:** Either resolve each filter from the per-publish scope at publish time, or — if singleton caching is kept for performance — validate at seal time that registered filter types resolve as singletons and throw `StreamBusConfigurationException` otherwise. Document the singleton contract on `IStreamPublishFilter`. Apply the same fix on the consume side (see FLT-4).
```csharp
// At seal time, for each registered filter type:
var descriptor = services.FirstOrDefault(d => d.ServiceType == registration.FilterType);
if (descriptor is { Lifetime: ServiceLifetime.Scoped or ServiceLifetime.Transient })
    throw new StreamBusConfigurationException(
        $"Publish filter '{registration.FilterType}' is registered as {descriptor.Lifetime}; publish filters must be singletons. Resolve per-publish scoped services from PublishContext.Services.");
```

- **Verification & resolution (2026-11-01):** Confirmed genuine, but the defect is a contract mismatch rather than the
  cache itself. `docs/guide/publish.md` already states "Filters are singletons per bus and must be thread-safe; each
  publish gets its own DI scope through `context.Services`", and the consume side pins the same model (filters are
  singleton/instance; scoped dependencies come from `context.Services`). The code contradicted that in two ways:
  1. `AddStreamBus` registered every type-based publish filter with `TryAddTransient`
     (`StreamBusConfiguratorExtensions.AddCore`), while `PublishPipelineCache` resolved it from the root provider
     exactly once and cached it for the bus lifetime — a transient registration silently behaved as a singleton.
  2. Nothing rejected a user-supplied scoped/transient registration. A `Scoped` filter resolved from the root is a
     captive dependency; with ASP.NET Core's scope validation enabled it instead fails at publish time with a raw
     `InvalidOperationException`.

  The per-publish scope is not intended to construct filters (MassTransit uses the same model); it exists so
  singleton filters can resolve scoped services per publish through `context.Services`. Resolving filters per publish
  would have changed the documented and consume-side design and added a DI resolve on every publish, so the
  singleton contract was made explicit and enforced instead:
  1. Type-based publish filters are registered with `TryAddSingleton` (`AddCore`).
  2. `FilterRegistrationSupport.EnsureSingletonLifetime` compares the root and per-scope resolution of a filter type
     and throws `StreamBusConfigurationException` for scoped/transient (or root-unresolvable) registrations. It is
     applied to universal registrations in the cache constructor and to typed registrations when a pipeline is built,
     so declared pipes fail at bus construction and other types fail on first use. Instance registrations
     (`UsePublishFilter(IStreamPublishFilter)`) and factory registrations are singleton-by-construction and skipped.
     Consume-side lifetime validation remains part of FLT-4.
  3. The singleton/thread-safety contract and the `context.Services` guidance are documented on
     `IStreamPublishFilter`, `IStreamPublishFilter<T>`, `PublishContext.Services`, the `UsePublishFilter` overloads,
     `docs/guide/publish.md`, and CHANGELOG (`### Breaking`).
  4. Tests (TDD — all four failed before the fix): `Publish_filter_types_are_registered_as_singletons`,
     `Scoped_publish_filter_is_rejected_at_bus_construction`,
     `Transient_publish_filter_is_rejected_at_bus_construction`, and
     `Scoped_typed_publish_filter_is_rejected_at_bus_construction`. The two `TypedFilterRegistrationTests` that
     construct the cache directly now register their probe filters as singletons (they test pipeline composition, not
     DI lifetime). UnitTests 462/462; the brokered integration suite could not run in this environment because no
     RabbitMQ broker is listening (every test times out at connect).

---

**[PUB-3] Pipeline catch-all wraps foreign exceptions as transient; fast path records nothing on the activity — VALID, fixed (2026-11-01)**
- **Severity:** IMPORTANT
- **File:** src\StreamBus\Bus.cs (lines 158–215)
- **Problem:** The zero-filter fast path (159–163) has no try/catch: raw exceptions propagate unwrapped and `StreamBusActivity.RecordException` is never called, so tracing differs depending on whether filters are configured. The filtered path catches every non-fatal, non-`StreamBusException` exception — including a user-filter `NullReferenceException` and a foreign `OperationCanceledException` with an untriggered caller token — and wraps it in `StreamBusPublishTransientException`. Callers that retry on transient exceptions will retry a deterministically broken filter forever.
```csharp
if (pipeline.Count == 0)
{
    await SendTypedAsync(stream, message, effective, ct, activity).ConfigureAwait(false);
    return;
}
```
```csharp
catch (OperationCanceledException) when (ct.IsCancellationRequested)
{
    throw;
}
catch (Exception ex)
{
    StreamBusActivity.RecordException(activity, ex);
    throw new StreamBusPublishTransientException(
        $"Publish to stream '{stream}' failed transiently in the publish pipeline.",
        ex);
}
```
- **Solution:** Hoist one `try/catch` around both branches so both paths get identical treatment. Reorder the cancellation filter so foreign-token OCE never gets wrapped as transient. Keep the transient wrapper for genuine transport failures only — user-filter failures surface unwrapped (the registry already classifies transport errors as transient).
```csharp
var pipeline = _publishPipelines.GetOrCreate(typeof(T));
try
{
    if (pipeline.Count == 0)
    {
        await SendTypedAsync(stream, message, effective, ct, activity).ConfigureAwait(false);
        return;
    }
    // ... scope, context, pipeline.RunAsync(...) unchanged ...
}
catch (StreamBusException ex)
{
    StreamBusActivity.RecordException(activity, ex);
    throw;
}
catch (OperationCanceledException)
{
    // Never wrap cancellation as a transient publish failure.
    throw;
}
catch (Exception ex)
{
    // User-filter failures are deterministic; surface them unwrapped so
    // callers don't retry-burn on their own bugs.
    StreamBusActivity.RecordException(activity, ex);
    throw;
}
```

- **Verification & resolution (2026-11-01):** Confirmed genuine on all three counts.
  1. The zero-filter fast path bypassed every `StreamBusActivity.RecordException` call
     (the only publish-path recordings were inside the filtered-path catch blocks), so tracing
     differed by configuration. The filtered path's `catch (OperationCanceledException) when
     (ct.IsCancellationRequested)` let a foreign-token OCE from user filter code fall into the
     generic catch, and `StreamBusErrorClassification.IsFatal` only matches protocol/SSL/auth
     errors, so a filter `InvalidOperationException`/`NullReferenceException` was wrapped as
     `StreamBusPublishTransientException` ("failed transiently in the publish pipeline") — an
     exception consumers of the API classify as retryable (see `BrokerRestartTests`: users key
     retry on `StreamBusPublishTransientException`), causing deterministic retry-burn.
  2. The wrapping was also redundant: `StreamProducerRegistry.PublishAsync` already classifies
     genuine transport failures (invalidating the producer and throwing `StreamBusPublishTransientException`
     / `StreamBusTimeoutException` / `StreamBusPublishException`) before anything reaches the
     pipeline catch-all, so the only non-`StreamBusException` exceptions reaching it came from
     user filter/pipeline code.
  3. **Applied fix** (`Bus.PublishThroughPipelineAsync`): one `try` now covers the fast path
     and the filtered path; the whole block collapses to `catch (OperationCanceledException)
     { throw; }` (never record or wrap cancellation) plus a single `catch (Exception ex)
     { StreamBusActivity.RecordException(activity, ex); throw; }`. Transport failures stay
     classified as transient by the producer layer; user-filter failures (including foreign-token
     OCE) surface unwrapped; both paths trace identically.
  4. Note: `PublishRawAsync` also publishes without recording an exception event; like the
     pre-fix fast path it delegates classification to the producer layer. Left as-is — the
     reviewer did not flag it and it has no pipeline semantics to unify; recording can be added
     when other raw-path telemetry work lands.
  5. Tests (TDD — all three failed before the fix): the pinning test
     `Filter_exception_is_classified_as_a_publish_transient_failure` was inverted to
     `Filter_exception_surfaces_unwrapped_and_is_recorded_on_activity` (asserts an unwrapped
     `InvalidOperationException` plus an `exception` event on the `streambus.publish` activity
     via an `ActivityListener`); `Foreign_cancelled_exception_from_filter_is_not_wrapped_as_transient`
     (filter cancels its own CTS; caller token untouched → OCE propagates, not wrapped);
     `Transport_failure_without_filters_is_recorded_on_publish_activity` (no-filter publish whose
     transport throws `IOException` → producer-wrapped transient, now recorded on the activity).
     CHANGELOG (`### Breaking`) and `docs/guide/publish.md` document the semantics change.
     UnitTests 464/464; the brokered integration suite could not run in this environment
     (no RabbitMQ broker is listening).
  6. **Review follow-up (2026-11-01):** a fresh `csharp-reviewer` pass confirmed the production
     change is correct (transport transient classification, double-recording, and
     `OperationCanceledException`-race paths all verified) and flagged only test-harness
     robustness, which was addressed: the `ActivityListener` is now disposed in a
     `try/finally` around each test body (previously it leaked on any failing assertion and
     polluted later tests); activity selection is scoped to the test's
     `streambus.stream` tag instead of `SingleOrDefault` over all publish activities; an
     `ActivityEvent`-is-a-struct bug in the helper (`FirstOrDefault` returning a
     non-null boxed default) was fixed; the foreign-OCE test now pins both the
     never-record-cancellation tracing contract (`GetPublishExceptionEvent` is null) and
     that the *filter's own* token crossed the boundary (`ex.CancellationToken ==
     filter.Own.Token`); the CHANGELOG breaking entry was widened to note that deterministic
     pipeline-terminal failures (e.g., serializer errors) also surface raw — intended, since
     callers should not retry them either. UnitTests 464/464 after follow-up.

---

**[PUB-4] Requeued/replayed messages lose user headers and ContentType — VALID, fixed (2026-11-02)**
- **Severity:** IMPORTANT
- **File:** src\StreamBus\ErrorEnvelope.cs (lines 61–73; consumer at src\StreamBus\Bus.cs line 242)
- **Problem:** When a failed message is requeued, the drain path parses only the `streambus.error.*` properties — the original user application headers (`SendHeaders`) are dropped at parse time. `ToReplayOptions` then rebuilds `StreamMessageProperties` from just `MessageId`/`CorrelationId`/`SentTime`, so the republished message carries no `x-tenant`-style headers on the wire and no `ContentType`. Downstream consumers receive a semantically different message than the one that failed.
```csharp
internal PublishOptions ToReplayOptions() => new()
{
    Properties = new StreamMessageProperties
    {
        MessageId = MessageId,
        CorrelationId = CorrelationId,
        SentTime = SentTime,
    },
};
```
- **Solution:** Persist the original user headers inside the envelope under a reserved key (e.g., `streambus.error.user-headers`), parse them back into a new `UserHeaders` property on the envelope, and include them in `ToReplayOptions()`. Preserve `ContentType` the same way, or stamp `_serializer.ContentType` on the requeue call the way `SendTypedAsync` does.

- **Verification & resolution (2026-11-02):** Confirmed genuine, and the loss is worse than the
  reviewer described — there are two drop points, not one:
  1. **At handoff:** the envelope never captured the original delivery headers. The
    `SendHeaders` carried on the envelope are an *empty-seeded write-only bag* for consume
     filter output (`StreamPartitionConsumer.cs` seeds `new StreamSendHeaders(null, ...)`),
     so original headers like `x-tenant` were lost before the message reached the error
     destination. The reviewer's "dropped at parse time" applies to what little remained.
  2. **At replay/requeue:** `ToReplayOptions` rebuilt properties from identity fields
     only, and `ContentType` is stamped only by the typed-publish layer (`Bus.cs:220`),
     which neither the requeue path (`Bus.RequeueErrorsAsync`) nor the replay path
     (`ConsumerRegistry.ReplayAsync`) uses.
  The fix keeps the original values end to end:
  1. `ErrorEnvelope` gained `UserHeaders` and `ContentType`. The original user headers are
     captured at exhaustion (`ErrorEnvelope.CaptureUserHeaders(consumeContext.Headers)`),
     which excludes every `streambus.*` framework key and values the republish path could
     not legally carry (anything but string/bool/int/long/null). `ConsumeContext` gained
     `ContentType` (set from the AMQP message properties in `BuildContext`, copied by the
     typed context factory), and both `ExhaustAsync` envelope constructions set both fields.
  2. Wire round trip (MassTransit model, revised after design review): user headers travel
     on the error destination as **plain keys next to the error metadata** — not repackaged
     into a reserved-key JSON blob. A first implementation used
     `streambus.error.user-headers` with a tagged-JSON encoding (the reviewer's literal
     suggestion), but AMQP field tables already carry string/bool/int/long with their
     types intact, so the encoding layer added complexity (and a JSON-vs-int/long
     fidelity problem it then had to solve) without adding fidelity. Consume-filter
     output (`SendHeaders`) wins collisions over original user headers, envelope fields
     win over both, and parse reconstructs `UserHeaders` from every non-`streambus.*`
     key (byte-array string values from classic queues are decoded; null values are
     treated as absent, matching AMQP's inability to carry them as application
     properties). `ContentType` is stored as a plain `streambus.error.content-type`
     string and lifted into the queue path's `BasicProperties.ContentType`.
     Documented AMQP size constraints: header keys and `content-type` are limited to
     255 bytes, and the whole headers table must fit `frame_max` (128 KiB by default).
  3. `ToReplayOptions` now restores both fields, so `Bus.RequeueErrorsAsync` and
     `ConsumerRegistry.ReplayAsync` republish with the original headers and media type.
     The error-destination copies themselves also carry the content type (the queue
     `BasicProperties.ContentType` and the error-stream publish properties), so
     inspection tooling sees the original media type too.
  4. Tests (TDD — the replay/round-trip/capture tests were written first and watched
     failing before the fix; the wire-format tests were added for the design revision and
     one was watched failing against the blob implementation, driving its removal):
     replay-options preservation, application-properties round trip, error-queue-headers
     round trip, `CaptureUserHeaders` framework-key/null/type filtering, missing-headers
     degrade, mixed primitive round trip (string/int/long/bool), raw flat keys on the
     wire (user headers as plain keys, filter output wins collisions), and byte-array
     header-value decoding on the queue path. UnitTests 471/471; the brokered
     integration suite could not run in this environment (no RabbitMQ broker is
     listening).
```csharp
internal PublishOptions ToReplayOptions() => new()
{
    Properties = new StreamMessageProperties
    {
        MessageId = MessageId,
        CorrelationId = CorrelationId,
        SentTime = SentTime,
        Headers = UserHeaders ?? new Dictionary<string, object?>(),
    },
};
```

---

**[PUB-5] Super handle implements `SendEnvelopeAsync` without routing — VALID, fixed (2026-11-02)**
- **Severity:** IMPORTANT
- **File:** src\StreamBus\ProducerHandles.cs (interface at lines 18–32; implementation at lines 259–268)
- **Problem:** `IStreamSuperProducerHandle` inherits `IStreamPlainProducerHandle`, so `SendEnvelopeAsync` is part of the super handle's public surface. Its implementation never calls `_routing.Stamp(message, ...)` (no routing key registered in the routing table), but the client's `Routing` callback fires during send and throws `KeyNotFoundException` — a non-`StreamBusException`, so it ends up misclassified as "transient". The interface doc claims the split "keeps transport-specific members off the wrong surface"; this method violates that. It is internally unreachable today only because the error hand-off always fetches the plain-slot handle, but it is a public interface.
```csharp
public interface IStreamSuperProducerHandle : IStreamPlainProducerHandle { ... }
```
```csharp
public Task SendEnvelopeAsync(
    ReadOnlyMemory<byte> body,
    StreamMessageProperties properties,
    CancellationToken ct)
{
    ArgumentNullException.ThrowIfNull(properties);
    var message = new Message(body.ToArray());
    StreamMessageProperties.ApplyTo(message, properties);
    return _confirmations.PublishAsync(_producer, _stream, message, ct);
}
```
- **Solution:** Do not derive `IStreamSuperProducerHandle` from `IStreamPlainProducerHandle`; derive both from `IStreamProducerHandle` and place `SendEnvelopeAsync` only where routing is genuinely supported. Alternatively, route this method through `SendMessageAsync` like every other send on the class.
```csharp
public interface IStreamSuperProducerHandle : IStreamProducerHandle
{
    Task PublishWithIdAsync(ulong publishingId, ReadOnlyMemory<byte> body, PublishOptions options, CancellationToken ct);
    Task PublishReplayAsync(ReadOnlyMemory<byte> body, ulong publishingId, PublishOptions options, CancellationToken ct);
}
```

- **Verification & resolution (2026-11-02):** Confirmed genuine on every factual
  claim, and the "internally unreachable" clause holds:
  1. `SuperStreamProducerHandle.SendEnvelopeAsync` never called
     `_routing.Stamp`, and `SuperStreamRouting.Route` is the dictionary
     indexer (`_table[message]`), which every super producer passes as the
     client's `Routing` callback (`Connector.cs:95/122`) — so the method threw
     `KeyNotFoundException` on every call, mid-send, outside the
     `StreamBusException` hierarchy.
  2. Reachability bounded: the only internal caller is
     `ProducerRegistry.PublishEnvelopeAsync`, which requests the plain slot
     (`super: false`), and the replay/requeue paths explicitly reject super
     streams (`ConsumerRegistry.cs:542–554`). A latent public-API trap, not a
     live outage — IMPORTANT is the correct severity.
  3. The reviewer's alternative fix ("route this method through
     `SendMessageAsync`") was rejected: `SendMessageAsync` requires a routing
     key and refuses to send without one, and `StreamMessageProperties` carries
     no routing key — the envelope send is intrinsically a plain-stream
     operation, so there is nothing to make work.
  4. **Applied fix:** `IStreamSuperProducerHandle` now derives directly from
     `IStreamProducerHandle`; `SendEnvelopeAsync` lives only on
     `IStreamPlainProducerHandle`. `PublishReplayAsync` stays on the super
     surface (declared there directly) since it routes via a caller-supplied
     key. The un-routed `SuperStreamProducerHandle.SendEnvelopeAsync`
     implementation was deleted. Misuse is now a compile error instead of a
     runtime crash. Test stubs that used one handle object for both slots
     (`PublishTests`, `EffectivelyOnceTests`, `AcceptanceTests`,
     `PublishRoutingTests`, `ResultPublisherTests`) now implement both
     interfaces explicitly.
  5. Tests (TDD — watched the surface tests fail first for exactly the
     expected reasons: `SendEnvelopeAsync` present on the super interface,
     super assignable to plain): new `ProducerHandleSurfaceTests` pins by
     reflection that the super handle exposes no `SendEnvelopeAsync`, is not
     assignable to the plain handle contract, keeps
     `SendAsync`/`PublishReplayAsync`/`PublishWithIdAsync`/`PublishResultAsync`,
     and that the plain handle keeps envelope + replay sends. CHANGELOG
     records the interface split under `### Breaking`. UnitTests 475/475
     (471 prior + 4 new); integration suite unavailable in this environment
     (no broker) but builds clean against the new surface.

---

**[CON-3] Non-EO dispatch maintains `_state.Inflight` decrements in five hand-placed spots — fragile, with future leak risk — VALID, fixed (2026-11-03)**
- **Severity:** IMPORTANT
- **File:** src\StreamBus\StreamPartitionConsumer.cs (lines 520–601 vs 913–917)
- **Problem:** `DispatchEffectivelyOnceAsync` correctly owns the decrement in a single `finally` (913–917). The non-EO path instead decrements at 545, 565, 578, 590 individually, and the shutdown catch at 585–589 returns after the 520 increment without decrementing. Any future branch added to the region (e.g., the fix for CON-1!) reintroduces the leak.
```csharp
// 585-589: returns after the increment at 520
catch (OperationCanceledException) when (attach.LoopToken.IsCancellationRequested)
{
    // Shutdown raced dispatch; exit quietly without signalling failure.
    return;
}
Interlocked.Decrement(ref _state.Inflight);
Interlocked.Exchange(ref _state.RetryBlocked, 0);
```
- **Solution:** Restructure the non-EO path exactly like the EO one: increment at the top, then wrap the attempt loop, exhaustion, and store in one `try` whose `finally` does the decrement/reset. Delete all five scattered decrement sites.
```csharp
Interlocked.Increment(ref _state.Inflight);
try
{
    ...attempt loop / ExhaustAsync / store...
}
finally
{
    Interlocked.Decrement(ref _state.Inflight);
    Interlocked.Exchange(ref _state.RetryBlocked, 0);
}
```

- **Verification & resolution (2026-11-03):** Confirmed genuine on the structural claim, with two
  corrections to the exact failure inventory:
  1. **Fragility confirmed and already realized once.** At verification time the non-EO path had
     *five* scattered decrement sites (not four as counted against the pre-CON-1 code — the
     CON-1 foreign-token OCE catch had added exactly the predicted kind of branch, each requiring
     its own hand-placed decrement). The EO method's "single finally owns the decrement" comment
     (`DispatchEffectivelyOnceAsync`) is the in-codebase proof of the intended pattern; the
     non-EO path simply didn't follow it.
  2. **Line-number accuracy.** The reviewer's line numbers predate CON-1's fix; in the
     post-CON-1 code the sites were 571/581/602/615/627 and the shutdown-race catch was 622–626.
     The "shutdown catch returns without decrementing" hazard was real in the *original* shape:
     that catch was reachable (an `OperationCanceledException` on the loop token escaping
     `ExhaustAsync` after exhaustion), and in the pre-CON-1 code such a path leaked the counter.
     The CON-1 fix had already de-fanged the specific exhaustion-adjacent paths by moving the
     decrement ahead of `ExhaustAsync` — which traded correctness for an accuracy bug:
     `Inflight` dropped to 0 *before* the exhaustion hand-off ran, so a blocked hand-off
     (retry-forever publish, `OnExhausted` waiting on its token) was invisible to the
     `streambus_partition_inflight` gauge and to replay-drain idle detection while the dispatch
     gate was still held. Either way, every future branch in the region was one forgotten
     decrement away from a real leak — the pattern the reviewer flagged.
  3. **Applied fix** (`StreamPartitionConsumer.DispatchAsync`): the EO branch is hoisted out
     (its own method's `finally` remains the sole owner of its decrement, avoiding a
     double-decrement); the non-EO attempt loop, exhaustion, and shutdown-race catch now sit in
     one `try` with a single `finally` doing `Decrement(Inflight)` + `RetryBlocked = 0`,
     mirroring the EO method. All five scattered sites deleted, the unreachable-on-success
     `_eo is null` store guard simplified away. The success store still runs after the
     decrement (unchanged ordering); `StoreAndTrackAsync` failures still propagate to the
     dispatch safety net.
  4. **Tests (TDD — watched RED first, failing exactly at the in-handoff assert: pre-fix
     `Inflight == 0` while the hand-off was blocked):**
     `DispatchShutdownReleaseTests.Shutdown_racing_exhaustion_handoff_releases_the_inflight_decrement`
     pins that the inflight counter is held (1) while the `OnExhausted` hand-off is blocked, and
     that a shutdown racing the blocked hand-off releases both `Inflight` and `RetryBlocked`
     back to 0. UnitTests 476/476; the brokered integration suite could not run in this
     environment (no RabbitMQ broker is listening) but builds clean.

---

**[CON-4] Two competing exhaustion callbacks: `OnRetryExhausted` only fires on the ErrorStream branch — VALID (structurally), fixed (2026-11-03)**
- **Severity:** IMPORTANT → resolved as a public-API break
- **File:** src\StreamBus\RetryPolicy.cs (lines 45–54) + src\StreamBus\Configuration\RetryConfiguration.cs (lines 57–75) + src\StreamBus\StreamPartitionConsumer.cs (lines 643–789)
- **Problem:** Two exhaustion hooks exist with different shapes: `ErrorHandoff` + `OnRetryExhausted` (no `CancellationToken`) and `ExhaustedContext` + `ExhaustionCallback` (with token and `Services`). `ExhaustAsync` dispatches to the callback branch, the dead-letter branch, or the error-stream branch — and `OnRetryExhausted` only executes inside the ErrorStream branch. A user who registers `OnRetryExhausted` with a queue dead-letter or callback destination is never notified about exhausted messages at all.
```csharp
// RetryPolicy.cs
public Func<ErrorHandoff, Task>? OnRetryExhausted { get; init; }
// StreamPartitionConsumer.cs 748: only inside the ErrorStream branch
if (_options.OnRetryExhausted is { } hook)
```
- **Solution:** Keep one callback. `ExhaustedContext`/`ExhaustionCallback` is the better shape (has `CancellationToken`, `Services`, `Attempts`). Delete `OnRetryExhausted`/`ErrorHandoff` (or at minimum fire it on every exhaustion path); port any payload it provides into `ExhaustedContext`.

- **Verification & resolution (2026-11-03):** Structurally genuine, with the reported
  failure path refuted as unreachable; resolved by unifying the surface anyway (breaking
  changes are acceptable pre-release).
  1. **Refuted as a user-facing bug:** `OnRetryExhausted` and the dead-letter/callback
     branches cannot coexist through the public API. `DeadLetter` and `ExhaustionCallback`
     were `internal` on `ConsumeOptions`; the endpoint configurator (the only producer of
     those fields) never set `OnRetryExhausted`; and `ConsumerRegistry` validation rejected
     bounded retry without a handoff and unbounded retry with one while `hasHandoff`
     excluded `OnRetryExhausted` — so the only constructible combination was bounded +
     `ErrorStream`, exactly the branch where the hook fired. The reviewer also cited
     `RetryPolicy.cs:45-54`, which is `ErrorStreamOptions`, not the hook.
  2. **Genuine as design debt:** two exhaustion shapes in one assembly, one of them
     undocumented in the guides and observable only on the raw path, is a trap for the next
     maintainer (`StreamBusConfigurator` maps `OnExhausted` to the internal duplicate, so a
     future field added to one shape silently misses the other).
  3. **Applied fix (unify, do not delete-and-lose):** `ConsumeOptions.OnExhausted`
     (`Func<ExhaustedContext, CancellationToken, Task>`) is now public and is the single
     callback for both the raw and endpoint APIs; `OnRetryExhausted`, `ErrorHandoff`, and
     the internal `ExhaustionCallback` duplicate are deleted. `ExhaustAsync` runs the one
     callback branch, and the post-handoff observe-only hook block is gone. The raw path
     rejects `ErrorStream` + `OnExhausted` with `StreamBusConfigurationException`
     (endpoints keep resolving the ambiguity by shadowing the destination with a startup
     warning, which is unchanged).
  4. Tests (TDD — the new tests failed first with `CS0117: 'ConsumeOptions' does not
     contain a definition for 'OnExhausted'`): `RetryPolicyTests.Bounded_with_on_exhausted_callback_is_accepted`
     (the callback counts as a handoff) and `Bounded_with_error_stream_and_on_exhausted_throws`;
     the raw-path integration test was rewritten as
     `AcceptanceIntegrationTests.OnExhausted_owns_exhaustion_and_advances_past_poison`
     (full `ExhaustedContext` fields, returning advances, no error-stream copy). UnitTests
     478/478; IntegrationTests 71/71 with 1 pre-existing cluster-gated skip
     (`Leader_placement_gate_G3`, needs a 3-node cluster); build clean (0 warnings).
  5. **Docs/changelog:** CHANGELOG `### Breaking` records the surface change and the raw-path
     mutual exclusivity; `docs/guide/errors.md` documents the shared callback; the
     acceptance evidence row/notes point at the new test.

---

**[CON-5] Registry validation leaves unbounded-forever-retry as the implicit default — REFUTED as IMPORTANT, doc-only (2026-11-03)**
- **Severity:** IMPORTANT → minor (refuted as IMPORTANT; documentation-only)
- **File:** src\StreamBus\ConsumerRegistry.cs (lines 212–227)
- **Problem:** The validation demands that bounded retry have a handoff and unbounded retry have none — but `new RetryPolicy()` (the `ConsumeOptions` default) already has `MaxAttempts = null`, so a user who touches nothing gets a policy where a persistent handler failure halts the partition indefinitely with no dead-letter escape. Configurator users always get bounded policies via `IRetryConfigurator`; raw `ConsumeOptions` users get silently different, dangerous semantics.
```csharp
if (retry.MaxAttempts is null)
{
    if (hasHandoff)
    {
        throw new StreamBusConfigurationException(
            "ConsumeOptions.Retry is unbounded (MaxAttempts is null); a hand-off destination (ErrorStream/DeadLetter/OnExhausted) cannot fire. Set MaxAttempts or remove the destination.");
    }
}
```
- **Solution:** Require an explicit choice: reject a default-constructed unbounded policy unless the user opts in (e.g., `RetryPolicy.StrictForever()`), or at minimum document on `ConsumeOptions.Retry` and `RetryPolicy.MaxAttempts` that the default means "the partition halts on any persistent handler failure".

- **Verification & resolution (2026-11-03):** Refuted as IMPORTANT; the mechanics are real but the
  "implicit / silently different" defect is not. Closed with a documentation-only change.
  1. **Confirmed mechanically:** `ConsumeOptions.Retry` defaults to `new RetryPolicy()`
     (`ConsumeOptions.cs:25`) → `MaxAttempts = null`, and `ConsumerRegistry.ConsumeAsync`
     (220–227) rejects unbounded retry only *with* a hand-off, so unbounded-without-hand-off
     passes. A persistent handler failure therefore retries forever and the partition never
     advances — the behavior the review describes.
  2. **Refuted — no raw-vs-fluent divergence:** the review claims "configurator users always get
     bounded policies via `IRetryConfigurator`". `RetryDerivation.Derive`
     (`RetryConfiguration.cs:188–195`) falls back to `new RetryPolicy()` when neither
     `UseMessageRetry` nor `UseRetry` is configured and returns
     `ExhaustionKind.StrictForever`; `RetryConfigurationTests.Unbounded_default_is_strict_forever`
     pins exactly that. Both APIs share one default.
  3. **Refuted — it is the specified default, not an implicit one:** FR-4.4 defines
     `FailureMode.Strict` (now expressed as `MaxAttempts is null`) as the default with
     retry-forever/stall semantics; `docs/guide/errors.md:63` and `docs/llms.txt:47` state the
     same table row. Effectively-once *requires* unbounded retry
     (`EffectivelyOnceValidation.ValidateConsume`, `EffectivelyOnce.cs:90–91`), and both
     `EffectivelyOnceOptions.Retry` and `PublishResultOptions.Retry` default to `new()`, so
     rejecting a default-constructed unbounded policy would break the EO contract too.
  4. **Considered and rejected:** the review's primary suggestion (explicit
     `RetryPolicy.StrictForever()` opt-in, reject the default) is a breaking behavior change that
     contradicts FR-4.4 and the pinned default test, for a documented at-least-once (never-skip,
     partition back-pressure) design choice. The hazard is already documented on
     `RetryPolicy.MaxAttempts` (added in the CON-1 follow-up), which points at the bounded +
     hand-off alternative.
  5. **Applied (doc-only):** `ConsumeOptions.Retry` carried no XML doc, so the raw path's
     IntelliSense did not surface the default. It now states that the default sets no attempt
     limit (retry forever, partition waits) and names every hand-off: `ErrorStream` (error
     stream), `OnExhausted` (callback), and the endpoint API's `DeadLetterToQueue` (dead-letter
     queue). Talk of "bounded/unbounded" was replaced with "attempt limit / no attempt limit"
     across the user-facing surface (`RetryPolicy`, `ConsumeOptions`, `EffectivelyOnceOptions`,
     `PublishResultOptions`, the three configuration exception messages, the retry-mode log value,
     `docs/guide/errors.md`, `docs/guide/effectively-once.md`, `docs/llms.txt`); the two
     message-pinning assertions in `RetryPolicyTests` were updated to match. No validation or
     runtime change; build clean and unit suite green.

---

**[CON-6] Duplicated readiness checks and duplicated teardown catch blocks**
- **Severity:** IMPORTANT
- **File:** src\StreamBus\ConsumerRegistry.cs (lines 250–260 vs 390–400; 339–361 vs 476–497)
- **Problem:** `ConsumeAsync` and `ConsumeEffectivelyOnceAsync` contain identical readiness-probe code, and each contains a line-for-line duplicate of the same teardown `catch` block (cancel family, await loops, dispose loops/scopes/tracker, rethrow). `EnsureReadyAsync` already exists but isn't reused. Copy-pasted teardown is exactly where a future fix lands in one copy and not the other.
```csharp
// 250-260 (ConsumeAsync) is identical to 390-400 (ConsumeEffectivelyOnceAsync)
var readiness = await _supervisor.WaitUntilReadyAsync(options.ReadinessPolicy, ct).ConfigureAwait(false);
if (readiness == BusReadiness.Fatal)
{
    throw new StreamBusFatalException($"StreamBus for vhost '{_vhost}' is in Fatal state; consume of '{stream}' refused.");
}
```
- **Solution:** Extract `private Task<IStreamSystemHandle> RequireReadyAsync(ReadinessPolicy policy, string operation, CancellationToken ct)` and `private async Task TearDownAsync(CancellationTokenSource family, List<(StreamPartitionConsumer, Task)> loops, List<IDisposable> scopes, RegistrationTracker? tracker)`; call both from the two catch blocks.

- **Verification & resolution (2026-11-05):** Confirmed genuine, with two corrections to the
  reported shape:
  1. **Readiness duplication confirmed:** the probe blocks in `ConsumeAsync` (255–265) and
     `ConsumeEffectivelyOnceAsync` (395–405) are identical apart from the operation noun in the
     exception messages. `EnsureReadyAsync` existed (720–731) but could not be reused verbatim:
     it hard-coded `ReadinessPolicy.FailFastPolicy`, always said "offset operation", and did not
     return the handle.
  2. **Teardown duplication confirmed, and there was a third copy:** the two failed-start `catch`
     blocks (344–366, 481–502) were line-for-line identical except one comment, and
     `StreamConsumerRegistration.DisposeAsync` (45–70) ran the same cancel/drain/dispose
     sequence with only `_onStopped`/`_deregister` added.
  3. **Applied fix:** `RequireReadyAsync(policy, operation, ct)` now owns the probe and returns
     `IStreamSystemHandle`; both consume paths and the four former `EnsureReadyAsync` call sites
     (offset query/store, replay, result publish) use it, and `EnsureReadyAsync` is deleted.
     `ConsumerTeardown.RunAsync(family, loops, scopes, onStopped, onDisposed,
     swallowRunTaskFailures)` is the single teardown owner for the registration's
     `DisposeAsync` and both failed-start catch blocks; the callers keep `throw;` so the original
     failure still surfaces, and `swallowRunTaskFailures` preserves the old
     registration-propagates / failed-start-swallows exception split.
  4. **Tests:** no new tests (per review decision); the change is behavior-preserving by
     inspection. Build clean (0 warnings), UnitTests 478/478. The brokered integration suite was
     not run (no RabbitMQ broker listening).

---

**[CON-7] `ExhaustedContext.GetService<T>` throws a configuration exception for a runtime lookup miss**
- **Severity:** IMPORTANT
- **File:** src\StreamBus\Configuration\RetryConfiguration.cs (lines 72–74)
- **Problem:** A missing DI registration at message-exhaustion time is a runtime resolution failure, but this throws `StreamBusConfigurationException`, misleading callers into thinking their *bus configuration* is invalid and polluting configuration-exception handling.
```csharp
public T GetService<T>() where T : notnull
    => (T)(Services.GetService(typeof(T))
        ?? throw new StreamBusConfigurationException($"ExhaustedContext has no service of type {typeof(T).FullName} registered."));
```
- **Solution:** Delegate to standard required-service semantics, which throw `InvalidOperationException`:
```csharp
public T GetService<T>() where T : notnull
    => Services.GetRequiredService<T>();
```

- **Verification & resolution (2026-11-05):** Genuine, with severity corrected from IMPORTANT to
  MINOR. The defect is real but has no behavioural impact:
  1. **Confirmed against the code**: `RetryConfiguration.cs:72-74` matched the review verbatim, and
     the type contradicted the library's own documented exception contract —
     `docs/guide/errors.md:164` defines `StreamBusConfigurationException` as "Invalid or contradictory
     configuration" that the user fixes before running. Every other throw of that type in the
     codebase is a registration/validation/build-time check; a missing DI registration inside an
     `OnExhausted` callback happens after the bus started and cannot be a bus-configuration defect.
  2. **Refuted as IMPORTANT (no runtime consequence)**: `ExhaustedContext.Services` is the
     per-delivery DI scope (`StreamPartitionConsumer.cs:691,705`) and the callback runs through
     `ExhaustionCallbackRunner.RunAsync` (`RetryConfiguration.cs:208-236`), which catches every
     exception, logs a warning, and recycles the attach. The exception type never reaches library
     classification logic, so retryability, dispatch, and health behaviour are unchanged; the type
     only reaches the user's own callback-level `catch` and the log entry. Nothing on the startup
     path (`Hosting.cs:517`) can observe it, so "polluting configuration-exception handling" is
     limited to user code.
  3. **Applied fix:** `GetService<T>()` now delegates to `Services.GetRequiredService<T>()`, with an
     XML doc stating the `InvalidOperationException` contract. The review's alternative — deleting
     the helper and making users call `ctx.Services.GetRequiredService<T>()` like
     `ConsumeContext`/`PublishContext` — was rejected as an unnecessary public-API break; the helper
     is documented (`docs/guide/errors.md`).
  4. **Deliberate consequence:** the failure is no longer a `StreamBusException`. Verified at compile
     time — an attempt to assert `missing is StreamBusException` in the test is `CS0184` (unrelated
     types), which is exactly the intended classification: a missing user registration is a DI
     error, not a library error.
  5. **Tests (TDD — watched RED first, failing because the old code threw
     `StreamBusConfigurationException`):** `RetryConfigurationTests.Exhausted_context_resolves_services`
     now asserts `InvalidOperationException` and that the standard DI message names the missing type.
     UnitTests 478/478; the brokered integration suite was not run (no RabbitMQ broker listening).
  6. **Docs/changelog:** `docs/guide/errors.md` notes the `InvalidOperationException` contract next
     to `ctx.GetService<T>()`; CHANGELOG `### Breaking` records the exception-type change for callers
     that caught the configuration exception around this call.

---

**[CON-11] Naming drift: "ReceiveEndpoint" in errors with no such type; three consumer type names; inconsistent health registration schemes**
- **Severity:** IMPORTANT
- **File:** src\StreamBus\ConsumerRegistry.cs (lines 160, 431, 790–794) + src\StreamBus\Configuration\StreamBusConfigurator.cs (lines 564, 571)
- **Problem:** Error messages say `ReceiveEndpoint '{stream}'` but the public types are `IStreamConsumer`/`StreamConsumerRegistration`/`StreamPartitionConsumer` — a user reading the message has no type to grep for. Health rows additionally mix identity schemes: `eo:{sourceStream}` for effectively-once, `raw:{stream}` for unnamed registrations, and `busType.FullName` for markers. Three names for one concept plus inconsistent telemetry identity is classic generated-code drift.
```csharp
// ConsumerRegistry.cs 160
$"ReceiveEndpoint '{stream}': consume filter '{registration.FilterType}' does not implement IStreamConsumeFilter<{messageType.Name}> for this endpoint's message type.");
// 431 vs 790-794
new RegistrationTracker(_health, _vhost, $"eo:{sourceStream}", sourcePartitions.Length);
return sequence == 1 ? $"raw:{stream}" : $"raw:{stream}:{sequence}";
```
- **Solution:** Pick one term ("consumer" or "endpoint") and use it in errors, type names, and health rows. Unify health registration names with a single helper (e.g., `$"consumer:{busName}:{stream}"` / `$"consumer:eo:{busName}:{sourceStream}"`).

- **Verification & resolution (2026-11-05):** Partially genuine — the naming claims are refuted, but the
  telemetry-identity problem underneath them is real and was fixed.
  1. **"`ReceiveEndpoint` in errors has no such type" — refuted.** `ReceiveEndpoint(string,
     Action<IReceiveEndpointConfigurator>)` is the public endpoint-configuration API
     (`StreamBusConfigurator.cs:34`), backed by public `IReceiveEndpointConfigurator` (`:93`) and
     `ReceiveEndpointDescriptor`. The messages name exactly what the user wrote; grepping `ReceiveEndpoint`
     finds the configuration surface. The "three consumer type names" are `IStreamConsumer` (the public
     handle) plus `StreamConsumerRegistration` and `StreamPartitionConsumer`, both `internal`
     implementation types users never reference — roles, not drift. The cited `ConsumerRegistry.cs:160` is
     now `:191`, so the item also predates recent commits.
  2. **"`busType.FullName` for markers" — refuted.** No health row is keyed by a bus type alone. Rows are
     `consumer` rows (`ConsumerRegistry.cs:838`) and `declare:{name}` rows
     (`Hosting.cs:595/599/685/689`); `busName` was used only for metrics and log scoping.
  3. **The underlying identity problem was real.** Health rows key `{vhost}|{registration}` on a per-vhost
     aggregator (`HealthAggregator.cs:31`, `Registry.cs:109-110`), while typed endpoints registered under
     the bare stream name and `eo:`/`raw:` rows carried no bus name. Two buses on one vhost consuming the
     same stream therefore shared one row and overwrote each other's state — the check could report
     `Attached` while one bus was `FailedRetrying`. That is exactly the multi-bus case the review's
     "inconsistent telemetry identity" pointed at, even though its evidence was wrong.
  4. **Applied fix:** new `ConsumerHealthNames` centralizes the three consumer schemes and every name
     carries the bus type: `endpoint:{busName}:{stream}`, `raw:{busName}:{stream}` (`:2`, `:3`, … on repeat
     raw consumes of the same stream on the same bus; `_rawSequence` is now keyed by (bus, stream)), and
     `effectively-once:{busName}:{sourceStream}`. Declared endpoints enter through the new internal
     `StreamConsumerRegistry.ConsumeDeclaredEndpointAsync`, so the registry owns the scheme without adding a
     mode parameter to the raw `ConsumeAsync` surface; `Hosting` calls that method.
  5. **Rejected:** the proposed rename of the `ReceiveEndpoint` messages and the "pick one term" sweep.
     The messages already name the public API.
  6. **Docs/changelog:** `docs/guide/hosting.md`, `docs/guide/observability.md`, the A6 architecture spec,
     and CHANGELOG (`### Fixed`) describe the new keys.
  7. **Tests (TDD — watched RED first):** restoring the old names made the three bus-attribution tests fail
     (`Raw_consumers_are_first_class_registrations`,
     `Raw_consumer_rows_on_a_shared_vhost_name_the_bus_that_owns_them`,
     `Declared_endpoint_rows_name_the_bus_that_owns_them`), and
     `Effectively_once_rows_name_the_bus_that_owns_them` failed with the row name `eo:…` before the
     `effectively-once:` rename. UnitTests 481/481; solution builds with 0 warnings. The brokered
     integration suite was not run (no RabbitMQ broker listening).

---

**[CON-13] `_inflight` tracks only the latest dispatch; earlier queued dispatches can run after `DisposeAsync` completes**
- **Severity:** IMPORTANT
- **File:** src\StreamBus\StreamPartitionConsumer.cs (lines 264–273, 434–480)
- **Problem:** Each delivery assigns `_inflight = dispatch` (latest wins) while earlier dispatch tasks can still be queued on `_dispatchGate` (the gate serializes but does not cancel queued waiters). `DisposeAsync` awaits only the single tracked task, so a queued dispatch released after dispose runs the handler against a torn-down attach state, touching disposed objects (`_loopCts` disposed at 473).
```csharp
MessageHandler = (c, context, message) =>
{
    var tracked = _consumerHandle.Current ?? new RawConsumerHandle(c);
    var dispatch = DispatchAsync(tracked, context, message, attach);
    lock (_life)
    {
        _inflight = dispatch;
    }
    return AwaitTrackedAsync(dispatch);
},
```
- **Solution:** Replace `_inflight` with a counted set: increment a counter on queueing, decrement in `DispatchAsync`'s finally, and have `DisposeAsync` await a `TaskCompletionSource` that completes when the count reaches zero. Ensure queued dispatches observe the disposed/drained state and exit before touching state.
```csharp
// in message handler:
Interlocked.Increment(ref _inflightCount);
var dispatch = DispatchAsync(...);
lock (_life) { _trackedDispatches.Add(dispatch); }
_ = dispatch.ContinueWith(
    _ => { Interlocked.Decrement(ref _inflightCount); if (_inflightCount == 0) _drainedTcs.TrySetResult(); },
    TaskScheduler.Default);
// DisposeAsync: await _drainedTcs.Task (with timeout)
```

- **Verification & resolution (2026-11-06):** Structurally genuine; the exact failure
  description needed correction, and the fix is smaller than the counted-set rewrite.
  1. **Confirmed:** `_inflight` is a single latest-wins task (`MessageHandler` assigns it
     per delivery) and `DisposeAsync` awaits only the snapshot, while `_dispatchGate`
     serializes waiters without cancelling them. `attach.Drained` is only set on a failed
     attach, so a dispatch from a *newer* (current) generation is not protected by it.
  2. **Reachability correction:** `RabbitMQ.Stream.Client` (1.12.1) dispatches
     `RawConsumerConfig.MessageHandler` strictly sequentially — `ProcessChunks` awaits
     `ParseChunk`, which awaits each `DispatchMessage`/handler — so one attach cannot have
     two queued dispatches. Queueing requires cross-generation overlap: `Close()` cancels
     the client token but does not await a handler already in progress, and the attach loop
     recreates after backoff while that handler can still run. An old-generation dispatch
     is drained and exits at the existing check; a queueing dispatch from the *new*
     generation is not. The literal "runs after `DisposeAsync` returns" variant additionally
     needs the `_inflight` assignment inversion (the older handler's synchronous prefix,
     including user code before its first `await`, outlasts the newer attach's start and
     overwrites `_inflight` with the older task). Narrow, but both are genuine.
  3. **Applied fix** (`StreamPartitionConsumer.DispatchAsync`): after acquiring the gate,
     a dispatch exits when `_disposed != 0`, before building a context or invoking any
     handler. Disposal already waits for the running dispatch — or for a later queued one
     that waits behind it on the FIFO gate — so the flag closes the queued-waiter gap
     without the reviewer's counter/TCS machinery.
  4. **Test (TDD — watched RED first with `Expected: 0 / Actual: 1`):**
     `DisposeDispatchTests.Queued_dispatch_does_not_run_after_dispose` queues a second
     delivery behind a blocked first dispatch, starts `DisposeAsync`, and asserts the
     queued handler never runs. UnitTests 482/482; solution builds with 0 warnings. The
     brokered integration suite was not run (no RabbitMQ broker listening).

---

**[INF-2] `HandOffAsync` retries every exception forever, including fatal ones — partition wedges permanently — VALID, fixed (2026-11-06)**
- **Severity:** IMPORTANT
- **File:** src\StreamBus\ErrorPublisher.cs (lines 64–101)
- **Problem:** The handoff retry loop is `while (true)` with a bare `catch (Exception ex)`; only cancellation escapes. Fatal, non-recoverable errors — a missing error stream, an auth failure, `StreamBusFatalException` — spin forever at the caller-supplied delay. This loop runs on the error-handoff path of a partition dispatch loop whose contract is "never advances past an unconfirmed handoff", so one permanently broken error destination wedges that partition for the process lifetime with only warning logs. Every other retry loop in the codebase classifies fatal vs transient; this one doesn't.
```csharp
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
    catch (OperationCanceledException) when (ct.IsCancellationRequested) { throw; }
    catch (Exception ex)
    {
        _logger.LogWarning(ex, "...; retrying without advancing.", ...);
        var delay = delayFor(attempt, ex);
        if (delay < TimeSpan.Zero) delay = TimeSpan.Zero;
        await Task.Delay(delay, ct).ConfigureAwait(false);
    }
}
```
- **Solution:** Add a fatal classifier (defaulting to `StreamBusErrorClassification.IsFatal`); on fatal, throw immediately so the existing exhaustion/dispatch machinery handles it visibly instead of silently wedging the partition.
```csharp
catch (Exception ex)
{
    if (ex is StreamBusFatalException || StreamBusErrorClassification.IsFatal(ex))
        throw;   // let the visible recycle/exhaustion path own permanent failures
    ...
}
```

- **Verification & resolution (2026-11-06):** Confirmed genuine on the core mechanics; IMPORTANT
  is the correct severity. Three corrections to the review's framing:
  1. **"A missing error stream" is not fatal by the proposed classifier.** `CreateAsync` maps
     `ResponseCode.StreamDoesNotExist` to `StreamBusStreamNotFoundException`
     (`ProducerRegistry.cs:213-216`), which `StreamBusErrorClassification.IsFatal` does not match.
     Retrying it is correct (topology convergence: the destination may be declared or recreated),
     so the classifier deliberately keeps it on the retry path. The genuinely unrecoverable cases
     are `StreamBusFatalException` (the supervisor latches `Fatal` and returns —
     `ConnectionSupervisor.cs:97-101`; the AMQP transport latches `_fatal` —
     `ErrorQueueTransport.cs:485-500`), fatal protocol/SSL/auth errors, and configuration errors.
  2. **The reviewer's snippet alone would not catch fatal stream-path transport errors.**
     `ProducerRegistry.PublishEnvelopeAsync` had no fatal arm (unlike `PublishAsync`), so a
     `ProtocolException`/`SslException`/auth failure from `SendEnvelopeAsync` was wrapped as
     `StreamBusPublishTransientException`; `IsFatal(ex)` in `HandOffAsync` would only ever have
     matched a raw `StreamBusFatalException` from the readiness latch (and the queue transport).
     A matching fatal arm was added to `PublishEnvelopeAsync` (`ProducerRegistry.cs:102-111`).
  3. **Throwing is visibility, not recovery.** It converts a silent stall into
     `FailAttach` → health `FailedRetrying`/`Unhealthy` → attach recycle → redelivery (handler
     re-runs, never a skip). Infinite retry stays the rule for transient/topology failures, per
     the phase-6 ruling ("handoff publish failure retries until confirmed or cancelled and NEVER
     advances", `docs/superpowers/plans/2026-10-13-streambus-phase6-retry.md:65`). The v2 plan had
     already specified an `isFatal` classifier for `HandOffAsync`
     (`docs/superpowers/plans/2026-10-19-streambus-v2-architecture-and-middleware.md:137`); it was
     never implemented, which corroborates the gap.
  4. **Applied fix** (`ErrorPublisher.HandOffAsync`): new `IsFatalHandoffFailure` guard
     (`StreamBusFatalException`, `StreamBusConfigurationException`, or
     `StreamBusErrorClassification.IsFatal`) logs at Error and rethrows before the generic retry
     catch, so the dispatch safety net owns the failure without consuming an error-destination
     retry. `ProducerRegistry.PublishEnvelopeAsync` rethrows fatal transport errors raw instead of
     wrapping them as transient, mirroring `PublishAsync`.
  5. **Tests (TDD — all three watched RED):**
     `Fatal_handoff_failure_is_not_retried_and_commits_nothing` (pre-fix: the loop retried until
     the test CTS fired, so the await threw `TaskCanceledException` instead of
     `StreamBusFatalException`), `Protocol_handoff_failure_is_not_retried` (`IsFatal` path), and
     `Fatal_envelope_failure_is_not_wrapped_as_transient` (pre-fix: the envelope registry returned
     `StreamBusPublishTransientException` wrapping the protocol error).
     `Missing_error_stream_is_retried_until_the_destination_exists` pins the deliberate
     non-fatal classification. `ErrorHandoffTests.Publish_failure_retries_then_commits_once_after_confirmation`
     continues to pin transient retry. Build clean (0 warnings); UnitTests 486/486;
     IntegrationTests 70/70 with 2 pre-existing cluster-gated skips. The integration run also
     surfaced one pre-existing stale assertion from the CON-5 wording change
     (`TypedStartupIntegrationTests.Unbounded_retry_with_destination_fails_at_registration`
     still expected `"bound"` after the messages moved to "no attempt limit"); it now asserts
     the current wording. Unrelated to INF-2, but the suite was red without it.

---

**[INF-5] `TopologyRegistry._declared` cache never invalidated across reconnect or broker-side deletion — VALID, fixed (2026-11-07)**
- **Severity:** IMPORTANT
- **File:** src\StreamBus\TopologyRegistry.cs (lines 14–15, 83–88, 101)
- **Problem:** After a successful declare, the fingerprint is cached in `_declared` keyed only by stream name. The fast path then returns success without any probe for the rest of the process lifetime — across supervisor reconnects, broker restarts, and streams deleted externally. `EnsureStreamAsync` then reports success while publishing fails with `StreamBusStreamNotFoundException`, and the cache can never recover because nothing clears it.
```csharp
if (_declared.TryGetValue(name, out var known) && known == fingerprint)
    return;
...
    await create(handle, declaration, ct).ConfigureAwait(false);
    _declared[name] = fingerprint;
    return;
```
- **Solution:** Subscribe to the supervisor's `StateChanged` and clear `_declared` whenever the connection leaves `Ready`; the next `Ensure` call re-establishes truth via the normal probe/create/compare path.
```csharp
// constructor
_supervisor.StateChanged += state =>
{
    if (state != BusConnectionState.Ready)
        _declared.Clear();
};
```

- **Verification & resolution (2026-11-07):** Confirmed genuine on every factual point.
  1. `_declared` is written only (`TopologyRegistry.cs` lines 101/117, fast path 87–88) and never
     cleared anywhere in the codebase — no `TryRemove`, no `Clear`, no invalidation hook. The topology
     registry lives for the bus lifetime and survives supervisor reconnects (the supervisor swaps
     handles without notifying it), so a fingerprint cached at startup is trusted forever.
  2. Impact bounded to the public surface: the internal declare loop
     (`DeclareDerivedUntilReadyAsync`, called only from hosted `StartAsync`) runs once at startup, so
     the stale-fast-path matters when `bus.Topology.EnsureStreamAsync` (`IStreamTopology` is public,
     `Bus.cs:96`) is invoked after a broker-side deletion — it returns instantly with false success,
     and worse, the cache *blocks the self-healing* a post-reconnect re-probe would otherwise provide.
     IMPORTANT severity confirmed.
  3. **Applied fix:** `StreamTopologyRegistry` now subscribes to
     `StreamBusConnectionSupervisor.StateChanged` in its constructor (the same pattern
     `Registry.cs` already uses for health) and clears `_declared` whenever the state leaves
     `Ready`, so the next `Ensure` re-establishes truth through the probe/create/compare path.
     A benign residual race remains (a declare completing on the old handle as the connection
     drops may repopulate one stale entry; the next disconnect clears it) — accepted, not worth
     machinery.
  4. Test (TDD — watched RED first, failing exactly at the second-create assert:
     `Assert.Equal(2, CreatedStreams.Count)` got 1, proving the reconnect left the cache intact):
     `TopologyTests.Reconnect_clears_declared_cache_and_recreates_a_broker_deleted_stream`
     drives a real supervisor connection drop (monitor observes a closed handle at a 50 ms
     interval → `TransientFailure` → reconnect → `Ready`), then asserts the next
     `EnsureStreamAsync` re-probes and re-creates the broker-deleted stream. The test harness's
     `ScriptSystem.IsClosed` became settable to script the drop. UnitTests 487/487; the brokered
     integration suite could not run in this environment (no RabbitMQ broker is listening).

---

**[INF-6] Publish channel/connection stored as two separate torn `Interlocked` writes**
- **Severity:** IMPORTANT
- **File:** src\StreamBus\ErrorQueueTransport.cs (lines 226–228, 375–395, 504–518)
- **Problem:** `_publishChannel` and `_publishChannelConnection` are written and exchanged as two independent fields. `ResetPublishChannelAsync` (called from `DisposeAsync` **without** holding `_publishGate`) exchanges the channel first, then the connection; a concurrent `GetPublishChannelAsync` can observe a torn pair — a new channel paired with the *previous* connection, or a channel created on a connection `DisposeAsync` is concurrently disposing. Correctness then relies on downstream `ObjectDisposedException` catches.
```csharp
// ErrorQueueTransport.cs:379-385
private async Task<IErrorQueuePublishChannel> GetPublishChannelAsync(
    IErrorQueueConnection connection, CancellationToken ct)
{
    if (_publishChannel is { IsOpen: true } channel && ReferenceEquals(_publishChannelConnection, connection))
        return channel;
    await ResetPublishChannelAsync().ConfigureAwait(false);
    var created = await connection.CreatePublishChannelAsync(ct).ConfigureAwait(false);
    _publishChannel = created;
    _publishChannelConnection = connection;
    return created;
}
```
- **Solution:** Pair the two in one immutable holder swapped with a single write, and have `DisposeAsync` take `_publishGate` before reset so it cannot interleave with an in-flight publish/channel swap.
```csharp
private sealed record PublishChannelState(IErrorQueuePublishChannel? Channel, IErrorQueueConnection? Connection);
private PublishChannelState? _publishState;

private async Task<IErrorQueuePublishChannel> GetPublishChannelAsync(IErrorQueueConnection connection, CancellationToken ct)
{
    var state = Volatile.Read(ref _publishState);
    if (state is { Channel: { IsOpen: true } ch } && ReferenceEquals(state.Connection, connection))
        return ch;
    await ResetPublishChannelAsync().ConfigureAwait(false);
    var created = await connection.CreatePublishChannelAsync(ct).ConfigureAwait(false);
    Volatile.Write(ref _publishState, new PublishChannelState(created, connection));
    return created;
}
```

---

**[INF-7] `BlockUntilConsumersAttached` treats `rows.Count == 0` as success — startup can pass with zero consumers**
- **Severity:** IMPORTANT
- **File:** src\StreamBus\Hosting.cs (lines 538–561)
- **Problem:** The attach-wait loop polls the health snapshot and breaks when `rows.Count == 0`. Health rows for consumers are only upserted from inside `StreamConsumerRegistry.ConsumeAsync`, which the just-launched start tasks reach seconds later. If the loop snapshots health before any consumer has registered, `StartAsync` completes believing all consumers are attached while none exist — the exact failure the option exists to prevent.
```csharp
var failed = rows.FirstOrDefault(r => r.State is BusRegistrationState.FailedRetrying or BusRegistrationState.Stopped);
if (failed is not null)
    throw new StreamBusStartupBlockedException(...);
if (rows.Count == 0 || rows.All(r => r.State == BusRegistrationState.Attached))
    break;
```
- **Solution:** Compare against the expected registration count derived from `starters`, not zero:
```csharp
int expected = starters.Sum(s => /* per-registration expected partition rows, or 1 */ 1);
...
if (rows.Count >= expected && rows.All(r => r.State == BusRegistrationState.Attached))
    break;
```

---

**[FLT-2] Publish zero/double-`next` misclassified as a configuration exception**
- **Severity:** IMPORTANT
- **File:** src\StreamBus\PublishPipeline.cs (lines 137–139, 151–157)
- **Problem:** A publish filter calling `next()` twice or zero times is a user runtime bug at publish time, but `PublishPipeline` throws `StreamBusConfigurationException` — the public exception whose meaning elsewhere is "the bus was configured wrong at construction". Consume classifies the identical violation as `StreamBusFilterContractException`. Callers catching `StreamBusConfigurationException` to surface setup diagnostics will also intercept publish filter bugs on every publish, hiding them.
```csharp
if (call > 1)
{
    throw new StreamBusConfigurationException(
        $"Publish filter {filterName} invoked next {call} times; exactly one continuation is allowed.");
}
```
```csharp
if (calls == 0)
{
    throw new StreamBusConfigurationException(
        $"Publish filter '{filterName}' returned without calling next; the message would have been silently dropped.");
}
```
- **Solution:** Make `StreamBusFilterContractException` public (or add a public derived type), throw it from `PublishPipeline`, and reserve `StreamBusConfigurationException` strictly for configuration-time failures.
```csharp
// Exceptions.cs
public sealed class StreamBusFilterContractException(string message, Exception? inner = null)
    : StreamBusException(message, inner);

// PublishPipeline.cs — both throw sites
throw new StreamBusFilterContractException(...);
```

---

**[FLT-3] Effectively-once contract violations wedge the partition with no poison guard**
- **Severity:** IMPORTANT
- **File:** src\StreamBus\ConsumePipeline.cs (lines 205–233); src\StreamBus\StreamPartitionConsumer.cs (lines 830–837)
- **Problem:** EO pipelines raise `StreamBusFilterContractException` for zero/double `next`, and the EO retry catch deliberately rethrows it — but the only landing spot is the dispatch safety net (`FailAttach` → close + backoff + reattach → redeliver the same offset). A single double-call bug in a filter turns that partition into an infinite crashloop: no retry budget consumed, no error-stream envelope, and the health surface shows broker flapping rather than a poison delivery.
```csharp
// StreamPartitionConsumer.cs 834-837
catch (StreamBusFilterContractException)
{
    throw;
}
```
- **Solution:** Track contract-violation count for the current offset; on the Nth occurrence (N = 1 or configurable), route the delivery through the same exhaustion machinery used for retry budget exhaustion, with the contract exception on the envelope and an explicit critical log / health event.
```csharp
catch (StreamBusFilterContractException fex)
{
    if (state.ContractViolationsFor(offset) >= MaxContractViolations)
    {
        _logger.LogCritical(fex,
            "Effectively-once filter contract violated {Count} times for offset {Offset}; handing off as poison.",
            count, offset);
        await ExhaustAsync(consumer, offset, consumeContext, count, fex, attach, sendHeaders)
            .ConfigureAwait(false);
        return;
    }
    state.RecordContractViolation(offset);
    throw; // existing safety-net recycle on the first occurrence
}
```

---

**[FLT-4] Consume-side filter caches resolve once from the root provider — captive dependency risk, undocumented singleton contract**
- **Severity:** IMPORTANT
- **File:** src\StreamBus\ConsumerRegistry.cs (lines 91–93, 163–174)
- **Problem:** `BuildPipeline` resolves both universal and typed filters via `GetOrAdd` from `_rootProvider` and caches them for the process lifetime — yet the registration tests register typed filter types as *transient* services. A user who registers a filter (or one of its constructor dependencies) as `Scoped` gets a captive dependency silently resolved at the root; the "transient" registration is effectively a singleton. Contrast the handler path, which correctly resolves `TConsumer` from the per-delivery scope (`context.Services`). The publish side has the same defect (PUB-2) and shares the fix direction.
```csharp
// ConsumerRegistry.cs 163-168
var instance = _typedFilterInstances.GetOrAdd(
    (busType, closedType),
    _ => registration.ResolveTyped(closedType, _rootProvider!));
var filter = _typedFilterAdapters.GetOrAdd(
    (busType, closedType, messageType),
    _ => ConsumeFilterRegistration.CreateTypedAdapter(messageType, instance));

// 172-174
filter = _filterInstances.GetOrAdd(
    (busType, registration),
    _ => registration.CreateUniversal(_rootProvider!));
```
- **Solution:** Pick one contract and enforce it on both sides: (a) resolve filters per delivery from `context.Services` (MassTransit-style, honors user lifetimes), or (b) keep the singleton cache but register filter types as singletons at `AddStreamBus` time and document the contract in the XML docs on `IStreamConsumeFilter`/`IStreamPublishFilter`.
```csharp
// Option (a): resolve per delivery, honoring user lifetime
internal sealed class ScopedConsumeFilter(Type filterType) : IStreamConsumeFilter
{
    public Task OnConsume(ConsumeContext context, ConsumeDelegate next)
    {
        var filter = (IStreamConsumeFilter)context.Services.GetRequiredService(filterType);
        return filter.OnConsume(context, next);
    }
}
```

---

**[FLT-5] PublishPipelineCache grows unbounded for explicit-stream publishes and leaks typed filters across pipes**
- **Severity:** IMPORTANT
- **File:** src\StreamBus\Bus.cs (lines 111–118, 158); src\StreamBus\PublishPipelineCache.cs (lines 52–96)
- **Problem:** `PublishThroughPipelineAsync` keys the pipeline cache on `typeof(T)` of the call site — including the unregistered `PublishAsync<T>(string stream, T message, ...)` overloads. A caller (or a reflection-driven outbox replayer) publishing with many distinct generic instantiations grows `_pipelines`, `_typedInstances`, and `_typed` forever. Worse, `Build(messageType)` eagerly closes every open-generic typed registration against that arbitrary `typeof(T)`, so a filter registered with the declared pipe `OrderEvent` also runs on an explicit `PublishAsync("raw-stream", orderEvent)` call it was never registered for — a semantics leak, not just a memory issue.
```csharp
// Bus.cs 158
var pipeline = _publishPipelines.GetOrCreate(typeof(T));

// PublishPipelineCache.cs 71-74
var closedFilterType = registration.ClosedFilterType(messageType);
if (closedFilterType is null)
    continue;
```
- **Solution:** Distinguish declared pipes from explicit-stream publishes: only consult `GetOrCreate(typeof(T))` for declared pipes; for explicit-stream publishes, use a cached universal-only pipeline (typed filters are a declared-pipe concept by definition).
```csharp
// Bus.cs PublishAsync<T>(string stream, ...) — universal filters only
var pipeline = _publishPipelines.GetOrCreateUniversal();
return PublishThroughPipelineAsync(stream, routingKey: null, message, effective, ct, pipeline);
```
```csharp
// PublishPipelineCache.cs
private readonly PublishPipeline _universalOnly;
public PublishPipeline GetOrCreateUniversal() => _universalOnly ?? BuildUniversalOnly();
// BuildUniversalOnly: include only registrations where !registration.IsTyped
```

---

**[DOC-1] IStreamBus public surface is mostly undocumented; stale Topology doc; CS1591 suppression hides the gap**
- **Severity:** IMPORTANT
- **File:** src\StreamBus\Bus.cs (lines 6–55); src\StreamBus\StreamBus.csproj (lines 21–24)
- **Problem:** Of the 27 public members of `IStreamBus`, only three carry XML docs. The `Topology` doc is also stale: it says "these methods cover explicit/operator-driven declaration anytime," but the interface only exposes the property — the declaration methods were removed by this same changeset. Meanwhile `<NoWarn>$(NoWarn);1591</NoWarn>` suppresses the missing-doc warning, so the build stays green while the package ships an undocumented API reference. Biggest single polish win available.
```text
/// Declares streams and super streams idempotently (FR-7). Startup
/// declarations configured via topology are declared automatically;
/// these methods cover explicit/operator-driven declaration anytime.
/// </summary>
IStreamTopology Topology { get; }
```
- **Solution:** Add `<summary>` (plus `<param>`/`<returns>`/`<exception>` where useful) to every public member of `IStreamBus`, rewrite the `Topology` doc to describe the property actually present, and remove the `NoWarn` 1591 suppression so CI enforces documentation for future public members.
```text
/// <summary>Virtual host this bus connects to.</summary>
string Vhost { get; }
/// <summary>Lifecycle state of the underlying broker connections.</summary>
BusConnectionState State { get; }
/// <summary>Waits until the bus satisfies <paramref name="policy"/> or the token is cancelled.</summary>
Task<BusReadiness> WaitUntilReadyAsync(ReadinessPolicy policy, CancellationToken cancellationToken = default);
```

---

**[DOC-2] Public `CancellationToken` parameters named `ct` violate .NET naming guidelines**
- **Severity:** IMPORTANT
- **File:** src\StreamBus\Bus.cs (lines 10–48, all IStreamBus members)
- **Problem:** .NET guidelines require `CancellationToken` parameters to be named `cancellationToken`, not `ct`. Every public `IStreamBus` signature uses `ct`, which shows up verbatim in IntelliSense and public docs; user code using named arguments (`cancellationToken: token`) written against the current name will break on any later rename. Placement is uniformly last (good); only the name is wrong.
```text
Task<BusReadiness> WaitUntilReadyAsync(ReadinessPolicy policy, CancellationToken ct = default);
Task PublishAsync<T>(T message, PublishOptions? options = null, CancellationToken ct = default);
```
- **Solution:** Rename to `cancellationToken` on every public member. Pre-1.0 breaking rename — batch it with the other 0.2.0/0.3.0 breaking changes already in this PR.
```text
Task<BusReadiness> WaitUntilReadyAsync(ReadinessPolicy policy, CancellationToken cancellationToken = default);
Task PublishAsync<T>(T message, PublishOptions? options = null, CancellationToken cancellationToken = default);
```

---

**[DOC-3] README still advertises the removed "explicit consume skip" feature**
- **Severity:** IMPORTANT
- **File:** README.md (line 98)
- **Problem:** The README feature list still mentions "explicit consume skip," but `AllowPipelineSkip`/`SkipHandler`/`CanSkip` were removed by this same PR — skipping is now implicit (a filter that returns without calling `next()` filters the delivery). Every other doc states the new rule correctly; the README is the one stale claim, and it misleads users evaluating the middleware contract.
```text
- Middleware: ordered publish/consume filters, explicit consume skip, telemetry (`docs/guide/publish.md`, `docs/guide/consume.md`)
```
- **Solution:** Drop "explicit consume skip" from the bullet.
```text
- Middleware: ordered publish/consume filters, telemetry (`docs/guide/publish.md`, `docs/guide/consume.md`)
```

---

**[DOC-5] No SourceLink — snupkg contains untraceable PDBs**
- **Severity:** IMPORTANT
- **File:** src\StreamBus\StreamBus.csproj (lines 18–24)
- **Problem:** The project emits a symbol package and publishes the repository URL, but has no `Microsoft.SourceLink.GitHub` reference and no `EmbedUntrackedSources`/`DeterministicSourcePaths`. The PDBs ship without source-path mapping or commit hash, so debugger source-indexing never works for consumers and there is no tamper-evident link between the binary and the GitHub commit.
```text
<IncludeSymbols>true</IncludeSymbols>
<SymbolPackageFormat>snupkg</SymbolPackageFormat>
<PublishRepositoryUrl>true</PublishRepositoryUrl>
```
- **Solution:** Add the SourceLink package reference and the determinism properties; verify the snupkg PDB contains GitHub source-link JSON.
```text
<PropertyGroup>
  <EmbedUntrackedSources>true</EmbedUntrackedSources>
  <DeterministicSourcePaths>true</DeterministicSourcePaths>
</PropertyGroup>
<ItemGroup>
  <PackageReference Include="Microsoft.SourceLink.GitHub" PrivateAssets="All" />
</ItemGroup>
```

---

**[DOC-7] `StreamConfigurationConflictException` breaks the `StreamBus*` prefix convention**
- **Severity:** IMPORTANT
- **File:** src\StreamBus\Exceptions.cs (lines 46–59); src\StreamBus\DeclareSet.cs (lines 145–195, seven throw sites)
- **Problem:** Every other public exception is prefixed `StreamBus*` (`StreamBusException`, `StreamBusFatalException`, `StreamBusConfigurationException`, `StreamBusStartupBlockedException`); ADR-5's stated purpose for the base type is "catch the whole library surface with one type." `StreamConfigurationConflictException` is the lone outlier — the kind of naming drift this review set out to catch. Renaming before release is cheap; after publish it becomes API-breaking.
```text
public sealed class StreamConfigurationConflictException : StreamBusException
{
    public StreamConfigurationConflictException(string stream, string expected, string actual)
        : base($"Stream '{stream}' already exists with a conflicting spec ...")
```
- **Solution:** Rename the class, constructor, and all seven throw sites to `StreamBusStreamConfigurationConflictException` (or merge under `StreamBusConfigurationException` with a `Conflict` property). Update `docs/guide/topology.md` references.

---

**[DOC-8] Duplicate test class names across suites and missing `*IntegrationTests` suffix**
- **Severity:** IMPORTANT
- **File:** tests\StreamBus.UnitTests\ConsumerDeathTests.cs (line 77) vs tests\StreamBus.IntegrationTests\ConsumerDeathTests.cs (line 10); tests\StreamBus.IntegrationTests\BrokerRestartTests.cs (line 11)
- **Problem:** `ConsumerDeathTests` is declared in both assemblies; CI dashboards and retries that key on the class name collide, making failures ambiguous. Meanwhile every other integration class follows the `*IntegrationTests` suffix — `BrokerRestartTests` breaks the convention (ADR-1 and ADR-3 also cite the old names).
```text
// UnitTests\ConsumerDeathTests.cs:77
public sealed class ConsumerDeathTests
// IntegrationTests\ConsumerDeathTests.cs:10
public sealed class ConsumerDeathTests : IAsyncDisposable
// IntegrationTests\BrokerRestartTests.cs:11
public sealed class BrokerRestartTests : IAsyncDisposable
```
- **Solution:** Rename the integration classes (and files) to `ConsumerDeathIntegrationTests` and `BrokerRestartIntegrationTests`; update the ADR references.

---

**[DOC-12] `ConsumeContext`/`PublishContext` are unsealed public classes with no pin on construction**
- **Severity:** IMPORTANT
- **File:** src\StreamBus\ConsumeContext.cs (lines 7–77); src\StreamBus\PublishPipeline.cs (lines 40–75)
- **Problem:** Both types have an implicit public parameterless constructor, so any user can subclass and hand instances to the bus or place user-derived contexts in pipelines. The library is not designed for this: user-crafted contexts can't round-trip through the framework, and the internal `ResetPayloads()` hook would never be honored by foreign instances — logic that appears to work but silently loses per-delivery payload reset.
```csharp
// ConsumeContext.cs:7
public class ConsumeContext
{
    internal void ResetPayloads() => _payloads.Clear();  // line 76
```
- **Solution:** Declare an explicit `protected internal` constructor on both classes so construction is pinned to library code (tests still work via `InternalsVisibleTo`), and add an XML doc stating that filters receive and enrich but never construct contexts.
```csharp
public class ConsumeContext
{
    protected internal ConsumeContext() { }
    ...
}
```

---

# 🟡 MINOR

---

**[PUB-6] `ProducerSlot.Failed` is a write-only field**
- **Severity:** MINOR
- **File:** src\StreamBus\ProducerRegistry.cs (line 17; written at 168, 185, 195)
- **Problem:** `Failed` is assigned in three places and never read for any decision. Readers of the lock section must reason about a third state that has no effect, obscuring the slot state machine.
```csharp
private sealed class ProducerSlot
{
    public readonly object Gate = new();
    public TaskCompletionSource<IStreamProducerHandle>? Pending;
    public TaskCompletionSource<IStreamProducerHandle>? Success;
    public bool Failed;
}
```
- **Solution:** Delete the field and its three assignments. Failure recovery already works: the catch block clears `Pending`, so the next owner retries creation.
```csharp
private sealed class ProducerSlot
{
    public readonly object Gate = new();
    public TaskCompletionSource<IStreamProducerHandle>? Pending;
    public TaskCompletionSource<IStreamProducerHandle>? Success;
}
```

---

**[PUB-7] `PublishResultAsync` on the super handle interface has no callers**
- **Severity:** MINOR
- **File:** src\StreamBus\ProducerHandles.cs (declaration line 31; implementation 284–289)
- **Problem:** After this PR, `ResultProducerRegistry` routes result publishes through `PublishWithIdAsync`/`PublishReplayAsync` directly, so `IStreamSuperProducerHandle.PublishResultAsync` has no internal callers — a leftover pre-PR interface shape on a *public* interface, where every member becomes a permanent compatibility commitment.
```csharp
public Task PublishResultAsync(ReadOnlyMemory<byte> body, ulong publishingId, string? routingKey, CancellationToken ct)
    => PublishWithIdAsync(
        publishingId,
        body,
        new PublishOptions { IsSuperStream = true, RoutingKey = routingKey },
        ct);
```
- **Solution:** Remove `PublishResultAsync` from the interface and delete the implementation (remove before the package ships; if already shipped, `[Obsolete]` for one release then remove).

---

**[PUB-8] `PublishContext<T>.Message` re-casts `Payload` on every access**
- **Severity:** MINOR
- **File:** src\StreamBus\PublishPipeline.cs (lines 82–86)
- **Problem:** `Message` performs `(T)Payload` on each property access. Filters commonly read `context.Message` multiple times, and the cast is an unboxing operation for value types paid on every access — although the context is immutable.
```csharp
public sealed class PublishContext<T> : PublishContext
{
    /// <summary>The published payload as <typeparamref name="T"/>.</summary>
    public T Message => (T)Payload;
}
```
- **Solution:** Capture the typed value once at construction — stash the typed reference via a private field assigned by the only construction site (Bus.cs ~line 170), e.g. `internal T TypedPayload { get; init; }` with `public T Message => TypedPayload;`.

---

**[PUB-9] `PublishPipelineCache` keeps two parallel typed-filter caches**
- **Severity:** MINOR
- **File:** src\StreamBus\PublishPipelineCache.cs (lines 10–12 and 76–82)
- **Problem:** `Build` looks up the resolved filter instance in `_typedInstances` (keyed `Type`) and then a second dictionary `_typed` (keyed `(Type, Type)`) to cache the cheap adapter wrapper. Two lookups, tuple allocations per miss, and two dictionaries that grow in lockstep.
```csharp
private readonly ConcurrentDictionary<Type, object> _typedInstances = new();
private readonly ConcurrentDictionary<(Type ClosedFilterType, Type MessageType), IStreamPublishFilter> _typed = new();
```
- **Solution:** Delete `_typed` and wrap the instance directly — `Build` already runs exactly once per message type because `_pipelines` is the outer cache.
```csharp
var instance = _typedInstances.GetOrAdd(
    closedFilterType,
    type => registration.ResolveTyped(type, _rootProvider!));

filter = PublishFilterRegistration.CreateTypedAdapter(messageType, instance);
```

---

**[PUB-10] `GetJson<T>` only catches `JsonException`; custom serializers escape uncaught**
- **Severity:** MINOR
- **File:** src\StreamBus\Headers.cs (lines 103–113)
- **Problem:** `IStreamSerializer` is a public extension point (custom protobuf/other serializers), yet only the built-in `JsonException` is handled — any other deserializer's parse exception propagates raw out of a "read a header" API, and the friendly `ArgumentException` path is never taken.
```csharp
try
{
    return Serializer.Deserialize<T>(Encoding.UTF8.GetBytes(json));
}
catch (System.Text.Json.JsonException ex)
{
    throw new ArgumentException(
        $"Header '{key}' is not valid JSON for {typeof(T).FullName}.",
        nameof(key),
        ex);
}
```
- **Solution:** Catch the broad deserialization surface of the pluggable serializer (plain broad catch is acceptable for a synchronous deserialize).
```csharp
try
{
    return Serializer.Deserialize<T>(Encoding.UTF8.GetBytes(json));
}
catch (Exception ex)
{
    throw new ArgumentException(
        $"Header '{key}' could not be deserialized to {typeof(T).FullName} using {Serializer.GetType().Name}.",
        nameof(key),
        ex);
}
```

---

**[PUB-11] `RecordException` omits the standard `exception.stacktrace` tag**
- **Severity:** MINOR
- **File:** src\StreamBus\StreamBusActivity.cs (lines 48–57)
- **Problem:** The exception event writes `exception.type` and `exception.message` only. OpenTelemetry semantic conventions expect `exception.stacktrace`; without it, exporters and dashboards show the failure type and message but not the failure site.
```csharp
activity.AddEvent(new ActivityEvent("exception", tags: new ActivityTagsCollection
{
    ["exception.type"] = exception.GetType().FullName,
    ["exception.message"] = exception.Message,
}));
```
- **Solution:**
```csharp
activity.AddEvent(new ActivityEvent("exception", tags: new ActivityTagsCollection
{
    ["exception.type"] = exception.GetType().FullName,
    ["exception.message"] = exception.Message,
    ["exception.stacktrace"] = exception.ToString(),
}));
```

---

**[PUB-12] Duplicate-filter check in `Build` is near-unreachable and runs lazily**
- **Severity:** MINOR
- **File:** src\StreamBus\PublishPipelineCache.cs (lines 85–90)
- **Problem:** `StreamBusPublishStore.AddFilter` already rejects duplicate registrations, so the `seen` check inside `Build` guards a nearly-impossible state — and because `Build` runs lazily from `GetOrCreate`, any conflicting registration that slips past surfaces at the *first publish* of that message type rather than at configuration time. Confusing, delayed, per-type failure.
```csharp
var filterType = FilterIdentity.TypeOf(filter);
if (!seen.Add(filterType))
{
    throw new StreamBusConfigurationException(
        $"Publish filter '{filterType}' is already registered on this bus; duplicate registration would run it twice.");
}
```
- **Solution:** Move validation to seal time: in the `PublishPipelineCache` constructor, eagerly `GetOrAdd` every registered typed message type so config errors surface at build time; `Build` becomes a pure assembly step.

---

**[CON-8] Sync-over-async `DisposeAsync().AsTask().GetAwaiter().GetResult()` in `ReplayAsync` producer disposal**
- **Severity:** MINOR
- **File:** src\StreamBus\ConsumerRegistry.cs (lines 571–585)
- **Problem:** Replay producers are disposed with a blocking `.GetAwaiter().GetResult()` inside a `finally` of an async method — a thread-pool blocking call that adds no value where `await` is available.
```csharp
void DisposeProducers()
{
    foreach (var (_, producer) in producers)
    {
        try
        {
            producer.DisposeAsync().AsTask().GetAwaiter().GetResult();
        }
```
- **Solution:** Convert to `async Task DisposeProducersAsync()` and `await producer.DisposeAsync()` in a try/catch, then await it in the outer `finally`.

---

**[CON-9] Replay's 5 s idle termination silently truncates a stalled backlog and is unconfigurable**
- **Severity:** MINOR
- **File:** src\StreamBus\ConsumerRegistry.cs (lines 645–654)
- **Problem:** Replay terminates on "5 s with no delivery and nothing in flight" because no usable broker end offset exists. A broker stall longer than the hardcoded 5 s (throttled stream, network hiccup) makes `ReplayAsync` return a short count with no error — silently incomplete replay treated as success.
```csharp
// No usable broker end offset exists (Phase 5 Round 2), so replay
// terminates on idle: 5 s with no delivery and nothing in flight.
var idle = TimeSpan.FromSeconds(5);
```
- **Solution:** Expose the idle window (internal constant at minimum, ideally a `ReplayOptions.IdleTimeout`), and log a warning on idle termination stating the replay may be incomplete for slow trickle backlogs.

---

**[CON-10] `ConsumePipeline` tolerates fire-and-forget `next()` — unawaited continuation escapes the exactly-once counter**
- **Severity:** MINOR
- **File:** src\StreamBus\ConsumePipeline.cs (lines 198–233)
- **Problem:** A filter that calls `next()` but doesn't await it increments the call counter (so the zero-call check passes) and the pipeline returns while the continuation is still running. The offset then advances concurrently with a live handler, and a faulted continuation goes unobserved.
```csharp
var nextCalls = 0;
Task Next()
{
    var call = Interlocked.Increment(ref nextCalls);
    if (call > 1) { ... throw ... }
    return InvokeAsync(index + 1, context, terminal);
}

await filter.OnConsume(context, Next).ConfigureAwait(false);
```
- **Solution:** Have `Next()` record the returned task; after `OnConsume` returns with `calls == 1`, throw if the continuation hasn't completed. Also document "await `next` exactly once".
```csharp
Task? nextTask = null;
Task Next()
{
    ...
    nextTask = InvokeAsync(index + 1, context, terminal);
    return nextTask;
}
await filter.OnConsume(context, Next).ConfigureAwait(false);
var calls = Volatile.Read(ref nextCalls);
if (calls == 1 && nextTask is { IsCompleted: false })
    throw new StreamBusFilterContractException(
        $"Consume filter '{filterName}' returned before its continuation completed; await next().");
```

---

**[CON-12] `EmptyServiceProvider` is misplaced in `ConsumePipeline.cs` and surprises raw `GetService` callers**
- **Severity:** MINOR
- **File:** src\StreamBus\ConsumePipeline.cs (lines 280–285)
- **Problem:** A generic infrastructure type is defined at the bottom of the pipeline file, making it undiscoverable. It returns `null` for every service type — permitted, but filter authors calling raw `context.Services.GetService` get silent nulls on untyped/no-scope endpoints instead of the standard required-service exception.
```csharp
internal sealed class EmptyServiceProvider : IServiceProvider
{
    public static readonly EmptyServiceProvider Instance = new();

    public object? GetService(Type serviceType) => null;
}
```
- **Solution:** Move it to its own small infrastructure file (e.g., next to `Registry.cs`), and guide `GetRequiredService` usage in `ConsumeContext.Services` docs.

---

**[CON-14] `CreateScope()` used instead of `CreateAsyncScope()` for per-message scopes**
- **Severity:** MINOR
- **File:** src\StreamBus\StreamPartitionConsumer.cs (lines 502–505)
- **Problem:** The dispatch path creates per-delivery service scopes synchronously with `CreateScope()` and disposes via sync `Dispose`. `CreateAsyncScope()` exists precisely so scoped services implementing `IAsyncDisposable` (EF `DbContext` etc.) are disposed asynchronously; the sync path forces sync disposal of async resources on the dispatch thread.
```csharp
using var messageScope =
    (_pipeline.Count > 0 || _options.MessageType is not null) && _scopes is not null
        ? _scopes.CreateScope()
        : null;
```
- **Solution:**
```csharp
await using var messageScope =
    (_pipeline.Count > 0 || _options.MessageType is not null) && _scopes is not null
        ? _scopes.CreateAsyncScope()
        : null;
```

---

**[INF-8] `StopAsync` disposes the startup CTS that `StartAsync` and its continuations still reference**
- **Severity:** MINOR
- **File:** src\StreamBus\Hosting.cs (lines 384–390, 404/431/451, 499–511, 762–772)
- **Problem:** `StopAsync` nulls and disposes `_startupCts` while `StartAsync` may still be mid-flight. The `ContinueWith` `.Cancel()` calls guard `ObjectDisposedException`, but the plain `startupCts.Token` dereferences when launching tasks do not — reading `.Token` on a disposed CTS throws uncaught if `StopAsync` is invoked before `StartAsync` completes, which the `IHostedService` contract permits.
```csharp
lock (_gate)
{
    startupCts = _startupCts;
    _startupCts = null;
}
if (startupCts is not null)
{
    try { startupCts.Cancel(); }
    finally { startupCts.Dispose(); }   // disposed while StartAsync still references it
}
```
- **Solution:** Don't dispose the CTS in `StopAsync` — cancel and null the field only; ownership of disposal stays with `StartAsync` after its launched tasks complete.

---

**[INF-9] Dead-letter-queue cohort logic duplicated in `Hosting` while `CollectQueueDeclarations` is dead code**
- **Severity:** MINOR
- **File:** src\StreamBus\Hosting.cs (lines 422–428); src\StreamBus\ErrorQueueTransport.cs (lines 270–278)
- **Problem:** `StartAsync` re-implements the "which queue dead-letters need declaring" filter, while `CollectQueueDeclarations` — written for precisely this — is referenced nowhere and has already drifted structurally (it drops the vhost grouping `Hosting` needs). Two paths that must agree will diverge on the next change.
```csharp
// Hosting.cs:422-427
var queuesByVhost = starters
    .Where(s => s.Reg.Options.DeadLetter is not null && s.Reg.Options.ExhaustionCallback is null)
    .GroupBy(s => s.Vhost, StringComparer.Ordinal)
    .ToDictionary(...);

// ErrorQueueTransport.cs:271-277 — unused twin
internal static IReadOnlyList<DeadLetterOptions> CollectQueueDeclarations(...)
```
- **Solution:** Keep one owner: have `Hosting` group starters by vhost and call `CollectQueueDeclarations(groupMembers)`; delete the inline filter.

---

**[INF-10] `StreamBusCredentialValidator` is dead public API**
- **Severity:** MINOR
- **File:** src\StreamBus\CredentialValidation.cs (lines 1–11)
- **Problem:** The whole file is a public static wrapper around `options.ValidateCredentials()`, called nowhere. It contradicts `StreamBusOptions.Validate`'s documented contract ("the single validation boundary") and invites users to call a partial validation entry point.
```csharp
public static class StreamBusCredentialValidator
{
    public static void Validate(StreamBusOptions options)
    {
        ArgumentNullException.ThrowIfNull(options);
        foreach (var error in options.ValidateCredentials())
            throw new StreamBusConfigurationException(error);
    }
}
```
- **Solution:** Delete the file before it ships in the package and becomes an obligation (the supervisor/connector paths already call `options.Validate()`).

---

**[INF-11] Health-check data dictionary can throw on a key collision between bus-marker and registration names**
- **Severity:** MINOR
- **File:** src\StreamBus\Hosting.cs (lines 798–813)
- **Problem:** `CheckHealthAsync` builds `data` from two key namespaces sharing the format `$"{x}|{y}"`, and the row loop uses an indexer add which throws `ArgumentException` on duplicate keys. A free-form registration name colliding with a bus marker makes the health check itself throw — surfacing as a failed check with a stack trace instead of real health data.
```csharp
var data = states.ToDictionary(
    r => $"{r.Vhost}|{r.BusType}",
    r => (object)r.State.ToString());
foreach (var row in rows)
    data[$"{row.Vhost}|{row.Registration}"] = $"...";
```
- **Solution:** Prefix the two namespaces: `"bus:{r.Vhost}|{r.BusType}"` and `"reg:{row.Vhost}|{row.Registration}"`.

---

**[INF-12] `declareExhausted` derived from `FailedRetrying` — one enum state, two meanings**
- **Severity:** MINOR
- **File:** src\StreamBus\HealthAggregator.cs (lines 5, 51–60)
- **Problem:** `BusRegistrationState.FailedRetrying` means both "temporarily failing, retrying" (consumers) and "declaration in a fatal-conflict state that never recovers" (declares). `Compute()` names the flag `declareExhausted`, but nothing ever exhausts — declares retry with `int.MaxValue`. Overloaded states make health semantics hard to audit.
```csharp
bool declareExhausted = declares.Any(d => d.State == BusRegistrationState.FailedRetrying);
...
if (connection == BusConnectionState.Fatal || declareExhausted || allConsumersFailed)
    return HealthStatus.Unhealthy;
```
- **Solution:** Rename the local to `anyDeclareFailed`; longer term split the enum into `Failed` (terminal) vs `Retrying` so `Compute()` maps them without `All()` heuristics.

---

**[INF-13] `CreateRawConsumerAsync` blind cast to `RawConsumer`**
- **Severity:** MINOR
- **File:** src\StreamBus\Connector.cs (lines 155–165)
- **Problem:** The raw client implementation is returned with an unguarded `(RawConsumer)consumer`. If a future RabbitMQ.Stream.Client version returns a different concrete type, users get an opaque `InvalidCastException` from deep inside the partition attach loop, far from the cause.
```csharp
var consumer = await _system.CreateRawConsumer(config, _factory.CreateLogger<RawConsumer>()).ConfigureAwait(false);
ct.ThrowIfCancellationRequested();
return (RawConsumer)consumer;
```
- **Solution:** Guard with a type pattern and fail fast with an actionable message:
```csharp
return consumer as RawConsumer
    ?? throw new StreamBusConfigurationException(
        $"The stream client returned '{consumer.GetType().FullName}' where a RawConsumer was required (client/library version mismatch).");
```

---

**[INF-14] `AmqpErrorQueueConnection.DisposeAsync` falls back to synchronous `Dispose()`**
- **Severity:** MINOR
- **File:** src\StreamBus\ErrorQueueTransport.cs (lines 103–107)
- **Problem:** The class is part of an `IAsyncDisposable` seam whose purpose is clean async lifecycle, but disposal delegates to the synchronous `IConnection.Dispose()`, bypassing the graceful close the client v7 offers.
```csharp
public ValueTask DisposeAsync()
{
    _connection.Dispose();
    return ValueTask.CompletedTask;
}
```
- **Solution:**
```csharp
public async ValueTask DisposeAsync()
    => await _connection.DisposeAsync().ConfigureAwait(false);
```

---

**[INF-15] Metric `streambus_partition_lag` doesn't measure lag**
- **Severity:** MINOR
- **File:** src\StreamBus\StreamBusMetrics.cs (lines 27–31)
- **Problem:** The counter named `streambus_partition_lag` publishes only the consumer-local `delivered - stored` backlog; "lag" has an industry-standard meaning (broker last offset − consumer offset). Users polling the metrics surface will misread it and make capacity decisions on the wrong number.
```csharp
_ = _meter.CreateObservableUpDownCounter("streambus_partition_lag",
    () => ObservePartitions(s => Math.Max(0, s.LastDelivered - s.LastStored)),
    description: "Consumer-held unprocessed backlog max(0, delivered - stored), not broker-total backlog.");
```
- **Solution:** Rename to `streambus_partition_backlog` (ship with the 0.2.0/0.3.0 breaking release that already rewrites the metrics surface).

---

**[INF-16] `timeouts` single-element list built only to call `Max()`**
- **Severity:** MINOR
- **File:** src\StreamBus\Hosting.cs (lines 470–474)
- **Problem:** A `List<TimeSpan>` is allocated, given at most one element, then reduced with `.Max()` to feed `CancelAfter` — maximal generality for a one-case problem that obscures the actual rule.
```csharp
var timeouts = new List<TimeSpan>();
if (_hosting is { BlockStartupUntilReady: true, StartupTimeout: not null })
    timeouts.Add(_hosting.StartupTimeout!.Value);
using var cts = CancellationTokenSource.CreateLinkedTokenSource(ct);
if (timeouts.Count > 0) cts.CancelAfter(timeouts.Max());
```
- **Solution:**
```csharp
using var cts = CancellationTokenSource.CreateLinkedTokenSource(ct);
if (_hosting is { BlockStartupUntilReady: true, StartupTimeout: { } startupTimeout })
    cts.CancelAfter(startupTimeout);
```

---

**[INF-17] `LogSendHeaderCollisions` allocates the envelope header dictionary before the log-enabled check**
- **Severity:** MINOR
- **File:** src\StreamBus\ErrorPublisher.cs (lines 45–59)
- **Problem:** The method allocates a 13-entry dictionary and prefixes each key on every error send — a hot path in the partition dispatch loop — regardless of whether any Warning sink is attached.
```csharp
private void LogSendHeaderCollisions(ErrorEnvelope envelope)
{
    if (envelope.SendHeaders is null)
        return;
    var envelopeKeys = envelope.EnvelopeApplicationProperties();
```
- **Solution:**
```csharp
private void LogSendHeaderCollisions(ErrorEnvelope envelope)
{
    if (envelope.SendHeaders is null || !_logger.IsEnabled(LogLevel.Warning))
        return;
    ...
}
```

---

**[FLT-6] Duplicated retry knobs: `UseMessageRetry` vs `UseRetry`**
- **Severity:** MINOR
- **File:** src\StreamBus\Configuration\StreamBusConfigurator.cs (lines 243–255 and 451–462)
- **Problem:** The same policy is configured through two differently-named members duplicated at both bus scope and endpoint scope. Users can't tell which is canonical, IntelliSense shows four near-identical retry members, and docs must always explain the pair.
```csharp
public void UseMessageRetry(Action<IRetryConfigurator> configure)
{
    ArgumentNullException.ThrowIfNull(configure);
    var retry = new RetryConfigurator();
    configure(retry);
    _globalRetry = ((IRetryConfigurator)retry).BuildPolicy();
}

public void UseRetry(RetryPolicy policy)
{
    ArgumentNullException.ThrowIfNull(policy);
    _globalRetry = policy;
}
```
- **Solution:** Keep one verb with two overloads; mark `UseMessageRetry` `[Obsolete("Use UseRetry")]` for one release then remove.

---

**[FLT-7] Factory/instance filter registrations escape duplicate-type detection**
- **Severity:** MINOR
- **File:** src\StreamBus\FilterRegistrationSupport.cs (lines 22–28); src\StreamBus\ConsumerRegistry.cs (lines 135–139, 176–181); src\StreamBus\PublishPipelineCache.cs (lines 84–90)
- **Problem:** Configuration-time duplicate guards key on `FilterType`, but factory registrations have `FilterType == null`, so the same universal filter registered twice through two factories (a common dev-config mistake) passes silently and is caught only later — with a different error message per site.
```csharp
var type = filter.FilterType;
if (type is not null && _consumeFilters.Any(existing => existing.FilterType == type))
{
    throw new StreamBusConfigurationException("...");
}
```
- **Solution:** At seal/bus-build time, resolve factory and instance registrations once and dedupe by `FilterIdentity.TypeOf(resolvedFilter)` with one canonical error message; the ad-hoc `seen` checks in `Build`/`BuildPipeline` then become redundant.

---

**[FLT-8] `Interlocked`/`Volatile` on `nextCalls` pays for a guarantee the code doesn't deliver**
- **Severity:** MINOR
- **File:** src\StreamBus\ConsumePipeline.cs (lines 197–211); src\StreamBus\PublishPipeline.cs (lines 131–139)
- **Problem:** `nextCalls` is a closure local consumed only by `Next()` and the post-await check, yet uses `Interlocked.Increment`/`Volatile.Read` — machinery that only matters if a filter calls `next()` concurrently from another thread, a case the pipeline doesn't actually guarantee anything for (the unawaited-next contract hole is CON-10). The atomics add cost and an implicit thread-safety suggestion without delivering one.
```csharp
var nextCalls = 0;
Task Next()
{
    var call = Interlocked.Increment(ref nextCalls);
    if (call > 1) { ... throw ... }
    return InvokeAsync(index + 1, context, terminal);
}
```
- **Solution:** Once CON-10 pins the "next must complete within the filter's returned task" contract, concurrent invocation is impossible — simplify to plain integer arithmetic with the same post-return checks, mirrored in `PublishPipeline`.

---

**[FLT-9] Typed endpoints deserialize eagerly even when a filter short-circuits**
- **Severity:** MINOR
- **File:** src\StreamBus\StreamPartitionConsumer.cs (lines 620–632)
- **Problem:** On a typed endpoint the message is deserialized *before* the pipeline runs, so every delivery — including ones a universal filter drops via zero-`next`, and every retry attempt — pays full deserialization even when neither a typed filter nor the handler touches `Message`. Pinned by tests ("Typed_zero_next_still_deserializes_but_skips_handler"), so it's contractual, not accidental.
```csharp
private async Task InvokeHandlerPipelineAsync(ConsumeContext consumeContext)
{
    if (_options.MessageType is { } messageType)
    {
        var message = TypedMessageDeserializer.Deserialize(_serializer, messageType, consumeContext);
        consumeContext = TypedConsumeContext.Create(messageType, consumeContext, message);
    }
    ...
}
```
- **Solution:** Make `ConsumeContext<T>.Message` lazy: construct the typed context with a deserialize-once `Lazy<T>` built from `Body`, so the payload decodes only on first read. Deserialization failures surface at first materialization — still inside the pipeline, so existing retry/exhaustion classification applies unchanged.

---

**[DOC-4] Directory.Build.props CA1711 suppression rationale references now-internal `DeclaredStream`**
- **Severity:** MINOR
- **File:** Directory.Build.props (lines 20–23)
- **Problem:** The comment justifying the `CA1711` suppression claims `DeclaredStream` is public API, but it's now `internal` (`DeclareSet.cs:10`) — CA1711 only targets externally visible types, so the suppression (and its rationale) is dead documentation.
```text
CA1711: DeclaredStream is public API and models
a declaration, not System.IO.Stream. ... -->
<NoWarn>$(NoWarn);CA1001;CA1068;CA1711;CA1716;CA1848;CA1873</NoWarn>
```
- **Solution:** Remove `CA1711` from the `NoWarn` list, delete its rationale sentence, and build to confirm no warning appears.

---

**[DOC-6] ActivitySource version "2.0.0" contradicts package version 0.2.0**
- **Severity:** MINOR
- **File:** src\StreamBus\StreamBusActivity.cs (line 7); docs\guide\observability.md (line 50)
- **Problem:** The `ActivitySource` is constructed with version "2.0.0" while `VersionPrefix` is 0.2.0 and ADR-6 commits to 0.x versioning. Telemetry tooling filtering on source version sees a number matching no released package, implying a stability the ADR explicitly rejected.
```csharp
internal static readonly ActivitySource Source = new("StreamBus", "2.0.0");
```
- **Solution:** Prefer generating the version from the MSBuild `$(Version)` property so it can never drift; at minimum hardcode "0.2.0" and update `observability.md:50` to match.

---

**[DOC-9] Duplicated header-bag test assertions across HeaderCarrierTests and HeadersTests**
- **Severity:** MINOR
- **File:** tests\StreamBus.UnitTests\HeaderCarrierTests.cs (lines 106–134, 136–146) vs HeadersTests.cs (lines 7–30, 47–55)
- **Problem:** `Header_accessors_read_the_legal_value_set` and `GetJson_invalid_json_names_the_key_and_keeps_the_json_exception` are copy-pasted verbatim in both files, testing the same `StreamHeaders` type with no distinct coverage — duplicated maintenance cost.
- **Solution:** Keep exactly one legal-value-set test and one invalid-JSON test in `HeadersTests.cs`; keep only the serializer-usage assertion in `HeaderCarrierTests` as a narrow test (e.g., `GetJson_deserializes_via_the_context_serializer`).

---

**[DOC-10] Hand-rolled `IStreamSystemHandle` fakes copy-pasted across ~15 unit test files (18 private declarations)**
- **Severity:** MINOR
- **File:** tests\StreamBus.UnitTests\AcceptanceTests.cs (line 87), ActivityTests.cs (line 204), ConsumerDeathTests.cs (line 44), EffectivelyOnceTests.cs (line 155), HostingTests.cs (lines 15, 57), PublishRoutingTests.cs (lines 31, 220), and 9 more files
- **Problem:** Each file contains a private hand-rolled implementation of the fourteen-member interface, almost all `Task.CompletedTask`/`NotImplementedException` boilerplate except one or two scripted behaviors. Duplicated, unreviewed code silently rots each time the interface grows — several hundred duplicated lines.
- **Solution:** Create one shared `Fakes\RecordingSystemHandle.cs` with virtual no-op members and recording lists; express per-test behavior by subclassing and overriding one member. Replace the 18 private duplicates.

---

**[DOC-11] Options.cs validation message typo "Host host must be supplied."**
- **Severity:** MINOR
- **File:** src\StreamBus\Options.cs (lines 47–48)
- **Problem:** The validation error for a missing host reads like a copy-paste slip and is surfaced verbatim to configuring users.
```text
if (string.IsNullOrWhiteSpace(Host))
    yield return "Host host must be supplied.";
```
- **Solution:** `yield return "Host must be supplied.";` and update any tests comparing the text.

---

**[DOC-13] `StreamMessageProperties` is public with an all-internal contract undocumented**
- **Severity:** MINOR
- **File:** src\StreamBus\MessageProperties.cs (lines 3–19)
- **Problem:** The public record carries no XML docs at all, while its behavior — reserved-key rejection, value-type limits (`string|bool|int|long`), AMQP property mapping — is entirely internal. Users constructing `PublishOptions.Properties` see a bare data shape with zero explanation of constraints that throw at publish time.
```csharp
public sealed record StreamMessageProperties
{
    public Guid? MessageId { get; init; }
    public Guid? CorrelationId { get; init; }
    public DateTime? SentTime { get; init; }
    internal string? ContentType { get; init; }
    public IReadOnlyDictionary<string, object?> Headers { get; init; } = new Dictionary<string, object?>();
```
- **Solution:** Add a `<summary>` explaining the AMQP mapping, value restrictions, and reserved-key rejection; document each public property; link from `docs/guide/publish.md`.
