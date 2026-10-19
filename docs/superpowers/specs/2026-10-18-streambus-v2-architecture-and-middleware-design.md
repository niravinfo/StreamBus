# StreamBus v2 — Architecture Remediation & Publish/Consume Middleware Design

- Status: Draft — pending human approval
- Date: 2026-10-18
- Source of findings: architecture review 2026-10-18 (four-track: publish path, config/hosting, consumer core, tests/docs). Verdict: sound invariants, unfinished liveness, drift-marked surface. Not AI slop; production-grade in the steady state, incomplete at failure boundaries.
- Scope: StreamBus library (`src/StreamBus`), its tests, examples, docs. .NET 10, `RabbitMQ.Stream.Client` 1.12.1, `RabbitMQ.Client` 7.2.2.

## 1. Why now

Three findings make v2 necessary rather than optional:

1. **Liveness is not closed-loop.** The library's stated contract ("we own reconnect") fails silently on the most common production events: connection death while idle and lost publish confirms. The supervisor stops after the first successful connect (`ConnectionSupervisor.cs` — `RunLoopAsync` returns at first `Ready`), consumer death produces no callback because `RawConsumerConfig` wires neither `ConnectionClosedHandler` nor `MetadataHandler`, `IStreamSystemHandle.IsClosed` is implemented and referenced by nothing, the error-handoff confirm wait is unbounded while holding the partition `_dispatchGate`, and the health check reads only supervisor state — so the system can go green-and-dead.

2. **Every queued feature (middleware) lands on the same seams the fixes create.** There is no header carrier on publish today (`PublishOptions` has no properties), no middleware position on either path, and the publish facade is a ~170-line copy-paste twin (`StreamBus` vs `StreamBus<TBus>` in `Bus.cs`). Building middleware on that surface first would double the remediation cost later.

3. **Drift debt will rot**: duplicated retry skeletons, 4–5 fold validation, orphaned builders, one-value unread enum, stringly-typed keys, plaintext password in a public property, a README flagship snippet that does not compile.

## 2. Goals, non-goals, anti-goals

### Goals
- G1 — Liveness closed loop: every failure mode that can idle the library is detected within seconds and surfaces through state, metrics, logs, and the health check.
- G2 — The supervisor genuinely supervises; the health check tells the truth (connection + consumers + declarations).
- G3 — One source of truth per concern: one bus core class, one retry-loop skeleton, one validation boundary, one naming utility.
- G4 — Minimum safe public surface: no public mutable registration stores, no credential leakage, no orphaned builders/enums, no "internal error" escapes.
- G5 — Publish/consume middleware with header carriers (tenant, user, correlation, telemetry context), MassTransit-familiar shape, stream-native semantics.
- G6 — IConfiguration-section binding as documented in the README; validation at one boundary.
- G7 — CI: unit job always-green, integration job with a real RabbitMQ 4 broker (streams), docs that compile, analyzers on.
- G8 — Preserve exactly the current hard invariants (see §4) while changing everything else.

### Non-goals (v2)
- Transport abstraction beyond RabbitMQ (no multi-broker SPI).
- Publish-side message substitution or publish short-circuiting (ADR-2) — middleware is decoration on publish.
- Batch consume, scheduling features, new orchestrators.
- Backward source compatibility for removed dead API — this is a 0.2.0 release with a CHANGELOG of breaks (ADR-6).
- Re-implementing the stream protocol or replacing RabbitMQ.Stream.Client.

### Anti-goals (failure states the design must make impossible or noisy)
- Green-but-dead: healthy status while consumers idle-stall.
- Silent dedup loss: producer name collisions that make the broker discard republishes invisibly.
- Middleware that can reorder, bypass, or veto offset advance.
- Header loss through the error envelope on exhaustion.
- Public code paths that answer with "internal error".

## 3. Constraints

- Client realities (Stream.Client 1.12.1): connection death surfaces only through per-entity callbacks (`ConnectionClosedHandler`, `MetadataHandler`) and send-time exceptions; the system connection exposes no closed event — supervision must poll our own `IStreamSystemHandle.IsClosed` seam; pending confirmations for a dropped producer are **never invoked** — the producer's `ConnectionClosedHandler` is the only reliable signal.

- Confirms: a broker NAK is not fatal; a connection drop before confirm is "outcome unknown" → transient.

- The dispatch gate serializes per-partition handler execution; middleware must compose inside it, never around it.

- `TreatWarningsAsErrors` stays on; net10.0; central package management stays.

- Existing acceptance gates and IDs (FR-x, G-x) remain traceable; new work adds v2 gate IDs (V2-A.x, V2-B.x, V2-C.x, V2-D.x).

## 4. Invariants to preserve verbatim (regression points)

1. Strict per-partition handler ordering; at most one in-flight handler per partition.
2. Offset advances only after: successful handler (cadence rules), or **confirmed** error-destination publish (never before), with the monotonic-window guard rejecting regression.
3. Never skip before hand-off confirms; exhaustion is confirmed-then-commit.
4. Effectively-once: publishing id = source offset; resume = target's last confirmed publishing id recomputed each attach; source never stores offsets; `QuerySequence == 0` ambiguity resolves to replay-from-first under producer-dedup.
5. Unique process-wide producer names (broker dedup trap); replay/EO names stable per (source-partition → target).
6. SAC promotion listener never throws (client fallback for a throwing listener = scroll forward = skipped backlog).
7. `FirstAttach` semantics: `ConsumeAsync` returns only after the broker has confirmed the subscription.

## 5. Target architecture (end state)

```
                     ┌──────────────────────────────────────────────┐
                     │  StreamBusHostedService (internal, factory)  │
                     │  Start/Stop orchestration, blocking startup  │
                     └──────┬───────────────────────────────────────┘
        configurator-time   │ runtime
   ┌───────────┐            ▼
   │ AddStream │     ┌──────────────┐    IsClosed poll + reconnect loop
   │ Bus (...) │────►│ Connection-  │◄──────────────┐
   └───────────┘     │ Supervisor   │───────────────┘ (per system handle)
        (validated   └──────┬───────┘   state events → StateChanged
         once)              │ handles (current)
                            ▼
              ┌──────────────────────────────────┐
              │ StreamBusCore<TBus>  (single     │  publish/  consume facades
              │ generic core — no twins)         │  route table (per marker)
              └──────┬───────────────────────────┘
       publish path  │                            consume path
          ┌──────────▼────────────┐   ┌───────────────────▼──────────────┐
          │ PublishPipeline       │   │ StreamPartitionConsumer (split:  │
          │ (ordered filters)     │   │  AttachLoop / Dispatch+Pipeline  │
          │  → serialize →        │   │  / OffsetStore / Handoff)        │
          │ Message+properties →  │   │   ├ ConsumePipeline (filters)    │
          │ ProducerRegistry →    │   │   ├ RetryPolicy (in-gate)        │
          │ Handles(Confirmation  │   │   └ Exhaustion → ErrorPublisher/ │
          │ Tracker + CancelAll)  │   │     ErrorQueueTransport/Callback │
          └───────────────────────┘   └──────────────────────────────────┘
   Topology/DeclareSet (idempotent, cached per marker)          ErrorEnvelope
   HealthAggregator (connection + consumers + declares + retry-block) → IHealthCheck
   Observability: StreamBusMetrics (existing), ActivitySource "StreamBus" (new), log events
```

Component responsibilities (delta only; everything else unchanged):

- **ConnectionSupervisor**: connect at start; **continues as a monitor loop after `Ready`**; detects closed system handle at `MonitorInterval`; flips state via `StateChanged` events; jittered reconnect with the existing backoff function.

- **StreamBusCore\<TBus\>**: the single generic implementation under `IStreamBus`/`IStreamBus<TBus>`; owns route resolution, pipeline execution, and the per-marker declaration cache.

- **PublishPipeline / ConsumePipeline**: ordered user filters composing around serialize/send and around the handler call respectively (detailed in §8).

- **ConfirmationTracker**: gains producer `ConnectionClosedHandler → CancelAll()` semantics ("connection closed before confirm" → transient).

- **HealthAggregator**: new internal type per bus; aggregates supervisor state, per-registration attach status (last attempt, attach age, failure reason), declaration status, retry-block gauges; consumed by `IHealthCheck` and `WaitUntilReadyAsync` consumers-readiness expansion.

- **StreamBusNaming**: the single shared producer-name/hash utility (publish references, replay names, EO names, 255-byte rule pinned by test).

## 6. Phase A — Correctness & liveness (do first)

Ordered; each item lands with its acceptance evidence.

### A1 — Consumer liveness signals (highest priority)
- Wire `RawConsumerConfig.ConnectionClosedHandler` and `MetadataHandler` in the partition consumer's config assembly → route to `FailAttach(reason)` with a distinguishable reason so the attach loop treats it as transient, closes the consumer, and recreates per existing backoff.
- Add an `IsClosed` check as a cheap guard in the attach loop's park/tick positions (belt-and-braces with callbacks; the callback path is primary).
- No semantic change to offsets: a closed-connection attach fail must behave exactly like the existing dispatch-exception attach fail (redeliver from stored offset; dedup holds; never skip).
- **Acceptance (V2-A1)**: unit — a scripted consumer handle raises closed → attach fails → RecreateWithBackoff loop recreates; duplicate settlement stays idempotent (`TrySet` semantics). Integration — broker-side forced subscription close (management API or restart) while idle → partition reattaches ≤ monitor interval + backoff, resumes exactly from the stored offset, no gap/skip.

### A2 — Supervisor really supervises
- `RunLoopAsync` after first `Ready` continues into a monitor loop: tick at `MonitorInterval` (default 5s, configurable); on `handle.IsClosed` → `TransientFailure` → reconnect with the existing `ComputeDelay` sequence; on success state events fire and readiness waiters wake.
- `CurrentHandle` becomes a small atomic swap. Recovery ownership per ADR-1: attach loops (partition consumers, effectively-once resume providers) re-resolve the current handle from the supervisor on EVERY attach attempt — they must never cache a system handle across attempts; producer handles keep the existing failure-driven invalidation (no cache wipe on swap); EO resume queries additionally re-resolve per attach. A stale-captured handle is a defect: add an assertion-style guard in the attach loop.
- **Acceptance (V2-A2)**: unit — fake handle flips `IsClosed` → state sequence `Ready → TransientFailure → Ready`, `CurrentHandle` swaps; no reconnect storm (backoff preserved). Integration — broker restart → each bus transitions through transient failure ≤ interval + backoff; health check turns degraded/unhealthy first, recovered within one interval after broker returns.

### A3 — Confirm-path hardening
- Producer `ConnectionClosedHandler → ConfirmationTracker.CancelAll()`; awaiters receive `StreamBusTransientException("connection closed before confirm; outcome unknown")`; registry invalidates the handle (existing path).
- `PublishEnvelopeAsync` (error hand-off publish) gets a bounded confirm wait (default 10s, configurable per endpoint as `ErrorHandoffTimeout`) — removal of the hang-in-dispatch-gate.
- `PublishResultAsync` **keeps no confirm-timeout by design (FR-6.5 doctrine)**, but is now bounded in practice by connection-closed cancellation — document that.
- **Acceptance (V2-A3)**: unit — close-callback mid-wait resolves all pending awaiters transiently; NAK unaffected; hand-off with a dead transport unwedges `DispatchAsync` (hang regression test); existing "outcome unknown" timeout tests remain green.

### A4 — Unwedging dispatch on hand-off hang
Satisfied by A3 (single-wait bounded). No additional watchdog device.

### A5 — Producer registry & handle lifecycle
- Replace `GetOrAdd`-factory race with per-key task cache (double-checked `TaskCompletionSource` keyed slot); losers never orphan a producer.
- Handles gain `IAsyncDisposable`; registry disposes on host stop via the supervisor chain; `DisposeAsync` performs the synchronous client close on the background with a ~3s cap, rethrowing dispose exceptions to a logged sink rather than swallowing samples.
- **Acceptance (V2-A5)**: unit — concurrent first-publish same key produces one producer (await both, diff references); invalidate-during-flight keeps the existing "healthy waiter not evicted" test green.

### A6 — Health tells the truth
- `HealthAggregator` (internal per bus): connection state; per registration: `Attached | Attaching | Failed(retries) | Stopped` + last transition; declaration status per declare-set part; error-transport liveness. Aggregation is per vhost (the layer every registry coordinate space already shares), exposed per (vhost, busType) view to health/readiness. Ad-hoc raw consumers created via `bus.ConsumeAsync` are tracked as first-class registrations named `raw:{stream}` (repeat calls disambiguate with a counter: `raw:{stream}:2`); they participate in degraded/unhealthy mapping identically to typed registrations.
- `IHealthCheck` mapping: all attached + connection Ready → Healthy; any failed-and-retrying or declared-fail → Degraded; fatal connection or every attached-failed → Unhealthy (poison).
- `AddStreamBusHosting` readiness blocking includes consumer attach in blocking mode (configurable flag, default: block until attached).
- **Acceptance (V2-A6)**: unit — forced consumer attach failure → degraded body contains registration name and failure reason; integration — idle broker kill → unhealthy ≤ interval+2s; recovery → healthy ≤ interval+2s.

### A7 — Shutdown & startup classification
- `StopAsync` cancel during a blocking startup wait rethrows `OperationCanceledException` (not `StreamBusTimeoutException` with "Timed out within ." text).
- Consumer-disposal and supervisor-disposal errors in stop are logged, not swallowed; disposed supervisors become removable from the registry (ownership made explicit).
- **Acceptance (V2-A7)**: unit — cancel during blocking start rethrows OCE; text assertion (no "Timed out within"); no double-stop exceptions.

### A8 — ErrorQueueTransport round-trips
- One channel reused across bursts of dead-letter publishes (the per-envelope channel cycle on the dead-letter CLASSIC queue publish path; stream-side republish already flows through the producer path and opens no channels, so it is out of scope). Declare stays at startup cadence; `ConfirmTimeout` surfaces as `ErrorQueueOptions.ConfirmTimeout` (single timeout concept documented next to publish timeout).
- Disposal race on `_gate` closed; fatal auth latch stays.
- **Acceptance (V2-A8)**: existing drain/requeue integration tests pass; new unit — a burst of 2 000 dead-lettered envelopes to a classic queue publishes through one channel (spy transport) with return/shutdown mid-burst leaving remainders requeued.

## 7. Phase B — Surface & consolidation

### B1 — Bus twins collapse
`StreamBus` + `StreamBus<TBus>` → single `StreamBusCore<TBus>`; `DefaultBus` marker unchanged; public interfaces unchanged. All publish methods, options normalization (`with { IsSuperStream.. }`), and error text preserved.

### B2 — One retry-loop skeleton
`StreamBusRuntimeLoop.RetryUntilAsync<T>(Func<CancellationToken, Task<T>>, classify, backoff, ct)` reused by hosting (declare, error queue, consumer start) and supervisor connect loop. Cancellation/conflict/fatal classes in one place.

### B3 — Single validation boundary
Records validate themselves (`Options.cs`, `StreamOptions.cs` `Validate()`); configurators delegate to build/Validate; the supervisor and connector stop re-implementing range checks; message text compatibility asserted by existing tests.

### B4 — StreamBusNaming consolidation
One producer-naming/hash utility replaces the three independent recipes; 255-byte limit pinned by unit (existing EffectivelyOnceTests format tests extended).

### B5 — Handle-interface split
`IStreamProducerHandle` two-transport surface split into plain (`SendAsync`, `ReplayAsync`) and super (`PublishWithIdAsync`, `PublishResultAsync`, `QuerySequenceAsync`); no more silent-ignored arguments or throwing-unreachable methods; super-stream routing table becomes per-handle state (static `ConditionalWeakTable` removed; "No routing value was stamped" internal-error path disappears).

### B6 — State hygiene
Registration stores: `Add` internal, read projections public; `Registrations` → `StreamBusRegistrationView` without `Password` (host/ports/vhost only). `StreamBusHostedService` becomes internal with required (non-null) deps via an internal factory; the eight-param-with-nulls ctor disappears.

### B7 — Dead surface removal
Delete `StreamBusConnectionOptionsBuilder`, `ConsumeOptionsBuilder`, `ConsumeOrdering`, `ConsumeOptions.Ordering`; rename `PublishRouting.cs` → `PublishRegistration.cs`. Update all docs referencing them.

### B8 — IConfiguration binding (README honesty)
New overload `AddStreamBus(IConfigurationSection section, Action<IStreamRegistrationContext, IStreamBusConfigurator>)` plus a string section-name overload: binds `StreamBusConnectionOptions` (vhost/credentials/host/ports/heartbeat) with single validation boundary (B3); precedence code-over-config is documented in one paragraph + tiny tests; section optional (lambda-only still works).

Precedence is per-field: `Host(...)` becomes optional when the section supplies a host; every explicitly-set configurator field individually wins over the section value; validation requires a complete connection from the union (so a section with host but a configurator with credentials is legal and expected).

### B9 — Typed registry keys
`(busType, vhost)` key replaces the `FullName@vhost` string split (a vhost containing `@` corrupting `DescribeStates`).

### B10 — Exception hierarchy
New common base `StreamBusException : Exception`; all nine sealed exception types derive (the set includes `StreamBusDeserializationException`, which is the ninth); users can now `catch (StreamBusException)`. Public docs table: type → meaning → retryability.

### B11 — Declaration cache per marker
Per-publish declaration lookup (currently rebuilds a merge set) becomes a cached map built at bus creation (configuration is Add-time immutable) — removes the per-publish LINQ merge.

### B12 — TopologyRegistry generics merge + Source enum
`EnsureStreamAsync`/`EnsureSuperStreamAsync` and their compare twins merged via a kind parameter; `DeclaredStream.Source` becomes an enum with a `Description` projection (stringly-typed `"endpoint"|"publish"|"dead-letter"` literals removed).

### B13 — Parallel multi-supervisor startup
Per-vhost supervisors start via `Task.WhenAll` (conflict cancellation semantics preserved — tests exist).

### B14 — Replay naming & termination
`ReplayNaming.ProducerName` scopes per source stream (current cross-source collision silently discards via broker dedup when targeting a mixed error stream); idle-termination documented as idempotent (re-running never loses messages, count is advisory).

### B15 — Raw path guardrails
`ConsumeAsync` (raw) validates Strict+handoff-destination combos (reject with message naming the two options at play).

### B16 — Analyzer discipline
`<AnalysisLevel>latest-recommended` + curated rules in `Directory.Build.props`; final state: 0 warnings incl. analyzers; spurious rules suppressed with one-line reasons.

### B17 — Cross-process caveat doc
`SingleActiveConsumer=false` + shared reference across processes → duplicate delivery (never skip) caveat documented in the consume guide.

## 8. Phase C — Publish/consume middleware & headers

### C1 — Properties carrier (prerequisite)
- `PublishOptions.Properties` (new record `StreamMessageProperties`): `MessageId`, `CorrelationId`, `Headers` (IReadOnlyDictionary<string, object?>) → rendered to client `ApplicationProperties` on the stream publish path.
- Consume side already exposes via `MessageMetadata` — typed accessors `GetString/GetInt/GetJson<T>` added there.
- Legal header value type-set (v2): `string`, `bool`, `int`, `long` (the AMQP-native set Stream.Client round-trips); unsupported types throw at write time naming the type and key — no JSON-string coercion. `GetJson<T>` deserializes using the bus's configured `IStreamSerializer`. Confirm during M5 acceptance that application properties survive the super-stream publish path end-to-end.
- **Acceptance (V2-C1)**: unit — properties serialize roundtrip; integration — publish with headers → consume side reads identical values on plain, super, and composite streams.

### C2 — Publish pipeline
Shape (ADR-2): ordered, decoration-only; resolution of stream/routing happens *before* filters; filters may add/remove headers and observe; exception classification = publish failure (transient vs fatal per existing classification); **no short-circuit/body substitution** — compile-time guarantees (routing-key-required, misregistration errors) remain intact.
```
interface IStreamPublishFilter {                      // MT-familiar, stream-native
    Task OnPublish(StreamPublishFilterContext context, PublishDelegate next, CancellationToken ct);
}
delegate Task PublishDelegate(StreamPublishFilterContext context);
class StreamPublishFilterContext {
    string Stream { get; }                 // resolved
    string? RoutingKey { get; }            // super only (independent guarantee)
    object Message { get; }                // before serialization
    IStreamHeaders ImmutableView { get; }  // read
    IStreamHeaderWriter Headers            // write → Properties
    IServiceProvider Services { get; }
}
```
Registration: bus-level `cfg.UsePublishFilter(...)` (order of registration), endpoint(s)-level is not a concept (publish is per-message). No per-call filter list exists in v2; call-level enrichment uses the Properties carrier directly. Filters are DI-constructible and constructed ONCE per bus from the root provider (stateless/thread-safe contract; per-invocation state travels the context object). Each invocation exposes a per-publish `IServiceProvider` scope on the context for scoped dependencies.
The publish pipeline runs on the typed facade only (`PublishAsync`/`PublishSuperAsync`). `PublishRawAsync`, effectively-once result publishes, error-envelope hand-off publishes, requeue, and replay republishes do NOT run publish filters — documented as the pipeline-free escape hatch. Order: bus filters (registration order) → Properties (call-level) wins on conflict (documented).
**Acceptance (V2-C2)**: unit — enrichment landed in client-publish properties; exception classification; super-stream routing key untouched by filters; integration — headers visible at the consumer.

### C3 — Consume pipeline
Position: inside the dispatch block, wrapping the whole per-message OUTER delegate — deserialization, typed consumer resolution, and handler invocation. The pipeline's per-message DI scope is the same scope that resolves the typed consumer instance (a tenant filter and the consumer must see one shared tenant key). Consequences: skip also skips deserialization cost; deserialization exceptions flow through the pipeline as ordinary downstream exceptions and classify for retry/exhaustion exactly as they do today; for effectively-once pipelines the outer delegate is the compute step.
```
interface IStreamConsumeFilter {
    Task OnConsume(StreamConsumeFilterContext context, ConsumeDelegate next, CancellationToken ct);
}
delegate Task ConsumeDelegate(StreamConsumeFilterContext context);
class StreamConsumeFilterContext {
    ConsumeContext Message { get; }              // existing record; Metadata access full
    IStreamHeaderWriter    OutputHeaders        // forwarded on exhaustion hand-off
    T? GetPayload<T>(); void SetPayload<T>(T)    // cross-filter composition (tenant scope etc.)
    IServiceProvider Services { get; }           // per-message DI scope
    bool SkipHandler { get; set; }   // (see skip semantics)
}
```
Semantics (pinned):
- Filter exceptions classify exactly like handler exceptions: retry apply → exhaustion on budget exhaustion; the `OutputHeaders` at that moment land on the error envelope and publish as message properties at the destination.
- **Skip**: `SkipHandler = true` marks the message processed-by-pipeline — handler does not run; offset advances under the same cadence rules; no destination/publish; in Effectively-Once mode, skip is **rejected at configuration time** (an EO pipeline whose filter skips would emit no result and never advance the target id-space — replayable forever). Raw ConsumeAsync supports skip; typed endpoint consumers allow it only when explicitly enabled (`e.AllowPipelineSkip()`), failing fast otherwise.
- Capability mechanics: a filter declares skip capability via a `bool CanSkip { get; } = false` member on `IStreamConsumeFilter`. Effectively-once configurations reject any attached filter where `CanSkip == true` at configuration time (exception names the filter type). Typed endpoints additionally require `e.AllowPipelineSkip()` when any attached filter can skip; raw consume accepts skip unconditionally.
- Ordering: filters run in registration order (bus then endpoint), both outside-in for before-section and inside-out for after-section (nested composition); exact ordering pinned by a sequence-recording unit test.
- DI: filters constructed per attach (stable for the attach's life; scoped captures via `IServiceScopeFactory` for per-message needs).
- **Acceptance (V2-C3)**: unit — before/after ordering, exception→retry, skip→advance-without-destination, OutputHeaders landed in envelope; integration — tenant filter E2E (extract header → scoped service keyed by tenant), exhaustion envelope carries tenant header, skip advances exactly once across dispose/cancel redelivery windows.

### C4 — Shipped helpers + telemetry
- `CorrelationIdPublishFilter` (reuse incoming or generate; flows through consume `MessageMetadata.CorrelationId`); `TenantHeadersPublishFilter` + `TenantConsumeFilter` (payload slot for downstream scope) as reference samples in `examples/` (not part of the product assembly v1).
- Native telemetry: `ActivitySource("StreamBus")` spans on publish (stream/size/status) and consume (stream/partition/offset, link on correlation). Always-on no-cost when nothing samples. Users extract tenant/user/telemetry values to *propagate* via filters, not via ambient implicit header magic.
- **Acceptance (V2-C4)**: MeterListener/ActivityListener-based unit tests pin tag sets; observability guide updated.

### C5 — Effectively-once integration
- Filters wrap EO compute; `OutputHeaders` of success land on the result publish (properties on the result message).
- Skip rejected (C3); naming/id-space unchanged; resume semantics untouched.
- **Acceptance (V2-C5)**: EO integration test with a header-annotating pipeline: resume replays keep dedup; headers present in targets.

### C6 — Observability of pipelines
- New instruments: `bus_pipeline_duration` histogram (kind=publish|consume), `bus_pipeline_reject`? — pin names before implementation (metric docs as the existing 7-instrument contract).
- **Acceptance (V2-C6)**: MeterListener unit test; guide updated.

## 9. Phase D — Validation, docs, release

- D1 — CI (platform pending answer: GitHub Actions / Azure DevOps / none): unit + analyzers per-push; integration per-push with RabbitMQ 4 service (streams enabled, 5552 + management 15672, dedicated user); skip-lane env var for broker-free local runs; retention of existing gate discipline.
- D2 — Broker chaos integration: broker restart mid-test (supervisor/monitoring/health assertions), forced subscription close (A1), metadata-event close (A1) — the three production failure modes now have repro harnesses.
- D3 — Docs: README rewrite (snippets compile; 90% case: IConfiguration section B8 + blocking hosting); `llms.txt` regenerated from code-ground truths (guest rule correction, removed/dead API removals, new pipeline sections); guides updated per feature; CHANGELOG.md with break list; version → 0.2.0.
- D4 — Acceptance traceability matrix: every review finding → design item → acceptance ID → test file.

## 10. Milestones & PR sequence (gated like v1: G-11…)

| Milestone | Items | Gate |
|---|---|---|
| M1 Liveness mechanics | A1, A3, A4 (close callbacks, bounded handoff, unwedged dispatch) | V2-G1: no silent stall under scripted client-close; hang regression tests green |
| M2 Supervision & truth | A2, A5, A6, A7, A8 | V2-G2: broker-restart integration shows resume ≤ interval+backoff; health/degraded/unhealthy matrix |
| M3 Surface consolidation | B1–B7, B9–B12, B14–B17 | V2-G3: LOC delta negative; all existing behavior tests green unchanged (except text-of-error messages asserted by tests)|
| M4 Config & docs honesty | B8, D3 (README/llms.txt/CHANGELOG/version) | V2-G4: README snippet compiles; llms facts = code facts |
| M5 Properties + publish pipeline | C1, C2, C6(publish side) | V2-G5: headers E2E on all stream kinds |
| M6 Consume pipeline + helpers + EO | C3, C4, C5, C6 | V2-G6: semantics matrix green incl. EO reject-skip and skip-exactly-once |
| M7 CI & chaos | D1, D2 | V2-G7: CI green on clean clone |

Note: M1 before M3 deliberately — liveness first, then refactor on a correctly-behaving base; middleware (M5–6) rides the consolidated seams.

## 11. ADR summary (full text in `docs/adr/` landed per milestone)

- **ADR-1 Liveness closed loop**: per-entity callbacks + supervisor IsClosed polling; supervisor does not own per-producer/per-consumer recovery (they own theirs). Alternatives: client-agnostic watchdog process (too coarse), Reliable* wrappers (defeated the design).
- **ADR-2 Middleware shape**: decoration-only publish / decorate+explicit-skip consume; no publish substitution; familiar naming but stream-native semantics. Alternatives: full MassTransit IPipe (invites publish short-circuit and delegated verification against compile-time routing guarantees); lean-observe-only (cannot skip — blocks a legit drop policy).
- **ADR-3 Health aggregation**: new internal HealthAggregator per bus; health check delegates wholly to it. Alternative: extending `DescribeStates` (conflates connection-state with consumer-attach severity).
- **ADR-4 Config precedence**: code wins; config provides connection-level values; code owns everything else; loud doc + tests pin it. Alternative: config-wins (silent lambdas would break users' reasoning).
- **ADR-5 Exception hierarchy**: common base added, leaf types unchanged. Alternative: interface marker (`IStreamBusError`) — no pattern-matching benefit, semantically worse.
- **ADR-6 Versioning**: 0.2.0; breaks listed in CHANGELOG; no compat shims. Alternative: 1.0.0 GA already (breaks worse there).

## 12. Open items (need human answers before final plan)

1. **Pipeline shape — adopted**: decoration-only publish (no short-circuit/veto) + consume-with-skip (capability-gated). ADR-2 stands. Reopening trigger: if a tenant-gating/outbox-suppression use case appears, reopen ADR-2 with veto semantics.
2. **CI platform — adopted**: GitHub Actions with a `rabbitmq:4-management` service container (streams plugin enabled via mounted config); targeted at port 5552 + management 15672. The workflow is written to be swappable to Azure DevOps if the repo host changes.
3. **Breaking-change tolerance — adopted**: 0.2.0 with a complete break list in CHANGELOG.md; no compat shims; no known internal consumers pin the current surface.
4. **Defaults — adopted**: ActivitySource ("StreamBus") always-on-if-listened; supervisor `MonitorInterval` 5s; in-box helpers limited to correlation/tenant reference samples in `examples/` (no product-assembly filters in v2).

> Defaults were adopted 2026-10-19 (question round was skipped by the design owner). Each is veto-able during plan review without reopening the architecture.
