# Changelog

## 0.2.0

### Breaking

- Publish-filter contract violations (calling `next()` zero times or more
  than once) now throw the newly public
  `StreamBusFilterContractException` instead of
  `StreamBusConfigurationException`, which is now reserved strictly for
  configuration-time failures. `StreamBusFilterContractException` derives
  from `StreamBusException`, so callers that catch the base type are
  unaffected; callers that distinguished setup errors by catching
  `StreamBusConfigurationException` will no longer intercept publish-filter
  bugs on every publish. The effectively-once consume pipeline already threw
  this exception internally; both surfaces now share the public type.

- `IStreamSuperProducerHandle` no longer derives from
  `IStreamPlainProducerHandle`; both now derive from `IStreamProducerHandle`.
  `SendEnvelopeAsync` — which carries no routing key and therefore crashed
  every super-stream call with `KeyNotFoundException` from the client's
  `Routing` callback mid-send — is now only on the plain surface; misuse is a
  compile error instead of a runtime failure. `PublishReplayAsync` remains
  available on the super handle (declared directly) alongside
  `PublishWithIdAsync` and `PublishResultAsync`.

- Consume filters now receive one shared `ConsumeContext` per delivery
  (`StreamConsumeFilterContext*` → `ConsumeContext`, typed filters and the
  consumer use `ConsumeContext<T>`); publish filters receive one shared
  `PublishContext` (`StreamPublishFilterContext*` → `PublishContext`, typed
  filters use `PublishContext<T>`). The wrapper contexts and `context.Inner`
  are removed, and universal and typed filters see the same instance.
  `PublishDelegate` and `ConsumeDelegate` stay parameterless; filters call
  `next()`.
- `ConsumeContext` exposes `Stream`, `Partition`, `Offset`, `RoutingValue`,
  `Body`, `MessageId`, `CorrelationId`, `SentTime`, incoming `Headers`,
  outgoing `SendHeaders`, `Services`, and `CancellationToken`. `Raw`,
  `Metadata`, `OutputHeaders`, `MessageType`, and `RuntimeMessageType` are
  removed.
- `PublishContext` exposes `Stream`, `RoutingKey`, `Payload` (`Message` on
  `PublishContext<T>`), `MessageId`, `CorrelationId`, `SentTime`, `Headers`,
  `Services`, and `CancellationToken`. `context.Bus` and `OutputHeaders` are
  removed.
- `MessageMetadata` is removed. Consume ids are `Guid?` (`MessageId`,
  `CorrelationId`), `SentTime` is a nullable `DateTime`, and `Headers` /
  `SendHeaders` expose the application properties. `ExhaustedContext.Metadata`
  is likewise replaced by flat `MessageId`, `CorrelationId`, `SentTime`, and
  `Headers` members.
- `IConsumer<T>.Consume` and both filter interfaces drop the
  `CancellationToken` parameter; the token is on the context.
- `IStreamHeaderWriter` is replaced by `IStreamSendHeaders`. `Remove` accepts
  any non-blank key (removing a framework key is a no-op) and takes
  precedence over values supplied at the call site.
- `ErrorEnvelope.MessageId` / `CorrelationId` are `Guid?`, and the envelope
  carries `SentTime`.
- Removed `CanSkip`, `SkipHandler`, and `AllowPipelineSkip`. A normal or raw
  consume filter now filters a delivery by returning without calling `next()`;
  the handler is skipped and the offset advances. More than one `next()`
  remains a contract error, and effectively-once pipelines require exactly one
  call (zero is a runtime contract error).
- Removed `RetryPolicy.FailureMode`; retry strictness is derived from
  `MaxAttempts` (`null` is unbounded). An unbounded policy cannot target an
  error stream, dead-letter queue, or exhaustion callback, and a bounded
  policy must configure one.
- There is one exhaustion callback shape. `ConsumeOptions.OnRetryExhausted` /
  `ErrorHandoff` (the v1 observe-only hook that only fired for error-stream
  hand-off) and the internal `ExhaustionCallback` duplicate are removed;
  `ConsumeOptions.OnExhausted(Func<ExhaustedContext, CancellationToken, Task>)`
  is public and is the same callback the endpoint configurator exposes. It owns
  the outcome (returning advances past the message, throwing recycles the
  attach), and on the raw `ConsumeOptions` path `ErrorStream` and `OnExhausted`
  are mutually exclusive — configuring both is a `StreamBusConfigurationException`.
  Endpoints keep resolving the ambiguity by shadowing the declared destination
  with a startup warning.
- Consolidated `PublishResultOptions` retry knobs into a single
  `RetryPolicy Retry`.
- Removed the unused `ErrorStreamOptions.RoutingValue` and
  `ErrorEnvelope.ErrorRoutingValue` surface.
- Implementation plumbing is internal (`StreamBusConfigStore`, registration
  stores and descriptors, the publish router, the registry, connection
  supervisor, system connector/handle implementations, and the error-queue
  transport). Configure through the public builders and accessors.
- Registration-store `Add` methods are internal; use the public `AddStreamBus`
  / `ReceiveEndpoint` configurators. Public read accessors remain.
- `StreamBusConfigStore.Registrations` no longer exposes passwords. It returns
  credential-free `StreamBusRegistrationView` projections.
- Removed the unwired `StreamBusConnectionOptionsBuilder`,
  `ConsumeOptionsBuilder`, `ConsumeOrdering`, and `ConsumeOptions.Ordering`
  surface. Configure consumers through `ConsumeOptions` and endpoints through
  `IReceiveEndpointConfigurator`.
- `StreamBusHostedService` is internal; host it through dependency injection.
- `AddStreamBusHosting` now exposes `BlockUntilConsumersAttached`. Blocking
  startup (`BlockStartupUntilReady`) waits for typed consumer attaches by
  default; call `BlockUntilConsumersAttached(false)` to keep the old
  connections-and-topology-only behavior.
- Typed endpoints now deserialize before any filter runs on every delivery
  attempt, whether or not a typed filter is attached, and share one message
  instance with the typed consumer and typed filters. A payload that fails
  deserialization is retried and exhausted without running filters or the
  consumer; a universal filter can no longer quarantine it first.
- `UsePublishFilter<TFilter>()`, `UseConsumeFilter<TFilter>()`, and their
  `Type` overloads now carry `[RequiresUnreferencedCode]` /
  `[RequiresDynamicCode]`. Call sites that treat trim/AOT analyzer warnings as
  errors must suppress them or switch to the new
  `UsePublishFilter<TFilter, TMessage>()` / `UseConsumeFilter<TFilter, TMessage>()`
  overloads.
- `IStreamBusConfigurator` and `IReceiveEndpointConfigurator` gain the
  two-type-parameter `UsePublishFilter<TFilter, TMessage>()` /
  `UseConsumeFilter<TFilter, TMessage>()` members. Third-party implementations
  of these interfaces must add them.
- Endpoint-scoped concrete typed consume filters that do not match the
  endpoint message type now fail during `AddStreamBus` instead of being
  skipped.
- `UsePublishFilter<TFilter>()` / `UseConsumeFilter<TFilter>()` generic
  constraints relax to `class`, and open generic filter registration via the
  `Type` overloads is now valid.
- StreamBus writes `content-type`, `creation-time`, and `message-id` (when
  supplied or generated) to the AMQP properties section.
- Super-stream publishes write `streambus.routing-key` as an application
  property, so every partition consumer observes the routing key.
- `IStreamSerializer` gains a `ContentType` member, so third-party
  implementations must supply one. A null or blank content type is rejected
  when the bus is constructed.
- Publish filter types are now registered as singletons (previously
  transient). A filter instance is resolved once when the bus is built and
  cached for the bus lifetime; a scoped or transient registration of a publish
  filter type is rejected at bus construction instead of silently becoming a
  captive dependency. Resolve scoped services from `PublishContext.Services`.
- Exceptions thrown by publish filters (including foreign-token
  `OperationCanceledException`s) now surface unwrapped instead of being
  wrapped in `StreamBusPublishTransientException`, so callers no longer
  retry a deterministically broken filter — this also covers deterministic
  failures inside the pipeline terminal such as serializer errors, which
  now surface raw. Genuine transport failures are still classified as
  transient by the producer layer, and all publish failures — including the
  zero-filter fast path — are now recorded as exception events on the
  `streambus.publish` activity.
- `ExhaustedContext.GetService<T>()` now delegates to
  `GetRequiredService<T>()`, so a missing registration throws
  `InvalidOperationException` instead of `StreamBusConfigurationException`.
  The callback runs after the bus started; a DI miss is a runtime wiring
  problem, not invalid bus configuration (which is what the configuration
  exception is documented to mean). Callers that caught
  `StreamBusConfigurationException` around this call must catch
  `InvalidOperationException` instead.

### Added

- Configurable connection monitor interval: `h.MonitorInterval(...)` (default
  5s, positive and at most 60s) (A2 / V2-A2).
- A6 health aggregation: the `streambus` check reports per-registration
  consumer and declaration state, including failure reason and last
  transition time, plus per-bus connection state (A6 / V2-A6).
- Bounded error hand-off confirmation: `ErrorHandoffTimeout` for stream
  destinations and `ErrorQueueOptions.ConfirmTimeout` for queue destinations
  (both default 10s) (A3/A4 / V2-A3).
- Message properties/header carrier for publish and consume flows (C1 /
  V2-C1): `IStreamHeaders` reads application properties and
  `IStreamSendHeaders` writes them, with reserved-key guards that reject
  `streambus.message-id`, `streambus.correlation-id`, `streambus.sent-time`,
  and `streambus.routing-key` from user headers.
- Ordered publish and consume pipelines: `UsePublishFilter` /
  `UseConsumeFilter`, additive and repeatable, with bus-wide filters before
  endpoint filters (C2/C3 / V2-C2.1/C3.1).
- Native `ActivitySource` and pipeline-duration telemetry with EO result
  header flow (C4-C6 / V2-C4..C6).
- `StreamBusException` as the common base for every StreamBus exception
  (B10 / V2-B10.1).
- CI unit + brokered integration workflow with RabbitMQ Streams enabled
  (D1 / V2-G7).
- Forced-close, metadata-close, and full connection-kill chaos integration
  coverage (D2 / V2-D2).
- Typed publish and consume filters: `IStreamPublishFilter<T>` /
  `IStreamConsumeFilter<T>` attach per declared pipe type or endpoint message
  type, with open generic closure through the `Type` overloads. Typed consume
  endpoints deserialize eagerly and share one message instance with the
  consumer.
- `UsePublishFilter<TFilter, TMessage>()` and
  `UseConsumeFilter<TFilter, TMessage>()` construct the typed filter adapter at
  the call site, so typed filters avoid a runtime generic closure. The
  interface-derived and `Type` registrations are annotated
  `[RequiresUnreferencedCode]` / `[RequiresDynamicCode]` and the dynamic paths
  carry `[DynamicallyAccessedMembers]`. Trimming and NativeAOT remain
  unsupported end to end (the default serializer is reflection-based and typed
  consume still closes generics from runtime types).

### Fixed

- A1 consumer liveness: parked consumers detect a closed handle and recover
  instead of stalling silently (A1 / V2-A1).
- A2 supervision: the connection supervisor reconnects after the system
  handle closes, with the existing jittered backoff and no reconnect storm
  (A2 / V2-A2).
- A3 confirm-path hardening: close callbacks complete pending confirms
  instead of leaving producers hung (A3 / V2-A3).
- A4 unwedged dispatch: the error-stream hand-off send and confirmation share
  one budget, so a blocked hand-off publish or a lost confirmation cannot wedge
  the partition dispatch gate (A4 / V2-A3).
- A5 producer lifecycle: producer registry slots serialize creation,
  invalidation, and non-blocking disposal (A5 / V2-A5).
- A6 health truth: degraded/unhealthy states now reflect registration-level
  attach and declaration outcomes (A6 / V2-A6).
- A7 shutdown classification: host shutdown rethrows cancellation instead of
  reporting a startup timeout, and dispose failures are logged (A7 / V2-A7).
- A8 error-queue round-trips: dead-letter bursts reuse the AMQP channel and
  both destination kinds use a uniform confirmation budget (A8 / V2-A8).
- Stale-handle reattach: raw, typed, effectively-once, and replay partition
  consumers resolve the supervisor's current system handle on every attach,
  query, and replay producer operation, so a reconnect cannot retry against a
  disposed system forever (D2 / V2-D2).
- Partition-scoped replay identity: explicit-target replay names and caches
  producers per source partition, so equal source offsets from different
  partitions no longer share a broker dedup space (D2 / V2-D2).
- Late consumer-close callbacks no longer clear a newer consumer handle, so the
  watchdog keeps observing the live subscription (D2 / V2-D2).
- Replay and requeue identity: error-envelope replay and queue requeue carry
  the original `MessageId`, `CorrelationId`, `SentTime`, the original user
  headers, and the original `content-type` forward instead of minting fresh
  values or dropping them at parse time (PUB-4); `SentTime` is normalized to
  UTC on read.
- Effectively-once retries reuse one message identity: a logical result
  publish keeps the same `MessageId` and `SentTime` across every retry attempt.
- Header removal takes precedence over call-site values, so a key removed
  through `IStreamSendHeaders.Remove` is not resurrected by the publish
  options.
- Typed filter/context/consumer plumbing no longer looks members up by name or
  compiles expression trees. A rename can no longer turn into a delivery-time
  crash, and the deserialization path no longer pays `Expression.Compile`
  startup cost.
- Non-effectively-once dispatch now owns the inflight counter and
  retry-blocked flag in a single `try/finally` instead of five hand-placed
  decrement sites (CON-3). The `streambus_partition_inflight` gauge stays 1
  while a retry-exhaustion hand-off is still in progress and returns to 0 on
  every exit path, including a shutdown racing the hand-off.
- Consumer health rows are attributable when several buses share a vhost
  (CON-11). Registration names now include the bus type
  (`endpoint:{bus}:{stream}`, `raw:{bus}:{stream}`,
  `effectively-once:{bus}:{sourceStream}`) instead of only the stream name, so two buses consuming the same stream no
  longer overwrite one health row and the check can no longer report
  `Attached` for a bus that is actually retrying.
- Queued dispatches observe shutdown (CON-13): a delivery that acquires the
  serialized dispatch gate after `DisposeAsync` began now exits before touching
  the attach or invoking the handler. The gate does not cancel waiters and the
  current-generation attach is not drained at dispose time, so the disposed
  flag is checked immediately after the gate is acquired.
- Error hand-off no longer retries fatal failures forever (INF-2). A
  `StreamBusFatalException`, a configuration error, or a fatal transport error
  (protocol/SSL/auth) now surfaces immediately so the dispatch safety net marks
  the partition failed and recycles it, instead of spinning at the backoff delay
  for the process lifetime while health still reports the partition attached.
  Transient and topology failures (a missing error stream, not-ready, timeouts)
  still retry without advancing. The stream hand-off path also stops wrapping
  fatal transport errors as `StreamBusPublishTransientException`, matching the
  normal publish path.
- The error-queue transport's cached publish channel and its connection are now
  one atomic pair swapped with a single write (INF-6), and `DisposeAsync` /
  connection invalidation take the publish gate before resetting the pair, so
  disposal can no longer reset or dispose a channel while a publish holds the
  gate; the mismatched-pair torn read the two-field layout allowed is no longer
  representable.

### Validation

- Release gate: `dotnet build StreamBus.slnx` reports 0 warnings and 0 errors
  at `latest-recommended` analyzers (B16 / V2-B16); unit suite 428/428;
  integration suite 70 passed, 2 skipped against RabbitMQ 4.2.6; the three
  destructive chaos tests passed on three consecutive runs (9/9).
- Traceability: `docs/superpowers/specs/2026-10-18-streambus-v2-architecture-and-middleware-design.md`
  §10.1 maps every review finding to its acceptance ID, task, and test.
- Docs: the README and guide samples use the same API surface exercised by the
  unit, integration, and example projects. No
  `IConfiguration`/`appsettings.json` binding exists (D3 / V2-G4).
- Chaos: the three D2 integration tests pass together repeatedly in the
  serialized `ChaosCollection` (V2-D2).
