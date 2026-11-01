# Changelog

## 0.2.0

### Breaking

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
  the original `MessageId`, `CorrelationId`, and `SentTime` forward instead of
  minting fresh values. Requeue cannot restore `content-type` or
  consume-added headers, and `SentTime` is normalized to UTC on read.
- Effectively-once retries reuse one message identity: a logical result
  publish keeps the same `MessageId` and `SentTime` across every retry attempt.
- Header removal takes precedence over call-site values, so a key removed
  through `IStreamSendHeaders.Remove` is not resurrected by the publish
  options.

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
