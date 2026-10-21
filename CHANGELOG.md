# Changelog

## 0.2.0

### Breaking

- Registration-store `Add` methods are internal; use the public
  `AddStreamBus` / `ReceiveEndpoint` configurators. Public read accessors
  remain.
- `StreamBusConfigStore.Registrations` no longer exposes passwords. It
  returns credential-free `StreamBusRegistrationView` projections.
- Removed the unwired `StreamBusConnectionOptionsBuilder`,
  `ConsumeOptionsBuilder`, `ConsumeOrdering`, and `ConsumeOptions.Ordering`
  surface. Configure consumers through `ConsumeOptions` and endpoints through
  `IReceiveEndpointConfigurator`.
- `StreamBusHostedService` is internal; host it through dependency injection.
- `AddStreamBusHosting` now exposes `BlockUntilConsumersAttached`. Blocking
  startup (`BlockStartupUntilReady`) waits for typed consumer attaches by
  default; call `BlockUntilConsumersAttached(false)` to keep the old
  connections-and-topology-only behavior.

### Added

- Configurable connection monitor interval: `h.MonitorInterval(...)`
  (default 5s, positive and at most 60s) (A2 / V2-A2).
- A6 health aggregation: the `streambus` check reports per-registration
  consumer and declaration state, including failure reason and last
  transition time, plus per-bus connection state (A6 / V2-A6).
- Bounded error hand-off confirmation: `ErrorHandoffTimeout` for stream
  destinations and `ErrorQueueOptions.ConfirmTimeout` for queue
  destinations (both default 10s) (A3/A4 / V2-A3).
- Message properties/header carrier for publish and consume flows
  (C1 / V2-C1).
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

### Fixed

- A1 consumer liveness: parked consumers detect a closed handle and recover
  instead of stalling silently (A1 / V2-A1).
- A2 supervision: the connection supervisor reconnects after the system
  handle closes, with the existing jittered backoff and no reconnect storm
  (A2 / V2-A2).
- A3 confirm-path hardening: close callbacks complete pending confirms
  instead of leaving producers hung (A3 / V2-A3).
- A4 unwedged dispatch: error-stream hand-off confirmation is bounded, so a
  lost confirmation cannot wedge the partition dispatch gate
  (A4 / V2-A3).
- A5 producer lifecycle: producer registry slots serialize creation,
  invalidation, and non-blocking disposal (A5 / V2-A5).
- A6 health truth: degraded/unhealthy states now reflect registration-level
  attach and declaration outcomes (A6 / V2-A6).
- A7 shutdown classification: host shutdown rethrows cancellation instead of
  reporting a startup timeout, and dispose failures are logged
  (A7 / V2-A7).
- A8 error-queue round-trips: dead-letter bursts reuse the AMQP channel and
  both destination kinds use a uniform confirmation budget (A8 / V2-A8).
- Stale-handle reattach: partition consumers resolve the supervisor's current
  system handle on every attach and offset query, so a reconnect cannot retry
  against a disposed system forever (D2 / V2-D2).

### Validation

- Release gate: `dotnet build StreamBus.slnx` reports 0 warnings and 0 errors
  at `latest-recommended` analyzers (B16 / V2-B16); unit suite 328/328;
  integration suite 65 passed, 2 skipped against RabbitMQ 4.2.6.
- Traceability: `docs/superpowers/specs/2026-10-18-streambus-v2-architecture-and-middleware-design.md`
  §10.1 maps every review finding to its acceptance ID, task, and test.
- Docs: the README snippet compiles verbatim through `StreamBus.DocsSnippets`;
  guide samples use the same API surface exercised by the unit, integration,
  and example projects. No `IConfiguration`/`appsettings.json` binding exists
  (D3 / V2-G4).
- Chaos: the three D2 integration tests pass together repeatedly in the
  serialized `ChaosCollection` (V2-D2).
