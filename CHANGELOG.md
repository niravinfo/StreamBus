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
  (default 5s, positive and at most 60s).
- A6 health aggregation: the `streambus` check reports per-registration
  consumer and declaration state, including failure reason and last
  transition time, plus per-bus connection state.
- Bounded error hand-off confirmation: `ErrorHandoffTimeout` for stream
  destinations and `ErrorQueueOptions.ConfirmTimeout` for queue
  destinations (both default 10s).
- Message properties/header carrier for publish and consume flows.
- Ordered publish and consume pipelines: `UsePublishFilter` /
  `UseConsumeFilter`, additive and repeatable, with bus-wide filters before
  endpoint filters.
- `StreamBusException` as the common base for every StreamBus exception.

### Fixed

- A1 consumer liveness: parked consumers detect a closed handle and recover
  instead of stalling silently.
- A2 supervision: the connection supervisor reconnects after the system
  handle closes, with the existing jittered backoff and no reconnect storm.
- A3 confirm-path hardening: close callbacks complete pending confirms
  instead of leaving producers hung.
- A4 unwedged dispatch: error-stream hand-off confirmation is bounded, so a
  lost confirmation cannot wedge the partition dispatch gate.
- A5 producer lifecycle: producer registry slots serialize creation,
  invalidation, and non-blocking disposal.
- A6 health truth: degraded/unhealthy states now reflect registration-level
  attach and declaration outcomes.
- A7 shutdown classification: host shutdown rethrows cancellation instead of
  reporting a startup timeout, and dispose failures are logged.
- A8 error-queue round-trips: dead-letter bursts reuse the AMQP channel and
  both destination kinds use a uniform confirmation budget.
