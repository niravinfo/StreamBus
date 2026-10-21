# Changelog

## Unreleased

### Breaking

- Removed the unwired `StreamBusConnectionOptionsBuilder`,
  `ConsumeOptionsBuilder`, `ConsumeOrdering`, and `ConsumeOptions.Ordering`
  surface. Configure consumers through `ConsumeOptions` and endpoints through
  `IReceiveEndpointConfigurator`.
- Registration-store `Add` methods are internal; use the public
  `AddStreamBus` / `ReceiveEndpoint` configurators.
- `StreamBusHostedService` is internal; host it through dependency injection.

### Added

- A6 health aggregation: health checks now report per-registration consumer and
  declaration state, including the failure reason and last transition time.
- `AddStreamBusHosting(h => h.BlockUntilConsumersAttached(...))`. The option
  defaults to `false` in this milestone; the docs milestone flips the default in
  line with ADR-6's documented break list.
