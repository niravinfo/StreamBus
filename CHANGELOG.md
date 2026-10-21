# Changelog

## Unreleased

### Added

- A6 health aggregation: health checks now report per-registration consumer and
  declaration state, including the failure reason and last transition time.
- `AddStreamBusHosting(h => h.BlockUntilConsumersAttached(...))`. The option
  defaults to `false` in this milestone; the docs milestone flips the default in
  line with ADR-6's documented break list.
