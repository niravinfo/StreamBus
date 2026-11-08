# ADR-4: Configuration precedence

- **Status:** Accepted
- **Date:** 2026-10-21
- **Supersedes:** none

## Context

Binding StreamBus options from `IConfiguration`/`appsettings.json` would
create two sources of truth, make connection credentials ambient, and let a
silent configuration change override explicit code. StreamBus instead
configures everything through `AddStreamBus` and its configurators.

## Decision

**Code wins, and code is the only source.**

- No `IConfiguration` overload, no `appsettings.json` binding, no automatic
  discovery. Every option is an explicit configurator call.
- `cfg.Host(host, vhost, h => ...)` sets connection-level values (credentials,
  ports, heartbeat, monitor interval, pools) exactly once per bus.
- Publish, endpoint, retry, topology, pipeline, and hosting options are code
  configuration owned by the registration that consumes them.
- Validation happens once at registration; invalid values fail fast rather
  than surfacing on first use.
- Public registration projections are credential-free: passwords stay inside
  the connection options and are never exposed through
  `StreamBusConfigStore.Registrations`.
- Environment variables are not a configuration layer; tests and CI use them
  only to point at a broker.

Alternatives rejected: configuration-wins (a silent change could override a
lambda the user can see in code) and hybrid binding (doubles the precedence
models to document and test, and keeps credentials ambient).

## Consequences

- Deployment-specific values are supplied by the application's own code;
  StreamBus never reads a configuration file.
- The docs state the negative explicitly; a release-gate grep keeps
  `IConfiguration` out of `src/StreamBus`.
