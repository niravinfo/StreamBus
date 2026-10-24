# ADR-4: Configuration precedence

- **Status:** Accepted
- **Date:** 2026-10-21
- **Supersedes:** none

## Context

An earlier proposal bound StreamBus options from `IConfiguration` and
`appsettings.json`. That would create two sources of truth, make connection
credentials ambient, and let a silent configuration change override explicit
code. The v2 user-centric API instead configures everything through
`AddStreamBus` and its configurators.

## Decision

**Code wins, and code is the only source.** StreamBus has no `IConfiguration`
overload, no `appsettings.json` binding, and no automatic discovery. Every
option is an explicit configurator call:

- `cfg.Host(host, vhost, h => ...)` sets connection-level values exactly once
  per bus: credentials, stream and AMQP ports, heartbeat, monitor interval, and
  pools.
- Publish, endpoint, retry, topology, pipeline, and hosting options are code
  configuration owned by the registration that consumes them.
- Validation happens once at registration; invalid values fail fast rather than
  surfacing on first use.
- Public registration projections are credential-free. Passwords stay inside
  the connection options and are never exposed through
  `StreamBusConfigStore.Registrations`.

Environment variables are not a configuration layer. The integration tests and
CI use them only to point at a broker and management endpoint.

## Alternatives considered

- **Configuration wins.** A silent JSON or environment change could override a
  lambda the user can see in code, breaking their reasoning about which broker
  and credentials are in use.
- **Hybrid binding with code overrides.** Still requires documenting and
  testing two precedence models and keeps credentials in ambient configuration.

## Consequences

- Deployment-specific connection values are supplied by the application's own
  code (or by a typed options object the application owns); StreamBus does not
  read a configuration file.
- The docs state the negative explicitly, and the release gate greps
  `src/StreamBus` for `IConfiguration` (zero hits).
- Removing the proposed binding is a 0.2.0 breaking change for anyone who
  expected it, recorded in `CHANGELOG.md`.

## Evidence

- `grep -rn "IConfiguration" src/StreamBus` returns zero hits.
- README and guides contain no `IConfiguration` or `appsettings.json` example.
- `StreamBusConfigStore.Registrations` returns `StreamBusRegistrationView`
  without passwords.
