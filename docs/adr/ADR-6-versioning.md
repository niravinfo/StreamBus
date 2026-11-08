# ADR-6: Versioning

- **Status:** Accepted
- **Date:** 2026-10-21
- **Supersedes:** none

## Context

v2 removes dead and misleading surface, internalizes stores and the hosted
service, changes default startup behavior, and adds message properties and
middleware. These are deliberate breaks with no known internal consumers
pinning the v1 surface.

## Decision

Ship as **0.2.0** with **no compatibility shims**. The library is pre-1.0 and
not API-stable: breaking changes are allowed in any minor release, and no
changelog is published until the first stable release; until then, releases
are internal signals, not public contracts.

Earlier v1-era names (e.g. `StreamBusConnectionOptionsBuilder`,
`ConsumeOptionsBuilder`, `ConsumeOrdering`, the public `StreamBusHostedService`,
and registration-store `Add` methods) were removed or internalized without
deprecation bridges.

Alternatives rejected: shipping as **1.0.0** (would declare stability while the
surface is still changing) and compatibility shims (keeps dead names alive and
forces every future release to carry the debt).

## Consequences

- No deprecated surface remains to confuse new users.
- The version is a release signal, not a promise that the API will not change;
  the 1.0.0 decision is separate and future.
- A changelog will be introduced when the library has a stable release and
  users who need to migrate; until then breaking changes are recorded in
  commit history only.
