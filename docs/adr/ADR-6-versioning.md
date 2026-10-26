# ADR-6: Versioning

- **Status:** Accepted
- **Date:** 2026-10-21
- **Supersedes:** none

## Context

v2 removes dead and misleading surface, internalizes stores and the hosted
service, changes the default blocking-startup behavior, and adds message
properties and middleware. These are deliberate breaks, not accidents. There
are no known internal consumers pinning the v1 surface.

## Decision

Ship the consolidated API as **0.2.0**. List every break in `CHANGELOG.md`
under `0.2.0` and provide **no compatibility shims**. Keep the package
`VersionPrefix` at `0.2.0` for the release.

The break list covers, at minimum:

- registration-store `Add` methods are internal;
- `Registrations` exposes credential-free views;
- removed `StreamBusConnectionOptionsBuilder`, `ConsumeOptionsBuilder`,
  `ConsumeOrdering`, and `ConsumeOptions.Ordering`;
- `StreamBusHostedService` is internal;
- blocking startup waits for typed consumers by default, with
  `BlockUntilConsumersAttached(false)` as the opt-out.

## Alternatives considered

- **Ship as 1.0.0.** Would declare API stability while the surface is still
  changing and make the same breaks more expensive for users.
- **Provide compatibility shims.** Keeps dead names and split-brain behavior
  alive, contradicts the surface-consolidation goal, and forces every future
  release to carry the debt.

## Consequences

- Users must read the 0.2.0 break list and update call sites; the migration is
  mechanical for the removed names.
- No deprecated surface remains to confuse new users.
- The version is a release signal, not a promise that the API will never
  change again; a future 1.0.0 decision is separate.

## Evidence

- `CHANGELOG.md` `0.2.0` breaking and added sections.
- `src/StreamBus/StreamBus.csproj` sets `VersionPrefix` to `0.2.0`.
- Full unit and integration suites are green on the consolidated surface.
