# ADR-1: Liveness closed loop

- **Status:** Accepted
- **Date:** 2026-10-21
- **Supersedes:** none

## Context

The v1 failure modes A1 and A2 were silent stalls: a broker-side close could
leave a consumer parked on a handle that would never deliver again, and the
connection supervisor could keep reporting `Ready` after its `StreamSystem`
handle had closed. A process-wide watchdog would notice neither quickly
enough, and the client's `Reliable*` wrappers hide the recovery state the bus
needs for health and backoff.

## Decision

Recovery is a closed loop owned by the entity that owns the resource:

- The stream client's per-entity close and metadata callbacks fail the current
  attach exactly once. The partition attach loop then tears down its consumer
  and recreates it with the existing jittered backoff.
- The connection supervisor polls the current system handle's `IsClosed` state
  on its configurable monitor interval (default 5s). When the handle closes it
  swaps in a new `StreamSystem`, disposes the old one, and marks the connection
  transient until the new system is ready.
- The supervisor does **not** recover individual producers or consumers; it
  only supplies the current system handle. Attach loops and the producer
  registry own their own recovery.
- Because the supervisor swaps and disposes systems, an attach loop must
  resolve the current handle on every attach and on every offset query. It must
  never cache a system handle across attempts. A null handle means the
  connection is between attempts and is treated as transient so the attach loop
  backs off.

## Alternatives considered

- **Client-agnostic watchdog process.** Too coarse: it can only restart the
  whole bus, losing unrelated healthy connections and in-flight work.
- **`Reliable*` client wrappers.** They own retries internally, which would
  defeat the bus's own backoff, health reporting, and attach-point guarantees.

## Consequences

- Recovery is observable in the existing health and readiness surfaces.
- Duplicate settlement must remain idempotent, so close callbacks use
  `TrySet`-style completion and a failed attach is reported once.
- Every new attach-loop path must use the supervisor's current handle; a cached
  handle is a defect, not an optimization. The regression test
  `ConsumerDeathTests.Reattach_queries_through_current_system_after_supervisor_swap`
  pins the query path after a swap.

## Evidence

- `ConnectionSupervisorTests.Supervisor_reconnects_after_system_handle_closes`
- `ConsumerDeathTests` (unit close-callback and attach-loop tests)
- `ConsumerDeathTests.Connection_closed_while_idle_reattaches_within_bounds` and
  `ConsumerDeathTests.Metadata_event_close_reattaches_and_promotes_correctly`
  (integration, V2-A1)
- `BrokerRestartTests.Connections_kill_does_not_skip_or_gap` (integration,
  V2-D2)
