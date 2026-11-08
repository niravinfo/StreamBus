# ADR-1: Liveness closed loop

- **Status:** Accepted
- **Date:** 2026-10-21
- **Supersedes:** none

## Context

The v1 failure modes were silent stalls: a broker-side close could park a
consumer on a handle that would never deliver again, and the connection
supervisor could keep reporting `Ready` after its `StreamSystem` handle had
closed. The client's `Reliable*` wrappers hide the recovery state the bus
needs for health and backoff.

## Decision

Recovery is a closed loop owned by the entity that owns the resource:

- Close/metadata callbacks fail the current attach exactly once; the partition
  attach loop tears down its consumer and recreates it with the existing
  jittered backoff.
- The connection supervisor polls the current system handle's `IsClosed` state
  on its configurable monitor interval (default 5s); on close it swaps in a
  new `StreamSystem`, disposes the old one, and marks the connection transient
  until the new system is ready.
- The supervisor never recovers individual producers or consumers — attach
  loops and the producer registry own their own recovery.
- Attach loops must resolve the current handle on every attach and offset
  query. A cached system handle is a defect, not an optimization; a null
  handle is treated as transient so the loop backs off.

Alternatives rejected: a process-wide watchdog (too coarse, restarts healthy
work) and the client's `Reliable*` wrappers (own retries internally, defeating
the bus's own backoff and health reporting).

## Consequences

- Recovery is observable through the existing health and readiness surfaces.
- Close callbacks use idempotent (`TrySet`-style) settlement so a failed attach
  is reported exactly once.
- Every new attach-loop path must go through the supervisor's current handle.
