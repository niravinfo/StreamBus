# ADR-3: Health aggregation

- **Status:** Accepted
- **Date:** 2026-10-21
- **Supersedes:** none

## Context

The v1 health check described connection state but was blind to consumer
attach and declaration outcomes: a bus could report healthy while a consumer
was retrying forever.

## Decision

A single internal `StreamBusHealthAggregator` exists per virtual host, shared
by every bus on that host (they share one connection and supervisor).
Registration code writes consumer and declaration rows into it as state
changes; the public `StreamBusHealthCheck` delegates wholly to the aggregator.

The aggregate is computed from connection state and the registration rows:

- **Unhealthy** when the connection is fatal, any declaration has exhausted
  its retries, or every consumer row is in `FailedRetrying`.
- **Degraded** when any consumer is retrying or the connection is not `Ready`.
- **Healthy** otherwise.

Each row carries the registration name, kind, state, reason, and the
timestamp of the last state change; unchanged upserts preserve the original
timestamp so `Since` does not churn.

Alternative rejected: extending `DescribeStates` — it is a diagnostic
projection, not a severity model, and would conflate connection health with
attach and declaration outcomes.

## Consequences

- Health exposes the reason and duration of a degraded or unhealthy window.
- With a single consumer, a failed consumer is **Unhealthy**; Degraded is the
  partial-failure state with multiple consumers.
- A connection-only kill may not surface as a publisher outage because the
  client transparently reconnects its locator; tests assert only the
  non-Healthy consumer window the bus can actually guarantee.
