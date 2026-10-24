# ADR-3: Health aggregation

- **Status:** Accepted
- **Date:** 2026-10-21
- **Supersedes:** none

## Context

The v1 health check described connection state but was blind to consumer
attach and declaration outcomes. A bus could report healthy while a consumer
was retrying forever, and `DescribeStates` conflated connection state with the
severity of registration failures.

## Decision

A single internal `StreamBusHealthAggregator` exists per virtual host and is
shared by every bus on that host, because those buses share one connection and
supervisor. Registration code writes consumer and declaration rows into it as
state changes; the public `StreamBusHealthCheck` delegates wholly to the
aggregator and adds no independent judgement.

The aggregate is computed from connection state and the registration rows:

- **Unhealthy** when the connection is fatal, any declaration has exhausted
  its retries, or every consumer row is in `FailedRetrying`.
- **Degraded** when any consumer is retrying or the connection is not `Ready`.
- **Healthy** otherwise.

Each row carries the registration name, kind, state, reason, and the timestamp
of the last state change. Unchanged upserts preserve the original timestamp so
the reported `Since` does not churn.

## Alternatives considered

- **Extend `DescribeStates`.** It is a diagnostic projection, not a severity
  model; extending it would keep conflating connection health with attach and
  declaration outcomes and would give every caller a different aggregation.

## Consequences

- Health exposes the reason and duration of a degraded or unhealthy window.
- With one consumer, a failed consumer maps to **Unhealthy**, not Degraded;
  Degraded is the partial-failure state with multiple consumers.
- A connection-only kill may not surface as a publisher outage because the
  client's `StreamSystem` transparently reconnects its locator. The consumer
  aggregate still transitions through a non-Healthy window before recovering.
  Tests assert the behavior the bus can guarantee rather than a publisher
  outage window that this client hides.

## Evidence

- `HealthAggregatorTests` (status matrix, reason and `Since` behavior)
- `BrokerRestartTests.Connections_kill_does_not_skip_or_gap` (consumer health
  samples a non-Healthy window, then Healthy, V2-D2)
