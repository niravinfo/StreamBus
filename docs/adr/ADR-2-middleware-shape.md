# ADR-2: Middleware shape

- **Status:** Accepted
- **Date:** 2026-10-21
- **Supersedes:** none

## Context

v2 adds message properties and user-composable publish and consume
middleware. The API should feel familiar to users of message-bus middleware
without weakening StreamBus's compile-time routing guarantees. RabbitMQ
Streams is append-only, so the publish path cannot support the same
short-circuit-and-replace semantics that a queue-oriented bus can.

## Decision

**Publish is decoration-only.** Filters run after the stream and routing key
are resolved and before serialization. They may observe the publish and mutate
headers through the header writer, but they cannot replace the body, stream,
or routing key. The pipeline runs on the typed facade only
(`PublishAsync`/`PublishSuperAsync`). Raw publishes, effectively-once result
publishes, error-envelope hand-off, requeue, and replay republishes are the
documented pipeline-free escape hatches.

**Consume decorates the outer delegate with an explicit skip.** The pipeline
wraps deserialization, typed-consumer resolution, and handler invocation, and
shares one per-message DI scope with the typed consumer. A filter may set
`SkipHandler` to mark the message processed without invoking the handler or
publishing an error. Skip is capability-gated: a filter declares `CanSkip`,
effectively-once pipelines reject any skip-capable filter at configuration
time, and typed endpoints require `AllowPipelineSkip()`. Raw consumers accept
skip unconditionally.

**Both pipelines are ordered, additive, and guarded.** Filters execute in
registration order; bus-wide filters run before endpoint filters, and the
after-section unwinds in reverse. Zero `next` calls without skip and multiple
`next` calls are contract violations, classified through the existing
retry/exhaustion machinery on consume and as configuration errors on publish.

## Alternatives considered

- **Full MassTransit-style `IPipe`.** Invites publish short-circuiting and
  delegated verification against compile-time routing guarantees that StreamBus
  deliberately keeps.
- **Observe-only consume middleware.** Cannot express a legitimate drop
  policy, so a common tenant-filter use case would need a handler workaround.
- **Publish veto/suppression.** Rejected because an append-only stream publish
  that "succeeds" without writing is indistinguishable from data loss.

## Consequences

- Users get familiar filter ergonomics without a second routing model.
- Publish filters cannot implement outbox suppression or body replacement; a
  future need reopens this ADR rather than extending the filter contract.
- Consume skip is explicit and auditable, and cannot silently discard
  effectively-once work.

## Evidence

- `PublishPipelineTests` (order/unwind, zero/double `next`, duplicate types,
  header precedence, super routing non-substitution, raw bypass)
- `PipelineSemanticsTests` (three-filter order, bus-then-endpoint order,
  payload flow, skip allow/deny, EO rejection, shared per-message scope)
- `ConsumePipelineIntegrationTests` and `PublishPipelineIntegrationTests`
  (headers end to end, tenant filter, exhaustion envelope)
