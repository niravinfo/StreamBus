# ADR-2: Middleware shape

- **Status:** Accepted
- **Date:** 2026-10-21
- **Supersedes:** none

## Context

v2 adds user-composable publish and consume middleware. The API should feel
familiar without weakening StreamBus's compile-time routing guarantees.
RabbitMQ Streams is append-only, so the publish path cannot support
short-circuit-and-replace semantics.

## Decision

- **Publish is decoration-only.** Filters run after the stream and routing key
  are resolved and before serialization. They may observe the publish and
  mutate headers, but cannot replace the body, stream, or routing key. Raw
  publishes and error-envelope hand-off are documented pipeline-free escape
  hatches.
- **Consume decorates the outer delegate.** The pipeline wraps deserialization,
  typed-consumer resolution, and handler invocation, and shares one
  per-message DI scope with the typed consumer.
- **Filters are ordered, additive, and guarded.** Bus-wide filters run before
  endpoint filters; unwinding is in reverse. A consume filter calls `next()`
  zero times (skip the delivery: handler does not run, offset advances) or
  once; more than once is a contract violation that recycles the attach. A
  publish filter must call `next()` exactly once.
- **One context per publish/delivery.** Universal and typed filters share a
  single context; the continuation is parameterless. Typed contexts derive
  from the universal context and expose the message as `Message`.

Alternatives rejected: full MassTransit-style `IPipe` (invites publish
short-circuiting), observe-only consume middleware (cannot express a drop
policy), and publish veto/suppression (a stream publish that "succeeds"
without writing is indistinguishable from data loss).

## Consequences

- Users get familiar filter ergonomics without a second routing model.
- Publish filters cannot implement outbox suppression or body replacement; a
  future need reopens this ADR rather than extending the filter contract.
- A skip is explicit and auditable — the handler measurably does not run.
