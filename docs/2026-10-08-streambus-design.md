# StreamBus - Design Document

Date: 2026-10-08
Status: Final for v1 scope. Nothing implemented yet.
Supersedes the AMQP 0.9.1 design in `2026-10-03-rabbitmq-ordered-consumption-design-discussion.md`
and the `StreamRelay` draft of this document.

Requirements live in `2026-10-08-streambus-requirements.md`. This document is the
HOW; that one is the WHAT, and every requirement there has an ID referenced by
the phases in §10.

## 0. Summary

A MassTransit-style messaging library for .NET built on `RabbitMQ.Stream.Client`
(the native RabbitMQ Stream protocol client). No MassTransit.

Primary goal: ordered per-key consumption with load balancing across N instances,
which the earlier AMQP design could not deliver without a database.

Naming: `StreamBus.*`, deliberately not `RabbitMQ.Stream.*`, to avoid colliding
with the official client's namespace.

## 1. Verified environment facts

Measured against the local broker (RabbitMQ 4.2.6, single node `rabbit@local`).

| Fact | Evidence |
|---|---|
| `rabbitmq_stream` enabled, port 5552 open | Real producer/consumer round trip, 3/3 messages, offsets 0,1,2 |
| Broker is a **single node** | `/api/nodes` returns 1 |
| `guest` **cannot** be used by the stream client | `AuthenticationFailureLoopback` on first producer |
| Stream protocol is not AMQP | Command keys `0x0001` declarepublisher, `0x0002` publish, `0x0006` subscribe |
| Message *format* is AMQP 1.0 | `Message` exposes `Properties`/`ApplicationProperties`/`Data`/`MessageHeader` |

### 1.1 The guest/loopback trap

The locator connection succeeds, then the first producer fails with
`AuthenticationFailureLoopback: guest`. Cause: the client issues a `metadata` command
and connects to the node name it returns (`rabbit@local`). `local` resolves to
172.17.64.1 / 172.29.0.1 / 192.168.0.104, not loopback, so guest's loopback-only
restriction applies.

Any Docker or load-balancer deployment hits the same trap. **The library must
validate credentials at startup and fail with an actionable message rather than
letting this surface later as a confusing producer error.** A distinct non-guest
user is mandatory.

## 2. Connection model

### 2.1 One bus per vhost, not per stream

A `StreamSystem` binds to one vhost at construction. There is **no per-producer or
per-consumer vhost override** in the API. Verified by connecting a second
`StreamSystem` to a second vhost successfully.

One `StreamSystem` serves many streams. Verified: two producers targeting two
different streams from one bus instance, both confirmed.

So the registry is keyed by vhost:

```
IStreamBusRegistry
  GetOrCreateBus(vhost) -> StreamSystem        (cached, one per vhost)
  PublishAsync(vhost, stream, message, ...)
  ConsumeAsync(vhost, stream, ...)
```

A `StreamSystem` owns a locator connection plus pools of producer and consumer
connections. Callers never see it.

### 2.2 Connection pooling defaults

`ProducersPerConnection` and `ConsumersPerConnection` both default to **1**, i.e. one
TCP connection per producer and per consumer. The client's own comment says higher
values "is not the best for performance". Leave at 1 unless measured otherwise.

### 2.3 Leader/replica placement

The client automatically connects producers to the stream leader and consumers to a
replica, using `metadata` plus `AddressResolver`. Do not override this. A producer and
consumer for the same stream may legitimately be on different nodes.

On a single-node cluster this is a no-op.

## 3. Startup connection retry (gap in the client)

`StreamSystem.Create` throws `StreamSystemInitialisationException("no endpoints could
be reached")` when nothing is reachable. **There is no retry at startup.** Mid-life
disconnects are handled by the client's `Reliable` layer; initial connect is not.

The library must wrap it in a supervising loop:

- retry forever, never surface a terminal connection error to the caller
- exponential backoff with jitter, capped
- distinguish `ProtocolException` / `SslException` / `AuthMechanismNotSupportedException`
  (fatal, do not retry) from network errors (retry)
- expose state transitions so health checks and readiness probes work

This is the single most important thing the client does not do for us.

### 3.1 Readiness gate

The scenario the user raised: a consumer publishing through a producer connection
that is not up yet, during startup. The API must make the choice explicit per call
site rather than forcing one global behaviour.

```
enum ReadinessPolicy { FailFast, WaitUpTo(TimeSpan), WaitIndefinitely }

WaitUntilReadyAsync(ReadinessPolicy) -> BusReadiness  // Ready | TimedOut | Fatal
```

Publish and consume calls take a `ReadinessPolicy`. A health endpoint exposes current
state per vhost.

## 4. Ordering and load balancing (verified)

### 4.1 Mechanism

A **super stream** is N partitions (`{name}-0..N-1`) behind a direct exchange.
Producer picks a partition by hashing a routing value; **Single Active Consumer**
guarantees one active consumer per partition. Ordering is therefore per key, and
parallelism comes from having multiple partitions.

This replaces the `x-modulus-hash` + N-queue topology from the AMQP design.

### 4.2 The routing function is load-bearing

```
SuperStreamConfig = new() { Enabled = true, Routing = msg => <routing value> }
```

The return value is hashed to choose the partition. Everything that must stay ordered
for one entity **must be the routing value and nothing else**.

The AMQP design used routing key `{TenantId}.{EntityId}`. Prefixing an event type
into that key scattered one entity's events across partitions (measured 5 of 8 keys
split). The same trap applies to `Routing`. Do not put the event type in it.

If several event types for one entity must stay ordered relative to each other, they
must share a routing value. If they need not be ordered relative to each other,
distinct routing values give free parallelism.

### 4.3 SAC does balance partitions across instances

Measured: super stream of 6 partitions, 4 consumers sharing one `Reference`,
registering with a stagger.

```
instance 0: owns 2 [probe-super-lb-0, probe-super-lb-4]
instance 1: owns 2 [probe-super-lb-1, probe-super-lb-5]
instance 2: owns 1 [probe-super-lb-2]
instance 3: owns 1 [probe-super-lb-3]
```

Identical across 3 repeat runs. This **corrects** the earlier conclusion from the
AMQP demo that "SAC is not a load balancer, first-registered wins". That finding was
specific to the AMQP path.

Caveat: consumers that register simultaneously initially hand all partitions to the
first one, redistributed as the others appear. Distribution is not guaranteed even.
Size partitions somewhat above expected instance count and accept uneven shares.

### 4.4 The promotion replay trap

**Returning `OffsetTypeFirst()` from `ConsumerUpdateListener` on becoming ACTIVE
replays the entire partition from offset 0.** Measured with 3 instances:

```
E1: got [1,2,3,4,5,1,2,3,4,5]  *** OUT OF ORDER ***
E3: got [1,2,3,4,5,1,2,3,4,5]  *** OUT OF ORDER ***
total consumed: 55 of 30
```

Every entity was processed twice. The listener must track the last processed offset
per partition and return `OffsetTypeOffset(last + 1)`.

With that fix, same scenario, same broker:

```
E1..E6: got [1,2,3,4,5]  ORDERED   (all six)
total consumed: 30 of 30
STRICT ORDERING PER KEY: HOLDS
```

**The library owns this logic.** It is the easiest way to silently break ordering and
the documentation examples in the official repo show the `OffsetTypeFirst()` pattern.
`InitialCredits = 1` was also used to keep dispatch serialized.

### 4.5 Partition count is fixed

Set at creation, no rebalancing. Sizing guidance:

- partitions >= max expected instances, to allow spread
- one partition's throughput is the ceiling for one key
- a hot key cannot be split further

## 5. Retry (gap in the client)

**The `Reliable` client swallows exceptions from `MessageHandler`.** The wrapper
catches, logs, and advances the offset. A throwing handler means the message is
silently skipped and never redelivered.

So message-level retry **cannot** be layered outside the handler. It must be
implemented inside it.

Retry that sleeps inside the handler blocks that consumer's dispatch loop, which is
what preserves order for the partition, but also stalls it. Design:

```
RetryPolicy {
  MaxAttempts, BaseDelay, Multiplier, MaxDelay, Jitter,
  DelayCalculator: Func<int attempt, Exception, TimeSpan>   // user-configurable
  FailureMode = Strict | SkipAfterDeadline                   // per consumer, Strict default
}
```

`FailureMode` resolves what happens when the retry budget is exhausted:

- `Strict` — keep retrying; the offset stays where it is so a crash redelivers.
  The partition stalls behind the failing key. Preserves ordering.
- `SkipAfterDeadline` — advance the offset, publish to the error stream. The key
  falls behind and its ordering becomes best-effort from that point on.

Set per consumer. See §11.1. Under `Strict` the offset is not advanced until the
handler succeeds, so a crash mid-retry redelivers. `StoreOffset` is called
explicitly.

Streams have **no TTL, no DLX, no per-message priority**. An error queue is a real
stream that we publish to by hand, with `x-max-age` retention as the only mechanism
preventing unbounded growth. Retention is not optional there; it is the design.
See **FR-5.2**.

## 6. Error stream (dead letter destination)

This is the dead letter destination. Streams have no DLX, so it is a real stream
we publish to by hand. Per logical queue, or one shared with routing. Message
carries in `ApplicationProperties`:

- original stream, partition, offset
- exception type, message, stack
- attempt count, timestamps
- correlation / message id, original routing value

Replay = read the error stream, republish to the source, with a publishing id derived
from (partition, original offset) so replay is idempotent.

`x-max-age` retention is mandatory on this stream (**FR-5.2**). Without it there is
no bound on growth, and there is no other mechanism available.

## 7. Effectively-once read-process-write

> **Unverified end to end.** This is gate **G-2** and blocks phase 7. The
> individual constraints below are sound, but the full cycle has never been
> exercised. Do not treat it as working until G-2 passes.

When a handler produces a result for another stream, use the **source offset as the
publishing id of the result**. The broker stores message and publishing id atomically,
so a crash cannot desync them. Requirements (**FR-6.1** through **FR-6.7**):

- one write per processing step
- publishing to the target stream is the only side effect
- **only one instance publishes under a given producer name** (dedup does not support
  concurrent publishing) - enforce with SAC on the source
- stable producer name, unique per target stream, meaningful not a GUID
- results published in order, none skipped: keep retrying, never time out silently
- max 255 producer names per stream

For a super stream target, query the last publishing id on **every** partition and
resume from the maximum.

Note the interaction with SAC: a SAC reference usually enables automatic offset
tracking, which would override the derived offset. Turn automatic tracking off and
compute the start offset from the last publishing id on promotion.

## 8. Topology declaration

Publishing does **not** auto-create. Publishing to a missing stream fails with
`StreamDoesNotExist` (response code `0x02`).

`CreateStream` / `CreateSuperStream` are **not concurrency-safe**: 4 concurrent calls on
the same name produce `PreconditionFailed`. The library must serialize declaration per
stream name and cache results, or sporadic failures appear under load.

Treat stream specs as startup configuration. `x-max-age` and
`x-queue-leader-locator` are effectively fixed at declaration; prefer policies for
anything tuned in production.

### 8.1 x-queue-leader-locator

Chooses which node hosts the stream leader at declaration time only. Tested on 4.2.6:

| Value | Behaviour |
|---|---|
| `client-local` | Node the declaring client is on. Broker default. |
| `least-leaders` | Node with fewest stream leaders. |
| `balanced` | Accepted, stored verbatim. Legacy name for `least-leaders`. |
| `random` | Random node. |
| other | `PreconditionFailed` |

`balanced` and `least-leaders` are both accepted even though the 4.3 docs still
document only `balanced`. The C# `LeaderLocator` enum offers `client-local`,
`random`, `least-leaders`, and `StreamSpec` defaults to `least-leaders`. **Use
`least-leaders`.** On a single node this has no observable effect.

### 8.2 x-max-age

Retention: how long data is kept before the oldest is trimmed. Not TTL, not per
message. Streams have no per-message TTL at all.

| Requested | Stored |
|---|---|
| none | absent, grows forever |
| 1 hour | `x-max-age=3600s` |
| 7 days | `x-max-age=604800s` |

Units: `Y M D h m s`. Evaluated **per segment**, and at least one segment is always
kept, so a low-traffic stream retains data well past `x-max-age`. Lower
`x-stream-max-segment-size-bytes` for predictable expiry. Policy beats queue argument.

## 9. Public API sketch

```csharp
// bus per vhost
IStreamBusRegistry.GetOrCreateBus(vhost) -> IStreamBus
IStreamBus.WaitUntilReadyAsync(ReadinessPolicy) -> BusReadiness

// publish
IStreamBus.PublishAsync(stream, message, PublishOptions)
PublishOptions { ReadinessPolicy, RoutingKey, DeduplicationName, Timeout }

// consume - delegate overload, the lower-level path
IStreamBus.ConsumeAsync(stream, ConsumeOptions, handler)
ConsumeOptions {
    IsSuperStream, Reference,          // shared Reference = one SAC group
    SingleActiveConsumer = true,
    Ordering = Strict,                  // library owns offset-on-promotion
    Retry = RetryPolicy,                // carries FailureMode, per consumer
    ErrorStream = "<stream>.error",
}

// consume - typed, DI-resolved (FR-2.1)
services.AddStreamBusConsumer<OrderCreated, OrderCreatedConsumer>();

public interface IConsumer<T> {
    Task Consume(ConsumeContext<T> context, CancellationToken ct);
}

ConsumeContext<T> {
    T Message, string RoutingValue, string Stream, int Partition, ulong Offset,
    ReadOnlyMemory<byte> Body, MessageMetadata Metadata, CancellationToken
}

// topology
IStreamTopology.EnsureStreamAsync(spec)   // serialized, cached, idempotent
IStreamTopology.EnsureSuperStreamAsync(spec)
```

The delegate overload stays supported and is not deprecated (**FR-2.2**). The
typed `IConsumer<T>` is the preferred path, resolved from DI, with `T` mapped to a
stream.

Sagas and out-of-process consumers are explicitly out of v1 (requirements §0.2).

Hosting: `services.AddStreamBus(config)` with `IHealthCheck` and
`IHostedService` integration.

## 10. Phases

| # | Phase | Requirements | Gates |
|---|---|---|---|
| 1 | Environment — done (section 1) | — | — |
| 2 | Risk spike — largely done (sections 4, 8) | — | G-1, G-3, G-4 |
| 3 | Bus core: supervising connect loop, readiness gate, health | FR-8, FR-9.2 | — |
| 4 | Producer registry: N producers, confirms | FR-1 | — |
| 5 | Consumer registry: N consumers, offset tracking, SAC, the promotion fix from 4.4 | FR-2 | G-4 |
| 6 | Retry + error stream + replay | FR-4, FR-5 | **G-1** |
| 7 | Effectively-once read-process-write | FR-6 | **G-2**, G-3 |
| 8 | Topology with serialized declaration | FR-7 | G-3 |
| 9 | Hosting / DI / configuration, typed `IConsumer<T>` | FR-8.9, FR-2.1 | — |
| 10 | Tests: unit plus integration against a real broker | all | G-1..G-4 |

**No phase is complete until its gates pass.** Phase 6 in particular is blocked
on G-1 (retry under load), which is not yet verified. Phase 7 is blocked on G-2
(effectively-once end to end, unverified) and G-3.

## 11. Resolved questions

These were open in the `StreamRelay` draft. Each is now a requirement in
`2026-10-08-streambus-requirements.md`; the requirement ID is given.

### 11.1 Retry blocking one partition — RESOLVED

Configurable **per consumer**, not per message type and not globally. Two
consumers on different streams can use different modes at the same time.

| Mode | Behaviour |
|---|---|
| `FailureMode.Strict` (default) | Retry stalls the partition. Offset not advanced until the handler succeeds. |
| `FailureMode.SkipAfterDeadline` | Retry budget is bounded. On exhaustion the offset advances and the message goes to the error stream. |

`Strict` is the default because it is the only mode the §4.4 measurement
actually supports: nothing has been measured under `SkipAfterDeadline`.

Requirement: **FR-4.3** through **FR-4.7**. Note the cost of `Strict` explicitly:
one failing key stalls its whole partition, and no other key in that partition
moves until it resolves. That is the price of the ordering guarantee, not a bug.

### 11.2 In-handler retry under load — UNRESOLVED, now a gate

Still untested. §4.4 measured the promotion fix **without** retry in the path.
Carrying retry, concurrent load, and mid-stream SAC failover together is the
remaining spike item.

This is now gate **G-1** and blocks phase 6. It is a release blocker, not a
known-good behaviour. Do not claim ordering under retry until it passes.

Requirement: **FR-4.7**.

### 11.3 Hot-key starvation — RESOLVED (documented, not solved)

Documented ceiling plus metrics. No auto-scaling.

The partition count is fixed at declaration (§4.5) and the stream protocol cannot
migrate partitions, so there is no automatic remedy available. That is a
protocol limit, not a missing feature.

Required: document the ceiling, expose per-partition metrics (lag, in-flight,
retry-blocked) so the condition is visible, and document the caller's escape
hatch (sub-partitioning routing value, which gives up ordering across the
sub-keys).

Requirements: **FR-9.1**, **FR-9.3**, **NFR-4**, §4 of the requirements doc.

### 11.4 Single-node broker — UNRESOLVED, now an environment requirement

Leader locator (§8.1) and replica placement (§2.3) are both **no-ops on a single
node**. Everything measured in §1 was measured on `rabbit@local`, so neither has
ever been exercised.

A **3-node cluster is required** before claiming either works. This is gate
**G-3** and it blocks phase 8 (topology).

Requirements: **FR-7.3**, **NFR-5**.
