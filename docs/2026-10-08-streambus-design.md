# StreamBus - Design Document

Date: 2026-10-08
Status: Final for v1 scope. Nothing implemented yet.
Supersedes the AMQP 0.9.1 design in `2026-10-03-rabbitmq-ordered-consumption-design-discussion.md`
and the `StreamRelay` draft of this document. Those predecessors (including the
`x-modulus-hash` topology referenced in §4.1) are historical context only and are not
in this repo.

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

So the registry is keyed by (bus type, vhost):

```
IStreamBusRegistry
  GetOrCreateBus<TBus>(vhost) -> IStreamBus<TBus>   (wrapper cached per (TBus, vhost))
  // one StreamSystem (locator + pools) cached per vhost and shared by all TBus bound to it
```

`TBus` is a caller-supplied marker type (MassTransit multi-bus concept). The same
marker type MUST NOT be registered twice; a duplicate `AddStreamBus<TBus>` fails at
configuration time. `IStreamBus<TBus>` exposes `PublishAsync(stream, ...)` /
`ConsumeAsync(stream, ...)`; the non-generic `IStreamBus` is `IStreamBus<DefaultBus>`
for single-bus apps. Callers never see `StreamSystem`.

A `StreamSystem` owns a locator connection plus pools of producer and consumer
connections. Callers never see it.

### 2.2 Connection pooling defaults

`ProducersPerConnection` and `ConsumersPerConnection` both default to **1**, i.e. one
TCP connection per producer and per consumer, via `StreamBusConnectionOptions`
(overridable per `AddStreamBus<TBus>`). The client's own comment says higher
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
  and authentication failures (any `AuthenticationFailure*`, including the
  `guest`/loopback rejection in §1.1) as fatal (do not retry) from network
  errors (retry). Credential validation (FR-8.9) runs before the loop so bad
  credentials fail fast with an actionable message and never enter backoff.
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

Publish and consume calls take a `ReadinessPolicy`. `PublishOptions.Timeout` is the
publisher-confirm timeout only, not the readiness wait; `WaitUpTo(TimeSpan)` bounds
only the readiness wait.

Startup blocking is separate and user-configurable (MassTransit `HostOptions`-style).
`StreamBusHostOptions { BlockStartupUntilReady = false (default), StartupTimeout }`
is set per `AddStreamBus<TBus>`. When false (default), `IHostedService.StartAsync`
returns without waiting; the supervising loop keeps retrying in the background and
readiness stays visible via health. When true, startup waits until every registered
bus reports `Ready`, bounded by `StartupTimeout`; on timeout `StartAsync` surfaces
`TimedOut` (the background loop keeps retrying, it never gives up). A health
endpoint exposes current state per (vhost, bus type).

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
Do not set `InitialCredits = 1` for ordering: ordering comes from sequential dispatch,
not from credits (see correction below). Leave credits at the client default unless
measured otherwise.

Correction, from the client source: `InitialCredits` is prefetch measured in **chunks**,
not messages, and it is not what serializes dispatch. `ProcessChunks` is a single task that
reads a chunk, `await ParseChunk(chunk)`, then reads the next, so dispatch is sequential per
subscription at any credit value. The value only bounds how many chunks the broker may have
in flight to us. A chunk holds many messages, so `InitialCredits = 1` still allows a whole
chunk of messages to be dispatched one at a time, and a stall on a hot key holds up the rest
of that chunk, not just the rest of its own key.

The same rule governs reconnect. The `Reliable` consumer resolves a reconnect offset from its
own in-memory last-dispatched value, and it writes that value **after** the handler's
exception has been caught and logged. Under `FailureMode.Strict` a handler still retrying
has not stored its offset, so a reconnect in that window starts past the unprocessed
message. The built-in reconnect cannot be disabled, so the library does not use
`Reliable*` for ordered paths at all: it uses `RawConsumer` / per-partition raw
consumers plus its own supervision loop (same pattern as §3), resolving the offset
itself from the broker on every attach — see §12 and gate **G-5**.

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
what preserves order for the partition, but also stalls it. Per the §4.4 correction,
dispatch is chunk-sequential, so a hot-key stall holds up the rest of that chunk, not
just the rest of its own key. Design:

```
RetryPolicy {
  MaxAttempts, BaseDelay, Multiplier, MaxDelay, Jitter,
  DelayCalculator: Func<int attempt, Exception, TimeSpan>   // user-configurable
  FailureMode = Strict | SkipAfterDeadline                   // per consumer, Strict default
}
```

`attempt` passed to `DelayCalculator` is the 1-based retry number (1 = delay before
the first retry after the initial failure). `MaxAttempts` is total processing
attempts including the initial one (1 = no retry, 3 = initial + 2 retries).
When supplied, `DelayCalculator` overrides `BaseDelay`/`Multiplier`/`MaxDelay`/`Jitter`.
"Deadline" in `SkipAfterDeadline` means exhaustion of `MaxAttempts` (count-based);
there is no separate wall-clock deadline in v1.

`FailureMode` resolves what happens when the retry budget is exhausted.
It is also the error-management switch (user decision 8):

- `Strict` (default) — keep retrying; the offset stays where it is so a crash redelivers.
  The partition stalls behind the failing key. Preserves ordering. No error-stream
  management: `ErrorStream` MUST NOT be configured with `Strict` (rejected at
  configuration time). `MaxAttempts` MUST be null (unbounded) with `Strict`.
- `SkipAfterDeadline` — bounded by `MaxAttempts` (required). On exhaustion, advance
  the offset exactly once and publish to the error stream, then consider the message
  completed. The key falls behind and its ordering becomes best-effort from that point on.
  `ErrorStream` is REQUIRED with this mode. An optional `OnRetryExhausted` hook may
  observe the handoff but cannot change the outcome (advance + error publish).

Set per consumer. See §11.1. Under `Strict` the offset is not advanced until the
handler succeeds, so a crash mid-retry redelivers. `StoreOffset` is called
explicitly.

Streams have **no TTL, no DLX, no per-message priority**. An error queue is a real
stream that we publish to by hand, with `x-max-age` retention as the only mechanism
preventing unbounded growth. Retention is not optional there; it is the design.
See **FR-5.2**.

The stored offset never exceeds the last successfully processed offset of that
(reference, partition): **FR-2.7** (see §12). The client's `Reliable` consumer
tracks its own last-dispatched offset in memory and advances it even when the handler
throws, which is why ordered paths do not use `Reliable*` at all and the library
does not delegate the resume decision to it (**G-5**, see §12.4).

The two modes differ only in the store point. Under `Strict` nothing is stored until the
handler succeeds, so a crash mid-retry redelivers. Under `SkipAfterDeadline` the offset
advances exactly once, at the moment the message is handed to the error stream.
`StoreOffset` is called explicitly in both cases, and only after the handler has returned.

## 6. Error stream (dead letter destination)

This is the dead letter destination, used only with `FailureMode.SkipAfterDeadline`.
With `FailureMode.Strict` there is no error-stream management at all (user decision:
no skip, no DLQ). Streams have no DLX, so the error destination is a real stream
we publish to by hand. Per logical queue, or one shared with routing. The error
message preserves the original body and carries in `ApplicationProperties`:

- original stream, partition, offset
- exception type, message, stack
- attempt count, timestamps
- correlation / message id, original routing value

Error-stream writes use library-owned stable producer names per error stream.
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
so a crash cannot desync them. Handlers do not publish via `ConsumeContext`: the
plain path just returns and the library stores the offset on success. The result path
is a separate `PublishResultAsync(target, result, sourceOffset, ...)` API that enforces
publishing-id = sourceOffset and stable producer-name selection. Requirements
(**FR-6.1** through **FR-6.7**):

- one write per processing step
- publishing to the target stream is the only side effect
- **only one instance publishes under a given producer name** (dedup does not support
  concurrent publishing) - enforce with SAC on the source. A super-stream source has
  N concurrently-active partitions, so it needs N stable producer names (one per
  source partition to target), not one shared name.
- stable producer names, unique per (source partition, target stream), meaningful not a GUID
- results published in order, none skipped: keep retrying, never time out silently
- max 255 producer names per stream

For a super stream target, query the last publishing id on **every** partition and
resume from the maximum.

Note the interaction with SAC: on the `Raw*` path there is no automatic offset
tracking to turn off. The effectively-once source stores nothing (FR-10.14) and
computes the start offset from the last publishing id on every attach (create,
promote, reconnect).

## 8. Topology declaration

Publishing does **not** auto-create. Publishing to a missing stream fails with
`StreamDoesNotExist` (response code `0x02`).

`CreateStream` / `CreateSuperStream` are **not concurrency-safe**: 4 concurrent calls on
the same name produce `PreconditionFailed` / `StreamAlreadyExists`. The library must
(a) serialize declaration per stream name in-process and cache results, and
(b) tolerate the cross-process race by catching the already-exists response: when the
existing spec matches the requested spec, treat as success and cache; when specs
conflict (e.g. different partition count, which is immutable per FR-3.3), throw
`StreamConfigurationConflictException` naming expected vs actual. N instances starting
at once and declaring the same spec all succeed; only genuine conflicts fail.

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
// bus per (type, vhost); TBus marker must be unique per registration
IStreamBusRegistry.GetOrCreateBus<TBus>(vhost) -> IStreamBus<TBus>
IStreamBus<TBus>.WaitUntilReadyAsync(ReadinessPolicy) -> BusReadiness
// non-generic IStreamBus is IStreamBus<DefaultBus> for single-bus apps

// publish
IStreamBus<TBus>.PublishAsync(stream, message, PublishOptions)
PublishOptions { ReadinessPolicy, RoutingKey, DeduplicationName, Timeout /* confirm timeout only */ }

// effectively-once result path (FR-6 only, separate from ConsumeContext)
IStreamBus<TBus>.PublishResultAsync(targetStream, result, sourceOffset, producerName, ...)
// enforces publishing-id = sourceOffset and stable producer-name selection

// consume - delegate overload, the lower-level path
IStreamBus<TBus>.ConsumeAsync(stream, ConsumeOptions, handler)
ConsumeOptions {
    IsSuperStream, Reference = null,     // null = auto-derived (default); shared Reference = one SAC group
    SingleActiveConsumer = true,
    Ordering = Strict,                  // dispatch ordering, distinct from Retry.FailureMode; library owns offset-on-promotion
    Offset = ConsumeOffset.ResumeOrFirst,   // FR-10, see §12; AlwaysFirst/AlwaysNext are explicit escapes
    OffsetStore = OffsetStoreOptions,        // { EveryMessages = 1 (default), MaxInterval = null }
    Retry = RetryPolicy,                // carries FailureMode, per consumer; see §5 for attempt/MaxAttempts semantics
    ErrorStream = null,                 // ErrorStreamOptions { Stream, RoutingValue? }; required iff SkipAfterDeadline, forbidden iff Strict
}

// offsets, for operators and migrations - never on the per-message path
// partition is the 0-based partition index for super streams; 0 for plain streams
IStreamBus<TBus>.GetConsumerOffsetAsync(reference, partition)          -> ulong?
IStreamBus<TBus>.SetConsumerOffsetAsync(reference, partition, offset)

// consume - typed, DI-resolved MassTransit-style IConsumer<T> (FR-2.1)
// stream binding is explicit at registration; no convention magic
services.AddStreamBusConsumer<TBus, OrderCreated, OrderCreatedConsumer>(stream: "orders", configure: opts => { ... });

public interface IConsumer<T> {
    Task Consume(ConsumeContext<T> context, CancellationToken ct);
}

ConsumeContext<T> {
    T Message, string RoutingValue, string Stream, int Partition, ulong Offset,
    ReadOnlyMemory<byte> Body, MessageMetadata Metadata, CancellationToken
}
// ConsumeContext has no publish method. Plain paths just return and the library stores
// the offset. Only the FR-6 result path publishes, via PublishResultAsync.

// topology
IStreamTopology.EnsureStreamAsync(spec)   // serialized in-process, cached, cross-process race tolerated, idempotent
IStreamTopology.EnsureSuperStreamAsync(spec)
```

The delegate overload stays supported and is not deprecated (**FR-2.2**). The
typed `IConsumer<T>` is the preferred path, resolved from DI, with the stream bound
explicitly at registration.

Serialization: the typed path uses a configured `IStreamSerializer` (default JSON via
`System.Text.Json`); the message `Data` body deserializes to `T`, and a
deserialization failure is handled as a handler failure under the consumer's
`Retry`/`FailureMode` policy. The delegate overload receives raw bytes and never
deserializes.

Error destinations: per-stream error streams use `ErrorStreamOptions { Stream =
"<stream>.error" }`; a shared error stream uses one `Stream` plus a routing value
(defaulting to the source stream name) so both FR-5.4 shapes are expressible.

Sagas and out-of-process consumers are explicitly out of v1 (requirements §0.2).

Hosting: `services.AddStreamBus<TBus>(vhost, cfg => { BlockStartupUntilReady = false;
StartupTimeout = ...; })` with `IHealthCheck` and `IHostedService` integration.
Duplicate `AddStreamBus<SameTBus>` fails at configuration time. One `StreamSystem`
per vhost is shared by all `TBus` bound to it.

## 10. Phases

| # | Phase | Requirements | Gates |
|---|---|---|---|
| 1 | Environment — done (section 1) | — | — |
| 2 | Risk spike — largely done (sections 4, 8) | — | G-1, G-3, G-4 |
| 3 | Bus core: supervising connect loop, readiness gate, health | FR-8.1–FR-8.10, FR-9.2 | — |
| 4 | Producer registry: N producers, confirms | FR-1 | — |
| 5 | Consumer registry: N consumers, offset tracking, SAC, ordering, per-partition metrics, the promotion fix from 4.4 | FR-2, FR-3, FR-10, FR-9.1 | G-4, G-5 |
| 6 | Retry + error stream + replay + starvation visibility | FR-4, FR-5, FR-9.3 | **G-1** |
| 7 | Effectively-once read-process-write | FR-6 | **G-2** |
| 8 | Topology with serialized declaration | FR-7 | G-3 |
| 9 | Hosting / DI / configuration, typed `IConsumer<T>` | FR-8.9, FR-8.11, FR-2.1 | — |
| 10 | Tests: unit plus integration against a real broker (all FR-1–FR-10 plus NFR-1–NFR-7) | all | G-1..G-5 |

**No phase is complete until its gates pass.** Phase 6 in particular is blocked
on G-1 (retry under load), which is not yet verified. Phase 7 is blocked on G-2
(effectively-once end to end, unverified). NFR-1–NFR-7 are verified in Phase 10,
not per phase.

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

## 12. Offset tracking

Offsets are the broker's record of how far a consumer group has progressed. This section
is the mechanics behind **FR-10** and **NFR-7**; callers never see any of it.

### 12.1 What the broker and the client actually do

All verified in source, client main branch and `rabbitmq-server` main branch.

| Concern | Fact |
|---|---|
| Addressing | One stored value per `(reference, stream)`. For a super stream the second half is the **partition** stream, so each partition has its own value |
| Reading | `StreamSystem.TryQueryOffset(reference, stream)` returns `Task<ulong?>`, null when the broker answers `OffsetNotFound`. `QueryOffset` throws `OffsetNotFoundException` in the same case |
| Writing, per partition | `RawConsumer.StoreOffset(offset)` from inside the message handler. It keys on that consumer's own reference and stream, so a partition consumer stores against its own partition |
| Writing, super stream | `RawSuperStreamConsumer.StoreOffset` throws `NotImplementedException("use the store offset on the stream consumer, instead")`. The super-stream entity is never a store path |
| Broker response | **None.** The broker's `store_offset` handler resolves the leader and calls `osiris:write_tracking/3`, then returns. No response code, no confirmation, no rejection. A client cannot tell whether a store landed, and the broker will happily write a lower value over a higher one |
| Leader lookup | If the stream leader cannot be resolved at store time the handler logs and returns, so the store is a silent no-op |
| Reference length | 255 bytes (`IS_INVALID_REF(Ref)` is `byte_size(Ref) > 255`). A longer reference is rejected at `subscribe` with `PreconditionFailed`; at `store_offset` it is logged and dropped. Distinct from FR-6.4's 255 producer *names per stream* |
| Promotion listener | `Func<string, string, bool, Task<IOffsetType>>`. Parameter order is **(reference, stream, promotedAsActive)**. Throwing, or returning null, makes the client fall back to `OffsetTypeNext` |
| Client offset tracking | The reliable consumer keeps an in-memory last-dispatched offset and uses it for reconnects, writing it even when the handler threw. It is not a source of truth and the library does not use it |

### 12.2 Resolution rule

Used unchanged on all three attach points: create, promote, reconnect.

```
ResolveOffset(reference, partition) -> IOffsetType
    stored  = broker.TryQueryOffset(reference, partition)   // attach only, never per message
    local   = localWater[reference, partition] ?? 0

    switch (ConsumeOffset):
        AlwaysFirst: return OffsetTypeFirst()               // ignores the broker entirely
        AlwaysNext:  return OffsetTypeNext()                // ignores the broker entirely

    // ResumeOrFirst / ResumeOrNext / ResumeOrFail: the broker is the source of truth
    if stored == null:
        switch (ConsumeOffset):
            ResumeOrFirst: log warning(name this); return OffsetTypeFirst()
            ResumeOrNext:  log warning(name this); return OffsetTypeNext()
            ResumeOrFail:  throw OffsetConfigurationException(reference, partition)

    // The broker wins even when it holds less than we last wrote: that means our store
    // was lost (leader outage) or that something regressed it. Replaying is the safe
    // direction here, a gap is not.
    if local > stored: log warning(name this)
    return OffsetTypeOffset(stored + 1)
```

`TryQueryOffset` is called only on attach. `AlwaysFirst` and `AlwaysNext` bypass the read
entirely and are the escapes for replay and tail-attach.

### 12.3 Write rule and monotonicity

```
StoreOffset(reference, partition, candidate)   // candidate is the exact context.Offset just processed, never +1
    if candidate <= localWater[reference, partition]:
        log warning(name this) + increment streambus_offset_store_rejected_total
        return                                                     // never sent
    partitionConsumer.StoreOffset(candidate)                       // fire and forget, exact value
    localWater[reference, partition] = candidate
```

`candidate` is always the exact offset carried by the delivered message context.
`+1` never happens here; it happens only in §12.2 when resolving the resume point
(`stored + 1`) for the next attach.

The broker cannot be relied on to reject a lower value, so the rejection happens locally
before the frame is written (**FR-2.7**). `localWater` survives demotion, so a zombie that
wakes with a stale view still cannot write below what it last wrote. A demoted consumer
stores nothing at all: SAC delivers chunks to the active consumer only, so a non-active
instance has no offsets to store.

`StoreOffset` is not confirmed, so monotonicity is not verifiable on the wire. The
verification is a read-back: after the first store on attach, compare
`TryQueryOffset` with `localWater` and record `streambus_offset_regressed_total` if the
broker holds less. That counter being non-zero means something regressed the group's
resume point, which is a loud operational signal rather than a silently duplicated batch.

### 12.4 Who owns reconnect

The client's reliable reconnect resolves an offset from its in-memory last-dispatched
value. Under `Strict` that value can be ahead of what was stored, so a reconnect during a
retry window skips the unprocessed message. The built-in reconnect cannot be disabled,
so ordered paths do not use `Reliable*` at all. **G-5** exists because of this, and the
consequence is that the wrapper owns the reconnect: a supervision loop around
`RawConsumer` recreates the consumer with the offset resolved by §12.2 on every
attach (create, promote, reconnect) rather than letting the client resume on its own.
This is the same supervision the entity-death case needs, so the two share one loop.

### 12.5 Traps

| Trap | Consequence | Guard |
|---|---|---|
| A stored offset below the stream's retained first offset | The broker falls back to the first retained offset, so retention loss looks like a small replay, not a gap | Compare the resolved offset against `StreamStats.FirstOffset()` on startup diagnostic |
| `ResumeOrNext` on a first run | Attaches at the tail and silently discards the backlog | FR-10.12 requires the warning; the branch taken is in the health surface |
| A store during a leader outage | Silently dropped, no error anywhere | Monotonicity plus the attach-time re-read; the regression counter above |
| An over-long reference | `PreconditionFailed` at subscribe, or a silently ignored store | FR-10.11, validated at configuration time |
| Promotion listener throwing, or returning null | `OffsetTypeNext`: the partition's backlog is skipped | The listener must be O(1) and must not throw; covered by **G-5** (with G-4 for failover ordering) |

## 13. Metrics catalog

Single list; labels on every series include vhost, bus type, stream, partition, and
reference where applicable. This is the surface behind FR-9.1–FR-9.3, FR-10.7, FR-10.12,
and §12.3:

| Metric | Meaning |
|---|---|
| `streambus_partition_lag` | Unprocessed messages per partition (hot-key signal) |
| `streambus_partition_inflight` | Messages dispatched but not yet completed per partition |
| `streambus_partition_retry_blocked` | 1 when a partition is stalled in retry, else 0 (with attempt/failure-mode labels) |
| `streambus_offset_store_rejected_total` | Counter of locally rejected non-monotonic store candidates (§12.3, FR-2.7/FR-10.7) |
| `streambus_offset_regressed_total` | Counter of attach-time read-backs where the broker holds less than `localWater` (§12.3) |
| `streambus_offset_fallback_total` | Counter of no-stored-offset fallbacks, labelled by branch taken (`ResumeOrFirst`/`ResumeOrNext`/`ResumeOrFail`) and resolved offset (FR-10.12) |
| `streambus_connection_state` | Current supervising-loop state per (vhost, bus type), mirrored in health (FR-9.2, FR-8.8) |
