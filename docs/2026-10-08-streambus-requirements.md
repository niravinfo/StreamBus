# StreamBus - Requirements

Date: 2026-10-08
Status: Final for v1 scope.
Companion to `2026-10-08-streambus-design.md` (the HOW). This document is the WHAT.

Every requirement has an ID. The design doc's phases and the test suite reference
these IDs. `MUST` is binding; anything unverified is a gate (see §5), not an
assumption.

## 0. Scope

A MassTransit-style messaging library for .NET built on `RabbitMQ.Stream.Client`
(the native RabbitMQ Stream protocol client). No MassTransit dependency, no
database, no external scheduler.

The primary goal is **ordered per-key consumption with load balancing across N
instances**, which the earlier AMQP 0.9.1 design could not deliver without a
database.

### 0.1 In v1

- Publish and consume on plain streams and super streams
- Typed `IConsumer<T>` classes resolved from DI; delegate handler overload retained
- Superseding retry inside the handler, with a per-consumer strict/skip switch
- Error stream ("dead letter" destination) with idempotent replay
- Effectively-once read-process-write via offset-as-publishing-id (DES-07)
- Supervising connect loop, per-call-site readiness gate, health checks
- Broker-owned consumer offset tracking (`ConsumeOffset`), see FR-10
- Serialized, cached topology declaration
- DI hosting (`AddStreamBus`), `IHealthCheck` and `IHostedService` integration

### 0.2 Explicitly out of v1

| Excluded | Rationale |
|---|---|
| Sagas / orchestration state machines | Needs a state store; not designed or verified |
| Out-of-process consumers | Requires a plugin/bridge mechanism in RabbitMQ Streams; large unverified surface |
| Scheduled / delayed delivery | Streams have no TTL, no DLX, and no broker-side locking to implement a reservation safely |
| Automatic partition splitting | Partition count is fixed at declaration; the protocol cannot migrate partitions |
| Per-message priority, per-message TTL | Do not exist in the stream protocol |

### 0.3 Naming

`StreamBus.*`, deliberately not `RabbitMQ.Stream.*`, to avoid colliding with the
official client's namespace.

## 1. Functional requirements

### FR-1 Publish

| ID | Requirement | Acceptance |
|---|---|---|
| FR-1.1 | Publishing to an existing stream delivers the message | Round trip, ordered offsets 0,1,2 |
| FR-1.2 | Publishing to a non-existent stream fails with a clear error, never silently | `StreamDoesNotExist` surfaced, not swallowed |
| FR-1.3 | Publishing does NOT auto-create the stream | FR-1.2 must keep passing if auto-create is ever added |
| FR-1.4 | Publisher confirms are surfaced to the caller | Failed confirm raises |
| FR-1.5 | A producer is reused across publishes to the same stream | One producer instance, N publishes |

### FR-2 Consume

| ID | Requirement | Acceptance |
|---|---|---|
| FR-2.1 | A typed MassTransit-style `IConsumer<T>` registered in DI with an explicit stream binding (`AddStreamBusConsumer<TBus, T, TConsumer>(stream, ...)`) is invoked for `T` messages on that stream | Consumer receives deserialized `T`; stream binding is explicit, no convention magic |
| FR-2.2 | The delegate overload (`ConsumeAsync(stream, options, handler)`) works and is not deprecated | Both paths covered by tests |
| FR-2.3 | Consumers sharing one `Reference` form one SAC group and split partitions across instances | Super stream of 6 partitions, 4 instances → all 6 partitions owned, every message consumed exactly once |
| FR-2.4 | Consumers NOT sharing a `Reference` each receive every message | Broadcast semantics verified |
| FR-2.5 | On becoming ACTIVE, and on every reconnect, a consumer resumes from the offset the broker has stored for its (reference, partition) — never from offset 0, and never from a local, in-memory, or last-dispatched value. The offset is resolved on every attach (create, promote, reconnect) and is never cached across one. Ordered paths use `RawConsumer` plus the library supervision loop, never `Reliable*` (whose built-in reconnect cannot be disabled) | Promotion scenario and reconnect scenario: total consumed == total published, no entity processed twice |
| FR-2.6 | Strict per-key ordering holds across SAC promotion | Every entity's sequence arrives in order on the promoting instance |
| FR-2.7 | An offset is stored only when it is greater than the highest value already stored for that (reference, partition). A candidate that is not greater is rejected locally — never sent — and reported. Only the ACTIVE consumer for a (reference, partition) stores an offset | A demoted or stale instance's store is rejected and counted in the metrics; a verification read shows the stored value never regresses |

### FR-3 Ordering

| ID | Requirement | Acceptance |
|---|---|---|
| FR-3.1 | Messages sharing a routing value are processed in publish order | Per-key sequence verified |
| FR-3.2 | The routing value is the **only** input to partition selection | Changing the value changes the partition; nothing else does |
| FR-3.3 | Partition count is fixed at declaration and MUST NOT be mutated afterwards | Post-creation resize attempt rejected |
| FR-3.4 | A hot key's throughput is bounded by one partition's throughput | Documented and measured; see NFR-4 |

### FR-4 Retry

| ID | Requirement | Acceptance |
|---|---|---|
| FR-4.1 | Retry happens **inside** the handler. Retrying outside the handler is forbidden | No implementation may wrap the handler externally |
| FR-4.2 | A throwing handler MUST NOT cause the message to be silently skipped | Ordered paths use `RawConsumer`; the library must not rely on the `Reliable` layer (which swallows handler exceptions) |
| FR-4.3 | `RetryPolicy` supports `MaxAttempts` (total attempts including the initial one; 1 = no retry; null = unbounded, `Strict` only), `BaseDelay`, `Multiplier`, `MaxDelay`, `Jitter`, a caller-supplied `DelayCalculator(Func<int attempt, Exception, TimeSpan>`, `attempt` is the 1-based retry number), and a `FailureMode`. When supplied, `DelayCalculator` overrides the exponential knobs. "Deadline" in `SkipAfterDeadline` means exhaustion of `MaxAttempts` (count-based; no separate wall-clock deadline in v1) | Each knob exercised by a test; attempt numbering and `MaxAttempts` semantics asserted |
| FR-4.4 | `FailureMode.Strict` (default): retry is unbounded (`MaxAttempts` MUST be null) and stalls the partition; the offset is NOT advanced until the handler succeeds. No error-stream management: `ErrorStream` MUST NOT be configured with `Strict` (rejected at configuration time) | Crash mid-retry redelivers the same offset; `Strict` + `ErrorStream` fails configuration |
| FR-4.5 | `FailureMode.SkipAfterDeadline`: `MaxAttempts` is required. Once the budget is exhausted, the offset advances exactly once and the message (original body plus all FR-5.1 fields) is published to the error stream, then considered completed. `ErrorStream` is REQUIRED with this mode. An optional `OnRetryExhausted` hook may observe the handoff but cannot change the outcome | Offset advances exactly once; message appears in the error stream; missing `ErrorStream` fails configuration |
| FR-4.6 | The failure mode is settable **per consumer**, not globally | Two consumers on different streams with different modes coexist |
| FR-4.7 | Retrying in-handler preserves ordering under concurrent load | **Gate G-1.** Untested. Must pass before the retry phase is complete |

### FR-5 Error stream

| ID | Requirement | Acceptance |
|---|---|---|
| FR-5.1 | A failed message (`SkipAfterDeadline` only) is published to an error stream preserving the original body and carrying original stream, partition, offset, exception type/message/stack, attempt count, timestamps, correlation/message id, and original routing value | All fields plus original body present on a captured error message |
| FR-5.2 | The error stream MUST declare `x-max-age` retention. Retention is part of the design, not an option | Retention present in the declared spec |
| FR-5.3 | Replay reads the error stream and republishes to the source, deriving the publishing id from `(partition, original offset)` | Replaying the same error message twice yields one stored message |
| FR-5.4 | One shared error stream with routing and per-stream error streams are both supported | Both verified |

### FR-6 Effectively-once read-process-write (design §7)

| ID | Requirement | Acceptance |
|---|---|---|
| FR-6.1 | The result's publishing id is derived from the **source offset**, enforced by `PublishResultAsync` (`ConsumeContext` has no publish method) | Publishing id equals source offset |
| FR-6.2 | Exactly one write per processing step; publishing to the target is the only side effect | Verified by inspection and test |
| FR-6.3 | Only one instance publishes under a given producer name. Enforced by SAC on the source. A super-stream source needs one stable producer name per (source partition, target stream), not one shared name | Two instances under one name: no dedup corruption |
| FR-6.4 | Producer names are stable, unique per (source partition, target stream), meaningful (not a GUID), and within the 255-per-stream limit | Name set inspected; count enforced |
| FR-6.5 | Results are published in order with none skipped. Keep retrying; never time out silently | No gaps in the result sequence |
| FR-6.6 | For a super stream target, the last publishing id is queried on **every** partition and the maximum is used as the resume point | Resume from max across all partitions verified |
| FR-6.7 | No automatic offset tracking exists on the `Raw*` effectively-once path: the source consumer stores nothing and the start offset is computed from the last publishing id on every attach instead | No offset override observed |
| FR-6.8 | The full read-process-write cycle is correct end to end | **Gate G-2.** Unverified. Must pass before this phase is complete |

### FR-7 Topology

| ID | Requirement | Acceptance |
|---|---|---|
| FR-7.1 | `EnsureStreamAsync` / `EnsureSuperStreamAsync` are idempotent and cached | Repeat calls are no-ops |
| FR-7.2 | Concurrent declaration of the same stream name is serialized in-process and tolerated cross-process: a benign `StreamAlreadyExists` / `PreconditionFailed` race with a matching spec succeeds and caches; a genuine spec conflict throws `StreamConfigurationConflictException` naming expected vs actual | 4+ concurrent calls on one name (including simultaneous multi-instance startup with the same spec) all succeed |
| FR-7.3 | `x-queue-leader-locator` defaults to `least-leaders` | Declared spec shows `least-leaders` |
| FR-7.4 | Any production-tuned setting uses a policy, not a queue argument | Documented; policies preferred |
| FR-7.5 | Stream specs are treated as startup configuration | Declaration happens at startup, not per publish |

### FR-8 Connection and readiness

| ID | Requirement | Acceptance |
|---|---|---|
| FR-8.1 | A `StreamSystem` binds to exactly one vhost. There is no per-producer or per-consumer vhost override. Buses are .NET marker types (`IStreamBus<TBus>`): the same `TBus` MUST NOT be registered twice (duplicate `AddStreamBus<SameTBus>` fails at configuration time); one `StreamSystem` per vhost is shared by all `TBus` bound to it | API offers no override; duplicate bus-type registration fails fast |
| FR-8.2 | One bus instance serves many streams | Two streams, one bus, both confirmed |
| FR-8.3 | The initial connect is supervised and retries forever with exponential backoff plus jitter, capped | Broker down at startup, up later → connects, no unhandled error surfaced |
| FR-8.4 | `ProtocolException`, `SslException`, `AuthMechanismNotSupportedException`, and authentication failures (any `AuthenticationFailure*`, including the `guest`/loopback rejection) are fatal and MUST NOT be retried | Fatal error surfaces promptly, not after backoff exhaustion |
| FR-8.5 | Network errors are retried indefinitely | Sustained outage does not terminate the host process |
| FR-8.6 | `ReadinessPolicy` is selectable **per call site**: `FailFast`, `WaitUpTo(TimeSpan)`, `WaitIndefinitely`. `PublishOptions.Timeout` is the confirm timeout only; `WaitUpTo` bounds only the readiness wait | Three policies verified at a call site; readiness wait and confirm timeout asserted independently |
| FR-8.7 | `WaitUntilReadyAsync(policy)` returns `Ready`, `TimedOut`, or `Fatal` | All three outcomes reachable |
| FR-8.8 | A health endpoint reports current state per (vhost, bus type) | Endpoint returns per-(vhost, bus type) state |
| FR-8.9 | Credentials are validated at startup with an actionable message. The `guest`/loopback trap is detected explicitly | Using `guest` fails at startup with a message naming the cause, not later as a confusing producer error |
| FR-8.10 | Pooling defaults are 1 producer and 1 consumer per connection | Defaults asserted |
| FR-8.11 | Startup blocking is user-configurable MassTransit `HostOptions`-style: `BlockStartupUntilReady = false` (default), `StartupTimeout`. When false, `StartAsync` returns without waiting while the supervising loop keeps retrying. When true, startup waits for every registered bus to report `Ready`, bounded by `StartupTimeout`; on timeout `StartAsync` surfaces `TimedOut` while the background loop keeps retrying | Default (non-blocking) and blocking-with-timeout behaviours both verified |

### FR-9 Observability

| ID | Requirement | Acceptance |
|---|---|---|
| FR-9.1 | Per-partition metrics are exposed: lag, in-flight, retry-blocked | Metrics visible per partition |
| FR-9.2 | Connection state transitions are observable so readiness probes work | Transitions emitted and reflected in health |
| FR-9.3 | Hot-key starvation is detectable from the metrics | A saturated key is visible as retry-blocked / elevated lag on one partition |

### FR-10 Offset tracking

Offsets are the broker's record of how far a consumer group has progressed, addressed by
`(reference, stream)`. For a super stream the second half is the **partition** stream, so
each partition carries its own stored offset. The library reads that record on every attach
and writes it only after a successful message. Callers configure behaviour; they do not
touch the mechanics.

| ID | Requirement | Acceptance |
|---|---|---|
| FR-10.1 | A consumer's offset behaviour is one `ConsumeOffset` value: `ResumeOrFirst` (default), `ResumeOrNext`, `ResumeOrFail`, `AlwaysFirst`, `AlwaysNext`. Nothing else is needed to get the correct default | `ResumeOrFirst` yields broker-resume with no other configuration |
| FR-10.2 | `ResumeOrFirst`, `ResumeOrNext` and `ResumeOrFail` all resolve the resume point from the broker-stored offset for the consumer's (reference, partition) | Broker value observed on create, on promote and on reconnect |
| FR-10.3 | The three differ only in the no-stored-offset case: `First` replays from the head, `Next` attaches at the tail, `Fail` throws a configuration error naming the reference and the partition | All three branches exercised, including the fallback |
| FR-10.4 | The broker read happens on consumer creation, on SAC promotion and on reconnect via the library supervision loop around `RawConsumer`. It is never cached across an attach, and never performed on the hot path | Reconnect during a `Strict` retry resumes from the stored offset, not from a last-dispatched value |
| FR-10.5 | The resolved offset is `stored + 1`, or the no-stored-offset branch. The library never produces `OffsetTypeNext` implicitly; the only `Next` path is an explicit `AlwaysNext` | No code path returns `OffsetTypeNext` implicitly |
| FR-10.6 | The stored offset never exceeds the last successfully processed offset of that (reference, partition) | A handler that throws does not advance the stored offset |
| FR-10.7 | Monotonicity: an offset is stored only when greater than the highest value already written for that (reference, partition), per FR-2.7 | A lower candidate is rejected locally, counted, and never sent to the broker |
| FR-10.8 | Store cadence is configurable (`OffsetStoreOptions.EveryMessages`, `MaxInterval`) and defaults to storing after every successful message (`EveryMessages = 1`, `MaxInterval = null`). With `EveryMessages = N > 1` a crash redelivers up to N-1 messages; with `MaxInterval` set a timer stores the last-success offset (still monotonicity-checked, fire-and-forget) | Default and both options tested, including the redelivery window |
| FR-10.9 | Stores go through the per-partition consumer obtained inside the message handler. `RawSuperStreamConsumer.StoreOffset` throws `NotImplementedException`, so the super-stream entity is never a store path. `ConsumeContext` has no publish method; plain paths just return and the library stores the offset, while only the FR-6 result path publishes via `PublishResultAsync` | No call to the super-stream store path exists |
| FR-10.10 | `Reference` is optional and auto-derived by default. The derivation is deterministic from (bus marker type, consumer/message type, stream), stable across restarts, unique per consumer group — never a GUID/random, and never shared between two consumer types on one stream. A caller-supplied value MUST also be stable, unique per group, and is validated per FR-10.11 | Omitted reference derives to a stable unique value; two consumer types on one stream get distinct references; the value survives a restart |
| FR-10.11 | `Reference` is validated against the broker's 255-byte limit at configuration time with an actionable message | An over-long reference fails in our configuration error, not as a broker `PreconditionFailed` |
| FR-10.12 | Whenever the library falls back because the broker has no stored offset, it logs a warning naming the reference, the partition, the branch taken and the resolved offset, and publishes it to the metrics and health surface | The fallback is observable. It is never silent |
| FR-10.13 | `GetConsumerOffsetAsync(reference, partition)` returns `ulong?` (null when nothing is stored) and `SetConsumerOffsetAsync(reference, partition, offset)` seeds it, where `partition` is the 0-based partition index for super streams (0 for plain streams). Neither is used internally on the per-message path; `AlwaysFirst`/`AlwaysNext` remain explicit operator escapes for replay/tail-attach | Escape hatches exist and are not on the hot path |
| FR-10.14 | A consumer used for effectively-once read-process-write stores no offset on its source: the publishing ID is the resume point (FR-6.1, FR-6.7) | No store is observed on the effectively-once source reference |

## 2. Non-functional requirements

| ID | Requirement |
|---|---|
| NFR-1 | Ordering is the primary guarantee. A correct-but-slow result beats a fast-and-wrong one |
| NFR-2 | No database, no external scheduler, no MassTransit dependency |
| NFR-3 | The library owns offset-on-promotion and on-reconnect (FR-2.5). Official repo examples show `OffsetTypeFirst()`, which replays from 0. The client's own fallback when the promotion listener throws or returns null is `OffsetTypeNext`, which attaches at the tail and skips the partition's entire backlog — the more dangerous of the two. Callers must not be able to opt into either implicitly; explicit `AlwaysFirst`/`AlwaysNext` escapes stay available per consumer |
| NFR-4 | Hot-key starvation is documented, not auto-solved. See §4 |
| NFR-5 | Leader locator and replica placement follow the client's automatic behavior; the library MUST NOT override it |
| NFR-6 | Streams have no TTL, no DLX, no per-message priority. Any design implying otherwise is wrong |
| NFR-7 | Consumer offsets are the broker's record. The library reads them on every attach, writes only forward, and never infers a resume point from local state |

## 3. Acceptance summary

v1 is done when all FR-1 through FR-10 pass against a real broker, and gates
G-1 through G-5 (§5) pass.

## 4. Hot-key starvation (NFR-4)

One key can saturate its partition while other partitions idle. Partition count
is fixed at declaration (FR-3.3) and cannot be migrated, so there is no
automatic remedy, by design.

Required behaviour:

- Document the ceiling: one partition's throughput is the ceiling for one key;
  a hot key cannot be split further.
- Expose metrics (FR-9.1, FR-9.3) so the condition is observable.
- Documented escape hatch: a caller who cannot tolerate the ceiling supplies a
  sub-partitioning routing value, and accepts that ordering then holds only per
  sub-key.

Size partitions somewhat above the expected instance count and accept uneven
shares. SAC distribution is not guaranteed even when consumers register
simultaneously: partitions are initially handed to the first registrant and
redistributed as others appear.

## 5. Gates (unverified, must pass before the phase is complete)

These are **not** assumed to work. Each is a release blocker for its phase.

| ID | Gate | Blocks | Why |
|---|---|---|---|
| G-1 | In-handler retry preserves ordering under concurrent load and mid-stream SAC failover | FR-4.7, phase 6 | Untested. The design §4.4 measurement only covered the no-retry case |
| G-2 | Effectively-once read-process-write is correct end to end | FR-6.8, phase 7 | Design §7 is unverified end to end |
| G-3 | `x-queue-leader-locator` and replica placement behave as documented on a 3-node cluster | FR-7.3, NFR-5, phase 8 | All measurements so far were on a single node, where both are no-ops |
| G-4 | Mid-stream SAC failover preserves offsets and ordering | FR-2.5, FR-2.6, phase 5 | Only same-broker promotion was measured |
| G-5 | A reconnect that lands while a `Strict` retry is pending does not skip the unprocessed message, and a stale instance cannot regress a stored offset | FR-2.5, FR-2.7, FR-10.4, FR-10.7, phase 5 | Ordered paths use `RawConsumer` plus the library supervision loop (`Reliable*` is not used because its built-in reconnect cannot be disabled); the broker's `store_offset` accepts any value without responding |

**G-3 is an environment requirement: a 3-node cluster is needed before any claim
that leader placement works.** Do not assert those behaviours from single-node
evidence.

## 6. Hosting

`services.AddStreamBus<TBus>(vhost, cfg)` with `IHealthCheck` and `IHostedService`
integration. The same `TBus` marker type MUST NOT be registered twice. `IStreamBusRegistry`
is keyed by (bus type, vhost):

```
GetOrCreateBus<TBus>(vhost) -> IStreamBus<TBus>   (wrapper cached per (TBus, vhost))
```

`StreamBusHostOptions { BlockStartupUntilReady = false (default), StartupTimeout }`
controls whether `StartAsync` waits for every registered bus (MassTransit
`HostOptions`-style). A `StreamSystem` owns a locator connection plus producer and
consumer connection pools. Callers never see it.
