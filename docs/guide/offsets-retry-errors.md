# Offsets, Retry & Errors

> Cursors (`ConsumeOffset` × 5, references), ordering, retry modes, offset-store
> cadence, error streams, replay, and the exhaustion hook.
> Source: `OffsetResolution.cs`, `ConsumeOptions.cs`, `RetryPolicy.cs`,
> `StreamPartitionConsumer.cs`, `ErrorEnvelope.cs`, `ErrorPublisher.cs`,
> `ConsumerRegistry.cs:403-550`.

## 1. `ConsumeOffset`: the 5 start positions

`OffsetResolution.ResolveOffset` (`OffsetResolution.cs:9-46`). `stored` = broker
offset for (reference, partition); resume = `stored + 1`.

| Mode | Stored exists | No stored offset | Use when |
|---|---|---|---|
| `ResumeOrFirst` (default) | resume at `stored+1` | start at `First`, warn + fallback metric | Normal services — first deploy reads from beginning, restarts resume. |
| `ResumeOrNext` | resume at `stored+1` | start at `Next` (skip backlog), warn + metric | New consumer that must not replay history. |
| `ResumeOrFail` | resume at `stored+1` | **throw** `StreamBusConfigurationException` | Safety interlock: refuse to start without a cursor (pre-checked on all partitions). |
| `AlwaysFirst` | ignored → `First` | ignored → `First` | Operator replay / reprocessing. Escape hatch, not a default. |
| `AlwaysNext` | ignored → `Next` | ignored → `Next` | Tail-only (metrics/logs). Note: SAC is auto-disabled for `Next` (tail attach under SAC receives nothing). |

Two subtleties:

- If the local high-water exceeds broker-stored (e.g. after an operator rewind),
  the **broker wins**: warn + replay from `stored+1` (`OffsetResolution.cs:41-45`).
- A failed offset query degrades to the no-stored branch (warn + fallback metric),
  so `ResumeOrNext` + transient query failure skips — prefer `ResumeOrFirst` for
  must-not-lose consumers.

## 2. References (consumer groups)

- Explicit: `.Reference("billing-projection")` / `new ConsumeOptions { Reference = ... }`.
- Derived when omitted: raw → `streambus:<BusFullName>:<stream>`; typed →
  `streambus:<BusFullName>:<ConsumerFullName>:<stream>`; hashed `streambus-<32hex>`
  fallback over 255 bytes (`ReferenceDerivation.cs:10-31`).
- Limit 255 UTF-8 bytes, validated (`ReferenceDerivation.Validate`).
- Promotion (single-active-consumer failover) re-queries the broker and resumes
  at `stored+1` — never `First()` (avoids the replay trap); query failure falls
  back to `First` with a warn + metric (`StreamPartitionConsumer.cs:901-947`).

## 3. Ordering: Strict

Only `ConsumeOrdering.Strict` exists. One partition = one in-flight message
(`_dispatchGate(1,1)`). A throwing handler **stalls that partition** through
backoff; other partitions continue. There is no skip-on-failure except the
bounded mode below — order is never sacrificed silently.

## 4. Retry: two modes

`RetryPolicy` (`RetryPolicy.cs:12-20`):

```csharp
new RetryPolicy
{
    FailureMode = FailureMode.Strict,          // or SkipAfterDeadline
    MaxAttempts = null,                        // Strict: must stay null (unbounded)
    BaseDelay = TimeSpan.FromMilliseconds(200),
    Multiplier = 2.0,
    MaxDelay = TimeSpan.FromSeconds(30),
    MaxJitter = TimeSpan.FromSeconds(1),
    DelayCalculator = null,                    // (attempt /*1-based*/, ex) => TimeSpan
}
```

| Mode | Attempts | Delay | On exhausted | Error stream? |
|---|---|---|---|---|
| `Strict` (default) | unbounded (`MaxAttempts` must be `null`) | exp-backoff `BaseDelay×Multiplier^attempt` capped at `MaxDelay` + jitter; or `DelayCalculator(attempt, ex)` (throw → warn + fallback; negative → clamp 0 + warn) | never — retries forever | **forbidden** (config error) |
| `SkipAfterDeadline` | bounded (`MaxAttempts >= 1` **required**) | same schedule | hand off to error stream, **then** advance past the message exactly once | **required**, must exist, must be **plain** |

Retry runs **inside the handler slot** (partition stalled, order preserved);
warnings logged for attempts ≤ 5 then every 100th. Cancellation during backoff
aborts quietly (shutdown path).

## 5. Offset-store cadence

`OffsetStoreOptions { EveryMessages = 1, MaxInterval = null }`
(`ConsumeOptions.cs:17-21`):

- Store broker offset every N successful messages and/or at most every interval.
  Default = every message (safest, most store RPCs).
- Stores are **monotonic** (`MonotonicOffsetGuard`): rewinds rejected + counted
  (`streambus_offset_store_rejected_total`); regressions re-queried and warned.
- Store failures warn and retry on the next success (cadence not reset) — never
  skip silently. Timer (when `MaxInterval` set) stores the last success if
  unst​ored.
- Effectively-once consumers **store nothing** on the source (resume comes from
  the target) — the lag gauge is meaningless there by design.

Tune: high-throughput idempotent consumers → `EveryMessages = 100` +
`MaxInterval = TimeSpan.FromSeconds(5)` (fewer RPCs, ≤ 100-message replay after
crash). Must-not-replay → keep `1`.

## 6. Error stream (dead-letter with full context)

Only `SkipAfterDeadline` publishes here, via `ErrorPublisher.HandOffAsync`:

1. Build `ErrorEnvelope` (`ErrorEnvelope.cs:5-40`): source
   (stream/partition/partition-stream/offset), body copy, exception
   (type/message/stack), attempts, timestamp, message ids, routing values.
   Wire form = `streambus.error.*` application properties.
2. Retry confirmed publish to the error stream **until landed/cancelled**, then
   commit the source offset exactly once. Commit failure recycles the attach
   (redelivery) — never advances past an unconfirmed handoff.
3. Await `OnRetryExhausted` hook (`ErrorHandoff { ErrorStream, Source*,
   Attempts, FailedAt, Exception }`): observe-only; abandon on shutdown; throw →
   warn; outcome unchanged.

Declare the error stream at startup **with retention** (it is a real stream):

```csharp
o.Topology(t => t
    .DeclareStream("orders")
    .DeclareStream("orders-errors", k => k.MaxAge(TimeSpan.FromDays(14))));
```

## 7. Replay: reprocess the error stream

```csharp
int replayed = await bus.ReplayErrorStreamAsync("orders-errors", targetStream: "orders", ct);
// targetStream null = back to the recorded source stream.
```

- Plain error streams only; private replay reference; `AlwaysFirst` scan.
- Idles out after 5s with no delivery and no in-flight (200ms poll).
- Skips malformed/vanished-partition entries with warn + count; republishes with
  `publishingId = sourceOffset`; returns the replayed count
  (`ConsumerRegistry.cs:429-550`).

## 8. Minimal resilient consumer (recipe)

```csharp
services.AddStreamBusConsumer<OrderEvent, OrderConsumer>("orders", o => o
    .Offset(ConsumeOffset.ResumeOrFirst)
    .OffsetStore(new OffsetStoreOptions { EveryMessages = 50, MaxInterval = TimeSpan.FromSeconds(5) })
    .Retry(new RetryPolicy
    {
        FailureMode = FailureMode.SkipAfterDeadline,
        MaxAttempts = 5,
        BaseDelay = TimeSpan.FromMilliseconds(200),
        MaxDelay = TimeSpan.FromSeconds(10),
    })
    .ErrorStream(new ErrorStreamOptions { Stream = "orders-errors" })
    .OnRetryExhausted(h =>
    {
        Console.WriteLine($"Exhausted {h.SourceStream}@{h.SourceOffset} after {h.Attempts}: {h.Exception.Message}");
        return Task.CompletedTask;
    }));
```
