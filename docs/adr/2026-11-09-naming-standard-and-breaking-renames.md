# ADR-2026-11-09: Library Naming Standard and Breaking Renames

## Context

A naming audit found properties that described implementation mechanics
(`BlockStartupUntilReady`), private metaphors (`family`), or nothing at all
(`Reference`, `EveryMessages`) instead of the user-visible concept. Names for
the same concept diverged across files (`IsSuper` vs `IsSuperStream`,
`RoutingValue` vs `RoutingKey`, `ErrorHandoffTimeout` vs
`HandoffConfirmTimeout`), and conventions did not match the .NET bus
ecosystem (MassTransit: `WaitUntilStarted`) or RabbitMQ's own vocabulary
(offset specification, consumer reference).

We are pre-1.0 with no external consumers; breaking changes are acceptable
and no obsolete shims are kept.

## Decision

### Naming standard

1. **Booleans** are verb-first behavior phrases (`WaitUntil...`, `Use...`) or
   positive predicates (`Is...`, `Has...`); never mechanism words (`Block`).
2. **One concept = one word** everywhere (option, error messages, metric
   tags, internal identifiers).
3. **Properties never re-type themselves**: `ReadinessPolicy Readiness`,
   not `Readiness ReadinessPolicy`.
4. **Static instances are bare nouns**: `ReadinessPolicy.Default`, not
   `...FailFastPolicy`.
5. **Container provides the subject**: inside `OffsetStoreOptions`, members
   name the action (`StoreEveryMessages`, `MaxFlushInterval`).
6. **TimeSpans end in** `Timeout` / `Interval` / `Delay` / `Age`; thresholds
   use `*Limit` / `Max*`; counts use `*PerX`.
7. **Transport-level knobs copy RabbitMQ vocabulary** (`Vhost`, `Heartbeat`,
   `SingleActiveConsumer`); framework-level knobs copy the bus ecosystem
   (`WaitUntilStarted` ← MassTransit `MassTransitHostOptions`).

### Applied renames

| Old | New |
| --- | --- |
| `BlockStartupUntilReady` / `StartupTimeout` | `WaitUntilStarted` / `StartTimeout` |
| `BlockUntilConsumersAttached` | `WaitUntilConsumersAttached` |
| `ReadinessPolicy` (property) | `Readiness` |
| `ReadinessPolicy.FailFastPolicy` / `.Indefinitely` | `ReadinessPolicy.Default` / `.Forever` |
| `ReadinessPolicy` type members `WaitUpTo`/`WaitIndefinitely` | unchanged (pattern-matched records) |
| `Reference` (consumer-group identity) | `ConsumerGroup` (broker docs still call it the consumer *reference*; the XML doc notes it is used verbatim as the broker reference) |
| `ConsumeOffset` type, `ConsumeOptions.Offset`, endpoint builder `Offset(...)` | `StartOffset` (matches RabbitMQ's *offset specification*) |
| `PublishOptions.Timeout` | `ConfirmTimeout`, now `TimeSpan?` — **null waits indefinitely** (was always 10 s) |
| `HandoffConfirmTimeout` / endpoint `ErrorHandoffTimeout(...)` | `ErrorPublishTimeout`, now `TimeSpan?` — **null waits indefinitely** (was always 10 s) |
| `OffsetStoreOptions.EveryMessages` / `MaxInterval` | `StoreEveryMessages` / `MaxFlushInterval` |
| `IsSuper` | `IsSuperStream` |
| `RoutingValue` | `RoutingKey` |
| `BusMarker` | `BusType` |
| `HeartbeatSeconds` (double, hosting view) | `Heartbeat` (`TimeSpan`) |
| `DeadLetterOptions.Spec` (and internal tuples) | `StreamSpec` |
| `SuperPartitions` (descriptor) | `Partitions` |
| `family` (filter-support params) | `pipeline` |
| `ConsumerReferenceValidation` / `MaxReferenceBytes` | `ConsumerGroupValidation` / `MaxConsumerGroupBytes` |
| Metric properties `StoreRejected`/`Regressed`/`Fallback` | `OffsetStoreRejected`/`OffsetRegressed`/`OffsetFallback` (instrument names unchanged) |
| Metric partition tag `reference` | `group` |

Defaults changed deliberately with user approval: both publish confirm
timeouts now default to **null = wait indefinitely**, with cancellation (the
partition loop token / app token) as the only bound. Validations keep
rejecting non-positive values when a timeout is supplied.

## Consequences

### Positive
- The public API reads as sentences and matches ecosystem vocabulary.
- Every concept has exactly one name across API, errors, and metrics.
- Timeout semantics are now uniform (`null` = infinite) for both publish
  paths.

### Negative
- Breaking, with no obsolete shims (accepted; pre-1.0).
- Dashboards keyed on the old `reference` metric tag must switch to `group`.
- Infinite-by-default timeouts can hang a publish on a dead broker if
  callers pass `CancellationToken.None`; documented as the caller's
  responsibility.

### Alternatives considered
- `[Obsolete]` alias shims — rejected: no external users, clutter.
- `WaitForConsumersOnStartup`-style names — rejected in favor of
  MassTransit's `WaitUntilStarted` shape after checking ecosystem naming.
- Renaming `ReadinessPolicy` static to `FailFast` — impossible: collides
  with the nested `FailFast` record type (CS0102); `Default` chosen.

## Status
Accepted

## Date
2026-11-09
