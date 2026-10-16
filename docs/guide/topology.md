# Topology

## Explicit declarations

```csharp
cfg.DeclareStream("audit", k => k
    .MaxAge(TimeSpan.FromDays(30))
    .MaxLengthBytes(1_000_000_000)
    .MaxSegmentSizeBytes(16_000_000)
    .LeaderLocator(StreamLeaderLocator.LeastLeaders));

cfg.DeclareSuperStream("orders-super", partitions: 3);
```

Declarations are created or verified at startup (a conflicting
pre-existing spec throws `StreamConfigurationConflictException`).
Plain-stream argument drift is not readable via the stream protocol, so
keep declarations in sync across processes.

## Derived topology

The startup declare-set is the union of explicit declarations, endpoint
streams, and publish registrations:

- Plain endpoints and plain publishes without an explicit declaration
  are declared automatically.
- Super endpoints carry their partitions (`SetSuperStream(n)`) and are
  declared with them.
- A `PublishSuper` registration needs partitions from a matching
  endpoint or an explicit `DeclareSuperStream` — otherwise startup
  fails naming the stream.
- Explicit declarations win: a derived entry for the same name is
  skipped, and a kind mismatch between derived and explicit fails with
  a conflict naming both sources.
- Publish registration kind must agree with the declaration
  (`Publish`↔plain, `PublishSuper`↔super); mismatches fail naming the
  message type and the stream.

## Broker adopt rule

An endpoint stream with an explicit declaration in the same service is
declared and verified. Without one, the endpoint adopts broker reality
instead of creating:

- broker has the expected kind → consumed directly, read-only (no
  declare call; broker-reported partitions rule for super streams);
- broker has the other kind → `StreamConfigurationConflictException`;
- broker has nothing → a declarable stream is declared, otherwise a
  configuration error names the stream and the owning service.

```csharp
// This service only consumes: no DeclareStream here. If another service
// owns "orders", this endpoint adopts it; if nobody created it yet,
// startup fails naming "orders" instead of silently creating it.
cfg.ReceiveEndpoint("orders", e => e.Consumer<OrderConsumer>("order-processors"));
```

Error-stream dead-letter destinations are never derived — they must be
explicitly declared. Queue destinations are declared by the AMQP
transport, never by topology.

## Partition reads

Reading one super-stream partition is a plain consume of its physical
name (`{super}-{index}`), with no declaration ever:

```csharp
await using var sub = await bus.ConsumeAsync(
    "orders-super-0",
    new ConsumeOptions { Reference = "partition-inspector" },
    handler,
    ct);
```
