# Topology

## Stream specifications live on registrations

Startup topology is registration-owned: every stream's broker spec is
carried by the registrations that reference that stream - the publish
registration, consuming endpoints, and dead-letter stream destinations.
A registration without a spec counts: it references the stream but sets
no knobs, and the stream is declared with defaults. Runtime/operator
declarations use `bus.Topology` instead of a `cfg` declare call; see
[Explicit declarations](#explicit-declarations).

Plain (non-super) references:

```csharp
cfg.Publish<InvoiceEvent>("invoices", s =>
{
    s.MaxAge(TimeSpan.FromDays(30));
    s.MaxLengthBytes(1_000_000_000);
    s.MaxSegmentSizeBytes(16_000_000);
    s.LeaderLocator(StreamLeaderLocator.LeastLeaders);
});

// Bare form: reference "audit" with broker defaults.
cfg.Publish<AuditEvent>("audit");
```

Super references add partitions:

```csharp
cfg.PublishSuper<OrdersEvent>("orders-super", s => s.Partitions(3));
```

`Partitions` exists only on the super spec builder, so a plain stream
with partitions is not expressible. Setters return void by design: one
knob per statement, no fluent chaining.

```csharp
s.Partitions(3).MaxAge(TimeSpan.FromDays(7)); // does not compile
```

## Explicit declarations

`bus.Topology` declares streams and super streams idempotently at
runtime. Repeated calls are cached; kind conflicts and super-stream
partition mismatches throw `StreamConfigurationConflictException`.

```csharp
await bus.Topology.EnsureStreamAsync(
    new StreamDeclaration { Name = "audit", MaxAge = TimeSpan.FromDays(7) }, ct);

await bus.Topology.EnsureSuperStreamAsync(
    new SuperStreamDeclaration { Name = "orders-super", Partitions = 3 }, ct);
```

Startup declarations still come from registrations; this API is for
operator-driven or post-startup declarations. An existing plain stream
is adopted with a warning because argument detail is not readable via
the stream protocol (see below).

## Derived topology and agreement rules

The startup declare-set is the union of all registrations of one bus:
consuming endpoints, publish registrations, and stream dead-letter
destinations. Multiple references to the same stream must agree:

- **Kind** (topology-shaping, one choice): super vs plain must agree
  across every reference. Conflicts throw
  `StreamConfigurationConflictException` naming the stream and both
  disagreeing references.
- **Every knob** (per-knob agreement): where two references set the same
  knob, the values must match; where only one sets it, its value fills
  the hole. A second reference that silently disagrees instead of being
  dropped is the rule — the conflict names both values.
- A `PublishSuper` registration whose stream resolves no partition
  count anywhere (its own spec or a consuming endpoint's
  `Partitions(n)`) fails at startup naming the message type and the
  stream.
- Publish kind must match the merged kind (`Publish`↔plain,
  `PublishSuper`↔super); mismatches fail naming the message type and
  the stream.

## Declare semantics

Registration-owned streams are create-or-verify at startup. A
conflicting detected pre-existing spec throws
`StreamConfigurationConflictException`. Plain-stream argument drift is
not readable via the stream protocol, so an existing plain stream is
adopted with a warning; keep specs in sync across services.

Dead-letter **stream** destinations are declared by the service that
owns the endpoint (knobs from their `StreamOptions`); queue destinations
are declared by the AMQP transport, never by topology.

## Broker adopt rule

A consuming endpoint adopts broker reality instead of assuming it owns
topology the broker owns:

- broker has the expected kind → consumed directly (for super streams
  the broker-reported partition count rules);
- broker has the other kind → `StreamConfigurationConflictException`;
- broker has nothing → the endpoint's registration declares it.

```csharp
// This service only consumes: it adopts "orders" if another service
// registered it; if nothing exists yet, this service's registration
// declares it.
cfg.ReceiveEndpoint("orders", e => e.Consumer<OrderConsumer>("order-processors"));
```

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
