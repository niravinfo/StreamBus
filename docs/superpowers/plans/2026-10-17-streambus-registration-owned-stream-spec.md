# Spec: Registration-owned stream specs (delete Declare*, delete section binding)

Date: 2026-10-17
Status: approved — all decisions settled (2026-10-18)
Supersedes (in part): the explicit-declaration rules of
`2026-10-13-streambus-phase8-topology.md` and the `IConfigurationSection`
overload reserved in `2026-10-13-streambus-v2-user-centric-api.md`

## Context

The phase-8 declare-set already derives topology from registrations
(`DeclareSetBuilder.Build`): plain endpoints, super endpoints with
`SetSuperStream(n)`, and plain `Publish<T>(stream)` are auto-declared;
the broker adopt rule handles cross-service ownership; explicit
declarations win over derived and conflict on kind mismatch. The
explicit `DeclareStream`/`DeclareSuperStream` APIs exist only as a
fallback for two value holes:

1. `PublishSuper<T>("orders-super")` with no consuming endpoint in the
   process — partition count is unknowable, so startup fails unless an
   explicit `DeclareSuperStream` supplies it.
2. Stream knobs (`MaxAge`, `MaxLengthBytes`, `MaxSegmentSizeBytes`,
   `LeaderLocator`) on a publish-only stream — a derived plain publish
   carries no knobs.

Both are value holes, not missing concepts. The fix is to let the
registration that references a stream carry its full spec, then delete
the explicit-declaration API. Separately, `AddStreamBus(IConfigurationSection, …)`
+ `ConfigurationBinding.cs` re-implement parsing/defaults for knobs the
configurator already validates — two sources of truth for defaults and
errors. Configuration is the user's responsibility: no API reads
configuration automatically, so all validation stays in one place.

## Decisions (confirmed)

- Stream spec is expressed **on the publish registration** via a builder
  callback; an overload accepting a whole options object also exists.
- `DeclareStream` / `DeclareSuperStream` are **deleted** from the public
  API. The derive/adopt/conflict machinery in `DeclareSetBuilder` and
  `Hosting.DeclareDerivedUntilReadyAsync` is unchanged in behavior.
- The `IConfigurationSection` overloads and `ConfigurationBinding.cs`
  are **deleted**. Users configure in code; they may hand-write their
  own binding into the configurator if they want config-file values.
- Endpoint `Set*` setters are **renamed to builder style**: `Partitions`,
  `MaxAge`, `MaxLengthBytes`, `MaxSegmentSizeBytes`, `LeaderLocator`.
  `Partitions(n)` keeps `SetSuperStream(n)` semantics — it sets the
  super flag and partitions in one call; `SuperStream(bool)` stays as
  the flag toggle.
- Dead-letter destinations are **kind-split**: `DeadLetterToStream` /
  `DeadLetterToQueue` with a new `DeadLetterQueueType` enum; the
  3-value `DeadLetterType` enum is deleted (invalid kind/spec
  combinations are unrepresentable).
- Per-call `PublishOptions` (ReadinessPolicy, Timeout, RoutingKey,
  IsSuperStream) stays per-call runtime behavior — untouched.

## API surface (after)

```csharp
public interface IStreamBusConfigurator
{
    void Host(string host, string vhost, Action<IHostConfigurator> configure);
    void Serializer(IStreamSerializer serializer);
    void Serializer(Action<JsonSerializerOptions> configure);
    void Publish<TMessage>(string stream, Action<IStreamSpecBuilder>? configure = null);
    void Publish<TMessage>(string stream, StreamSpec spec);
    void PublishSuper<TMessage>(string stream, Action<ISuperStreamSpecBuilder> configure);
    void PublishSuper<TMessage>(string stream, StreamSpec spec);
    void ReceiveEndpoint(string stream, Action<IReceiveEndpointConfigurator> configure);
    void UseMessageRetry(Action<IRetryConfigurator> configure);
    void UseRetry(RetryPolicy policy);
    // DeclareStream / DeclareSuperStream — deleted
}

public interface IStreamSpecBuilder            // knobs, no partitions
{
    void MaxAge(TimeSpan maxAge);
    void MaxLengthBytes(ulong maxLengthBytes);
    void MaxSegmentSizeBytes(int maxSegmentSizeBytes);
    void LeaderLocator(StreamLeaderLocator locator);
}

public interface ISuperStreamSpecBuilder : IStreamSpecBuilder
{
    void Partitions(int partitions);           // required on the super path
}

public sealed record StreamSpec                // whole-object overload + carried in PublishRegistration
{
    public int? Partitions { get; init; }
    public TimeSpan? MaxAge { get; init; }
    public ulong? MaxLengthBytes { get; init; }
    public int? MaxSegmentSizeBytes { get; init; }
    public StreamLeaderLocator? LeaderLocator { get; init; }
}
```

```csharp
public interface IReceiveEndpointConfigurator
{
    void Consumer<TConsumer>(string consumerGroup) where TConsumer : class;
    void SuperStream(bool enabled = true);
    void SingleActiveConsumer(bool enabled = true);
    void Offset(ConsumeOffset offset);
    void OffsetStore(OffsetStoreOptions options);
    void Readiness(ReadinessPolicy policy);
    void UseMessageRetry(Action<IRetryConfigurator> configure);
    void UseRetry(RetryPolicy policy);
    void OnExhausted(Func<ExhaustedContext, CancellationToken, Task> handler);

    void Partitions(int partitions);                     // was SetSuperStream(n) — sets flag + partitions
    void MaxAge(TimeSpan maxAge);                        // was SetMaxAge
    void MaxLengthBytes(ulong maxLengthBytes);           // was SetMaxLengthBytes
    void MaxSegmentSizeBytes(int maxSegmentSizeBytes);   // was SetMaxSegmentSizeBytes
    void LeaderLocator(StreamLeaderLocator locator);     // was SetLeaderLocator

    void DeadLetterToStream(string name, Action<IStreamSpecBuilder>? configure = null);
    void DeadLetterToStream(string name, StreamSpec spec);
    void DeadLetterToQueue(string name, DeadLetterQueueType type);
    // DeadLetterTo(string, DeadLetterType) — deleted
}
```

Rules:

- The two methods keep kind compile-time safe: `Publish` takes an
  `IStreamSpecBuilder` (no `Partitions` reachable), `PublishSuper` takes
  `ISuperStreamSpecBuilder` (`Partitions` available). One internal
  `StreamSpecBuilder` class implements both.
- Builder setters return `void` and are last-call-wins (same as the
  endpoint `Set*` methods today); calling a setter twice is not an
  error. **No fluent chaining ("rail" style):** `s.Partitions(3).MaxAge(…)`
  does not compile — every call is its own statement on its own line.
- Overloads are mutually exclusive by shape: the callback mutates a
  fresh builder; the object overload is taken as-is. No combined form.
- `Publish` given a `StreamSpec` with `Partitions` set → registration-time
  config error (plain streams have no partitions).
- `PublishSuper` given a `StreamSpec` with `Partitions` null is allowed
  at registration time (a consuming endpoint in the same process may
  still supply partitions via `SetSuperStream`); startup fails only if
  no reference resolves partitions (rule S4 below).

## Declare-set semantics (after)

`PublishRegistration` gains `Partitions`, `MaxAge`, `MaxLengthBytes`,
`MaxSegmentSizeBytes`, `LeaderLocator` (from `StreamSpec`).
`StreamBusPublishStore.Add` grows accordingly. Declare-set sources
become `endpoint`, `publish`, `dead-letter` — `explicit` is gone.

- **S1 — kind agreement** (unchanged): `Publish` ↔ plain, `PublishSuper`
  ↔ super; a registration contradicting the merged stream kind throws
  `StreamBusConfigurationException` naming the message type and stream.
- **S2 — per-knob agreement across every reference to a stream**
  (endpoints, publishes, dead-letter entries): for each knob, values
  that are set on two or more references must be equal; a disagreement
  throws `StreamConfigurationConflictException` naming the stream, the
  knob, and every disagreeing source. A knob set on exactly one
  reference wins; set on none → declare-time default (`LeaderLocator`
  defaults `LeastLeaders` at `Ensure*`, as today). This also fixes an
  existing latent gap: today two endpoints on one stream silently drop
  the second endpoint's knobs (`DeclareSetBuilder` merges only
  partitions).
- **S3 — partition agreement**: `SetSuperStream(n)` on an endpoint vs
  `s.Partitions(m)` on a publish follows S2 (both set → must be equal;
  one → wins).
- **S4 — unresolvable partitions**: `PublishSuper` for a stream with no
  partition value from any reference → startup config error, reworded:
  "Message {type} is registered with PublishSuper for super stream
  '{stream}' with no partitions. Set Partitions on the PublishSuper
  registration (`s.Partitions(n)`) or `Partitions` on a consuming
  endpoint."
- **S5 — multi-stream-per-message** (unchanged): one message type → one
  stream per bus; explicit-stream `PublishAsync<T>(stream, …)` remains
  the escape hatch, call targets validated by `ValidateCallTarget`
  (unchanged).
- **S6 — host wiring** (unchanged): `DeclareDerivedUntilReadyAsync`
  loses its `explicitStreams`/`explicitSupers` parameters; probe →
  adopt → declare flow stays exactly as today, including the
  super-endpoint-missing-partitions error (Hosting.cs:678), reworded to
  reference `Partitions` on the registration or endpoint.

## Dead-letter streams (kind-split)

Error-stream and queue destinations get separate methods so kind, spec,
and arguments agree at compile time (same philosophy as
`Publish`/`PublishSuper`):

```csharp
void DeadLetterToStream(string name, Action<IStreamSpecBuilder>? configure = null);
void DeadLetterToStream(string name, StreamSpec spec);
void DeadLetterToQueue(string name, DeadLetterQueueType type);

public enum DeadLetterQueueType { ClassicQueue = 0, QuorumQueue = 1 }
```

- `DeadLetterToStream` destinations join the declare-set with source
  `dead-letter` and follow S1/S2/S3. Kind is plain (error streams are
  plain streams; a super-stream dead-letter destination is a config
  error — same rule as today's v1 "error streams must be plain").
- `DeadLetterToQueue` destinations never enter the declare-set
  (unchanged). There is no stream-spec overload on the queue path, so
  the old "spec given with a queue type" validation error path no
  longer exists — the invalid state is unrepresentable.
- **Exactly-once destination rule:** at most one dead-letter
  destination per endpoint. A second dead-letter call — either method,
  same or different destination — throws at registration:
  "ReceiveEndpoint '{stream}': a dead-letter destination is already
  configured ({kind} '{name}'); at most one is allowed per endpoint.
  Remove the duplicate call." This mirrors the `Consumer<T>` duplicate
  guard (StreamBusConfigurator.cs:376-377): destination selection is
  identity-tier (exactly-once), not a value knob (last-call-wins), and
  it covers the accidental stream↔queue switch a silent overwrite
  would allow — exhausted messages would strand in the wrong place.
- `DeadLetterType` (`Stream`/`ClassicQueue`/`QuorumQueue`,
  RetryConfiguration.cs:3-8) is deleted; `DeadLetterOptions`
  (RetryConfiguration.cs:10-14) becomes kind-based internally — stream
  destinations carry name + `StreamSpec`, queue destinations carry
  name + `DeadLetterQueueType` (record shape decided at implementation
  time; internal only).
- The old `DeadLetterTo(string name, DeadLetterType type)` signature
  (StreamBusConfigurator.cs:48, :419-425) is deleted.
- The "error-stream destinations are never derived from elsewhere"
  principle is preserved: the declaring registration is the endpoint
  with the `DeadLetterToStream` call, nothing else.

## Deleted or renamed surface

| Removed | Where |
|---|---|
| `DeclareStream` / `DeclareSuperStream` on `IStreamBusConfigurator` | `Configuration/StreamBusConfigurator.cs:29-30` + impls `:177-196`, backing `_streams`/`_supers`, and the `Topology` construction in `Build()` (`:277`) |
| `StreamTopologyDeclarationsBuilder` | `Hosting.cs:197-227` |
| `StreamDeclarationKnobsBuilder` (public) | `Hosting.cs:229-252` — replaced by `IStreamSpecBuilder`/`StreamSpecBuilder` |
| `StreamBusOptions.Topology` + options-builder wiring | `Options.cs:31` |
| `AddStreamBus(IServiceCollection, IConfigurationSection, …)` × 2 | `Configuration/StreamBusConfigurator.cs:506-524` |
| `ConfigurationBinding.cs` (whole file), `ApplySectionPrimitives`, `_sectionPath`, `SectionPrimitives` | `src/StreamBus/ConfigurationBinding.cs`, `StreamBusConfigurator.cs` |
| `DeadLetterTo(string, DeadLetterType)` + `DeadLetterType` enum | `Configuration/RetryConfiguration.cs:3-14`, `StreamBusConfigurator.cs:48` + `:419-425` |
| `Set*` endpoint setters — **renamed** to `Partitions` (`SetSuperStream` semantics), `MaxAge`, `MaxLengthBytes`, `MaxSegmentSizeBytes`, `LeaderLocator` | `Configuration/StreamBusConfigurator.cs:50-54` (interface), `:426-456` (impl) |

`StreamDeclaration` / `SuperStreamDeclaration` records stay — they are
the internal Ensure* plumbing. `TopologyValidation.ValidateStream/ValidateSuper`
stays, applied to the merged declaration before declare.

## Migration table

| Today | After |
|---|---|
| `cfg.DeclareStream("audit", k => k.MaxAge(30d))` + `cfg.Publish<T>("audit")` | `cfg.Publish<T>("audit", s => s.MaxAge(TimeSpan.FromDays(30)))` |
| `cfg.DeclareSuperStream("orders-super", 3)` + `cfg.PublishSuper<T>("orders-super")` | `cfg.PublishSuper<T>("orders-super", s => s.Partitions(3))` |
| `cfg.DeclareSuperStream("orders-super", 3)` + consuming endpoint in another service | publisher: `cfg.PublishSuper<T>("orders-super", s => s.Partitions(3))` (owner declares via its publish registration; consumer adopts as today) |
| `DeclareStream("errors")` for a dead-letter error stream | `DeadLetterTo("errors", DeadLetterType.Stream, s => s.MaxAge(...))` on the endpoint |
| `services.AddStreamBus(builder.Configuration.GetSection("StreamBus"), cfg => …)` | user hand-binds: read their own config values and call `cfg.Host(...)` etc. — no library binding |
| Super publish-only stream left undeclared (startup error) | same startup error, reworded per S4 |
| `e.SetSuperStream(3)` | `e.Partitions(3)` (same semantics: super flag + partitions) |
| `e.SetMaxAge(...)` / `SetMaxLengthBytes` / `SetMaxSegmentSizeBytes` / `SetLeaderLocator` | `e.MaxAge(...)` / `MaxLengthBytes` / `MaxSegmentSizeBytes` / `LeaderLocator` |
| `e.DeadLetterTo("orders-errors", DeadLetterType.Stream)` | `e.DeadLetterToStream("orders-errors")` |
| `e.DeadLetterTo("orders-dead", DeadLetterType.QuorumQueue)` | `e.DeadLetterToQueue("orders-dead", DeadLetterQueueType.QuorumQueue)` |

## Test plan

Delete:
- `tests/StreamBus.UnitTests/ConfigurationBindingTests.cs` (whole file)
- `ConfiguratorTests.cs:120/133` section-overload tests
- `HostingTests.cs:252` `IConfigurationSection` null-argument test
- `TopologyTests.cs:62-65` `StreamTopologyDeclarationsBuilder` tests

Update (declare calls → registration specs; error-message assertions
reworded per S4/S6):
- `ConfiguratorTests.cs:103-104`, `HostingTests.cs:448/453/513`,
  `TopologyIntegrationTests.cs:180-181/203-204/237`,
  `ErrorQueueIntegrationTests.cs:148` (→ `DeadLetterToStream` /
  `DeadLetterToQueue`), `examples/StreamBus.Examples.Producer/Program.cs:26/30`
- Rename sweep — every `SetSuperStream`/`SetMaxAge`(+ siblings) caller:
  `ReceiveEndpointTests.cs:182/739`, super endpoints in `HostingTests`
  and `TypedStartupIntegrationTests`, plus doc snippets; mechanical
  `Set` prefix drop, no semantic change

Stays green as-is:
- `TypedStartupIntegrationTests.cs:106` (endpoint-only derivation —
  the proof this spec doesn't regress derivation)

New unit tests:
- callback overload on `Publish` (knobs only, no Partitions reachable)
- callback overload on `PublishSuper` (`Partitions` ≥ 1, last-call-wins)
- object overloads; `Publish` + `Partitions` set → registration error
- S2 per-knob agreement: endpoint×publish agree / conflict naming both;
  endpoint×endpoint conflict (the fixed latent gap)
- S3 partition agreement / conflict
- S4 reworded error names type + stream and mentions both remedies
- dead-letter Stream spec joins declare-set; queue type + spec → error
- endpoint `Partitions(n)` implies the super flag (behaviour carried
  over from `SetSuperStream`; `Partitions` always sets the flag, as
  today)
- `DeadLetterToQueue` rejects undefined `DeadLetterQueueType` values
  (ported from the old `DeadLetterTo` enum check, StreamBusConfigurator.cs:422-423)
- duplicate dead-letter guard: `DeadLetterToStream` twice,
  `DeadLetterToQueue` twice, and stream-after-queue / queue-after-stream
  — all registration-time errors naming existing + new destination;
  `OnExhausted` + destination remains the shadowing rule (Hosting.cs:774),
  not an error
- `AddStreamBus(IConfigurationSection)` no longer compiles (covered by
  deletion of the null-check test)

## Docs to update

- `docs/guide/config.md` — delete "JSON binding" + "Full key reference"
  sections; state that configuration is the user's responsibility and
  every knob is validated at configurator build time.
- `docs/guide/topology.md` — rewrite "Explicit declarations" →
  "Registration-owned stream specs" (publish registration carries the
  spec); keep derived-topology and adopt-rule sections (S-references
  reworded).
- `docs/guide/publish.md`, `docs/guide/effectively-once.md`,
  `docs/guide/errors.md`, `docs/guide/consume.md` — replace
  `Declare*` snippets with registration-spec snippets; every `Set*`
  reference becomes the renamed builder-style call, and dead-letter
  examples use `DeadLetterToStream`/`DeadLetterToQueue`.
- `docs/2026-10-13-streambus-acceptance.md` — historical, untouched.

## Breaking-change note

API break on the v2 (pre-1.0) line: `DeclareStream`,
`DeclareSuperStream`, `StreamTopologyDeclarationsBuilder`,
`StreamDeclarationKnobsBuilder`, `StreamBusOptions.Topology`, both
`IConfigurationSection` overloads, `DeadLetterType`, and
`DeadLetterTo(string, DeadLetterType)` disappear; `Publish`/
`PublishSuper` gain optional/required spec parameters; the endpoint
`Set*` members are renamed with unchanged semantics. Runtime behavior
changes are limited to error wording and the new S2 conflict check.

## Verification results (2026-10-18)

- **Q1 resolved — keep the name `StreamSpec`.** The client-side type is
  always fully qualified (`RabbitMQ.Stream.Client.StreamSpec` —
  Connector.cs:24/124, Topology.cs:44-47), so a public
  `StreamBus.StreamSpec` record causes no collision.
- **Q4 resolved — `Options.Topology` consumers are all explicit-declare
  plumbing this spec removes anyway:** Hosting.cs:66-67 (explicit-list
  extraction), Hosting.cs:435-457 (the vhost-level
  `DeclareUntilReadyAsync` runner — dead once explicit declares are
  gone; delete it and its task wiring), Hosting.cs:469-477 (explicit
  params fed to `DeclareDerivedUntilReadyAsync` — strip both params
  from the signature).

## Decisions settled (2026-10-18)

- **Q2 — rename endpoint `Set*` to builder style** (chosen over keeping
  the asymmetry): `Partitions`, `MaxAge`, `MaxLengthBytes`,
  `MaxSegmentSizeBytes`, `LeaderLocator` — semantics unchanged; wider
  mechanical sweep across tests/examples/docs.
- **Q3 — kind-split dead-letter methods (option B)**:
  `DeadLetterToStream` (callback + `StreamSpec` object overloads) /
  `DeadLetterToQueue` with `DeadLetterQueueType`; queue destinations
  take no stream-spec overload, so invalid combinations don't compile
  and no extra validation error path is needed.
- **Q5 — exactly-once dead-letter destination** (user-raised
  2026-10-18): a second dead-letter call on an endpoint throws at
  registration, mirroring the `Consumer<T>` duplicate guard; the
  `OnExhausted` funnels-to-callback shadowing rule stays as-is.
