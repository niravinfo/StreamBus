# Spec: Registration-owned stream specs (delete Declare*, delete section binding)

Date: 2026-10-17
Status: draft — agreed direction, open questions listed at the end
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

Rules:

- The two methods keep kind compile-time safe: `Publish` takes an
  `IStreamSpecBuilder` (no `Partitions` reachable), `PublishSuper` takes
  `ISuperStreamSpecBuilder` (`Partitions` available). One internal
  `StreamSpecBuilder` class implements both.
- Builder setters are last-call-wins (same as the endpoint `Set*`
  methods today); calling a setter twice is not an error.
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
  registration (`s.Partitions(n)`) or SetSuperStream on a consuming
  endpoint."
- **S5 — multi-stream-per-message** (unchanged): one message type → one
  stream per bus; explicit-stream `PublishAsync<T>(stream, …)` remains
  the escape hatch, call targets validated by `ValidateCallTarget`
  (unchanged).
- **S6 — host wiring** (unchanged): `DeclareDerivedUntilReadyAsync`
  loses its `explicitStreams`/`explicitSupers` parameters; probe →
  adopt → declare flow stays exactly as today, including the
  super-endpoint-missing-partitions error (Hosting.cs:678), reworded to
  reference `SetSuperStream`/`Partitions` instead of DeclareSuperStream.

## Dead-letter streams

`DeadLetterTo` gains spec overloads so error-stream destinations are
fully declared from the endpoint that owns them:

```csharp
void DeadLetterTo(string name, DeadLetterType type, Action<IStreamSpecBuilder>? configure = null);
void DeadLetterTo(string name, DeadLetterType type, StreamSpec spec);
```

- `DeadLetterType.Stream` destinations join the declare-set with source
  `dead-letter` and follow S1/S2/S3. Kind is plain (error streams are
  plain streams; a super-stream dead-letter destination is a config
  error — same rule as today's v1 "error streams must be plain").
- `ClassicQueue`/`QuorumQueue` destinations never enter the declare-set
  (unchanged); passing a `StreamSpec` (object or callback) with a queue
  type is a registration-time config error.
- The "error-stream destinations are never derived from elsewhere"
  principle is preserved: the declaring registration is the endpoint
  with the `DeadLetterTo` call, nothing else.

## Deleted surface

| Removed | Where |
|---|---|
| `DeclareStream` / `DeclareSuperStream` on `IStreamBusConfigurator` | `Configuration/StreamBusConfigurator.cs:29-30` + impls `:177-196`, backing `_streams`/`_supers`, and the `Topology` construction in `Build()` (`:277`) |
| `StreamTopologyDeclarationsBuilder` | `Hosting.cs:197-227` |
| `StreamDeclarationKnobsBuilder` (public) | `Hosting.cs:229-252` — replaced by `IStreamSpecBuilder`/`StreamSpecBuilder` |
| `StreamBusOptions.Topology` + options-builder wiring | `Options.cs:31` |
| `AddStreamBus(IServiceCollection, IConfigurationSection, …)` × 2 | `Configuration/StreamBusConfigurator.cs:506-524` |
| `ConfigurationBinding.cs` (whole file), `ApplySectionPrimitives`, `_sectionPath`, `SectionPrimitives` | `src/StreamBus/ConfigurationBinding.cs`, `StreamBusConfigurator.cs` |

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
  `ErrorQueueIntegrationTests.cs:148` (→ `DeadLetterTo` spec overload),
  `examples/StreamBus.Examples.Producer/Program.cs:26/30`

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
  `Declare*` snippets with registration-spec snippets.
- `docs/2026-10-13-streambus-acceptance.md` — historical, untouched.

## Breaking-change note

API break on the v2 (pre-1.0) line: `DeclareStream`,
`DeclareSuperStream`, `StreamTopologyDeclarationsBuilder`,
`StreamDeclarationKnobsBuilder`, `StreamBusOptions.Topology`, and both
`IConfigurationSection` overloads disappear; `Publish`/`PublishSuper`
gain optional/required spec parameters. No runtime behavior changes
beyond error wording and the S2 conflict check.

## Open questions (to settle when we continue)

1. `StreamSpec` naming — verify no confusion with client-side spec
   types; fallback `StreamSpecOptions`. Cosmetic.
2. Style asymmetry: endpoints use `Set*` (`SetMaxAge`), the new builder
   uses non-Set names per the agreed sketch (`s.MaxAge(...)`). Keep as
   accepted asymmetry, or rename endpoint `Set*` to match in the same
   change?
3. Does `DeadLetterTo` need the `StreamSpec` object overload, or is the
   callback enough? (Spec'd both for consistency; happy to cut.)
4. Confirm: no other consumer of `StreamBusOptions.Topology` besides the
   deleted builder path (verify at implementation time).
