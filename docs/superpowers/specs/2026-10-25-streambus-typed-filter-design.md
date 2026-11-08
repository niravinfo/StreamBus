# StreamBus typed publish/consume filters

Date: 2026-10-25
Status: proposed

## Intent

Today StreamBus filters are message-agnostic: one bus-wide publish list and a
bus-wide/endpoint-wide consume list. Every filter runs for every message in its
scope. MassTransit instead makes filters typed by message and attaches each
filter only to the pipe for that message type; a globally registered
`MyFilter<T>` is a template that is closed per message type.

We want that per-type attachment, with a typed `T Message` in the filter
context. Breaking changes are acceptable because the library is still in
development.

Consume filters will see the already-deserialized message (eager). We do not
want a lazy "maybe deserialized" contract.

## Goals

- Add `IStreamPublishFilter<T>` and `IStreamConsumeFilter<T>` with typed
  contexts exposing `T Message`.
- Keep the existing non-generic `IStreamPublishFilter` / `IStreamConsumeFilter`
  as universal filters that run for every message in their scope.
- Attach typed filters per message type:
  - open generic registration (`typeof(TenantFilter<>)`) is closed once per
    published/consumed message type;
  - a concrete typed filter attaches only to the message types it implements;
  - universal filters always attach.
- Support typed filters at bus level and (for consume) per receive endpoint.
- For typed receive endpoints, deserialize before the consume filter pipeline
  whenever at least one typed filter is attached, and share that exact message
  instance with the consumer. No second deserialization.
- Preserve existing pipeline guarantees: registration order, bus-wide before
  endpoint filters, exactly-one-`next`, skip rules, effectively-once rejection
  of `CanSkip`, per-message DI scope sharing, retry/exhaustion behavior.

## Non-goals

- No filter include/exclude predicates (MassTransit's
  `configureMessageTypeFilter`). Type matching comes from the generic
  interfaces; consume endpoint scoping covers the common case.
- No per-publish-stream or per-publish-registration filters. Publish filters
  are per message type.
- No polymorphic/runtime payload dispatch. `PublishAsync<T>` uses declared
  `T`; a typed consumer endpoint uses the `IConsumer<T>` message type. A
  filter for a derived type does not automatically match a base type.
- No typed filters for raw `ConsumeAsync` or `ConsumeEffectivelyOnceAsync`
  (they have no message type). Raw pipelines remain universal-only.
- No message envelope or automatic wire metadata (message type, generated
  message id, timestamp, conversation/request/address fields). Type identity
  for this change is the compile-time pipe type only. See
  [`2026-10-25-streambus-wire-metadata-design.md`](2026-10-25-streambus-wire-metadata-design.md).
- No wire-level type validation in this change; it belongs to the wire
  metadata design.
- No change from singleton-per-bus filter instances to per-message scoped
  instances. Scoped services remain available through `context.Services`.
- No change to `PublishRawAsync`, error hand-off, requeue, replay, or result
  publishes; those still bypass publish filters.

## Current behavior (for reference)

- `StreamPublishFilterContext.Message` is `object` and holds the live message
  passed to `PublishAsync<T>` (`src/StreamBus/PublishPipeline.cs:12-20`,
  `src/StreamBus/Bus.cs:166-174`). Filters are built once per bus
  (`src/StreamBus/Registry.cs:181-203`) and run for every publish.
- `StreamConsumeFilterContext.Message` is the raw `ConsumeContext`
  (`src/StreamBus/ConsumePipeline.cs:14-29`). Typed consumers deserialize
  inside the terminal handler after the pipeline (`src/StreamBus/Hosting.cs:281-317`).
  Filters are bus/endpoint lists without message-type matching
  (`src/StreamBus/ConsumerRegistry.cs:121-173`).
- Open generic filter registration is rejected today
  (`src/StreamBus/Configuration/StreamBusConfigurator.cs:294-304, 341-351`).

Wire metadata and type validation are deliberately out of scope here. Typed
filters attach by the pipe type `T` and do not require message-type headers,
generated IDs, or timestamps. Those are designed separately in
[`2026-10-25-streambus-wire-metadata-design.md`](2026-10-25-streambus-wire-metadata-design.md).

## Approaches considered

1. **Eager typed filters (chosen).** Typed contexts with `T Message`;
   deserialize before the consume pipeline for typed endpoints. Simple mental
   model and MassTransit-like. Trade-off: when a typed filter is present, a
   skip no longer avoids deserialization, and filters do not observe
   deserialization failures.
2. **Lazy typed filters.** Typed context deserializes on first access and
   caches for the handler. Preserves skip/header-only cheapness, but exposes a
   misleading "not necessarily deserialized" contract and needs more plumbing.
   Rejected.
3. **Untyped accessor only.** Add `MessageType` and `TryGetMessage<T>()` to the
   existing contexts, without per-type attachment. Smallest change, but every
   filter still runs for every message. Rejected because per-type attachment is
   the requirement.

## Chosen design

### Public surface

```csharp
public interface IStreamPublishFilter<T>
{
    Task OnPublish(
        StreamPublishFilterContext<T> context,
        PublishDelegate next,
        CancellationToken ct);
}

public sealed class StreamPublishFilterContext<T>
{
    public required StreamPublishFilterContext Inner { get; init; }
    public T Message => (T)Inner.Message;
    public Type MessageType => typeof(T);
    public Type RuntimeMessageType => Inner.RuntimeMessageType;
    public string Stream => Inner.Stream;
    public string? RoutingKey => Inner.RoutingKey;
    public IStreamHeaderWriter Headers => Inner.Headers;
    public StreamBusRegistrationView Bus => Inner.Bus;
    public IServiceProvider Services => Inner.Services;
}

public interface IStreamConsumeFilter<T>
{
    bool CanSkip => false;

    Task OnConsume(
        StreamConsumeFilterContext<T> context,
        ConsumeDelegate next,
        CancellationToken ct);
}

public sealed class StreamConsumeFilterContext<T>
{
    public required StreamConsumeFilterContext Inner { get; init; }
    public required T Message { get; init; }
    public Type MessageType => typeof(T);
    public ConsumeContext Raw => Inner.Message;
    public IStreamHeaderWriter OutputHeaders => Inner.OutputHeaders;
    public IServiceProvider Services => Inner.Services;
    public bool SkipHandler
    {
        get => Inner.SkipHandler;
        set => Inner.SkipHandler = value;
    }
    public TPayload? GetPayload<TPayload>() => Inner.GetPayload<TPayload>();
    public void SetPayload<TPayload>(TPayload value) => Inner.SetPayload(value);
}
```

The existing non-generic interfaces and contexts stay. `StreamPublishFilterContext`
gains `Type? MessageType { get; init; }` and `Type RuntimeMessageType =>
Message.GetType()`. `MessageType` is the **declared pipe type** (`typeof(T)` at
the publish call site) when produced by the pipeline; it is null only for a
context constructed directly outside the bus. Runtime payload type is always
`RuntimeMessageType`. `StreamConsumeFilterContext` gains
`Type? MessageType { get; init; }` (the endpoint message type, null for raw
pipelines) and an internal `object? TypedMessage { get; init; }`.

Both generic contexts wrap the non-generic context so headers, payloads,
`Services`, `SkipHandler`, and metrics remain shared.

### Registration

Existing extension signatures are retained, with relaxed constraints:

```csharp
cfg.UsePublishFilter<TFilter>();        // now `where TFilter : class`
cfg.UsePublishFilter(typeof(TenantFilter<>));
cfg.UseConsumeFilter<TFilter>();
cfg.UseConsumeFilter(typeof(TenantFilter<>));
cfg.ReceiveEndpoint("orders", e => e.UseConsumeFilter<TFilter>());
```

Validation rules:

- A filter type must implement exactly one family:
  - universal (`IStreamPublishFilter` / `IStreamConsumeFilter`), or
  - one or more typed interfaces (`IStreamPublishFilter<T>` /
    `IStreamConsumeFilter<T>`), or
  - be an open generic definition with one generic parameter implementing the
    typed interface for that same parameter (`class F<T> : IStreamPublishFilter<T>`).
- Implementing both universal and typed is rejected: it would run twice for
  matching types.
- Open generic filters must have exactly one generic parameter. Arity greater
  than one is rejected with a clear configuration error.
- Factory and caller-owned instance overloads remain universal-only. A typed
  factory cannot be evaluated per message type without invoking it; typed
  filters must be registered by type so DI/reflection can close them.
- Duplicate registration of the same filter type in the same scope remains an
  error (bus scope and endpoint scope are checked separately, as today).
- At pipeline build for a message type, a closed typed filter that collides
  with another registration for that same type (for example `F<>` plus
  `F<Order>`) is rejected.

Open generic filter types and concrete typed filter types are registered as
transient services when they are not caller-owned; MS DI resolves the closed
type from the open generic registration.

### Attachment rules

Attachment is based on the message type of the pipe, not the runtime type of
the payload.

- **Publish, bus-wide:** for message type `T`, include universal filters and
  typed filters that match `T`, in registration order. An open generic is
  closed as `Filter<T>`. Non-matching typed filters are skipped silently.
  Pipelines are cached per message type. Explicit-stream publishes use the
  declared `T`, including types with no `Publish<T>` registration.
- **Consume, bus-wide:** for each typed receive endpoint with message type `T`,
  include universal filters and typed filters matching `T`, then the endpoint
  filters in their registration order. Non-matching typed filters are skipped
  silently (that is the point of a bus-wide template).
- **Consume, endpoint-scoped:** if the endpoint consumes `T`, a concrete typed
  filter must implement `IStreamConsumeFilter<T>`; otherwise configuration
  fails fast. An endpoint has exactly one message type, so a mismatch is
  certainly a bug; silently never running the filter would hide it. This is
  deliberately stricter than MassTransit, which skips mismatches even at
  endpoint level. Open generic endpoint filters always close to the endpoint's
  `T`; universal endpoint filters always run.
- **Raw consume / effectively-once:** no message type is known, so typed
  filters never attach. Universal filters behave as today. The EO `CanSkip`
  rejection remains.
- **Publish raw / hand-off / replay / requeue:** unchanged; no publish
  pipeline.

### Pipeline construction timing

- At bus creation (`Registry.CreateEntry`), build and cache a publish pipeline
  for every `PublishRegistration` message type for that bus. This preserves
  today's fail-fast behavior: missing DI registrations, open-generic closure
  failures, duplicate closed types, and the "filters configured but no root
  provider" check surface at host startup, not on the first live publish.
- Explicit-stream publishes of a type with no `Publish<T>`/`PublishSuper<T>`
  registration build their pipeline lazily on first use and cache it. Errors
  for that type surface on that publish call.
- Consume pipelines are built at consumer attach, where the endpoint message
  type and root provider are both known; endpoint mismatch errors are thrown
  earlier, during `AddStreamBus` (see below).

### Open generic closure failures

Closing an open generic filter with the pipe's message type can fail because
of generic constraints (for example `F<T> where T : class` with a struct
message) or because the closed type does not implement the expected typed
filter interface. That is a configuration error, not a non-match: throw
`StreamBusConfigurationException` naming the filter, message type, and
constraint problem at pipeline build. Non-match means a **concrete** typed
filter that does not implement the interface for this message type; those are
skipped silently.

### Eager consume message

- Add `internal Type? MessageType` to `ConsumeOptions`, set from the typed
  endpoint descriptor; null for raw/EO pipelines.
- `ConsumerRegistry.BuildPipeline` closes/attaches typed consume filters for
  `options.MessageType` and records whether the resulting pipeline contains at
  least one typed filter (`RequiresTypedMessage`).
- If a typed filter is present, `StreamPartitionConsumer` deserializes the body
  before running the filter pipeline, per attempt:
  - deserialization uses the same `IStreamSerializer` as the endpoint;
  - failures produce `StreamBusDeserializationException` exactly as today and
    enter the existing retry/exhaustion path; filters and the handler do not
    run for that attempt;
  - on success the same `object` is placed on the per-attempt `ConsumeContext`
    (`TypedMessage`) and passed to filters and to the consumer; the consumer
    handler reuses it instead of deserializing again.
- If the pipeline has only universal filters (or no filters), deserialization
  stays in the typed handler as today. This preserves the existing
  "skip avoids deserialization" behavior for universal-only pipelines.
- `TypedConsumerHandler.Create` keeps its current signature for direct use and
  tests; it uses `raw.TypedMessage` when present and otherwise deserializes as
  before. A new internal deserializer helper is used by the partition consumer.
- Raw consumers keep receiving raw `ConsumeContext`; typed filters cannot be
  attached to them.

### Ordering, duplicates, lifetime

- Order is exactly registration order within a scope; bus-wide filters are
  outside endpoint filters. For each message type, non-matching typed filters
  are omitted without changing the relative order of the rest.
- Filters remain one instance per bus per closed filter type. Open generics
  therefore resolve one `Filter<T>` per bus/type. Filters must be thread-safe;
  per-message state travels the context or a DI scope.
- Closed-type collision detection is **per scope**. Within one bus scope or
  within one endpoint scope, `F<>` plus `F<Order>` (or two registrations that
  close to the same type) is rejected for `Order`. Across scopes, bus + the
  same filter at endpoint level continues to run twice, exactly as today;
  cross-scope deduplication is out of scope.
- Endpoint-scoped typed mismatch is detected in
  `ReceiveEndpointConfigurator.Build()` during `AddStreamBus`, after
  `Consumer<T>()` has established the endpoint message type.
  `ConsumerRegistry.BuildPipeline` repeats the check defensively at attach.
- Adapters wrap typed filters so the pipeline error messages name the user's
  filter type, not the adapter type.

### Compatibility and breaking changes

- Non-generic filter implementations remain source-compatible. Contexts gain
  optional members only, so directly constructed test contexts still compile.
- `UsePublishFilter<TFilter>()` / `UseConsumeFilter<TFilter>()` constraints
  relax from `IStream*Filter` to `class`; existing universal filters still
  compile.
- Open generic registration becomes valid.
- Endpoint-scoped typed filters that do not match the endpoint message type now
  fail at configuration instead of being silently ignored.
- Typed consume filters change deserialization timing for the affected
  endpoint (before the pipeline) and skip no longer avoids deserialization on
  those endpoints.
- `ConsumeContext` gains an internal `TypedMessage` slot; public construction
  is unchanged.

## Test plan

Unit:

- registration validation: universal, typed concrete, open generic, no
  interface, dual universal+typed, open generic arity > 1, typed factory.
- publish attachment: `Filter<Order>` runs for Order only; universal runs for
  all; registration order; explicit-stream publish; unknown explicit type.
- publish pipeline timing: registered types fail at bus creation for missing
  DI / constraint violation / collision; unregistered explicit types fail on
  first publish.
- open generic constraint violation throws a configuration error rather than
  silently skipping.
- constructors: `MessageType` on the universal publish context equals the
  declared call-site `T`, while `RuntimeMessageType` is the runtime payload
  type.
- consume attachment: bus-level typed filter matches only the endpoint with
  that message type; endpoint-level typed mismatch throws during
  `AddStreamBus`; endpoint-level open generic closes for the endpoint type;
  universal runs everywhere.
- eager consume: typed filter and consumer receive the same instance; typed
  filter sees a deserialized `T`; universal filter still sees raw; typed
  deserialization failure retries/exhausts without running filters; skip with
  `CanSkip` and `AllowPipelineSkip`; EO rejects `CanSkip`.
- ordering: bus universal/typed then endpoint universal/typed; non-matching
  filters omitted.
- duplicate detection per closed type within one scope, and the documented
  bus+endpoint double registration across scopes.
- `TypedConsumerHandler` fallback still deserializes when `TypedMessage` is
  absent.

Integration:

- typed publish filter enriches headers -> typed consume filter reads typed
  message and header -> consumer sees the same message.
- a second message type on another endpoint does not run the first type's
  filter.
- universal filter still runs for every type.
- per-endpoint typed filter only affects its endpoint.

Docs/examples:

- update `docs/guide/publish.md` and `docs/guide/consume.md` with typed filter
  examples, open generic registration, per-type semantics, and eager
  deserialization.
- add a typed filter sample to the examples if it can be done without
  disturbing the existing universal samples.
- CHANGELOG entry under breaking changes.

## Implementation outline

1. Add generic filter interfaces/contexts and adapter wrappers.
2. Extend filter registration metadata/validation for open generics and typed
   concrete filters; accept open generic DI registrations.
3. Build per-message-type publish pipelines and cache them.
4. Attach typed consume filters in `ConsumerRegistry.BuildPipeline` using the
   endpoint message type; enforce endpoint strict matching.
5. Add `ConsumeOptions.MessageType`, eager deserialization in
   `StreamPartitionConsumer`, and share `TypedMessage` with the typed handler.
6. Update error naming, payload/skip sharing, and metrics tests.
7. Update docs, examples, CHANGELOG, and tests.

## Decisions

- Endpoint-scoped mismatch: fail fast during `AddStreamBus`. MassTransit
  silently skips, but an endpoint has exactly one message type, so a mismatch
  is a configuration bug.
- Keep universal filters alongside typed filters; typed-only would break every
  existing filter and loses the convenient all-types case.
- Typed factory/instance overloads are out of scope; typed filters are
  registered by type.
- Pipeline construction is eager for registered publish types, lazy for
  explicit unregistered types.
- Open generic closure failures are configuration errors; concrete typed
  non-matches are silent skips.
- Closed-type collisions are checked per scope, not across bus/endpoint.
- No envelope, automatic IDs/timestamp, or wire-level message-type validation
  in this change. Typed filters attach by compile-time `T` and work with the
  current wire format. Wire metadata is designed separately in
  [`2026-10-25-streambus-wire-metadata-design.md`](2026-10-25-streambus-wire-metadata-design.md).

## Definition of done

- All new unit and integration tests pass.
- Existing universal-filter tests pass unchanged.
- `dotnet test` is green.
- Docs state bus-wide vs endpoint behavior and eager deserialization timing.
