# StreamBus single filter context

Date: 2026-10-27
Status: accepted - implemented (2026-10-28)
Approach: A (selected by owner)

Supersedes the filter-context, delegate, and skip shape in
`2026-10-25-streambus-typed-filter-design.md`. That document's attachment,
registration, eager-deserialization, and ordering rules remain in force; its
`CanSkip` / `SkipHandler` / `AllowPipelineSkip` rules are removed.

## Intent

Remove `context.Inner` from typed filters. A filter must call `next()` and
mutate the one context it was given; nothing is unwrapped, copied, or passed
to the continuation.

Also make filtering a normal filter decision: a consume filter may return
without calling `next()`. The message is then handled by the filter and the
consumer is not invoked. There is no capability flag, no skip handler, and no
endpoint opt-in.

Finally, make the public filter surface self-documenting: every public member
a filter author touches gets a concise XML summary describing its purpose,
units, valid values, and default.

## Why the current shape is wrong

- `StreamPublishFilterContext<T>` / `StreamConsumeFilterContext<T>` are
  separate wrapper objects around `StreamPublishFilterContext` /
  `StreamConsumeFilterContext`. They cannot be passed to the non-generic
  `PublishDelegate` / `ConsumeDelegate`, so `Inner` is public only to unwrap.
- The pipeline ignores the delegate argument: `Next(StreamPublishFilterContext _)`
  continues with its captured context. The test
  `Super_routing_and_body_cannot_be_replaced_by_a_filter_context` deliberately
  passes a swapped context and asserts nothing changes.
- Therefore `next(context.Inner)` is ceremonial. Headers and payloads already
  flow because both objects delegate to the same state.
- `CanSkip` is only checked at configuration time, so it is not a runtime
  permission. It adds a second way to express “do not call the handler” that
  disagrees with the simpler filter decision: just return without `next()`.

MassTransit has one context instance and no wrapper. Its non-generic contexts
do not expose `Message` at all; only `PublishContext<T>` / `ConsumeContext<T>`
do. A MassTransit filter short-circuits by not calling `next.Send(context)`;
there is no `CanSkip` equivalent. StreamBus will follow that shape while
keeping its existing decoration-only pipeline (ADR-2), except that
effectively-once pipelines must still produce a result.

## Chosen API

### Publish

```csharp
/// <summary>Continues the publish pipeline. Call exactly once.</summary>
public delegate Task PublishDelegate();

/// <summary>Publish middleware that runs for every publish in its scope.</summary>
public interface IStreamPublishFilter
{
    /// <summary>Runs after the stream and routing key are resolved and before serialization. Call next exactly once; a publish cannot be dropped.</summary>
    /// <param name="context">The publish context shared by every filter in this publish.</param>
    /// <param name="next">Continues the pipeline. Call exactly once.</param>
    /// <param name="ct">Cancels the publish.</param>
    Task OnPublish(StreamPublishFilterContext context, PublishDelegate next, CancellationToken ct);
}

/// <summary>Publish middleware that runs only when the declared publish type is <typeparamref name="T"/>.</summary>
public interface IStreamPublishFilter<T>
{
    /// <summary>Runs after the stream and routing key are resolved and before serialization. Call next exactly once; a publish cannot be dropped.</summary>
    /// <param name="context">The typed publish context shared by every filter in this publish.</param>
    /// <param name="next">Continues the pipeline. Call exactly once.</param>
    /// <param name="ct">Cancels the publish.</param>
    Task OnPublish(StreamPublishFilterContext<T> context, PublishDelegate next, CancellationToken ct);
}

/// <summary>The untyped publish context shared by every filter in one publish.</summary>
public class StreamPublishFilterContext
{
    /// <summary>Stream the message is published to.</summary>
    public required string Stream { get; init; }

    /// <summary>Routing key used by a super-stream publish; <see langword="null"/> otherwise.</summary>
    public required string? RoutingKey { get; init; }

    /// <summary>The message being published as its runtime object. Typed filters read it as <c>T</c> through <see cref="StreamPublishFilterContext{T}.Message"/>.</summary>
    public required object Payload { get; init; }

    /// <summary>Declared publish type. Typed contexts return <c>typeof(T)</c>; <see langword="null"/> only for a context created outside the bus.</summary>
    public virtual Type? MessageType { get; init; }

    /// <summary>Runtime type of <see cref="Payload"/>.</summary>
    public Type RuntimeMessageType => Payload.GetType();

    /// <summary>Headers written by filters and sent with the message.</summary>
    public required IStreamHeaderWriter Headers { get; init; }

    /// <summary>Read-only view of the bus that owns this publish.</summary>
    public required StreamBusRegistrationView Bus { get; init; }

    /// <summary>Service scope for this publish.</summary>
    public required IServiceProvider Services { get; init; }
}

/// <summary>The typed publish context for the declared type <typeparamref name="T"/>; the same object as the universal context.</summary>
public sealed class StreamPublishFilterContext<T> : StreamPublishFilterContext
{
    /// <summary>Always <c>typeof(T)</c>.</summary>
    public override Type MessageType => typeof(T);

    /// <summary>The message being published as <typeparamref name="T"/>.</summary>
    public T Message => (T)Payload;
}
```

### Consume

```csharp
/// <summary>Continues the consume pipeline. Call at most once; omit it to filter the message.</summary>
public delegate Task ConsumeDelegate();

/// <summary>Consume middleware that runs before the handler.</summary>
public interface IStreamConsumeFilter
{
    /// <summary>Runs before the handler. Call next to continue; return without calling next to mark the message handled without invoking the consumer.</summary>
    /// <param name="context">The consume context shared by every filter in this delivery.</param>
    /// <param name="next">Continues the pipeline. Call at most once.</param>
    /// <param name="ct">Cancels the delivery.</param>
    Task OnConsume(StreamConsumeFilterContext context, ConsumeDelegate next, CancellationToken ct);
}

/// <summary>Consume middleware that runs only for the endpoint message type <typeparamref name="T"/>.</summary>
public interface IStreamConsumeFilter<T>
{
    /// <summary>Runs before the handler. Call next to continue; return without calling next to mark the message handled without invoking the consumer.</summary>
    /// <param name="context">The typed consume context shared by every filter in this delivery.</param>
    /// <param name="next">Continues the pipeline. Call at most once.</param>
    /// <param name="ct">Cancels the delivery.</param>
    Task OnConsume(StreamConsumeFilterContext<T> context, ConsumeDelegate next, CancellationToken ct);
}

/// <summary>The untyped consume context shared by every filter in one delivery.</summary>
public class StreamConsumeFilterContext
{
    /// <summary>The transport message: stream, partition, offset, body, metadata, and cancellation token.</summary>
    public required ConsumeContext Raw { get; init; }

    /// <summary>Endpoint message type; <see langword="null"/> for raw and effectively-once pipelines.</summary>
    public virtual Type? MessageType { get; init; }

    /// <summary>Headers added to exhaustion envelopes and effectively-once result publishes.</summary>
    public required IStreamHeaderWriter OutputHeaders { get; init; }

    /// <summary>Service scope for this delivery, shared with the consumer.</summary>
    public required IServiceProvider Services { get; init; }

    /// <summary>Gets a value set by an earlier filter in the same delivery.</summary>
    /// <typeparam name="T">Value type.</typeparam>
    /// <returns>The stored value, or <see langword="default"/> when none was set.</returns>
    public T? GetPayload<T>();

    /// <summary>Stores a value for later filters in the same delivery.</summary>
    /// <typeparam name="T">Value type.</typeparam>
    /// <param name="value">Value to store.</param>
    public void SetPayload<T>(T value);
}

/// <summary>The typed consume context for the endpoint message type <typeparamref name="T"/>; the same object as the universal context.</summary>
public sealed class StreamConsumeFilterContext<T> : StreamConsumeFilterContext
{
    /// <summary>Always <c>typeof(T)</c>.</summary>
    public override Type MessageType => typeof(T);

    /// <summary>The deserialized message. The consumer receives the same instance.</summary>
    public required T Message { get; init; }
}
```

### Header writer

```csharp
/// <summary>Writes application headers for a publish or consume flow.</summary>
public interface IStreamHeaderWriter
{
    /// <summary>Sets a header, replacing an existing value.</summary>
    /// <param name="key">Header name.</param>
    /// <param name="value">Header value; must be a supported header value.</param>
    void Set(string key, object? value);

    /// <summary>Gets a header value.</summary>
    /// <param name="key">Header name.</param>
    /// <param name="value">The value when found; otherwise <see langword="null"/>.</param>
    /// <returns><see langword="true"/> when the header exists.</returns>
    bool TryGet(string key, out object? value);

    /// <summary>Removes a header.</summary>
    /// <param name="key">Header name.</param>
    /// <returns><see langword="true"/> when the header existed.</returns>
    bool Remove(string key);
}
```

### Usage

```csharp
public Task OnPublish(StreamPublishFilterContext<OrderEvent> context, PublishDelegate next, CancellationToken ct)
{
    context.Headers.Set("x-order-id", context.Message.OrderId);
    return next();
}

public Task OnConsume(StreamConsumeFilterContext context, ConsumeDelegate next, CancellationToken ct)
{
    if (context.Raw.Metadata.GetString("x-tenant") is null)
        return Task.CompletedTask; // filtered: handler is not invoked

    scope.Tenant = context.Raw.Metadata.GetString("x-tenant");
    return next();
}
```

One context object per publish/delivery. Universal and typed filters receive
the same reference: the universal filter sees it as the base type, the typed
filter as the closed generic type. `Assert.Same` must hold between the two
views.

## Semantics

- `next()` is the only continuation. There is no context argument, so a filter
  cannot hand a different context downstream. Context replacement is
  structurally impossible, matching ADR-2's decoration-only decision.
- **Publish filters** call `next()` exactly once. A publish cannot be dropped;
  zero or multiple calls throw as today.
- **Consume filters** call `next()` zero or one time. Zero means the filter
  handled or filtered the message: the handler is not invoked and the offset
  advances. Multiple calls throw as today.
- **Effectively-once consume filters** call `next()` exactly once. Zero would
  produce no result while the source offset still advances; because the EO
  resume point is the target's last publishing id, that either replays the
  source forever or publishes an empty result. The pipeline therefore rejects
  a zero-`next` return with a contract error. This is a pipeline-mode rule, not
  a filter capability flag.
- `CanSkip`, `SkipHandler`, and `AllowPipelineSkip` are removed. There is no
  configuration rejection based on a filter's skip capability and no endpoint
  opt-in.
- `Payload` is the untyped live publish object. `Message` on a typed publish
  context is `(T)Payload`.
- `Raw` is the transport `ConsumeContext`. `Message` on a typed consume
  context is the eagerly deserialized `T`, shared with the consumer.
- `MessageType` is virtual: typed contexts override it with non-null
  `typeof(T)`, and a universal filter reading the same object through the base
  type sees the same non-null value.
- `StreamConsumeFilterContext.TypedMessage` (internal) is removed. The typed
  message lives only on `StreamConsumeFilterContext<T>.Message`.
- `Inner` is removed from both typed contexts.
- Filter lifetime, ordering, open-generic closure, eager deserialization,
  per-message DI scope, and effectively-once result flow remain unchanged.

### Filtering versus skipping

Returning without `next()` is a normal consume-filter decision, not a special
operation. A filter can inspect headers, payloads, or services and handle the
message itself. The consumer is skipped and the offset advances because the
filter accepted responsibility for the message.

MassTransit behaves the same way: a filter short-circuits by not calling
`next`. StreamBus does not need a capability declaration; the only special
case is effectively-once, where the pipeline must produce a result to advance
the target publishing id.

## XML documentation

Scope of this change: every public member of the filter surface and the public
types those members expose directly:

- filter interfaces and delegates
- both publish contexts and both consume contexts
- `IStreamHeaderWriter`
- `ConsumeContext` and `MessageMetadata` (reachable through `Raw`)
- `StreamBusRegistrationView` (reachable through `Bus`)

The rest of the public API is a separate documentation pass. Style rules:

- one sentence per member; state purpose, units, valid values, and default
- no restating the member name
- use `<see langword="null"/>`, `<see cref="..."/>`, `<typeparamref name="T"/>`
- methods get `<param>` and `<returns>` where they add meaning
- no marketing adjectives, no implementation detail that can drift

### Reachable public types

```csharp
/// <summary>A delivered stream message: transport identity plus the raw body.</summary>
public sealed record ConsumeContext
{
    /// <summary>Source stream name.</summary>
    public required string Stream { get; init; }

    /// <summary>Partition index within the source stream.</summary>
    public required int Partition { get; init; }

    /// <summary>Offset of this message in the partition.</summary>
    public required ulong Offset { get; init; }

    /// <summary>Routing value used for a super-stream message; <see langword="null"/> otherwise.</summary>
    public required string? RoutingValue { get; init; }

    /// <summary>Raw message bytes as delivered by the broker.</summary>
    public required ReadOnlyMemory<byte> Body { get; init; }

    /// <summary>Broker metadata and application properties.</summary>
    public required MessageMetadata Metadata { get; init; }

    /// <summary>Cancels this delivery.</summary>
    public required CancellationToken CancellationToken { get; init; }
}

/// <summary>Broker metadata and application properties for one delivery.</summary>
public sealed record MessageMetadata
{
    /// <summary>Application message id, when supplied.</summary>
    public string? MessageId { get; init; }

    /// <summary>Application correlation id, when supplied.</summary>
    public string? CorrelationId { get; init; }

    /// <summary>Application headers and properties carried with the message.</summary>
    public IReadOnlyDictionary<string, object?> ApplicationProperties { get; init; }

    /// <summary>Broker timestamp, when supplied.</summary>
    public DateTimeOffset? Timestamp { get; init; }

    /// <summary>Gets an application property as a string; <see langword="null"/> when absent or not a string.</summary>
    /// <param name="key">Property name.</param>
    public string? GetString(string key);

    /// <summary>Gets an application property as an <see cref="int"/>; <see langword="null"/> when absent or not numeric.</summary>
    /// <param name="key">Property name.</param>
    public int? GetInt(string key);

    /// <summary>Gets an application property as a <see cref="long"/>; <see langword="null"/> when absent or not numeric.</summary>
    /// <param name="key">Property name.</param>
    public long? GetLong(string key);

    /// <summary>Gets an application property as a <see cref="bool"/>; <see langword="null"/> when absent or not a boolean.</summary>
    /// <param name="key">Property name.</param>
    public bool? GetBool(string key);

    /// <summary>Deserializes a JSON application property into <typeparamref name="T"/>; <see langword="default"/> when absent.</summary>
    /// <typeparam name="T">Target type.</typeparam>
    /// <param name="key">Property name.</param>
    public T? GetJson<T>(string key);
}

/// <summary>Credential-free view of the bus that owns a publish or consume flow.</summary>
public sealed record StreamBusRegistrationView
{
    /// <summary>Virtual host.</summary>
    public required string Vhost { get; init; }

    /// <summary>Broker host name.</summary>
    public required string Host { get; init; }

    /// <summary>RabbitMQ Streams port.</summary>
    public required int StreamPort { get; init; }

    /// <summary>AMQP port.</summary>
    public required int AmqpPort { get; init; }

    /// <summary>Heartbeat interval in seconds.</summary>
    public required double HeartbeatSeconds { get; init; }
}
```

### Build

Add to `src/StreamBus/StreamBus.csproj`:

```xml
<GenerateDocumentationFile>true</GenerateDocumentationFile>
<NoWarn>$(NoWarn);CS1591</NoWarn>
```

`GenerateDocumentationFile` ships the XML file in the NuGet package, so
consumers get IntelliSense for the documented surface. `CS1591` stays
suppressed until the remaining public API is documented; removing the
suppression is the follow-up definition of done. Any other XML-doc warning
(CS1570/CS1573/CS1574/CS1587) must be fixed, not suppressed.

## Breaking changes and migration

| Surface | Before | After |
| --- | --- | --- |
| `PublishDelegate` | `(StreamPublishFilterContext)` | `()` |
| `ConsumeDelegate` | `(StreamConsumeFilterContext)` | `()` |
| Universal publish filter | `next(context)` | `next()` |
| Universal publish payload | `context.Message` | `context.Payload` |
| Typed publish filter | `next(context.Inner)` | `next()` |
| Universal consume filter | `next(context)` | `next()` |
| Universal consume raw context | `context.Message` | `context.Raw` |
| Typed consume filter | `next(context.Inner)` | `next()` |
| Typed publish context construction | `Inner = baseContext` | `Payload = message` |
| Typed consume context construction | `Inner = baseContext` + `Message = t` | `Raw = consumeContext` + `Message = t` |
| Universal consume context construction | `Message = consumeContext` | `Raw = consumeContext` |
| Consume filter zero-`next` | gated by `CanSkip` configuration checks, `SkipHandler`, and `AllowPipelineSkip` for typed endpoints | normal filtering; handler not invoked |
| `IStreamConsumeFilter.CanSkip` | capability declaration | removed |
| `IStreamConsumeFilter<T>.CanSkip` | capability declaration | removed |
| `StreamConsumeFilterContext.SkipHandler` | manual skip flag | removed |
| `IReceiveEndpointConfigurator.AllowPipelineSkip` | skip opt-in | removed |
| Effectively-once consume filter zero-`next` | configuration rejected `CanSkip => true` filters | runtime contract error; EO filters must call `next` once |

A filter that previously did:

```csharp
context.SkipHandler = true;
return Task.CompletedTask;
```

now does:

```csharp
return Task.CompletedTask; // filtered
```

A filter that previously called `next(context)` now calls `next()`. Typed
filters keep `context.Message`; `context.Raw`, `context.OutputHeaders`,
`context.Services`, and `GetPayload`/`SetPayload` keep their names.

This is a source-breaking change. The library is pre-1.0 and the 0.2.0
changelog already records breaking changes; no compatibility shims.

## Internal implementation

1. **Contexts.** `StreamPublishFilterContext` and `StreamConsumeFilterContext`
   become non-sealed base classes; typed contexts derive and are sealed.
   `MessageType` becomes virtual; typed overrides return `typeof(T)`.
2. **Publish pipeline.** `PublishPipeline` and `PublishDelegate` lose the
   context argument. `Bus.PublishThroughPipelineAsync<T>` creates
   `StreamPublishFilterContext<T>` directly (T is statically known) and passes
   it as the base type. No wrapper is allocated.
3. **Publish adapter.** `TypedPublishFilterAdapter<T>` casts the context to
   `StreamPublishFilterContext<T>` and passes `next` through. A failed cast is
   an internal configuration error naming the filter and type, not a public
   wrapper fallback.
4. **Consume pipeline.** `ConsumePipeline` is constructed with the endpoint
   message type, whether a typed context is required, and
   `bool requiresContinuation` (true only for effectively-once). It exposes
   `CreateContext(ConsumeContext, HeaderWriter, IServiceProvider)`. When a typed
   context is required, the method uses a per-type cached factory that creates
   `StreamConsumeFilterContext<T>` and reads `ConsumeContext.TypedMessage`;
   otherwise it creates the base context and reports the endpoint message type.
   The factory is compiled once per endpoint message type; no per-message
   reflection. A pipeline containing typed adapters but no message type fails
   fast at construction.
5. **Consume adapter.** `TypedConsumeFilterAdapter<T>` casts to
   `StreamConsumeFilterContext<T>` and passes the same `next` delegate. It no
   longer constructs a context.
6. **Partition consumer.** Typed and universal delivery both call
   `ConsumePipeline.CreateContext(...)`; the effectively-once path stays on
   the universal base context. The terminal is the handler directly; there is
   no `SkipHandler` check. Eager deserialization and `TypedMessage` sharing are
   unchanged.
7. **Zero-`next` enforcement.** `ConsumePipeline` allows a zero-`next` return
   when `requiresContinuation` is false. It throws the existing contract error
   when the pipeline is effectively-once (zero would produce no result) or
   when a filter calls `next` more than once.
8. **Configuration.** `ConsumerRegistry.BuildPipeline` loses the `allowSkip`
   and `CanSkip` checks. `ConsumeOptions` loses `AllowPipelineSkip`; the
   descriptor loses it too.

## Tests and docs

- Unit: universal and typed views are the same reference on publish and
  consume; `next()` zero/one semantics; header, payload, service, and message
  flow; typed `MessageType` non-null through both views; typed consume message
  is the consumer's instance; constructor/eager-deserialization failure paths;
  error names still point at the user filter.
- Filtering: a raw, universal-typed, and endpoint-typed consume filter that
  returns without `next` does not invoke the handler and stores the offset.
  More than one `next` still throws. A publish filter that returns without
  `next` still throws.
- Effectively-once: a filter that returns without `next` throws the contract
  error; a filter that calls `next` once publishes exactly one result.
- Remove all `CanSkip`, `SkipHandler`, and `AllowPipelineSkip` tests and
  replace the old skip assertions with filtering assertions.
- Replace `ContextSwapFilter`/`Super_routing_and_body_cannot_be_replaced_by_a_filter_context`
  with a test that mutation is header-only and body/routing/key stay as
  resolved. The API no longer has a context-swap path.
- Update every in-repo filter implementation and context construction:
  examples, unit tests, integration tests, docs guide snippets.
- Update `docs/guide/publish.md`, `docs/guide/consume.md`, README if needed,
  and add a 0.2.0 breaking-change entry to `CHANGELOG.md`.
- Add the XML summaries above and enable documentation generation.
- Historical specs/plans keep their original text; this document supersedes
  the shape they describe.

## Broader public API audit (proposed, not part of the filter change unless approved)

The filter refactor exposed a larger API-hygiene problem: several
implementation details are public, and several public fields duplicate or
contradict each other.

### Dead or derivable fields

| Surface | Finding | Recommendation |
| --- | --- | --- |
| `ErrorStreamOptions.RoutingValue` + `ErrorEnvelope.ErrorRoutingValue` | Written into the envelope but never read by publish, replay, or handoff logic. | Remove both; `ErrorEnvelope.TryParse` stops requiring the header. |
| `RetryPolicy.FailureMode` | Validation allows only two states: `Strict` with `MaxAttempts == null`, or `SkipAfterDeadline` with `MaxAttempts != null`. The enum is fully derivable. | Remove `FailureMode`; derive strictness from `MaxAttempts is null`. |
| `PublishResultOptions.BaseDelay`, `Multiplier`, `MaxDelay`, `MaxJitter` | Duplicate the retry knobs on `RetryPolicy`, are absent from the guide, and need a parallel validator. | Replace with one `RetryPolicy Retry` property, or remove the knobs and use the default. |
| `ConsumeContext<T>` transport fields | Duplicates `Stream`, `Partition`, `Offset`, `RoutingValue`, `Body`, `Metadata`, and `CancellationToken` from `ConsumeContext`. | Derive `ConsumeContext<T> : ConsumeContext` and add only `Message`, matching the filter-context shape. |
| `StreamBusConfigStore.ForVhost` | Returns the live `StreamBusOptions`, including `UserName` and `Password`; contradicts the credential-free `StreamBusRegistrationView` contract. | Make it internal, or return a credential-free view. |

### Confusing or overlapping surface

| Surface | Finding | Recommendation |
| --- | --- | --- |
| `ConsumeOptions.OnRetryExhausted` + `ErrorHandoff` | Observe-only callback after stream handoff; overlaps the endpoint `OnExhausted` + `ExhaustedContext` model. | Unify on `ExhaustedContext`, or document exactly when each fires. |
| `PublishOptions.IsSuperStream` + `RoutingKey` | Ignored/overwritten by typed `PublishAsync`/`PublishSuperAsync`; only meaningful for `PublishRawAsync` and internal producers. | Document them as raw-only, or split raw publish options from typed publish options. |
| `ErrorEnvelope.ErrorStream` | Stored but only read in tests; the destination is already the stream the envelope was read from. | Low priority: remove or keep for diagnostics. |

### Public plumbing that is not user API

These types are implementation details. They add IntelliSense noise, imply
extension points that do not exist, and make breaking changes harder:

- `ReceiveEndpointDescriptor`, `TypedConsumerRegistration`, `PublishRegistration`,
  `DeclaredStream`, `StreamPublishRouter`
- `StreamBusConfigStore`, `StreamBusConsumerStore`, `StreamBusEndpointStore`,
  `StreamBusPublishStore`, `StreamBusConsumeFilterStore`, `StreamBusHostingStore`
- `StreamSystemHandle`, `StreamSystemConnector`, `StreamBusConnectionSupervisor`,
  `ErrorQueueTransport` (keep interfaces only if custom transports are a
  supported extension)
- `StreamBusOptions` / `StreamBusConnectionOptions` (keep only if public
  custom-connector interfaces need them; otherwise internal)
- `StreamBusOptions.Validate()` / `StreamBusConnectionOptions.Validate()`
  (validation plumbing, not a user operation)

Recommendation: make the implementation types internal and keep
`StreamBusRegistrationView` as the public diagnostic projection. The test
assemblies already have `InternalsVisibleTo`, so this is mostly a visibility
change, not a test rewrite.

## Approaches considered

1. **One context, parameterless `next()` (chosen).** Honest for ADR-2: the
   continuation cannot replace the context, so it takes no context. One
   reference, no wrapper, smallest filter bodies. Trade-off: differs from
   MassTransit's `next.Send(context)` syntax.
2. **One context, `next(context)`.** Same context shape, familiar MassTransit
   syntax, but the argument is ignored by StreamBus's pipeline, so it remains
   ceremony. Rejected in favor of stating the actual contract.
3. **Minimal wrapper cleanup.** Keep two objects, add typed delegates, make
   `Inner` internal. Smallest diff, but keeps two instances and does not give
   universal and typed filters the same object. Rejected.
4. **Keep `CanSkip` and `SkipHandler`.** Explicit capability flag, but adds a
   second way to express filtering and a configuration gate that disagrees
   with the user's choice. Rejected by the owner: returning without `next()` is
   the filter decision.

## Definition of done

- No public `Inner` or filter-context `TypedMessage`.
- No filter calls `next` with an argument.
- Universal and typed filters receive the same context reference.
- No `CanSkip`, `SkipHandler`, or `AllowPipelineSkip` anywhere.
- Consume filters may return without `next` on raw and typed endpoints; the
  handler is not invoked and the offset advances.
- Effectively-once consume filters must call `next` exactly once; zero throws a
  contract error.
- Every public member in the documentation scope has an XML summary; the
  package generates the XML documentation file.
- `dotnet build` and the test suite pass; brokered integration tests pass when
  a broker is available.
- Active docs and examples use `next()`, `Payload`, and `Raw` correctly.
- Changelog records the breaking API change.
- The broader API audit remains a separate decision unless explicitly folded
  into this change.
