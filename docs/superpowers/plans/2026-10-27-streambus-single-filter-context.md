# Single Filter Context + Clear-Cut Cleanup — Implementation Plan

> **For the implementer:** Execute this plan task by task. Each task is test-first
> and ends with targeted tests plus a build. Do not mix the API cleanup into the
> filter migration until the filter tests are green.

**Goal:** Give every publish/delivery one filter context, make `next` parameterless,
turn consume filtering into a normal zero-`next` decision, remove the skip machinery,
and apply the approved clear-cut API cleanup.

**Architecture:** The pipeline owns one context per publish or delivery. Universal
filters see the base context; typed filters see the same object through a derived
typed context. No wrapper context and no `Inner`. The publish delegate is always
called exactly once. The consume delegate is called zero or one time; zero means
the filter handled/filtered the message. Effectively-once consume pipelines require
exactly one call because a missing result would replay forever.

**Tech stack:** .NET 10, xUnit, RabbitMQ.Stream.Client, Microsoft.Extensions.DependencyInjection.
Tests use `InternalsVisibleTo` for unit/integration assemblies.

**Source design:** `docs/superpowers/specs/2026-10-27-streambus-single-filter-context-design.md`

## Global Constraints

- One context object per publish or delivery. Never create a second context just
  to expose `T`.
- `PublishDelegate` and `ConsumeDelegate` are parameterless.
- Publish filters call `next()` exactly once.
- Normal/raw/typed consume filters call `next()` zero or one time. Zero means the
  handler is not invoked and the offset advances normally.
- Effectively-once consume filters call `next()` exactly once. Zero is a contract
  error and must not be retried in-handler.
- Remove `CanSkip`, `SkipHandler`, `AllowPipelineSkip`, and all related validation,
  options, tests, and docs. Do not replace them with a new capability flag.
- Cleanup scope is limited to the approved clear-cut items: dead error-routing
  fields, `FailureMode`, `PublishResultOptions` retry knobs, credential exposure
  through `ForVhost`, and implementation plumbing visibility. Do not unify
  `ConsumeContext<T>` or the two exhaustion callback shapes in this change.
- Preserve the user's uncommitted changes in
  `src/StreamBus/FilterTypeInspector.cs` and
  `tests/StreamBus.UnitTests/TypedFilterRegistrationTests.cs`. Update only the
  filter call syntax in the latter; keep the added open-generic tests' intent.
- Do not commit while the user's existing worktree changes are present. Leave the
  complete diff for review.
- Keep existing retry, exhaustion, effectively-once, header, and per-message DI
  scope behavior unless this plan explicitly changes it.
- Build command: `dotnet build StreamBus.slnx`
- Unit command: `dotnet test tests/StreamBus.UnitTests/StreamBus.UnitTests.csproj`
- Integration command (only when a broker is available):
  `dotnet test tests/StreamBus.IntegrationTests/StreamBus.IntegrationTests.csproj`

## Review Focus

These are the failure modes this plan must prove, not just assume:

1. A normal/raw/typed consume filter that returns without `next()` does not invoke
   the handler and still advances the offset. (Task 2)
2. A typed endpoint still deserializes before typed filters; universal-only
   endpoints keep the lazy path and avoid deserialization when filtered. (Task 2)
3. An effectively-once filter that returns without `next()` throws a contract error
   and is not caught by the EO in-handler retry loop. (Task 2)
4. A publish filter that returns without `next()` is still a configuration/contract
   error; publish cannot be dropped. (Task 1)
5. Universal and typed filters on the same delivery receive the same context
   reference, headers, services, payload bag, and raw message. (Tasks 1 and 2)

---

## Task 1: Publish Filter Contract

**Files**
- Modify: `src/StreamBus/PublishPipeline.cs`
- Modify: `src/StreamBus/FilterAdapters.cs`
- Modify: `src/StreamBus/Bus.cs`
- Test: `tests/StreamBus.UnitTests/PublishPipelineTests.cs`
- Test: `tests/StreamBus.UnitTests/TypedFilterContractTests.cs`
- Test: `tests/StreamBus.UnitTests/PipelineMetricsTests.cs`
- Test: `tests/StreamBus.IntegrationTests/TypedFilterIntegrationTests.cs`

**Test first**

1. In `PublishPipelineTests.cs`, change every test filter to the new signature:
   - `return next();`
   - no `next(context)` calls.
   - universal context reads `context.Payload`, not `context.Message`.
2. Add `Universal_and_typed_publish_filters_share_one_context_instance` in
   `PublishPipelineTests.cs`:
   - register one universal and one typed filter for `PipelineOrder`;
   - both record the `StreamPublishFilterContext` reference;
   - publish `new PipelineOrder("p-1")`;
   - assert `Assert.Same(universalContext, typedContext)`;
   - assert the universal filter sees `MessageType == typeof(PipelineOrder)` and
     `Payload` is the same instance; the typed filter sees `Message.Id == "p-1"`.
3. Replace `ContextSwapFilter`/`Super_routing_and_body_cannot_be_replaced_by_a_filter_context`
   with `HeaderOnlyFilter`/`Filter_headers_do_not_change_routing_key_or_body`:
   - a super publish filter sets only `context.Headers.Set("x-filter", "1")`;
   - assert the captured routing key and body are still the original values.
4. In `TypedFilterContractTests.cs`:
   - `Universal_publish_context_reports_declared_and_runtime_types` uses `Payload`;
   - `Typed_publish_adapter_names_the_user_filter_and_forwards_the_typed_message`
     constructs `StreamPublishFilterContext<ContractDerived>` directly and asserts
     the adapter forwards that same context (no wrapper);
   - `ContractTypedPublishFilter` calls `next()`.
5. Update `PipelineMetricsTests.NoopPublishFilter` to `next()`.
6. Update `TypedFilterIntegrationTests` typed publish filters to `next()` and
   `context.Payload` where applicable.
7. Run the targeted tests and confirm they fail with the expected compile errors
   and/or old-context assertions:

```powershell
dotnet test tests/StreamBus.UnitTests/StreamBus.UnitTests.csproj --filter "FullyQualifiedName~PublishPipelineTests|FullyQualifiedName~TypedFilterContractTests|FullyQualifiedName~PipelineMetricsTests"
```

**Implement**

1. `PublishPipeline.cs`:
   - `public delegate Task PublishDelegate();`
   - `public class StreamPublishFilterContext` (not sealed).
   - Replace `object Message` with `required object Payload`.
   - `RuntimeMessageType => Payload.GetType()`.
   - Make `MessageType` virtual:
     ```csharp
     public virtual Type? MessageType { get; init; }
     ```
   - `public sealed class StreamPublishFilterContext<T> : StreamPublishFilterContext`
     with:
     ```csharp
     public T Message => (T)Payload;
     public override Type? MessageType => typeof(T);
     ```
   - `PublishPipeline.RunAsync(StreamPublishFilterContext context, PublishDelegate terminal, CancellationToken ct)`.
   - `InvokeAsync` uses a local `Task Next()` that calls the next index, and calls
     `terminal()` at the end.
   - Keep the existing zero/double `next` errors and their filter names. Zero next
     remains an error for publish.
2. `FilterAdapters.cs`:
   - `TypedPublishFilterAdapter<T>.OnPublish` casts the incoming context to
     `StreamPublishFilterContext<T>` and calls the user filter. Do not construct a
     wrapper.
   - Keep `ITypedFilterAdapter.FilterType` so error messages name the user filter.
3. `Bus.cs`:
   - Add a generic context-creation helper used by `PublishAsync`,
     `PublishSuperAsync`, and explicit-stream typed publish:
     ```csharp
     private StreamPublishFilterContext<T> CreatePublishContext<T>(
         T message, string stream, string? routingKey, IStreamHeaderWriter headers,
         IServiceProvider services)
         => new()
         {
             Stream = stream,
             RoutingKey = routingKey,
             Payload = message!,
             Headers = headers,
             Bus = _busView,
             Services = services,
         };
     ```
   - The typed context's overridden `MessageType` reports `typeof(T)`.
   - A universal filter sees the same object as the base context.
   - Raw publish remains outside publish filters.
4. Do not change publish routing/body semantics: no filter can replace the
   stream, routing key, or payload.

**Verify**

```powershell
dotnet test tests/StreamBus.UnitTests/StreamBus.UnitTests.csproj --filter "FullyQualifiedName~PublishPipelineTests|FullyQualifiedName~TypedFilterContractTests|FullyQualifiedName~PipelineMetricsTests"
```

All pass. Confirm `context.Inner` no longer appears in publish code/tests.

---

## Task 2: Consume Filter Contract and Skip Removal

**Files**
- Modify: `src/StreamBus/ConsumePipeline.cs`
- Modify: `src/StreamBus/FilterAdapters.cs`
- Modify: `src/StreamBus/ConsumerRegistry.cs`
- Modify: `src/StreamBus/StreamPartitionConsumer.cs`
- Modify: `src/StreamBus/Configuration/StreamBusConfigurator.cs`
- Modify: `src/StreamBus/ConsumeOptions.cs`
- Modify: `src/StreamBus/Exceptions.cs`
- Test: `tests/StreamBus.UnitTests/PipelineSemanticsTests.cs`
- Test: `tests/StreamBus.UnitTests/TypedConsumeFilterTests.cs`
- Test: `tests/StreamBus.UnitTests/TypedFilterContractTests.cs`
- Test: `tests/StreamBus.UnitTests/TypedFilterRegistrationTests.cs`
- Test: `tests/StreamBus.UnitTests/PipelineMetricsTests.cs`
- Test: `tests/StreamBus.IntegrationTests/ConsumePipelineIntegrationTests.cs`
- Test: `tests/StreamBus.IntegrationTests/TypedFilterIntegrationTests.cs`

**Test first**

1. In `PipelineSemanticsTests.cs`, update all filters and harness calls to the new
   parameterless delegate:
   - `next()` instead of `next(context)`.
   - remove `AllowPipelineSkip` from `DispatchOptions` and `ConsumeOptions`.
   - remove `SkipHandler` from test filters.
2. Replace the old zero/skip matrix with these tests:
   - `Zero_next_filters_without_invoking_the_terminal`:
     direct `ConsumePipeline` with `new ZeroNextFilter()`; terminal call count 0;
     no exception.
   - `Zero_next_on_a_raw_endpoint_advances_the_offset_without_handler_or_error_publish`:
     derived from the old raw skip test; no error envelope; handler 0; stored offset.
   - `Zero_next_on_a_typed_endpoint_advances_the_offset_once`:
     typed endpoint; typed filter returns without `next()`; handler 0; one stored offset.
   - `Zero_next_delivery_is_filtered_and_not_published_to_error`:
     endpoint has an error stream; zero-next filter; no error publish; handler 0;
     stored offset.
   - `Effectively_once_zero_next_is_a_filter_contract_error`:
     `new ConsumePipeline(..., requiresContinuation: true)` with `ZeroNextFilter`;
     assert `StreamBusFilterContractException` and terminal 0.
3. Delete tests/helpers that only exist for the removed contract:
   - `CanSkip_filter_on_an_effectively_once_pipeline_is_rejected`
   - `CanSkip_filter_on_a_typed_endpoint_without_AllowPipelineSkip_is_rejected`
   - `Zero_next_with_skip_and_allowSkip_bypasses_the_terminal`
   - `Zero_next_with_skip_but_allowSkip_false_is_transient`
   - `Zero_next_without_skip_is_transient_and_names_the_filter`
   - `SkipFilter` skip flag usage; rename to `FilteringFilter` if still useful.
4. In `TypedConsumeFilterTests.cs`:
   - change all filters to `next()` and remove `CanSkip`/`SkipHandler`;
   - delete `Typed_can_skip_filter_on_a_typed_endpoint_without_AllowPipelineSkip_is_rejected`;
   - rename `Typed_skip_does_not_avoid_deserialization` to
     `Typed_zero_next_still_deserializes_but_skips_handler`;
   - rename `Universal_only_skip_still_avoids_deserialization` to
     `Universal_only_zero_next_still_avoids_deserialization`;
   - rename `Universal_filter_sees_raw_context_and_typed_message_slot` to
     `Universal_filter_sees_raw_context_and_declared_message_type`; assert
     `context.Raw`, `context.MessageType`, and `context.OutputHeaders` instead of
     the removed internal `TypedMessage` slot;
   - replace `Typed_filter_zero_next_error_names_the_user_filter_not_the_adapter`
     with `Effectively_once_typed_filter_zero_next_names_the_user_filter_not_the_adapter`
     using `requiresContinuation: true` and a typed pipeline context;
   - update `PipelineContext()` to return `StreamConsumeFilterContext<TypedOrderMessage>`
     with `Raw` and `Message`;
   - add `Universal_and_typed_consume_filters_share_one_context_instance`: register
     a universal and a typed filter; both record the context; assert `Assert.Same`,
     same `Raw`, same `OutputHeaders`, same `Services`, and the typed filter sees
     `Message`.
5. In `TypedFilterContractTests.cs`:
   - `Typed_consume_adapter_forwards_payload_and_typed_message` (renamed from
     `...skip_payload...`) constructs a typed context with `Raw` and `Message`;
   - `Typed_contexts_share_headers_payloads_services_and_raw_context` (renamed)
     asserts shared headers/services/raw/payload bag and no `Inner`;
   - `ContractTypedConsumeFilter` calls `next()` and no longer sets `SkipHandler`.
6. In `TypedFilterRegistrationTests.cs`, update only the filter bodies from
   `next(context)`/`next(context.Inner)` to `next()`. Do not remove the user's new
   open-generic tests.
7. Update `PipelineMetricsTests.NoopConsumeFilter` to `next()`.
8. Update integration test filters in `ConsumePipelineIntegrationTests.cs` and
   `TypedFilterIntegrationTests.cs` to the new syntax; remove `CanSkip`/`SkipHandler`.
9. Run the targeted tests and confirm they fail before implementation:

```powershell
dotnet test tests/StreamBus.UnitTests/StreamBus.UnitTests.csproj --filter "FullyQualifiedName~PipelineSemanticsTests|FullyQualifiedName~TypedConsumeFilterTests|FullyQualifiedName~TypedFilterContractTests|FullyQualifiedName~TypedFilterRegistrationTests|FullyQualifiedName~PipelineMetricsTests"
```

**Implement**

1. `ConsumePipeline.cs`:
   - `public delegate Task ConsumeDelegate();`
   - `public class StreamConsumeFilterContext` (not sealed):
     ```csharp
     public required ConsumeContext Raw { get; init; }
     public virtual Type? MessageType { get; init; }
     public required IStreamHeaderWriter OutputHeaders { get; init; }
     public required IServiceProvider Services { get; init; }
     public TPayload? GetPayload<TPayload>() ...
     public void SetPayload<TPayload>(TPayload value) ...
     ```
   - Delete `Message`, `Inner`, `SkipHandler`, and `TypedMessage` from the universal
     context.
   - `public sealed class StreamConsumeFilterContext<T> : StreamConsumeFilterContext`
     with `required T Message` and `override Type? MessageType => typeof(T)`.
   - `ConsumePipeline` constructor:
     ```csharp
     public ConsumePipeline(
         IReadOnlyList<IStreamConsumeFilter> filters,
         Type? endpointMessageType = null,
         bool requiresTypedMessage = false,
         bool requiresContinuation = false)
     ```
     Validate that `requiresTypedMessage` has a non-null endpoint message type.
   - Add `CreateContext(ConsumeContext raw, IStreamHeaderWriter headers, IServiceProvider services)`:
     - if `requiresTypedMessage`, call a cached generic factory that constructs
       `StreamConsumeFilterContext<T>` with `Raw`, `Message` from `raw.TypedMessage`,
       `OutputHeaders`, and `Services`;
     - otherwise construct the base context with `MessageType = endpointMessageType`.
     - cache the per-type factory in a static `ConcurrentDictionary<Type, Func<...>>`;
       do not reflect per message.
   - `RunAsync(StreamConsumeFilterContext context, ConsumeDelegate terminal, CancellationToken ct)`
     no longer takes `allowSkip`.
   - `InvokeAsync` uses a parameterless local `Task Next()`.
   - After the filter returns:
     - `calls == 0` and `requiresContinuation`: throw
       `StreamBusFilterContractException` naming the user filter;
     - `calls == 0`: return normally (filtered);
     - `calls > 1`: keep the existing transient error for normal pipelines; use
       `StreamBusFilterContractException` when `requiresContinuation`.
   - Keep pipeline duration metrics.
2. `Exceptions.cs`: add internal
   `StreamBusFilterContractException : StreamBusException` for filter contract
   violations that must bypass normal retry.
3. `FilterAdapters.cs`: typed consume adapter casts the incoming context to
   `StreamConsumeFilterContext<T>` and calls the user filter. No wrapper, no
   `CanSkip` forwarding.
4. `ConsumerRegistry.cs`:
   - `BuildPipeline` loses `typedEndpoint` and `allowSkip` parameters and the
     `CanSkip` validation loop.
   - Pass `options.MessageType`, `filters.Any(f => f is ITypedFilterAdapter)`, and
     `effectivelyOnce` to `ConsumePipeline`.
   - `ConsumeAsync` call site drops `AllowPipelineSkip`.
   - EO call site passes `requiresContinuation: true`.
5. `StreamPartitionConsumer.cs`:
   - remove `allowSkip` locals and parameters;
   - typed deserialization remains inside `InvokeHandlerPipelineAsync` and is still
     controlled by `_pipeline.RequiresTypedMessage`;
   - build the context via `_pipeline.CreateContext(...)`;
   - terminal delegate is `() => _handler(consumeContext)`;
   - pass `() => _handler(consumeContext)` for normal dispatch;
   - EO terminal is `() => { pipelineResult = await _eo.ResultHandler(consumeContext); }`;
   - in `DispatchEffectivelyOnceAsync`, catch and rethrow `StreamBusFilterContractException`
     before the generic `catch (Exception ex)` retry path.
6. `ConsumeOptions.cs`: remove internal `AllowPipelineSkip`.
7. `Configuration/StreamBusConfigurator.cs`:
   - remove `AllowPipelineSkip` from `IReceiveEndpointConfigurator` and its
     implementation;
   - remove `_allowPipelineSkip`, descriptor field, and `ConsumeOptions` assignment;
   - keep typed filter mismatch validation and duplicate registration checks.
8. Remove every remaining occurrence of `CanSkip`, `SkipHandler`, `AllowPipelineSkip`,
   `context.Inner`, `next(context)`, and `StreamConsumeFilterContext.TypedMessage`
   in source, tests, examples, and active docs.

**Verify**

```powershell
dotnet test tests/StreamBus.UnitTests/StreamBus.UnitTests.csproj --filter "FullyQualifiedName~PipelineSemanticsTests|FullyQualifiedName~TypedConsumeFilterTests|FullyQualifiedName~TypedFilterContractTests|FullyQualifiedName~TypedFilterRegistrationTests|FullyQualifiedName~PipelineMetricsTests"
```

All pass. Then run the whole unit suite. Confirm the EO zero-next test throws the
contract exception and does not publish a result.

---

## Task 3: Retry and Error API Cleanup

**Files**
- Modify: `src/StreamBus/RetryPolicy.cs`
- Modify: `src/StreamBus/Configuration/RetryConfiguration.cs`
- Modify: `src/StreamBus/ConsumerRegistry.cs`
- Modify: `src/StreamBus/StreamPartitionConsumer.cs`
- Modify: `src/StreamBus/StreamBusMetrics.cs`
- Modify: `src/StreamBus/EffectivelyOnce.cs`
- Modify: `src/StreamBus/ErrorEnvelope.cs`
- Modify: `src/StreamBus/ResultPublisher.cs`
- Test: `tests/StreamBus.UnitTests/RetryPolicyTests.cs`
- Test: `tests/StreamBus.UnitTests/RetryConfigurationTests.cs`
- Test: `tests/StreamBus.UnitTests/EffectivelyOnceTests.cs`
- Test: `tests/StreamBus.UnitTests/ResultPublisherTests.cs`
- Test: `tests/StreamBus.UnitTests/ErrorEnvelopeTests.cs`
- Test: `tests/StreamBus.UnitTests/PipelineSemanticsTests.cs`

**Test first**

1. `RetryPolicyTests.cs`:
   - remove `FailureMode` from every policy;
   - rename/replace the validation tests:
     - `Unbounded_with_error_stream_throws`
     - `Unbounded_with_dead_letter_throws`
     - `Bounded_without_error_stream_or_dead_letter_throws`
     - `Bounded_with_max_attempts_and_error_stream_is_accepted`
     - keep non-positive `MaxAttempts`, delay, jitter, multiplier tests.
2. `EffectivelyOnceTests.cs`:
   - `Consume_rejects_bounded` uses `new RetryPolicy { MaxAttempts = 3 }`;
   - `Consume_accepts_default_unbounded` uses `new RetryPolicy()`;
   - remove `FailureMode` references and change messages if needed.
3. `ResultPublisherTests.cs`:
   - use `PublishResultOptions { Retry = new RetryPolicy { ... } }` where tuning was
     tested;
   - keep delay/cancellation behavior tests.
4. `ErrorEnvelopeTests.cs` and `PipelineSemanticsTests.cs`:
   - remove assertions for `ErrorRoutingValue` and the
     `streambus.error.error-routing-value` header.
5. Add a test that `PublishResultOptions.Retry` drives the retry delay (or keep an
   existing test proving the configured `RetryPolicy` is used).
6. Run the targeted tests and confirm they fail:

```powershell
dotnet test tests/StreamBus.UnitTests/StreamBus.UnitTests.csproj --filter "FullyQualifiedName~RetryPolicyTests|FullyQualifiedName~RetryConfigurationTests|FullyQualifiedName~EffectivelyOnceTests|FullyQualifiedName~ResultPublisherTests|FullyQualifiedName~ErrorEnvelopeTests|FullyQualifiedName~PipelineSemanticsTests"
```

**Implement**

1. `RetryPolicy.cs`:
   - delete `FailureMode` enum and property;
   - delete `ErrorStreamOptions.RoutingValue`;
   - delete `PublishResultOptions.BaseDelay`, `Multiplier`, `MaxDelay`, `MaxJitter`;
   - add `public RetryPolicy Retry { get; init; } = new();`.
2. Validation:
   - unbounded = `MaxAttempts is null`; unbounded + error stream/dead letter is a
     configuration error;
   - bounded = `MaxAttempts is not null`; `MaxAttempts <= 0` is an error; bounded
     without an error stream, dead letter, or endpoint callback is an error;
   - effectively-once consume validation requires `MaxAttempts is null` and no
     error stream/dead letter.
3. `RetryConfiguration.cs`:
   - remove all `FailureMode = ...` assignments;
   - `RetryDerivation` already uses `MaxAttempts` for boundedness; keep it.
4. `ConsumerRegistry.cs`:
   - replace `FailureMode` checks with `MaxAttempts` checks;
   - `PublishResultAsync` validates `options.Retry` as unbounded and resolves
     delays through `RetryDelays.ResolveDelay`;
   - remove any now-unused retry-knob validation.
5. `StreamPartitionConsumer.cs`:
   - `strict` becomes `_options.Retry.MaxAttempts is null`;
   - log mode as `"unbounded"` / `"bounded"` (or remove the mode field);
   - remove `ErrorRoutingValue` from both error envelope constructions.
6. `StreamBusMetrics.cs`: remove the `failure_mode` tag from
   `streambus_partition_retry_blocked`; keep the metric.
7. `EffectivelyOnce.cs`: update `ValidateConsume` and `ValidatePublishResult` for
   the new fields; keep the “EO must not skip” rule expressed as unbounded retry
   plus exactly-one result.
8. `ResultPublisher.cs`: use `PublishResultOptions.Retry` and
   `RetryDelays.ResolveDelay`; preserve the existing clamping/fallback behavior.
9. `ErrorEnvelope.cs`: remove `ErrorRoutingValue` and its serialized/parsing key.

**Verify**

```powershell
dotnet test tests/StreamBus.UnitTests/StreamBus.UnitTests.csproj --filter "FullyQualifiedName~RetryPolicyTests|FullyQualifiedName~RetryConfigurationTests|FullyQualifiedName~EffectivelyOnceTests|FullyQualifiedName~ResultPublisherTests|FullyQualifiedName~ErrorEnvelopeTests|FullyQualifiedName~PipelineSemanticsTests"
```

All pass.

---

## Task 4: Visibility and Credential Surface Cleanup

**Files**
- Modify: `src/StreamBus/Hosting.cs`
- Modify: `src/StreamBus/Configuration/StreamBusConfigurator.cs`
- Modify: `src/StreamBus/ConsumePipeline.cs`
- Modify: `src/StreamBus/PublishRegistration.cs`
- Modify: `src/StreamBus/DeclareSet.cs`
- Modify: `src/StreamBus/Registry.cs`
- Modify: `src/StreamBus/ConnectionSupervisor.cs`
- Modify: `src/StreamBus/Connector.cs`
- Modify: `src/StreamBus/ErrorQueueTransport.cs`
- Modify: `src/StreamBus/Options.cs`
- Tests: compile-only; existing tests use `InternalsVisibleTo`.

**Test first**

1. No behavior test is needed. Use the existing suite as the regression net after
   each visibility change.
2. Before editing, run the full unit suite once to establish the green baseline.

**Implement**

1. Make these internal:
   - `StreamBusConfigStore` (including `ForVhost`);
   - `StreamBusConsumerStore`, `StreamBusEndpointStore`, `StreamBusPublishStore`,
     `StreamBusConsumeFilterStore`;
   - `StreamBusHostingStore`;
   - `TypedConsumerRegistration`;
   - `ReceiveEndpointDescriptor`;
   - `PublishRegistration`;
   - `DeclaredStream`;
   - `StreamPublishRouter`;
   - `StreamBusRegistry`;
   - `StreamBusConnectionSupervisor`;
   - `StreamSystemHandle`, `StreamSystemConnector`;
   - `ErrorQueueTransport`, `IErrorQueueConnectionFactory`.
2. Keep these public:
   - `IStreamSystemConnector`, `IStreamSystemHandle`, and `StreamBusOptions` /
     `StreamBusConnectionOptions` because the connector extension point exposes
     them;
   - `StreamBusRegistrationView` as the credential-free diagnostic projection;
   - `StreamBusHostingOptions` and `StreamBusHostingOptionsBuilder` because
     `AddStreamBusHosting` exposes the builder type;
   - `IStreamBusRegistry` and the filter interfaces/contexts.
3. Make `StreamBusOptions.Validate()` and `StreamBusConnectionOptions.Validate()`
   internal.
4. Do not change `IStreamSystemConnector`/`IStreamSystemHandle` signatures.
5. Do not expose `StreamBusOptions` through any public store accessor. If a
   credential-free diagnostic is needed, use `StreamBusRegistrationView`.

**Verify**

```powershell
dotnet build StreamBus.slnx
dotnet test tests/StreamBus.UnitTests/StreamBus.UnitTests.csproj
```

All pass. If a public signature requires a type that was made internal, make only
that concrete type internal or change the signature to the internal surface; do
not widen the type back unless the signature is a deliberate extension point.

---

## Task 5: XML Docs, Guides, Examples, and Verification

**Files**
- Modify: `src/StreamBus/StreamBus.csproj`
- Modify: `src/StreamBus/PublishPipeline.cs`
- Modify: `src/StreamBus/ConsumePipeline.cs`
- Modify: `src/StreamBus/ConsumeContext.cs`
- Modify: `src/StreamBus/MessageMetadata.cs`
- Modify: `src/StreamBus/PublishPipeline.cs` (defines `IStreamHeaderWriter`)
- Modify: `src/StreamBus/Hosting.cs` (registration view)
- Modify: `docs/guide/publish.md`
- Modify: `docs/guide/consume.md`
- Modify: `docs/guide/effectively-once.md` if it references filtering
- Modify: `docs/adr/ADR-2-middleware-shape.md` (amendment note)
- Modify: `examples/StreamBus.Examples.Producer/*.cs`
- Modify: `examples/StreamBus.Examples.Consumer/*.cs`
- Modify: `CHANGELOG.md`
- Modify: `docs/superpowers/specs/2026-10-27-streambus-single-filter-context-design.md`
  (status -> accepted/implemented after verification)

**Implement**

1. XML documentation:
   - Add concise `<summary>` docs to every public member in the filter surface:
     `IStreamPublishFilter`, `IStreamPublishFilter<T>`, `PublishDelegate`,
     `StreamPublishFilterContext`, `StreamPublishFilterContext<T>`,
     `IStreamConsumeFilter`, `IStreamConsumeFilter<T>`, `ConsumeDelegate`,
     `StreamConsumeFilterContext`, `StreamConsumeFilterContext<T>`,
     `IStreamHeaderWriter`.
   - Document the exact contract:
     - publish calls `next()` exactly once;
     - consume calls `next()` zero or one time;
     - zero means the filter handled/filtered the message and the handler is not
       invoked;
     - effectively-once filters must call `next()` exactly once;
     - callers should `await next()`.
   - Document `Payload`, `Raw`, `Message`, `MessageType`, `RuntimeMessageType`,
     `OutputHeaders`, `Services`, `Bus`, and the payload bag.
   - Add docs to `ConsumeContext` and `MessageMetadata` members reachable from
     `Raw`.
2. `StreamBus.csproj`:
   ```xml
   <GenerateDocumentationFile>true</GenerateDocumentationFile>
   <NoWarn>$(NoWarn);1591</NoWarn>
   ```
   Use `1591`, not a permanent suppression of all docs work.
3. Active guides:
   - `publish.md`: show `return next();`, `context.Payload` for universal filters,
     `context.Message` for typed filters, and one context.
   - `consume.md`: show `return next();`; zero-next filtering; no skip capability;
     typed filter context; eager deserialization for typed filters vs lazy for
     universal-only pipelines; EO exactly-one rule.
   - `effectively-once.md`: state that EO filters must call `next()` exactly once
     and that zero is a runtime contract error.
4. Examples:
   - producer/consumer filters use `next()`, `Payload`/`Raw`, no `Inner`, no skip.
5. ADR-2:
   - add an amendment note that the skip semantics described in the ADR were
     superseded by the 2026-10-27 single-context design; do not rewrite history.
6. `CHANGELOG.md`:
   - add a breaking-change entry for parameterless `next`, one context, zero-next
     consume filtering, removal of skip APIs, and the clear-cut API cleanup.
7. Update the design status from proposed to accepted/implemented after tests pass.

**Verify**

```powershell
dotnet build StreamBus.slnx
dotnet test tests/StreamBus.UnitTests/StreamBus.UnitTests.csproj
git diff --check
```

Then search the active source/docs/examples for stale syntax:

```powershell
Get-ChildItem -Path src,tests,examples,docs/guide,docs/adr -Recurse -Include *.cs,*.md |
  Select-String -Pattern 'CanSkip|SkipHandler|AllowPipelineSkip|context\.Inner|next\(context\)|\.Message\b'
```

Expected: no active-code hits for the removed API; `.Message` still appears for
typed context members and `ConsumeContext<T>.Message`, which are valid.

If a broker is available, run:

```powershell
dotnet test tests/StreamBus.IntegrationTests/StreamBus.IntegrationTests.csproj
```

Report the exact commands and results. Do not claim success without the output.

---

## Final Acceptance Checklist

- [x] One context per publish/delivery; universal and typed filters share the
      reference.
- [x] Publish filters call `next()` exactly once; zero still errors.
- [x] Normal/raw/typed consume filters may return without `next()`; handler is
      skipped and offset advances.
- [x] Typed endpoints still deserialize before typed filters; universal-only
      endpoints keep lazy deserialization.
- [x] EO filters must call `next()` exactly once; zero throws
      `StreamBusFilterContractException` and is not retried in-handler.
- [x] `CanSkip`, `SkipHandler`, `AllowPipelineSkip` and all related tests/docs are
      gone.
- [x] `ErrorRoutingValue`/`ErrorStreamOptions.RoutingValue` are gone.
- [x] `FailureMode` is gone; retry strictness comes from `MaxAttempts`.
- [x] `PublishResultOptions` uses one `RetryPolicy`, not four duplicate knobs.
- [x] `StreamBusConfigStore.ForVhost` is internal; no public store exposes
      credentials.
- [x] Implementation plumbing is internal; `InternalsVisibleTo` covers tests.
- [x] Filter surface and reachable public members have XML docs.
- [x] Build, unit tests, and (if available) integration tests pass.
- [x] User's uncommitted changes are preserved and not accidentally committed.
