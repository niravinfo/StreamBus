# StreamBus Typed Publish/Consume Filters Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add per-message-type `IStreamPublishFilter<T>` / `IStreamConsumeFilter<T>` attachment while preserving universal filters, ordering, skip/retry semantics, and sharing one eagerly deserialized message instance between typed consume filters and the consumer.

**Architecture:** Universal filters stay on the existing non-generic contracts. Typed filters are resolved per declared/endpoint message type, wrapped in internal adapters that expose the existing universal pipeline delegates, and cached per bus/type. Publish pipelines are built eagerly for registered types and lazily for explicit unregistered types. Consume pipelines attach typed filters at endpoint attach; when at least one typed filter is present, the partition consumer deserializes once per attempt before the pipeline and stores the object on `ConsumeContext.TypedMessage`.

**Tech Stack:** .NET 10 (`net10.0`), RabbitMQ.Stream.Client, Microsoft.Extensions.DependencyInjection/Hosting, xunit.v3.

**Spec:** `docs/superpowers/specs/2026-10-25-streambus-typed-filter-design.md` — the plan argues from the spec; executors read both.

## Global Constraints

- Breaking changes are acceptable; keep the existing non-generic filters as universal filters that run for every message in their scope.
- Preserve exactly: registration order, bus-wide filters before endpoint filters, exactly-one-`next`, `CanSkip` rules, effectively-once rejection of `CanSkip`, one per-message DI scope shared by filters and the typed consumer, retry/exhaustion behavior, and no second deserialization.
- Typed consume filters are eager: when any typed filter attaches, deserialize before the consume pipeline per attempt and share that exact object with the handler.
- No wire envelope/metadata, no per-publish-stream filters, no polymorphic dispatch, no typed raw/effectively-once filters, no change to raw/result/replay/requeue paths.
- The `next` delegate remains the existing universal `PublishDelegate` / `ConsumeDelegate`; typed filters continue the chain with `next(context.Inner)` as the spec's public surface shows.
- Public context members are additive; directly constructed universal test contexts must still compile.
- `TreatWarningsAsErrors` stays ON; `dotnet build StreamBus.slnx -v:minimal` must report 0 warnings and 0 errors after every task.
- Every task's unit-test gate: `dotnet test tests/StreamBus.UnitTests/StreamBus.UnitTests.csproj --no-build --nologo` is fully green.
- Integration tests need a broker on `localhost:5552`; when unavailable set `$env:STREAMBUS_SKIP_INTEGRATION='1'` and confirm they skip cleanly.
- Commit once per task, after the build and test gates pass.

## Review Focus

These are the five typed-filter failure modes most likely to bite a user; each is pinned by a test in the task that owns the code.

1. **A bus-wide concrete typed filter that does not match an endpoint must be silently omitted** — not an error, and it must not shift the order of the remaining filters. Pinned by `Non_matching_bus_typed_filter_is_omitted_without_changing_order` (Task 4).
2. **An endpoint-scoped concrete typed filter that does not match the endpoint message type must fail during `AddStreamBus`** — silently never running it hides a configuration bug. Pinned by `Endpoint_typed_consume_filter_mismatch_is_rejected_during_AddStreamBus` (Task 2).
3. **A malformed body on an endpoint with a typed filter must fail before filters and the handler run**, then enter the existing retry/exhaustion path. Pinned by `Typed_deserialization_failure_retries_and_never_runs_filters_or_handler` (Task 5).
4. **Universal-only skip must still avoid deserialization; typed-filter skip must not** — adding a typed filter changes timing, and `CanSkip` still requires `AllowPipelineSkip` on typed endpoints. Pinned by `Universal_only_skip_still_avoids_deserialization` and `Typed_skip_does_not_avoid_deserialization` (Task 5).
5. **An open-generic filter that cannot close for a registered message type must throw a configuration error at bus creation**, not be silently skipped. Pinned by `Open_generic_constraint_violation_is_a_configuration_error` (Task 3).

---

### Task 1: Typed filter contracts, contexts, and adapters

**Files:**
- Modify: `src/StreamBus/PublishPipeline.cs`
- Modify: `src/StreamBus/ConsumePipeline.cs`
- Modify: `src/StreamBus/ConsumeContext.cs`
- Create: `src/StreamBus/FilterAdapters.cs`
- Create: `tests/StreamBus.UnitTests/TypedFilterContractTests.cs`

**Interfaces:**
- Consumes: existing `PublishDelegate`, `ConsumeDelegate`, `StreamPublishFilterContext`, `StreamConsumeFilterContext`, `ConsumeContext`, `HeaderWriter`.
- Produces:
  - `public interface IStreamPublishFilter<T>` with `Task OnPublish(StreamPublishFilterContext<T> context, PublishDelegate next, CancellationToken ct)`.
  - `public sealed class StreamPublishFilterContext<T>` with `required StreamPublishFilterContext Inner`, `T Message`, `Type MessageType`, `Type RuntimeMessageType`, `Stream`, `RoutingKey`, `Headers`, `Bus`, `Services`.
  - `public interface IStreamConsumeFilter<T>` with default `bool CanSkip => false` and `Task OnConsume(StreamConsumeFilterContext<T> context, ConsumeDelegate next, CancellationToken ct)`.
  - `public sealed class StreamConsumeFilterContext<T>` with `required StreamConsumeFilterContext Inner`, `required T Message`, `Type MessageType`, `ConsumeContext Raw`, `OutputHeaders`, `Services`, `SkipHandler`, `GetPayload<TPayload>()`, `SetPayload<TPayload>(TPayload)`.
  - Universal `StreamPublishFilterContext` gains `Type? MessageType { get; init; }` and `Type RuntimeMessageType => Message.GetType()`.
  - Universal `StreamConsumeFilterContext` gains `Type? MessageType { get; init; }` and `internal object? TypedMessage { get; init; }`.
  - `internal object? TypedMessage` on `ConsumeContext`.
  - `internal interface ITypedFilterAdapter { Type FilterType { get; } }`, `internal static class FilterIdentity { static Type TypeOf(object filter); }`, `internal static class TypedFilterAdapter { static IStreamPublishFilter Publish(Type messageType, object filter); static IStreamConsumeFilter Consume(Type messageType, object filter); }`.

- [ ] **Step 1: Write the failing contract tests**

Create `tests/StreamBus.UnitTests/TypedFilterContractTests.cs` with local records `ContractBase`/`ContractDerived`, a typed publish filter, a typed consume filter, and these assertions:

```csharp
[Fact]
public void Universal_publish_context_reports_declared_and_runtime_types()
{
    var context = NewPublishContext(new ContractDerived("d-1"), declared: typeof(ContractBase));
    Assert.Equal(typeof(ContractBase), context.MessageType);
    Assert.Equal(typeof(ContractDerived), context.RuntimeMessageType);
}

[Fact]
public async Task Typed_publish_adapter_names_the_user_filter_and_forwards_the_typed_message()
{
    var filter = new ContractTypedPublishFilter();
    var adapter = new TypedPublishFilterAdapter<ContractDerived>(filter);
    var inner = NewPublishContext(new ContractDerived("d-1"), declared: typeof(ContractDerived));

    await adapter.OnPublish(inner, _ => Task.CompletedTask, TestContext.Current.CancellationToken);

    Assert.Equal("d-1", filter.Seen!.Id);
    Assert.Equal(typeof(ContractTypedPublishFilter), FilterIdentity.TypeOf(adapter));
    Assert.True(inner.Headers.TryGet("typed", out var value));
    Assert.Equal("d-1", value);
}

[Fact]
public async Task Typed_consume_adapter_forwards_skip_payload_and_typed_message()
{
    var filter = new ContractTypedConsumeFilter();
    var adapter = new TypedConsumeFilterAdapter<ContractDerived>(filter);
    var inner = NewConsumeContext(new ContractDerived("d-1"));

    await adapter.OnConsume(inner, _ => Task.CompletedTask, TestContext.Current.CancellationToken);

    Assert.Equal("d-1", filter.Seen!.Id);
    Assert.True(adapter.CanSkip);
    Assert.True(inner.SkipHandler);
    Assert.Equal("typed", inner.GetPayload<string>());
    Assert.Equal(typeof(ContractTypedConsumeFilter), FilterIdentity.TypeOf(adapter));
}

[Fact]
public void Typed_contexts_share_headers_payloads_skip_and_raw_context()
{
    var innerPublish = NewPublishContext(new ContractDerived("d-1"), declared: typeof(ContractDerived));
    var typedPublish = new StreamPublishFilterContext<ContractDerived> { Inner = innerPublish };
    typedPublish.Headers.Set("shared", true);
    Assert.True(innerPublish.Headers.TryGet("shared", out _));
    Assert.Same(innerPublish.Message, typedPublish.Message);

    var innerConsume = NewConsumeContext(new ContractDerived("d-1"));
    var typedConsume = new StreamConsumeFilterContext<ContractDerived>
    {
        Inner = innerConsume,
        Message = new ContractDerived("d-1"),
    };
    typedConsume.SetPayload("payload");
    typedConsume.SkipHandler = true;
    Assert.Equal("payload", innerConsume.GetPayload<string>());
    Assert.True(innerConsume.SkipHandler);
    Assert.Same(innerConsume.Message, typedConsume.Raw);
    Assert.Same(innerConsume.OutputHeaders, typedConsume.OutputHeaders);
}
```

`ContractTypedPublishFilter` must set `context.Headers.Set("typed", context.Message.Id)` and call `next(context.Inner)`.
`ContractTypedConsumeFilter` must set `context.SetPayload("typed")`, `context.SkipHandler = true`, and call `next(context.Inner)`.
Helpers construct the universal contexts with `MessageType`, `TypedMessage`, `HeaderWriter`, and a `StreamBusRegistrationView`/`EmptyProvider`.

- [ ] **Step 2: Run the tests to verify they fail**

Run: `dotnet test tests/StreamBus.UnitTests/StreamBus.UnitTests.csproj --nologo --filter "FullyQualifiedName~TypedFilterContractTests"`
Expected: FAIL to compile because the generic interfaces, contexts, adapters, and `MessageType`/`TypedMessage` members do not exist.

- [ ] **Step 3: Implement the contracts and adapters**

Add the exact public surface from the Interfaces block. In `FilterAdapters.cs`:

```csharp
internal sealed class TypedPublishFilterAdapter<T> : IStreamPublishFilter, ITypedFilterAdapter
{
    private readonly IStreamPublishFilter<T> _filter;
    public TypedPublishFilterAdapter(IStreamPublishFilter<T> filter)
    {
        _filter = filter;
        FilterType = filter.GetType();
    }
    public Type FilterType { get; }
    public Task OnPublish(StreamPublishFilterContext context, PublishDelegate next, CancellationToken ct)
        => _filter.OnPublish(new StreamPublishFilterContext<T> { Inner = context }, next, ct);
}

internal sealed class TypedConsumeFilterAdapter<T> : IStreamConsumeFilter, ITypedFilterAdapter
{
    private readonly IStreamConsumeFilter<T> _filter;
    public TypedConsumeFilterAdapter(IStreamConsumeFilter<T> filter)
    {
        _filter = filter;
        FilterType = filter.GetType();
    }
    public Type FilterType { get; }
    public bool CanSkip => _filter.CanSkip;
    public Task OnConsume(StreamConsumeFilterContext context, ConsumeDelegate next, CancellationToken ct)
        => _filter.OnConsume(new StreamConsumeFilterContext<T>
        {
            Inner = context,
            Message = context.TypedMessage is T message
                ? message
                : throw new StreamBusConfigurationException(
                    $"Typed consume filter for {typeof(T).FullName} ran without a deserialized message (internal error)."),
        }, next, ct);
}
```

`TypedFilterAdapter.Publish/Consume` use `Activator.CreateInstance(typeof(TypedPublishFilterAdapter<>).MakeGenericType(messageType), filter)` / the consume equivalent. `FilterIdentity.TypeOf` returns `ITypedFilterAdapter.FilterType` when present, otherwise `filter.GetType()`.

Add `internal object? TypedMessage { get; init; }` to `ConsumeContext` and to universal `StreamConsumeFilterContext`; add `MessageType` to both universal contexts. Add `MessageType = typeof(T)` at the publish call site later in Task 3; for this task only the context members need to exist.

- [ ] **Step 4: Run the tests to verify they pass**

Run: `dotnet test tests/StreamBus.UnitTests/StreamBus.UnitTests.csproj --no-build --nologo --filter "FullyQualifiedName~TypedFilterContractTests"`
Expected: PASS (4 tests).

- [ ] **Step 5: Run the full unit suite and build**

Run: `dotnet build StreamBus.slnx -v:minimal`
Run: `dotnet test tests/StreamBus.UnitTests/StreamBus.UnitTests.csproj --no-build --nologo`
Expected: build 0 warnings/0 errors; all existing 334 unit tests plus 4 new tests pass.

- [ ] **Step 6: Commit**

```bash
git add src/StreamBus/PublishPipeline.cs src/StreamBus/ConsumePipeline.cs src/StreamBus/ConsumeContext.cs src/StreamBus/FilterAdapters.cs tests/StreamBus.UnitTests/TypedFilterContractTests.cs
git commit -m "feat: add typed filter contracts and contexts"
```

---

### Task 2: Registration classification, validation, DI, and endpoint mismatch

**Files:**
- Create: `src/StreamBus/FilterTypeInspector.cs`
- Modify: `src/StreamBus/PublishRegistration.cs`
- Modify: `src/StreamBus/ConsumePipeline.cs`
- Modify: `src/StreamBus/Configuration/StreamBusConfigurator.cs`
- Create: `tests/StreamBus.UnitTests/TypedFilterRegistrationTests.cs`

**Interfaces:**
- Consumes: Task 1's typed interfaces and adapters.
- Produces:
  - `internal enum FilterRegistrationKind { Universal, ConcreteTyped, OpenGenericTyped }`.
  - `internal static class FilterTypeInspector` with `static (FilterRegistrationKind Kind, IReadOnlyList<Type> MessageTypes) Classify(Type filterType, Type universalInterface, Type typedInterfaceDefinition, string family)`.
  - `PublishFilterRegistration` / `ConsumeFilterRegistration` gain `Kind`, `TypedMessageTypes`, `bool IsTyped`, `IStreamPublishFilter CreateUniversal(IServiceProvider)` / `IStreamConsumeFilter CreateUniversal(IServiceProvider)`, `Type? ClosedFilterType(Type messageType)`, and `IStreamPublishFilter CreateTyped(Type closedFilterType, IServiceProvider)` / consume equivalent.
  - `IStreamBusConfigurator.UsePublishFilter<TFilter>() where TFilter : class` and `UseConsumeFilter<TFilter>() where TFilter : class`; same relaxation on `IReceiveEndpointConfigurator`.
  - `ReceiveEndpointConfigurator.Build()` throws `StreamBusConfigurationException` for a concrete endpoint typed filter that does not implement the endpoint's message interface.
  - `AddCore` registers open generic and concrete typed filter types as transient services when not caller-owned.

- [ ] **Step 1: Write the failing registration tests**

Create `tests/StreamBus.UnitTests/TypedFilterRegistrationTests.cs`. Use a helper that calls `new ServiceCollection().AddStreamBus(...)` with a host and asserts `AddStreamBus` itself succeeds or throws (no provider build is needed for these tests).

Test types: `RegistrationOrder`, `RegistrationInvoice`; universal/typed/open-generic publish filters; `RegistrationTwoArityPublishFilter<T1,T2> : IStreamPublishFilter<T1>`; `RegistrationWrongParameterPublishFilter<T> : IStreamPublishFilter<string>`; `RegistrationDualPublishFilter : IStreamPublishFilter, IStreamPublishFilter<RegistrationOrder>`; the consume equivalents; `RegistrationConsumer : IConsumer<RegistrationOrder>`; `RegistrationInvoiceConsumeFilter : IStreamConsumeFilter<RegistrationInvoice>`.

Assertions:

```csharp
[Fact] public void Typed_concrete_publish_filter_is_accepted() => Configure(cfg => cfg.UsePublishFilter<RegistrationTypedPublishFilter>());
[Fact] public void Open_generic_publish_filter_is_accepted() => Configure(cfg => cfg.UsePublishFilter(typeof(RegistrationOpenPublishFilter<>)));

[Fact]
public void Open_generic_publish_filter_with_two_parameters_is_rejected()
{
    var ex = Assert.Throws<StreamBusConfigurationException>(() =>
        Configure(cfg => cfg.UsePublishFilter(typeof(RegistrationTwoArityPublishFilter<,>))));
    Assert.Contains("exactly one generic parameter", ex.Message, StringComparison.Ordinal);
}

[Fact]
public void Universal_and_typed_publish_filter_is_rejected()
{
    var ex = Assert.Throws<StreamBusConfigurationException>(() =>
        Configure(cfg => cfg.UsePublishFilter<RegistrationDualPublishFilter>()));
    Assert.Contains("both", ex.Message, StringComparison.OrdinalIgnoreCase);
}

[Fact]
public void Open_generic_publish_filter_for_a_different_parameter_is_rejected()
{
    var ex = Assert.Throws<StreamBusConfigurationException>(() =>
        Configure(cfg => cfg.UsePublishFilter(typeof(RegistrationWrongParameterPublishFilter<>))));
    Assert.Contains("same generic parameter", ex.Message, StringComparison.Ordinal);
}

[Fact]
public void Endpoint_typed_consume_filter_mismatch_is_rejected_during_AddStreamBus()
{
    var ex = Assert.Throws<StreamBusConfigurationException>(() => Configure(cfg =>
        cfg.ReceiveEndpoint("orders", e =>
        {
            e.Consumer<RegistrationConsumer>("grp");
            e.UseConsumeFilter<RegistrationInvoiceConsumeFilter>();
        })));
    Assert.Contains("orders", ex.Message, StringComparison.Ordinal);
    Assert.Contains(nameof(RegistrationInvoiceConsumeFilter), ex.Message, StringComparison.Ordinal);
    Assert.Contains(nameof(RegistrationOrder), ex.Message, StringComparison.Ordinal);
}
```

Also assert: typed concrete consume accepted; open generic consume accepted; endpoint open generic accepted; endpoint matching typed filter accepted; bus-wide typed consume filter that matches no endpoint is accepted (silent skip is a runtime attachment rule, not a registration error); non-filter type rejected with "must implement"; abstract typed filter rejected.

- [ ] **Step 2: Run the tests to verify they fail**

Run: `dotnet test tests/StreamBus.UnitTests/StreamBus.UnitTests.csproj --nologo --filter "FullyQualifiedName~TypedFilterRegistrationTests"`
Expected: FAIL — open generic types are rejected by the current concrete-only validation, and endpoint mismatch is not checked.

- [ ] **Step 3: Implement classification and validation**

`FilterTypeInspector.Classify` rules, in order:

1. Type must be a class, non-abstract, and not a partially open constructed type.
2. If `filterType.IsGenericTypeDefinition`: require exactly one generic parameter; reject universal implementation; require at least one `IStreamPublishFilter<>`/`IStreamConsumeFilter<>` interface whose argument is that same generic parameter; return `OpenGenericTyped`.
3. Otherwise: compute `universal = universalInterface.IsAssignableFrom(filterType)` and the distinct typed message types from the typed interface definitions. Reject `universal && typed.Any()` ("both"). Reject neither. Return `Universal` or `ConcreteTyped`.
4. Reject `filterType.ContainsGenericParameters` for non-definitions.

`ForType` calls `Classify`; `ForFactory`/`ForInstance` stay `Universal`. `ClosedFilterType(messageType)`:

- Universal → `null` (handled separately by the cache).
- Concrete typed → `TypedMessageTypes.Contains(messageType) ? FilterType : null`.
- Open generic → `FilterType.MakeGenericType(messageType)` inside `try/catch (ArgumentException ex)`, rethrow as `StreamBusConfigurationException` naming the filter, message type, and the constraint failure text; then defensively require the closed type implements `IStreamPublishFilter<messageType>` / `IStreamConsumeFilter<messageType>`.

`CreateTyped(closedType, provider)` resolves `provider.GetService(closedType)` and wraps it with `TypedFilterAdapter.Publish/Consume`; a null service throws the existing "could not be resolved from the root service provider" configuration error. `CreateUniversal(provider)` keeps the current instance/factory behavior.

In `StreamBusConfigurator`:

- Relax both generic constraints to `where TFilter : class` and delete the old concrete-only `ValidateFilterType`/`ValidateConsumeFilterType` methods (classification now throws from `ForType`).
- `AddCore` keeps `TryAddTransient(filterType, filterType)` for every non-instance filter; this is valid for open generic definitions and resolves closed types.
- `ReceiveEndpointConfigurator.Build()` calls a new `ValidateEndpointTypedFilters(_messageType)` before creating the descriptor. For each registration with `Kind == ConcreteTyped`, require `TypedMessageTypes.Contains(_messageType)`; otherwise throw a `StreamBusConfigurationException` naming the endpoint stream, the filter type, and `IStreamConsumeFilter<{messageType.Name}>`. Open generic and universal endpoint filters are accepted here.
- Keep the existing duplicate-registration checks and messages (`already registered on this bus` / `already registered on this endpoint`).

- [ ] **Step 4: Run the tests to verify they pass**

Run: `dotnet test tests/StreamBus.UnitTests/StreamBus.UnitTests.csproj --no-build --nologo --filter "FullyQualifiedName~TypedFilterRegistrationTests"`
Expected: PASS.

- [ ] **Step 5: Run the full unit suite and build**

Run: `dotnet build StreamBus.slnx -v:minimal`
Run: `dotnet test tests/StreamBus.UnitTests/StreamBus.UnitTests.csproj --no-build --nologo`
Expected: 0 warnings/0 errors; all tests green.

- [ ] **Step 6: Commit**

```bash
git add src/StreamBus/FilterTypeInspector.cs src/StreamBus/PublishRegistration.cs src/StreamBus/ConsumePipeline.cs src/StreamBus/Configuration/StreamBusConfigurator.cs tests/StreamBus.UnitTests/TypedFilterRegistrationTests.cs
git commit -m "feat: validate and register typed filters"
```

---

### Task 3: Per-message-type publish pipelines

**Files:**
- Create: `src/StreamBus/PublishPipelineCache.cs`
- Modify: `src/StreamBus/PublishPipeline.cs` (error names use the user filter type)
- Modify: `src/StreamBus/Registry.cs`
- Modify: `src/StreamBus/Bus.cs`
- Modify: `tests/StreamBus.UnitTests/PublishPipelineTests.cs`

**Interfaces:**
- Consumes: Task 1 contexts/adapters; Task 2 `PublishFilterRegistration.ClosedFilterType/CreateUniversal/CreateTyped`.
- Produces:
  - `internal sealed class PublishPipelineCache` with `PublishPipelineCache(Type busType, IReadOnlyList<PublishFilterRegistration> registrations, IServiceProvider? rootProvider)` and `PublishPipeline GetOrCreate(Type messageType)`.
  - `StreamBusCore<TBus>` uses the cache instead of one `PublishPipeline`.
  - `StreamPublishFilterContext.MessageType = typeof(T)` for every pipelined publish.
  - Eager cache population for every `PublishRegistration.MessageType` of the bus in `Registry.CreateEntry`.

- [ ] **Step 1: Extend the publish test harness and write failing tests**

In `PublishPipelineTests.cs`, add optional hooks to the private `TestBus.StartAsync` without changing existing call sites:

```csharp
public static Task<TestBus> StartAsync(
    Action<IStreamBusConfigurator> configure,
    CancellationToken ct,
    Action<IServiceCollection>? configureServices = null,
    Action<IServiceCollection>? afterAddStreamBus = null)
```

Run `configureServices` before `AddStreamBus` and `afterAddStreamBus` after it. Add test types: `PipelineInvoice`, `PipelineBase`, `PipelineDerived : PipelineBase`, `PipelineStruct` (readonly record struct), a typed `OrderOnlyPublishFilter`, a universal context-probe filter, an open-generic `OpenGenericPublishFilter<T>`, and `ClassOnlyPublishFilter<T> where T : class`.

Tests and assertions:

```csharp
[Fact]
public async Task Typed_publish_filter_runs_only_for_its_message_type()
{
    var probe = new TypedPublishProbe();
    await using var bus = await TestBus.StartAsync(cfg =>
    {
        cfg.Publish<PipelineOrder>("orders");
        cfg.Publish<PipelineInvoice>("invoices");
        cfg.UsePublishFilter<OrderOnlyPublishFilter>();
    }, TestContext.Current.CancellationToken, s => s.AddSingleton(probe));

    await bus.Bus.PublishAsync(new PipelineOrder("o-1"), TestContext.Current.CancellationToken);
    await bus.Bus.PublishAsync(new PipelineInvoice("i-1"), TestContext.Current.CancellationToken);

    var order = Assert.Single(bus.System.Publishes.Where(p => p.Stream == "orders"));
    var invoice = Assert.Single(bus.System.Publishes.Where(p => p.Stream == "invoices"));
    Assert.Equal("o-1", order.Options.Properties!.Headers["order"]);
    Assert.False(invoice.Options.Properties?.Headers.ContainsKey("order") ?? false);
    Assert.Equal(1, probe.Calls);
}

[Fact]
public async Task Universal_and_typed_publish_filters_preserve_registration_order()
{
    // universal A, typed Order filter, universal C.
    // Invoice publish logs A-in, C-in, terminal, C-out, A-out.
    // Order publish logs A-in, typed-in, C-in, terminal, C-out, typed-out, A-out.
}

[Fact]
public async Task Explicit_stream_publish_uses_the_declared_message_type()
{
    // No Publish<PipelineOrder> registration; publish explicitly to "manual".
    // Order publish runs the typed filter; Invoice publish does not.
}

[Fact]
public async Task Publish_context_reports_declared_and_runtime_message_types()
{
    // PublishAsync<PipelineBase>(new PipelineDerived("d-1"), ct)
    // Assert filter saw MessageType == typeof(PipelineBase)
    // and RuntimeMessageType == typeof(PipelineDerived).
}
```

Eager/lazy failure tests:

```csharp
[Fact]
public async Task Open_generic_constraint_violation_is_a_configuration_error()
{
    // Publish<PipelineStruct>("structs") + UsePublishFilter(typeof(ClassOnlyPublishFilter<>)).
    // hosted.StartAsync must throw StreamBusConfigurationException containing
    // nameof(ClassOnlyPublishFilter) and nameof(PipelineStruct).
}

[Fact]
public async Task Unregistered_explicit_message_type_builds_its_pipeline_lazily()
{
    // No Publish<PipelineStruct>; open generic class-only filter.
    // Bus creation succeeds; PublishAsync("manual", new PipelineStruct(1), ct)
    // throws StreamBusConfigurationException on that call.
}

[Fact]
public async Task Open_generic_and_closed_registration_collision_is_rejected_at_bus_creation()
{
    // Publish<PipelineOrder>("orders");
    // UsePublishFilter(typeof(OpenGenericPublishFilter<>));
    // UsePublishFilter(typeof(OpenGenericPublishFilter<PipelineOrder>));
    // hosted.StartAsync throws StreamBusConfigurationException containing
    // "already registered on this bus".
}

[Fact]
public async Task Missing_filter_registration_fails_at_bus_creation()
{
    // afterAddStreamBus: services.RemoveAll<SomeUniversalFilter>();
    // hosted.StartAsync throws "could not be resolved from the root service provider".
}
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `dotnet test tests/StreamBus.UnitTests/StreamBus.UnitTests.csproj --nologo --filter "FullyQualifiedName~PublishPipelineTests"`
Expected: FAIL — typed publish filters are not attached; open generics are not resolved per type; one bus-wide pipeline still runs every filter for every type.

- [ ] **Step 3: Implement the cache and wire the bus**

`PublishPipelineCache`:

```csharp
internal sealed class PublishPipelineCache
{
    private readonly Type _busType;
    private readonly IReadOnlyList<PublishFilterRegistration> _registrations;
    private readonly IServiceProvider? _rootProvider;
    private readonly Dictionary<PublishFilterRegistration, IStreamPublishFilter> _universal = new();
    private readonly ConcurrentDictionary<Type, IStreamPublishFilter> _typed = new();
    private readonly ConcurrentDictionary<Type, PublishPipeline> _pipelines = new();

    public PublishPipelineCache(Type busType, IReadOnlyList<PublishFilterRegistration> registrations, IServiceProvider? rootProvider);
    public PublishPipeline GetOrCreate(Type messageType);
    private PublishPipeline Build(Type messageType);
}
```

Constructor: if `registrations.Count > 0 && rootProvider is null`, throw the existing "Publish filters are configured for ..., but no root service provider is available." message. Resolve every universal registration once, in order, and reject duplicate resolved types with the existing "already registered on this bus" message. Typed registrations stay unresolved.

`Build(messageType)`: iterate registrations in order; universal filters come from `_universal`; typed registrations call `ClosedFilterType(messageType)` (null = concrete non-match, skip; open generic closure failure throws), then `_typed.GetOrAdd(closedType, _ => registration.CreateTyped(closedType, _rootProvider!))`. Track a per-pipeline `HashSet<Type>` of `FilterIdentity.TypeOf(filter)` and throw the existing duplicate message on collision. Return `new PublishPipeline(filters)`.

`Registry.CreateEntry`: construct the cache with `_publishStore?.FiltersFor(busType)` and `_rootProvider`; then, when `_publishStore` is not null, call `GetOrCreate` for every `PublishRegistration` whose `BusMarker == busType`. Pass the cache into `StreamBus`/`StreamBus<TBus>` instead of the single pipeline.

`StreamBusCore<TBus>`: replace `_publishPipeline` with `PublishPipelineCache _publishPipelines`; default to `new PublishPipelineCache(typeof(TBus), Array.Empty<PublishFilterRegistration>(), null)`. In `PublishThroughPipelineAsync<T>`:

```csharp
var pipeline = _publishPipelines.GetOrCreate(typeof(T));
if (pipeline.Count == 0) { /* existing SendTypedAsync fast path */ }
...
MessageType = typeof(T),
```

Pass the same `pipeline` to `RunAsync`. Keep the scope creation and header materialization exactly as today. `PublishRawAsync` remains untouched.

In `PublishPipeline`, replace `filter.GetType().FullName` in zero-next/double-next messages with `FilterIdentity.TypeOf(filter).FullName ?? FilterIdentity.TypeOf(filter).Name`; keep the existing message substrings (`returned without calling next`, `invoked next {n} times`).

- [ ] **Step 4: Run the tests to verify they pass**

Run: `dotnet test tests/StreamBus.UnitTests/StreamBus.UnitTests.csproj --no-build --nologo --filter "FullyQualifiedName~PublishPipelineTests"`
Expected: PASS, including the existing universal-filter tests unchanged.

- [ ] **Step 5: Run the full unit suite and build**

Run: `dotnet build StreamBus.slnx -v:minimal`
Run: `dotnet test tests/StreamBus.UnitTests/StreamBus.UnitTests.csproj --no-build --nologo`
Expected: 0 warnings/0 errors; all tests green.

- [ ] **Step 6: Commit**

```bash
git add src/StreamBus/PublishPipelineCache.cs src/StreamBus/PublishPipeline.cs src/StreamBus/Registry.cs src/StreamBus/Bus.cs tests/StreamBus.UnitTests/PublishPipelineTests.cs
git commit -m "feat: build publish pipelines per message type"
```

---

### Task 4: Typed consume attachment, ordering, and scope collisions

**Files:**
- Modify: `src/StreamBus/ConsumePipeline.cs`
- Modify: `src/StreamBus/ConsumerRegistry.cs`
- Create: `tests/StreamBus.UnitTests/TypedConsumeFilterTests.cs`

**Interfaces:**
- Consumes: Task 1 contexts/adapters; Task 2 `ConsumeFilterRegistration.ClosedFilterType/CreateUniversal/CreateTyped`; Task 3's `ConsumeOptions` is not yet changed (message type is passed as a parameter here).
- Produces:
  - `ConsumePipeline(IReadOnlyList<IStreamConsumeFilter> filters, bool requiresTypedMessage = false)` with `public bool RequiresTypedMessage { get; }`.
  - `StreamConsumerRegistry.BuildPipeline(..., Type? messageType)` attaches typed filters; `RequiresTypedMessage` is true when at least one `ITypedFilterAdapter` attached.
  - Endpoint-scoped concrete mismatch repeats defensively at attach; bus-scoped concrete non-matches are silently skipped.
  - Typed adapter `CanSkip` participates in the existing `AllowPipelineSkip` and effectively-once rejection checks.
  - One typed filter instance per bus per closed type; one universal filter instance per bus/registration as today.

- [ ] **Step 1: Write the failing consume attachment tests**

Create `tests/StreamBus.UnitTests/TypedConsumeFilterTests.cs`. Reuse `PipelineSemanticsTests.Harness` (`internal` nested type) and its `CreateAsync`, `Filters`, `Registry`, `DeliverAsync`, and `ScopeFactory`. Add local records/consumers/filters:

```csharp
internal sealed record TypedOrderMessage(string Id);
internal sealed record TypedInvoiceMessage(string Id);
internal sealed class TypedOrderProbe { public List<TypedOrderMessage> FilterMessages = new(); public List<TypedOrderMessage> ConsumerMessages = new(); }
internal sealed class TypedOrderFilter(TypedOrderProbe probe) : IStreamConsumeFilter<TypedOrderMessage> { /* records context.Message; next(context.Inner) */ }
internal sealed class TypedInvoiceFilter(TypedOrderProbe probe) : IStreamConsumeFilter<TypedInvoiceMessage> { /* records a call count */ }
internal sealed class TypedOrderConsumer(TypedOrderProbe probe) : IConsumer<TypedOrderMessage> { /* records context.Message */ }
internal sealed class TypedOpenFilter<T> : IStreamConsumeFilter<T> { /* records T */ }
```

A local `TypedOptions(...)` helper builds `ConsumeOptions` with `Reference = "grp"`, `SingleActiveConsumer = false`, `Offset = ConsumeOffset.AlwaysFirst`, `OffsetStore = { EveryMessages = 1 }`, `Retry` (Strict unbounded by default or SkipAfterDeadline with `maxAttempts`), `ConsumeFilters`, `IsTypedEndpoint = true`, and `AllowPipelineSkip`.

Tests and assertions:

```csharp
[Fact]
public async Task Bus_typed_filter_runs_only_for_matching_endpoint_message_type()
{
    // Store: TypedOrderFilter and TypedInvoiceFilter on DefaultBus.
    // Endpoint message type TypedOrderMessage; handler = TypedConsumerHandler.Create(...).
    // Deliver valid TypedOrderMessage JSON.
    // Assert order filter saw one message, invoice filter calls == 0, consumer saw one message.
}

[Fact]
public async Task Non_matching_bus_typed_filter_is_omitted_without_changing_order()
{
    // Registration order: universal A, typed Invoice, typed Order, universal C.
    // Endpoint consumes Order. Log must be A-in, Order-in, C-in, terminal,
    // C-out, Order-out, A-out; no Invoice entry.
}

[Fact]
public async Task Bus_typed_and_universal_filters_run_in_registration_order()
{
    // Bus universal then bus typed then endpoint universal then endpoint typed.
    // Assert the full in/out order and that bus filters wrap endpoint filters.
}

[Fact]
public async Task Endpoint_typed_filter_mismatch_is_rejected_at_attach()
{
    // endpointFilters = [ForType(typeof(TypedInvoiceFilter))], message type Order.
    // ConsumeAsync throws StreamBusConfigurationException naming TypedInvoiceFilter.
}

[Fact]
public async Task Endpoint_open_generic_filter_closes_for_the_endpoint_type()
{
    // endpointFilters = [ForType(typeof(TypedOpenFilter<>))]; message type Order.
    // Configure services AddTransient(typeof(TypedOpenFilter<>), typeof(TypedOpenFilter<>)).
    // Deliver Order JSON; assert the filter saw TypedOrderMessage.
}

[Fact]
public async Task Closed_type_collision_is_rejected_per_scope()
{
    // Bus: ForType(typeof(TypedOpenFilter<>)) + ForType(typeof(TypedOpenFilter<TypedOrderMessage>)).
    // ConsumeAsync throws "already registered on this bus".
    // Repeat with endpoint filters and assert "already registered on this endpoint".
}

[Fact]
public async Task Bus_and_endpoint_same_filter_type_runs_twice()
{
    // Same TypedOrderFilter registered at bus and endpoint scope.
    // Deliver once; assert the filter ran twice (cross-scope dedup is out of scope).
}

[Fact]
public async Task Typed_can_skip_filter_on_a_typed_endpoint_without_AllowPipelineSkip_is_rejected()
{
    // endpointFilters = [ForType(typeof(TypedSkipFilter))], typed endpoint, allowSkip false.
    // ConsumeAsync throws StreamBusConfigurationException containing "AllowPipelineSkip".
}
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `dotnet test tests/StreamBus.UnitTests/StreamBus.UnitTests.csproj --nologo --filter "FullyQualifiedName~TypedConsumeFilterTests"`
Expected: FAIL — consume filters are still universal-only; typed filters are never attached; endpoint mismatch is not checked at attach.

- [ ] **Step 3: Implement typed attachment**

Add `RequiresTypedMessage` to `ConsumePipeline` with the optional constructor parameter (default false). `BuildPipeline` gains a `Type? messageType` parameter and passes it from `ConsumeAsync`; effectively-once passes `null`.

In `StreamConsumerRegistry`, keep `_filterInstances` for universal filters and add:

```csharp
private readonly ConcurrentDictionary<(Type BusType, Type FilterType), IStreamConsumeFilter> _typedFilterInstances = new();
```

`AddFilters` takes a `bool endpointScope` and processes registrations in order:

```csharp
if (registration.IsTyped)
{
    if (messageType is null) continue;
    var closedType = registration.ClosedFilterType(messageType);
    if (closedType is null)
    {
        if (endpointScope)
            throw new StreamBusConfigurationException(
                $"ReceiveEndpoint '{stream}': consume filter '{registration.FilterType}' does not implement IStreamConsumeFilter<{messageType.Name}> for this endpoint's message type.");
        continue;
    }
    filter = _typedFilterInstances.GetOrAdd(
        (busType, closedType),
        _ => registration.CreateTyped(closedType, _rootProvider));
}
else
{
    filter = _filterInstances.GetOrAdd(
        (busType, registration),
        _ => registration.CreateUniversal(_rootProvider));
}
```

Keep the per-scope `HashSet<Type>` of `FilterIdentity.TypeOf(filter)` and the existing "already registered on this bus/endpoint" messages. Keep the `CanSkip` loop exactly as today, but name filters through `FilterIdentity.TypeOf(filter).Name` so adapter errors name the user filter. Return `new ConsumePipeline(filters, filters.Any(f => f is ITypedFilterAdapter))`.

Do not change raw or effectively-once attachment behavior: both pass `messageType: null`, so typed registrations are skipped and universal filters behave as before.

- [ ] **Step 4: Run the tests to verify they pass**

Run: `dotnet test tests/StreamBus.UnitTests/StreamBus.UnitTests.csproj --no-build --nologo --filter "FullyQualifiedName~TypedConsumeFilterTests"`
Expected: PASS, including existing `PipelineSemanticsTests` unchanged.

- [ ] **Step 5: Run the full unit suite and build**

Run: `dotnet build StreamBus.slnx -v:minimal`
Run: `dotnet test tests/StreamBus.UnitTests/StreamBus.UnitTests.csproj --no-build --nologo`
Expected: 0 warnings/0 errors; all tests green.

- [ ] **Step 6: Commit**

```bash
git add src/StreamBus/ConsumePipeline.cs src/StreamBus/ConsumerRegistry.cs tests/StreamBus.UnitTests/TypedConsumeFilterTests.cs
git commit -m "feat: attach typed consume filters per endpoint type"
```

---

### Task 5: Eager consume deserialization and typed-handler reuse

**Files:**
- Modify: `src/StreamBus/ConsumeOptions.cs`
- Create: `src/StreamBus/TypedMessageDeserializer.cs`
- Modify: `src/StreamBus/StreamPartitionConsumer.cs`
- Modify: `src/StreamBus/Hosting.cs`
- Modify: `src/StreamBus/Configuration/StreamBusConfigurator.cs`
- Modify: `tests/StreamBus.UnitTests/TypedConsumeFilterTests.cs`

**Interfaces:**
- Consumes: Task 4's `ConsumePipeline.RequiresTypedMessage`; Task 1's `ConsumeContext.TypedMessage` and universal `StreamConsumeFilterContext.TypedMessage`.
- Produces:
  - `internal Type? MessageType` on `ConsumeOptions`, set from `ReceiveEndpointDescriptor.MessageType` in `AddCore`.
  - `internal static class TypedMessageDeserializer { static object Deserialize(IStreamSerializer serializer, Type messageType, ConsumeContext context); }`.
  - `StreamPartitionConsumer.InvokeHandlerPipelineAsync` deserializes per attempt when `RequiresTypedMessage`, stores the object with `consumeContext with { TypedMessage = message }`, and passes the same context to filters and handler.
  - `TypedConsumerHandler.CreateTyped` uses `raw.TypedMessage` when present and otherwise calls `TypedMessageDeserializer`.

- [ ] **Step 1: Write the failing eager-consume tests**

Add to `TypedConsumeFilterTests.cs`:

```csharp
[Fact]
public async Task Typed_filter_and_consumer_share_the_same_deserialized_instance()
{
    // Bus TypedOrderFilter; handler = TypedConsumerHandler.Create(
    //   typeof(TypedOrderMessage), typeof(TypedOrderConsumer), harness.ScopeFactory, serializer).
    // Deliver valid JSON. Assert Assert.Same(probe.FilterMessages[0], probe.ConsumerMessages[0]).
}

[Fact]
public async Task Universal_filter_sees_raw_context_and_typed_message_slot()
{
    // Universal probe filter before a typed filter.
    // Universal context.Message is the raw ConsumeContext; context.TypedMessage
    // is the same object the typed filter receives; context.MessageType is the endpoint type.
}

[Fact]
public async Task Typed_deserialization_failure_retries_and_never_runs_filters_or_handler()
{
    // TypedOrderFilter; options maxAttempts = 2, errorStream = "errors".
    // Deliver "{bad json".
    // Assert filter calls == 0, handler calls == 0, one envelope with attempts "2",
    // exception message contains "Could not deserialize", stored offset [0].
}

[Fact]
public async Task Typed_skip_does_not_avoid_deserialization()
{
    // Typed skip filter (CanSkip => true), allowSkip true, maxAttempts 1, errorStream.
    // Deliver invalid JSON.
    // Assert filter calls == 0 and one exhaustion envelope: deserialization failed first.
}

[Fact]
public async Task Universal_only_skip_still_avoids_deserialization()
{
    // Universal skip filter only, typed endpoint with MessageType set, allowSkip true.
    // Handler is TypedConsumerHandler.Create(...).
    // Deliver invalid JSON.
    // Assert filter ran, handler not called, offset stored [0], no envelope:
    // universal-only pipelines keep lazy deserialization.
}
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `dotnet test tests/StreamBus.UnitTests/StreamBus.UnitTests.csproj --nologo --filter "FullyQualifiedName~TypedConsumeFilterTests"`
Expected: FAIL — typed filters currently run after the handler's lazy deserialization; the same-instance and eager-failure assertions fail.

- [ ] **Step 3: Implement eager deserialization**

Add `internal Type? MessageType { get; init; }` to `ConsumeOptions` and set `MessageType = descriptor.MessageType` in the `ConsumeOptions` object created by `AddCore` for typed endpoints. Raw and effectively-once options leave it null.

Add `TypedMessageDeserializer` with a cached compiled delegate per message type:

```csharp
private static readonly ConcurrentDictionary<Type, Func<IStreamSerializer, ReadOnlyMemory<byte>, object>> Deserializers = new();

public static object Deserialize(IStreamSerializer serializer, Type messageType, ConsumeContext context)
{
    object? message;
    try { message = Deserializers.GetOrAdd(messageType, Create)(serializer, context.Body); }
    catch (OperationCanceledException) { throw; }
    catch (Exception ex)
    {
        throw new StreamBusDeserializationException(context.Stream, context.Partition, context.Offset, messageType, ex);
    }
    if (message is null)
        throw new StreamBusDeserializationException(context.Stream, context.Partition, context.Offset, messageType, null);
    return message;
}
```

`Create` reflects `IStreamSerializer.Deserialize<T>`, builds an `Expression.Call`, boxes the result to `object`, and compiles a `Func<IStreamSerializer, ReadOnlyMemory<byte>, object>`. This keeps the same serializer and the same `StreamBusDeserializationException` shape as the current typed handler.

In `StreamPartitionConsumer.InvokeHandlerPipelineAsync`, before the `_pipeline.Count == 0` fast path:

```csharp
if (_pipeline.RequiresTypedMessage)
{
    var messageType = _options.MessageType
        ?? throw new StreamBusConfigurationException("Typed consume filter attached without an endpoint message type (internal error).");
    consumeContext = consumeContext with
    {
        TypedMessage = TypedMessageDeserializer.Deserialize(_serializer, messageType, consumeContext),
    };
}
```

Then set `MessageType = _options.MessageType` and `TypedMessage = consumeContext.TypedMessage` on the universal `StreamConsumeFilterContext` and pass the updated `consumeContext` to `_handler`. The deserialization call sits inside the existing per-attempt `try`, so failures use the existing retry/exhaustion machinery and filters/handler do not run for that attempt.

In `TypedConsumerHandler.CreateTyped`, replace the direct `serializer.Deserialize<T>` block with:

```csharp
var message = raw.TypedMessage is { } typed
    ? (T)typed
    : (T)TypedMessageDeserializer.Deserialize(serializer, typeof(T), raw);
```

Keep the existing consumer invocation, per-message scope selection, and `ConsumeContext<T>` construction unchanged. The fallback path preserves direct `TypedConsumerHandler.Create` callers and tests.

- [ ] **Step 4: Run the tests to verify they pass**

Run: `dotnet test tests/StreamBus.UnitTests/StreamBus.UnitTests.csproj --no-build --nologo --filter "FullyQualifiedName~TypedConsumeFilterTests"`
Run: `dotnet test tests/StreamBus.UnitTests/StreamBus.UnitTests.csproj --no-build --nologo --filter "FullyQualifiedName~TypedConsumerTests"`
Expected: PASS; the existing typed-handler fallback tests remain green.

- [ ] **Step 5: Run the full unit suite and build**

Run: `dotnet build StreamBus.slnx -v:minimal`
Run: `dotnet test tests/StreamBus.UnitTests/StreamBus.UnitTests.csproj --no-build --nologo`
Expected: 0 warnings/0 errors; all tests green.

- [ ] **Step 6: Commit**

```bash
git add src/StreamBus/ConsumeOptions.cs src/StreamBus/TypedMessageDeserializer.cs src/StreamBus/StreamPartitionConsumer.cs src/StreamBus/Hosting.cs src/StreamBus/Configuration/StreamBusConfigurator.cs tests/StreamBus.UnitTests/TypedConsumeFilterTests.cs
git commit -m "feat: deserialize eagerly for typed consume filters"
```

---

### Task 6: Broker integration coverage

**Files:**
- Create: `tests/StreamBus.IntegrationTests/TypedFilterIntegrationTests.cs`

**Interfaces:**
- Consumes: all production behavior from Tasks 1–5.
- Produces: broker-backed proof of typed publish→consume flow, per-type isolation, and per-endpoint scoping.

- [ ] **Step 1: Write the integration tests**

Use the existing integration conventions: `IntegrationLane.RequireBroker()` in the constructor, `STREAMBUS_USER`/`STREAMBUS_PASSWORD`, unique stream names, a `BusInstance` that calls `AddStreamBus`/`StartAsync`/`WaitUntilReadyAsync`, and cleanup that deletes streams. Define records `TypedFilterOrder` and `TypedFilterInvoice`, a typed publish filter, a typed consume filter, a typed consumer, an invoice consumer, and a universal filter with a shared probe singleton.

Tests:

```csharp
[Fact]
public async Task Typed_publish_filter_header_reaches_typed_consume_filter_and_consumer()
{
    // One bus: Publish<TypedFilterOrder>(stream), UsePublishFilter<TypedOrderPublishFilter>(),
    // UseConsumeFilter<TypedOrderConsumeFilter>(),
    // ReceiveEndpoint(stream, e => e.Consumer<TypedOrderConsumer>("grp")).
    // Publish one order; wait for the consumer.
    // Assert the consume filter saw the deserialized TypedFilterOrder,
    // the consumer saw the same instance, and the publish header is visible.
}

[Fact]
public async Task Bus_typed_filter_does_not_run_for_another_endpoint_message_type()
{
    // Two endpoints: orders (TypedFilterOrder) and invoices (TypedFilterInvoice).
    // Bus-wide TypedOrderConsumeFilter plus a universal filter.
    // Publish one order and one invoice.
    // Assert the typed filter ran only for the order, while the universal filter ran for both.
}

[Fact]
public async Task Per_endpoint_typed_filter_only_affects_its_endpoint()
{
    // Two endpoints both consuming TypedFilterOrder on different streams.
    // Only one endpoint registers the typed filter.
    // Publish to both; assert the filter ran only for the endpoint that registered it.
}
```

- [ ] **Step 2: Compile and run the integration lane**

Run: `dotnet build StreamBus.slnx -v:minimal`
Run: `$env:STREAMBUS_SKIP_INTEGRATION='1'; dotnet test tests/StreamBus.IntegrationTests/StreamBus.IntegrationTests.csproj --no-build --nologo`
Expected: build succeeds; integration tests are skipped because no broker is configured.

If a broker is available at `localhost:5552`, run without `STREAMBUS_SKIP_INTEGRATION` and confirm all typed-filter integration tests pass.

- [ ] **Step 3: Run the full unit suite**

Run: `dotnet test tests/StreamBus.UnitTests/StreamBus.UnitTests.csproj --no-build --nologo`
Expected: all unit tests green.

- [ ] **Step 4: Commit**

```bash
git add tests/StreamBus.IntegrationTests/TypedFilterIntegrationTests.cs
git commit -m "test: cover typed filters against the broker"
```

---

### Task 7: Documentation, examples, and CHANGELOG

**Files:**
- Modify: `docs/guide/publish.md`
- Modify: `docs/guide/consume.md`
- Modify: `CHANGELOG.md`
- Create: `examples/StreamBus.Examples.Producer/OrderPublishFilter.cs`
- Modify: `examples/StreamBus.Examples.Producer/Program.cs`
- Create: `examples/StreamBus.Examples.Consumer/OrderEventFilter.cs`
- Modify: `examples/StreamBus.Examples.Consumer/Program.cs`

**Interfaces:**
- Consumes: the final public surface from Tasks 1–5.
- Produces: user-facing docs and compiling samples that use `IStreamPublishFilter<T>` / `IStreamConsumeFilter<T>`, open generic registration, per-type semantics, and eager consume timing.

- [ ] **Step 1: Update the publish guide**

In `docs/guide/publish.md`, extend the Pipelines section:

- Explain universal filters (`IStreamPublishFilter`) run for every publish, while `IStreamPublishFilter<T>` attaches only to the declared pipe type `T`.
- Show `cfg.UsePublishFilter<OrderPublishFilter>()` and `cfg.UsePublishFilter(typeof(TenantFilter<>))`.
- Show a typed filter calling `next(context.Inner)`.
- State that `context.MessageType` is the declared call-site `T`, while `context.RuntimeMessageType` is the payload's runtime type.
- State that non-matching concrete typed filters are silently skipped, open generic closure failures are configuration errors, and explicit-stream publishes use the declared `T` (building a pipeline lazily for unregistered types).
- State that `PublishRawAsync` still bypasses the pipeline.

- [ ] **Step 2: Update the consume guide**

In `docs/guide/consume.md`, extend the Pipelines section:

- Explain bus-wide typed filters match the endpoint message type; non-matching bus-wide filters are omitted silently.
- Explain endpoint-scoped concrete typed filters must match the endpoint message type or `AddStreamBus` fails.
- Show `cfg.UseConsumeFilter(typeof(TenantFilter<>))` and an endpoint `e.UseConsumeFilter<OrderEnrichmentFilter>()`.
- Show a typed consume filter calling `next(context.Inner)`, reading `context.Message`, and using `context.SetPayload`.
- State that when at least one typed filter attaches, the message is deserialized before the pipeline per attempt; failures retry/exhaust without running filters or the handler; the same instance is shared with the consumer.
- State that universal-only pipelines keep lazy deserialization, so `CanSkip` skip still avoids deserialization; typed-filter pipelines deserialize before a skip.
- State that raw and effectively-once pipelines never attach typed filters and retain the existing `CanSkip` rules.

- [ ] **Step 3: Add example filters**

Producer: add `OrderPublishFilter : IStreamPublishFilter<OrderEvent>` that sets a header such as `x-order-id` from `context.Message.OrderId` and calls `next(context.Inner)`. Register it in `Program.cs` alongside the existing universal filters. Do not remove or change the universal samples.

Consumer: add `OrderEventFilter : IStreamConsumeFilter<OrderEvent>` that validates the typed message (for example, rejects a blank `OrderId`) and calls `next(context.Inner)`. Register it bus-wide in `Program.cs`; it will attach only to the `OrderEvent` endpoint, not the payment endpoint.

- [ ] **Step 4: Update the CHANGELOG**

Under `## 0.2.0`:

- `### Breaking`: typed filter attachment changes deserialization timing for typed consume endpoints; endpoint-scoped concrete typed filters that do not match now fail at configuration; `UsePublishFilter<TFilter>()` / `UseConsumeFilter<TFilter>()` constraints relax to `class`; open generic filter registration is now valid.
- `### Added`: `IStreamPublishFilter<T>` / `IStreamConsumeFilter<T>`, per-message-type attachment, open generic closure, `StreamPublishFilterContext.MessageType`/`RuntimeMessageType`, `StreamConsumeFilterContext.MessageType`, and eager shared consume deserialization.

- [ ] **Step 5: Build and run the full verification gates**

Run: `dotnet build StreamBus.slnx -v:minimal`
Run: `dotnet test tests/StreamBus.UnitTests/StreamBus.UnitTests.csproj --no-build --nologo`
Run: `$env:STREAMBUS_SKIP_INTEGRATION='1'; dotnet test tests/StreamBus.IntegrationTests/StreamBus.IntegrationTests.csproj --no-build --nologo`
Expected: build 0 warnings/0 errors; all unit tests pass; integration tests compile and skip cleanly.

- [ ] **Step 6: Commit**

```bash
git add docs/guide/publish.md docs/guide/consume.md CHANGELOG.md examples/StreamBus.Examples.Producer/OrderPublishFilter.cs examples/StreamBus.Examples.Producer/Program.cs examples/StreamBus.Examples.Consumer/OrderEventFilter.cs examples/StreamBus.Examples.Consumer/Program.cs
git commit -m "docs: document typed publish and consume filters"
```

---

## Definition of Done

- `IStreamPublishFilter<T>` / `IStreamConsumeFilter<T>` exist with the exact typed contexts from the spec, and universal filters remain source-compatible.
- Open generic and concrete typed filters attach per declared/endpoint message type; bus-wide non-matches skip silently; endpoint mismatches fail during `AddStreamBus`.
- Publish pipelines are cached per message type, built eagerly for registered types and lazily for explicit unregistered types.
- Typed consume filters receive the eagerly deserialized message; the consumer receives the same instance with no second deserialization.
- Registration order, bus-before-endpoint order, exactly-one-`next`, skip rules, effectively-once `CanSkip` rejection, per-message DI scope sharing, and retry/exhaustion behavior are preserved.
- All new unit and integration tests pass; existing universal-filter tests pass unchanged; `dotnet test` is green.
- `docs/guide/publish.md`, `docs/guide/consume.md`, examples, and `CHANGELOG.md` describe bus-wide vs endpoint behavior and eager deserialization timing.
