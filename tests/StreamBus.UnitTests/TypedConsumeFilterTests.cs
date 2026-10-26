using Microsoft.Extensions.DependencyInjection;

namespace StreamBus.UnitTests;

internal sealed record TypedOrderMessage(string Id);

internal sealed record TypedInvoiceMessage(string Id);

internal sealed class TypedOrderProbe
{
    public List<TypedOrderMessage> FilterMessages { get; } = new();
    public List<TypedOrderMessage> ConsumerMessages { get; } = new();
    public List<StreamConsumeFilterContext> UniversalContexts { get; } = new();
    public int InvoiceFilterCalls;
    public int TypedSkipCalls;
    public int UniversalSkipCalls;
}

internal sealed class TypedOrderFilter(TypedOrderProbe probe) : IStreamConsumeFilter<TypedOrderMessage>
{
    public Task OnConsume(StreamConsumeFilterContext<TypedOrderMessage> context, ConsumeDelegate next, CancellationToken ct)
    {
        lock (probe.FilterMessages) probe.FilterMessages.Add(context.Message);
        return next(context.Inner);
    }
}

internal sealed class TypedInvoiceFilter(TypedOrderProbe probe) : IStreamConsumeFilter<TypedInvoiceMessage>
{
    public Task OnConsume(StreamConsumeFilterContext<TypedInvoiceMessage> context, ConsumeDelegate next, CancellationToken ct)
    {
        Interlocked.Increment(ref probe.InvoiceFilterCalls);
        return next(context.Inner);
    }
}

internal sealed class TypedOrderConsumer(TypedOrderProbe probe) : IConsumer<TypedOrderMessage>
{
    public Task Consume(ConsumeContext<TypedOrderMessage> context, CancellationToken cancellationToken)
    {
        lock (probe.ConsumerMessages) probe.ConsumerMessages.Add(context.Message);
        return Task.CompletedTask;
    }
}

internal sealed class TypedOpenFilterProbe
{
    public List<object> Messages { get; } = new();
}

internal sealed class TypedOpenFilter<T>(TypedOpenFilterProbe probe) : IStreamConsumeFilter<T>
{
    public Task OnConsume(StreamConsumeFilterContext<T> context, ConsumeDelegate next, CancellationToken ct)
    {
        lock (probe.Messages) probe.Messages.Add(context.Message!);
        return next(context.Inner);
    }
}

internal sealed class TypedSkipFilter : IStreamConsumeFilter<TypedOrderMessage>
{
    public bool CanSkip => true;

    public Task OnConsume(StreamConsumeFilterContext<TypedOrderMessage> context, ConsumeDelegate next, CancellationToken ct)
    {
        context.SkipHandler = true;
        return Task.CompletedTask;
    }
}

internal sealed class UniversalCaptureFilter(TypedOrderProbe probe) : IStreamConsumeFilter
{
    public Task OnConsume(StreamConsumeFilterContext context, ConsumeDelegate next, CancellationToken ct)
    {
        lock (probe.UniversalContexts) probe.UniversalContexts.Add(context);
        return next(context);
    }
}

internal sealed class CountingTypedSkipFilter(TypedOrderProbe probe) : IStreamConsumeFilter<TypedOrderMessage>
{
    public bool CanSkip => true;

    public Task OnConsume(StreamConsumeFilterContext<TypedOrderMessage> context, ConsumeDelegate next, CancellationToken ct)
    {
        Interlocked.Increment(ref probe.TypedSkipCalls);
        context.SkipHandler = true;
        return Task.CompletedTask;
    }
}

internal sealed class CountingUniversalSkipFilter(TypedOrderProbe probe) : IStreamConsumeFilter
{
    public bool CanSkip => true;

    public Task OnConsume(StreamConsumeFilterContext context, ConsumeDelegate next, CancellationToken ct)
    {
        Interlocked.Increment(ref probe.UniversalSkipCalls);
        context.SkipHandler = true;
        return Task.CompletedTask;
    }
}

internal sealed class MultiInterfaceConsumeProbe
{
    public int Instances;
    public int Calls;
    public int OrderConsumerCalls;
    public int InvoiceConsumerCalls;
}

internal sealed class MultiMessageBusConsumeFilter : IStreamConsumeFilter<TypedOrderMessage>, IStreamConsumeFilter<TypedInvoiceMessage>
{
    private readonly MultiInterfaceConsumeProbe _probe;

    public MultiMessageBusConsumeFilter(MultiInterfaceConsumeProbe probe)
    {
        _probe = probe;
        Interlocked.Increment(ref probe.Instances);
    }

    public Task OnConsume(StreamConsumeFilterContext<TypedOrderMessage> context, ConsumeDelegate next, CancellationToken ct)
    {
        Interlocked.Increment(ref _probe.Calls);
        return next(context.Inner);
    }

    public Task OnConsume(StreamConsumeFilterContext<TypedInvoiceMessage> context, ConsumeDelegate next, CancellationToken ct)
    {
        Interlocked.Increment(ref _probe.Calls);
        return next(context.Inner);
    }
}

internal sealed class MultiInterfaceOrderConsumer(MultiInterfaceConsumeProbe probe) : IConsumer<TypedOrderMessage>
{
    public Task Consume(ConsumeContext<TypedOrderMessage> context, CancellationToken cancellationToken)
    {
        Interlocked.Increment(ref probe.OrderConsumerCalls);
        return Task.CompletedTask;
    }
}

internal sealed class MultiInterfaceInvoiceConsumer(MultiInterfaceConsumeProbe probe) : IConsumer<TypedInvoiceMessage>
{
    public Task Consume(ConsumeContext<TypedInvoiceMessage> context, CancellationToken cancellationToken)
    {
        Interlocked.Increment(ref probe.InvoiceConsumerCalls);
        return Task.CompletedTask;
    }
}

internal sealed class NoNextTypedFilter : IStreamConsumeFilter<TypedOrderMessage>
{
    public Task OnConsume(StreamConsumeFilterContext<TypedOrderMessage> context, ConsumeDelegate next, CancellationToken ct)
        => Task.CompletedTask;
}

internal sealed class DoubleNextTypedFilter : IStreamConsumeFilter<TypedOrderMessage>
{
    public async Task OnConsume(StreamConsumeFilterContext<TypedOrderMessage> context, ConsumeDelegate next, CancellationToken ct)
    {
        await next(context.Inner);
        await next(context.Inner);
    }
}

internal sealed class LoggingUniversalFilter<TTag>(string name, List<string> log) : IStreamConsumeFilter
{
    public async Task OnConsume(StreamConsumeFilterContext context, ConsumeDelegate next, CancellationToken ct)
    {
        lock (log) log.Add($"{name}-in");
        await next(context);
        lock (log) log.Add($"{name}-out");
    }
}

internal sealed class LoggingTypedFilter<TMessage, TTag>(string name, List<string> log) : IStreamConsumeFilter<TMessage>
{
    public async Task OnConsume(StreamConsumeFilterContext<TMessage> context, ConsumeDelegate next, CancellationToken ct)
    {
        lock (log) log.Add($"{name}-in");
        await next(context.Inner);
        lock (log) log.Add($"{name}-out");
    }
}

internal sealed class BusOrderingATag;

internal sealed class BusOrderingCTag;

internal sealed class BusOrderingInvoiceTag;

internal sealed class BusOrderingOrderTag;

internal sealed class BusMixedUniversalTag;

internal sealed class BusMixedTypedTag;

internal sealed class EndpointMixedUniversalTag;

internal sealed class EndpointMixedTypedTag;

public sealed class TypedConsumeFilterTests
{
    private static readonly SystemTextJsonStreamSerializer Serializer = SystemTextJsonStreamSerializer.Default;

    [Fact]
    public async Task Bus_typed_filter_runs_only_for_matching_endpoint_message_type()
    {
        var probe = new TypedOrderProbe();
        var filters = new StreamBusConsumeFilterStore();
        filters.AddFilter(typeof(DefaultBus), ConsumeFilterRegistration.ForType(typeof(TypedOrderFilter)));
        filters.AddFilter(typeof(DefaultBus), ConsumeFilterRegistration.ForType(typeof(TypedInvoiceFilter)));
        await using var harness = await PipelineSemanticsTests.Harness.CreateAsync(filters, services =>
        {
            services.AddSingleton(probe);
            services.AddTransient<TypedOrderFilter>();
            services.AddTransient<TypedInvoiceFilter>();
            services.AddTransient<TypedOrderConsumer>();
        });

        var options = TypedOptions(typeof(TypedOrderMessage));
        harness.Consumer = await harness.Registry.ConsumeAsync(
            typeof(DefaultBus),
            "orders",
            options,
            TypedConsumerHandler.Create(
                typeof(TypedOrderMessage),
                typeof(TypedOrderConsumer),
                harness.ScopeFactory,
                Serializer),
            TestContext.Current.CancellationToken);

        await harness.DeliverAsync(0, Serializer.Serialize(new TypedOrderMessage("one")));

        var filterMessage = Assert.Single(probe.FilterMessages);
        Assert.Equal("one", filterMessage.Id);
        Assert.Equal(0, probe.InvoiceFilterCalls);
        var consumerMessage = Assert.Single(probe.ConsumerMessages);
        Assert.Equal("one", consumerMessage.Id);
    }

    [Fact]
    public async Task Non_matching_bus_typed_filter_is_omitted_without_changing_order()
    {
        var log = new List<string>();
        var filters = new StreamBusConsumeFilterStore();
        filters.AddFilter(typeof(DefaultBus), ConsumeFilterRegistration.ForType(typeof(LoggingUniversalFilter<BusOrderingATag>)));
        filters.AddFilter(typeof(DefaultBus), ConsumeFilterRegistration.ForType(typeof(LoggingTypedFilter<TypedInvoiceMessage, BusOrderingInvoiceTag>)));
        filters.AddFilter(typeof(DefaultBus), ConsumeFilterRegistration.ForType(typeof(LoggingTypedFilter<TypedOrderMessage, BusOrderingOrderTag>)));
        filters.AddFilter(typeof(DefaultBus), ConsumeFilterRegistration.ForType(typeof(LoggingUniversalFilter<BusOrderingCTag>)));
        await using var harness = await PipelineSemanticsTests.Harness.CreateAsync(filters, services =>
        {
            services.AddSingleton(new LoggingUniversalFilter<BusOrderingATag>("A", log));
            services.AddSingleton(new LoggingTypedFilter<TypedInvoiceMessage, BusOrderingInvoiceTag>("Invoice", log));
            services.AddSingleton(new LoggingTypedFilter<TypedOrderMessage, BusOrderingOrderTag>("Order", log));
            services.AddSingleton(new LoggingUniversalFilter<BusOrderingCTag>("C", log));
        });

        var options = TypedOptions(typeof(TypedOrderMessage));
        harness.Consumer = await harness.Registry.ConsumeAsync(
            typeof(DefaultBus),
            "orders",
            options,
            _ =>
            {
                lock (log) log.Add("terminal");
                return Task.CompletedTask;
            },
            TestContext.Current.CancellationToken);

        await harness.DeliverAsync(0, Serializer.Serialize(new TypedOrderMessage("one")));

        Assert.Equal(
            new[] { "A-in", "Order-in", "C-in", "terminal", "C-out", "Order-out", "A-out" },
            log);
    }

    [Fact]
    public async Task Bus_typed_and_universal_filters_run_in_registration_order()
    {
        var log = new List<string>();
        var filters = new StreamBusConsumeFilterStore();
        filters.AddFilter(typeof(DefaultBus), ConsumeFilterRegistration.ForType(typeof(LoggingUniversalFilter<BusMixedUniversalTag>)));
        filters.AddFilter(typeof(DefaultBus), ConsumeFilterRegistration.ForType(typeof(LoggingTypedFilter<TypedOrderMessage, BusMixedTypedTag>)));
        await using var harness = await PipelineSemanticsTests.Harness.CreateAsync(filters, services =>
        {
            services.AddSingleton(new LoggingUniversalFilter<BusMixedUniversalTag>("bus-universal", log));
            services.AddSingleton(new LoggingTypedFilter<TypedOrderMessage, BusMixedTypedTag>("bus-typed", log));
            services.AddSingleton(new LoggingUniversalFilter<EndpointMixedUniversalTag>("endpoint-universal", log));
            services.AddSingleton(new LoggingTypedFilter<TypedOrderMessage, EndpointMixedTypedTag>("endpoint-typed", log));
        });

        var options = TypedOptions(
            typeof(TypedOrderMessage),
            endpointFilters: new[]
            {
                ConsumeFilterRegistration.ForType(typeof(LoggingUniversalFilter<EndpointMixedUniversalTag>)),
                ConsumeFilterRegistration.ForType(typeof(LoggingTypedFilter<TypedOrderMessage, EndpointMixedTypedTag>)),
            });
        harness.Consumer = await harness.Registry.ConsumeAsync(
            typeof(DefaultBus),
            "orders",
            options,
            _ =>
            {
                lock (log) log.Add("terminal");
                return Task.CompletedTask;
            },
            TestContext.Current.CancellationToken);

        await harness.DeliverAsync(0, Serializer.Serialize(new TypedOrderMessage("one")));

        Assert.Equal(
            new[]
            {
                "bus-universal-in",
                "bus-typed-in",
                "endpoint-universal-in",
                "endpoint-typed-in",
                "terminal",
                "endpoint-typed-out",
                "endpoint-universal-out",
                "bus-typed-out",
                "bus-universal-out",
            },
            log);
    }

    [Fact]
    public async Task Endpoint_typed_filter_mismatch_is_rejected_at_attach()
    {
        var probe = new TypedOrderProbe();
        await using var harness = await PipelineSemanticsTests.Harness.CreateAsync(configureServices: services =>
        {
            services.AddSingleton(probe);
            services.AddTransient<TypedInvoiceFilter>();
        });

        var options = TypedOptions(
            typeof(TypedOrderMessage),
            endpointFilters: new[] { ConsumeFilterRegistration.ForType(typeof(TypedInvoiceFilter)) });

        var ex = await Assert.ThrowsAsync<StreamBusConfigurationException>(() =>
            harness.Registry.ConsumeAsync(
                typeof(DefaultBus),
                "orders",
                options,
                _ => Task.CompletedTask,
                TestContext.Current.CancellationToken));

        Assert.Contains(nameof(TypedInvoiceFilter), ex.Message, StringComparison.Ordinal);
        Assert.Contains("does not implement IStreamConsumeFilter<", ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task Endpoint_open_generic_filter_closes_for_the_endpoint_type()
    {
        var probe = new TypedOpenFilterProbe();
        await using var harness = await PipelineSemanticsTests.Harness.CreateAsync(configureServices: services =>
        {
            services.AddSingleton(probe);
            services.AddTransient(typeof(TypedOpenFilter<>), typeof(TypedOpenFilter<>));
        });

        var options = TypedOptions(
            typeof(TypedOrderMessage),
            endpointFilters: new[] { ConsumeFilterRegistration.ForType(typeof(TypedOpenFilter<>)) });
        harness.Consumer = await harness.Registry.ConsumeAsync(
            typeof(DefaultBus),
            "orders",
            options,
            _ => Task.CompletedTask,
            TestContext.Current.CancellationToken);

        await harness.DeliverAsync(0, Serializer.Serialize(new TypedOrderMessage("one")));

        var seen = Assert.Single(probe.Messages);
        var message = Assert.IsType<TypedOrderMessage>(seen);
        Assert.Equal("one", message.Id);
    }

    [Fact]
    public async Task Closed_type_collision_is_rejected_per_scope()
    {
        var probe = new TypedOpenFilterProbe();
        await using (var harness = await PipelineSemanticsTests.Harness.CreateAsync(configureServices: services =>
        {
            services.AddSingleton(probe);
            services.AddTransient(typeof(TypedOpenFilter<>), typeof(TypedOpenFilter<>));
        }))
        {
            harness.Filters.AddFilter(typeof(DefaultBus), ConsumeFilterRegistration.ForType(typeof(TypedOpenFilter<>)));
            harness.Filters.AddFilter(typeof(DefaultBus), ConsumeFilterRegistration.ForType(typeof(TypedOpenFilter<TypedOrderMessage>)));

            var ex = await Assert.ThrowsAsync<StreamBusConfigurationException>(() =>
                harness.Registry.ConsumeAsync(
                    typeof(DefaultBus),
                    "orders",
                    TypedOptions(typeof(TypedOrderMessage)),
                    _ => Task.CompletedTask,
                    TestContext.Current.CancellationToken));

            Assert.Contains("already registered on this bus", ex.Message, StringComparison.Ordinal);
        }

        await using (var harness = await PipelineSemanticsTests.Harness.CreateAsync(configureServices: services =>
        {
            services.AddSingleton(probe);
            services.AddTransient(typeof(TypedOpenFilter<>), typeof(TypedOpenFilter<>));
        }))
        {
            var options = TypedOptions(
                typeof(TypedOrderMessage),
                endpointFilters: new[]
                {
                    ConsumeFilterRegistration.ForType(typeof(TypedOpenFilter<>)),
                    ConsumeFilterRegistration.ForType(typeof(TypedOpenFilter<TypedOrderMessage>)),
                });

            var ex = await Assert.ThrowsAsync<StreamBusConfigurationException>(() =>
                harness.Registry.ConsumeAsync(
                    typeof(DefaultBus),
                    "orders",
                    options,
                    _ => Task.CompletedTask,
                    TestContext.Current.CancellationToken));

            Assert.Contains("already registered on this endpoint", ex.Message, StringComparison.Ordinal);
        }
    }

    [Fact]
    public async Task Bus_and_endpoint_same_filter_type_runs_twice()
    {
        var probe = new TypedOrderProbe();
        var filters = new StreamBusConsumeFilterStore();
        filters.AddFilter(typeof(DefaultBus), ConsumeFilterRegistration.ForType(typeof(TypedOrderFilter)));
        await using var harness = await PipelineSemanticsTests.Harness.CreateAsync(filters, services =>
        {
            services.AddSingleton(probe);
            services.AddTransient<TypedOrderFilter>();
        });

        var options = TypedOptions(
            typeof(TypedOrderMessage),
            endpointFilters: new[] { ConsumeFilterRegistration.ForType(typeof(TypedOrderFilter)) });
        harness.Consumer = await harness.Registry.ConsumeAsync(
            typeof(DefaultBus),
            "orders",
            options,
            _ => Task.CompletedTask,
            TestContext.Current.CancellationToken);

        await harness.DeliverAsync(0, Serializer.Serialize(new TypedOrderMessage("one")));

        Assert.Equal(2, probe.FilterMessages.Count);
    }

    [Fact]
    public async Task Typed_filter_and_consumer_share_the_same_deserialized_instance()
    {
        var probe = new TypedOrderProbe();
        var filters = new StreamBusConsumeFilterStore();
        filters.AddFilter(typeof(DefaultBus), ConsumeFilterRegistration.ForType(typeof(TypedOrderFilter)));
        await using var harness = await PipelineSemanticsTests.Harness.CreateAsync(filters, services =>
        {
            services.AddSingleton(probe);
            services.AddTransient<TypedOrderFilter>();
            services.AddTransient<TypedOrderConsumer>();
        });

        harness.Consumer = await harness.Registry.ConsumeAsync(
            typeof(DefaultBus),
            "orders",
            TypedOptions(typeof(TypedOrderMessage)),
            TypedConsumerHandler.Create(
                typeof(TypedOrderMessage),
                typeof(TypedOrderConsumer),
                harness.ScopeFactory,
                Serializer),
            TestContext.Current.CancellationToken);

        await harness.DeliverAsync(0, Serializer.Serialize(new TypedOrderMessage("one")));

        var filterMessage = Assert.Single(probe.FilterMessages);
        var consumerMessage = Assert.Single(probe.ConsumerMessages);
        Assert.Same(filterMessage, consumerMessage);
    }

    [Fact]
    public async Task Universal_filter_sees_raw_context_and_typed_message_slot()
    {
        var probe = new TypedOrderProbe();
        var filters = new StreamBusConsumeFilterStore();
        filters.AddFilter(typeof(DefaultBus), ConsumeFilterRegistration.ForType(typeof(UniversalCaptureFilter)));
        filters.AddFilter(typeof(DefaultBus), ConsumeFilterRegistration.ForType(typeof(TypedOrderFilter)));
        await using var harness = await PipelineSemanticsTests.Harness.CreateAsync(filters, services =>
        {
            services.AddSingleton(probe);
            services.AddTransient<UniversalCaptureFilter>();
            services.AddTransient<TypedOrderFilter>();
            services.AddTransient<TypedOrderConsumer>();
        });

        harness.Consumer = await harness.Registry.ConsumeAsync(
            typeof(DefaultBus),
            "orders",
            TypedOptions(typeof(TypedOrderMessage)),
            TypedConsumerHandler.Create(
                typeof(TypedOrderMessage),
                typeof(TypedOrderConsumer),
                harness.ScopeFactory,
                Serializer),
            TestContext.Current.CancellationToken);

        await harness.DeliverAsync(0, Serializer.Serialize(new TypedOrderMessage("one")));

        var captured = Assert.Single(probe.UniversalContexts);
        Assert.Equal(typeof(ConsumeContext), captured.Message.GetType());
        Assert.False(captured.Message.Body.IsEmpty);
        Assert.Equal(typeof(TypedOrderMessage), captured.MessageType);
        var typedMessage = Assert.IsType<TypedOrderMessage>(captured.TypedMessage);
        var filterMessage = Assert.Single(probe.FilterMessages);
        Assert.Same(filterMessage, typedMessage);
        Assert.Same(typedMessage, captured.Message.TypedMessage);
    }

    [Fact]
    public async Task Typed_deserialization_failure_retries_and_never_runs_filters_or_handler()
    {
        var probe = new TypedOrderProbe();
        var filters = new StreamBusConsumeFilterStore();
        filters.AddFilter(typeof(DefaultBus), ConsumeFilterRegistration.ForType(typeof(TypedOrderFilter)));
        await using var harness = await PipelineSemanticsTests.Harness.CreateAsync(filters, services =>
        {
            services.AddSingleton(probe);
            services.AddTransient<TypedOrderFilter>();
        });

        var handlerCalls = 0;
        harness.Consumer = await harness.Registry.ConsumeAsync(
            typeof(DefaultBus),
            "orders",
            TypedOptions(typeof(TypedOrderMessage), maxAttempts: 2, errorStream: "errors"),
            _ =>
            {
                Interlocked.Increment(ref handlerCalls);
                return Task.CompletedTask;
            },
            TestContext.Current.CancellationToken);

        await harness.DeliverAsync(0, "{bad json");

        Assert.Empty(probe.FilterMessages);
        Assert.Equal(0, handlerCalls);
        var envelope = Assert.Single(harness.System.Envelopes);
        Assert.Equal("2", envelope.Properties["streambus.error.attempts"]?.ToString());
        Assert.Contains(
            "Could not deserialize",
            envelope.Properties["streambus.error.exception-message"]?.ToString(),
            StringComparison.Ordinal);
        Assert.Equal(new ulong[] { 0 }, harness.Factory.Current.StoredOffsets);
    }

    [Fact]
    public async Task Typed_skip_does_not_avoid_deserialization()
    {
        var probe = new TypedOrderProbe();
        await using var harness = await PipelineSemanticsTests.Harness.CreateAsync(configureServices: services =>
        {
            services.AddSingleton(probe);
            services.AddTransient<CountingTypedSkipFilter>();
        });

        var options = TypedOptions(
            typeof(TypedOrderMessage),
            endpointFilters: new[] { ConsumeFilterRegistration.ForType(typeof(CountingTypedSkipFilter)) },
            allowSkip: true,
            maxAttempts: 1,
            errorStream: "errors");
        var handlerCalls = 0;
        harness.Consumer = await harness.Registry.ConsumeAsync(
            typeof(DefaultBus),
            "orders",
            options,
            _ =>
            {
                Interlocked.Increment(ref handlerCalls);
                return Task.CompletedTask;
            },
            TestContext.Current.CancellationToken);

        await harness.DeliverAsync(0, "{bad json");

        Assert.Equal(0, Volatile.Read(ref probe.TypedSkipCalls));
        Assert.Equal(0, handlerCalls);
        var envelope = Assert.Single(harness.System.Envelopes);
        Assert.Equal("1", envelope.Properties["streambus.error.attempts"]?.ToString());
        Assert.Contains(
            "Could not deserialize",
            envelope.Properties["streambus.error.exception-message"]?.ToString(),
            StringComparison.Ordinal);
    }

    [Fact]
    public async Task Universal_only_skip_still_avoids_deserialization()
    {
        var probe = new TypedOrderProbe();
        await using var harness = await PipelineSemanticsTests.Harness.CreateAsync(configureServices: services =>
        {
            services.AddSingleton(probe);
            services.AddTransient<CountingUniversalSkipFilter>();
            services.AddTransient<TypedOrderConsumer>();
        });

        var options = TypedOptions(
            typeof(TypedOrderMessage),
            endpointFilters: new[] { ConsumeFilterRegistration.ForType(typeof(CountingUniversalSkipFilter)) },
            allowSkip: true);
        harness.Consumer = await harness.Registry.ConsumeAsync(
            typeof(DefaultBus),
            "orders",
            options,
            TypedConsumerHandler.Create(
                typeof(TypedOrderMessage),
                typeof(TypedOrderConsumer),
                harness.ScopeFactory,
                Serializer),
            TestContext.Current.CancellationToken);

        await harness.DeliverAsync(0, "{bad json");

        Assert.Equal(1, Volatile.Read(ref probe.UniversalSkipCalls));
        Assert.Empty(probe.ConsumerMessages);
        Assert.Equal(new ulong[] { 0 }, harness.Factory.Current.StoredOffsets);
        Assert.Empty(harness.System.Envelopes);
    }

    [Fact]
    public async Task Multi_interface_bus_consume_filter_uses_one_instance_across_endpoints()
    {
        var probe = new MultiInterfaceConsumeProbe();
        var filters = new StreamBusConsumeFilterStore();
        filters.AddFilter(typeof(DefaultBus), ConsumeFilterRegistration.ForType(typeof(MultiMessageBusConsumeFilter)));
        await using var harness = await PipelineSemanticsTests.Harness.CreateAsync(filters, services =>
        {
            services.AddSingleton(probe);
            services.AddTransient<MultiMessageBusConsumeFilter>();
            services.AddTransient<MultiInterfaceOrderConsumer>();
            services.AddTransient<MultiInterfaceInvoiceConsumer>();
        });

        await using var orders = await harness.Registry.ConsumeAsync(
            typeof(DefaultBus),
            "orders",
            TypedOptions(typeof(TypedOrderMessage)),
            TypedConsumerHandler.Create(
                typeof(TypedOrderMessage),
                typeof(MultiInterfaceOrderConsumer),
                harness.ScopeFactory,
                Serializer),
            TestContext.Current.CancellationToken);
        await using var invoices = await harness.Registry.ConsumeAsync(
            typeof(DefaultBus),
            "invoices",
            TypedOptions(typeof(TypedInvoiceMessage)),
            TypedConsumerHandler.Create(
                typeof(TypedInvoiceMessage),
                typeof(MultiInterfaceInvoiceConsumer),
                harness.ScopeFactory,
                Serializer),
            TestContext.Current.CancellationToken);

        await harness.DeliverToAsync(0, 0, Serializer.Serialize(new TypedOrderMessage("order-1")));
        await harness.DeliverToAsync(1, 0, Serializer.Serialize(new TypedInvoiceMessage("invoice-1")));

        Assert.Equal(1, Volatile.Read(ref probe.Instances));
        Assert.Equal(2, Volatile.Read(ref probe.Calls));
        Assert.Equal(1, Volatile.Read(ref probe.OrderConsumerCalls));
        Assert.Equal(1, Volatile.Read(ref probe.InvoiceConsumerCalls));
    }

    [Fact]
    public async Task Typed_can_skip_filter_on_a_typed_endpoint_without_AllowPipelineSkip_is_rejected()
    {
        await using var harness = await PipelineSemanticsTests.Harness.CreateAsync(configureServices: services =>
        {
            services.AddTransient<TypedSkipFilter>();
        });

        var options = TypedOptions(
            typeof(TypedOrderMessage),
            endpointFilters: new[] { ConsumeFilterRegistration.ForType(typeof(TypedSkipFilter)) },
            allowSkip: false);

        var ex = await Assert.ThrowsAsync<StreamBusConfigurationException>(() =>
            harness.Registry.ConsumeAsync(
                typeof(DefaultBus),
                "orders",
                options,
                _ => Task.CompletedTask,
                TestContext.Current.CancellationToken));

        Assert.Contains(nameof(TypedSkipFilter), ex.Message, StringComparison.Ordinal);
        Assert.Contains("AllowPipelineSkip", ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task Typed_filter_zero_next_error_names_the_user_filter_not_the_adapter()
    {
        var pipeline = new ConsumePipeline(new IStreamConsumeFilter[]
        {
            new TypedConsumeFilterAdapter<TypedOrderMessage>(new NoNextTypedFilter()),
        });

        var ex = await Assert.ThrowsAsync<StreamBusTransientException>(() =>
            pipeline.RunAsync(
                PipelineContext(),
                _ => Task.CompletedTask,
                TestContext.Current.CancellationToken,
                allowSkip: false));

        Assert.Contains(nameof(NoNextTypedFilter), ex.Message, StringComparison.Ordinal);
        Assert.Contains("returned without calling next", ex.Message, StringComparison.Ordinal);
        Assert.DoesNotContain("TypedConsumeFilterAdapter", ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task Typed_filter_double_next_error_names_the_user_filter_not_the_adapter()
    {
        var pipeline = new ConsumePipeline(new IStreamConsumeFilter[]
        {
            new TypedConsumeFilterAdapter<TypedOrderMessage>(new DoubleNextTypedFilter()),
        });

        var ex = await Assert.ThrowsAsync<StreamBusTransientException>(() =>
            pipeline.RunAsync(
                PipelineContext(),
                _ => Task.CompletedTask,
                TestContext.Current.CancellationToken,
                allowSkip: false));

        Assert.Contains(nameof(DoubleNextTypedFilter), ex.Message, StringComparison.Ordinal);
        Assert.Contains("invoked next 2 times", ex.Message, StringComparison.Ordinal);
        Assert.DoesNotContain("TypedConsumeFilterAdapter", ex.Message, StringComparison.Ordinal);
    }

    private static StreamConsumeFilterContext PipelineContext()
    {
        var message = new TypedOrderMessage("one");
        return new StreamConsumeFilterContext
        {
            Message = new ConsumeContext
            {
                Stream = "orders",
                Partition = 0,
                Offset = 0,
                RoutingValue = null,
                Body = ReadOnlyMemory<byte>.Empty,
                Metadata = new MessageMetadata(),
                CancellationToken = TestContext.Current.CancellationToken,
                TypedMessage = message,
            },
            MessageType = typeof(TypedOrderMessage),
            TypedMessage = message,
            OutputHeaders = new HeaderWriter(),
            Services = EmptyProvider.Instance,
        };
    }

    private static ConsumeOptions TypedOptions(
        Type messageType,
        IReadOnlyList<ConsumeFilterRegistration>? endpointFilters = null,
        bool allowSkip = false,
        int maxAttempts = 1,
        string? errorStream = null) => new()
    {
        Reference = "grp",
        SingleActiveConsumer = false,
        Offset = ConsumeOffset.AlwaysFirst,
        OffsetStore = new OffsetStoreOptions { EveryMessages = 1 },
        Retry = errorStream is null
            ? new RetryPolicy
            {
                FailureMode = FailureMode.Strict,
                MaxAttempts = null,
                BaseDelay = TimeSpan.FromMilliseconds(1),
                MaxDelay = TimeSpan.FromMilliseconds(2),
                Multiplier = 1.0,
                MaxJitter = TimeSpan.Zero,
            }
            : new RetryPolicy
            {
                FailureMode = FailureMode.SkipAfterDeadline,
                MaxAttempts = maxAttempts,
                BaseDelay = TimeSpan.FromMilliseconds(1),
                MaxDelay = TimeSpan.FromMilliseconds(2),
                Multiplier = 1.0,
                MaxJitter = TimeSpan.Zero,
            },
        ErrorStream = errorStream is null ? null : new ErrorStreamOptions { Stream = errorStream },
        ConsumeFilters = endpointFilters ?? Array.Empty<ConsumeFilterRegistration>(),
        IsTypedEndpoint = true,
        AllowPipelineSkip = allowSkip,
        MessageType = messageType,
    };

    private sealed class EmptyProvider : IServiceProvider
    {
        public static readonly EmptyProvider Instance = new();

        public object? GetService(Type serviceType) => null;
    }
}
