using Microsoft.Extensions.DependencyInjection;

namespace StreamBus.UnitTests;

internal sealed record TypedOrderMessage(string Id);

internal sealed record TypedInvoiceMessage(string Id);

internal sealed class TypedOrderProbe
{
    public List<TypedOrderMessage> FilterMessages { get; } = new();
    public List<TypedOrderMessage> ConsumerMessages { get; } = new();
    public List<ConsumeContext> UniversalContexts { get; } = new();
    public List<ConsumeContext<TypedOrderMessage>> TypedContexts { get; } = new();
    public int InvoiceFilterCalls;
    public int TypedFilteredCalls;
    public int UniversalFilteredCalls;
}

internal sealed class TypedOrderFilter(TypedOrderProbe probe) : IStreamConsumeFilter<TypedOrderMessage>
{
    public Task OnConsume(ConsumeContext<TypedOrderMessage> context, ConsumeDelegate next)
    {
        lock (probe.TypedContexts) probe.TypedContexts.Add(context);
        lock (probe.FilterMessages) probe.FilterMessages.Add(context.Message);
        return next();
    }
}

internal sealed class TypedInvoiceFilter(TypedOrderProbe probe) : IStreamConsumeFilter<TypedInvoiceMessage>
{
    public Task OnConsume(ConsumeContext<TypedInvoiceMessage> context, ConsumeDelegate next)
    {
        Interlocked.Increment(ref probe.InvoiceFilterCalls);
        return next();
    }
}

internal sealed class TypedOrderConsumer(TypedOrderProbe probe) : IConsumer<TypedOrderMessage>
{
    public Task Consume(ConsumeContext<TypedOrderMessage> context)
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
    public Task OnConsume(ConsumeContext<T> context, ConsumeDelegate next)
    {
        lock (probe.Messages) probe.Messages.Add(context.Message!);
        return next();
    }
}

internal sealed class TypedFilteringFilter : IStreamConsumeFilter<TypedOrderMessage>
{
    public Task OnConsume(ConsumeContext<TypedOrderMessage> context, ConsumeDelegate next)
        => Task.CompletedTask;
}

internal sealed class UniversalCaptureFilter(TypedOrderProbe probe) : IStreamConsumeFilter
{
    public Task OnConsume(ConsumeContext context, ConsumeDelegate next)
    {
        lock (probe.UniversalContexts) probe.UniversalContexts.Add(context);
        return next();
    }
}

internal sealed class CountingTypedFilteringFilter(TypedOrderProbe probe) : IStreamConsumeFilter<TypedOrderMessage>
{
    public Task OnConsume(ConsumeContext<TypedOrderMessage> context, ConsumeDelegate next)
    {
        Interlocked.Increment(ref probe.TypedFilteredCalls);
        return Task.CompletedTask;
    }
}

internal sealed class CountingUniversalFilteringFilter(TypedOrderProbe probe) : IStreamConsumeFilter
{
    public Task OnConsume(ConsumeContext context, ConsumeDelegate next)
    {
        Interlocked.Increment(ref probe.UniversalFilteredCalls);
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

    public Task OnConsume(ConsumeContext<TypedOrderMessage> context, ConsumeDelegate next)
    {
        Interlocked.Increment(ref _probe.Calls);
        return next();
    }

    public Task OnConsume(ConsumeContext<TypedInvoiceMessage> context, ConsumeDelegate next)
    {
        Interlocked.Increment(ref _probe.Calls);
        return next();
    }
}

internal sealed class MultiInterfaceOrderConsumer(MultiInterfaceConsumeProbe probe) : IConsumer<TypedOrderMessage>
{
    public Task Consume(ConsumeContext<TypedOrderMessage> context)
    {
        Interlocked.Increment(ref probe.OrderConsumerCalls);
        return Task.CompletedTask;
    }
}

internal sealed class MultiInterfaceInvoiceConsumer(MultiInterfaceConsumeProbe probe) : IConsumer<TypedInvoiceMessage>
{
    public Task Consume(ConsumeContext<TypedInvoiceMessage> context)
    {
        Interlocked.Increment(ref probe.InvoiceConsumerCalls);
        return Task.CompletedTask;
    }
}

internal sealed class NoNextTypedFilter : IStreamConsumeFilter<TypedOrderMessage>
{
    public Task OnConsume(ConsumeContext<TypedOrderMessage> context, ConsumeDelegate next)
        => Task.CompletedTask;
}

internal sealed class DoubleNextTypedFilter : IStreamConsumeFilter<TypedOrderMessage>
{
    public async Task OnConsume(ConsumeContext<TypedOrderMessage> context, ConsumeDelegate next)
    {
        await next();
        await next();
    }
}

internal sealed class LoggingUniversalFilter<TTag>(string name, List<string> log) : IStreamConsumeFilter
{
    public async Task OnConsume(ConsumeContext context, ConsumeDelegate next)
    {
        lock (log) log.Add($"{name}-in");
        await next();
        lock (log) log.Add($"{name}-out");
    }
}

internal sealed class LoggingTypedFilter<TMessage, TTag>(string name, List<string> log) : IStreamConsumeFilter<TMessage>
{
    public async Task OnConsume(ConsumeContext<TMessage> context, ConsumeDelegate next)
    {
        lock (log) log.Add($"{name}-in");
        await next();
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
            TypedConsumerHandler.Create(typeof(TypedOrderMessage), typeof(TypedOrderConsumer)),
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
            TypedConsumerHandler.Create(typeof(TypedOrderMessage), typeof(TypedOrderConsumer)),
            TestContext.Current.CancellationToken);

        await harness.DeliverAsync(0, Serializer.Serialize(new TypedOrderMessage("one")));

        var filterMessage = Assert.Single(probe.FilterMessages);
        var consumerMessage = Assert.Single(probe.ConsumerMessages);
        Assert.Same(filterMessage, consumerMessage);
    }

    [Fact]
    public async Task Universal_filter_sees_raw_context_and_declared_message_type()
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
            TypedConsumerHandler.Create(typeof(TypedOrderMessage), typeof(TypedOrderConsumer)),
            TestContext.Current.CancellationToken);

        await harness.DeliverAsync(0, Serializer.Serialize(new TypedOrderMessage("one")));

        var captured = Assert.Single(probe.UniversalContexts);
        Assert.IsType<ConsumeContext<TypedOrderMessage>>(captured);
        Assert.False(captured.Body.IsEmpty);
        var filterMessage = Assert.Single(probe.FilterMessages);
        var consumerMessage = Assert.Single(probe.ConsumerMessages);
        Assert.Same(filterMessage, consumerMessage);
    }

    [Fact]
    public async Task Universal_and_typed_consume_filters_share_one_context_instance()
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
            TypedConsumerHandler.Create(typeof(TypedOrderMessage), typeof(TypedOrderConsumer)),
            TestContext.Current.CancellationToken);

        await harness.DeliverAsync(0, Serializer.Serialize(new TypedOrderMessage("one")));

        var universal = Assert.Single(probe.UniversalContexts);
        var typed = Assert.Single(probe.TypedContexts);
        Assert.Same(universal, typed);
        Assert.Same(universal.Headers, typed.Headers);
        Assert.Same(universal.SendHeaders, typed.SendHeaders);
        Assert.Same(universal.Services, typed.Services);
        Assert.Equal("one", typed.Message.Id);
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
    public async Task Typed_zero_next_still_deserializes_but_skips_handler()
    {
        var probe = new TypedOrderProbe();
        await using var harness = await PipelineSemanticsTests.Harness.CreateAsync(configureServices: services =>
        {
            services.AddSingleton(probe);
            services.AddTransient<CountingTypedFilteringFilter>();
        });

        var options = TypedOptions(
            typeof(TypedOrderMessage),
            endpointFilters: new[] { ConsumeFilterRegistration.ForType(typeof(CountingTypedFilteringFilter)) },
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

        Assert.Equal(0, Volatile.Read(ref probe.TypedFilteredCalls));
        Assert.Equal(0, handlerCalls);
        var envelope = Assert.Single(harness.System.Envelopes);
        Assert.Equal("1", envelope.Properties["streambus.error.attempts"]?.ToString());
        Assert.Contains(
            "Could not deserialize",
            envelope.Properties["streambus.error.exception-message"]?.ToString(),
            StringComparison.Ordinal);
    }

    [Fact]
    public async Task Universal_only_zero_next_still_deserializes_on_a_typed_endpoint()
    {
        var probe = new TypedOrderProbe();
        await using var harness = await PipelineSemanticsTests.Harness.CreateAsync(configureServices: services =>
        {
            services.AddSingleton(probe);
            services.AddTransient<CountingUniversalFilteringFilter>();
            services.AddTransient<TypedOrderConsumer>();
        });

        var options = TypedOptions(
            typeof(TypedOrderMessage),
            endpointFilters: new[] { ConsumeFilterRegistration.ForType(typeof(CountingUniversalFilteringFilter)) });
        harness.Consumer = await harness.Registry.ConsumeAsync(
            typeof(DefaultBus),
            "orders",
            options,
            TypedConsumerHandler.Create(typeof(TypedOrderMessage), typeof(TypedOrderConsumer)),
            TestContext.Current.CancellationToken);

        await harness.DeliverAsync(0, Serializer.Serialize(new TypedOrderMessage("one")));

        Assert.Equal(1, Volatile.Read(ref probe.UniversalFilteredCalls));
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
            TypedConsumerHandler.Create(typeof(TypedOrderMessage), typeof(MultiInterfaceOrderConsumer)),
            TestContext.Current.CancellationToken);
        await using var invoices = await harness.Registry.ConsumeAsync(
            typeof(DefaultBus),
            "invoices",
            TypedOptions(typeof(TypedInvoiceMessage)),
            TypedConsumerHandler.Create(typeof(TypedInvoiceMessage), typeof(MultiInterfaceInvoiceConsumer)),
            TestContext.Current.CancellationToken);

        await harness.DeliverToAsync(0, 0, Serializer.Serialize(new TypedOrderMessage("order-1")));
        await harness.DeliverToAsync(1, 0, Serializer.Serialize(new TypedInvoiceMessage("invoice-1")));

        Assert.Equal(1, Volatile.Read(ref probe.Instances));
        Assert.Equal(2, Volatile.Read(ref probe.Calls));
        Assert.Equal(1, Volatile.Read(ref probe.OrderConsumerCalls));
        Assert.Equal(1, Volatile.Read(ref probe.InvoiceConsumerCalls));
    }

    [Fact]
    public async Task Zero_next_on_a_typed_endpoint_advances_the_offset_once()
    {
        await using var harness = await PipelineSemanticsTests.Harness.CreateAsync(configureServices: services =>
        {
            services.AddTransient<TypedFilteringFilter>();
        });

        var options = TypedOptions(
            typeof(TypedOrderMessage),
            endpointFilters: new[] { ConsumeFilterRegistration.ForType(typeof(TypedFilteringFilter)) },
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

        await harness.DeliverAsync(9, Serializer.Serialize(new TypedOrderMessage("one")));

        Assert.Equal(0, handlerCalls);
        Assert.Equal(new ulong[] { 9 }, harness.Factory.Current.StoredOffsets);
        Assert.Empty(harness.System.Envelopes);
    }

    [Fact]
    public async Task Effectively_once_typed_filter_zero_next_names_the_user_filter_not_the_adapter()
    {
        var pipeline = new ConsumePipeline(
            new IStreamConsumeFilter[]
            {
                new TypedConsumeFilterAdapter<TypedOrderMessage>(new NoNextTypedFilter()),
            },
            requiresContinuation: true);

        var ex = await Assert.ThrowsAsync<StreamBusFilterContractException>(() =>
            pipeline.RunAsync(
                PipelineContext(),
                () => Task.CompletedTask));

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
                () => Task.CompletedTask));

        Assert.Contains(nameof(DoubleNextTypedFilter), ex.Message, StringComparison.Ordinal);
        Assert.Contains("invoked next 2 times", ex.Message, StringComparison.Ordinal);
        Assert.DoesNotContain("TypedConsumeFilterAdapter", ex.Message, StringComparison.Ordinal);
    }

    private static ConsumeContext<TypedOrderMessage> PipelineContext()
    {
        var message = new TypedOrderMessage("one");
        return new ConsumeContext<TypedOrderMessage>
        {
            Stream = "orders",
            Partition = 0,
            Offset = 0,
            RoutingValue = null,
            Body = ReadOnlyMemory<byte>.Empty,
            Headers = new StreamHeaders(new Dictionary<string, object?>(), SystemTextJsonStreamSerializer.Default),
            SendHeaders = new StreamSendHeaders(null, SystemTextJsonStreamSerializer.Default),
            Message = message,
            Services = EmptyProvider.Instance,
            CancellationToken = TestContext.Current.CancellationToken,
        };
    }

    private static ConsumeOptions TypedOptions(
        Type messageType,
        IReadOnlyList<ConsumeFilterRegistration>? endpointFilters = null,
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
                MaxAttempts = null,
                BaseDelay = TimeSpan.FromMilliseconds(1),
                MaxDelay = TimeSpan.FromMilliseconds(2),
                Multiplier = 1.0,
                MaxJitter = TimeSpan.Zero,
            }
            : new RetryPolicy
            {
                MaxAttempts = maxAttempts,
                BaseDelay = TimeSpan.FromMilliseconds(1),
                MaxDelay = TimeSpan.FromMilliseconds(2),
                Multiplier = 1.0,
                MaxJitter = TimeSpan.Zero,
            },
        ErrorStream = errorStream is null ? null : new ErrorStreamOptions { Stream = errorStream },
        ConsumeFilters = endpointFilters ?? Array.Empty<ConsumeFilterRegistration>(),
        IsTypedEndpoint = true,
        MessageType = messageType,
    };

    private sealed class EmptyProvider : IServiceProvider
    {
        public static readonly EmptyProvider Instance = new();

        public object? GetService(Type serviceType) => null;
    }
}
