using System.Text;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using RabbitMQ.Stream.Client;
using RabbitMQ.Stream.Client.AMQP;

namespace StreamBus.UnitTests;

internal sealed class ConsumePipelineConfigFilter : IStreamConsumeFilter
{
    public Task OnConsume(StreamConsumeFilterContext context, ConsumeDelegate next, CancellationToken ct)
        => next();
}

internal sealed record ConsumePipelineConfigMessage(int Id);

internal sealed class ConsumePipelineConfigConsumer : IConsumer<ConsumePipelineConfigMessage>
{
    public Task Consume(ConsumeContext<ConsumePipelineConfigMessage> context, CancellationToken cancellationToken)
        => Task.CompletedTask;
}

internal sealed record TenantMessage(string Name);

internal sealed class TenantAccessor(TenantProbe probe) : IDisposable
{
    public string? Tenant { get; set; }

    public void Dispose() => probe.DisposedAccessors++;
}

internal sealed class TenantProbe
{
    public List<TenantAccessor> FilterAccessors { get; } = new();
    public List<TenantAccessor> ConsumerAccessors { get; } = new();
    public int DisposedAccessors { get; set; }
}

internal sealed class TenantProbeFilter(TenantProbe probe) : IStreamConsumeFilter
{
    public Task OnConsume(StreamConsumeFilterContext context, ConsumeDelegate next, CancellationToken ct)
    {
        var accessor = (TenantAccessor)context.Services.GetRequiredService(typeof(TenantAccessor));
        accessor.Tenant = context.Raw.Metadata.ApplicationProperties["tenant"]?.ToString();
        context.SetPayload(accessor);
        lock (probe.FilterAccessors) probe.FilterAccessors.Add(accessor);
        return next();
    }
}

internal sealed class TenantProbeConsumer(TenantAccessor accessor, TenantProbe probe) : IConsumer<TenantMessage>
{
    public Task Consume(ConsumeContext<TenantMessage> context, CancellationToken cancellationToken)
    {
        lock (probe.ConsumerAccessors) probe.ConsumerAccessors.Add(accessor);
        return Task.CompletedTask;
    }
}

public sealed class PipelineSemanticsTests
{
    private static readonly TimeSpan TestTimeout = TimeSpan.FromSeconds(10);

    // ---------------------------------------------------------------------
    // Runner semantics (ConsumePipeline in isolation)
    // ---------------------------------------------------------------------

    [Fact]
    public async Task Three_filters_run_in_registration_order_and_unwind_in_reverse()
    {
        var log = new List<string>();
        var pipeline = new ConsumePipeline(new IStreamConsumeFilter[]
        {
            new RecordingFilter("A", log),
            new RecordingFilter("B", log),
            new RecordingFilter("C", log),
        });

        await pipeline.RunAsync(NewContext(), () =>
        {
            log.Add("terminal");
            return Task.CompletedTask;
        }, TestContext.Current.CancellationToken);

        Assert.Equal(new[] { "A-in", "B-in", "C-in", "terminal", "C-out", "B-out", "A-out" }, log);
    }

    [Fact]
    public async Task Payloads_flow_between_filters_keyed_by_type()
    {
        var probe = new PayloadProbe();
        var pipeline = new ConsumePipeline(new IStreamConsumeFilter[]
        {
            new PayloadWriterFilter(),
            new PayloadReaderFilter(probe),
        });

        await pipeline.RunAsync(NewContext(), () => Task.CompletedTask, TestContext.Current.CancellationToken);

        Assert.Equal("tenant-acme", probe.Seen);
    }

    [Fact]
    public async Task Zero_next_filters_without_invoking_the_terminal()
    {
        var pipeline = new ConsumePipeline(new IStreamConsumeFilter[] { new ZeroNextFilter() });
        var terminalCalls = 0;

        await pipeline.RunAsync(NewContext(), () =>
        {
            Interlocked.Increment(ref terminalCalls);
            return Task.CompletedTask;
        }, TestContext.Current.CancellationToken);

        Assert.Equal(0, terminalCalls);
    }

    [Fact]
    public async Task Effectively_once_zero_next_is_a_filter_contract_error()
    {
        var pipeline = new ConsumePipeline(
            new IStreamConsumeFilter[] { new ZeroNextFilter() },
            requiresContinuation: true);
        var terminalCalls = 0;

        var ex = await Assert.ThrowsAsync<StreamBusFilterContractException>(() =>
            pipeline.RunAsync(NewContext(), () =>
            {
                Interlocked.Increment(ref terminalCalls);
                return Task.CompletedTask;
            }, TestContext.Current.CancellationToken));

        Assert.Contains(nameof(ZeroNextFilter), ex.Message, StringComparison.Ordinal);
        Assert.Contains("returned without calling next", ex.Message, StringComparison.Ordinal);
        Assert.Equal(0, terminalCalls);
    }

    [Fact]
    public async Task Double_next_is_transient_and_names_the_nested_filter()
    {
        var log = new List<string>();
        var pipeline = new ConsumePipeline(new IStreamConsumeFilter[]
        {
            new RecordingFilter("outer", log),
            new DoubleNextFilter(),
        });
        var terminalCalls = 0;

        var ex = await Assert.ThrowsAsync<StreamBusTransientException>(() =>
            pipeline.RunAsync(NewContext(), () =>
            {
                Interlocked.Increment(ref terminalCalls);
                return Task.CompletedTask;
            }, TestContext.Current.CancellationToken));

        Assert.Contains(nameof(DoubleNextFilter), ex.Message, StringComparison.Ordinal);
        Assert.Contains("invoked next 2 times", ex.Message, StringComparison.Ordinal);
        Assert.Equal(1, terminalCalls);
        Assert.Equal(new[] { "outer-in" }, log);
    }

    // ---------------------------------------------------------------------
    // Configuration rejection
    // ---------------------------------------------------------------------

    [Fact]
    public void Duplicate_bus_consume_filter_type_is_rejected_at_configuration_time()
    {
        var services = new ServiceCollection();
        var ex = Assert.Throws<StreamBusConfigurationException>(() =>
            services.AddStreamBus((_, cfg) =>
            {
                cfg.Host("localhost", "/", h => h.Credentials("app", "secret"));
                cfg.UseConsumeFilter<ConsumePipelineConfigFilter>();
                cfg.UseConsumeFilter<ConsumePipelineConfigFilter>();
            }));

        Assert.Contains(nameof(ConsumePipelineConfigFilter), ex.Message, StringComparison.Ordinal);
        Assert.Contains("already registered on this bus", ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void Duplicate_endpoint_consume_filter_type_is_rejected_at_configuration_time()
    {
        var services = new ServiceCollection();
        var ex = Assert.Throws<StreamBusConfigurationException>(() =>
            services.AddStreamBus((_, cfg) =>
            {
                cfg.Host("localhost", "/", h => h.Credentials("app", "secret"));
                cfg.ReceiveEndpoint("orders", e =>
                {
                    e.Consumer<ConsumePipelineConfigConsumer>("grp");
                    e.UseConsumeFilter<ConsumePipelineConfigFilter>();
                    e.UseConsumeFilter<ConsumePipelineConfigFilter>();
                });
            }));

        Assert.Contains(nameof(ConsumePipelineConfigFilter), ex.Message, StringComparison.Ordinal);
        Assert.Contains("already registered on this endpoint", ex.Message, StringComparison.Ordinal);
    }

    // ---------------------------------------------------------------------
    // Composition and dispatch (registry + consumer attach)
    // ---------------------------------------------------------------------

    [Fact]
    public async Task Bus_filters_run_before_endpoint_filters_and_unwind_endpoint_first()
    {
        var log = new List<string>();
        await using var harness = await Harness.CreateAsync();
        harness.AddBusFilter(new BusScopeFilter(log));
        var options = DispatchOptions(
            endpointFilters: new[]
            {
                ConsumeFilterRegistration.ForInstance(new EndpointOuterFilter(log)),
                ConsumeFilterRegistration.ForInstance(new EndpointInnerFilter(log)),
            });
        harness.Consumer = await harness.Registry.ConsumeAsync(
            typeof(DefaultBus), "orders", options, _ =>
            {
                log.Add("terminal");
                return Task.CompletedTask;
            }, TestContext.Current.CancellationToken);

        await harness.DeliverAsync(0, "hello");

        Assert.Equal(new[]
        {
            "bus-in", "endpoint-outer-in", "endpoint-inner-in", "terminal",
            "endpoint-inner-out", "endpoint-outer-out", "bus-out",
        }, log);
    }

    [Fact]
    public async Task Zero_next_on_a_raw_endpoint_advances_the_offset_without_handler_or_error_publish()
    {
        await using var harness = await Harness.CreateAsync();
        var handlerCalls = 0;
        var options = DispatchOptions(
            endpointFilters: new[] { ConsumeFilterRegistration.ForInstance(new FilteringFilter("tenant", "acme")) });
        harness.Consumer = await harness.Registry.ConsumeAsync(
            typeof(DefaultBus), "orders", options, _ =>
            {
                Interlocked.Increment(ref handlerCalls);
                return Task.CompletedTask;
            }, TestContext.Current.CancellationToken);

        await harness.DeliverAsync(5, "hello");

        Assert.Equal(0, handlerCalls);
        Assert.Equal(new ulong[] { 5 }, harness.Factory.Current.StoredOffsets);
        Assert.Empty(harness.System.Envelopes);
    }

    [Fact]
    public async Task Zero_next_delivery_is_filtered_and_not_published_to_error()
    {
        await using var harness = await Harness.CreateAsync();
        var zero = new ZeroNextFilter();
        var handlerCalls = 0;
        var options = DispatchOptions(
            endpointFilters: new[] { ConsumeFilterRegistration.ForInstance(zero) },
            maxAttempts: 1,
            errorStream: "errors");
        harness.Consumer = await harness.Registry.ConsumeAsync(
            typeof(DefaultBus), "orders", options, _ =>
            {
                Interlocked.Increment(ref handlerCalls);
                return Task.CompletedTask;
            }, TestContext.Current.CancellationToken);

        await harness.DeliverAsync(3, "hello");

        Assert.Equal(1, zero.Calls);
        Assert.Equal(0, handlerCalls);
        Assert.Empty(harness.System.Envelopes);
        Assert.Equal(new ulong[] { 3 }, harness.Factory.Current.StoredOffsets);
    }

    [Fact]
    public async Task Double_next_delivery_exhausts_to_the_error_stream_after_invoking_the_handler_once()
    {
        await using var harness = await Harness.CreateAsync();
        var handlerCalls = 0;
        var options = DispatchOptions(
            endpointFilters: new[] { ConsumeFilterRegistration.ForInstance(new DoubleNextFilter()) },
            maxAttempts: 1,
            errorStream: "errors");
        harness.Consumer = await harness.Registry.ConsumeAsync(
            typeof(DefaultBus), "orders", options, _ =>
            {
                Interlocked.Increment(ref handlerCalls);
                return Task.CompletedTask;
            }, TestContext.Current.CancellationToken);

        await harness.DeliverAsync(4, "hello");

        Assert.Equal(1, handlerCalls);
        var envelope = Assert.Single(harness.System.Envelopes);
        Assert.Contains("invoked next 2 times", envelope.Properties["streambus.error.exception-message"]?.ToString(), StringComparison.Ordinal);
        Assert.Equal(new ulong[] { 4 }, harness.Factory.Current.StoredOffsets);
    }

    [Fact]
    public async Task Filter_exception_retries_then_exhausts_with_output_headers_on_the_envelope()
    {
        await using var harness = await Harness.CreateAsync();
        var throwing = new OutputHeaderThrowingFilter();
        var handlerCalls = 0;
        var options = DispatchOptions(
            endpointFilters: new[] { ConsumeFilterRegistration.ForInstance(throwing) },
            maxAttempts: 2,
            errorStream: "errors");
        harness.Consumer = await harness.Registry.ConsumeAsync(
            typeof(DefaultBus), "orders", options, _ =>
            {
                Interlocked.Increment(ref handlerCalls);
                return Task.CompletedTask;
            }, TestContext.Current.CancellationToken);

        await harness.DeliverAsync(7, "hello");

        var envelope = Assert.Single(harness.System.Envelopes);
        Assert.Equal("2", envelope.Properties["streambus.error.attempts"]?.ToString());
        Assert.Equal(2, throwing.Calls);
        Assert.Equal(0, handlerCalls);
        Assert.Equal(typeof(InvalidOperationException).FullName, envelope.Properties["streambus.error.exception-type"]?.ToString());
        Assert.Equal("acme", envelope.Properties["tenant"]?.ToString());
        Assert.Equal("orders", envelope.Properties["streambus.error.source-stream"]?.ToString());
        Assert.Equal(new ulong[] { 7 }, harness.Factory.Current.StoredOffsets);
    }

    [Fact]
    public async Task Output_headers_colliding_with_envelope_keys_lose_to_the_envelope()
    {
        await using var harness = await Harness.CreateAsync();
        var options = DispatchOptions(
            endpointFilters: new[] { ConsumeFilterRegistration.ForInstance(new CollidingHeaderThrowingFilter()) },
            maxAttempts: 1,
            errorStream: "errors");
        harness.Consumer = await harness.Registry.ConsumeAsync(
            typeof(DefaultBus), "orders", options, _ => Task.CompletedTask,
            TestContext.Current.CancellationToken);

        await harness.DeliverAsync(2, "hello");

        var envelope = Assert.Single(harness.System.Envelopes);
        Assert.Equal("orders", envelope.Properties["streambus.error.source-stream"]?.ToString());
        Assert.Equal("acme", envelope.Properties["tenant"]?.ToString());
    }

    [Fact]
    public async Task Output_header_collision_logs_a_warning_naming_the_key()
    {
        await using var harness = await Harness.CreateAsync();
        var logger = new CapturingLogger<ErrorPublisher>();
        var errors = new ErrorPublisher(harness.Producers, logger);
        var envelope = NewEnvelope(new Dictionary<string, object?>
        {
            ["streambus.error.source-stream"] = "hacked",
            ["tenant"] = "acme",
        });

        await errors.PublishHandoffAsync("errors", envelope, TimeSpan.FromSeconds(5), TestContext.Current.CancellationToken);

        var captured = Assert.Single(harness.System.Envelopes);
        Assert.Equal("orders", captured.Properties["streambus.error.source-stream"]?.ToString());
        Assert.Equal("acme", captured.Properties["tenant"]?.ToString());
        Assert.Contains(logger.Messages, m =>
            m.Contains("collides", StringComparison.Ordinal)
            && m.Contains("streambus.error.source-stream", StringComparison.Ordinal));
    }

    [Fact]
    public async Task Per_message_scope_is_shared_by_the_filter_and_the_typed_consumer()
    {
        var probe = new TenantProbe();
        await using var harness = await Harness.CreateAsync(configureServices: services =>
        {
            services.AddSingleton(probe);
            services.AddScoped<TenantAccessor>();
            services.AddTransient<TenantProbeConsumer>();
        });
        var handler = TypedConsumerHandler.Create(
            typeof(TenantMessage),
            typeof(TenantProbeConsumer),
            harness.ScopeFactory,
            SystemTextJsonStreamSerializer.Default);
        var options = DispatchOptions(
            endpointFilters: new[] { ConsumeFilterRegistration.ForInstance(new TenantProbeFilter(probe)) },
            typedEndpoint: true);
        harness.Consumer = await harness.Registry.ConsumeAsync(
            typeof(DefaultBus), "orders", options, handler, TestContext.Current.CancellationToken);

        await harness.DeliverAsync(0, SystemTextJsonStreamSerializer.Default.Serialize(new TenantMessage("first")), tenant: "acme");
        await harness.DeliverAsync(1, SystemTextJsonStreamSerializer.Default.Serialize(new TenantMessage("second")), tenant: "globex");

        Assert.Equal(2, probe.FilterAccessors.Count);
        Assert.Equal(2, probe.ConsumerAccessors.Count);
        Assert.Same(probe.FilterAccessors[0], probe.ConsumerAccessors[0]);
        Assert.Same(probe.FilterAccessors[1], probe.ConsumerAccessors[1]);
        Assert.NotSame(probe.ConsumerAccessors[0], probe.ConsumerAccessors[1]);
        Assert.Equal("acme", probe.ConsumerAccessors[0].Tenant);
        Assert.Equal("globex", probe.ConsumerAccessors[1].Tenant);
        Assert.Equal(2, probe.DisposedAccessors);
    }

    // ---------------------------------------------------------------------
    // Helpers
    // ---------------------------------------------------------------------

    private static StreamConsumeFilterContext NewContext() => new()
    {
        Raw = new ConsumeContext
        {
            Stream = "orders",
            Partition = 0,
            Offset = 0,
            RoutingValue = null,
            Body = ReadOnlyMemory<byte>.Empty,
            Metadata = new MessageMetadata(),
            CancellationToken = TestContext.Current.CancellationToken,
        },
        OutputHeaders = new HeaderWriter(),
        Services = EmptyProvider.Instance,
    };

    private static ConsumeOptions DispatchOptions(
        IReadOnlyList<ConsumeFilterRegistration>? endpointFilters = null,
        bool typedEndpoint = false,
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
        IsTypedEndpoint = typedEndpoint,
    };

    private static ErrorEnvelope NewEnvelope(IReadOnlyDictionary<string, object?>? outputHeaders = null) => new()
    {
        ErrorStream = "errors",
        SourceStream = "orders",
        SourcePartition = 0,
        SourcePartitionStream = "orders",
        SourceOffset = 1,
        Body = Encoding.UTF8.GetBytes("hello"),
        ExceptionType = typeof(InvalidOperationException).FullName!,
        ExceptionMessage = "boom",
        ExceptionStack = "boom",
        Attempts = 1,
        FailedAt = DateTimeOffset.UtcNow,
        MessageId = null,
        CorrelationId = null,
        RoutingValue = null,
        OutputHeaders = outputHeaders,
    };

    private sealed class RecordingFilter(string name, List<string> log) : IStreamConsumeFilter
    {
        public async Task OnConsume(StreamConsumeFilterContext context, ConsumeDelegate next, CancellationToken ct)
        {
            log.Add($"{name}-in");
            await next();
            log.Add($"{name}-out");
        }
    }

    private sealed class BusScopeFilter(List<string> log) : IStreamConsumeFilter
    {
        public async Task OnConsume(StreamConsumeFilterContext context, ConsumeDelegate next, CancellationToken ct)
        {
            log.Add("bus-in");
            await next();
            log.Add("bus-out");
        }
    }

    private sealed class EndpointOuterFilter(List<string> log) : IStreamConsumeFilter
    {
        public async Task OnConsume(StreamConsumeFilterContext context, ConsumeDelegate next, CancellationToken ct)
        {
            log.Add("endpoint-outer-in");
            await next();
            log.Add("endpoint-outer-out");
        }
    }

    private sealed class EndpointInnerFilter(List<string> log) : IStreamConsumeFilter
    {
        public async Task OnConsume(StreamConsumeFilterContext context, ConsumeDelegate next, CancellationToken ct)
        {
            log.Add("endpoint-inner-in");
            await next();
            log.Add("endpoint-inner-out");
        }
    }

    private sealed class ZeroNextFilter : IStreamConsumeFilter
    {
        public int Calls { get; private set; }

        public Task OnConsume(StreamConsumeFilterContext context, ConsumeDelegate next, CancellationToken ct)
        {
            Calls++;
            return Task.CompletedTask;
        }
    }

    private sealed class DoubleNextFilter : IStreamConsumeFilter
    {
        public async Task OnConsume(StreamConsumeFilterContext context, ConsumeDelegate next, CancellationToken ct)
        {
            await next();
            await next();
        }
    }

    private sealed class FilteringFilter(string key, string value) : IStreamConsumeFilter
    {
        public Task OnConsume(StreamConsumeFilterContext context, ConsumeDelegate next, CancellationToken ct)
        {
            context.OutputHeaders.Set(key, value);
            return Task.CompletedTask;
        }
    }

    private sealed class OutputHeaderThrowingFilter : IStreamConsumeFilter
    {
        public int Calls { get; private set; }

        public Task OnConsume(StreamConsumeFilterContext context, ConsumeDelegate next, CancellationToken ct)
        {
            Calls++;
            context.OutputHeaders.Set("tenant", "acme");
            throw new InvalidOperationException("filter boom");
        }
    }

    private sealed class CollidingHeaderThrowingFilter : IStreamConsumeFilter
    {
        public Task OnConsume(StreamConsumeFilterContext context, ConsumeDelegate next, CancellationToken ct)
        {
            context.OutputHeaders.Set("streambus.error.source-stream", "hacked");
            context.OutputHeaders.Set("tenant", "acme");
            throw new InvalidOperationException("filter boom");
        }
    }

    private sealed class PayloadWriterFilter : IStreamConsumeFilter
    {
        public Task OnConsume(StreamConsumeFilterContext context, ConsumeDelegate next, CancellationToken ct)
        {
            context.SetPayload("tenant-acme");
            return next();
        }
    }

    private sealed class PayloadReaderFilter(PayloadProbe probe) : IStreamConsumeFilter
    {
        public Task OnConsume(StreamConsumeFilterContext context, ConsumeDelegate next, CancellationToken ct)
        {
            probe.Seen = context.GetPayload<string>();
            return next();
        }
    }

    private sealed class PayloadProbe
    {
        public string? Seen { get; set; }
    }

    private sealed class EmptyProvider : IServiceProvider
    {
        public static readonly EmptyProvider Instance = new();
        public object? GetService(Type serviceType) => null;
    }

    private sealed class CapturingLogger<T> : ILogger<T>
    {
        public readonly List<string> Messages = new();

        public IDisposable? BeginScope<TState>(TState state) where TState : notnull => null;

        public bool IsEnabled(LogLevel logLevel) => true;

        public void Log<TState>(
            LogLevel logLevel,
            EventId eventId,
            TState state,
            Exception? exception,
            Func<TState, Exception?, string> formatter)
        {
            lock (Messages) Messages.Add(formatter(state, exception));
        }
    }

    internal sealed class ScriptedConsumer : IStreamConsumerHandle
    {
        private int _open = 1;

        public bool IsOpen => Volatile.Read(ref _open) == 1;

        public readonly List<ulong> StoredOffsets = new();

        public Task<ResponseCode> Close()
        {
            Volatile.Write(ref _open, 0);
            return Task.FromResult(ResponseCode.Ok);
        }

        public Task StoreOffset(ulong offset)
        {
            lock (StoredOffsets) StoredOffsets.Add(offset);
            return Task.CompletedTask;
        }
    }

    internal sealed class ScriptedConsumerFactory
    {
        private readonly object _gate = new();
        private readonly List<RawConsumerConfig> _configs = new();
        private readonly List<ScriptedConsumer> _consumers = new();

        public RawConsumerConfig FirstConfig { get { lock (_gate) return _configs[0]; } }

        public ScriptedConsumer Current { get { lock (_gate) return _consumers[^1]; } }

        public RawConsumerConfig ConfigAt(int index) { lock (_gate) return _configs[index]; }

        public ScriptedConsumer ConsumerAt(int index) { lock (_gate) return _consumers[index]; }

        public Task<IStreamConsumerHandle> CreateAsync(IStreamSystemHandle system, RawConsumerConfig config, CancellationToken ct)
        {
            lock (_gate)
            {
                _configs.Add(config);
                _consumers.Add(new ScriptedConsumer());
                return Task.FromResult<IStreamConsumerHandle>(_consumers[^1]);
            }
        }
    }

    internal sealed class ReadyConnector(CapturingSystem system) : IStreamSystemConnector
    {
        public Task<IStreamSystemHandle> ConnectAsync(StreamBusOptions options, CancellationToken ct)
            => Task.FromResult<IStreamSystemHandle>(system);
    }

    internal sealed class CapturingSystem : IStreamSystemHandle
    {
        public readonly List<CapturedEnvelope> Envelopes = new();

        public bool IsClosed => false;

        public Task<IStreamPlainProducerHandle> CreatePlainProducerAsync(string stream, CancellationToken ct)
            => Task.FromResult<IStreamPlainProducerHandle>(new CapturingPlainProducer(stream, this));

        public Task<IStreamPlainProducerHandle> CreatePlainProducerWithReferenceAsync(string stream, string reference, CancellationToken ct)
            => CreatePlainProducerAsync(stream, ct);

        public Task<IStreamSuperProducerHandle> CreateSuperStreamProducerAsync(string stream, CancellationToken ct)
            => throw new NotSupportedException("PipelineSemanticsTests publish error envelopes only.");

        public Task<IStreamSuperProducerHandle> CreateSuperStreamProducerWithReferenceAsync(string stream, string reference, CancellationToken ct)
            => throw new NotSupportedException("PipelineSemanticsTests publish error envelopes only.");

        public Task<ulong?> TryQueryOffsetAsync(string reference, string stream, CancellationToken ct) => Task.FromResult<ulong?>(null);

        public Task StoreSystemOffsetAsync(string reference, string stream, ulong offset, CancellationToken ct) => Task.CompletedTask;

        public Task<bool> StreamExistsAsync(string stream, CancellationToken ct) => Task.FromResult(true);

        public Task<bool> SuperStreamExistsAsync(string superStream, CancellationToken ct) => Task.FromResult(false);

        public Task<string[]> QueryPartitionAsync(string superStream, CancellationToken ct) => Task.FromResult(new[] { superStream });

        public Task<ulong> QuerySequenceAsync(string reference, string stream, CancellationToken ct) => Task.FromResult(0UL);

        public Task<RawConsumer> CreateRawConsumerAsync(RawConsumerConfig config, CancellationToken ct)
            => throw new NotSupportedException("PipelineSemanticsTests inject their own consumer factory.");

        public Task CreateStreamAsync(StreamSpec spec, CancellationToken ct) => Task.CompletedTask;

        public Task CreateSuperStreamAsync(PartitionsSuperStreamSpec spec, CancellationToken ct) => Task.CompletedTask;

        public ValueTask DisposeAsync() => ValueTask.CompletedTask;

        public void Add(string stream, IReadOnlyDictionary<string, object?> properties)
        {
            lock (Envelopes) Envelopes.Add(new CapturedEnvelope(stream, properties));
        }
    }

    internal sealed record CapturedEnvelope(string Stream, IReadOnlyDictionary<string, object?> Properties);

    internal sealed class CapturingPlainProducer(string stream, CapturingSystem system) : IStreamPlainProducerHandle
    {
        public Task SendAsync(ReadOnlyMemory<byte> body, PublishOptions options, CancellationToken ct)
            => throw new NotSupportedException("PipelineSemanticsTests publish error envelopes only.");

        public Task SendEnvelopeAsync(
            ReadOnlyMemory<byte> body,
            PublishOptions options,
            IReadOnlyDictionary<string, object?> applicationProperties,
            string? messageId,
            string? correlationId,
            CancellationToken ct)
        {
            system.Add(stream, new Dictionary<string, object?>(applicationProperties, StringComparer.Ordinal));
            return Task.CompletedTask;
        }

        public Task PublishReplayAsync(ReadOnlyMemory<byte> body, ulong publishingId, PublishOptions options, CancellationToken ct)
            => SendAsync(body, options, ct);

        public ValueTask DisposeAsync() => ValueTask.CompletedTask;
    }

    internal sealed class Harness : IAsyncDisposable
    {
        public required StreamBusConnectionSupervisor Supervisor { get; init; }
        public required StreamConsumerRegistry Registry { get; init; }
        public required StreamProducerRegistry Producers { get; init; }
        public required ScriptedConsumerFactory Factory { get; init; }
        public required CapturingSystem System { get; init; }
        public required StreamBusConsumeFilterStore Filters { get; init; }
        public required ServiceProvider Provider { get; init; }
        public IServiceScopeFactory ScopeFactory => Provider.GetRequiredService<IServiceScopeFactory>();

        public IStreamConsumer? Consumer { get; set; }

        public static async Task<Harness> CreateAsync(
            StreamBusConsumeFilterStore? filters = null,
            Action<IServiceCollection>? configureServices = null)
        {
            var system = new CapturingSystem();
            var supervisor = new StreamBusConnectionSupervisor(
                new StreamBusOptions { Vhost = "/", UserName = "app", Password = "secret" },
                new ReadyConnector(system));
            await supervisor.StartAsync(CancellationToken.None);
            using (var timeout = new CancellationTokenSource(TestTimeout))
            {
                Assert.Equal(BusReadiness.Ready, await supervisor.WaitUntilReadyAsync(ReadinessPolicy.Indefinitely, timeout.Token));
            }

            var services = new ServiceCollection();
            services.AddLogging();
            configureServices?.Invoke(services);
            var provider = services.BuildServiceProvider();
            var producers = new StreamProducerRegistry(supervisor, "/");
            var factory = new ScriptedConsumerFactory();
            var filterStore = filters ?? new StreamBusConsumeFilterStore();
            var registry = new StreamConsumerRegistry(
                supervisor,
                "/",
                producers,
                loggerFactory: null,
                results: new ResultProducerRegistry(supervisor, "/"),
                scopeFactory: provider.GetRequiredService<IServiceScopeFactory>(),
                consumerFactory: factory.CreateAsync,
                consumeFilters: filterStore,
                rootProvider: provider);
            return new Harness
            {
                Supervisor = supervisor,
                Registry = registry,
                Producers = producers,
                Factory = factory,
                System = system,
                Filters = filterStore,
                Provider = provider,
            };
        }

        public void AddBusFilter(IStreamConsumeFilter filter)
            => Filters.AddFilter(typeof(DefaultBus), ConsumeFilterRegistration.ForInstance(filter));

        public Task DeliverAsync(ulong offset, string body, string? tenant = null, string? correlationId = null)
            => DeliverToAsync(0, offset, body, tenant, correlationId);

        public Task DeliverAsync(ulong offset, ReadOnlyMemory<byte> body, string? tenant = null, string? correlationId = null)
            => DeliverToAsync(0, offset, body, tenant, correlationId);

        public Task DeliverToAsync(int configIndex, ulong offset, string body, string? tenant = null, string? correlationId = null)
            => DeliverToAsync(configIndex, offset, Encoding.UTF8.GetBytes(body), tenant, correlationId);

        public Task DeliverToAsync(int configIndex, ulong offset, ReadOnlyMemory<byte> body, string? tenant = null, string? correlationId = null)
        {
            var message = new Message(body.ToArray());
            if (tenant is not null)
            {
                message.ApplicationProperties ??= new ApplicationProperties();
                message.ApplicationProperties["tenant"] = tenant;
            }
            if (correlationId is not null)
            {
                message.Properties = new RabbitMQ.Stream.Client.AMQP.Properties { CorrelationId = correlationId };
            }
            return Factory.ConfigAt(configIndex).MessageHandler(
                null!,
                new MessageContext(offset, TimeSpan.Zero, 0, 0),
                message);
        }

        public async ValueTask DisposeAsync()
        {
            if (Consumer is not null)
                await Consumer.DisposeAsync();
            await Provider.DisposeAsync();
            await Supervisor.DisposeAsync();
        }
    }
}
