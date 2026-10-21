using System.Diagnostics;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using RabbitMQ.Stream.Client;

namespace StreamBus.UnitTests;

public sealed record ActivityMessage(string Value);

internal sealed class ActivityZeroNextFilter : IStreamConsumeFilter
{
    public Task OnConsume(StreamConsumeFilterContext context, ConsumeDelegate next, CancellationToken ct)
        => Task.CompletedTask;
}

public sealed class ActivityTests
{
    private static ActivityListener Listen(List<Activity> stopped)
    {
        var listener = new ActivityListener
        {
            ShouldListenTo = source => source.Name == "StreamBus",
            Sample = (ref ActivityCreationOptions<ActivityContext> _) => ActivitySamplingResult.AllDataAndRecorded,
            ActivityStopped = activity => { lock (stopped) stopped.Add(activity); },
        };
        ActivitySource.AddActivityListener(listener);
        return listener;
    }

    private static Activity SingleSpan(List<Activity> spans, string stream)
    {
        lock (spans)
        {
            return Assert.Single(spans, activity =>
                activity.OperationName == "streambus.publish"
                && (string?)activity.GetTagItem("streambus.stream") == stream);
        }
    }

    [Fact]
    public async Task Typed_publish_span_carries_stream_and_serialized_size()
    {
        var spans = new List<Activity>();
        using var listener = Listen(spans);
        var ct = TestContext.Current.CancellationToken;
        var stream = "activity-typed";
        await using var bus = await FakeBus.StartAsync(cfg => cfg.Publish<ActivityMessage>(stream), ct);

        await bus.Bus.PublishAsync(new ActivityMessage("hello"), ct);

        var span = SingleSpan(spans, stream);
        Assert.Equal(SystemTextJsonStreamSerializer.Default.Serialize(new ActivityMessage("hello")).Length,
            Convert.ToInt32(span.GetTagItem("streambus.size"), System.Globalization.CultureInfo.InvariantCulture));
        Assert.Null(span.GetTagItem("streambus.routing_key"));
    }

    [Fact]
    public async Task Raw_publish_span_carries_stream_and_body_size()
    {
        var spans = new List<Activity>();
        using var listener = Listen(spans);
        var ct = TestContext.Current.CancellationToken;
        var stream = "activity-raw";
        await using var bus = await FakeBus.StartAsync(cfg => cfg.Publish<ActivityMessage>(stream), ct);
        var body = new byte[] { 1, 2, 3, 4 };

        await bus.Bus.PublishRawAsync(stream, body, new PublishOptions(), ct);

        var span = SingleSpan(spans, stream);
        Assert.Equal(4, Convert.ToInt32(span.GetTagItem("streambus.size"), System.Globalization.CultureInfo.InvariantCulture));
    }

    [Fact]
    public async Task Super_publish_span_carries_the_routing_key_tag()
    {
        var spans = new List<Activity>();
        using var listener = Listen(spans);
        var ct = TestContext.Current.CancellationToken;
        var stream = "activity-super";
        await using var bus = await FakeBus.StartAsync(cfg => cfg.PublishSuper<ActivityMessage>(stream, s => s.Partitions(1)), ct);

        await bus.Bus.PublishSuperAsync(new ActivityMessage("hello"), "rk-1", ct);

        var span = SingleSpan(spans, stream);
        Assert.Equal("rk-1", span.GetTagItem("streambus.routing_key"));
    }

    [Fact]
    public async Task Consume_span_carries_stream_partition_offset_and_links_the_correlation_trace()
    {
        var spans = new List<Activity>();
        using var listener = Listen(spans);
        await using var harness = await PipelineSemanticsTests.Harness.CreateAsync();
        var options = new ConsumeOptions
        {
            Reference = "activity-grp",
            SingleActiveConsumer = false,
            Offset = ConsumeOffset.AlwaysFirst,
            OffsetStore = new OffsetStoreOptions { EveryMessages = 1 },
        };
        harness.Consumer = await harness.Registry.ConsumeAsync(
            typeof(DefaultBus), "activity-consume", options, _ => Task.CompletedTask,
            TestContext.Current.CancellationToken);

        var traceparent = $"00-{ActivityTraceId.CreateRandom().ToHexString()}-{ActivitySpanId.CreateRandom().ToHexString()}-01";
        Assert.True(ActivityContext.TryParse(traceparent, null, out var expected));
        await harness.DeliverAsync(7, "hello", correlationId: traceparent);

        Activity span;
        lock (spans)
        {
            span = Assert.Single(spans, activity =>
                activity.OperationName == "streambus.consume"
                && (string?)activity.GetTagItem("streambus.stream") == "activity-consume");
        }
        Assert.Equal("activity-consume", span.GetTagItem("streambus.stream"));
        Assert.Equal(0, Convert.ToInt32(span.GetTagItem("streambus.partition"), System.Globalization.CultureInfo.InvariantCulture));
        Assert.Equal(7UL, Convert.ToUInt64(span.GetTagItem("streambus.offset"), System.Globalization.CultureInfo.InvariantCulture));
        Assert.Contains(span.Links, link => link.Context.TraceId == expected.TraceId && link.Context.SpanId == expected.SpanId);
    }

    [Fact]
    public async Task Consume_classification_adds_an_exception_event_to_the_span()
    {
        var spans = new List<Activity>();
        using var listener = Listen(spans);
        await using var harness = await PipelineSemanticsTests.Harness.CreateAsync();
        var options = new ConsumeOptions
        {
            Reference = "activity-error-grp",
            SingleActiveConsumer = false,
            Offset = ConsumeOffset.AlwaysFirst,
            OffsetStore = new OffsetStoreOptions { EveryMessages = 1 },
            Retry = new RetryPolicy
            {
                FailureMode = FailureMode.SkipAfterDeadline,
                MaxAttempts = 1,
                BaseDelay = TimeSpan.FromMilliseconds(1),
                MaxDelay = TimeSpan.FromMilliseconds(2),
                Multiplier = 1.0,
                MaxJitter = TimeSpan.Zero,
            },
            ErrorStream = new ErrorStreamOptions { Stream = "errors" },
            ConsumeFilters = new[] { ConsumeFilterRegistration.ForInstance(new ActivityZeroNextFilter()) },
        };
        harness.Consumer = await harness.Registry.ConsumeAsync(
            typeof(DefaultBus), "activity-consume-error", options, _ => Task.CompletedTask,
            TestContext.Current.CancellationToken);

        await harness.DeliverAsync(1, "hello");

        Activity span;
        lock (spans)
        {
            span = Assert.Single(spans, activity =>
                activity.OperationName == "streambus.consume"
                && (string?)activity.GetTagItem("streambus.stream") == "activity-consume-error");
        }
        var exceptionEvent = Assert.Single(span.Events, e => e.Name == "exception");
        Assert.Equal(typeof(StreamBusTransientException).FullName, exceptionEvent.Tags.FirstOrDefault(t => t.Key == "exception.type").Value);
    }

    private sealed class FakeBus : IAsyncDisposable
    {
        public required ServiceProvider Provider { get; init; }
        public required IStreamBus Bus { get; init; }
        public required IHostedService Hosted { get; init; }

        public static async Task<FakeBus> StartAsync(Action<IStreamBusConfigurator> configure, CancellationToken ct)
        {
            var services = new ServiceCollection();
            services.AddLogging();
            services.AddSingleton<IStreamSystemConnector>(new Connector());
            services.AddStreamBus((_, cfg) =>
            {
                cfg.Host("localhost", "/", h => h.Credentials("app", "secret"));
                configure(cfg);
            });
            var provider = services.BuildServiceProvider();
            var hosted = provider.GetServices<IHostedService>().OfType<StreamBusHostedService>().Single();
            await hosted.StartAsync(ct);
            var bus = provider.GetRequiredService<IStreamBus>();
            Assert.Equal(BusReadiness.Ready, await bus.WaitUntilReadyAsync(ReadinessPolicy.UpTo(TimeSpan.FromSeconds(10)), ct));
            return new FakeBus { Provider = provider, Bus = bus, Hosted = hosted };
        }

        public async ValueTask DisposeAsync()
        {
            try { await Hosted.StopAsync(CancellationToken.None); }
            finally { await Provider.DisposeAsync(); }
        }
    }

    private sealed class Connector : IStreamSystemConnector
    {
        private readonly SystemHandle _system = new();

        public Task<IStreamSystemHandle> ConnectAsync(StreamBusOptions options, CancellationToken ct)
            => Task.FromResult<IStreamSystemHandle>(_system);
    }

    private sealed class SystemHandle : IStreamSystemHandle
    {
        public bool IsClosed => false;

        public Task<IStreamPlainProducerHandle> CreatePlainProducerAsync(string stream, CancellationToken ct)
            => Task.FromResult<IStreamPlainProducerHandle>(new Handle());

        public Task<IStreamPlainProducerHandle> CreatePlainProducerWithReferenceAsync(string stream, string reference, CancellationToken ct)
            => CreatePlainProducerAsync(stream, ct);

        public Task<IStreamSuperProducerHandle> CreateSuperStreamProducerAsync(string stream, CancellationToken ct)
            => Task.FromResult<IStreamSuperProducerHandle>(new SuperHandle());

        public Task<IStreamSuperProducerHandle> CreateSuperStreamProducerWithReferenceAsync(string stream, string reference, CancellationToken ct)
            => CreateSuperStreamProducerAsync(stream, ct);

        public Task<RawConsumer> CreateRawConsumerAsync(RawConsumerConfig config, CancellationToken ct)
            => throw new NotSupportedException("ActivityTests publish only.");

        public Task<ulong?> TryQueryOffsetAsync(string reference, string stream, CancellationToken ct) => Task.FromResult<ulong?>(null);
        public Task StoreSystemOffsetAsync(string reference, string stream, ulong offset, CancellationToken ct) => Task.CompletedTask;
        public Task<bool> StreamExistsAsync(string stream, CancellationToken ct) => Task.FromResult(true);
        public Task<bool> SuperStreamExistsAsync(string superStream, CancellationToken ct) => Task.FromResult(true);
        public Task<string[]> QueryPartitionAsync(string superStream, CancellationToken ct) => Task.FromResult(new[] { superStream });
        public Task<ulong> QuerySequenceAsync(string reference, string stream, CancellationToken ct) => Task.FromResult(0UL);
        public Task CreateStreamAsync(StreamSpec spec, CancellationToken ct) => Task.CompletedTask;
        public Task CreateSuperStreamAsync(PartitionsSuperStreamSpec spec, CancellationToken ct) => Task.CompletedTask;
        public ValueTask DisposeAsync() => ValueTask.CompletedTask;
    }

    private class Handle : IStreamPlainProducerHandle
    {
        public Task SendAsync(ReadOnlyMemory<byte> body, PublishOptions options, CancellationToken ct) => Task.CompletedTask;
        public Task SendEnvelopeAsync(ReadOnlyMemory<byte> body, PublishOptions options, IReadOnlyDictionary<string, object?> applicationProperties, string? messageId, string? correlationId, CancellationToken ct) => Task.CompletedTask;
        public Task PublishReplayAsync(ReadOnlyMemory<byte> body, ulong publishingId, PublishOptions options, CancellationToken ct) => Task.CompletedTask;
        public ValueTask DisposeAsync() => ValueTask.CompletedTask;
    }

    private sealed class SuperHandle : Handle, IStreamSuperProducerHandle
    {
        public Task PublishWithIdAsync(ulong publishingId, ReadOnlyMemory<byte> body, PublishOptions options, CancellationToken ct) => Task.CompletedTask;
        public Task PublishResultAsync(ReadOnlyMemory<byte> body, ulong publishingId, string? routingKey, CancellationToken ct) => Task.CompletedTask;
    }
}
