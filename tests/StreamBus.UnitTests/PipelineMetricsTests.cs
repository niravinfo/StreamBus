using System.Diagnostics.Metrics;

namespace StreamBus.UnitTests;

public sealed class PipelineMetricsTests : IDisposable
{
    private readonly MeterListener _listener = new();
    private readonly List<(string Instrument, string? Unit, double Value, KeyValuePair<string, object?>[] Tags)> _measurements = new();

    public PipelineMetricsTests()
    {
        _listener.InstrumentPublished = (instrument, listener) =>
        {
            if (instrument.Meter.Name == "StreamBus")
                listener.EnableMeasurementEvents(instrument);
        };
        _listener.SetMeasurementEventCallback<double>(Record);
        _listener.Start();
    }

    private void Record<T>(Instrument instrument, T measurement, ReadOnlySpan<KeyValuePair<string, object?>> tags, object? state)
        => _measurements.Add((instrument.Name, instrument.Unit, Convert.ToDouble(measurement, System.Globalization.CultureInfo.InvariantCulture), tags.ToArray()));

    public void Dispose() => _listener.Dispose();

    [Fact]
    public async Task Publish_pipeline_duration_is_recorded_in_seconds_with_the_publish_kind()
    {
        var pipeline = new PublishPipeline(new IStreamPublishFilter[] { new NoopPublishFilter() });
        var context = new StreamPublishFilterContext
        {
            Stream = "orders",
            RoutingKey = null,
            Payload = new object(),
            Headers = new HeaderWriter(),
            Bus = new StreamBusRegistrationView
            {
                Vhost = "/",
                Host = "localhost",
                StreamPort = 5552,
                AmqpPort = 5672,
                HeartbeatSeconds = 60,
            },
            Services = EmptyProvider.Instance,
        };

        await pipeline.RunAsync(context, () => Task.CompletedTask, TestContext.Current.CancellationToken);

        var measurement = Assert.Single(_measurements, m =>
            m.Instrument == "streambus_pipeline_duration"
            && m.Tags.Any(t => t.Key == "kind" && (string?)t.Value == "publish"));
        Assert.Equal("s", measurement.Unit);
        Assert.InRange(measurement.Value, 0, 60);
    }

    [Fact]
    public async Task Consume_pipeline_duration_is_recorded_in_seconds_with_the_consume_kind()
    {
        var pipeline = new ConsumePipeline(new IStreamConsumeFilter[] { new NoopConsumeFilter() });
        var context = new StreamConsumeFilterContext
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

        await pipeline.RunAsync(context, () => Task.CompletedTask, TestContext.Current.CancellationToken);

        var measurement = Assert.Single(_measurements, m =>
            m.Instrument == "streambus_pipeline_duration"
            && m.Tags.Any(t => t.Key == "kind" && (string?)t.Value == "consume"));
        Assert.Equal("s", measurement.Unit);
        Assert.InRange(measurement.Value, 0, 60);
    }

    private sealed class NoopPublishFilter : IStreamPublishFilter
    {
        public Task OnPublish(StreamPublishFilterContext context, PublishDelegate next, CancellationToken ct)
            => next();
    }

    private sealed class NoopConsumeFilter : IStreamConsumeFilter
    {
        public Task OnConsume(StreamConsumeFilterContext context, ConsumeDelegate next, CancellationToken ct)
            => next();
    }

    private sealed class EmptyProvider : IServiceProvider
    {
        public static readonly EmptyProvider Instance = new();
        public object? GetService(Type serviceType) => null;
    }
}
