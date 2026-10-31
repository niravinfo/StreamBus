using System.Text;

namespace StreamBus.UnitTests;

public sealed class ErrorQueueTests
{
    private static ErrorEnvelope SampleEnvelope() => new()
    {
        ErrorStream = "orders-errors",
        SourceStream = "orders",
        SourcePartition = 0,
        SourcePartitionStream = "orders",
        SourceOffset = 42,
        Body = new byte[] { 1, 2, 3 },
        ExceptionType = "System.InvalidOperationException",
        ExceptionMessage = "boom",
        ExceptionStack = "at Boom()",
        Attempts = 3,
        FailedAt = new DateTimeOffset(2026, 10, 15, 12, 0, 0, TimeSpan.Zero),
        MessageId = Guid.Parse("0f8fad5b-d9cb-469f-a165-70867728950e"),
        CorrelationId = Guid.Parse("6b1f5c1a-0d9e-4b6f-9a4e-1f2d3c4b5a69"),
        RoutingValue = "o-1",
    };

    private sealed class NeverConnectFactory : IErrorQueueConnectionFactory
    {
        public int Calls;
        public Task<RabbitMQ.Client.IConnection> ConnectAsync(StreamBusOptions options, CancellationToken ct)
        {
            Interlocked.Increment(ref Calls);
            throw new InvalidOperationException("AMQP connector must not be built when no queue destination exists.");
        }
    }

    private static StreamBusOptions QueueOptions() => new()
    {
        Vhost = "/",
        UserName = "app",
        Password = "secret",
    };

    [Fact]
    public void Headers_round_trip_through_broker_encoding()
    {
        var envelope = SampleEnvelope();
        var headers = envelope.ToHeaders();
        Assert.DoesNotContain(headers.Keys, k => k.StartsWith("other.", StringComparison.Ordinal));
        var onWire = headers.ToDictionary(
            kv => kv.Key,
            kv => (object?)Encoding.UTF8.GetBytes(kv.Value?.ToString() ?? string.Empty),
            StringComparer.Ordinal);
        Assert.True(ErrorEnvelope.TryParseHeaders("orders-errors", envelope.Body, onWire, out var parsed));
        Assert.NotNull(parsed);
        Assert.Equal(envelope.SourceStream, parsed.SourceStream);
        Assert.Equal(envelope.SourcePartition, parsed.SourcePartition);
        Assert.Equal(envelope.SourcePartitionStream, parsed.SourcePartitionStream);
        Assert.Equal(envelope.SourceOffset, parsed.SourceOffset);
        Assert.Equal(envelope.ExceptionType, parsed.ExceptionType);
        Assert.Equal(envelope.ExceptionMessage, parsed.ExceptionMessage);
        Assert.Equal(envelope.Attempts, parsed.Attempts);
        Assert.Equal(envelope.FailedAt, parsed.FailedAt);
        Assert.Equal(envelope.MessageId, parsed.MessageId);
        Assert.Equal(envelope.CorrelationId, parsed.CorrelationId);
        Assert.Equal(envelope.RoutingValue, parsed.RoutingValue);
        Assert.Equal(envelope.Body, parsed.Body);
    }

    [Fact]
    public void Headers_omit_nulls_but_parse_them_back()
    {
        var envelope = SampleEnvelope() with { MessageId = null, CorrelationId = null, RoutingValue = null };
        var headers = envelope.ToHeaders();
        Assert.DoesNotContain("streambus.error.message-id", headers.Keys);
        Assert.True(ErrorEnvelope.TryParseHeaders("orders-errors", envelope.Body, headers, out var parsed));
        Assert.NotNull(parsed);
        Assert.Null(parsed.MessageId);
        Assert.Null(parsed.CorrelationId);
        Assert.Null(parsed.RoutingValue);
    }

    [Fact]
    public void Headers_malformed_returns_false()
    {
        Assert.False(ErrorEnvelope.TryParseHeaders("orders-errors", new byte[] { 1 }, new Dictionary<string, object?>(), out _));
        Assert.False(ErrorEnvelope.TryParseHeaders("", new byte[] { 1 }, new Dictionary<string, object?>(), out _));
    }

    [Fact]
    public void Queue_arguments_map_classic_and_quorum()
    {
        Assert.Equal("classic", ErrorQueueTransport.QueueArguments(DeadLetterQueueType.ClassicQueue)["x-queue-type"]);
        Assert.Equal("quorum", ErrorQueueTransport.QueueArguments(DeadLetterQueueType.QuorumQueue)["x-queue-type"]);
    }

    [Fact]
    public async Task Empty_queue_set_builds_no_connection()
    {
        var factory = new NeverConnectFactory();
        var transport = new ErrorQueueTransport(QueueOptions(), factory);
        await transport.DeclareAsync(Array.Empty<DeadLetterOptions>(), TestContext.Current.CancellationToken);
        Assert.Equal(0, factory.Calls);
        await transport.DisposeAsync();
    }

    private sealed record WidgetEvent(string Id);

    private sealed class WidgetConsumer : IConsumer<WidgetEvent>
    {
        public Task Consume(ConsumeContext<WidgetEvent> context) => Task.CompletedTask;
    }

    private static TypedConsumerRegistration Registration(string stream, DeadLetterOptions? deadLetter, bool callback)
        => new()
        {
            BusMarker = typeof(DefaultBus),
            MessageType = typeof(WidgetEvent),
            ConsumerType = typeof(WidgetConsumer),
            Stream = stream,
            Options = new ConsumeOptions
            {
                Reference = "grp",
                Retry = new RetryPolicy { MaxAttempts = 2 },
                ErrorStream = deadLetter is { IsStream: true } ? new ErrorStreamOptions { Stream = deadLetter.Name } : null,
                DeadLetter = deadLetter is { IsStream: false } ? deadLetter : null,
                ExhaustionCallback = callback ? (_, _) => Task.CompletedTask : null,
            },
        };

    [Fact]
    public void Collect_includes_derived_and_explicit_queues_but_not_shadowed()
    {
        var regs = new[]
        {
            Registration("orders", new DeadLetterOptions { Name = "orders_error", QueueType = DeadLetterQueueType.ClassicQueue }, callback: false),
            Registration("audit", new DeadLetterOptions { Name = "audit-errors", QueueType = DeadLetterQueueType.QuorumQueue }, callback: false),
            Registration("pay", new DeadLetterOptions { Name = "pay-errors", QueueType = DeadLetterQueueType.ClassicQueue }, callback: true),
            Registration("plain", null, callback: false),
        };
        var queues = ErrorQueueTransport.CollectQueueDeclarations(regs);
        Assert.Equal(2, queues.Count);
        Assert.Contains(queues, q => q.Name == "orders_error" && q.QueueType == DeadLetterQueueType.ClassicQueue);
        Assert.Contains(queues, q => q.Name == "audit-errors" && q.QueueType == DeadLetterQueueType.QuorumQueue);
    }

    [Fact]
    public void Collect_empty_means_no_amqp_footprint()
    {
        var regs = new[]
        {
            Registration("plain", null, callback: false),
        };
        Assert.Empty(ErrorQueueTransport.CollectQueueDeclarations(regs));
        Assert.Empty(ErrorQueueTransport.CollectQueueDeclarations(Array.Empty<TypedConsumerRegistration>()));
    }
}
