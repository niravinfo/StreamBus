using System.Text.Json;
using RabbitMQ.Stream.Client;

namespace StreamBus.UnitTests;

public sealed record HeaderPayload(int Id);

public sealed class HeaderCarrierTests
{
    [Fact]
    public void Legal_header_values_are_accepted()
    {
        StreamMessageProperties.ValidateValue("string", "value");
        StreamMessageProperties.ValidateValue("bool", true);
        StreamMessageProperties.ValidateValue("int", 42);
        StreamMessageProperties.ValidateValue("long", 9_000_000_000L);
        StreamMessageProperties.ValidateValue("null", null);
    }

    [Fact]
    public void Illegal_header_value_names_the_key_and_type()
    {
        var ex = Assert.Throws<ArgumentException>(() => StreamMessageProperties.ValidateValue("tenant", 1.5));
        Assert.Contains("tenant", ex.Message, StringComparison.Ordinal);
        Assert.Contains("Double", ex.Message, StringComparison.Ordinal);
        Assert.Contains("string, bool, int, long", ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task Plain_publish_renders_ids_and_headers_on_the_client_message()
    {
        var producer = new CapturingProducer();
        var confirmations = new ConfirmationTracker();
        var handle = new StreamProducerHandle(producer, "orders", confirmations);
        var properties = new StreamMessageProperties
        {
            MessageId = "m-1",
            CorrelationId = "c-1",
            Headers = new Dictionary<string, object?>
            {
                ["tenant"] = "acme",
                ["attempt"] = 3,
                ["sequence"] = 9_000_000_000L,
                ["flag"] = true,
            },
        };

        var send = handle.SendAsync(new byte[] { 1 }, new PublishOptions { Properties = properties }, TestContext.Current.CancellationToken);
        var (id, message) = await AwaitSendAsync(producer.Sent, 0, TestContext.Current.CancellationToken);
        confirmations.Complete(new Confirmation { PublishingId = id, Code = ResponseCode.Ok });
        await send;

        Assert.Equal("m-1", message.Properties!.MessageId);
        Assert.Equal("c-1", message.Properties.CorrelationId);
        Assert.Equal("acme", message.ApplicationProperties!["tenant"]);
        Assert.Equal(3, message.ApplicationProperties["attempt"]);
        Assert.Equal(9_000_000_000L, message.ApplicationProperties["sequence"]);
        Assert.Equal(true, message.ApplicationProperties["flag"]);
    }

    [Fact]
    public async Task Super_publish_keeps_properties_and_the_routing_value()
    {
        var producer = new CapturingSuperProducer();
        var confirmations = new ConfirmationTracker();
        var routing = new SuperStreamRouting();
        var handle = new SuperStreamProducerHandle(producer, "events", confirmations, routing);
        var properties = new StreamMessageProperties
        {
            MessageId = "m-2",
            Headers = new Dictionary<string, object?> { ["tenant"] = "acme" },
        };

        var send = handle.SendAsync(
            new byte[] { 2 },
            new PublishOptions { IsSuperStream = true, RoutingKey = "key-a", Properties = properties },
            TestContext.Current.CancellationToken);
        var (id, message) = await AwaitSendAsync(producer.Sent, 0, TestContext.Current.CancellationToken);
        Assert.Equal("key-a", routing.Route(message));
        confirmations.Complete(new Confirmation { PublishingId = id, Code = ResponseCode.Ok });
        await send;

        Assert.Equal("m-2", message.Properties!.MessageId);
        Assert.Equal("acme", message.ApplicationProperties!["tenant"]);
    }

    [Fact]
    public async Task Illegal_header_value_fails_before_the_producer_sends()
    {
        var producer = new CapturingProducer();
        var handle = new StreamProducerHandle(producer, "orders", new ConfirmationTracker());
        var properties = new StreamMessageProperties
        {
            Headers = new Dictionary<string, object?> { ["bad"] = 1.5 },
        };

        var ex = await Assert.ThrowsAsync<ArgumentException>(() =>
            handle.SendAsync(new byte[] { 1 }, new PublishOptions { Properties = properties }, TestContext.Current.CancellationToken));
        Assert.Contains("bad", ex.Message, StringComparison.Ordinal);
        Assert.Empty(producer.Sent);
    }

    [Fact]
    public void Metadata_accessors_read_the_legal_value_set()
    {
        var serializer = new RecordingSerializer();
        var metadata = new MessageMetadata
        {
            ApplicationProperties = new Dictionary<string, object?>
            {
                ["text"] = "value",
                ["int"] = 42,
                ["long"] = 9_000_000_000L,
                ["bool"] = true,
                ["json"] = "{\"Id\":7}",
                ["not-a-number"] = "42",
            },
            Serializer = serializer,
        };

        Assert.Equal("value", metadata.GetString("text"));
        Assert.Equal(42, metadata.GetInt("int"));
        Assert.Null(metadata.GetInt("long"));
        Assert.Equal(42L, metadata.GetLong("int"));
        Assert.Equal(9_000_000_000L, metadata.GetLong("long"));
        Assert.True(metadata.GetBool("bool"));
        Assert.Null(metadata.GetBool("text"));
        Assert.Null(metadata.GetInt("not-a-number"));
        Assert.Null(metadata.GetString("missing"));

        var payload = metadata.GetJson<HeaderPayload>("json");
        Assert.NotNull(payload);
        Assert.Equal(7, payload.Id);
        Assert.True(serializer.DeserializeCalled);
    }

    [Fact]
    public void GetJson_invalid_json_names_the_key_and_keeps_the_json_exception()
    {
        var metadata = new MessageMetadata
        {
            ApplicationProperties = new Dictionary<string, object?> { ["payload"] = "{not-json" },
        };

        var ex = Assert.Throws<ArgumentException>(() => metadata.GetJson<HeaderPayload>("payload"));
        Assert.Contains("payload", ex.Message, StringComparison.Ordinal);
        Assert.IsType<JsonException>(ex.InnerException);
    }

    private static async Task<(ulong Id, Message Message)> AwaitSendAsync(
        List<(ulong Id, Message Message)> sent,
        int before,
        CancellationToken ct)
    {
        var deadline = DateTime.UtcNow + TimeSpan.FromSeconds(5);
        while (DateTime.UtcNow < deadline)
        {
            lock (sent)
            {
                if (sent.Count > before)
                    return sent[^1];
            }
            await Task.Delay(10, ct);
        }
        throw new TimeoutException("The producer did not send the message.");
    }

    private sealed class RecordingSerializer : IStreamSerializer
    {
        public bool DeserializeCalled { get; private set; }

        public byte[] Serialize<T>(T message) => throw new NotSupportedException("Header tests never serialize bodies.");

        public T Deserialize<T>(ReadOnlyMemory<byte> body)
        {
            DeserializeCalled = true;
            return SystemTextJsonStreamSerializer.Default.Deserialize<T>(body);
        }
    }

    private sealed class CapturingProducer : IProducer
    {
        public readonly List<(ulong Id, Message Message)> Sent = new();

        public ValueTask Send(ulong publishingId, Message message)
        {
            lock (Sent) Sent.Add((publishingId, message));
            return ValueTask.CompletedTask;
        }

        public ValueTask Send(List<(ulong, Message)> messages) => throw new NotSupportedException("Header tests send one message at a time.");
        public ValueTask Send(ulong publishingId, List<Message> subEntryMessages, CompressionType compressionType) => throw new NotSupportedException("Header tests never send sub-entry batches.");
        public Task<ulong> GetLastPublishingId() => Task.FromResult(0UL);
        public Task<ResponseCode> Close() => Task.FromResult(ResponseCode.Ok);
        public bool IsOpen() => true;
        public void Dispose() { }
        public int MessagesSent => 0;
        public int ConfirmFrames => 0;
        public int IncomingFrames => 0;
        public int PublishCommandsSent => 0;
        public int PendingCount => 0;
        public ProducerInfo Info => throw new NotSupportedException("Header tests never inspect producer info.");
    }

    private sealed class CapturingSuperProducer : ISuperStreamProducer
    {
        public readonly List<(ulong Id, Message Message)> Sent = new();

        public ValueTask Send(ulong publishingId, Message message)
        {
            lock (Sent) Sent.Add((publishingId, message));
            return ValueTask.CompletedTask;
        }

        public ValueTask Send(List<(ulong, Message)> messages) => throw new NotSupportedException("Header tests send one message at a time.");
        public ValueTask Send(ulong publishingId, List<Message> subEntryMessages, CompressionType compressionType) => throw new NotSupportedException("Header tests never send sub-entry batches.");
        public Task<ulong> GetLastPublishingId() => Task.FromResult(0UL);
        public bool IsOpen() => true;
        public void Dispose() { }
        public Task<ResponseCode> Close() => Task.FromResult(ResponseCode.Ok);
        public int MessagesSent => 0;
        public int ConfirmFrames => 0;
        public int IncomingFrames => 0;
        public int PublishCommandsSent => 0;
        public int PendingCount => 0;
        public ProducerInfo Info => throw new NotSupportedException("Header tests never inspect producer info.");
        public Task ReconnectPartition(StreamInfo streamInfo) => throw new NotSupportedException("Header tests never reconnect partitions.");
    }
}
