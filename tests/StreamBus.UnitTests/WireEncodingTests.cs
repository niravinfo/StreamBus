using RabbitMQ.Stream.Client;
using RabbitMQ.Stream.Client.AMQP;

namespace StreamBus.UnitTests;

public sealed class WireEncodingTests
{
    [Fact]
    public void Publish_writes_ids_sent_time_and_content_type_to_amqp_properties()
    {
        var messageId = Guid.Parse("0f8fad5b-d9cb-469f-a165-70867728950e");
        var correlationId = Guid.Parse("6b1f5c1a-0d9e-4b6f-9a4e-1f2d3c4b5a69");
        var sentTime = new DateTime(2026, 10, 31, 9, 12, 3, 284, DateTimeKind.Utc);
        var message = new Message(new byte[] { 1 });

        StreamMessageProperties.ApplyTo(message, new StreamMessageProperties
        {
            MessageId = messageId,
            CorrelationId = correlationId,
            SentTime = sentTime,
            ContentType = "application/json",
            Headers = new Dictionary<string, object?> { ["x-tenant"] = "acme" },
        });

        Assert.Equal("0f8fad5b-d9cb-469f-a165-70867728950e", message.Properties!.MessageId);
        Assert.Equal("6b1f5c1a-0d9e-4b6f-9a4e-1f2d3c4b5a69", message.Properties.CorrelationId);
        Assert.Equal(sentTime, message.Properties.CreationTime);
        Assert.Equal("application/json", message.Properties.ContentType);
        Assert.Equal("acme", message.ApplicationProperties!["x-tenant"]);
        Assert.False(message.ApplicationProperties.ContainsKey("streambus.message-id"));
    }

    [Fact]
    public void Publish_generates_no_properties_section_when_nothing_to_write()
    {
        var message = new Message(new byte[] { 1 });
        StreamMessageProperties.ApplyTo(message, new StreamMessageProperties());
        Assert.Null(message.Properties);
        Assert.Null(message.ApplicationProperties);
    }

    [Fact]
    public void Sent_time_survives_the_wire_round_trip()
    {
        var sentTime = StreamWire.NormalizeSentTime(DateTime.UtcNow);
        var message = new Message(new byte[] { 1 });
        StreamMessageProperties.ApplyTo(message, new StreamMessageProperties { SentTime = sentTime });
        Assert.Equal(sentTime, StreamWire.ReadSentTime(message.Properties));
        Assert.Equal(DateTimeKind.Utc, StreamWire.ReadSentTime(message.Properties)!.Value.Kind);
    }

    [Fact]
    public void Absent_creation_time_reads_as_null()
    {
        Assert.Null(StreamWire.ReadSentTime(new Properties()));
        Assert.Null(StreamWire.ReadSentTime(null));
    }

    [Fact]
    public void Message_ids_parse_from_strings_and_native_uuids()
    {
        var expected = Guid.Parse("0f8fad5b-d9cb-469f-a165-70867728950e");
        Assert.Equal(expected, StreamWire.ReadGuid(expected.ToString("D")));

        // AMQP 1.0 UUID is network byte order; .NET Guid is little-endian in its
        // first three groups, so the native form must be converted, not copied.
        var amqpUuid = new byte[]
        {
            0x0f, 0x8f, 0xad, 0x5b, 0xd9, 0xcb, 0x46, 0x9f,
            0xa1, 0x65, 0x70, 0x86, 0x77, 0x28, 0x95, 0x0e,
        };
        Assert.Equal(expected, StreamWire.ReadGuid(amqpUuid));
    }

    [Fact]
    public void Non_guid_message_ids_read_as_null()
    {
        Assert.Null(StreamWire.ReadGuid("m-1"));
        Assert.Null(StreamWire.ReadGuid(new byte[] { 1, 2, 3 }));
        Assert.Null(StreamWire.ReadGuid(42));
        Assert.Null(StreamWire.ReadGuid(null));
    }

    [Fact]
    public void Routing_key_is_written_as_an_application_property()
    {
        var message = new Message(new byte[] { 1 });
        StreamMessageProperties.ApplyTo(message, new StreamMessageProperties(), routingKey: "key-a");
        Assert.Equal("key-a", message.ApplicationProperties!["streambus.routing-key"]);
    }

    [Fact]
    public void Reserved_user_headers_are_rejected_before_the_send()
    {
        var headers = new Dictionary<string, object?> { ["streambus.routing-key"] = "spoofed" };
        var ex = Assert.Throws<ArgumentException>(() => StreamMessageProperties.ValidateHeaders(headers));
        Assert.Contains("streambus.routing-key", ex.Message, StringComparison.Ordinal);
        StreamMessageProperties.ValidateHeaders(null); // must not throw
    }

    [Fact]
    public void Sent_time_is_truncated_to_milliseconds()
    {
        var unrounded = new DateTime(2026, 10, 31, 9, 12, 3, 284, DateTimeKind.Utc).AddTicks(6197);
        var normalized = StreamWire.NormalizeSentTime(unrounded);
        Assert.Equal(TimeSpan.Zero, TimeSpan.FromTicks(normalized.Ticks % TimeSpan.TicksPerMillisecond));
        Assert.Equal(DateTimeKind.Utc, normalized.Kind);
    }

    [Fact]
    public void Json_serializer_reports_application_json()
        => Assert.Equal("application/json", SystemTextJsonStreamSerializer.Default.ContentType);
}
