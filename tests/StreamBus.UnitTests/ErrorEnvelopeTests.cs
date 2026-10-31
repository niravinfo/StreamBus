using System.Globalization;

namespace StreamBus.UnitTests;

public sealed class ErrorEnvelopeTests
{
    private static ErrorEnvelope Sample() => new()
    {
        ErrorStream = "orders.error",
        SourceStream = "orders",
        SourcePartition = 2,
        SourcePartitionStream = "orders-2",
        SourceOffset = 41,
        Body = new byte[] { 1, 2, 3 },
        ExceptionType = "System.InvalidOperationException",
        ExceptionMessage = "boom",
        ExceptionStack = "stack",
        Attempts = 3,
        FailedAt = new DateTimeOffset(2026, 10, 13, 12, 0, 0, TimeSpan.Zero),
        MessageId = Guid.Parse("0f8fad5b-d9cb-469f-a165-70867728950e"),
        CorrelationId = Guid.Parse("6b1f5c1a-0d9e-4b6f-9a4e-1f2d3c4b5a69"),
        SentTime = new DateTime(2026, 10, 31, 9, 12, 3, 284, DateTimeKind.Utc),
        RoutingValue = "k1",
    };

    [Fact]
    public void Round_trip_preserves_every_field_and_body()
    {
        var sample = Sample();
        Assert.True(ErrorEnvelope.TryParse(sample.ErrorStream, sample.Body, sample.ToApplicationProperties(), out var parsed));
        Assert.NotNull(parsed);
        Assert.Equal(sample, parsed);
    }

    [Theory]
    [InlineData("source-stream")]
    [InlineData("source-partition-stream")]
    [InlineData("exception-type")]
    [InlineData("source-offset")]
    [InlineData("attempts")]
    public void Missing_required_key_fails_parse(string key)
    {
        var app = Sample().ToApplicationProperties();
        app.Remove("streambus.error." + key);
        Assert.False(ErrorEnvelope.TryParse("e", new byte[] { 9 }, app, out _));
    }

    [Fact]
    public void Garbage_values_fail_parse_without_throwing()
    {
        var app = Sample().ToApplicationProperties();
        app["streambus.error.source-offset"] = "not-a-number";
        Assert.False(ErrorEnvelope.TryParse("e", new byte[] { 9 }, app, out _));
    }

    [Fact]
    public void Replay_options_preserve_message_identity()
    {
        var sample = Sample();

        var options = sample.ToReplayOptions();

        Assert.Equal(sample.MessageId, options.Properties!.MessageId);
        Assert.Equal(sample.CorrelationId, options.Properties.CorrelationId);
        Assert.Equal(sample.SentTime, options.Properties.SentTime);
    }

    [Fact]
    public void Sent_time_with_an_offset_is_normalized_to_the_same_instant()
    {
        var instant = new DateTimeOffset(2026, 10, 31, 9, 12, 3, 284, TimeSpan.FromHours(2));
        var app = Sample().ToApplicationProperties();
        app["streambus.error.sent-time"] = instant.ToString("o", CultureInfo.InvariantCulture);

        Assert.True(ErrorEnvelope.TryParse("e", new byte[] { 9 }, app, out var parsed));

        Assert.Equal(instant.UtcDateTime, parsed!.SentTime);
        Assert.Equal(DateTimeKind.Utc, parsed.SentTime!.Value.Kind);
    }

    [Fact]
    public void Replay_naming_is_stable_bounded_and_id_is_the_offset()
    {
        var a = ReplayNaming.ProducerName("orders", "orders-2");
        Assert.Equal(a, ReplayNaming.ProducerName("orders", "orders-2"));
        Assert.NotEqual(a, ReplayNaming.ProducerName("orders", "orders-3"));
        Assert.True(System.Text.Encoding.UTF8.GetByteCount(a) <= 255);
        Assert.Equal(41UL, ReplayNaming.PublishingId(41));
        var r = ReplayNaming.Reference("orders.error");
        Assert.Equal(r, ReplayNaming.Reference("orders.error"));
    }
}
