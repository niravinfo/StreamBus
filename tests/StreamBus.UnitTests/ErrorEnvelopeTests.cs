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
        MessageId = "m-1",
        CorrelationId = "c-1",
        RoutingValue = "k1",
        ErrorRoutingValue = "orders",
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
    [InlineData("error-routing-value")]
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
    public void Replay_naming_is_stable_bounded_and_id_is_the_offset()
    {
        var a = ReplayNaming.ProducerName("orders-2");
        Assert.Equal(a, ReplayNaming.ProducerName("orders-2"));
        Assert.NotEqual(a, ReplayNaming.ProducerName("orders-3"));
        Assert.True(System.Text.Encoding.UTF8.GetByteCount(a) <= 255);
        Assert.Equal(41UL, ReplayNaming.PublishingId(41));
        var r = ReplayNaming.Reference("orders.error");
        Assert.Equal(r, ReplayNaming.Reference("orders.error"));
    }
}
