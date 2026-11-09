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
    public void Sent_time_with_an_offset_is_normalized_to_the_same_instant()
    {
        var instant = new DateTimeOffset(2026, 10, 31, 9, 12, 3, 284, TimeSpan.FromHours(2));
        var app = Sample().ToApplicationProperties();
        app["streambus.error.sent-time"] = instant.ToString("o", CultureInfo.InvariantCulture);

        Assert.True(ErrorEnvelope.TryParse("e", new byte[] { 9 }, app, out var parsed));

        Assert.Equal(instant.UtcDateTime, parsed!.SentTime);
        Assert.Equal(DateTimeKind.Utc, parsed.SentTime!.Value.Kind);
    }

    private static Dictionary<string, object?> UserHeaders() => new()
    {
        ["x-tenant"] = "acme",
        ["x-count"] = 3,
        ["x-big"] = 5_000_000_000L,
        ["x-flag"] = true,
    };

    private static void AssertUserHeadersEqual(
        IReadOnlyDictionary<string, object?> expected,
        IReadOnlyDictionary<string, object?>? actual)
    {
        Assert.NotNull(actual);
        var actualDict = new Dictionary<string, object?>(actual!);
        var expectedDict = new Dictionary<string, object?>(expected);
        Assert.Equal(expectedDict.Count, actualDict.Count);
        foreach (var (key, value) in expectedDict)
        {
            Assert.True(actualDict.TryGetValue(key, out var roundTripped), $"missing header '{key}'");
            Assert.Equal(value, roundTripped);
        }
    }

    [Fact]
    public void Round_trip_via_application_properties_preserves_user_headers_and_content_type()
    {
        var sample = Sample() with
        {
            UserHeaders = UserHeaders(),
            ContentType = "application/vnd.acme+json",
        };

        Assert.True(ErrorEnvelope.TryParse(
            sample.ErrorStream, sample.Body, sample.ToApplicationProperties(), out var parsed));

        Assert.Equal(sample.ContentType, parsed!.ContentType);
        AssertUserHeadersEqual(sample.UserHeaders!, parsed.UserHeaders);
    }

    [Fact]
    public void Round_trip_via_queue_headers_preserves_user_headers_and_content_type()
    {
        var sample = Sample() with
        {
            UserHeaders = UserHeaders(),
            ContentType = "application/vnd.acme+json",
        };

        Assert.True(ErrorEnvelope.TryParseHeaders(
            sample.ErrorStream, sample.Body, sample.ToHeaders(), out var parsed));

        Assert.Equal(sample.ContentType, parsed!.ContentType);
        AssertUserHeadersEqual(sample.UserHeaders!, parsed.UserHeaders);
    }

    [Fact]
    public void CaptureUserHeaders_drops_framework_keys_and_unsupported_values()
    {
        var serializer = SystemTextJsonStreamSerializer.Default;
        var headers = new StreamHeaders(new Dictionary<string, object?>
        {
            ["x-tenant"] = "acme",
            ["x-amount"] = 3,
            ["x-null"] = null,
            ["streambus.message-id"] = "ignored",
            ["streambus.routing-key"] = "ignored",
            ["x-foreign"] = 1.5,
            ["x-raw"] = new byte[] { 1 },
        }, serializer);

        var captured = ErrorEnvelope.CaptureUserHeaders(headers);

        Assert.NotNull(captured);
        Assert.Equal(2, captured!.Count);
        Assert.Equal("acme", captured["x-tenant"]);
        Assert.Equal(3, captured["x-amount"]);
    }

    [Fact]
    public void User_headers_travel_on_the_wire_as_plain_keys_not_as_a_blob()
    {
        // MassTransit model: the original headers are carried as-is next to the
        // error metadata; nothing repackaged into a single JSON field.
        var sample = Sample() with
        {
            UserHeaders = UserHeaders(),
            SendHeaders = new Dictionary<string, object?>
            {
                // Consume-filter output wins over the original user value.
                ["x-tenant"] = "override",
                ["x-output"] = "from-filter",
            },
        };

        var app = sample.ToApplicationProperties();
        var queueHeaders = sample.ToHeaders();

        foreach (var (key, value) in sample.UserHeaders!)
        {
            Assert.True(app.ContainsKey(key), $"missing raw key '{key}'");
            Assert.True(queueHeaders.TryGetValue(key, out var _), $"missing raw key '{key}'");
        }
        Assert.Equal(3, app["x-count"]); // raw, typed value — not a JSON blob
        Assert.Equal("override", queueHeaders["x-tenant"]); // filter output wins
        Assert.Equal("from-filter", queueHeaders["x-output"]);
        Assert.False(app.ContainsKey("streambus.error.user-headers"));
    }

    [Fact]
    public void Queue_path_decodes_byte_array_header_values_as_strings()
    {
        // The AMQP client hands longstr header values back as byte[].
        var sample = Sample() with
        {
            UserHeaders = new Dictionary<string, object?> { ["x-tenant"] = "acme" },
        };
        var brokerLike = sample.ToApplicationProperties().ToDictionary(
            kv => kv.Key,
            kv => (object?)(kv.Value is string s ? System.Text.Encoding.UTF8.GetBytes(s) : kv.Value));

        Assert.True(ErrorEnvelope.TryParseHeaders(
            sample.ErrorStream, sample.Body, brokerLike, out var parsed));

        Assert.NotNull(parsed!.UserHeaders);
        Assert.Equal("acme", parsed.UserHeaders["x-tenant"]);
        Assert.IsType<string>(parsed.UserHeaders["x-tenant"]);
    }

    [Fact]
    public void Missing_user_headers_round_trip_yields_null_without_failing_parse()
    {
        var sample = Sample();
        Assert.True(ErrorEnvelope.TryParse(
            sample.ErrorStream, sample.Body, sample.ToApplicationProperties(), out var parsed));
        Assert.Null(parsed!.UserHeaders);
        Assert.Null(parsed.ContentType);
    }
}
