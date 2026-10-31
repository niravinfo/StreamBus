using System.Text.Json;

namespace StreamBus.UnitTests;

public sealed class HeadersTests
{
    [Fact]
    public void Read_accessors_return_the_legal_value_set()
    {
        var headers = new StreamHeaders(new Dictionary<string, object?>
        {
            ["text"] = "value",
            ["int"] = 42,
            ["long"] = 9_000_000_000L,
            ["bool"] = true,
            ["json"] = "{\"Id\":7}",
            ["not-a-number"] = "42",
        }, SystemTextJsonStreamSerializer.Default);

        Assert.Equal("value", headers.GetString("text"));
        Assert.Equal(42, headers.GetInt("int"));
        Assert.Null(headers.GetInt("long"));
        Assert.Equal(42L, headers.GetLong("int"));
        Assert.Equal(9_000_000_000L, headers.GetLong("long"));
        Assert.True(headers.GetBool("bool"));
        Assert.Null(headers.GetBool("text"));
        Assert.Null(headers.GetInt("not-a-number"));
        Assert.Null(headers.GetString("missing"));
        Assert.Equal(7, headers.GetJson<HeaderPayload>("json")!.Id);
    }

    [Fact]
    public void GetJson_uses_the_configured_serializer()
    {
        var camelCase = new SystemTextJsonStreamSerializer(new JsonSerializerOptions
        {
            PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
        });
        var headers = new StreamHeaders(new Dictionary<string, object?>
        {
            ["payload"] = "{\"id\":7}",
        }, camelCase);

        Assert.Equal(7, headers.GetJson<HeaderPayload>("payload")!.Id);
    }

    [Fact]
    public void GetJson_invalid_json_names_the_key_and_keeps_the_json_exception()
    {
        var headers = new StreamHeaders(new Dictionary<string, object?>
        {
            ["payload"] = "{not-json",
        }, SystemTextJsonStreamSerializer.Default);

        var ex = Assert.Throws<ArgumentException>(() => headers.GetJson<HeaderPayload>("payload"));
        Assert.Contains("payload", ex.Message, StringComparison.Ordinal);
        Assert.IsType<JsonException>(ex.InnerException);
    }

    [Theory]
    [InlineData("streambus.message-id")]
    [InlineData("streambus.correlation-id")]
    [InlineData("streambus.sent-time")]
    [InlineData("streambus.routing-key")]
    public void Send_headers_reject_framework_keys(string key)
    {
        var headers = new StreamSendHeaders(null, SystemTextJsonStreamSerializer.Default);
        var ex = Assert.Throws<ArgumentException>(() => headers.Set(key, "spoofed"));
        Assert.Contains(key, ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void Send_headers_reject_illegal_value_types()
    {
        var headers = new StreamSendHeaders(null, SystemTextJsonStreamSerializer.Default);
        var ex = Assert.Throws<ArgumentException>(() => headers.Set("tenant", 1.5));
        Assert.Contains("tenant", ex.Message, StringComparison.Ordinal);
        Assert.Contains("Double", ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void Send_headers_support_set_get_remove()
    {
        var headers = new StreamSendHeaders(new Dictionary<string, object?> { ["seed"] = 1 },
            SystemTextJsonStreamSerializer.Default);
        headers.Set("tenant", "acme");
        Assert.True(headers.TryGet("tenant", out var value));
        Assert.Equal("acme", value);
        Assert.True(headers.Remove("tenant"));
        Assert.False(headers.TryGet("tenant", out _));
        headers.Reset();
        Assert.Empty(headers.All);
    }

    [Fact]
    public void Materialize_lets_call_level_headers_win()
    {
        var callLevel = new StreamMessageProperties
        {
            Headers = new Dictionary<string, object?> { ["tenant"] = "call-level", ["call-only"] = true },
        };
        var headers = new StreamSendHeaders(callLevel.Headers, SystemTextJsonStreamSerializer.Default);
        headers.Set("tenant", "filter-level");
        headers.Set("filter-only", 7);

        var merged = headers.Materialize(callLevel)!;
        Assert.Equal("call-level", merged.Headers["tenant"]);
        Assert.Equal(true, merged.Headers["call-only"]);
        Assert.Equal(7, merged.Headers["filter-only"]);
    }

    [Fact]
    public void Remove_hides_a_call_level_header_from_the_materialized_set()
    {
        var callLevel = new StreamMessageProperties
        {
            Headers = new Dictionary<string, object?> { ["tenant"] = "call-level", ["keep"] = true },
        };
        var headers = new StreamSendHeaders(callLevel.Headers, SystemTextJsonStreamSerializer.Default);

        Assert.True(headers.Remove("tenant"));
        var merged = headers.Materialize(callLevel)!;

        Assert.False(merged.Headers.ContainsKey("tenant"));
        Assert.Equal(true, merged.Headers["keep"]);
    }

    [Fact]
    public void Reset_clears_removal_tombstones_so_the_next_attempt_sees_the_header_again()
    {
        var callLevel = new StreamMessageProperties
        {
            Headers = new Dictionary<string, object?> { ["tenant"] = "call-level" },
        };
        var headers = new StreamSendHeaders(callLevel.Headers, SystemTextJsonStreamSerializer.Default);
        headers.Remove("tenant");
        Assert.False(headers.Materialize(callLevel)!.Headers.ContainsKey("tenant"));

        headers.Reset();

        Assert.Equal("call-level", headers.Materialize(callLevel)!.Headers["tenant"]);
    }

    [Fact]
    public void All_includes_framework_and_user_keys()
    {
        var headers = new StreamHeaders(new Dictionary<string, object?>
        {
            ["streambus.routing-key"] = "key-a",
            ["x-tenant"] = "acme",
        }, SystemTextJsonStreamSerializer.Default);

        Assert.Equal("acme", headers.All["x-tenant"]);
        Assert.Equal("key-a", headers.All["streambus.routing-key"]);
    }
}
