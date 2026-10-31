using System.Text;

namespace StreamBus;

/// <summary>Read-only access to the application properties of a delivery.</summary>
public interface IStreamHeaders
{
    /// <summary>Every header, framework keys included.</summary>
    IReadOnlyDictionary<string, object?> All { get; }

    /// <summary>Gets a header value by key.</summary>
    bool TryGet(string key, out object? value);

    /// <summary>Gets a header value as a string.</summary>
    string? GetString(string key);

    /// <summary>Gets a header value as an <see cref="int"/>.</summary>
    int? GetInt(string key);

    /// <summary>Gets a header value as a <see cref="long"/>.</summary>
    long? GetLong(string key);

    /// <summary>Gets a header value as a <see cref="bool"/>.</summary>
    bool? GetBool(string key);

    /// <summary>Deserializes a JSON string header with the bus serializer.</summary>
    T? GetJson<T>(string key);
}

/// <summary>Writable headers carried on a publish or on a consume result.</summary>
public interface IStreamSendHeaders : IStreamHeaders
{
    /// <summary>Sets a header value.</summary>
    void Set(string key, object? value);

    /// <summary>Removes a header value; returns whether it was present.</summary>
    bool Remove(string key);
}

internal static class StreamHeaderKeys
{
    internal const string MessageId = "streambus.message-id";
    internal const string CorrelationId = "streambus.correlation-id";
    internal const string SentTime = "streambus.sent-time";
    internal const string RoutingKey = "streambus.routing-key";

    internal static void EnsureNotReserved(string key)
    {
        if (key is MessageId or CorrelationId or SentTime or RoutingKey)
        {
            throw new ArgumentException(
                $"Header '{key}' is reserved by StreamBus and cannot be set by user code.",
                nameof(key));
        }
    }
}

internal abstract class StreamHeaderBag : IStreamHeaders
{
    protected StreamHeaderBag(IStreamSerializer serializer) => Serializer = serializer;

    protected IStreamSerializer Serializer { get; }

    protected abstract IReadOnlyDictionary<string, object?> Values { get; }

    public IReadOnlyDictionary<string, object?> All => Values;

    public bool TryGet(string key, out object? value) => Values.TryGetValue(key, out value);

    public string? GetString(string key)
        => Values.TryGetValue(key, out var value) ? value as string : null;

    public int? GetInt(string key)
        => TryNumeric(key, out var value) && value is >= int.MinValue and <= int.MaxValue
            ? (int)value
            : null;

    public long? GetLong(string key)
        => TryNumeric(key, out var value) ? value : null;

    public bool? GetBool(string key)
        => Values.TryGetValue(key, out var value) && value is bool boolean
            ? boolean
            : null;

    public T? GetJson<T>(string key)
    {
        if (!Values.TryGetValue(key, out var value))
            return default;
        if (value is null)
            return default;
        if (value is not string json)
        {
            throw new ArgumentException(
                $"Header '{key}' must be a string containing JSON for {typeof(T).FullName}.",
                nameof(key));
        }

        try
        {
            return Serializer.Deserialize<T>(Encoding.UTF8.GetBytes(json));
        }
        catch (System.Text.Json.JsonException ex)
        {
            throw new ArgumentException(
                $"Header '{key}' is not valid JSON for {typeof(T).FullName}.",
                nameof(key),
                ex);
        }
    }

    private bool TryNumeric(string key, out long value)
    {
        if (Values.TryGetValue(key, out var raw))
        {
            switch (raw)
            {
                case int i:
                    value = i;
                    return true;
                case long l:
                    value = l;
                    return true;
            }
        }

        value = 0;
        return false;
    }
}

internal sealed class StreamHeaders : StreamHeaderBag
{
    private readonly IReadOnlyDictionary<string, object?> _values;

    public StreamHeaders(IReadOnlyDictionary<string, object?> values, IStreamSerializer serializer)
        : base(serializer)
        => _values = values;

    protected override IReadOnlyDictionary<string, object?> Values => _values;
}

internal sealed class StreamSendHeaders : StreamHeaderBag, IStreamSendHeaders
{
    private readonly Dictionary<string, object?> _values;
    private readonly HashSet<string> _removed = new(StringComparer.Ordinal);

    public StreamSendHeaders(IReadOnlyDictionary<string, object?>? initial, IStreamSerializer serializer)
        : base(serializer)
        => _values = initial is null
            ? new Dictionary<string, object?>(StringComparer.Ordinal)
            : new Dictionary<string, object?>(initial, StringComparer.Ordinal);

    protected override IReadOnlyDictionary<string, object?> Values => _values;

    public void Set(string key, object? value)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(key);
        StreamHeaderKeys.EnsureNotReserved(key);
        StreamMessageProperties.ValidateValue(key, value);
        _values[key] = value;
        _removed.Remove(key);
    }

    public bool Remove(string key)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(key);
        _removed.Add(key);
        return _values.Remove(key);
    }

    internal void Reset()
    {
        _values.Clear();
        _removed.Clear();
    }

    internal IReadOnlyDictionary<string, object?> Snapshot()
        => new Dictionary<string, object?>(_values, StringComparer.Ordinal);

    internal StreamMessageProperties Materialize(StreamMessageProperties? callLevel)
    {
        var merged = new Dictionary<string, object?>(_values, StringComparer.Ordinal);
        if (callLevel is not null)
        {
            foreach (var (key, value) in callLevel.Headers)
                merged[key] = value;
        }

        foreach (var key in _removed)
            merged.Remove(key);

        return (callLevel ?? new StreamMessageProperties()) with { Headers = merged };
    }
}
