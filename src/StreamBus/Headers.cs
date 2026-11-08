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
    /// <summary>
    /// Sets a header value. Filter mutations take precedence over values
    /// supplied at the call site: <see cref="Set"/> overwrites a call-site
    /// value for the same key.
    /// </summary>
    void Set(string key, object? value);

    /// <summary>
    /// Removes a header value; returns whether it was present. Any non-blank
    /// key is accepted, so removing a framework key is a no-op. Removal
    /// takes precedence over values supplied at the call site.
    /// </summary>
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
        catch (Exception ex)
        {
            // IStreamSerializer is pluggable: a custom serializer's parse errors
            // are not JsonException, but they are still header-value problems and
            // must surface as this API's documented ArgumentException shape.
            throw new ArgumentException(
                $"Header '{key}' could not be deserialized to {typeof(T).FullName} using {Serializer.GetType().Name}.",
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
    private readonly Dictionary<string, object?> _initial;
    private readonly Dictionary<string, object?> _values;
    private readonly HashSet<string> _removed = new(StringComparer.Ordinal);

    public StreamSendHeaders(IReadOnlyDictionary<string, object?>? initial, IStreamSerializer serializer)
        : base(serializer)
    {
        _initial = initial is null
            ? new Dictionary<string, object?>(StringComparer.Ordinal)
            : new Dictionary<string, object?>(initial, StringComparer.Ordinal);
        _values = new Dictionary<string, object?>(_initial, StringComparer.Ordinal);
    }

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

    /// <summary>
    /// Restores the pre-filter state: seeded call-site headers return,
    /// filter Set/Remove mutations and removal tombstones are cleared.
    /// Called between retry attempts so each attempt sees the original values.
    /// </summary>
    internal void Reset()
    {
        _values.Clear();
        foreach (var (key, value) in _initial)
            _values[key] = value;
        _removed.Clear();
    }

    internal IReadOnlyDictionary<string, object?> Snapshot()
        => new Dictionary<string, object?>(_values, StringComparer.Ordinal);

    internal StreamMessageProperties Materialize(StreamMessageProperties? callLevel)
    {
        // _values was seeded from the call-level headers at construction, so
        // filter Set/Remove mutations already reflect the correct precedence:
        // filters are the last writer on the send pipeline.
        var merged = new Dictionary<string, object?>(_values, StringComparer.Ordinal);

        foreach (var key in _removed)
            merged.Remove(key);

        return (callLevel ?? new StreamMessageProperties()) with { Headers = merged };
    }
}
