using System.Text;
using System.Text.Json;

namespace StreamBus;

/// <summary>
/// Broker metadata and application properties for one delivery.
/// </summary>
public sealed record MessageMetadata
{
    /// <summary>Broker message id, when present.</summary>
    public string? MessageId { get; init; }

    /// <summary>Broker correlation id, when present.</summary>
    public string? CorrelationId { get; init; }

    /// <summary>
    /// Application properties/headers supplied by the publisher or filters.
    /// </summary>
    public IReadOnlyDictionary<string, object?> ApplicationProperties { get; init; }
        = new Dictionary<string, object?>();

    /// <summary>Broker timestamp, when present.</summary>
    public DateTimeOffset? Timestamp { get; init; }

    internal IStreamSerializer Serializer { get; init; } = SystemTextJsonStreamSerializer.Default;

    /// <summary>Gets a header value as a string.</summary>
    /// <param name="key">Header key.</param>
    /// <returns>The value when it is a string; otherwise null.</returns>
    public string? GetString(string key)
        => ApplicationProperties.TryGetValue(key, out var value) ? value as string : null;

    /// <summary>Gets a header value as an <see cref="int"/>.</summary>
    /// <param name="key">Header key.</param>
    /// <returns>The value when it is an integer in range; otherwise null.</returns>
    public int? GetInt(string key)
        => TryNumeric(key, out var value) && value is >= int.MinValue and <= int.MaxValue
            ? (int)value
            : null;

    /// <summary>Gets a header value as a <see cref="long"/>.</summary>
    /// <param name="key">Header key.</param>
    /// <returns>The value when it is an integer; otherwise null.</returns>
    public long? GetLong(string key)
        => TryNumeric(key, out var value) ? value : null;

    /// <summary>Gets a header value as a <see cref="bool"/>.</summary>
    /// <param name="key">Header key.</param>
    /// <returns>The value when it is a Boolean; otherwise null.</returns>
    public bool? GetBool(string key)
        => ApplicationProperties.TryGetValue(key, out var value) && value is bool boolean
            ? boolean
            : null;

    /// <summary>
    /// Deserializes a JSON string header into <typeparamref name="T"/>.
    /// </summary>
    /// <typeparam name="T">Target type.</typeparam>
    /// <param name="key">Header key.</param>
    /// <returns>The deserialized value, or the default value when absent.</returns>
    /// <exception cref="ArgumentException">
    /// The header is not a string or does not contain valid JSON for
    /// <typeparamref name="T"/>.
    /// </exception>
    public T? GetJson<T>(string key)
    {
        if (!ApplicationProperties.TryGetValue(key, out var value))
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
        catch (JsonException ex)
        {
            throw new ArgumentException(
                $"Header '{key}' is not valid JSON for {typeof(T).FullName}.",
                nameof(key),
                ex);
        }
    }

    private bool TryNumeric(string key, out long value)
    {
        if (ApplicationProperties.TryGetValue(key, out var raw))
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

/// <summary>
/// One raw delivery from a stream partition.
/// </summary>
public sealed record ConsumeContext
{
    /// <summary>Source stream name.</summary>
    public required string Stream { get; init; }

    /// <summary>Source partition index.</summary>
    public required int Partition { get; init; }

    /// <summary>Source offset for this delivery.</summary>
    public required ulong Offset { get; init; }

    /// <summary>Routing value selected by the publisher; null for a plain stream.</summary>
    public required string? RoutingValue { get; init; }

    /// <summary>Serialized message body.</summary>
    public required ReadOnlyMemory<byte> Body { get; init; }

    /// <summary>Broker metadata and application properties.</summary>
    public required MessageMetadata Metadata { get; init; }

    /// <summary>Cancellation token for this delivery.</summary>
    public required CancellationToken CancellationToken { get; init; }

    internal IServiceProvider? Services { get; init; }

    internal object? TypedMessage { get; init; }
}
