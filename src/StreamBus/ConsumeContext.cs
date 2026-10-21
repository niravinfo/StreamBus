using System.Text;
using System.Text.Json;

namespace StreamBus;

public sealed record MessageMetadata
{
    public string? MessageId { get; init; }
    public string? CorrelationId { get; init; }
    public IReadOnlyDictionary<string, object?> ApplicationProperties { get; init; }
        = new Dictionary<string, object?>();
    public DateTimeOffset? Timestamp { get; init; }

    internal IStreamSerializer Serializer { get; init; } = SystemTextJsonStreamSerializer.Default;

    public string? GetString(string key)
        => ApplicationProperties.TryGetValue(key, out var value) ? value as string : null;

    public int? GetInt(string key)
        => TryNumeric(key, out var value) && value is >= int.MinValue and <= int.MaxValue
            ? (int)value
            : null;

    public long? GetLong(string key)
        => TryNumeric(key, out var value) ? value : null;

    public bool? GetBool(string key)
        => ApplicationProperties.TryGetValue(key, out var value) && value is bool boolean
            ? boolean
            : null;

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

public sealed record ConsumeContext
{
    public required string Stream { get; init; }
    public required int Partition { get; init; }
    public required ulong Offset { get; init; }
    public required string? RoutingValue { get; init; }
    public required ReadOnlyMemory<byte> Body { get; init; }
    public required MessageMetadata Metadata { get; init; }
    public required CancellationToken CancellationToken { get; init; }
}
