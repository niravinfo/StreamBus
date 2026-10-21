using System.Globalization;

namespace StreamBus;

public sealed record ErrorEnvelope
{
    public required string ErrorStream { get; init; }
    public required string SourceStream { get; init; }
    public required int SourcePartition { get; init; }
    public required string SourcePartitionStream { get; init; }
    public required ulong SourceOffset { get; init; }
    public required byte[] Body { get; init; }
    public required string ExceptionType { get; init; }
    public required string ExceptionMessage { get; init; }
    public required string ExceptionStack { get; init; }
    public required int Attempts { get; init; }
    public required DateTimeOffset FailedAt { get; init; }
    public required string? MessageId { get; init; }
    public required string? CorrelationId { get; init; }
    public required string? RoutingValue { get; init; }
    public required string ErrorRoutingValue { get; init; }

    /// <summary>Headers added by the consume pipeline for the failed delivery.
    /// They are forwarded on top of the envelope's own fields; envelope fields
    /// win when a key collides.</summary>
    public IReadOnlyDictionary<string, object?>? OutputHeaders { get; init; }

    private const string P = "streambus.error.";

    internal Dictionary<string, object?> EnvelopeApplicationProperties() => new()
    {
        [P + "source-stream"] = SourceStream,
        [P + "source-partition"] = SourcePartition.ToString(CultureInfo.InvariantCulture),
        [P + "source-partition-stream"] = SourcePartitionStream,
        [P + "source-offset"] = SourceOffset.ToString(CultureInfo.InvariantCulture),
        [P + "exception-type"] = ExceptionType,
        [P + "exception-message"] = ExceptionMessage,
        [P + "exception-stack"] = ExceptionStack,
        [P + "attempts"] = Attempts.ToString(CultureInfo.InvariantCulture),
        [P + "failed-at"] = FailedAt.ToString("o", CultureInfo.InvariantCulture),
        [P + "message-id"] = MessageId,
        [P + "correlation-id"] = CorrelationId,
        [P + "routing-value"] = RoutingValue,
        [P + "error-routing-value"] = ErrorRoutingValue,
    };

    public Dictionary<string, object?> ToApplicationProperties()
    {
        var properties = EnvelopeApplicationProperties();
        if (OutputHeaders is not null)
        {
            foreach (var (key, value) in OutputHeaders)
            {
                if (!properties.ContainsKey(key))
                    properties[key] = value;
            }
        }
        return properties;
    }

    public Dictionary<string, object?> ToHeaders()
    {
        var headers = new Dictionary<string, object?>(StringComparer.Ordinal);
        foreach (var (key, value) in ToApplicationProperties())
        {
            if (value is not null)
                headers[key] = value;
        }
        return headers;
    }

    public static bool TryParse(string errorStream, byte[] body, IReadOnlyDictionary<string, object?> app, out ErrorEnvelope? envelope)
    {
        envelope = null;
        if (string.IsNullOrWhiteSpace(errorStream) || body is null || app is null) return false;
        string? Get(string name) => app.TryGetValue(P + name, out var v) ? v?.ToString() : null;
        return TryParseCore(errorStream, body, Get, out envelope);
    }

    public static bool TryParseHeaders(string errorQueue, byte[] body, IReadOnlyDictionary<string, object?> headers, out ErrorEnvelope? envelope)
    {
        envelope = null;
        if (string.IsNullOrWhiteSpace(errorQueue) || body is null || headers is null) return false;
        string? Get(string name)
        {
            if (!headers.TryGetValue(P + name, out var v) || v is null) return null;
            return v is byte[] bytes ? System.Text.Encoding.UTF8.GetString(bytes) : v.ToString();
        }
        return TryParseCore(errorQueue, body, Get, out envelope);
    }

    private static bool TryParseCore(string errorDestination, byte[] body, Func<string, string?> get, out ErrorEnvelope? envelope)
    {
        envelope = null;
        var sourceStream = get("source-stream");
        var partitionStream = get("source-partition-stream");
        var exceptionType = get("exception-type");
        if (string.IsNullOrWhiteSpace(sourceStream)
            || string.IsNullOrWhiteSpace(partitionStream)
            || string.IsNullOrWhiteSpace(exceptionType)
            || get("source-partition") is not { } partRaw || !int.TryParse(partRaw, NumberStyles.Integer, CultureInfo.InvariantCulture, out var partition)
            || get("source-offset") is not { } offsetRaw || !ulong.TryParse(offsetRaw, NumberStyles.Integer, CultureInfo.InvariantCulture, out var offset)
            || get("attempts") is not { } attemptsRaw || !int.TryParse(attemptsRaw, NumberStyles.Integer, CultureInfo.InvariantCulture, out var attempts)
            || get("failed-at") is not { } failedRaw || !DateTimeOffset.TryParse(failedRaw, CultureInfo.InvariantCulture, DateTimeStyles.RoundtripKind, out var failedAt)
            || get("error-routing-value") is not { } routing || string.IsNullOrWhiteSpace(routing))
            return false;
        envelope = new ErrorEnvelope
        {
            ErrorStream = errorDestination,
            SourceStream = sourceStream,
            SourcePartition = partition,
            SourcePartitionStream = partitionStream,
            SourceOffset = offset,
            Body = body,
            ExceptionType = exceptionType,
            ExceptionMessage = get("exception-message") ?? string.Empty,
            ExceptionStack = get("exception-stack") ?? string.Empty,
            Attempts = attempts,
            FailedAt = failedAt,
            MessageId = get("message-id"),
            CorrelationId = get("correlation-id"),
            RoutingValue = get("routing-value"),
            ErrorRoutingValue = routing,
        };
        return true;
    }
}

internal static class ReplayNaming
{
    public static string ProducerName(string sourceStream, string partitionStream)
        => StreamBusNaming.ReplayProducerName(sourceStream, partitionStream);

    public static string Reference(string errorStream)
        => StreamBusNaming.ReplayReference(errorStream);

    public static ulong PublishingId(ulong sourceOffset) => sourceOffset;
}
