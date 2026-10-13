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

    private const string P = "streambus.error.";

    public Dictionary<string, object?> ToApplicationProperties() => new()
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

    public static bool TryParse(string errorStream, byte[] body, IReadOnlyDictionary<string, object?> app, out ErrorEnvelope? envelope)
    {
        envelope = null;
        if (string.IsNullOrWhiteSpace(errorStream) || body is null || app is null) return false;
        string? Get(string name) => app.TryGetValue(P + name, out var v) ? v?.ToString() : null;
        var sourceStream = Get("source-stream");
        var partitionStream = Get("source-partition-stream");
        var exceptionType = Get("exception-type");
        if (string.IsNullOrWhiteSpace(sourceStream)
            || string.IsNullOrWhiteSpace(partitionStream)
            || string.IsNullOrWhiteSpace(exceptionType)
            || Get("source-partition") is not { } partRaw || !int.TryParse(partRaw, NumberStyles.Integer, CultureInfo.InvariantCulture, out var partition)
            || Get("source-offset") is not { } offsetRaw || !ulong.TryParse(offsetRaw, NumberStyles.Integer, CultureInfo.InvariantCulture, out var offset)
            || Get("attempts") is not { } attemptsRaw || !int.TryParse(attemptsRaw, NumberStyles.Integer, CultureInfo.InvariantCulture, out var attempts)
            || Get("failed-at") is not { } failedRaw || !DateTimeOffset.TryParse(failedRaw, CultureInfo.InvariantCulture, DateTimeStyles.RoundtripKind, out var failedAt)
            || Get("error-routing-value") is not { } routing || string.IsNullOrWhiteSpace(routing))
            return false;
        envelope = new ErrorEnvelope
        {
            ErrorStream = errorStream,
            SourceStream = sourceStream,
            SourcePartition = partition,
            SourcePartitionStream = partitionStream,
            SourceOffset = offset,
            Body = body,
            ExceptionType = exceptionType,
            ExceptionMessage = Get("exception-message") ?? string.Empty,
            ExceptionStack = Get("exception-stack") ?? string.Empty,
            Attempts = attempts,
            FailedAt = failedAt,
            MessageId = Get("message-id"),
            CorrelationId = Get("correlation-id"),
            RoutingValue = Get("routing-value"),
            ErrorRoutingValue = routing,
        };
        return true;
    }
}

internal static class ReplayNaming
{
    public static string ProducerName(string partitionStream)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(partitionStream);
        var hash = Convert.ToHexString(System.Security.Cryptography.SHA256.HashData(
            System.Text.Encoding.UTF8.GetBytes(partitionStream)))[..32];
        return $"streambus-replay-{hash}";
    }

    public static string Reference(string errorStream)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(errorStream);
        var hash = Convert.ToHexString(System.Security.Cryptography.SHA256.HashData(
            System.Text.Encoding.UTF8.GetBytes(errorStream)))[..32];
        return $"streambus-replay-{hash}";
    }

    public static ulong PublishingId(ulong sourceOffset) => sourceOffset;
}
