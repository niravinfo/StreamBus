using System.Globalization;

namespace StreamBus;

/// <summary>
/// The envelope of one failed delivery as it travels to an error destination:
/// the original body plus the failure metadata carried as
/// <c>streambus.error.*</c> application properties, so the destination
/// alone explains what happened. Consuming or draining the destination
/// afterwards is the application's job.
/// </summary>
public sealed record ErrorEnvelope
{
    /// <summary>Error destination the failed delivery was routed to: an
    /// error stream name, or an AMQP queue name.</summary>
    public required string ErrorStream { get; init; }

    /// <summary>Logical source stream the delivery failed on; the
    /// super-stream name when the source is partitioned.</summary>
    public required string SourceStream { get; init; }

    /// <summary>Source partition index; 0 for a plain stream.</summary>
    public required int SourcePartition { get; init; }

    /// <summary>Physical partition stream the delivery failed on; equals
    /// <see cref="SourceStream"/> for a plain stream.</summary>
    public required string SourcePartitionStream { get; init; }

    /// <summary>Offset of the failed delivery within its source partition stream.</summary>
    public required ulong SourceOffset { get; init; }

    /// <summary>Body of the failed delivery, as received.</summary>
    public required byte[] Body { get; init; }

    /// <summary>Type name of the exception that exhausted the retries.</summary>
    public required string ExceptionType { get; init; }

    /// <summary>Message of the exception that exhausted the retries.</summary>
    public required string ExceptionMessage { get; init; }

    /// <summary>Stack trace of the exception that exhausted the retries.</summary>
    public required string ExceptionStack { get; init; }

    /// <summary>Total attempts made, counting the first delivery.</summary>
    public required int Attempts { get; init; }

    /// <summary>When the delivery exhausted its retries (UTC).</summary>
    public required DateTimeOffset FailedAt { get; init; }

    /// <summary>AMQP <c>message-id</c> of the failed delivery; null when
    /// absent or not a Guid.</summary>
    public required Guid? MessageId { get; init; }

    /// <summary>AMQP <c>correlation-id</c> of the failed delivery; null when
    /// absent.</summary>
    public required Guid? CorrelationId { get; init; }

    /// <summary>AMQP <c>creation-time</c> of the failed delivery, UTC; null
    /// when absent.</summary>
    public DateTime? SentTime { get; init; }

    /// <summary>Routing value of the original super-stream publish; null for
    /// a plain stream.</summary>
    public required string? RoutingKey { get; init; }

    /// <summary>Content type of the failed delivery, as it arrived from the
    /// broker's AMQP <c>content-type</c> property.</summary>
    public string? ContentType { get; init; }

    /// <summary>Original user headers of the failed delivery; framework keys
    /// (<c>streambus.*</c>) and values with unsupported types are excluded.</summary>
    public IReadOnlyDictionary<string, object?>? UserHeaders { get; init; }

    /// <summary>Headers added by the consume pipeline for the failed delivery.
    /// They are forwarded on top of the envelope's own fields; envelope fields
    /// win when a key collides.</summary>
    public IReadOnlyDictionary<string, object?>? SendHeaders { get; init; }

    private const string P = "streambus.error.";

    /// <summary>Serializes the envelope's own metadata as the flat
    /// <c>streambus.error.*</c> application-property map that travels with
    /// the failed body.</summary>
    /// <returns>The envelope metadata keyed by <c>streambus.error.*</c>.</returns>
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
        [P + "message-id"] = MessageId?.ToString("D"),
        [P + "correlation-id"] = CorrelationId?.ToString("D"),
        [P + "sent-time"] = SentTime?.ToString("o", CultureInfo.InvariantCulture),
        [P + "routing-value"] = RoutingKey,
        [P + "content-type"] = ContentType,
    };

    /// <summary>
    /// Flattens the envelope into the application properties for the
    /// error-destination publish: the <c>streambus.error.*</c> metadata,
    /// with the consume-pipeline <see cref="SendHeaders"/> and then the
    /// original <see cref="UserHeaders"/> merged on top as plain keys.
    /// Envelope fields win key collisions.
    /// </summary>
    /// <returns>The application-property map to attach to the error publish.</returns>
    public Dictionary<string, object?> ToApplicationProperties()
    {
        var properties = EnvelopeApplicationProperties();

        // The original user headers and the consume-pipeline
        // output travel on the wire as plain keys next to the error metadata.
        // Envelope fields win collisions; consume-filter output (merged before
        // the original user headers) wins over the original user value.
        if (SendHeaders is not null)
        {
            foreach (var (key, value) in SendHeaders)
            {
                if (!properties.ContainsKey(key))
                    properties[key] = value;
            }
        }

        if (UserHeaders is not null)
        {
            foreach (var (key, value) in UserHeaders)
            {
                if (!properties.ContainsKey(key))
                    properties[key] = value;
            }
        }

        return properties;
    }

    /// <summary>
    /// Captures the original user headers of a delivery for the error
    /// envelope: framework keys (any <c>streambus.*</c> key, which covers the
    /// envelope's own fields), null values (AMQP application properties
    /// cannot carry them; a null header is treated as absent), and values a
    /// republish could not legally carry (anything but string, bool, int,
    /// long) are dropped. Returns null when the delivery carried no user
    /// headers.
    /// </summary>
    internal static IReadOnlyDictionary<string, object?>? CaptureUserHeaders(IStreamHeaders? headers)
    {
        if (headers is null) return null;
        Dictionary<string, object?>? captured = null;

        foreach (var (key, value) in headers.All)
        {
            if (key.StartsWith("streambus.", StringComparison.Ordinal)) continue;
            if (value is null) continue;
            if (value is not (string or bool or int or long)) continue;
            captured ??= new Dictionary<string, object?>(StringComparer.Ordinal);
            captured[key] = value;
        }

        return captured;
    }

    // Flat reconstruction: user headers are the plain
    // non-framework keys that traveled next to the error metadata, so AMQP's
    // own field-table typing (string/bool/int/long) round-trips the values
    // with no encoding layer.
    private static Dictionary<string, object?>? CaptureWireUserHeaders(IReadOnlyDictionary<string, object?> raw)
    {
        Dictionary<string, object?>? captured = null;
        foreach (var (key, value) in raw)
        {
            if (key.StartsWith("streambus.", StringComparison.Ordinal)) continue;
            if (value is null) continue;
            if (value is not (string or bool or int or long or byte[])) continue;
            captured ??= new Dictionary<string, object?>(StringComparer.Ordinal);
            captured[key] = value is byte[] bytes ? System.Text.Encoding.UTF8.GetString(bytes) : value;
        }

        return captured;
    }

    /// <summary>
    /// Flattens the envelope into a header map: the same content as
    /// <see cref="ToApplicationProperties"/> minus entries with null values.
    /// </summary>
    /// <returns>The header map describing this failure.</returns>
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

    /// <summary>Attempts to read an envelope from a delivery that carried the
    /// failure metadata as application properties (error-stream hand-off).</summary>
    /// <param name="errorStream">Error stream the delivery was read from.</param>
    /// <param name="body">Raw body of the failed delivery.</param>
    /// <param name="app">Application properties as received.</param>
    /// <param name="envelope">The parsed envelope, or <see langword="null"/> when parsing fails.</param>
    /// <returns><see langword="true"/> when the required metadata is present and well-formed.</returns>
    public static bool TryParse(string errorStream, byte[] body, IReadOnlyDictionary<string, object?> app, out ErrorEnvelope? envelope)
    {
        envelope = null;
        if (string.IsNullOrWhiteSpace(errorStream) || body is null || app is null) return false;
        string? Get(string name) => app.TryGetValue(P + name, out var v) ? v?.ToString() : null;
        return TryParseCore(errorStream, body, app, Get, out envelope);
    }

    /// <summary>Attempts to read an envelope from a delivery that carried the
    /// failure metadata as AMQP message headers (queue hand-off).</summary>
    /// <param name="errorQueue">Error queue the delivery was read from.</param>
    /// <param name="body">Raw body of the failed delivery.</param>
    /// <param name="headers">Message headers as received.</param>
    /// <param name="envelope">The parsed envelope, or <see langword="null"/> when parsing fails.</param>
    /// <returns><see langword="true"/> when the required metadata is present and well-formed.</returns>
    public static bool TryParseHeaders(string errorQueue, byte[] body, IReadOnlyDictionary<string, object?> headers, out ErrorEnvelope? envelope)
    {
        envelope = null;
        if (string.IsNullOrWhiteSpace(errorQueue) || body is null || headers is null) return false;
        string? Get(string name)
        {
            if (!headers.TryGetValue(P + name, out var v) || v is null) return null;
            return v is byte[] bytes ? System.Text.Encoding.UTF8.GetString(bytes) : v.ToString();
        }
        return TryParseCore(errorQueue, body, headers, Get, out envelope);
    }

    private static bool TryParseCore(
        string errorDestination, byte[] body, IReadOnlyDictionary<string, object?> raw,
        Func<string, string?> get, out ErrorEnvelope? envelope)
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
            || get("failed-at") is not { } failedRaw || !DateTimeOffset.TryParse(failedRaw, CultureInfo.InvariantCulture, DateTimeStyles.RoundtripKind, out var failedAt))
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
            MessageId = ParseGuid(get("message-id")),
            CorrelationId = ParseGuid(get("correlation-id")),
            SentTime = ParseSentTime(get("sent-time")),
            RoutingKey = get("routing-value"),
            ContentType = get("content-type"),
            UserHeaders = CaptureWireUserHeaders(raw),
        };

        return true;
    }

    private static Guid? ParseGuid(string? raw)
        => Guid.TryParse(raw, out var value) ? value : null;

    private static DateTime? ParseSentTime(string? raw)
    {
        if (!DateTime.TryParse(raw, CultureInfo.InvariantCulture, DateTimeStyles.RoundtripKind, out var value))
            return null;
        return value.Kind == DateTimeKind.Unspecified
            ? DateTime.SpecifyKind(value, DateTimeKind.Utc)
            : value.ToUniversalTime();
    }
}

