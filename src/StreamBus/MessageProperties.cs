namespace StreamBus;

/// <summary>
/// AMQP message properties applied to a publish. Set on
/// <see cref="PublishOptions.Properties"/> to control identity, timing, and
/// application headers on the wire.
/// </summary>
/// <remarks>
/// <para>
/// Values map to AMQP message properties: <see cref="MessageId"/>,
/// <see cref="CorrelationId"/>, and <see cref="SentTime"/> become the AMQP
/// <c>message-id</c>, <c>correlation-id</c>, and <c>creation-time</c>. The
/// media type is not set here; StreamBus stamps it from the configured
/// serializer.
/// </para>
/// <para>
/// <see cref="Headers"/> are written as AMQP application properties, which
/// restricts values to <see cref="string"/>, <see cref="bool"/>,
/// <see cref="int"/>, and <see cref="long"/> (or <see langword="null"/>) —
/// other types throw <see cref="ArgumentException"/> at publish time. Keys
/// colliding with reserved <c>streambus.*</c> framework keys are rejected.
/// When a publish filter also writes a header, the filter wins over values
/// supplied here.
/// </para>
/// </remarks>
public sealed record StreamMessageProperties
{
    /// <summary>AMQP <c>message-id</c>; null leaves it unset on the wire.</summary>
    public Guid? MessageId { get; init; }

    /// <summary>AMQP <c>correlation-id</c>; never invented by StreamBus.</summary>
    public Guid? CorrelationId { get; init; }

    /// <summary>AMQP <c>creation-time</c>; normalized to UTC, millisecond precision.</summary>
    public DateTime? SentTime { get; init; }

    internal string? ContentType { get; init; }

    /// <summary>Application properties written as AMQP headers on the message.
    /// Values must be string, bool, int, long, or null; keys must not collide
    /// with reserved <c>streambus.*</c> framework keys.</summary>
    public IReadOnlyDictionary<string, object?> Headers { get; init; } = new Dictionary<string, object?>();

    internal static void ValidateValue(string key, object? value)
    {
        if (value is string or bool or int or long or null) return;
        throw new ArgumentException(
            $"Header '{key}' has unsupported type {value?.GetType().Name ?? "null"}; " +
            "legal header values are string, bool, int, long.", nameof(value));
    }

    internal static void ValidateHeaders(IReadOnlyDictionary<string, object?>? headers)
    {
        if (headers is null) return;
        foreach (var (key, value) in headers)
        {
            StreamHeaderKeys.EnsureNotReserved(key);
            ValidateValue(key, value);
        }
    }

    internal static void ApplyTo(
        RabbitMQ.Stream.Client.Message message,
        StreamMessageProperties? properties,
        string? routingKey = null)
    {
        if (properties is not null)
        {
            ValidateHeaders(properties.Headers);

            if (properties.MessageId is not null || properties.CorrelationId is not null
                || properties.SentTime is not null || properties.ContentType is not null)
            {
                var messageProperties = message.Properties
                    ?? new RabbitMQ.Stream.Client.AMQP.Properties();
                if (properties.MessageId is { } messageId)
                    messageProperties.MessageId = messageId.ToString("D");
                if (properties.CorrelationId is { } correlationId)
                    messageProperties.CorrelationId = correlationId.ToString("D");
                if (properties.SentTime is { } sentTime)
                    messageProperties.CreationTime = StreamWire.NormalizeSentTime(sentTime);
                if (properties.ContentType is { } contentType)
                    messageProperties.ContentType = contentType;
                message.Properties = messageProperties;
            }

            if (properties.Headers.Count > 0 || routingKey is not null)
            {
                var applicationProperties = new RabbitMQ.Stream.Client.AMQP.ApplicationProperties();
                foreach (var (key, value) in properties.Headers)
                    applicationProperties[key] = value;
                // Framework value is written after user headers so it always wins.
                if (routingKey is not null)
                    applicationProperties[StreamHeaderKeys.RoutingKey] = routingKey;
                message.ApplicationProperties = applicationProperties;
            }
        }
        else if (routingKey is not null)
        {
            message.ApplicationProperties =
                new RabbitMQ.Stream.Client.AMQP.ApplicationProperties
                {
                    [StreamHeaderKeys.RoutingKey] = routingKey,
                };
        }
    }
}
