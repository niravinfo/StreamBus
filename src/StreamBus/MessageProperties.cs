namespace StreamBus;

public sealed record StreamMessageProperties
{
    public Guid? MessageId { get; init; }
    public Guid? CorrelationId { get; init; }
    public DateTime? SentTime { get; init; }

    internal string? ContentType { get; init; }

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
