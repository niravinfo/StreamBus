namespace StreamBus;

public sealed record StreamMessageProperties
{
    public string? MessageId { get; init; }
    public string? CorrelationId { get; init; }
    public IReadOnlyDictionary<string, object?> Headers { get; init; } = new Dictionary<string, object?>();

    internal static void ValidateValue(string key, object? value)
    {
        if (value is string or bool or int or long or null) return;
        throw new ArgumentException(
            $"Header '{key}' has unsupported type {value?.GetType().Name ?? "null"}; " +
            "legal header values are string, bool, int, long.", nameof(value));
    }

    internal static void ApplyTo(RabbitMQ.Stream.Client.Message message, StreamMessageProperties? properties)
    {
        if (properties is null) return;

        if (properties.MessageId is not null || properties.CorrelationId is not null)
        {
            var messageProperties = message.Properties
                ?? new RabbitMQ.Stream.Client.AMQP.Properties();
            if (properties.MessageId is not null)
                messageProperties.MessageId = properties.MessageId;
            if (properties.CorrelationId is not null)
                messageProperties.CorrelationId = properties.CorrelationId;
            message.Properties = messageProperties;
        }

        if (properties.Headers.Count == 0) return;
        foreach (var (key, value) in properties.Headers)
            ValidateValue(key, value);

        var app = message.ApplicationProperties
            ?? new RabbitMQ.Stream.Client.AMQP.ApplicationProperties();
        foreach (var (key, value) in properties.Headers)
            app[key] = value;
        message.ApplicationProperties = app;
    }
}
