using System.Text;

namespace StreamBus;

internal static class StreamBusLimits
{
    public const int MaxConsumerGroupBytes = 255;
}

internal static class ConsumerGroupValidation
{
    public static string Require(string? consumerGroup, string stream)
    {
        if (string.IsNullOrWhiteSpace(consumerGroup))
            throw new StreamBusConfigurationException($"Consume of stream '{stream}' requires a consumer group. Supply options.ConsumerGroup (raw: ConsumeOptions {{ ConsumerGroup = \"group\" }}; typed: ReceiveEndpoint(e => e.Consumer<TConsumer>(\"group\"))). The library never derives a group name.");
        Validate(consumerGroup);
        return consumerGroup;
    }

    public static void Validate(string consumerGroup)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(consumerGroup);
        if (Encoding.UTF8.GetByteCount(consumerGroup) > StreamBusLimits.MaxConsumerGroupBytes)
            throw new StreamBusConfigurationException(
                $"Consumer group exceeds the broker's 255-byte limit ({Encoding.UTF8.GetByteCount(consumerGroup)} bytes). Supply a shorter group name.");
    }
}
