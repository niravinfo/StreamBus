using System.Text;

namespace StreamBus;

internal static class StreamBusLimits
{
    public const int MaxReferenceBytes = 255;
}

internal static class ConsumerReferenceValidation
{
    public static string Require(string? reference, string stream)
    {
        if (string.IsNullOrWhiteSpace(reference))
            throw new StreamBusConfigurationException($"Consume of stream '{stream}' requires a consumer group Reference. Supply options.Reference (raw: ConsumeOptions {{ Reference = \"group\" }}; typed: ReceiveEndpoint(e => e.Consumer<TConsumer>(\"group\"))). The library never derives a group name.");
        Validate(reference);
        return reference;
    }

    public static void Validate(string reference)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(reference);
        if (Encoding.UTF8.GetByteCount(reference) > StreamBusLimits.MaxReferenceBytes)
            throw new StreamBusConfigurationException(
                $"Consumer Reference exceeds the broker's 255-byte limit ({Encoding.UTF8.GetByteCount(reference)} bytes). Supply a shorter stable reference.");
    }
}
