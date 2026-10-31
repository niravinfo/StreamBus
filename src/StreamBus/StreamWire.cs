using RabbitMQ.Stream.Client.AMQP;

namespace StreamBus;

internal static class StreamWire
{
    /// <summary>
    /// Decodes an AMQP <c>message-id</c>/<c>correlation-id</c>: a Guid string or a
    /// 16-byte native AMQP UUID. Anything else reads as null, never throws.
    /// </summary>
    internal static Guid? ReadGuid(object? value) => value switch
    {
        string text when Guid.TryParse(text, out var guid) => guid,
        byte[] { Length: 16 } uuid => FromAmqpUuid(uuid),
        _ => null,
    };

    internal static DateTime? ReadSentTime(Properties? properties)
    {
        if (properties is null || properties.CreationTime == default)
            return null;

        var value = properties.CreationTime;
        return value.Kind == DateTimeKind.Local
            ? value.ToUniversalTime()
            : DateTime.SpecifyKind(value, DateTimeKind.Utc);
    }

    internal static DateTime NormalizeSentTime(DateTime value)
    {
        var utc = value.Kind switch
        {
            DateTimeKind.Local => value.ToUniversalTime(),
            DateTimeKind.Unspecified => DateTime.SpecifyKind(value, DateTimeKind.Utc),
            _ => value,
        };
        return DateTimeOffset.FromUnixTimeMilliseconds(
            new DateTimeOffset(utc).ToUnixTimeMilliseconds()).UtcDateTime;
    }

    // AMQP 1.0 UUID is network byte order; .NET Guid is little-endian in its
    // first three groups, so the native form must be converted, not copied.
    private static Guid FromAmqpUuid(byte[] b) => new(
        (b[0] << 24) | (b[1] << 16) | (b[2] << 8) | b[3],
        (short)((b[4] << 8) | b[5]),
        (short)((b[6] << 8) | b[7]),
        b[8], b[9], b[10], b[11], b[12], b[13], b[14], b[15]);
}
