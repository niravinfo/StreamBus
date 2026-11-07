using System.Text.Json;

namespace StreamBus;

/// <summary>Serializes published messages and deserializes consumed bodies for one bus.</summary>
public interface IStreamSerializer
{
    /// <summary>Media type written to the AMQP content-type property.</summary>
    string ContentType { get; }

    /// <summary>Serializes <paramref name="message"/> to the bytes published to the stream.</summary>
    /// <typeparam name="T">Message type.</typeparam>
    /// <param name="message">The message to serialize.</param>
    byte[] Serialize<T>(T message);

    /// <summary>Deserializes a delivered body back into <typeparamref name="T"/>.</summary>
    /// <typeparam name="T">Message type.</typeparam>
    /// <param name="body">The delivered body.</param>
    T Deserialize<T>(ReadOnlyMemory<byte> body);
}

/// <summary>
/// System.Text.Json serializer; <see cref="ContentType"/> is
/// <c>application/json</c>. Construct with custom
/// <see cref="JsonSerializerOptions"/> or use <see cref="Default"/>.
/// </summary>
public sealed class SystemTextJsonStreamSerializer : IStreamSerializer
{
    /// <summary>Shared instance with default options.</summary>
    public static readonly SystemTextJsonStreamSerializer Default = new();

    private readonly JsonSerializerOptions _options;

    /// <summary>Creates a serializer over the given options; null uses defaults.</summary>
    public SystemTextJsonStreamSerializer(JsonSerializerOptions? options = null)
        => _options = options ?? new JsonSerializerOptions();

    /// <summary>Media type written to the AMQP content-type property.</summary>
    public string ContentType => "application/json";

    /// <summary>Serializes <paramref name="message"/> as UTF-8 JSON.</summary>
    public byte[] Serialize<T>(T message) => JsonSerializer.SerializeToUtf8Bytes(message, _options);

    /// <summary>Deserializes UTF-8 JSON into <typeparamref name="T"/>; a null result throws <see cref="JsonException"/>.</summary>
    public T Deserialize<T>(ReadOnlyMemory<byte> body)
        => JsonSerializer.Deserialize<T>(body.Span, _options) ?? throw new JsonException($"Deserialization of {typeof(T).FullName} returned null.");
}

/// <summary>Thrown when a delivered body cannot be deserialized as the endpoint's message type.</summary>
public sealed class StreamBusDeserializationException : StreamBusException
{
    /// <summary>Builds the exception with the failed delivery's coordinates.</summary>
    public StreamBusDeserializationException(string stream, int partition, ulong offset, Type messageType, Exception? inner)
        : base($"Could not deserialize the message at {stream} partition {partition} offset {offset} as {messageType.FullName}." + (inner is null ? " The payload deserialized to null." : ""), inner)
    {
        Stream = stream;
        Partition = partition;
        Offset = offset;
        MessageType = messageType;
    }

    /// <summary>Stream the delivery came from.</summary>
    public string Stream { get; }
    /// <summary>Partition the delivery came from.</summary>
    public int Partition { get; }
    /// <summary>Offset of the failed delivery.</summary>
    public ulong Offset { get; }
    /// <summary>Message type the body failed to deserialize as.</summary>
    public Type MessageType { get; }
}
