using System.Text.Json;

namespace StreamBus;

public interface IStreamSerializer
{
    byte[] Serialize<T>(T message);
    T Deserialize<T>(ReadOnlyMemory<byte> body);
}

public sealed class SystemTextJsonStreamSerializer : IStreamSerializer
{
    public static readonly SystemTextJsonStreamSerializer Default = new();

    private readonly JsonSerializerOptions _options;

    public SystemTextJsonStreamSerializer(JsonSerializerOptions? options = null)
        => _options = options ?? new JsonSerializerOptions();

    public byte[] Serialize<T>(T message) => JsonSerializer.SerializeToUtf8Bytes(message, _options);

    public T Deserialize<T>(ReadOnlyMemory<byte> body)
        => JsonSerializer.Deserialize<T>(body.Span) ?? throw new JsonException($"Deserialization of {typeof(T).FullName} returned null.");
}

public sealed class StreamBusDeserializationException : StreamBusException
{
    public StreamBusDeserializationException(string stream, int partition, ulong offset, Type messageType, Exception? inner)
        : base($"Could not deserialize the message at {stream} partition {partition} offset {offset} as {messageType.FullName}." + (inner is null ? " The payload deserialized to null." : ""), inner)
    {
        Stream = stream;
        Partition = partition;
        Offset = offset;
        MessageType = messageType;
    }

    public string Stream { get; }
    public int Partition { get; }
    public ulong Offset { get; }
    public Type MessageType { get; }
}
