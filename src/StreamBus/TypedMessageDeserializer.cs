using System.Collections.Concurrent;
using System.Diagnostics.CodeAnalysis;

namespace StreamBus;

internal interface ITypedMessageReader
{
    object? Deserialize(IStreamSerializer serializer, ReadOnlyMemory<byte> body);
}

internal sealed class TypedMessageReader<T> : ITypedMessageReader
{
    public object? Deserialize(IStreamSerializer serializer, ReadOnlyMemory<byte> body)
        => serializer.Deserialize<T>(body);
}

internal static class TypedMessageDeserializer
{
    private static readonly ConcurrentDictionary<Type, ITypedMessageReader> Readers = new();

    public static object Deserialize(IStreamSerializer serializer, Type messageType, ConsumeContext context)
    {
        ArgumentNullException.ThrowIfNull(serializer);
        ArgumentNullException.ThrowIfNull(messageType);
        ArgumentNullException.ThrowIfNull(context);

        object? message;
        try
        {
            var reader = Readers.GetOrAdd(messageType, Create);
            message = reader.Deserialize(serializer, context.Body);
        }
        catch (OperationCanceledException)
        {
            throw;
        }
        catch (Exception ex)
        {
            throw new StreamBusDeserializationException(context.Stream, context.Partition, context.Offset, messageType, ex);
        }

        if (message is null)
        {
            throw new StreamBusDeserializationException(context.Stream, context.Partition, context.Offset, messageType, null);
        }
        return message;
    }

    [RequiresUnreferencedCode(RuntimeGeneric.TrimMessage)]
    [RequiresDynamicCode(RuntimeGeneric.AotMessage)]
    private static ITypedMessageReader Create(Type messageType)
        => RuntimeGeneric.Create<ITypedMessageReader>(typeof(TypedMessageReader<>), messageType);
}
