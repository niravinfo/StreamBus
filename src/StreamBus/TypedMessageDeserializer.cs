using System.Collections.Concurrent;
using System.Linq.Expressions;

namespace StreamBus;

internal static class TypedMessageDeserializer
{
    private static readonly ConcurrentDictionary<Type, Func<IStreamSerializer, ReadOnlyMemory<byte>, object>> Deserializers = new();

    public static object Deserialize(IStreamSerializer serializer, Type messageType, ConsumeContext context)
    {
        ArgumentNullException.ThrowIfNull(serializer);
        ArgumentNullException.ThrowIfNull(messageType);
        ArgumentNullException.ThrowIfNull(context);

        object? message;
        try
        {
            message = Deserializers.GetOrAdd(messageType, Create)(serializer, context.Body);
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

    private static Func<IStreamSerializer, ReadOnlyMemory<byte>, object> Create(Type messageType)
    {
        var closed = typeof(IStreamSerializer)
            .GetMethod(nameof(IStreamSerializer.Deserialize))!
            .MakeGenericMethod(messageType);
        var serializer = Expression.Parameter(typeof(IStreamSerializer), "serializer");
        var body = Expression.Parameter(typeof(ReadOnlyMemory<byte>), "body");
        var call = Expression.Call(serializer, closed, body);
        var boxed = Expression.Convert(call, typeof(object));
        return Expression.Lambda<Func<IStreamSerializer, ReadOnlyMemory<byte>, object>>(boxed, serializer, body).Compile();
    }
}
