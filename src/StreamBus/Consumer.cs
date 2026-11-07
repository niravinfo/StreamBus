namespace StreamBus;

/// <summary>
/// A typed consumer bound to exactly one message type by a receive
/// endpoint. One DI scope is created per delivered message and shared by
/// the pipeline and the consumer constructor.
/// </summary>
/// <typeparam name="T">The endpoint message type.</typeparam>
public interface IConsumer<T>
{
    /// <summary>Handles one delivery of <typeparamref name="T"/>. Throwing
    /// triggers the endpoint's retry policy; the offset advances only when
    /// the handler succeeds or the failure is handed off.</summary>
    /// <param name="context">The delivery with its deserialized message.</param>
    /// <returns>A task that completes when the message is handled.</returns>
    Task Consume(ConsumeContext<T> context);
}
