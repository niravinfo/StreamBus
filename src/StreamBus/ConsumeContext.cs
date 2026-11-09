namespace StreamBus;

/// <summary>
/// One delivery from a stream partition, shared by the handler and every
/// consume filter.
/// </summary>
public class ConsumeContext
{
    /// <summary>
    /// Construction is pinned to library code (and test assemblies via
    /// <see langword="InternalsVisibleTo"/>): filters receive and enrich a
    /// context built by StreamBus; they never construct their own.
    /// </summary>
    protected internal ConsumeContext() { }

    /// <summary>Source stream name.</summary>
    public required string Stream { get; init; }

    /// <summary>Source partition index.</summary>
    public required int Partition { get; init; }

    /// <summary>Source offset for this delivery.</summary>
    public required ulong Offset { get; init; }

    /// <summary>Routing value selected by the publisher; null for a plain stream.</summary>
    public required string? RoutingKey { get; init; }

    /// <summary>Serialized message body.</summary>
    public required ReadOnlyMemory<byte> Body { get; init; }

    /// <summary>AMQP <c>message-id</c>; null when absent or not a Guid.</summary>
    public Guid? MessageId { get; init; }

    /// <summary>AMQP <c>correlation-id</c>; never invented by StreamBus.</summary>
    public Guid? CorrelationId { get; init; }

    /// <summary>AMQP <c>creation-time</c>, UTC and millisecond precision.</summary>
    public DateTime? SentTime { get; init; }

    /// <summary>AMQP <c>content-type</c> as received; copied into the error
    /// envelope so the destination carries the original media type.</summary>
    public string? ContentType { get; init; }

    /// <summary>Application properties as received, framework keys included.</summary>
    public required IStreamHeaders Headers { get; init; }

    /// <summary>Headers carried on the error envelope.</summary>
    public required IStreamSendHeaders SendHeaders { get; init; }

    /// <summary>
    /// Per-delivery service scope shared with the typed consumer. Use
    /// <see cref="IServiceProvider.GetService"/> — preferably the
    /// <c>GetRequiredService</c> extension — for services the consumer
    /// requires. On filterless, untyped endpoints (or a bus built without
    /// dependency injection) no scope is created and this provider resolves
    /// nothing: it returns <see langword="null"/> for every type, so treat a
    /// <see langword="null"/> here as "no DI" rather than "service not registered".
    /// </summary>
    public required IServiceProvider Services { get; init; }

    /// <summary>Cancellation token for this delivery.</summary>
    public required CancellationToken CancellationToken { get; init; }

    private readonly Dictionary<Type, object?> _payloads = new();

    /// <summary>Gets a value stored by an earlier filter.</summary>
    /// <typeparam name="T">Type used as the storage key.</typeparam>
    /// <returns>The stored value, or the default value when absent.</returns>
    public T? GetPayload<T>()
        => _payloads.TryGetValue(typeof(T), out var value) && value is T typed
            ? typed
            : default;

    /// <summary>Gets a value stored by an earlier filter.</summary>
    /// <typeparam name="T">Type used as the storage key.</typeparam>
    /// <param name="payload">The stored value when present and of type <typeparamref name="T"/>.</param>
    /// <returns><see langword="true"/> when the value is present and of type <typeparamref name="T"/>.</returns>
    public bool TryGetPayload<T>(out T? payload)
    {
        if (_payloads.TryGetValue(typeof(T), out var value) && value is T typed)
        {
            payload = typed;
            return true;
        }

        payload = default;
        return false;
    }

    /// <summary>Stores a value for later filters in this pipeline.</summary>
    /// <typeparam name="T">Type used as the storage key.</typeparam>
    /// <param name="value">The value to store.</param>
    public void SetPayload<T>(T value) => _payloads[typeof(T)] = value;

    internal void ResetPayloads() => _payloads.Clear();
}

/// <summary>
/// One delivery with its deserialized message, shared by the typed consumer and
/// every consume filter for <typeparamref name="T"/>.
/// </summary>
/// <typeparam name="T">The endpoint message type.</typeparam>
public sealed class ConsumeContext<T> : ConsumeContext
{
    /// <summary>The deserialized endpoint message.</summary>
    public required T Message { get; init; }
}
