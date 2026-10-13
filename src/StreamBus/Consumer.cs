namespace StreamBus;

public interface IConsumer<T>
{
    Task Consume(ConsumeContext<T> context, CancellationToken cancellationToken);
}

public sealed record ConsumeContext<T>
{
    public required T Message { get; init; }
    public required string Stream { get; init; }
    public required int Partition { get; init; }
    public required ulong Offset { get; init; }
    public required string? RoutingValue { get; init; }
    public required ReadOnlyMemory<byte> Body { get; init; }
    public required MessageMetadata Metadata { get; init; }
    public required CancellationToken CancellationToken { get; init; }
}
