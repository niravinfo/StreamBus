namespace StreamBus;

public sealed record MessageMetadata
{
    public string? MessageId { get; init; }
    public string? CorrelationId { get; init; }
    public IReadOnlyDictionary<string, object?> ApplicationProperties { get; init; }
        = new Dictionary<string, object?>();
    public DateTimeOffset? Timestamp { get; init; }
}

public sealed record ConsumeContext
{
    public required string Stream { get; init; }
    public required int Partition { get; init; }
    public required ulong Offset { get; init; }
    public required string? RoutingValue { get; init; }
    public required ReadOnlyMemory<byte> Body { get; init; }
    public required MessageMetadata Metadata { get; init; }
    public required CancellationToken CancellationToken { get; init; }
}
