namespace StreamBus;

public enum StreamLeaderLocator
{
    LeastLeaders = 0,
    ClientLocal = 1,
    Random = 2,
}

public sealed record StreamDeclaration
{
    public required string Name { get; init; }
    public TimeSpan? MaxAge { get; init; }
    public ulong? MaxLengthBytes { get; init; }
    public int? MaxSegmentSizeBytes { get; init; }
    public StreamLeaderLocator LeaderLocator { get; init; } = StreamLeaderLocator.LeastLeaders;
}

public sealed record SuperStreamDeclaration
{
    public required string Name { get; init; }
    public required int Partitions { get; init; }
    public TimeSpan? MaxAge { get; init; }
    public ulong? MaxLengthBytes { get; init; }
    public int? MaxSegmentSizeBytes { get; init; }
    public StreamLeaderLocator LeaderLocator { get; init; } = StreamLeaderLocator.LeastLeaders;
}

public interface IStreamTopology
{
    string Vhost { get; }
    Task EnsureStreamAsync(StreamDeclaration declaration, CancellationToken ct = default);
    Task EnsureSuperStreamAsync(SuperStreamDeclaration declaration, CancellationToken ct = default);
}

internal static class TopologyMapping
{
    public static RabbitMQ.Stream.Client.StreamSpec ToStreamSpec(StreamDeclaration declaration)
    {
        ArgumentNullException.ThrowIfNull(declaration);
        var spec = new RabbitMQ.Stream.Client.StreamSpec(declaration.Name);
        if (declaration.MaxAge is { } age) spec.MaxAge = age;
        if (declaration.MaxLengthBytes is { } length) spec.MaxLengthBytes = length;
        if (declaration.MaxSegmentSizeBytes is { } segment) spec.MaxSegmentSizeBytes = segment;
        spec.LeaderLocator = Map(declaration.LeaderLocator);
        return spec;
    }

    public static RabbitMQ.Stream.Client.PartitionsSuperStreamSpec ToSuperStreamSpec(SuperStreamDeclaration declaration)
    {
        ArgumentNullException.ThrowIfNull(declaration);
        var spec = new RabbitMQ.Stream.Client.PartitionsSuperStreamSpec(declaration.Name, declaration.Partitions);
        if (declaration.MaxAge is { } age) spec.MaxAge = age;
        if (declaration.MaxLengthBytes is { } length) spec.MaxLengthBytes = length;
        if (declaration.MaxSegmentSizeBytes is { } segment) spec.MaxSegmentSizeBytes = segment;
        spec.LeaderLocator = Map(declaration.LeaderLocator);
        return spec;
    }

    public static RabbitMQ.Stream.Client.LeaderLocator Map(StreamLeaderLocator locator) => locator switch
    {
        StreamLeaderLocator.LeastLeaders => RabbitMQ.Stream.Client.LeaderLocator.LeastLeaders,
        StreamLeaderLocator.ClientLocal => RabbitMQ.Stream.Client.LeaderLocator.ClientLocal,
        StreamLeaderLocator.Random => RabbitMQ.Stream.Client.LeaderLocator.Random,
        _ => throw new StreamBusConfigurationException($"Unknown stream leader locator {locator}."),
    };
}

internal static class TopologyValidation
{
    public static void ValidateStream(StreamDeclaration declaration)
    {
        ArgumentNullException.ThrowIfNull(declaration);
        ArgumentException.ThrowIfNullOrWhiteSpace(declaration.Name);
        StreamOptionsValidation.Validate(
            new StreamOptions
            {
                MaxAge = declaration.MaxAge,
                MaxLengthBytes = declaration.MaxLengthBytes,
                MaxSegmentSizeBytes = declaration.MaxSegmentSizeBytes,
                LeaderLocator = declaration.LeaderLocator,
            },
            $"StreamDeclaration '{declaration.Name}'",
            allowPartitions: false,
            separator: " ");
    }

    public static void ValidateSuper(SuperStreamDeclaration declaration)
    {
        ArgumentNullException.ThrowIfNull(declaration);
        ArgumentException.ThrowIfNullOrWhiteSpace(declaration.Name);
        StreamOptionsValidation.Validate(
            new StreamOptions
            {
                Partitions = declaration.Partitions,
                MaxAge = declaration.MaxAge,
                MaxLengthBytes = declaration.MaxLengthBytes,
                MaxSegmentSizeBytes = declaration.MaxSegmentSizeBytes,
                LeaderLocator = declaration.LeaderLocator,
            },
            $"SuperStreamDeclaration '{declaration.Name}'",
            allowPartitions: true,
            separator: " ");
    }
}

internal static class TopologyDescribe
{
    public static string Expected(SuperStreamDeclaration declaration)
        => $"super stream '{declaration.Name}' with {declaration.Partitions} partitions ({Knobs(declaration.MaxAge, declaration.MaxLengthBytes, declaration.MaxSegmentSizeBytes, declaration.LeaderLocator)})";

    public static string Expected(StreamDeclaration declaration)
        => $"plain stream '{declaration.Name}' ({Knobs(declaration.MaxAge, declaration.MaxLengthBytes, declaration.MaxSegmentSizeBytes, declaration.LeaderLocator)})";

    private static string Knobs(TimeSpan? maxAge, ulong? maxLengthBytes, int? maxSegmentSizeBytes, StreamLeaderLocator locator)
        => $"max-age={maxAge?.ToString() ?? "unset"}, max-length-bytes={maxLengthBytes?.ToString() ?? "unset"}, max-segment-size-bytes={maxSegmentSizeBytes?.ToString() ?? "unset"}, leader-locator={locator}";
}
