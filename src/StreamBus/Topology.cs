using System.Globalization;

namespace StreamBus;

/// <summary>
/// Broker rule for placing the leader replica of a stream; applied at
/// declare time only.
/// </summary>
public enum StreamLeaderLocator
{
    /// <summary>Leader goes to the node currently hosting the fewest leaders.</summary>
    LeastLeaders = 0,

    /// <summary>Leader goes to the node the client is connected to, when possible.</summary>
    ClientLocal = 1,

    /// <summary>Leader is placed on a random node.</summary>
    Random = 2,
}

/// <summary>
/// An explicit declaration of a plain stream, passed to
/// <see cref="IStreamTopology.EnsureStreamAsync"/>.
/// </summary>
public sealed record StreamDeclaration
{
    /// <summary>Stream name.</summary>
    public required string Name { get; init; }

    /// <summary>Retention bound after which older segments are truncated;
    /// unset uses the broker default.</summary>
    public TimeSpan? MaxAge { get; init; }

    /// <summary>Total stream size bound in bytes; unset uses the broker default.</summary>
    public ulong? MaxLengthBytes { get; init; }

    /// <summary>Size of a single segment file in bytes; unset uses the
    /// broker default.</summary>
    public int? MaxSegmentSizeBytes { get; init; }

    /// <summary>Leader placement rule applied at declare time.</summary>
    public StreamLeaderLocator LeaderLocator { get; init; } = StreamLeaderLocator.LeastLeaders;
}

/// <summary>
/// An explicit declaration of a super stream, passed to
/// <see cref="IStreamTopology.EnsureSuperStreamAsync"/>.
/// </summary>
public sealed record SuperStreamDeclaration
{
    /// <summary>Super-stream name; partitions are named <c>{name}-{index}</c>.</summary>
    public required string Name { get; init; }

    /// <summary>Number of partitions to create.</summary>
    public required int Partitions { get; init; }

    /// <summary>Retention bound after which older segments are truncated;
    /// unset uses the broker default.</summary>
    public TimeSpan? MaxAge { get; init; }

    /// <summary>Total stream size bound in bytes; unset uses the broker default.</summary>
    public ulong? MaxLengthBytes { get; init; }

    /// <summary>Size of a single segment file in bytes; unset uses the
    /// broker default.</summary>
    public int? MaxSegmentSizeBytes { get; init; }

    /// <summary>Leader placement rule applied at declare time.</summary>
    public StreamLeaderLocator LeaderLocator { get; init; } = StreamLeaderLocator.LeastLeaders;
}

/// <summary>
/// Entry point for idempotent stream and super-stream declarations on one
/// vhost. Startup declarations come from registrations; this API covers
/// runtime, operator-driven declarations.
/// </summary>
public interface IStreamTopology
{
    /// <summary>Vhost the declarations are sent to.</summary>
    string Vhost { get; }

    /// <summary>Declares a plain stream idempotently: creates it when absent,
    /// re-verifies repeated calls from the cache, and throws
    /// <see cref="StreamBusStreamConfigurationConflictException"/> on kind
    /// conflicts. An existing plain stream is adopted with a warning because
    /// its arguments are not readable via the stream protocol.</summary>
    /// <param name="declaration">The stream to declare.</param>
    /// <param name="cancellationToken">Cancellation for the declare operation.</param>
    /// <returns>A task that completes when the declaration is ensured.</returns>
    /// <exception cref="StreamBusConfigurationException">The declaration is invalid.</exception>
    /// <exception cref="StreamBusStreamConfigurationConflictException">The existing broker topology disagrees with the declaration.</exception>
    Task EnsureStreamAsync(StreamDeclaration declaration, CancellationToken cancellationToken = default);

    /// <summary>Declares a super stream with the given number of partitions
    /// idempotently: creates it when absent, re-verifies repeated calls from
    /// the cache, and throws <see cref="StreamBusStreamConfigurationConflictException"/>
    /// on kind conflicts or partition-count mismatches.</summary>
    /// <param name="declaration">The super stream to declare.</param>
    /// <param name="cancellationToken">Cancellation for the declare operation.</param>
    /// <returns>A task that completes when the declaration is ensured.</returns>
    /// <exception cref="StreamBusConfigurationException">The declaration is invalid.</exception>
    /// <exception cref="StreamBusStreamConfigurationConflictException">The existing broker topology disagrees with the declaration.</exception>
    Task EnsureSuperStreamAsync(SuperStreamDeclaration declaration, CancellationToken cancellationToken = default);
}

internal static class TopologyMapping
{
    /// <summary>Builds the wire-level stream spec for a declaration.</summary>
    /// <param name="declaration">Source declaration.</param>
    /// <returns>StreamSpec carrying only the knobs the declaration sets.</returns>
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

    /// <summary>Builds the wire-level super-stream spec for a declaration.</summary>
    /// <param name="declaration">Source declaration.</param>
    /// <returns>StreamSpec carrying only the knobs the declaration sets.</returns>
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

    /// <summary>Maps the library locator enum onto the client's enum.</summary>
    /// <param name="locator">Locator value to map.</param>
    /// <returns>The client-side locator value.</returns>
    /// <exception cref="StreamBusConfigurationException"><paramref name="locator"/> is not a defined enum value.</exception>
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
    /// <summary>Validates a plain-stream declaration (name plus knob limits).</summary>
    /// <param name="declaration">Declaration to validate.</param>
    /// <exception cref="ArgumentNullException"><paramref name="declaration"/> is null.</exception>
    /// <exception cref="ArgumentException">The stream name is null or whitespace.</exception>
    /// <exception cref="StreamBusConfigurationException">A knob value is invalid.</exception>
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

    /// <summary>Validates a super-stream declaration: knob limits plus
    /// <c>Partitions >= 1</c>.</summary>
    /// <param name="declaration">Declaration to validate.</param>
    /// <exception cref="ArgumentNullException"><paramref name="declaration"/> is null.</exception>
    /// <exception cref="ArgumentException">The stream name is null or whitespace.</exception>
    /// <exception cref="StreamBusConfigurationException">A knob value is invalid.</exception>
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
    /// <summary>Human-readable description of a super-stream declaration
    /// for logs and exception messages.</summary>
    /// <param name="declaration">Declaration to describe.</param>
    /// <returns>A one-line description naming the stream, partition count, and knobs.</returns>
    public static string Expected(SuperStreamDeclaration declaration)
        => $"super stream '{declaration.Name}' with {declaration.Partitions} partitions ({Knobs(declaration.MaxAge, declaration.MaxLengthBytes, declaration.MaxSegmentSizeBytes, declaration.LeaderLocator)})";

    /// <summary>Human-readable description of a plain-stream declaration
    /// for logs and exception messages.</summary>
    /// <param name="declaration">Declaration to describe.</param>
    /// <returns>A one-line description naming the stream and its knobs.</returns>
    public static string Expected(StreamDeclaration declaration)
        => $"plain stream '{declaration.Name}' ({Knobs(declaration.MaxAge, declaration.MaxLengthBytes, declaration.MaxSegmentSizeBytes, declaration.LeaderLocator)})";

    private static string Knobs(TimeSpan? maxAge, ulong? maxLengthBytes, int? maxSegmentSizeBytes, StreamLeaderLocator locator)
        => $"max-age={maxAge?.ToString("c", CultureInfo.InvariantCulture) ?? "unset"}, max-length-bytes={maxLengthBytes?.ToString(CultureInfo.InvariantCulture) ?? "unset"}, max-segment-size-bytes={maxSegmentSizeBytes?.ToString(CultureInfo.InvariantCulture) ?? "unset"}, leader-locator={locator}";
}
