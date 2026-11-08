namespace StreamBus;

/// <summary>
/// Broker-side stream specification carried by the registration that
/// references the stream. All members are optional; unset members are
/// filled by other references to the same stream (per-knob agreement
/// enforced at startup) or fall back to declare-time defaults.
/// </summary>
public sealed record StreamOptions
{
    /// <summary>Partition count for a super-stream reference; null on plain
    /// streams. Must be >= 1 when set.</summary>
    public int? Partitions { get; init; }

    /// <summary>Retention bound after which older segments are truncated;
    /// unset uses the broker default at declare time.</summary>
    public TimeSpan? MaxAge { get; init; }

    /// <summary>Total stream size bound in bytes; unset uses the broker
    /// default at declare time.</summary>
    public ulong? MaxLengthBytes { get; init; }

    /// <summary>Size of a single segment file in bytes; unset uses the broker
    /// default at declare time.</summary>
    public int? MaxSegmentSizeBytes { get; init; }

    /// <summary>Leader placement rule; unset uses the broker default at
    /// declare time.</summary>
    public StreamLeaderLocator? LeaderLocator { get; init; }
}

/// <summary>
/// Builder for a plain-stream registration spec. No Partitions member:
/// plain streams have no partitions, so the invalid state is not
/// expressible. Setters return void on purpose — no fluent chaining.
/// </summary>
public interface IStreamOptionsBuilder
{
    /// <summary>Sets the retention bound after which older segments are truncated.</summary>
    /// <param name="maxAge">Maximum age for retained data; must be positive.</param>
    void MaxAge(TimeSpan maxAge);

    /// <summary>Sets the total stream size bound in bytes.</summary>
    /// <param name="maxLengthBytes">Maximum total size; must be >= 1.</param>
    void MaxLengthBytes(ulong maxLengthBytes);

    /// <summary>Sets the size of a single segment file in bytes.</summary>
    /// <param name="maxSegmentSizeBytes">Segment file size; must be >= 1.</param>
    void MaxSegmentSizeBytes(int maxSegmentSizeBytes);

    /// <summary>Sets the leader placement rule applied at declare time.</summary>
    /// <param name="locator">Leader locator to use.</param>
    void LeaderLocator(StreamLeaderLocator locator);
}

/// <summary>
/// Builder for a super-stream registration spec: adds Partitions.
/// </summary>
public interface ISuperStreamOptionsBuilder : IStreamOptionsBuilder
{
    /// <summary>Sets the number of partitions of the super stream.</summary>
    /// <param name="partitions">Partition count; must be >= 1.</param>
    void Partitions(int partitions);
}

internal sealed class StreamOptionsBuilder : ISuperStreamOptionsBuilder
{
    private int? _partitions;
    private TimeSpan? _maxAge;
    private ulong? _maxLengthBytes;
    private int? _maxSegmentSizeBytes;
    private StreamLeaderLocator? _locator;

    /// <summary>Stores the partition count for the builder's next build.</summary>
    public void Partitions(int partitions) => _partitions = partitions;

    /// <summary>Stores the retention bound for the builder's next build.</summary>
    public void MaxAge(TimeSpan maxAge) => _maxAge = maxAge;

    /// <summary>Stores the size bound for the builder's next build.</summary>
    public void MaxLengthBytes(ulong maxLengthBytes) => _maxLengthBytes = maxLengthBytes;

    /// <summary>Stores the segment size for the builder's next build.</summary>
    public void MaxSegmentSizeBytes(int maxSegmentSizeBytes) => _maxSegmentSizeBytes = maxSegmentSizeBytes;

    /// <summary>Stores the leader locator for the builder's next build.</summary>
    public void LeaderLocator(StreamLeaderLocator locator) => _locator = locator;

    internal StreamOptions Build() => new()
    {
        Partitions = _partitions,
        MaxAge = _maxAge,
        MaxLengthBytes = _maxLengthBytes,
        MaxSegmentSizeBytes = _maxSegmentSizeBytes,
        LeaderLocator = _locator,
    };
}

internal static class StreamOptionsValidation
{
    /// <summary>Validates the knob values on a stream spec: rejects
    /// partitions on plain streams, non-positive counts, and undefined
    /// leader locators with <see cref="StreamBusConfigurationException"/>
    /// naming the spec.</summary>
    /// <param name="spec">The spec to validate.</param>
    /// <param name="what">Human-readable name of the spec, used in error messages.</param>
    /// <param name="allowPartitions">Whether the spec may carry partitions (super references only).</param>
    /// <param name="separator">Separator between <paramref name="what"/> and the error message.</param>
    /// <param name="partitionsError">Message used when the partition count is invalid.</param>
    public static void Validate(
        StreamOptions spec,
        string what,
        bool allowPartitions,
        string separator = ": ",
        string partitionsError = "Partitions must be >= 1.")
    {
        ArgumentNullException.ThrowIfNull(spec);
        ArgumentException.ThrowIfNullOrWhiteSpace(what);
        if (spec.Partitions is { } partitions && !allowPartitions)
            throw new StreamBusConfigurationException($"{what}{separator}plain streams have no partitions; use PublishSuper with Partitions.");
        if (spec.Partitions is { } p && p < 1)
            throw new StreamBusConfigurationException($"{what}{separator}{partitionsError}");
        if (spec.MaxAge is { } age && age <= TimeSpan.Zero)
            throw new StreamBusConfigurationException($"{what}{separator}MaxAge must be positive when set.");
        if (spec.MaxLengthBytes is { } length && length < 1)
            throw new StreamBusConfigurationException($"{what}{separator}MaxLengthBytes must be >= 1 when set.");
        if (spec.MaxSegmentSizeBytes is { } segment && segment < 1)
            throw new StreamBusConfigurationException($"{what}{separator}MaxSegmentSizeBytes must be >= 1 when set.");
        if (spec.LeaderLocator is { } locator && !Enum.IsDefined(locator))
            throw new StreamBusConfigurationException($"{what}{separator}LeaderLocator has an undefined value.");
    }
}
