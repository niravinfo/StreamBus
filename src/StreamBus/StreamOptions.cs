namespace StreamBus;

/// <summary>
/// Broker-side stream specification carried by the registration that
/// references the stream. All members are optional; unset members are
/// filled by other references to the same stream (per-knob agreement
/// enforced at startup) or fall back to declare-time defaults.
/// </summary>
public sealed record StreamOptions
{
    public int? Partitions { get; init; }
    public TimeSpan? MaxAge { get; init; }
    public ulong? MaxLengthBytes { get; init; }
    public int? MaxSegmentSizeBytes { get; init; }
    public StreamLeaderLocator? LeaderLocator { get; init; }
}

/// <summary>
/// Builder for a plain-stream registration spec. No Partitions member:
/// plain streams have no partitions, so the invalid state is not
/// expressible. Setters return void on purpose — no fluent chaining.
/// </summary>
public interface IStreamOptionsBuilder
{
    void MaxAge(TimeSpan maxAge);
    void MaxLengthBytes(ulong maxLengthBytes);
    void MaxSegmentSizeBytes(int maxSegmentSizeBytes);
    void LeaderLocator(StreamLeaderLocator locator);
}

/// <summary>
/// Builder for a super-stream registration spec: adds Partitions.
/// </summary>
public interface ISuperStreamOptionsBuilder : IStreamOptionsBuilder
{
    void Partitions(int partitions);
}

internal sealed class StreamOptionsBuilder : ISuperStreamOptionsBuilder
{
    private int? _partitions;
    private TimeSpan? _maxAge;
    private ulong? _maxLengthBytes;
    private int? _maxSegmentSizeBytes;
    private StreamLeaderLocator? _locator;

    public void Partitions(int partitions) => _partitions = partitions;

    public void MaxAge(TimeSpan maxAge) => _maxAge = maxAge;

    public void MaxLengthBytes(ulong maxLengthBytes) => _maxLengthBytes = maxLengthBytes;

    public void MaxSegmentSizeBytes(int maxSegmentSizeBytes) => _maxSegmentSizeBytes = maxSegmentSizeBytes;

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
