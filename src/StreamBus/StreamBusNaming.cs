using System.Security.Cryptography;
using System.Text;

namespace StreamBus;

/// <summary>
/// The single producer-name/hash utility. Every stable broker name is bounded
/// by <see cref="MaxName"/> UTF-8 bytes; the 255-byte rule is pinned by tests.
/// </summary>
internal static class StreamBusNaming
{
    public const int MaxName = StreamBusLimits.MaxReferenceBytes;

    /// <summary>Unique-per-handle publish reference (broker dedup safety).</summary>
    public static string PublishReference(string streamName, long sequence)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(streamName);
        var full = $"streambus-pub-{streamName}-{sequence}";
        return Encoding.UTF8.GetByteCount(full) <= MaxName
            ? full
            : $"streambus-pub-{Sha32(full)}";
    }

    /// <summary>
    /// Stable publisher reference for explicit-dedup producers (replay).
    /// Task 8 preserves the pre-consolidation recipe byte-for-byte; the source
    /// stream becomes part of the name when replay naming is scoped per source.
    /// </summary>
    public static string ReplayProducerName(string sourceStream, string sourcePartitionStream)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(sourceStream);
        ArgumentException.ThrowIfNullOrWhiteSpace(sourcePartitionStream);
        return $"streambus-replay-{Sha32(sourcePartitionStream)}";
    }

    /// <summary>Stable consumer reference for a replay run over one error stream.</summary>
    public static string ReplayReference(string errorStream)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(errorStream);
        return $"streambus-replay-{Sha32(errorStream)}";
    }

    /// <summary>Effectively-once producer name per (source partition stream -> target).</summary>
    public static string EffectivelyOnceProducerName(string sourcePartitionStream, string targetStream)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(sourcePartitionStream);
        ArgumentException.ThrowIfNullOrWhiteSpace(targetStream);
        var full = $"streambus-eo-{sourcePartitionStream}-to-{targetStream}";
        return Encoding.UTF8.GetByteCount(full) <= MaxName
            ? full
            : $"streambus-eo-{Sha32(full)}";
    }

    /// <summary>Stable consumer reference for an effectively-once pipeline.</summary>
    public static string EffectivelyOnceConsumerReference(Type busType, string sourceStream, string targetStream)
    {
        ArgumentNullException.ThrowIfNull(busType);
        ArgumentException.ThrowIfNullOrWhiteSpace(sourceStream);
        ArgumentException.ThrowIfNullOrWhiteSpace(targetStream);
        var full = $"streambus-eo:{busType.FullName}:{sourceStream}->{targetStream}";
        return Encoding.UTF8.GetByteCount(full) <= MaxName
            ? full
            : $"streambus-eo-{Sha32(full)}";
    }

    public static string Enforce(string name)
    {
        ArgumentNullException.ThrowIfNull(name);
        return Encoding.UTF8.GetByteCount(name) <= MaxName
            ? name
            : throw new StreamBusConfigurationException(
                $"Stable producer name would exceed {MaxName} bytes: {TruncateForMessage(name)}");
    }

    private static string Sha32(string input)
        => Convert.ToHexString(SHA256.HashData(Encoding.UTF8.GetBytes(input)))[..32];

    private static string TruncateForMessage(string name)
        => name.Length <= 64 ? name : string.Concat(name.AsSpan(0, 61), "...");
}
