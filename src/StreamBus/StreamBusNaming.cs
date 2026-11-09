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
