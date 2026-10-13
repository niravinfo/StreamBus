using System.Security.Cryptography;
using System.Text;

namespace StreamBus;

internal static class ReferenceDerivation
{
    public const int MaxReferenceBytes = 255;

    public static string Derive(Type busType, string stream)
    {
        ArgumentNullException.ThrowIfNull(busType);
        ArgumentException.ThrowIfNullOrWhiteSpace(stream);
        var full = $"streambus:{busType.FullName}:{stream}";
        if (Encoding.UTF8.GetByteCount(full) <= MaxReferenceBytes)
            return full;
        var hash = Convert.ToHexString(SHA256.HashData(Encoding.UTF8.GetBytes(full)))[..32];
        return $"streambus-{hash}";
    }

    public static void Validate(string reference)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(reference);
        if (Encoding.UTF8.GetByteCount(reference) > MaxReferenceBytes)
            throw new StreamBusConfigurationException(
                $"Consumer Reference exceeds the broker's 255-byte limit ({Encoding.UTF8.GetByteCount(reference)} bytes). Supply a shorter stable reference.");
    }
}
