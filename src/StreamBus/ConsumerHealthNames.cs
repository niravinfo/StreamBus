namespace StreamBus;

/// <summary>
/// Builds the per-registration identity shown by the health check.
/// </summary>
/// <remarks>
/// Every consumer row carries the bus type name because a vhost is shared by
/// all buses connected to it. Without it, two buses consuming the same stream
/// on one vhost collapse into a single row and overwrite each other's state.
/// </remarks>
internal static class ConsumerHealthNames
{
    public static string Endpoint(string busName, string stream)
        => $"endpoint:{busName}:{stream}";

    public static string Raw(string busName, string stream, int sequence)
        => sequence <= 1 ? $"raw:{busName}:{stream}" : $"raw:{busName}:{stream}:{sequence}";
}
