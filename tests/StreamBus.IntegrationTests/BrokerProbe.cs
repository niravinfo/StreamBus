using RabbitMQ.Stream.Client;

namespace StreamBus.IntegrationTests;

/// <summary>
/// Broker probes used by readiness polling in the integration tests.
/// </summary>
/// <remarks>
/// The stream client answers a missing stream two different ways: a stream that was
/// never declared comes back as "not found" and <c>StreamExists</c> returns false,
/// while a stream that exists but is not ready yet (being created, or still coming
/// up on its leader) comes back as "not available" and the client throws
/// <see cref="GenericProtocolException"/>. A readiness poll has to read the second
/// case as "not there yet" instead of letting the exception end the test.
/// </remarks>
internal static class BrokerProbe
{
    public static bool StreamExists(StreamSystem admin, string stream)
    {
        try { return admin.StreamExists(stream).GetAwaiter().GetResult(); }
        catch (GenericProtocolException) { return false; }
    }

    public static bool SuperStreamExists(StreamSystem admin, string superStream)
    {
        try { return admin.SuperStreamExists(superStream).GetAwaiter().GetResult(); }
        catch (GenericProtocolException) { return false; }
    }

    public static int PartitionCount(StreamSystem admin, string superStream)
    {
        try { return admin.QueryPartition(superStream).GetAwaiter().GetResult().Length; }
        catch (GenericProtocolException) { return 0; }
    }
}
