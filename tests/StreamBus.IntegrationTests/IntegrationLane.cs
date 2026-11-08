namespace StreamBus.IntegrationTests;

/// <summary>
/// Gates the broker-backed integration lane.
/// </summary>
/// <remarks>
/// Every integration test class calls <see cref="RequireBroker"/> from its
/// constructor so a host without RabbitMQ can run the solution's test projects
/// with <c>STREAMBUS_SKIP_INTEGRATION=1</c> instead of failing to connect.
/// </remarks>
internal static class IntegrationLane
{
    public static void RequireBroker()
    {
        if (Environment.GetEnvironmentVariable("STREAMBUS_SKIP_INTEGRATION") == "1")
        {
            Assert.Skip("Integration lane disabled (STREAMBUS_SKIP_INTEGRATION=1).");
        }
    }
}
