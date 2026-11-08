using System.Net;
using RabbitMQ.Stream.Client;

namespace StreamBus.IntegrationTests;

public sealed class TestBrokerSmokeTests()
{
    [Fact]
    public async Task Start_returns_running_broker_with_stream_listener()
    {
        IntegrationLane.RequireBroker();
        var broker = await TestBroker.StartAsync(TestContext.Current.CancellationToken);
        Assert.Equal("app", broker.Username);
        Assert.True(broker.StreamPort > 0);

        await using var system = await StreamSystem.Create(new StreamSystemConfig
        {
            Endpoints = new List<EndPoint> { new IPEndPoint(IPAddress.Loopback, broker.StreamPort) },
            UserName = broker.Username,
            Password = broker.Password,
            VirtualHost = "/",
        });

        const string stream = "smoke-test-stream";
        await system.CreateStream(new StreamSpec(stream));
        try
        {
            Assert.True(BrokerProbe.StreamExists(system, stream));
        }
        finally
        {
            await system.DeleteStream(stream);
        }
    }
}
