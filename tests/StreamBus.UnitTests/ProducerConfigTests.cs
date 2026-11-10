using RabbitMQ.Stream.Client;

namespace StreamBus.UnitTests;

public sealed class ProducerConfigTests
{
    [Fact]
    public void Plain_config_is_anonymous_and_wired()
    {
        var config = ProducerConfigs.Plain("orders", new ConfirmationTracker());

        Assert.Equal("orders", config.Stream);
        Assert.Null(config.Reference);
        Assert.Equal(100, config.MessagesBufferSize);
        Assert.NotNull(config.ConfirmHandler);
        Assert.NotNull(config.ConnectionClosedHandler);
    }

    [Fact]
    public void Super_config_is_anonymous_and_keeps_routing_wired()
    {
        var routing = new SuperStreamRouting();
        var config = ProducerConfigs.Super("orders-super", new ConfirmationTracker(), routing);

        Assert.Equal("orders-super", config.SuperStream);
        Assert.Null(config.Reference);
        Assert.Equal(100, config.MessagesBufferSize);
        Assert.NotNull(config.ConfirmHandler);
        Assert.NotNull(config.ConnectionClosedHandler);
        Assert.NotNull(config.Routing);

        var message = new Message(new byte[] { 1 });
        routing.Stamp(message, "k0");
        Assert.Equal("k0", config.Routing(message));
    }
}
