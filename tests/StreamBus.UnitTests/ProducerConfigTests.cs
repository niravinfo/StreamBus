using RabbitMQ.Stream.Client;

namespace StreamBus.UnitTests;

public sealed class ProducerConfigTests
{
    [Fact]
    public void Plain_config_keeps_the_supplied_reference_and_handlers()
    {
        var confirmations = new ConfirmationTracker();

        var config = ProducerConfigs.Plain("orders", "stable-ref", confirmations);

        Assert.Equal("orders", config.Stream);
        Assert.Equal("stable-ref", config.Reference);
        Assert.Equal(100, config.MessagesBufferSize);
        Assert.NotNull(config.ConfirmHandler);
        Assert.NotNull(config.ConnectionClosedHandler);
    }

    [Fact]
    public void Super_config_keeps_the_supplied_reference_and_routing()
    {
        var confirmations = new ConfirmationTracker();
        var routing = new SuperStreamRouting();

        var config = ProducerConfigs.Super("orders-super", "stable-ref", confirmations, routing);

        Assert.Equal("orders-super", config.SuperStream);
        Assert.Equal("stable-ref", config.Reference);
        Assert.Equal(100, config.MessagesBufferSize);
        Assert.NotNull(config.ConfirmHandler);
        Assert.NotNull(config.ConnectionClosedHandler);
        Assert.NotNull(config.Routing);

        var message = new Message(new byte[] { 1 });
        routing.Stamp(message, "k0");
        Assert.Equal("k0", config.Routing(message));
    }
}
