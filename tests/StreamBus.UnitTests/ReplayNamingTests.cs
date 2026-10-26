using System.Text;

namespace StreamBus.UnitTests;

public sealed class ReplayNamingTests
{
    private sealed class OrdersBus;

    private sealed class NeverConnector : IStreamSystemConnector
    {
        public Task<IStreamSystemHandle> ConnectAsync(StreamBusOptions options, CancellationToken ct)
            => new TaskCompletionSource<IStreamSystemHandle>().Task;
    }

    [Fact]
    public void Replay_names_are_unique_per_source_stream()
    {
        var orders = ReplayNaming.ProducerName("orders", "orders-2");
        var invoices = ReplayNaming.ProducerName("invoices", "orders-2");

        Assert.NotEqual(orders, invoices);
        Assert.Equal(orders, ReplayNaming.ProducerName("orders", "orders-2"));
        Assert.True(Encoding.UTF8.GetByteCount(orders) <= 255);
        Assert.True(Encoding.UTF8.GetByteCount(invoices) <= 255);
    }

    [Fact]
    public void Typed_registry_keys_survive_vhost_with_at_sign()
    {
        var registry = new StreamBusRegistry(
            vhost => new StreamBusOptions { Vhost = vhost, UserName = "app", Password = "secret" },
            options => new StreamBusConnectionSupervisor(options, new NeverConnector()));

        registry.GetOrCreateBus<OrdersBus>("v@h");

        var state = Assert.Single(registry.DescribeStates());
        Assert.Equal("v@h", state.Vhost);
        Assert.Contains(nameof(OrdersBus), state.BusType);
    }
}
