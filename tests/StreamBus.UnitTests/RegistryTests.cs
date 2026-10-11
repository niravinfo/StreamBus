namespace StreamBus.UnitTests;

public sealed class RegistryTests
{
    private static StreamBusRegistry Create(out List<StreamBusConnectionSupervisor> supervisors)
    {
        var list = new List<StreamBusConnectionSupervisor>();
        supervisors = list;
        return new StreamBusRegistry(
            vhost => new StreamBusOptions { Vhost = vhost, UserName = "app", Password = "secret" },
            options =>
            {
                var sup = new StreamBusConnectionSupervisor(options, new ScriptedConnector());
                list.Add(sup);
                return sup;
            });
    }

    private sealed class ScriptedConnector : IStreamSystemConnector
    {
        public Task<IStreamSystemHandle> ConnectAsync(StreamBusOptions options, CancellationToken ct)
            => new TaskCompletionSource<IStreamSystemHandle>().Task;
    }

    private sealed class OrdersBus;
    private sealed class BillingBus;

    [Fact]
    public void Same_key_returns_same_wrapper()
    {
        var registry = Create(out _);
        Assert.Same(registry.GetOrCreateBus("/"), registry.GetOrCreateBus("/"));
        Assert.Same(registry.GetOrCreateBus<OrdersBus>("/"), registry.GetOrCreateBus<OrdersBus>("/"));
    }

    [Fact]
    public void Default_and_marker_buses_coexist()
    {
        var registry = Create(out _);
        Assert.NotSame(registry.GetOrCreateBus("/"), (IStreamBus)registry.GetOrCreateBus<OrdersBus>("/"));
        Assert.NotSame(registry.GetOrCreateBus<OrdersBus>("/"), registry.GetOrCreateBus<BillingBus>("/"));
    }

    [Fact]
    public void States_describe_every_registered_bus()
    {
        var registry = Create(out _);
        registry.GetOrCreateBus("/");
        registry.GetOrCreateBus<OrdersBus>("orders-vh");
        var states = registry.DescribeStates().ToList();
        Assert.Equal(2, states.Count);
        Assert.Contains(states, s => s.Vhost == "/" && s.BusType.Contains(nameof(DefaultBus)));
        Assert.Contains(states, s => s.Vhost == "orders-vh" && s.BusType.Contains(nameof(OrdersBus)));
    }
}
