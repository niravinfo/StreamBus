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

    [Fact]
    public async Task Buses_on_one_vhost_share_one_supervisor()
    {
        var registry = Create(out _);
        var a = registry.GetOrCreateBus("/");
        var b = registry.GetOrCreateBus<OrdersBus>("/");
        Assert.NotSame(a, (IStreamBus)b);
        var busRegistry = (IStreamBusRegistry)registry;
        var entryA = busRegistry.GetOrCreateEntry(typeof(DefaultBus), "/");
        var entryB = busRegistry.GetOrCreateEntry(typeof(OrdersBus), "/");
        Assert.Same(entryA.Supervisor, entryB.Supervisor);
        await Task.CompletedTask;
    }

    [Fact]
    public void Different_vhosts_get_different_supervisors()
    {
        var registry = Create(out _);
        registry.GetOrCreateBus("/");
        registry.GetOrCreateBus("other-vh");
        var busRegistry = (IStreamBusRegistry)registry;
        var s1 = busRegistry.SupervisorFor("/");
        var s2 = busRegistry.SupervisorFor("other-vh");
        Assert.NotSame(s1, s2);
    }
}
