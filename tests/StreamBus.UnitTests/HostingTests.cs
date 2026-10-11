using Microsoft.Extensions.DependencyInjection;

namespace StreamBus.UnitTests;

public sealed class HostingTests
{
    private sealed class OrdersBus;
    private sealed class BillingBus;

    [Fact]
    public void Duplicate_default_registration_fails()
    {
        var services = new ServiceCollection();
        services.AddStreamBus("/", o => o.Credentials("app", "secret"));
        Assert.Throws<StreamBusConfigurationException>(() =>
            services.AddStreamBus("/", o => o.Credentials("app", "secret")));
    }

    [Fact]
    public void Duplicate_marker_registration_fails_but_distinct_markers_coexist()
    {
        var services = new ServiceCollection();
        services.AddStreamBus<OrdersBus>("/", o => o.Credentials("app", "secret"));
        Assert.Throws<StreamBusConfigurationException>(() =>
            services.AddStreamBus<OrdersBus>("/", o => o.Credentials("app", "secret")));
        services.AddStreamBus<BillingBus>("/", o => o.Credentials("app", "secret"));
        var sp = services.BuildServiceProvider();
        Assert.NotSame(sp.GetRequiredService<IStreamBus<OrdersBus>>(), sp.GetRequiredService<IStreamBus<BillingBus>>());
    }

    [Fact]
    public void Default_and_marker_resolve_distinct_buses()
    {
        var services = new ServiceCollection();
        services.AddStreamBus("/", o => o.Credentials("app", "secret"));
        services.AddStreamBus<OrdersBus>("/", o => o.Credentials("app", "secret"));
        var sp = services.BuildServiceProvider();
        Assert.NotSame(sp.GetRequiredService<IStreamBus>(), (IStreamBus)sp.GetRequiredService<IStreamBus<OrdersBus>>());
    }

    [Fact]
    public void Pooling_defaults_are_one_and_overridable()
    {
        var services = new ServiceCollection();
        services.AddStreamBus("/", o =>
        {
            o.Credentials("app", "secret");
            o.Connection(c => c.ProducersPerConnection(3).ConsumersPerConnection(5));
        });
        var sp = services.BuildServiceProvider();
        var opts = sp.GetRequiredService<StreamBusConfigStore>().ForVhost("/");
        Assert.Equal(3, opts.Connection.ProducersPerConnection);
        Assert.Equal(5, opts.Connection.ConsumersPerConnection);
        var defaults = new StreamBusConnectionOptions();
        Assert.Equal(1, defaults.ProducersPerConnection);
        Assert.Equal(1, defaults.ConsumersPerConnection);
    }
}
