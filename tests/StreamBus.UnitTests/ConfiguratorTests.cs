using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;

namespace StreamBus.UnitTests;

public sealed class ConfiguratorTests
{
    private sealed class OrdersBus;
    private sealed class BillingBus;

    [Fact]
    public void Missing_host_throws_naming_bus()
    {
        var services = new ServiceCollection();
        var ex = Assert.Throws<StreamBusConfigurationException>(() =>
            services.AddStreamBus((_, _) => { }));
        Assert.Contains(typeof(DefaultBus).FullName!, ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void Duplicate_host_throws()
    {
        var services = new ServiceCollection();
        var ex = Assert.Throws<StreamBusConfigurationException>(() =>
            services.AddStreamBus((_, cfg) =>
            {
                cfg.Host("127.0.0.1", "/", h => h.Credentials("app", "secret"));
                cfg.Host("127.0.0.1", "/", h => h.Credentials("app", "secret"));
            }));
        Assert.Contains("exactly once", ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void Guest_credentials_rejected()
    {
        var services = new ServiceCollection();
        var ex = Assert.Throws<StreamBusConfigurationException>(() =>
            services.AddStreamBus((_, cfg) => cfg.Host("127.0.0.1", "/", h => h.Credentials("guest", "guest"))));
        Assert.Contains("guest", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void Defaults_are_pinned()
    {
        var services = new ServiceCollection();
        services.AddStreamBus((_, cfg) => cfg.Host("127.0.0.1", "/", h => h.Credentials("app", "secret")));
        var sp = services.BuildServiceProvider();
        var options = sp.GetRequiredService<StreamBusConfigStore>().ForVhost("/");
        Assert.Equal("127.0.0.1", options.Host);
        Assert.Equal("/", options.Vhost);
        Assert.Equal(5552, options.StreamPort);
        Assert.Equal(5672, options.AmqpPort);
        Assert.Equal(TimeSpan.FromSeconds(60), options.Heartbeat);
        Assert.Equal(1, options.Connection.ProducersPerConnection);
        Assert.Equal(1, options.Connection.ConsumersPerConnection);
    }

    [Fact]
    public void Explicit_heartbeat_and_ports_map_verbatim()
    {
        var services = new ServiceCollection();
        var heartbeat = TimeSpan.FromSeconds(17);
        services.AddStreamBus((_, cfg) => cfg.Host("rabbit.internal", "/vh", h =>
        {
            h.Credentials("app", "secret");
            h.StreamPort(5553);
            h.AmqpPort(5673);
            h.Heartbeat(heartbeat);
            h.ProducersPerConnection(3);
            h.ConsumersPerConnection(5);
        }));
        var sp = services.BuildServiceProvider();
        var options = sp.GetRequiredService<StreamBusConfigStore>().ForVhost("/vh");
        Assert.Equal("rabbit.internal", options.Host);
        Assert.Equal(5553, options.StreamPort);
        Assert.Equal(5673, options.AmqpPort);
        Assert.Equal(heartbeat, options.Heartbeat);
        Assert.Equal(3, options.Connection.ProducersPerConnection);
        Assert.Equal(5, options.Connection.ConsumersPerConnection);
    }

    [Fact]
    public void Marker_and_default_coexist()
    {
        var services = new ServiceCollection();
        services.AddStreamBus((_, cfg) => cfg.Host("127.0.0.1", "/", h => h.Credentials("app", "secret")));
        services.AddStreamBus<OrdersBus>((_, cfg) => cfg.Host("127.0.0.1", "/", h => h.Credentials("app", "secret")));
        services.AddStreamBus<BillingBus>((_, cfg) => cfg.Host("127.0.0.1", "/", h => h.Credentials("app", "secret")));
        var sp = services.BuildServiceProvider();
        Assert.NotNull(sp.GetRequiredService<IStreamBus>());
        Assert.NotSame(
            sp.GetRequiredService<IStreamBus<OrdersBus>>(),
            sp.GetRequiredService<IStreamBus<BillingBus>>());
    }

    [Fact]
    public void Declare_streams_accumulate()
    {
        var services = new ServiceCollection();
        services.AddStreamBus((_, cfg) =>
        {
            cfg.Host("127.0.0.1", "/", h => h.Credentials("app", "secret"));
            cfg.DeclareStream("audit", k => k.MaxAge(TimeSpan.FromDays(30)));
            cfg.DeclareSuperStream("orders-super", 3);
        });
        var sp = services.BuildServiceProvider();
        var options = sp.GetRequiredService<StreamBusConfigStore>().ForVhost("/");
        Assert.Contains(options.Topology.Streams, s => s.Name == "audit" && s.MaxAge == TimeSpan.FromDays(30));
        Assert.Contains(options.Topology.SuperStreams, s => s is { Name: "orders-super", Partitions: 3 });
    }

    [Fact]
    public void Section_vhost_is_overridden_by_code_host()
    {
        var config = new ConfigurationBuilder()
            .AddInMemoryCollection(new Dictionary<string, string?> { ["StreamBus:Vhost"] = "/section-vh" })
            .Build();
        var section = config.GetSection("StreamBus");
        var services = new ServiceCollection();
        services.AddStreamBus(section, (_, cfg) => cfg.Host("127.0.0.1", "/code-vh", h => h.Credentials("app", "secret")));
        var sp = services.BuildServiceProvider();
        Assert.Equal("/code-vh", sp.GetRequiredService<StreamBusConfigStore>().ForVhost("/code-vh").Vhost);
    }

    [Fact]
    public void Section_without_host_still_requires_host()
    {
        var config = new ConfigurationBuilder()
            .AddInMemoryCollection(new Dictionary<string, string?> { ["StreamBus:Vhost"] = "/section-vh" })
            .Build();
        var services = new ServiceCollection();
        Assert.Throws<StreamBusConfigurationException>(() =>
            services.AddStreamBus(config.GetSection("StreamBus")));
    }

    [Fact]
    public void Invalid_ports_and_heartbeat_throw()
    {
        var services = new ServiceCollection();
        Assert.Throws<StreamBusConfigurationException>(() =>
            services.AddStreamBus((_, cfg) => cfg.Host("127.0.0.1", "/", h =>
            {
                h.Credentials("app", "secret");
                h.StreamPort(0);
            })));
        Assert.Throws<StreamBusConfigurationException>(() =>
            services.AddStreamBus((_, cfg) => cfg.Host("127.0.0.1", "/", h =>
            {
                h.Credentials("app", "secret");
                h.Heartbeat(TimeSpan.Zero);
            })));
    }
}
