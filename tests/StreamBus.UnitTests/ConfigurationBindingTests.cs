using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;

namespace StreamBus.UnitTests;

public sealed class ConfigurationBindingTests
{
    private sealed record OrderEvent(string OrderId, int Sequence);

    private sealed class OrderConsumer : IConsumer<OrderEvent>
    {
        public Task Consume(ConsumeContext<OrderEvent> context, CancellationToken ct) => Task.CompletedTask;
    }

    private static IConfigurationSection Section(IEnumerable<KeyValuePair<string, string?>> values)
        => new ConfigurationBuilder()
            .AddInMemoryCollection(values.ToDictionary(kv => kv.Key, kv => kv.Value))
            .Build()
            .GetSection("StreamBus");

    private static KeyValuePair<string, string?> Kv(string key, string? value) => new(key, value);

    [Fact]
    public void Full_bind_applies_primitives_topology_and_code_lambda()
    {
        var section = Section(new[]
        {
            Kv("StreamBus:Vhost", "/section-vh"),
            Kv("StreamBus:UserName", "app"),
            Kv("StreamBus:Password", "secret"),
            Kv("StreamBus:Host", "rabbit.internal"),
            Kv("StreamBus:StreamPort", "5553"),
            Kv("StreamBus:AmqpPort", "5673"),
            Kv("StreamBus:HeartbeatSeconds", "45"),
            Kv("StreamBus:Connection:ProducersPerConnection", "3"),
            Kv("StreamBus:Connection:ConsumersPerConnection", "5"),
            Kv("StreamBus:Topology:Streams:0:Name", "audit"),
            Kv("StreamBus:Topology:Streams:0:MaxAgeDays", "30"),
            Kv("StreamBus:Topology:SuperStreams:0:Name", "orders-super"),
            Kv("StreamBus:Topology:SuperStreams:0:Partitions", "3"),
            Kv("StreamBus:Topology:SuperStreams:0:LeaderLocator", "Random"),
        });
        var services = new ServiceCollection();
        services.AddStreamBus(section, (_, cfg) =>
        {
            cfg.Publish<OrderEvent>("orders");
            cfg.ReceiveEndpoint("orders", e => e.Consumer<OrderConsumer>("order-processors"));
        });
        var sp = services.BuildServiceProvider();
        var options = sp.GetRequiredService<StreamBusConfigStore>().ForVhost("/section-vh");
        Assert.Equal("rabbit.internal", options.Host);
        Assert.Equal("app", options.UserName);
        Assert.Equal(5553, options.StreamPort);
        Assert.Equal(5673, options.AmqpPort);
        Assert.Equal(TimeSpan.FromSeconds(45), options.Heartbeat);
        Assert.Equal(3, options.Connection.ProducersPerConnection);
        Assert.Equal(5, options.Connection.ConsumersPerConnection);
        Assert.Contains(options.Topology.Streams, s => s.Name == "audit" && s.MaxAge == TimeSpan.FromDays(30));
        var super = Assert.Single(options.Topology.SuperStreams);
        Assert.Equal("orders-super", super.Name);
        Assert.Equal(3, super.Partitions);
        Assert.Equal(StreamLeaderLocator.Random, super.LeaderLocator);
        Assert.NotNull(sp.GetRequiredService<StreamBusPublishStore>());
        Assert.Equal("order-processors", Assert.Single(sp.GetRequiredService<StreamBusConsumerStore>().Registrations).Options.Reference);
    }

    [Fact]
    public void Missing_user_and_password_name_the_section_path()
    {
        var section = Section(new[] { Kv("StreamBus:Vhost", "/") });
        var services = new ServiceCollection();
        var ex = Assert.Throws<StreamBusConfigurationException>(() =>
            services.AddStreamBus(section, (_, cfg) =>
            {
                cfg.Publish<OrderEvent>("orders");
            }));
        Assert.Contains("StreamBus", ex.Message, StringComparison.Ordinal);
        Assert.Contains("UserName", ex.Message, StringComparison.Ordinal);
        Assert.Contains("Password", ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void Bad_values_are_reported_together()
    {
        var section = Section(new[]
        {
            Kv("StreamBus:UserName", "app"),
            Kv("StreamBus:Password", "secret"),
            Kv("StreamBus:StreamPort", "not-a-port"),
            Kv("StreamBus:Topology:SuperStreams:0:Name", "ss"),
            Kv("StreamBus:Topology:SuperStreams:0:Partitions", "0"),
            Kv("StreamBus:Topology:Streams:0:Name", "s"),
            Kv("StreamBus:Topology:Streams:0:LeaderLocator", "Nope"),
        });
        var services = new ServiceCollection();
        var ex = Assert.Throws<StreamBusConfigurationException>(() =>
            services.AddStreamBus(section, (_, cfg) => cfg.Publish<OrderEvent>("orders")));
        Assert.Contains("StreamPort", ex.Message, StringComparison.Ordinal);
        Assert.Contains("Partitions", ex.Message, StringComparison.Ordinal);
        Assert.Contains("LeaderLocator", ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void Section_creds_without_code_host_build_with_defaults()
    {
        var section = Section(new[]
        {
            Kv("StreamBus:UserName", "app"),
            Kv("StreamBus:Password", "secret"),
        });
        var services = new ServiceCollection();
        services.AddStreamBus(section, (_, cfg) => cfg.Publish<OrderEvent>("orders"));
        var sp = services.BuildServiceProvider();
        var options = sp.GetRequiredService<StreamBusConfigStore>().ForVhost("/");
        Assert.Equal("127.0.0.1", options.Host);
        Assert.Equal(5552, options.StreamPort);
        Assert.Equal(5672, options.AmqpPort);
        Assert.Equal(TimeSpan.FromSeconds(60), options.Heartbeat);
        Assert.Equal(1, options.Connection.ProducersPerConnection);
        Assert.Equal(1, options.Connection.ConsumersPerConnection);
    }

    [Fact]
    public void Code_host_supplies_creds_for_topology_only_section()
    {
        var section = Section(new[]
        {
            Kv("StreamBus:Topology:Streams:0:Name", "audit"),
        });
        var services = new ServiceCollection();
        services.AddStreamBus(section, (_, cfg) =>
            cfg.Host("127.0.0.1", "/", h => h.Credentials("app", "secret")));
        var sp = services.BuildServiceProvider();
        var options = sp.GetRequiredService<StreamBusConfigStore>().ForVhost("/");
        Assert.Contains(options.Topology.Streams, s => s.Name == "audit");
    }

    [Fact]
    public void Guest_credentials_from_section_rejected()
    {
        var section = Section(new[]
        {
            Kv("StreamBus:UserName", "guest"),
            Kv("StreamBus:Password", "guest"),
        });
        var services = new ServiceCollection();
        var ex = Assert.Throws<StreamBusConfigurationException>(() =>
            services.AddStreamBus(section));
        Assert.Contains("guest", ex.Message, StringComparison.OrdinalIgnoreCase);
    }
}
