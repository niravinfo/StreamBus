using Microsoft.Extensions.DependencyInjection;

namespace StreamBus.UnitTests;

public sealed class ReceiveEndpointTests
{
    private sealed record OrderEvent(string OrderId, int Seq);

    private sealed class OrderConsumer : IConsumer<OrderEvent>
    {
        public Task Consume(ConsumeContext<OrderEvent> context) => Task.CompletedTask;
    }

    private sealed class AuditConsumer : IConsumer<OrderEvent>
    {
        public Task Consume(ConsumeContext<OrderEvent> context) => Task.CompletedTask;
    }

    private sealed class NotAConsumer;

    private sealed class NeverReadyConnector : IStreamSystemConnector
    {
        public Task<IStreamSystemHandle> ConnectAsync(StreamBusOptions options, CancellationToken ct)
            => new TaskCompletionSource<IStreamSystemHandle>().Task;
    }

    private static StreamConsumerRegistry UnstartedRegistry()
    {
        var options = new StreamBusOptions { Vhost = "/", UserName = "app", Password = "secret" };
        var supervisor = new StreamBusConnectionSupervisor(options, new NeverReadyConnector());
        return new StreamConsumerRegistry(supervisor, "/", new StreamProducerRegistry(supervisor, "/"), null, new ResultProducerRegistry(supervisor, "/"));
    }

    private static IReadOnlyList<TypedConsumerRegistration> RegistrationsFor(IServiceProvider sp)
        => sp.GetRequiredService<StreamBusConsumerStore>().Registrations;

    [Fact]
    public void Handoff_confirm_timeout_defaults_and_clamps()
    {
        var defaults = new ServiceCollection();
        defaults.AddStreamBus((_, cfg) =>
        {
            cfg.Host("localhost", "/", h => h.Credentials("app", "secret"));
            cfg.ReceiveEndpoint("orders", e => e.Consumer<OrderConsumer>("order-processors"));
        });
        Assert.Equal(TimeSpan.FromSeconds(10),
            Assert.Single(RegistrationsFor(defaults.BuildServiceProvider())).Options.HandoffConfirmTimeout);

        var clamped = new ServiceCollection();
        clamped.AddStreamBus((_, cfg) =>
        {
            cfg.Host("localhost", "/", h => h.Credentials("app", "secret"));
            cfg.ReceiveEndpoint("orders", e =>
            {
                e.Consumer<OrderConsumer>("order-processors");
                e.ErrorHandoffTimeout(TimeSpan.FromMilliseconds(5));
            });
        });
        Assert.Equal(TimeSpan.FromSeconds(1),
            Assert.Single(RegistrationsFor(clamped.BuildServiceProvider())).Options.HandoffConfirmTimeout);

        var invalid = new ServiceCollection();
        Assert.Throws<StreamBusConfigurationException>(() =>
            invalid.AddStreamBus((_, cfg) =>
            {
                cfg.Host("localhost", "/", h => h.Credentials("app", "secret"));
                cfg.ReceiveEndpoint("orders", e =>
                {
                    e.Consumer<OrderConsumer>("order-processors");
                    e.ErrorHandoffTimeout(TimeSpan.Zero);
                });
            }));
    }

    [Fact]
    public void Group_is_used_verbatim()
    {
        var services = new ServiceCollection();
        services.AddStreamBus((_, cfg) =>
        {
            cfg.Host("localhost", "/", h => h.Credentials("app", "secret"));
            cfg.ReceiveEndpoint("orders", e => e.Consumer<OrderConsumer>("order-processors"));
        });
        var sp = services.BuildServiceProvider();
        var reg = Assert.Single(RegistrationsFor(sp));
        Assert.Equal("order-processors", reg.Options.Reference);
        Assert.Equal("orders", reg.Stream);
        Assert.Equal(typeof(OrderEvent), reg.MessageType);
        Assert.Equal(typeof(OrderConsumer), reg.ConsumerType);
    }

    [Theory]
    [InlineData("")]
    [InlineData("   ")]
    public void Empty_group_rejected(string group)
    {
        var services = new ServiceCollection();
        Assert.Throws<StreamBusConfigurationException>(() =>
            services.AddStreamBus((_, cfg) =>
            {
                cfg.Host("localhost", "/", h => h.Credentials("app", "secret"));
                cfg.ReceiveEndpoint("orders", e => e.Consumer<OrderConsumer>(group));
            }));
    }

    [Fact]
    public void Over_long_group_rejected()
    {
        var services = new ServiceCollection();
        var ex = Assert.Throws<StreamBusConfigurationException>(() =>
            services.AddStreamBus((_, cfg) =>
            {
                cfg.Host("localhost", "/", h => h.Credentials("app", "secret"));
                cfg.ReceiveEndpoint("orders", e => e.Consumer<OrderConsumer>(new string('g', 300)));
            }));
        Assert.Contains("255", ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void Two_consumer_calls_throw()
    {
        var services = new ServiceCollection();
        Assert.Throws<StreamBusConfigurationException>(() =>
            services.AddStreamBus((_, cfg) =>
            {
                cfg.Host("localhost", "/", h => h.Credentials("app", "secret"));
                cfg.ReceiveEndpoint("orders", e =>
                {
                    e.Consumer<OrderConsumer>("first");
                    e.Consumer<AuditConsumer>("second");
                });
            }));
    }

    [Fact]
    public void Missing_consumer_call_throws()
    {
        var services = new ServiceCollection();
        Assert.Throws<StreamBusConfigurationException>(() =>
            services.AddStreamBus((_, cfg) =>
            {
                cfg.Host("localhost", "/", h => h.Credentials("app", "secret"));
                cfg.ReceiveEndpoint("orders", _ => { });
            }));
    }

    [Fact]
    public void Non_consumer_type_throws()
    {
        var services = new ServiceCollection();
        var ex = Assert.Throws<StreamBusConfigurationException>(() =>
            services.AddStreamBus((_, cfg) =>
            {
                cfg.Host("localhost", "/", h => h.Credentials("app", "secret"));
                cfg.ReceiveEndpoint("orders", e => e.Consumer<NotAConsumer>("grp"));
            }));
        Assert.Contains(typeof(NotAConsumer).FullName!, ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void Same_consumer_different_groups_yields_independent_registrations()
    {
        var services = new ServiceCollection();
        services.AddStreamBus((_, cfg) =>
        {
            cfg.Host("localhost", "/", h => h.Credentials("app", "secret"));
            cfg.ReceiveEndpoint("orders", e => e.Consumer<OrderConsumer>("group-a"));
            cfg.ReceiveEndpoint("orders", e => e.Consumer<OrderConsumer>("group-b"));
        });
        var sp = services.BuildServiceProvider();
        var regs = RegistrationsFor(sp);
        Assert.Equal(2, regs.Count);
        Assert.Contains(regs, r => r.Options.Reference == "group-a");
        Assert.Contains(regs, r => r.Options.Reference == "group-b");
    }

    [Fact]
    public void Same_group_across_instances_shares_one_cursor_identity()
    {
        static string Register()
        {
            var services = new ServiceCollection();
            services.AddStreamBus((_, cfg) =>
            {
                cfg.Host("localhost", "/", h => h.Credentials("app", "secret"));
                cfg.ReceiveEndpoint("orders", e => e.Consumer<OrderConsumer>("shared-scale-out"));
            });
            var sp = services.BuildServiceProvider();
            return Assert.Single(RegistrationsFor(sp)).Options.Reference!;
        }
        Assert.Equal(Register(), Register());
    }

    [Theory]
    [InlineData(null)]
    [InlineData("")]
    [InlineData("   ")]
    public async Task Raw_consume_without_reference_throws(string? reference)
    {
        var registry = UnstartedRegistry();
        var ex = await Assert.ThrowsAsync<StreamBusConfigurationException>(() =>
            registry.ConsumeAsync(typeof(DefaultBus), "orders",
                new ConsumeOptions { Reference = reference },
                _ => Task.CompletedTask, TestContext.Current.CancellationToken));
        Assert.Contains("orders", ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void Endpoint_descriptor_flows_knobs_to_consumer_options()
    {
        var services = new ServiceCollection();
        services.AddStreamBus((_, cfg) =>
        {
            cfg.Host("localhost", "/", h => h.Credentials("app", "secret"));
            cfg.ReceiveEndpoint("orders", e =>
            {
                e.Consumer<OrderConsumer>("grp");
                e.SingleActiveConsumer(false);
                e.Offset(ConsumeOffset.AlwaysFirst);
                e.Partitions(3);
            });
        });
        var sp = services.BuildServiceProvider();
        var reg = Assert.Single(RegistrationsFor(sp));
        Assert.True(reg.Options.IsSuperStream);
        Assert.False(reg.Options.SingleActiveConsumer);
        Assert.Equal(ConsumeOffset.AlwaysFirst, reg.Options.Offset);
        var endpoint = Assert.Single(sp.GetRequiredService<StreamBusEndpointStore>().Endpoints);
        Assert.Equal(3, endpoint.Descriptor.SuperPartitions);
    }
}
