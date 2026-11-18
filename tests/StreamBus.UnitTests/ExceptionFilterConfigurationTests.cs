using Microsoft.Extensions.DependencyInjection;

namespace StreamBus.UnitTests;

public sealed class ExceptionFilterConfigurationTests
{
    private sealed record OrderEvent(string OrderId, int Sequence);

    private sealed class OrderConsumer : IConsumer<OrderEvent>
    {
        public Task Consume(ConsumeContext<OrderEvent> context) => Task.CompletedTask;
    }

    private static RetryPolicy Schedule(Action<IRetryConfigurator> configure)
    {
        var retry = new RetryConfigurator();
        configure(retry);
        return ((IRetryConfigurator)retry).BuildPolicy();
    }

    private static ConsumeOptions EndpointOptions(
        Action<IStreamBusConfigurator> bus,
        Action<IReceiveEndpointConfigurator> endpoint)
    {
        var services = new ServiceCollection();
        services.AddStreamBus((_, cfg) =>
        {
            cfg.Host("localhost", "/", h => h.Credentials("app", "secret"));
            bus(cfg);
            cfg.ReceiveEndpoint("orders", endpoint);
        });
        var sp = services.BuildServiceProvider();
        return Assert.Single(sp.GetRequiredService<StreamBusConsumerStore>().Registrations).Options;
    }

    [Fact]
    public void Filter_rules_survive_schedule_overwrites()
    {
        var policy = Schedule(r =>
        {
            r.Handle<InvalidOperationException>();
            r.Immediate(3);
            r.Ignore<ArgumentException>();
        });

        var filters = Assert.IsType<ExceptionFilterSet>(policy.Filters);
        Assert.Equal(RetryDecision.Retry, filters.Decide(new InvalidOperationException()));
        Assert.Equal(RetryDecision.Ignored, filters.Decide(new ArgumentException()));
    }

    [Fact]
    public void Filters_called_before_schedule_are_kept()
    {
        var policy = Schedule(r =>
        {
            r.Ignore<ArgumentException>();
            r.Immediate(1);
        });

        var filters = Assert.IsType<ExceptionFilterSet>(policy.Filters);
        Assert.Equal(RetryDecision.Ignored, filters.Decide(new ArgumentException()));
    }

    [Fact]
    public void Endpoint_use_retry_replaces_global_filters()
    {
        var options = EndpointOptions(
            bus => bus.UseRetry(r =>
            {
                r.Handle<InvalidOperationException>();
                r.Immediate(5);
            }),
            endpoint =>
            {
                endpoint.Consumer<OrderConsumer>("grp");
                endpoint.UseRetry(r => r.Immediate(2));
            });

        Assert.Null(options.Retry.Filters);
        Assert.Equal(3, options.Retry.MaxAttempts);
    }

    [Fact]
    public void Argument_validation_rejects_invalid_filter_arguments()
    {
        Assert.Throws<ArgumentNullException>(() => Schedule(r => r.Handle((Type[])null!)));
        Assert.Throws<StreamBusConfigurationException>(() => Schedule(r => r.Handle(Array.Empty<Type>())));
        Assert.Throws<StreamBusConfigurationException>(() => Schedule(r => r.Handle(typeof(string))));
        Assert.Throws<StreamBusConfigurationException>(() => Schedule(r => r.Handle(new Type[] { null! })));
        Assert.Throws<ArgumentNullException>(() => Schedule(r => r.Handle((Func<Exception, bool>)null!)));
        Assert.Throws<ArgumentNullException>(() => Schedule(r => r.Ignore((Type[])null!)));
        Assert.Throws<StreamBusConfigurationException>(() => Schedule(r => r.Ignore(Array.Empty<Type>())));
        Assert.Throws<StreamBusConfigurationException>(() => Schedule(r => r.Ignore(typeof(string))));
        Assert.Throws<ArgumentNullException>(() => Schedule(r => r.Ignore<ArgumentException>(null!)));
    }
}
