using Microsoft.Extensions.DependencyInjection;

namespace StreamBus.UnitTests;

public sealed class TypedFilterRegistrationTests
{
    private static void Configure(Action<IStreamBusConfigurator> configure)
    {
        var services = new ServiceCollection();
        services.AddStreamBus((_, cfg) =>
        {
            cfg.Host("localhost", "/", h => h.Credentials("app", "secret"));
            configure(cfg);
        });
    }

    [Fact]
    public void Universal_publish_filter_is_accepted()
        => Configure(cfg => cfg.UsePublishFilter<RegistrationUniversalPublishFilter>());

    [Fact]
    public void Typed_concrete_publish_filter_is_accepted()
        => Configure(cfg => cfg.UsePublishFilter<RegistrationTypedPublishFilter>());

    [Fact]
    public void Concrete_publish_filter_with_multiple_typed_interfaces_is_accepted()
        => Configure(cfg => cfg.UsePublishFilter<RegistrationMultiPublishFilter>());

    [Fact]
    public void Open_generic_publish_filter_is_accepted()
        => Configure(cfg => cfg.UsePublishFilter(typeof(RegistrationOpenPublishFilter<>)));

    [Fact]
    public void Open_generic_publish_filter_with_two_parameters_is_rejected()
    {
        var ex = Assert.Throws<StreamBusConfigurationException>(() =>
            Configure(cfg => cfg.UsePublishFilter(typeof(RegistrationTwoArityPublishFilter<,>))));

        Assert.Contains("exactly one generic parameter", ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void Universal_and_typed_publish_filter_is_rejected()
    {
        var ex = Assert.Throws<StreamBusConfigurationException>(() =>
            Configure(cfg => cfg.UsePublishFilter<RegistrationDualPublishFilter>()));

        Assert.Contains("both", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void Universal_and_typed_open_generic_publish_filter_is_rejected()
    {
        var ex = Assert.Throws<StreamBusConfigurationException>(() =>
            Configure(cfg => cfg.UsePublishFilter(typeof(RegistrationDualOpenPublishFilter<>))));

        Assert.Contains("both", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void Open_generic_publish_filter_for_a_different_parameter_is_rejected()
    {
        var ex = Assert.Throws<StreamBusConfigurationException>(() =>
            Configure(cfg => cfg.UsePublishFilter(typeof(RegistrationWrongParameterPublishFilter<>))));

        Assert.Contains("same generic parameter", ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void Non_filter_publish_type_is_rejected()
    {
        var ex = Assert.Throws<StreamBusConfigurationException>(() =>
            Configure(cfg => cfg.UsePublishFilter(typeof(RegistrationOrder))));

        Assert.Contains("must implement", ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void Abstract_typed_publish_filter_is_rejected()
    {
        var ex = Assert.Throws<StreamBusConfigurationException>(() =>
            Configure(cfg => cfg.UsePublishFilter(typeof(RegistrationAbstractPublishFilter))));

        Assert.Contains(nameof(RegistrationAbstractPublishFilter), ex.Message, StringComparison.Ordinal);
        Assert.Contains("non-abstract", ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void Universal_consume_filter_is_accepted()
        => Configure(cfg => cfg.UseConsumeFilter<RegistrationUniversalConsumeFilter>());

    [Fact]
    public void Typed_concrete_consume_filter_is_accepted()
        => Configure(cfg => cfg.UseConsumeFilter<RegistrationTypedConsumeFilter>());

    [Fact]
    public void Concrete_consume_filter_with_multiple_typed_interfaces_is_accepted()
        => Configure(cfg => cfg.UseConsumeFilter<RegistrationMultiConsumeFilter>());

    [Fact]
    public void Open_generic_consume_filter_is_accepted()
        => Configure(cfg => cfg.UseConsumeFilter(typeof(RegistrationOpenConsumeFilter<>)));

    [Fact]
    public void Open_generic_consume_filter_with_two_parameters_is_rejected()
    {
        var ex = Assert.Throws<StreamBusConfigurationException>(() =>
            Configure(cfg => cfg.UseConsumeFilter(typeof(RegistrationTwoArityConsumeFilter<,>))));

        Assert.Contains("exactly one generic parameter", ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void Universal_and_typed_consume_filter_is_rejected()
    {
        var ex = Assert.Throws<StreamBusConfigurationException>(() =>
            Configure(cfg => cfg.UseConsumeFilter<RegistrationDualConsumeFilter>()));

        Assert.Contains("both", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void Open_generic_consume_filter_for_a_different_parameter_is_rejected()
    {
        var ex = Assert.Throws<StreamBusConfigurationException>(() =>
            Configure(cfg => cfg.UseConsumeFilter(typeof(RegistrationWrongParameterConsumeFilter<>))));

        Assert.Contains("same generic parameter", ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void Non_filter_consume_type_is_rejected()
    {
        var ex = Assert.Throws<StreamBusConfigurationException>(() =>
            Configure(cfg => cfg.UseConsumeFilter(typeof(RegistrationInvoice))));

        Assert.Contains("must implement", ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void Abstract_typed_consume_filter_is_rejected()
    {
        var ex = Assert.Throws<StreamBusConfigurationException>(() =>
            Configure(cfg => cfg.UseConsumeFilter(typeof(RegistrationAbstractConsumeFilter))));

        Assert.Contains(nameof(RegistrationAbstractConsumeFilter), ex.Message, StringComparison.Ordinal);
        Assert.Contains("non-abstract", ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void Endpoint_typed_consume_filter_mismatch_is_rejected_during_AddStreamBus()
    {
        var ex = Assert.Throws<StreamBusConfigurationException>(() => Configure(cfg =>
            cfg.ReceiveEndpoint("orders", e =>
            {
                e.Consumer<RegistrationConsumer>("grp");
                e.UseConsumeFilter<RegistrationInvoiceConsumeFilter>();
            })));

        Assert.Contains("orders", ex.Message, StringComparison.Ordinal);
        Assert.Contains(nameof(RegistrationInvoiceConsumeFilter), ex.Message, StringComparison.Ordinal);
        Assert.Contains(nameof(RegistrationOrder), ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void Endpoint_matching_typed_consume_filter_is_accepted()
        => Configure(cfg => cfg.ReceiveEndpoint("orders", e =>
        {
            e.Consumer<RegistrationConsumer>("grp");
            e.UseConsumeFilter<RegistrationTypedConsumeFilter>();
        }));

    [Fact]
    public void Endpoint_open_generic_consume_filter_is_accepted()
        => Configure(cfg => cfg.ReceiveEndpoint("orders", e =>
        {
            e.Consumer<RegistrationConsumer>("grp");
            e.UseConsumeFilter(typeof(RegistrationOpenConsumeFilter<>));
        }));

    [Fact]
    public void Endpoint_multi_typed_consume_filter_matching_one_interface_is_accepted()
        => Configure(cfg => cfg.ReceiveEndpoint("orders", e =>
        {
            e.Consumer<RegistrationConsumer>("grp");
            e.UseConsumeFilter<RegistrationMultiConsumeFilter>();
        }));

    [Fact]
    public void Bus_wide_typed_consume_filter_that_matches_no_endpoint_is_accepted()
        => Configure(cfg => cfg.UseConsumeFilter<RegistrationInvoiceConsumeFilter>());

    [Fact]
    public void Typed_filter_types_are_registered_as_transient_services()
    {
        var services = new ServiceCollection();
        services.AddStreamBus((_, cfg) =>
        {
            cfg.Host("localhost", "/", h => h.Credentials("app", "secret"));
            cfg.UsePublishFilter<RegistrationTypedPublishFilter>();
            cfg.UsePublishFilter(typeof(RegistrationOpenPublishFilter<>));
            cfg.UseConsumeFilter<RegistrationTypedConsumeFilter>();
            cfg.UseConsumeFilter(typeof(RegistrationOpenConsumeFilter<>));
        });

        using var provider = services.BuildServiceProvider();
        Assert.NotNull(provider.GetService<RegistrationTypedPublishFilter>());
        Assert.NotNull(provider.GetService<RegistrationOpenPublishFilter<RegistrationOrder>>());
        Assert.NotNull(provider.GetService<RegistrationTypedConsumeFilter>());
        Assert.NotNull(provider.GetService<RegistrationOpenConsumeFilter<RegistrationOrder>>());
    }

    [Fact]
    public void Classify_returns_each_typed_message_type_for_a_multi_interface_filter()
    {
        var result = FilterTypeInspector.Classify(
            typeof(RegistrationMultiPublishFilter),
            typeof(IStreamPublishFilter),
            typeof(IStreamPublishFilter<>),
            "Publish");

        Assert.Equal(FilterRegistrationKind.ConcreteTyped, result.Kind);
        Assert.Equal(2, result.MessageTypes.Distinct().Count());
        Assert.Contains(typeof(RegistrationOrder), result.MessageTypes);
        Assert.Contains(typeof(RegistrationInvoice), result.MessageTypes);
    }

    [Fact]
    public void Classify_returns_the_generic_parameter_for_an_open_generic_filter()
    {
        var result = FilterTypeInspector.Classify(
            typeof(RegistrationOpenPublishFilter<>),
            typeof(IStreamPublishFilter),
            typeof(IStreamPublishFilter<>),
            "Publish");

        Assert.Equal(FilterRegistrationKind.OpenGenericTyped, result.Kind);
        Assert.Equal(typeof(RegistrationOpenPublishFilter<>).GetGenericArguments(), result.MessageTypes);
    }

    private sealed record RegistrationOrder(string Id);

    private sealed record RegistrationInvoice(string Id);

    private sealed class RegistrationUniversalPublishFilter : IStreamPublishFilter
    {
        public Task OnPublish(StreamPublishFilterContext context, PublishDelegate next, CancellationToken ct)
            => next(context);
    }

    private sealed class RegistrationTypedPublishFilter : IStreamPublishFilter<RegistrationOrder>
    {
        public Task OnPublish(
            StreamPublishFilterContext<RegistrationOrder> context,
            PublishDelegate next,
            CancellationToken ct)
            => next(context.Inner);
    }

    private sealed class RegistrationMultiPublishFilter
        : IStreamPublishFilter<RegistrationOrder>, IStreamPublishFilter<RegistrationInvoice>
    {
        public Task OnPublish(
            StreamPublishFilterContext<RegistrationOrder> context,
            PublishDelegate next,
            CancellationToken ct)
            => next(context.Inner);

        public Task OnPublish(
            StreamPublishFilterContext<RegistrationInvoice> context,
            PublishDelegate next,
            CancellationToken ct)
            => next(context.Inner);
    }

    private sealed class RegistrationOpenPublishFilter<T> : IStreamPublishFilter<T>
    {
        public Task OnPublish(StreamPublishFilterContext<T> context, PublishDelegate next, CancellationToken ct)
            => next(context.Inner);
    }

    private sealed class RegistrationTwoArityPublishFilter<T1, T2> : IStreamPublishFilter<T1>
    {
        public Task OnPublish(StreamPublishFilterContext<T1> context, PublishDelegate next, CancellationToken ct)
            => next(context.Inner);
    }

    private sealed class RegistrationWrongParameterPublishFilter<T> : IStreamPublishFilter<string>
    {
        public Task OnPublish(StreamPublishFilterContext<string> context, PublishDelegate next, CancellationToken ct)
            => next(context.Inner);
    }

    private sealed class RegistrationDualPublishFilter
        : IStreamPublishFilter, IStreamPublishFilter<RegistrationOrder>
    {
        public Task OnPublish(StreamPublishFilterContext context, PublishDelegate next, CancellationToken ct)
            => next(context);

        public Task OnPublish(
            StreamPublishFilterContext<RegistrationOrder> context,
            PublishDelegate next,
            CancellationToken ct)
            => next(context.Inner);
    }

    private sealed class RegistrationDualOpenPublishFilter<T>
        : IStreamPublishFilter, IStreamPublishFilter<T>
    {
        public Task OnPublish(StreamPublishFilterContext context, PublishDelegate next, CancellationToken ct)
            => next(context);

        public Task OnPublish(StreamPublishFilterContext<T> context, PublishDelegate next, CancellationToken ct)
            => next(context.Inner);
    }

    private abstract class RegistrationAbstractPublishFilter : IStreamPublishFilter<RegistrationOrder>
    {
        public abstract Task OnPublish(
            StreamPublishFilterContext<RegistrationOrder> context,
            PublishDelegate next,
            CancellationToken ct);
    }

    private sealed class RegistrationUniversalConsumeFilter : IStreamConsumeFilter
    {
        public Task OnConsume(StreamConsumeFilterContext context, ConsumeDelegate next, CancellationToken ct)
            => next(context);
    }

    private sealed class RegistrationTypedConsumeFilter : IStreamConsumeFilter<RegistrationOrder>
    {
        public Task OnConsume(
            StreamConsumeFilterContext<RegistrationOrder> context,
            ConsumeDelegate next,
            CancellationToken ct)
            => next(context.Inner);
    }

    private sealed class RegistrationMultiConsumeFilter
        : IStreamConsumeFilter<RegistrationInvoice>, IStreamConsumeFilter<RegistrationOrder>
    {
        public Task OnConsume(
            StreamConsumeFilterContext<RegistrationInvoice> context,
            ConsumeDelegate next,
            CancellationToken ct)
            => next(context.Inner);

        public Task OnConsume(
            StreamConsumeFilterContext<RegistrationOrder> context,
            ConsumeDelegate next,
            CancellationToken ct)
            => next(context.Inner);
    }

    private sealed class RegistrationOpenConsumeFilter<T> : IStreamConsumeFilter<T>
    {
        public Task OnConsume(StreamConsumeFilterContext<T> context, ConsumeDelegate next, CancellationToken ct)
            => next(context.Inner);
    }

    private sealed class RegistrationTwoArityConsumeFilter<T1, T2> : IStreamConsumeFilter<T1>
    {
        public Task OnConsume(StreamConsumeFilterContext<T1> context, ConsumeDelegate next, CancellationToken ct)
            => next(context.Inner);
    }

    private sealed class RegistrationWrongParameterConsumeFilter<T> : IStreamConsumeFilter<string>
    {
        public Task OnConsume(StreamConsumeFilterContext<string> context, ConsumeDelegate next, CancellationToken ct)
            => next(context.Inner);
    }

    private sealed class RegistrationDualConsumeFilter
        : IStreamConsumeFilter, IStreamConsumeFilter<RegistrationOrder>
    {
        public Task OnConsume(StreamConsumeFilterContext context, ConsumeDelegate next, CancellationToken ct)
            => next(context);

        public Task OnConsume(
            StreamConsumeFilterContext<RegistrationOrder> context,
            ConsumeDelegate next,
            CancellationToken ct)
            => next(context.Inner);
    }

    private sealed class RegistrationInvoiceConsumeFilter : IStreamConsumeFilter<RegistrationInvoice>
    {
        public Task OnConsume(
            StreamConsumeFilterContext<RegistrationInvoice> context,
            ConsumeDelegate next,
            CancellationToken ct)
            => next(context.Inner);
    }

    private abstract class RegistrationAbstractConsumeFilter : IStreamConsumeFilter<RegistrationOrder>
    {
        public abstract Task OnConsume(
            StreamConsumeFilterContext<RegistrationOrder> context,
            ConsumeDelegate next,
            CancellationToken ct);
    }

    private sealed class RegistrationConsumer : IConsumer<RegistrationOrder>
    {
        public Task Consume(ConsumeContext<RegistrationOrder> context, CancellationToken cancellationToken)
            => Task.CompletedTask;
    }
}
