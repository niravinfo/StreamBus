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
    public void Open_generic_universal_publish_filter_is_rejected()
    {
        var ex = Assert.Throws<StreamBusConfigurationException>(() =>
            Configure(cfg => cfg.UsePublishFilter(typeof(RegistrationUniversalOpenPublishFilter<>))));

        Assert.Contains("open generic universal filter", ex.Message, StringComparison.Ordinal);
        Assert.DoesNotContain("both", ex.Message, StringComparison.OrdinalIgnoreCase);
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
    public void Open_generic_universal_consume_filter_is_rejected()
    {
        var ex = Assert.Throws<StreamBusConfigurationException>(() =>
            Configure(cfg => cfg.UseConsumeFilter(typeof(RegistrationUniversalOpenConsumeFilter<>))));

        Assert.Contains("open generic universal filter", ex.Message, StringComparison.Ordinal);
        Assert.DoesNotContain("both", ex.Message, StringComparison.OrdinalIgnoreCase);
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

    [Fact]
    public void ClosedFilterType_returns_expected_types_for_publish_registrations()
    {
        var universal = PublishFilterRegistration.ForType(typeof(RegistrationUniversalPublishFilter));
        Assert.Null(universal.ClosedFilterType(typeof(RegistrationOrder)));

        var concrete = PublishFilterRegistration.ForType(typeof(RegistrationTypedPublishFilter));
        Assert.Null(concrete.ClosedFilterType(typeof(RegistrationInvoice)));
        Assert.Equal(typeof(RegistrationTypedPublishFilter), concrete.ClosedFilterType(typeof(RegistrationOrder)));

        var multi = PublishFilterRegistration.ForType(typeof(RegistrationMultiPublishFilter));
        Assert.Equal(typeof(RegistrationMultiPublishFilter), multi.ClosedFilterType(typeof(RegistrationOrder)));
        Assert.Equal(typeof(RegistrationMultiPublishFilter), multi.ClosedFilterType(typeof(RegistrationInvoice)));

        var open = PublishFilterRegistration.ForType(typeof(RegistrationOpenPublishFilter<>));
        Assert.Equal(
            typeof(RegistrationOpenPublishFilter<RegistrationOrder>),
            open.ClosedFilterType(typeof(RegistrationOrder)));
    }

    [Fact]
    public void ClosedFilterType_returns_expected_types_for_consume_registrations()
    {
        var universal = ConsumeFilterRegistration.ForType(typeof(RegistrationUniversalConsumeFilter));
        Assert.Null(universal.ClosedFilterType(typeof(RegistrationOrder)));

        var concrete = ConsumeFilterRegistration.ForType(typeof(RegistrationTypedConsumeFilter));
        Assert.Null(concrete.ClosedFilterType(typeof(RegistrationInvoice)));
        Assert.Equal(typeof(RegistrationTypedConsumeFilter), concrete.ClosedFilterType(typeof(RegistrationOrder)));

        var open = ConsumeFilterRegistration.ForType(typeof(RegistrationOpenConsumeFilter<>));
        Assert.Equal(
            typeof(RegistrationOpenConsumeFilter<RegistrationOrder>),
            open.ClosedFilterType(typeof(RegistrationOrder)));
    }

    [Fact]
    public void CreateTyped_wraps_the_resolved_publish_filter_with_the_user_filter_identity()
    {
        var services = new ServiceCollection();
        services.AddTransient<RegistrationTypedPublishFilter>();
        using var provider = services.BuildServiceProvider();

        var registration = PublishFilterRegistration.ForType(typeof(RegistrationTypedPublishFilter));
        var closedFilterType = registration.ClosedFilterType(typeof(RegistrationOrder));
        Assert.NotNull(closedFilterType);

        var adapter = registration.CreateTyped(closedFilterType, typeof(RegistrationOrder), provider);

        Assert.Equal(typeof(RegistrationTypedPublishFilter), FilterIdentity.TypeOf(adapter));
    }

    [Fact]
    public void CreateTyped_wraps_the_resolved_open_generic_publish_filter_with_the_closed_user_filter_identity()
    {
        var services = new ServiceCollection();
        services.AddTransient(typeof(RegistrationOpenPublishFilter<>));
        using var provider = services.BuildServiceProvider();

        var registration = PublishFilterRegistration.ForType(typeof(RegistrationOpenPublishFilter<>));
        var closedFilterType = registration.ClosedFilterType(typeof(RegistrationOrder));
        Assert.NotNull(closedFilterType);

        var adapter = registration.CreateTyped(closedFilterType, typeof(RegistrationOrder), provider);

        Assert.Equal(typeof(RegistrationOpenPublishFilter<RegistrationOrder>), FilterIdentity.TypeOf(adapter));
    }

    [Fact]
    public void CreateTyped_wraps_the_resolved_consume_filter_with_the_user_filter_identity()
    {
        var services = new ServiceCollection();
        services.AddTransient<RegistrationTypedConsumeFilter>();
        using var provider = services.BuildServiceProvider();

        var registration = ConsumeFilterRegistration.ForType(typeof(RegistrationTypedConsumeFilter));
        var closedFilterType = registration.ClosedFilterType(typeof(RegistrationOrder));
        Assert.NotNull(closedFilterType);

        var adapter = registration.CreateTyped(closedFilterType, typeof(RegistrationOrder), provider);

        Assert.Equal(typeof(RegistrationTypedConsumeFilter), FilterIdentity.TypeOf(adapter));
    }

    [Fact]
    public void CreateTyped_wraps_a_multi_interface_publish_filter_for_each_requested_message_type()
    {
        var services = new ServiceCollection();
        services.AddTransient<RegistrationMultiPublishFilter>();
        using var provider = services.BuildServiceProvider();

        var registration = PublishFilterRegistration.ForType(typeof(RegistrationMultiPublishFilter));
        var orderFilterType = registration.ClosedFilterType(typeof(RegistrationOrder));
        var invoiceFilterType = registration.ClosedFilterType(typeof(RegistrationInvoice));
        Assert.NotNull(orderFilterType);
        Assert.NotNull(invoiceFilterType);

        var orderAdapter = registration.CreateTyped(orderFilterType, typeof(RegistrationOrder), provider);
        var invoiceAdapter = registration.CreateTyped(invoiceFilterType, typeof(RegistrationInvoice), provider);

        Assert.Equal(typeof(RegistrationMultiPublishFilter), FilterIdentity.TypeOf(orderAdapter));
        Assert.Equal(typeof(RegistrationMultiPublishFilter), FilterIdentity.TypeOf(invoiceAdapter));
    }

    [Fact]
    public void CreateTyped_reports_a_missing_service_with_the_existing_resolution_message()
    {
        using var provider = new ServiceCollection().BuildServiceProvider();
        var registration = PublishFilterRegistration.ForType(typeof(RegistrationTypedPublishFilter));

        var ex = Assert.Throws<StreamBusConfigurationException>(() =>
            registration.CreateTyped(typeof(RegistrationTypedPublishFilter), typeof(RegistrationOrder), provider));

        Assert.Contains("could not be resolved from the root service provider", ex.Message, StringComparison.Ordinal);
    }

    private sealed record RegistrationOrder(string Id);

    private sealed record RegistrationInvoice(string Id);

    private sealed class RegistrationUniversalPublishFilter : IStreamPublishFilter
    {
        public Task OnPublish(StreamPublishFilterContext context, PublishDelegate next, CancellationToken ct)
            => next();
    }

    private sealed class RegistrationTypedPublishFilter : IStreamPublishFilter<RegistrationOrder>
    {
        public Task OnPublish(
            StreamPublishFilterContext<RegistrationOrder> context,
            PublishDelegate next,
            CancellationToken ct)
            => next();
    }

    private sealed class RegistrationMultiPublishFilter
        : IStreamPublishFilter<RegistrationOrder>, IStreamPublishFilter<RegistrationInvoice>
    {
        public Task OnPublish(
            StreamPublishFilterContext<RegistrationOrder> context,
            PublishDelegate next,
            CancellationToken ct)
            => next();

        public Task OnPublish(
            StreamPublishFilterContext<RegistrationInvoice> context,
            PublishDelegate next,
            CancellationToken ct)
            => next();
    }

    private sealed class RegistrationOpenPublishFilter<T> : IStreamPublishFilter<T>
    {
        public Task OnPublish(StreamPublishFilterContext<T> context, PublishDelegate next, CancellationToken ct)
            => next();
    }

    private sealed class RegistrationTwoArityPublishFilter<T1, T2> : IStreamPublishFilter<T1>
    {
        public Task OnPublish(StreamPublishFilterContext<T1> context, PublishDelegate next, CancellationToken ct)
            => next();
    }

    private sealed class RegistrationWrongParameterPublishFilter<T> : IStreamPublishFilter<string>
    {
        public Task OnPublish(StreamPublishFilterContext<string> context, PublishDelegate next, CancellationToken ct)
            => next();
    }

    private sealed class RegistrationDualPublishFilter
        : IStreamPublishFilter, IStreamPublishFilter<RegistrationOrder>
    {
        public Task OnPublish(StreamPublishFilterContext context, PublishDelegate next, CancellationToken ct)
            => next();

        public Task OnPublish(
            StreamPublishFilterContext<RegistrationOrder> context,
            PublishDelegate next,
            CancellationToken ct)
            => next();
    }

    private sealed class RegistrationDualOpenPublishFilter<T>
        : IStreamPublishFilter, IStreamPublishFilter<T>
    {
        public Task OnPublish(StreamPublishFilterContext context, PublishDelegate next, CancellationToken ct)
            => next();

        public Task OnPublish(StreamPublishFilterContext<T> context, PublishDelegate next, CancellationToken ct)
            => next();
    }

    private sealed class RegistrationUniversalOpenPublishFilter<T> : IStreamPublishFilter
    {
        public Task OnPublish(StreamPublishFilterContext context, PublishDelegate next, CancellationToken ct)
            => next();
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
            => next();
    }

    private sealed class RegistrationTypedConsumeFilter : IStreamConsumeFilter<RegistrationOrder>
    {
        public Task OnConsume(
            StreamConsumeFilterContext<RegistrationOrder> context,
            ConsumeDelegate next,
            CancellationToken ct)
            => next();
    }

    private sealed class RegistrationMultiConsumeFilter
        : IStreamConsumeFilter<RegistrationInvoice>, IStreamConsumeFilter<RegistrationOrder>
    {
        public Task OnConsume(
            StreamConsumeFilterContext<RegistrationInvoice> context,
            ConsumeDelegate next,
            CancellationToken ct)
            => next();

        public Task OnConsume(
            StreamConsumeFilterContext<RegistrationOrder> context,
            ConsumeDelegate next,
            CancellationToken ct)
            => next();
    }

    private sealed class RegistrationOpenConsumeFilter<T> : IStreamConsumeFilter<T>
    {
        public Task OnConsume(StreamConsumeFilterContext<T> context, ConsumeDelegate next, CancellationToken ct)
            => next();
    }

    private sealed class RegistrationTwoArityConsumeFilter<T1, T2> : IStreamConsumeFilter<T1>
    {
        public Task OnConsume(StreamConsumeFilterContext<T1> context, ConsumeDelegate next, CancellationToken ct)
            => next();
    }

    private sealed class RegistrationWrongParameterConsumeFilter<T> : IStreamConsumeFilter<string>
    {
        public Task OnConsume(StreamConsumeFilterContext<string> context, ConsumeDelegate next, CancellationToken ct)
            => next();
    }

    private sealed class RegistrationDualConsumeFilter
        : IStreamConsumeFilter, IStreamConsumeFilter<RegistrationOrder>
    {
        public Task OnConsume(StreamConsumeFilterContext context, ConsumeDelegate next, CancellationToken ct)
            => next();

        public Task OnConsume(
            StreamConsumeFilterContext<RegistrationOrder> context,
            ConsumeDelegate next,
            CancellationToken ct)
            => next();
    }

    private sealed class RegistrationUniversalOpenConsumeFilter<T> : IStreamConsumeFilter
    {
        public Task OnConsume(StreamConsumeFilterContext context, ConsumeDelegate next, CancellationToken ct)
            => next();
    }

    private sealed class RegistrationInvoiceConsumeFilter : IStreamConsumeFilter<RegistrationInvoice>
    {
        public Task OnConsume(
            StreamConsumeFilterContext<RegistrationInvoice> context,
            ConsumeDelegate next,
            CancellationToken ct)
            => next();
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
