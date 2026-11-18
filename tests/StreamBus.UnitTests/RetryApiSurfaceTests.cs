using System.Linq;
using System.Reflection;
using Microsoft.Extensions.DependencyInjection;

namespace StreamBus.UnitTests;

/// <summary>
/// Pins the retry-surface unification: retry is configured through one verb,
/// <c>UseRetry</c>, with a fluent overload and a raw-policy overload, at both
/// bus and endpoint scope; the duplicated <c>UseRetry</c> verb is gone.
/// </summary>
public sealed class RetryApiSurfaceTests
{
    private sealed record OrderEvent(string OrderId, int Sequence);

    private sealed class OrderConsumer : IConsumer<OrderEvent>
    {
        public Task Consume(ConsumeContext<OrderEvent> context) => Task.CompletedTask;
    }

    [Fact]
    public void UseRetry_offers_the_fluent_and_raw_overloads_on_both_scopes()
    {
        foreach (var type in new[] { typeof(IStreamBusConfigurator), typeof(IReceiveEndpointConfigurator) })
        {
            var methods = type.GetMethods();
            Assert.Contains(methods, m =>
                m.Name == "UseRetry" &&
                m.GetParameters().Length == 1 &&
                m.GetParameters()[0].ParameterType == typeof(Action<IRetryConfigurator>));
            Assert.Contains(methods, m =>
                m.Name == "UseRetry" &&
                m.GetParameters().Length == 1 &&
                m.GetParameters()[0].ParameterType == typeof(RetryPolicy));
        }
    }

    [Fact]
    public void UseRetry_no_longer_exists_on_either_scope()
    {
        foreach (var type in new[] { typeof(IStreamBusConfigurator), typeof(IReceiveEndpointConfigurator) })
        {
            Assert.DoesNotContain(type.GetMethods(), m => m.Name == "UseMessageRetry");
        }
    }

    [Fact]
    public void UseRetry_fluent_overload_sets_the_schedule_identically_at_bus_and_endpoint_scope()
    {
        var services = new ServiceCollection();
        services.AddStreamBus((_, cfg) =>
        {
            cfg.Host("localhost", "/", h => h.Credentials("app", "secret"));
            cfg.UseRetry(r => r.Immediate(3));
            cfg.ReceiveEndpoint("orders", e =>
            {
                e.Consumer<OrderConsumer>("grp");
                e.UseRetry(r => r.Immediate(5));
            });
        });
        var sp = services.BuildServiceProvider();
        var options = Assert.Single(sp.GetRequiredService<StreamBusConsumerStore>().Registrations).Options;
        Assert.Equal(6, options.Retry.MaxAttempts); // endpoint override wins over the bus schedule
    }

    [Fact]
    public void UseRetry_raw_overload_sets_the_schedule_from_a_policy()
    {
        var services = new ServiceCollection();
        services.AddStreamBus((_, cfg) =>
        {
            cfg.Host("localhost", "/", h => h.Credentials("app", "secret"));
            cfg.UseRetry(new RetryPolicy { MaxAttempts = 2, BaseDelay = TimeSpan.FromMilliseconds(10), MaxJitter = TimeSpan.Zero });
            cfg.ReceiveEndpoint("orders", e => e.Consumer<OrderConsumer>("grp"));
        });
        var sp = services.BuildServiceProvider();
        var options = Assert.Single(sp.GetRequiredService<StreamBusConsumerStore>().Registrations).Options;
        Assert.Equal(2, options.Retry.MaxAttempts);
    }

    [Fact]
    public void Retry_configurator_inherits_the_exception_filter_overloads()
    {
        Assert.Contains(typeof(IRetryConfigurator).GetInterfaces(), i => i == typeof(IExceptionConfigurator));

        var methods = typeof(IExceptionConfigurator).GetMethods();
        foreach (var name in new[] { "Handle", "Ignore" })
        {
            Assert.Contains(methods, m => m.Name == name && HasParamsTypeArray(m));
            Assert.Contains(methods, m => m.Name == name && m.GetGenericArguments().Length == 1 && m.GetParameters().Length == 0);
            Assert.Contains(methods, m => m.Name == name && m.GetGenericArguments().Length == 2 && m.GetParameters().Length == 0);
            Assert.Contains(methods, m => m.Name == name && m.GetGenericArguments().Length == 3 && m.GetParameters().Length == 0);
            Assert.Contains(methods, m => m.Name == name && m.GetGenericArguments().Length == 1 && IsPredicateParameter(m.GetParameters().SingleOrDefault()));
            Assert.Contains(methods, m => m.Name == name && m.GetGenericArguments().Length == 0
                && m.GetParameters().Length == 1 && m.GetParameters()[0].ParameterType == typeof(Func<Exception, bool>));
        }
    }

    private static bool HasParamsTypeArray(MethodInfo method)
    {
        var parameters = method.GetParameters();
        return parameters.Length == 1
            && parameters[0].ParameterType == typeof(Type[])
            && parameters[0].IsDefined(typeof(ParamArrayAttribute));
    }

    private static bool IsPredicateParameter(ParameterInfo? parameter)
        => parameter is not null
            && parameter.ParameterType.IsGenericType
            && parameter.ParameterType.GetGenericTypeDefinition() == typeof(Func<,>);
}
