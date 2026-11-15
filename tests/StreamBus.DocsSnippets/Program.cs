using System.Reflection;
using Microsoft.Extensions.DependencyInjection;
using StreamBus;

namespace StreamBus.DocsSnippets;

/// <summary>
/// Harness that verifies every C# snippet shown in the docs compiles and
/// passes <c>AddStreamBus</c> registration-time validation.
/// </summary>
/// <remarks>
/// Each documentation page adds a <c>Verify&lt;Page&gt;()</c> method (e.g.
/// <see cref="VerifyHarnessSmoke"/>); <see cref="Main"/> discovers and runs
/// every <c>Verify*</c> method automatically. A failed registration prints
/// <c>FAILED: &lt;page&gt; : &lt;message&gt;</c> and exits non-zero; success
/// prints <c>ALL SNIPPET REGISTRATIONS VALID</c>.
/// </remarks>
internal static class Program
{
    internal static int Main()
    {
        var failures = 0;

        foreach (var verify in typeof(Program).GetMethods(BindingFlags.Static | BindingFlags.Public | BindingFlags.NonPublic)
            .Where(m => m.Name.StartsWith("Verify", StringComparison.Ordinal)
                && m.ReturnType == typeof(void)
                && m.GetParameters().Length == 0)
            .OrderBy(m => m.Name, StringComparer.Ordinal))
        {
            try
            {
                verify.Invoke(null, null);
            }
            catch (TargetInvocationException ex)
            {
                failures++;
                Console.Error.WriteLine($"FAILED: {verify.Name} : {ex.InnerException?.Message ?? ex.Message}");
            }
        }

        if (failures > 0)
            return 1;

        Console.WriteLine("ALL SNIPPET REGISTRATIONS VALID");
        return 0;
    }

    /// <summary>
    /// Shared helper for page verification methods: applies <paramref name="configure"/>
    /// to a fresh <see cref="IServiceCollection"/>, then builds the provider so
    /// registration-time validation and container construction both run. No
    /// broker connection is made (buses connect on host start, which we never trigger).
    /// </summary>
    internal static void RegisterAndValidate(Action<IServiceCollection> configure)
    {
        var services = new ServiceCollection();
        configure(services);
        using var provider = services.BuildServiceProvider();
    }

    // Listed first alphabetically by design: proves the harness pattern works.
    private static void VerifyHarnessSmoke() => RegisterAndValidate(services =>
        services.AddStreamBus((_, cfg) =>
            cfg.Host("localhost", "/", h => h.Credentials("guest", "guest"))));

    /// <summary>
    /// Concepts/Middleware — the composed registration sample that teaches
    /// scoping and ordering: bus-wide publish/consume filters plus one
    /// endpoint-scoped consume filter. The filter classes and message types
    /// live in this namespace so the snippet shape matches the page while
    /// staying isolated from other pages' samples.
    /// </summary>
    private static void VerifyMiddleware() => RegisterAndValidate(services => services
        .AddSingleton<MiddlewareSample.TenantFilter>()
        .AddSingleton<MiddlewareSample.MetricsFilter>()
        .AddSingleton<MiddlewareSample.EnrichmentFilter>()
        .AddStreamBus((_, cfg) =>
        {
            cfg.Host("localhost", "/", h => h.Credentials("guest", "guest"));

            cfg.UsePublishFilter<MiddlewareSample.TenantFilter>();
            cfg.UseConsumeFilter<MiddlewareSample.MetricsFilter>();

            cfg.ReceiveEndpoint("orders", e =>
            {
                e.Consumer<MiddlewareSample.OrderConsumer>("billing");
                e.UseConsumeFilter<MiddlewareSample.EnrichmentFilter>();
            });
        }));
}

internal static class MiddlewareSample
{
    public sealed record OrderPlaced(Guid OrderId);

    public sealed class TenantScope
    {
        public string Tenant { get; set; } = "default";
    }

    public sealed class TenantFilter(TenantScope ambient) : IStreamPublishFilter
    {
        public Task OnPublish(PublishContext context, PublishNext next)
        {
            context.Headers.Set("x-tenant", ambient.Tenant);
            return next();
        }
    }

    public sealed class MetricsFilter : IStreamConsumeFilter
    {
        public Task OnConsume(ConsumeContext context, ConsumeNext next) => next();
    }

    public sealed class EnrichmentFilter : IStreamConsumeFilter
    {
        public Task OnConsume(ConsumeContext context, ConsumeNext next) => next();
    }

    public sealed class OrderConsumer : IConsumer<OrderPlaced>
    {
        public Task Consume(ConsumeContext<OrderPlaced> context) => Task.CompletedTask;
    }
}
