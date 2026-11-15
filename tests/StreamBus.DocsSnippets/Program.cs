using System.Reflection;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
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
    /// Guides/Get started — the page's registration block (Host + Publish +
    /// ReceiveEndpoint + UseRetry) plus the blocking-start host options.
    /// </summary>
    /// <remarks>
    /// The <c>OrderPlaced</c>, <c>OrderConsumer</c>, and <c>OrderPublisher</c>
    /// classes from the page's full Program.cs are reproduced here in
    /// <see cref="GetStartedSample"/>; registration-time validation covers
    /// the consumer binding, group requirement, and retry policy.
    /// </remarks>
    private static void VerifyGetStarted() => RegisterAndValidate(services =>
    {
        services.AddStreamBus((_, cfg) =>
        {
            cfg.Host("localhost", "/", h => h.Credentials("guest", "guest"));

            cfg.Publish<GetStartedSample.OrderPlaced>("orders");

            cfg.ReceiveEndpoint("orders", e =>
            {
                e.Consumer<GetStartedSample.OrderConsumer>("billing");
                e.UseRetry(r => r.Immediate(3));
            });
        });

        services.AddOptions<StreamBusHostOptions>().Configure(o =>
        {
            o.WaitUntilStarted = true;
            o.StartTimeout = TimeSpan.FromSeconds(30);
        });

        services.AddHostedService<GetStartedSample.OrderPublisher>();
    });

    /// <summary>
    /// Guides/Publish an event — the declare-route registration block.
    /// The publish call snippets (basic pair, Properties/Headers,
    /// Readiness/ConfirmTimeout budgets, PublishRawAsync, explicit-target
    /// PublishAsync) are reproduced in <see cref="PublishAnEventSample"/>
    /// so their syntax compiles, but are NOT executed: publishing needs a
    /// live broker connection, which the harness never starts. The
    /// explicit-target stream ("orders-audit") is intentionally NOT
    /// registered (the page's point: it must be declared, not
    /// auto-created), so only the page's registration block is here.
    /// </summary>
    private static void VerifyPublishAnEvent() => RegisterAndValidate(services =>
        services.AddStreamBus((_, cfg) =>
        {
            cfg.Host("localhost", "/", h => h.Credentials("guest", "guest"));

            cfg.Publish<PublishAnEventSample.OrderPlaced>("orders");
        }));

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

/// <summary>
/// Types from the Guides/Get started page's Program.cs, isolated in their own
/// namespace so the snippet shape matches the page. Skips the logging lines
/// (not part of registration) and never connects — the harness only builds.
/// </summary>
internal static class GetStartedSample
{
    public sealed record OrderPlaced(string OrderId);

    public sealed class OrderConsumer(ILogger<OrderConsumer> log) : IConsumer<OrderPlaced>
    {
        public Task Consume(ConsumeContext<OrderPlaced> context)
        {
            log.LogInformation("Order {OrderId} at offset {Offset}", context.Message.OrderId, context.Offset);
            return Task.CompletedTask;
        }
    }

    public sealed class OrderPublisher(IStreamBus bus, ILogger<OrderPublisher> log)
        : BackgroundService
    {
        private readonly IStreamBus _bus = bus;
        private readonly ILogger<OrderPublisher> _log = log;

        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {
            while (!stoppingToken.IsCancellationRequested)
            {
                await _bus.PublishAsync(new OrderPlaced(OrderId: Guid.NewGuid().ToString("N")), stoppingToken);
                await Task.Delay(TimeSpan.FromSeconds(1), stoppingToken);
            }
        }
    }
}

/// <summary>
/// Types and compile-only call shapes from Guides/Publish an event. The
/// methods reproduce the page's publish-call snippets verbatim in form so
/// their syntax and option/property names compile against src/StreamBus, but
/// nothing is invoked — publishing needs a live broker.
/// </summary>
internal static class PublishAnEventSample
{
    public sealed record OrderPlaced(string OrderId);

    // Compile-only stand-ins; never invoked.
    private static IStreamBus bus = null!;
    private static CancellationToken ct = CancellationToken.None;
    private static Guid correlationId = Guid.NewGuid();
    private static ReadOnlyMemory<byte> preSerializedBytes = ReadOnlyMemory<byte>.Empty;

    internal static void BasicPair()
        => bus.PublishAsync(new OrderPlaced(OrderId: "o-1"), ct);

    internal static void PropertiesAndHeaders()
        => bus.PublishAsync(new OrderPlaced(OrderId: "o-1"), new PublishOptions
        {
            Properties = new StreamMessageProperties
            {
                CorrelationId = correlationId,
                Headers = new Dictionary<string, object?>
                {
                    ["x-tenant"] = "acme",
                    ["x-retry-hint"] = 3,
                },
            },
        }, ct);

    internal static void Budgets()
        => bus.PublishAsync(new OrderPlaced(OrderId: "o-1"), new PublishOptions
        {
            Readiness = ReadinessPolicy.UpTo(TimeSpan.FromSeconds(10)),
            ConfirmTimeout = TimeSpan.FromSeconds(5),
        }, ct);

    internal static void RawPublish()
        => bus.PublishRawAsync("orders", preSerializedBytes, new PublishOptions(), ct);

    internal static void ExplicitTarget()
        => bus.PublishAsync("orders-audit", new OrderPlaced(OrderId: "o-1"), ct);
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
