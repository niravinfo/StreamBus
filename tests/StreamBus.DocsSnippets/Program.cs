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
    /// Guides/Consume events — the page's two endpoint registrations: the
    /// billing endpoint (group "billing") and the audit endpoint (group
    /// "audit") on the SAME "orders" stream, proving the two-endpoint
    /// pattern validates. The consumer classes and the ConsumeContext tour
    /// snippets (coordinates, MessageId dedup, typed header getters,
    /// Services resolve) live in <see cref="ConsumeEventsSample"/> so their
    /// syntax compiles; only the registration is exercised (needs no broker).
    /// The start_offset/StartOffset FAQ prose has no snippet on the page.
    /// </summary>
    private static void VerifyConsumeEvents() => RegisterAndValidate(services =>
    {
        services.AddScoped<ConsumeEventsSample.IInvoicer, ConsumeEventsSample.Invoicer>();

        services.AddStreamBus((_, cfg) =>
        {
            cfg.Host("localhost", "/", h => h.Credentials("guest", "guest"));

            cfg.Publish<ConsumeEventsSample.OrderPlaced>("orders");

            // Billing's endpoint, from the page's registration block.
            cfg.ReceiveEndpoint("orders", e =>
            {
                e.Consumer<ConsumeEventsSample.OrderConsumer>("billing");
            });

            // Audit's endpoint from "A second team wants the same events":
            // same stream, different group — an independent cursor.
            cfg.ReceiveEndpoint("orders", e =>
            {
                e.Consumer<ConsumeEventsSample.AuditConsumer>("audit");
            });
        });
    });

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

/// <summary>
/// Guides/Handle failures — the page's registration snippets: the bounded
/// schedules (Exponential on billing, Immediate on payments), the explicit
/// stream dead-letter destination with MaxAge, and the OnExhausted
/// callback publishing the raw body to its target stream. The commented
/// queue alternative (QuorumQueue + ConfirmTimeout) on the page is
/// replaced here with the stream call the later sections build on — the
/// page presents the two as either/or because a second DeadLetterTo* call
/// on one endpoint throws. The ErrorEnvelope drain loop and the
/// PaymentsConsumer live in <see cref="HandleFailuresSample"/> so their
/// syntax compiles; consuming needs a live broker, so only registration is
/// exercised.
/// </summary>
private static void VerifyHandleFailures() => RegisterAndValidate(services =>
    services.AddStreamBus((_, cfg) =>
    {
        cfg.Host("localhost", "/", h => h.Credentials("guest", "guest"));

        cfg.Publish<HandleFailuresSample.OrderPlaced>("orders");
        cfg.Publish<HandleFailuresSample.PaymentFailed>("payments-dlq");

        // From "Bound the schedule, name the destination": bounded schedule,
        // derived hand-off (no destination declared).
        cfg.ReceiveEndpoint("orders", e =>
        {
            e.Consumer<HandleFailuresSample.OrderConsumer>("billing");
            e.UseRetry(r => r.Exponential(4,
                TimeSpan.FromSeconds(1), TimeSpan.FromSeconds(30)));
        });

        // The explicit-destination form: named error stream with MaxAge. The
        // page shows this as an alternative shape of the SAME billing
        // endpoint; the harness registers it under its own group so both
        // shapes exercise registration validation in one run.
        cfg.ReceiveEndpoint("orders", e =>
        {
            e.Consumer<HandleFailuresSample.OrderConsumer>("billing-explicit");
            e.UseRetry(r => r.Immediate(5));
            e.DeadLetterToStream("orders-errors", s => s.MaxAge(TimeSpan.FromDays(30)));
        });

        // From "When attempts run out": OnExhausted owns the outcome.
        cfg.ReceiveEndpoint("payments", e =>
        {
            e.Consumer<HandleFailuresSample.PaymentsConsumer>("payments-ops");
            e.UseRetry(r => r.Immediate(5));
            e.OnExhausted(async (ctx, ct) =>
            {
                var bus = ctx.GetService<IStreamBus>();
                await bus.PublishRawAsync("payments-dlq", ctx.Body, new PublishOptions(), ct);
            });
        });
    }));

/// <summary>
/// Guides/Scale out — the page's single code block: the billing endpoint
/// registration that deploys byte-identical in every instance (same
/// stream, same consumer class, same group). That is the whole page-point:
/// scale-out changes topology, not API, so there is nothing else to
/// register — the SAC warning knob (SingleActiveConsumer) is deliberately
/// NOT exercised here, because the page's doctrine is to leave it at its
/// default (true), which this registration does implicitly.
/// </summary>
private static void VerifyScaleOut() => RegisterAndValidate(services =>
    services.AddStreamBus((_, cfg) =>
    {
        cfg.Host("localhost", "/", h => h.Credentials("guest", "guest"));

        cfg.Publish<ScaleOutSample.OrderPlaced>("orders");

        // From "Nothing in your code changes" — identical in every
        // instance; the group "billing" is the consumer's identity.
        cfg.ReceiveEndpoint("orders", e =>
        {
            e.Consumer<ScaleOutSample.OrderConsumer>("billing");
        });
    }));
}

/// <summary>
/// Types from the Guides/Get started page's Program.cs, isolated in their own
/// namespace so the snippet shape matches the page. Skips the logging lines
/// (not part of registration) and never connects — the harness only builds.
/// </summary>
internal static class ScaleOutSample
{
    public sealed record OrderPlaced(string OrderId);

    public sealed class OrderConsumer(ILogger<OrderConsumer> log) : IConsumer<OrderPlaced>
    {
        public Task Consume(ConsumeContext<OrderPlaced> context)
        {
            log.LogInformation("Billing order {OrderId} at offset {Offset}",
                context.Message.OrderId, context.Offset);
            return Task.CompletedTask;
        }
    }
}

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

internal static class ConsumeEventsSample
{
    public sealed record OrderPlaced(string OrderId);

    public sealed record ServiceLevel(string Tier, TimeSpan Target);

    public interface IInvoicer
    {
        void Invoice(OrderPlaced order);
    }

    public sealed class Invoicer : IInvoicer
    {
        public void Invoice(OrderPlaced order) { }
    }

    private static readonly HashSet<Guid> _seen = new();

    public sealed class OrderConsumer(ILogger<OrderConsumer> log) : IConsumer<OrderPlaced>
    {
        public Task Consume(ConsumeContext<OrderPlaced> context)
        {
            log.LogInformation("Billing order {OrderId} at offset {Offset}",
                context.Message.OrderId, context.Offset);
            return Task.CompletedTask;
        }
    }

    // Compile-only stand-ins for the page's context-tour snippets; never
    // executed by the harness (consuming needs a live broker).
    internal static void Coordinates(ConsumeContext<OrderPlaced> context, ILogger log)
        => log.LogInformation("Stream {Stream} partition {Partition} offset {Offset}",
            context.Stream, context.Partition, context.Offset);

    internal static Task DedupById(ConsumeContext<OrderPlaced> context)
    {
        var firstTime = _seen.Add(context.MessageId ?? Guid.Empty);
        if (!firstTime)
        {
            return Task.CompletedTask; // duplicate delivery — skip
        }
        return Task.CompletedTask;
    }

    internal sealed class TenantAwareConsumer : IConsumer<OrderPlaced>
    {
        public Task Consume(ConsumeContext<OrderPlaced> context)
        {
            string? tenant = context.Headers.GetString("x-tenant");
            int? priority = context.Headers.GetInt("x-priority");
            var sla = context.Headers.GetJson<ServiceLevel>("x-sla");
            return Task.CompletedTask;
        }
    }

    internal static void ResolveScoped(ConsumeContext<OrderPlaced> context)
    {
        var invoicer = context.Services.GetRequiredService<IInvoicer>();
        invoicer.Invoice(context.Message);
    }

    public sealed class AuditConsumer(ILogger<AuditConsumer> log) : IConsumer<OrderPlaced>
    {
        public Task Consume(ConsumeContext<OrderPlaced> context)
        {
            log.LogInformation("Audit order {OrderId} at offset {Offset}",
                context.Message.OrderId, context.Offset);
            return Task.CompletedTask;
        }
    }
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

/// <summary>
/// Types and compile-only call shapes from Guides/Handle failures. The
/// drain loop reproduces the page's ErrorEnvelope reading snippet verbatim
/// in form; it is never invoked (consuming needs a live broker). The
/// dead-letter queue alternative shown on the page is compile-checked
/// fragment-free: its signature lives in
/// <c>DeadLetterToQueue(name, DeadLetterQueueType, q => q.ConfirmTimeout(...))</c>
/// and is exercised as the commented either/or on the page.
/// </summary>
internal static class HandleFailuresSample
{
    public sealed record OrderPlaced(string OrderId);

    public sealed record PaymentFailed(string PaymentId, string Reason);

    public sealed class OrderConsumer(ILogger<OrderConsumer> log) : IConsumer<OrderPlaced>
    {
        public Task Consume(ConsumeContext<OrderPlaced> context)
        {
            log.LogInformation("Billing order {OrderId} at offset {Offset}",
                context.Message.OrderId, context.Offset);
            return Task.CompletedTask;
        }
    }

    public sealed class PaymentsConsumer : IConsumer<PaymentFailed>
    {
        public Task Consume(ConsumeContext<PaymentFailed> context) => Task.CompletedTask;
    }

    // Compile-only stand-ins; never invoked.
    private static IStreamBus bus = null!;
    private static CancellationToken ct = CancellationToken.None;

    internal static async Task DrainErrorStream(CancellationToken cancellationToken)
    {
        await using var inspector = await bus.ConsumeAsync(
            "orders-errors",
            new ConsumeOptions { ConsumerGroup = "error-inspector", StartOffset = StartOffset.AlwaysFirst },
            ctx =>
            {
                if (ErrorEnvelope.TryParse("orders-errors", ctx.Body.ToArray(), ctx.Headers.All, out var envelope)
                    && envelope is not null)
                {
                    Console.WriteLine(
                        $"{envelope.ExceptionType} at {envelope.SourceStream}/{envelope.SourceOffset}: " +
                        $"{envelope.ExceptionMessage}");
                }
                return Task.CompletedTask;
            },
            cancellationToken);
    }
}
