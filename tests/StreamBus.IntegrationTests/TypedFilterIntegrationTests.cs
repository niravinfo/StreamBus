using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using RabbitMQ.Stream.Client;

namespace StreamBus.IntegrationTests;

internal sealed record TypedFilterOrder(string Id, int Seq);

internal sealed record TypedFilterInvoice(string Id);

internal static class TypedFilterHeaders
{
    public const string Order = "typed-filter-order";
}

internal sealed record TypedFilterObservation(
    string Stage,
    string Stream,
    Type MessageType,
    object Message,
    string? Header);

/// <summary>
/// Shared, singleton-recorded evidence for typed-filter attachment and
/// instance identity. Filters and consumers resolve the same probe from DI.
/// </summary>
internal sealed class TypedFilterProbe
{
    public const string TypedConsume = "typed-consume";
    public const string UniversalConsume = "universal-consume";
    public const string OrderConsumer = "order-consumer";
    public const string InvoiceConsumer = "invoice-consumer";

    private readonly List<TypedFilterObservation> _observations = new();

    public void Record(string stage, string stream, Type messageType, object message, string? header)
    {
        lock (_observations)
        {
            _observations.Add(new TypedFilterObservation(stage, stream, messageType, message, header));
        }
    }

    public IReadOnlyList<TypedFilterObservation> For(string stage)
    {
        lock (_observations)
        {
            return _observations.Where(observation => observation.Stage == stage).ToArray();
        }
    }

    public int Count(string stage) => For(stage).Count;
}

internal sealed class TypedFilterOrderPublishFilter : IStreamPublishFilter<TypedFilterOrder>
{
    public Task OnPublish(
        StreamPublishFilterContext<TypedFilterOrder> context,
        PublishDelegate next,
        CancellationToken ct)
    {
        context.Headers.Set(TypedFilterHeaders.Order, context.Message.Id);
        return next();
    }
}

internal sealed class TypedFilterOrderConsumeFilter(TypedFilterProbe probe)
    : IStreamConsumeFilter<TypedFilterOrder>
{
    public Task OnConsume(
        ConsumeContext<TypedFilterOrder> context,
        ConsumeDelegate next)
    {
        probe.Record(
            TypedFilterProbe.TypedConsume,
            context.Stream,
            typeof(TypedFilterOrder),
            context.Message,
            context.Headers.GetString(TypedFilterHeaders.Order));
        return next();
    }
}

internal sealed class TypedFilterUniversalConsumeFilter(TypedFilterProbe probe) : IStreamConsumeFilter
{
    public Task OnConsume(ConsumeContext context, ConsumeDelegate next)
    {
        probe.Record(
            TypedFilterProbe.UniversalConsume,
            context.Stream,
            context.GetType().IsGenericType
                ? context.GetType().GetGenericArguments()[0]
                : typeof(object),
            context.Body,
            context.Headers.GetString(TypedFilterHeaders.Order));
        return next();
    }
}

internal sealed class TypedFilterOrderConsumer(TypedFilterProbe probe) : IConsumer<TypedFilterOrder>
{
    public Task Consume(ConsumeContext<TypedFilterOrder> context)
    {
        probe.Record(
            TypedFilterProbe.OrderConsumer,
            context.Stream,
            typeof(TypedFilterOrder),
            context.Message,
            context.Headers.GetString(TypedFilterHeaders.Order));
        return Task.CompletedTask;
    }
}

internal sealed class TypedFilterInvoiceConsumer(TypedFilterProbe probe) : IConsumer<TypedFilterInvoice>
{
    public Task Consume(ConsumeContext<TypedFilterInvoice> context)
    {
        probe.Record(
            TypedFilterProbe.InvoiceConsumer,
            context.Stream,
            typeof(TypedFilterInvoice),
            context.Message,
            context.Headers.GetString(TypedFilterHeaders.Order));
        return Task.CompletedTask;
    }
}

public sealed class TypedFilterIntegrationTests : IAsyncDisposable
{
    public TypedFilterIntegrationTests() => IntegrationLane.RequireBroker();

    private static string User() => Environment.GetEnvironmentVariable("STREAMBUS_USER") ?? "app";
    private static string Password() => Environment.GetEnvironmentVariable("STREAMBUS_PASSWORD") ?? "password";
    private readonly List<string> _streams = new();

    private static async Task<StreamSystem> ConnectAdminAsync()
        => await StreamSystem.Create(new StreamSystemConfig
        {
            UserName = User(),
            Password = Password(),
            VirtualHost = "/",
            Endpoints = new List<System.Net.EndPoint> { new System.Net.IPEndPoint(System.Net.IPAddress.Loopback, 5552) },
        });

    private static string Unique(string prefix) => $"{prefix}-{Guid.NewGuid():N}";

    public async ValueTask DisposeAsync()
    {
        var admin = await ConnectAdminAsync();
        try
        {
            foreach (var stream in _streams)
            {
                try { await admin.DeleteStream(stream); } catch { }
            }
        }
        finally { await admin.DisposeAsync(); }
    }

    private sealed class BusInstance : IAsyncDisposable
    {
        public ServiceProvider Provider { get; private set; } = null!;
        public StreamBusHostedService Hosted { get; private set; } = null!;
        public IStreamBus Bus { get; private set; } = null!;

        public static async Task<BusInstance> StartAsync(
            Action<ServiceCollection>? configure = null,
            Action<IStreamBusConfigurator>? busConfigure = null)
        {
            var services = new ServiceCollection();
            services.AddLogging();
            services.AddStreamBus((_, cfg) =>
            {
                cfg.Host("localhost", "/", h => h.Credentials(User(), Password()));
                busConfigure?.Invoke(cfg);
            });
            configure?.Invoke(services);
            var sp = services.BuildServiceProvider();
            var hosted = sp.GetServices<IHostedService>().OfType<StreamBusHostedService>().Single();
            await hosted.StartAsync(TestContext.Current.CancellationToken);
            var bus = sp.GetRequiredService<IStreamBus>();
            Assert.Equal(BusReadiness.Ready, await bus.WaitUntilReadyAsync(ReadinessPolicy.UpTo(TimeSpan.FromSeconds(30)), TestContext.Current.CancellationToken));
            return new BusInstance { Provider = sp, Hosted = hosted, Bus = bus };
        }

        public async ValueTask DisposeAsync()
        {
            try { await Hosted.StopAsync(TestContext.Current.CancellationToken); }
            finally { await Provider.DisposeAsync(); }
        }
    }

    private static async Task WaitForAsync(Func<bool> done, TimeSpan timeout, string what)
    {
        var deadline = DateTime.UtcNow + timeout;
        while (!done())
        {
            if (DateTime.UtcNow >= deadline) Assert.Fail($"Timed out waiting for {what}.");
            await Task.Delay(100, TestContext.Current.CancellationToken);
        }
    }

    [Fact]
    public async Task Typed_publish_filter_header_reaches_typed_consume_filter_and_consumer()
    {
        var ct = TestContext.Current.CancellationToken;
        var stream = Unique("typed-filter-round-trip");
        _streams.Add(stream);
        await using var admin = await ConnectAdminAsync();
        await admin.CreateStream(new StreamSpec(stream));

        var probe = new TypedFilterProbe();
        await using var inst = await BusInstance.StartAsync(
            s => s.AddSingleton(probe),
            cfg =>
            {
                cfg.Publish<TypedFilterOrder>(stream);
                cfg.UsePublishFilter<TypedFilterOrderPublishFilter>();
                cfg.UseConsumeFilter<TypedFilterOrderConsumeFilter>();
                cfg.ReceiveEndpoint(stream, e => e.Consumer<TypedFilterOrderConsumer>("typed-filter-group"));
            });

        await inst.Bus.PublishAsync(stream, new TypedFilterOrder("order-1", 1), ct);
        await WaitForAsync(
            () => probe.Count(TypedFilterProbe.OrderConsumer) >= 1,
            TimeSpan.FromSeconds(30),
            "typed order delivery");

        var filterSaw = Assert.Single(probe.For(TypedFilterProbe.TypedConsume));
        var consumerSaw = Assert.Single(probe.For(TypedFilterProbe.OrderConsumer));

        Assert.Equal(stream, filterSaw.Stream);
        Assert.Equal(typeof(TypedFilterOrder), filterSaw.MessageType);
        var deserialized = Assert.IsType<TypedFilterOrder>(filterSaw.Message);
        Assert.Equal("order-1", deserialized.Id);
        Assert.Equal(1, deserialized.Seq);

        // The publish filter's header crossed the broker and is visible to both
        // the typed consume filter and the typed consumer.
        Assert.Equal("order-1", filterSaw.Header);
        Assert.Equal("order-1", consumerSaw.Header);

        // Eager deserialization: the consumer handles the very instance the
        // typed filter observed, not a second deserialized copy.
        Assert.Same(filterSaw.Message, consumerSaw.Message);
        Assert.Equal(stream, consumerSaw.Stream);
    }

    [Fact]
    public async Task Bus_typed_filter_does_not_run_for_another_endpoint_message_type()
    {
        var ct = TestContext.Current.CancellationToken;
        var orders = Unique("typed-filter-orders");
        var invoices = Unique("typed-filter-invoices");
        _streams.Add(orders);
        _streams.Add(invoices);
        await using var admin = await ConnectAdminAsync();
        await admin.CreateStream(new StreamSpec(orders));
        await admin.CreateStream(new StreamSpec(invoices));

        var probe = new TypedFilterProbe();
        await using var inst = await BusInstance.StartAsync(
            s => s.AddSingleton(probe),
            cfg =>
            {
                cfg.Publish<TypedFilterOrder>(orders);
                cfg.Publish<TypedFilterInvoice>(invoices);
                cfg.UseConsumeFilter<TypedFilterOrderConsumeFilter>();
                cfg.UseConsumeFilter<TypedFilterUniversalConsumeFilter>();
                cfg.ReceiveEndpoint(orders, e => e.Consumer<TypedFilterOrderConsumer>("typed-filter-orders-group"));
                cfg.ReceiveEndpoint(invoices, e => e.Consumer<TypedFilterInvoiceConsumer>("typed-filter-invoices-group"));
            });

        await inst.Bus.PublishAsync(orders, new TypedFilterOrder("order-1", 1), ct);
        await inst.Bus.PublishAsync(invoices, new TypedFilterInvoice("invoice-1"), ct);
        await WaitForAsync(
            () => probe.Count(TypedFilterProbe.OrderConsumer) >= 1
                && probe.Count(TypedFilterProbe.InvoiceConsumer) >= 1
                && probe.Count(TypedFilterProbe.UniversalConsume) >= 2,
            TimeSpan.FromSeconds(30),
            "order and invoice delivery");

        // The bus-wide typed filter only attaches to the matching endpoint; the
        // invoice consumer's receipt proves it never ran on the invoice endpoint.
        var typed = Assert.Single(probe.For(TypedFilterProbe.TypedConsume));
        Assert.Equal(orders, typed.Stream);
        Assert.IsType<TypedFilterOrder>(typed.Message);
        Assert.Single(probe.For(TypedFilterProbe.InvoiceConsumer));

        // The universal filter stays universal: one run per endpoint message.
        var universal = probe.For(TypedFilterProbe.UniversalConsume);
        Assert.Equal(2, universal.Count);
        Assert.Equal(
            new[] { orders, invoices }.OrderBy(name => name, StringComparer.Ordinal).ToList(),
            universal.Select(observation => observation.Stream).OrderBy(name => name, StringComparer.Ordinal).ToList());
        Assert.Equal(
            new[] { typeof(TypedFilterInvoice), typeof(TypedFilterOrder) }.OrderBy(type => type.Name, StringComparer.Ordinal).ToList(),
            universal.Select(observation => observation.MessageType).OrderBy(type => type.Name, StringComparer.Ordinal).ToList());
    }

    [Fact]
    public async Task Per_endpoint_typed_filter_only_affects_its_endpoint()
    {
        var ct = TestContext.Current.CancellationToken;
        var filteredStream = Unique("typed-filter-scoped-a");
        var plainStream = Unique("typed-filter-scoped-b");
        _streams.Add(filteredStream);
        _streams.Add(plainStream);
        await using var admin = await ConnectAdminAsync();
        await admin.CreateStream(new StreamSpec(filteredStream));
        await admin.CreateStream(new StreamSpec(plainStream));

        var probe = new TypedFilterProbe();
        await using var inst = await BusInstance.StartAsync(
            s => s.AddSingleton(probe),
            cfg =>
            {
                cfg.Publish<TypedFilterOrder>(filteredStream);
                cfg.ReceiveEndpoint(filteredStream, e =>
                {
                    e.Consumer<TypedFilterOrderConsumer>("typed-filter-scoped-a-group");
                    e.UseConsumeFilter<TypedFilterOrderConsumeFilter>();
                });
                cfg.ReceiveEndpoint(plainStream, e => e.Consumer<TypedFilterOrderConsumer>("typed-filter-scoped-b-group"));
            });

        await inst.Bus.PublishAsync(filteredStream, new TypedFilterOrder("filtered", 1), ct);
        await inst.Bus.PublishAsync(plainStream, new TypedFilterOrder("plain", 2), ct);
        await WaitForAsync(
            () => probe.Count(TypedFilterProbe.OrderConsumer) >= 2,
            TimeSpan.FromSeconds(30),
            "both scoped endpoints to deliver");

        // Only the endpoint that registered the typed filter ran it.
        var typed = Assert.Single(probe.For(TypedFilterProbe.TypedConsume));
        Assert.Equal(filteredStream, typed.Stream);
        Assert.Equal("filtered", Assert.IsType<TypedFilterOrder>(typed.Message).Id);

        // Both endpoints still consumed their own message.
        var consumers = probe.For(TypedFilterProbe.OrderConsumer);
        Assert.Equal(2, consumers.Count);
        Assert.Equal(
            new[] { filteredStream, plainStream }.OrderBy(name => name, StringComparer.Ordinal).ToList(),
            consumers.Select(observation => observation.Stream).OrderBy(name => name, StringComparer.Ordinal).ToList());
        Assert.Equal(
            new[] { "filtered", "plain" }.OrderBy(id => id, StringComparer.Ordinal).ToList(),
            consumers.Select(observation => Assert.IsType<TypedFilterOrder>(observation.Message).Id)
                .OrderBy(id => id, StringComparer.Ordinal).ToList());
    }
}
