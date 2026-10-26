using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging.Abstractions;

namespace StreamBus.UnitTests;

public sealed class TypedConsumerTests
{
    private sealed record OrderEvent(string OrderId, int Seq);

    private sealed class OrderConsumer : IConsumer<OrderEvent>
    {
        public readonly List<OrderEvent> Got = new();
        public Task Consume(ConsumeContext<OrderEvent> context, CancellationToken ct)
        {
            lock (Got) Got.Add(context.Message);
            return Task.CompletedTask;
        }
    }

    [Fact]
    public void Serializer_round_trips()
    {
        var serializer = SystemTextJsonStreamSerializer.Default;
        var original = new OrderEvent("o-1", 7);
        var back = serializer.Deserialize<OrderEvent>(serializer.Serialize(original));
        Assert.Equal(original, back);
    }

    [Fact]
    public void Serializer_throws_on_malformed()
    {
        Assert.ThrowsAny<System.Text.Json.JsonException>(() =>
            SystemTextJsonStreamSerializer.Default.Deserialize<OrderEvent>(System.Text.Encoding.UTF8.GetBytes("{bad json")));
    }

    [Fact]
    public void Store_rejects_duplicates_but_allows_variants()
    {
        var store = new StreamBusConsumerStore();
        var options = new ConsumeOptions { Reference = "grp" };
        store.Add(typeof(DefaultBus), typeof(OrderEvent), typeof(OrderConsumer), "orders", options);
        Assert.Throws<StreamBusConfigurationException>(() =>
            store.Add(typeof(DefaultBus), typeof(OrderEvent), typeof(OrderConsumer), "orders", options));
        store.Add(typeof(DefaultBus), typeof(OrderEvent), typeof(OrderConsumer), "orders", new ConsumeOptions { Reference = "other-grp" });
        store.Add(typeof(DefaultBus), typeof(OrderEvent), typeof(OrderConsumer), "other-stream", options);
        store.Add(typeof(DefaultBus), typeof(OrderEvent), typeof(OrderConsumerTests_Other), "orders", options);
        Assert.Equal(4, store.Registrations.Count);
    }

    private sealed class OrderConsumerTests_Other : IConsumer<OrderEvent>
    {
        public Task Consume(ConsumeContext<OrderEvent> context, CancellationToken ct) => Task.CompletedTask;
    }

    [Fact]
    public void Store_rejects_missing_reference()
    {
        var store = new StreamBusConsumerStore();
        Assert.Throws<StreamBusConfigurationException>(() =>
            store.Add(typeof(DefaultBus), typeof(OrderEvent), typeof(OrderConsumer), "orders", new ConsumeOptions()));
    }

    [Fact]
    public void Explicit_service_lifetime_wins()
    {
        var services = new ServiceCollection();
        var singleton = new OrderConsumer();
        services.AddSingleton<IConsumer<OrderEvent>>(singleton);
        services.AddSingleton(singleton);
        services.AddStreamBus((_, cfg) =>
        {
            cfg.Host("localhost", "/", h => h.Credentials("app", "secret"));
            cfg.ReceiveEndpoint("orders", e => e.Consumer<OrderConsumer>("test-group"));
        });
        var sp = services.BuildServiceProvider();
        Assert.Same(singleton, sp.GetRequiredService<OrderConsumer>());
    }

    private sealed class FlakyConsumer : IConsumer<OrderEvent>
    {
        public static int Constructions;
        private readonly Dictionary<int, int> _attempts;
        public FlakyConsumer(Dictionary<int, int> attempts)
        {
            Interlocked.Increment(ref Constructions);
            _attempts = attempts;
        }
        public Task Consume(ConsumeContext<OrderEvent> context, CancellationToken ct)
        {
            var seq = context.Message.Seq;
            int attempt;
            lock (_attempts)
            {
                attempt = _attempts.TryGetValue(seq, out var n) ? n + 1 : 1;
                _attempts[seq] = attempt;
            }
            if (seq == 2 && attempt < 3) throw new InvalidOperationException("flaky 2");
            return Task.CompletedTask;
        }
    }

    private static ConsumeContext RawOf(string body) => new()
    {
        Stream = "orders",
        Partition = 0,
        Offset = 0,
        RoutingValue = null,
        Body = System.Text.Encoding.UTF8.GetBytes(body),
        Metadata = new MessageMetadata(),
        CancellationToken = TestContext.Current.CancellationToken,
    };

    [Fact]
    public async Task Attempts_resolve_fresh_and_poison_translates()
    {
        var services = new ServiceCollection();
        var attempts = new Dictionary<int, int>();
        services.AddSingleton(attempts);
        services.AddTransient<FlakyConsumer>();
        await using var sp = services.BuildServiceProvider();
        var factory = sp.GetRequiredService<IServiceScopeFactory>();
        FlakyConsumer.Constructions = 0;
        var handler = TypedConsumerHandler.Create(typeof(OrderEvent), typeof(FlakyConsumer), factory, SystemTextJsonStreamSerializer.Default);
        var good = "{\"OrderId\":\"k\",\"Seq\":2}";
        await Assert.ThrowsAsync<InvalidOperationException>(() => handler(RawOf(good)));
        await Assert.ThrowsAsync<InvalidOperationException>(() => handler(RawOf(good)));
        await handler(RawOf(good));
        Assert.Equal(3, FlakyConsumer.Constructions);
        Assert.Equal(3, attempts[2]);
        var poison = await Assert.ThrowsAsync<StreamBusDeserializationException>(() => handler(RawOf("{bad json}")));
        Assert.Equal("orders", poison.Stream);
        Assert.Equal(typeof(OrderEvent), poison.MessageType);
        var nil = await Assert.ThrowsAsync<StreamBusDeserializationException>(() => handler(RawOf("null")));
        Assert.Contains("null", nil.InnerException?.Message ?? nil.Message, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void Startup_transient_matrix()
    {
        Assert.True(StreamBusHostedService.IsStartupTransient(new StreamBusNotReadyException("x")));
        Assert.True(StreamBusHostedService.IsStartupTransient(new StreamBusStreamNotFoundException("x")));
        Assert.False(StreamBusHostedService.IsStartupTransient(new StreamBusConfigurationException("x")));
        Assert.False(StreamBusHostedService.IsStartupTransient(new StreamBusFatalException("x")));
        Assert.False(StreamBusHostedService.IsStartupTransient(new StreamBusPublishException("x")));
        Assert.False(StreamBusHostedService.IsStartupTransient(new InvalidOperationException("x")));
    }

    [Fact]
    public void Missing_scope_factory_fails_fast()
    {
        var store = new StreamBusConfigStore();
        store.Add(typeof(DefaultBus), new StreamBusOptions { Vhost = "/", UserName = "a", Password = "b" });
        var consumers = new StreamBusConsumerStore();
        consumers.Add(typeof(DefaultBus), typeof(OrderEvent), typeof(OrderConsumer), "orders", new ConsumeOptions { Reference = "test-group" });
        var registry = new StreamBusRegistry(
            _ => throw new InvalidOperationException("no resolve"),
            _ => throw new InvalidOperationException("no supervisor"),
            null);
        var ex = Assert.Throws<ArgumentNullException>(() => new StreamBusHostedService(
            store,
            registry,
            NullLogger<StreamBusHostedService>.Instance,
            null!,
            consumers,
            new StreamBusEndpointStore(),
            new StreamBusPublishStore(),
            new StreamBusHostingStore(new StreamBusHostingOptions())));
        Assert.Equal("scopeFactory", ex.ParamName);
    }

    [Fact]
    public void Non_positive_timeout_rejected_at_config()
    {
        Assert.Throws<StreamBusConfigurationException>(() => new StreamBusHostingOptionsBuilder().BlockStartupUntilReady(TimeSpan.Zero));
        Assert.Throws<StreamBusConfigurationException>(() => new StreamBusHostingOptionsBuilder().BlockStartupUntilReady(TimeSpan.FromSeconds(-1)));
    }
}
