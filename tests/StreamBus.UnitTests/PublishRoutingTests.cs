using Microsoft.Extensions.DependencyInjection;
using RabbitMQ.Stream.Client;

namespace StreamBus.UnitTests;

public sealed class PublishRoutingTests
{
    private sealed record OrderEvent(string OrderId, int Sequence);
    private sealed record InvoiceEvent(string InvoiceId);
    private sealed class OrdersBus;

    private sealed class CapturingHandle : IStreamProducerHandle
    {
        public readonly List<(byte[] Body, string? RoutingKey)> Sent = new();
        public Task SendAsync(ReadOnlyMemory<byte> body, string? routingKey, CancellationToken ct)
        {
            lock (Sent) Sent.Add((body.ToArray(), routingKey));
            return Task.CompletedTask;
        }
        public Task SendEnvelopeAsync(ReadOnlyMemory<byte> body, string? routingKey, IReadOnlyDictionary<string, object?> applicationProperties, string? messageId, string? correlationId, CancellationToken ct)
            => throw new NotImplementedException("PublishRoutingTests never sends envelopes.");
        public Task PublishReplayAsync(ReadOnlyMemory<byte> body, ulong publishingId, CancellationToken ct)
            => throw new NotImplementedException("PublishRoutingTests never replays.");
        public Task PublishResultAsync(ReadOnlyMemory<byte> body, ulong publishingId, string? routingKey, CancellationToken ct)
            => throw new NotImplementedException("PublishRoutingTests never publishes results.");
        public ValueTask DisposeAsync() => ValueTask.CompletedTask;
    }

    private sealed class CapturingSystem : IStreamSystemHandle
    {
        public readonly List<string> PlainStreams = new();
        public readonly List<string> SuperStreams = new();
        public readonly CapturingHandle Plain = new();
        public readonly CapturingHandle Super = new();
        public bool IsClosed => false;
        public Task<IStreamProducerHandle> CreatePlainProducerAsync(string stream, CancellationToken ct)
        {
            lock (PlainStreams) PlainStreams.Add(stream);
            return Task.FromResult<IStreamProducerHandle>(Plain);
        }
        public Task<IStreamProducerHandle> CreatePlainProducerWithReferenceAsync(string stream, string reference, CancellationToken ct)
            => throw new NotImplementedException("PublishRoutingTests never replays.");
        public Task<IStreamProducerHandle> CreateSuperStreamProducerAsync(string stream, CancellationToken ct)
        {
            lock (SuperStreams) SuperStreams.Add(stream);
            return Task.FromResult<IStreamProducerHandle>(Super);
        }
        public Task<IStreamProducerHandle> CreateSuperStreamProducerWithReferenceAsync(string stream, string reference, CancellationToken ct)
            => throw new NotImplementedException("PublishRoutingTests never replays.");
        public Task<ulong> QuerySequenceAsync(string reference, string stream, CancellationToken ct)
            => throw new NotImplementedException("PublishRoutingTests never queries sequences.");
        public Task<ulong?> TryQueryOffsetAsync(string reference, string stream, CancellationToken ct) => Task.FromResult<ulong?>(null);
        public Task StoreSystemOffsetAsync(string reference, string stream, ulong offset, CancellationToken ct) => Task.CompletedTask;
        public Task<bool> StreamExistsAsync(string stream, CancellationToken ct) => Task.FromResult(false);
        public Task<bool> SuperStreamExistsAsync(string superStream, CancellationToken ct) => Task.FromResult(false);
        public Task<string[]> QueryPartitionAsync(string superStream, CancellationToken ct) => Task.FromResult(Array.Empty<string>());
        public Task<RawConsumer> CreateRawConsumerAsync(RawConsumerConfig config, CancellationToken ct)
            => throw new NotImplementedException("PublishRoutingTests never consumes.");
        public Task CreateStreamAsync(StreamSpec spec, CancellationToken ct) => Task.CompletedTask;
        public Task CreateSuperStreamAsync(PartitionsSuperStreamSpec spec, CancellationToken ct) => Task.CompletedTask;
        public ValueTask DisposeAsync() => ValueTask.CompletedTask;
    }

    private sealed class ReadyConnector : IStreamSystemConnector
    {
        private readonly IStreamSystemHandle _system;
        public ReadyConnector(IStreamSystemHandle system) => _system = system;
        public Task<IStreamSystemHandle> ConnectAsync(StreamBusOptions options, CancellationToken ct)
            => Task.FromResult(_system);
    }

    private static (StreamBusRegistry Registry, CapturingSystem System) CreateRegistry(Action<StreamBusPublishStore>? configureStore = null)
    {
        var system = new CapturingSystem();
        var store = new StreamBusPublishStore();
        configureStore?.Invoke(store);
        var registry = new StreamBusRegistry(
            vhost => new StreamBusOptions { Vhost = vhost, UserName = "app", Password = "secret" },
            options => new StreamBusConnectionSupervisor(options, new ReadyConnector(system)),
            null,
            new StreamPublishRouter(store),
            _ => SystemTextJsonStreamSerializer.Default);
        var supervisor = ((IStreamBusRegistry)registry).SupervisorFor("/");
        supervisor.StartAsync(CancellationToken.None).GetAwaiter().GetResult();
        var readiness = supervisor.WaitUntilReadyAsync(ReadinessPolicy.Indefinitely, new CancellationTokenSource(TimeSpan.FromSeconds(5)).Token).GetAwaiter().GetResult();
        Assert.Equal(BusReadiness.Ready, readiness);
        return (registry, system);
    }

    [Fact]
    public void Resolve_ok_per_kind()
    {
        var store = new StreamBusPublishStore();
        store.Add(typeof(DefaultBus), typeof(OrderEvent), "orders", isSuper: false);
        store.Add(typeof(DefaultBus), typeof(InvoiceEvent), "invoices-super", isSuper: true);
        var router = new StreamPublishRouter(store);
        Assert.Equal("orders", router.Resolve(typeof(DefaultBus), typeof(OrderEvent)).Stream);
        Assert.False(router.Resolve(typeof(DefaultBus), typeof(OrderEvent)).IsSuper);
        Assert.True(router.Resolve(typeof(DefaultBus), typeof(InvoiceEvent)).IsSuper);
    }

    [Fact]
    public void Unknown_type_names_missing_registration()
    {
        var router = new StreamPublishRouter(new StreamBusPublishStore());
        var ex = Assert.Throws<StreamBusConfigurationException>(() =>
            router.Resolve(typeof(DefaultBus), typeof(OrderEvent)));
        Assert.Contains(typeof(OrderEvent).FullName!, ex.Message, StringComparison.Ordinal);
        Assert.Contains("cfg.Publish", ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void Two_streams_one_type_lists_both()
    {
        var store = new StreamBusPublishStore();
        store.Add(typeof(DefaultBus), typeof(OrderEvent), "orders-a", isSuper: false);
        store.Add(typeof(DefaultBus), typeof(OrderEvent), "orders-b", isSuper: false);
        var router = new StreamPublishRouter(store);
        var ex = Assert.Throws<StreamBusConfigurationException>(() =>
            router.Resolve(typeof(DefaultBus), typeof(OrderEvent)));
        Assert.Contains("orders-a", ex.Message, StringComparison.Ordinal);
        Assert.Contains("orders-b", ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void Duplicate_same_type_and_stream_throws()
    {
        var store = new StreamBusPublishStore();
        store.Add(typeof(DefaultBus), typeof(OrderEvent), "orders", isSuper: false);
        var ex = Assert.Throws<StreamBusConfigurationException>(() =>
            store.Add(typeof(DefaultBus), typeof(OrderEvent), "orders", isSuper: false));
        Assert.Contains("orders", ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void Configurator_registers_publish_kinds()
    {
        var services = new ServiceCollection();
        services.AddStreamBus((_, cfg) =>
        {
            cfg.Host("127.0.0.1", "/", h => h.Credentials("app", "secret"));
            cfg.Publish<OrderEvent>("orders");
            cfg.PublishSuper<InvoiceEvent>("invoices-super");
        });
        var sp = services.BuildServiceProvider();
        var store = sp.GetRequiredService<StreamBusPublishStore>();
        var router = new StreamPublishRouter(store);
        Assert.False(router.Resolve(typeof(DefaultBus), typeof(OrderEvent)).IsSuper);
        Assert.True(router.Resolve(typeof(DefaultBus), typeof(InvoiceEvent)).IsSuper);
    }

    [Fact]
    public async Task Typed_publish_lands_on_registered_stream()
    {
        var (registry, system) = CreateRegistry(s => s.Add(typeof(DefaultBus), typeof(OrderEvent), "orders", isSuper: false));
        var bus = registry.GetOrCreateBus("/");
        await bus.PublishAsync(new OrderEvent("o-1", 1), TestContext.Current.CancellationToken);
        Assert.Contains("orders", system.PlainStreams);
        var sent = Assert.Single(system.Plain.Sent);
        Assert.Equal(new OrderEvent("o-1", 1), SystemTextJsonStreamSerializer.Default.Deserialize<OrderEvent>(sent.Body));
    }

    [Fact]
    public async Task Typed_super_publish_requires_key_and_uses_super_producer()
    {
        var (registry, system) = CreateRegistry(s => s.Add(typeof(DefaultBus), typeof(OrderEvent), "orders-super", isSuper: true));
        var bus = registry.GetOrCreateBus("/");
        await bus.PublishSuperAsync(new OrderEvent("o-1", 1), "o-1", ct: TestContext.Current.CancellationToken);
        Assert.Contains("orders-super", system.SuperStreams);
        var sent = Assert.Single(system.Super.Sent);
        Assert.Equal("o-1", sent.RoutingKey);
    }

    [Fact]
    public async Task Empty_routing_key_throws_before_broker()
    {
        var (registry, _) = CreateRegistry(s => s.Add(typeof(DefaultBus), typeof(OrderEvent), "orders-super", isSuper: true));
        var bus = registry.GetOrCreateBus("/");
        await Assert.ThrowsAsync<ArgumentException>(() =>
            bus.PublishSuperAsync(new OrderEvent("o-1", 1), "", ct: TestContext.Current.CancellationToken));
    }

    [Fact]
    public async Task Wrong_method_for_registration_throws_both_sides()
    {
        var (plainRegistry, _) = CreateRegistry(s => s.Add(typeof(DefaultBus), typeof(OrderEvent), "orders", isSuper: false));
        var plainBus = plainRegistry.GetOrCreateBus("/");
        var plainEx = await Assert.ThrowsAsync<StreamBusConfigurationException>(() =>
            plainBus.PublishSuperAsync(new OrderEvent("o-1", 1), "o-1", ct: TestContext.Current.CancellationToken));
        Assert.Contains("Use PublishAsync", plainEx.Message, StringComparison.Ordinal);

        var (superRegistry, _) = CreateRegistry(s => s.Add(typeof(DefaultBus), typeof(OrderEvent), "orders-super", isSuper: true));
        var superBus = superRegistry.GetOrCreateBus("/");
        var superEx = await Assert.ThrowsAsync<StreamBusConfigurationException>(() =>
            superBus.PublishAsync(new OrderEvent("o-1", 1), ct: TestContext.Current.CancellationToken));
        Assert.Contains("Use PublishSuperAsync", superEx.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task Explicit_stream_override_publishes_to_given_stream()
    {
        var (registry, system) = CreateRegistry(s => s.Add(typeof(DefaultBus), typeof(OrderEvent), "orders", isSuper: false));
        var bus = registry.GetOrCreateBus("/");
        await bus.PublishAsync("orders-manual", new OrderEvent("o-1", 1), TestContext.Current.CancellationToken);
        Assert.Contains("orders-manual", system.PlainStreams);
    }

    [Fact]
    public async Task Explicit_super_override_publishes_with_key()
    {
        var (registry, system) = CreateRegistry();
        var bus = registry.GetOrCreateBus("/");
        await bus.PublishSuperAsync("manual-super", new OrderEvent("o-1", 1), "o-1", TestContext.Current.CancellationToken);
        Assert.Contains("manual-super", system.SuperStreams);
        Assert.Equal("o-1", Assert.Single(system.Super.Sent).RoutingKey);
    }

    private sealed class NotFoundSystem : IStreamSystemHandle
    {
        public bool IsClosed => false;
        public Task<IStreamProducerHandle> CreatePlainProducerAsync(string stream, CancellationToken ct)
            => Task.FromException<IStreamProducerHandle>(new RabbitMQ.Stream.Client.CreateProducerException(
                "producer could not be created code: StreamDoesNotExist",
                RabbitMQ.Stream.Client.ResponseCode.StreamDoesNotExist));
        public Task<IStreamProducerHandle> CreatePlainProducerWithReferenceAsync(string stream, string reference, CancellationToken ct)
            => throw new NotImplementedException("NotFoundSystem never replays.");
        public Task<IStreamProducerHandle> CreateSuperStreamProducerAsync(string stream, CancellationToken ct)
            => throw new NotImplementedException("NotFoundSystem never creates super producers.");
        public Task<IStreamProducerHandle> CreateSuperStreamProducerWithReferenceAsync(string stream, string reference, CancellationToken ct)
            => throw new NotImplementedException("NotFoundSystem never replays.");
        public Task<ulong> QuerySequenceAsync(string reference, string stream, CancellationToken ct)
            => throw new NotImplementedException("NotFoundSystem never queries sequences.");
        public Task<ulong?> TryQueryOffsetAsync(string reference, string stream, CancellationToken ct) => Task.FromResult<ulong?>(null);
        public Task StoreSystemOffsetAsync(string reference, string stream, ulong offset, CancellationToken ct) => Task.CompletedTask;
        public Task<bool> StreamExistsAsync(string stream, CancellationToken ct) => Task.FromResult(false);
        public Task<bool> SuperStreamExistsAsync(string superStream, CancellationToken ct) => Task.FromResult(false);
        public Task<string[]> QueryPartitionAsync(string superStream, CancellationToken ct) => Task.FromResult(Array.Empty<string>());
        public Task<RabbitMQ.Stream.Client.RawConsumer> CreateRawConsumerAsync(RabbitMQ.Stream.Client.RawConsumerConfig config, CancellationToken ct)
            => throw new NotImplementedException("NotFoundSystem never consumes.");
        public Task CreateStreamAsync(RabbitMQ.Stream.Client.StreamSpec spec, CancellationToken ct) => Task.CompletedTask;
        public Task CreateSuperStreamAsync(RabbitMQ.Stream.Client.PartitionsSuperStreamSpec spec, CancellationToken ct) => Task.CompletedTask;
        public ValueTask DisposeAsync() => ValueTask.CompletedTask;
    }

    [Fact]
    public async Task Explicit_override_of_undeclared_stream_surfaces_not_found()
    {
        var system = new NotFoundSystem();
        var registry = new StreamBusRegistry(
            vhost => new StreamBusOptions { Vhost = vhost, UserName = "app", Password = "secret" },
            options => new StreamBusConnectionSupervisor(options, new ReadyConnector(system)),
            null,
            new StreamPublishRouter(new StreamBusPublishStore()),
            _ => SystemTextJsonStreamSerializer.Default);
        var supervisor = ((IStreamBusRegistry)registry).SupervisorFor("/");
        await supervisor.StartAsync(TestContext.Current.CancellationToken);
        var readiness = await supervisor.WaitUntilReadyAsync(ReadinessPolicy.Indefinitely, TestContext.Current.CancellationToken);
        Assert.Equal(BusReadiness.Ready, readiness);
        var bus = registry.GetOrCreateBus("/");
        var ex = await Assert.ThrowsAsync<StreamBusStreamNotFoundException>(() =>
            bus.PublishAsync("undeclared", new OrderEvent("o-1", 1), TestContext.Current.CancellationToken));
        Assert.Contains("undeclared", ex.Message, StringComparison.Ordinal);
    }
}
