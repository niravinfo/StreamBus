using RabbitMQ.Stream.Client;

namespace StreamBus.UnitTests;

sealed class StubProducerHandle : IStreamProducerHandle
{
    private readonly Func<ReadOnlyMemory<byte>, string?, CancellationToken, Task> _send;
    public int SendCount;
    public StubProducerHandle(Func<ReadOnlyMemory<byte>, string?, CancellationToken, Task>? send = null)
        => _send = send ?? ((_, _, _) => Task.CompletedTask);
    public Task SendAsync(ReadOnlyMemory<byte> body, string? routingKey, CancellationToken ct)
    {
        SendCount++;
        return _send(body, routingKey, ct);
    }
    public Task SendEnvelopeAsync(ReadOnlyMemory<byte> body, string? routingKey, IReadOnlyDictionary<string, object?> applicationProperties, string? messageId, string? correlationId, CancellationToken ct)
        => throw new NotImplementedException("PublishTests never sends envelopes.");
    public Task PublishReplayAsync(ReadOnlyMemory<byte> body, ulong publishingId, CancellationToken ct)
        => throw new NotImplementedException("PublishTests never replays.");
    public Task PublishResultAsync(ReadOnlyMemory<byte> body, ulong publishingId, string? routingKey, CancellationToken ct)
        => throw new NotImplementedException("StubProducerHandle never publishes results.");
    public ValueTask DisposeAsync() => ValueTask.CompletedTask;
}

sealed class StubSystemHandle : IStreamSystemHandle
{
    public int PlainCreations;
    public int SuperCreations;
    public Func<string, CancellationToken, Task<IStreamProducerHandle>>? OnCreatePlain;
    public Func<string, CancellationToken, Task<IStreamProducerHandle>>? OnCreateSuper = (_, _) => Task.FromResult<IStreamProducerHandle>(new StubProducerHandle());
    public bool IsClosed => false;
    public Task<IStreamProducerHandle> CreatePlainProducerAsync(string stream, CancellationToken ct)
    {
        PlainCreations++;
        return OnCreatePlain is not null ? OnCreatePlain(stream, ct) : Task.FromResult<IStreamProducerHandle>(new StubProducerHandle());
    }
    public Task<IStreamProducerHandle> CreatePlainProducerWithReferenceAsync(string stream, string reference, CancellationToken ct)
        => throw new NotImplementedException("PublishTests never replays.");
    public Task<IStreamProducerHandle> CreateSuperStreamProducerAsync(string stream, CancellationToken ct)
    {
        SuperCreations++;
        return OnCreateSuper is not null ? OnCreateSuper(stream, ct) : Task.FromResult<IStreamProducerHandle>(new StubProducerHandle());
    }
    public Task<IStreamProducerHandle> CreateSuperStreamProducerWithReferenceAsync(string stream, string reference, CancellationToken ct)
        => throw new NotImplementedException("StubSystemHandle never creates stable super producers.");
    public Task<ulong> QuerySequenceAsync(string reference, string stream, CancellationToken ct)
        => throw new NotImplementedException("StubSystemHandle never queries publishing sequences.");
    public Task<ulong?> TryQueryOffsetAsync(string reference, string stream, CancellationToken ct)
        => throw new NotImplementedException("PublishTests never queries offsets.");
    public Task StoreSystemOffsetAsync(string reference, string stream, ulong offset, CancellationToken ct)
        => throw new NotImplementedException("PublishTests never queries offsets.");
    public Task<bool> StreamExistsAsync(string stream, CancellationToken ct)
        => throw new NotImplementedException("PublishTests never queries offsets.");
    public Task<bool> SuperStreamExistsAsync(string superStream, CancellationToken ct)
        => throw new NotImplementedException("PublishTests never queries offsets.");
    public Task<string[]> QueryPartitionAsync(string superStream, CancellationToken ct)
        => throw new NotImplementedException("PublishTests never queries offsets.");
    public Task<RawConsumer> CreateRawConsumerAsync(RawConsumerConfig config, CancellationToken ct)
        => throw new NotImplementedException("PublishTests never consumes.");
    public Task CreateStreamAsync(StreamSpec spec, CancellationToken ct)
        => throw new NotImplementedException("PublishTests never declares topology.");
    public Task CreateSuperStreamAsync(PartitionsSuperStreamSpec spec, CancellationToken ct)
        => throw new NotImplementedException("PublishTests never declares topology.");
    public ValueTask DisposeAsync() => ValueTask.CompletedTask;
}

sealed class ReadyConnector : IStreamSystemConnector
{
    private readonly StubSystemHandle _handle;
    public ReadyConnector(StubSystemHandle handle) => _handle = handle;
    public Task<IStreamSystemHandle> ConnectAsync(StreamBusOptions options, CancellationToken ct)
        => Task.FromResult<IStreamSystemHandle>(_handle);
}

public sealed class PublishTests
{
    private static (StreamProducerRegistry Registry, StubSystemHandle System) CreateRegistry()
    {
        var system = new StubSystemHandle();
        var options = new StreamBusOptions { Vhost = "/", UserName = "app", Password = "secret" };
        var supervisor = new StreamBusConnectionSupervisor(options, new ReadyConnector(system));
        supervisor.StartAsync(CancellationToken.None).GetAwaiter().GetResult();
        var readiness = supervisor.WaitUntilReadyAsync(ReadinessPolicy.Indefinitely, new CancellationTokenSource(TimeSpan.FromSeconds(5)).Token).GetAwaiter().GetResult();
        Assert.Equal(BusReadiness.Ready, readiness);
        return (new StreamProducerRegistry(supervisor, "/"), system);
    }

    [Fact]
    public async Task N_publishes_reuse_one_producer()
    {
        var (registry, system) = CreateRegistry();
        for (var i = 0; i < 5; i++)
            await registry.PublishAsync("orders", new byte[] { (byte)i }, new PublishOptions(), TestContext.Current.CancellationToken);
        Assert.Equal(1, system.PlainCreations);
    }

    [Fact]
    public async Task Failed_confirm_raises_with_stream_and_status()
    {
        var (registry, system) = CreateRegistry();
        system.OnCreatePlain = (_, _) => Task.FromResult<IStreamProducerHandle>(
            new StubProducerHandle((_, _, _) => throw new StreamBusPublishException("Publish to stream 'orders' was not confirmed (status InternalError).")));
        var ex = await Assert.ThrowsAsync<StreamBusPublishException>(() =>
            registry.PublishAsync("orders", new byte[] { 1 }, new PublishOptions(), TestContext.Current.CancellationToken));
        Assert.Contains("orders", ex.Message);
    }

    [Fact]
    public async Task Missing_stream_throws_not_found()
    {
        var system = new StubSystemHandle
        {
            OnCreatePlain = (_, _) => Task.FromException<IStreamProducerHandle>(
                new CreateProducerException("producer could not be created code: StreamDoesNotExist", ResponseCode.StreamDoesNotExist)),
        };
        var options = new StreamBusOptions { Vhost = "/", UserName = "app", Password = "secret" };
        var supervisor = new StreamBusConnectionSupervisor(options, new ReadyConnector(system));
        await supervisor.StartAsync(CancellationToken.None);
        var throwing = new StreamProducerRegistry(supervisor, "/");
        var ex = await Assert.ThrowsAsync<StreamBusStreamNotFoundException>(() =>
            throwing.PublishAsync("nope", new byte[] { 1 }, new PublishOptions(), TestContext.Current.CancellationToken));
        Assert.Equal("nope", ex.Stream);
    }

    [Fact]
    public async Task Confirm_timeout_throws_and_bus_stays_ready()
    {
        var slow = new StubSystemHandle
        {
            OnCreatePlain = (_, _) => Task.FromResult<IStreamProducerHandle>(
                new StubProducerHandle(async (_, _, ct) => { await Task.Delay(60_000, ct); })),
        };
        var options = new StreamBusOptions { Vhost = "/", UserName = "app", Password = "secret" };
        var supervisor = new StreamBusConnectionSupervisor(options, new ReadyConnector(slow));
        await supervisor.StartAsync(CancellationToken.None);
        var reg = new StreamProducerRegistry(supervisor, "/");
        var ex = await Assert.ThrowsAsync<StreamBusTimeoutException>(() =>
            reg.PublishAsync("orders", new byte[] { 1 }, new PublishOptions { Timeout = TimeSpan.FromMilliseconds(200) }, TestContext.Current.CancellationToken));
        Assert.Contains("outcome unknown", ex.Message);
        Assert.Equal(BusConnectionState.Ready, supervisor.State);
    }

    [Fact]
    public async Task Transient_send_invalidates_and_recreates()
    {
        var (registry, system) = CreateRegistry();
        var calls = 0;
        system.OnCreatePlain = (_, _) => Task.FromResult<IStreamProducerHandle>(
            new StubProducerHandle((_, _, _) => ++calls == 1
                ? throw new System.IO.IOException("reset")
                : Task.CompletedTask));
        await Assert.ThrowsAsync<StreamBusPublishTransientException>(() =>
            registry.PublishAsync("orders", new byte[] { 1 }, new PublishOptions(), TestContext.Current.CancellationToken));
        await registry.PublishAsync("orders", new byte[] { 2 }, new PublishOptions(), TestContext.Current.CancellationToken);
        Assert.Equal(2, system.PlainCreations);
    }

    [Fact]
    public async Task Timeout_then_republish_creates_new_producer()
    {
        var system = new StubSystemHandle();
        system.OnCreatePlain = (_, _) => system.PlainCreations == 1
            ? Task.FromResult<IStreamProducerHandle>(
                new StubProducerHandle(async (_, _, ct) => { await Task.Delay(60_000, ct); }))
            : Task.FromResult<IStreamProducerHandle>(new StubProducerHandle());
        var options = new StreamBusOptions { Vhost = "/", UserName = "app", Password = "secret" };
        var supervisor = new StreamBusConnectionSupervisor(options, new ReadyConnector(system));
        await supervisor.StartAsync(CancellationToken.None);
        var reg = new StreamProducerRegistry(supervisor, "/");
        await Assert.ThrowsAsync<StreamBusTimeoutException>(() =>
            reg.PublishAsync("orders", new byte[] { 1 }, new PublishOptions { Timeout = TimeSpan.FromMilliseconds(200) }, TestContext.Current.CancellationToken));
        await reg.PublishAsync("orders", new byte[] { 2 }, new PublishOptions(), TestContext.Current.CancellationToken);
        Assert.Equal(2, system.PlainCreations);
    }

    [Fact]
    public async Task Failed_confirm_does_not_invalidate_producer()
    {
        var (registry, system) = CreateRegistry();
        system.OnCreatePlain = (_, _) => Task.FromResult<IStreamProducerHandle>(
            new StubProducerHandle((_, _, _) => throw new StreamBusPublishException("Publish to stream 'orders' was not confirmed (status InternalError).")));
        await Assert.ThrowsAsync<StreamBusPublishException>(() =>
            registry.PublishAsync("orders", new byte[] { 1 }, new PublishOptions(), TestContext.Current.CancellationToken));
        await Assert.ThrowsAsync<StreamBusPublishException>(() =>
            registry.PublishAsync("orders", new byte[] { 2 }, new PublishOptions(), TestContext.Current.CancellationToken));
        Assert.Equal(1, system.PlainCreations);
    }

    [Fact]
    public async Task Cancelled_waiter_does_not_evict_healthy_producer()
    {
        var system = new StubSystemHandle
        {
            OnCreatePlain = (_, _) => Task.FromResult<IStreamProducerHandle>(
                new StubProducerHandle((_, _, ct) =>
                {
                    ct.ThrowIfCancellationRequested();
                    return Task.CompletedTask;
                })),
        };
        var options = new StreamBusOptions { Vhost = "/", UserName = "app", Password = "secret" };
        var supervisor = new StreamBusConnectionSupervisor(options, new ReadyConnector(system));
        await supervisor.StartAsync(CancellationToken.None);
        var readiness = await supervisor.WaitUntilReadyAsync(ReadinessPolicy.Indefinitely, new CancellationTokenSource(TimeSpan.FromSeconds(5)).Token);
        Assert.Equal(BusReadiness.Ready, readiness);
        var registry = new StreamProducerRegistry(supervisor, "/");
        await registry.PublishAsync("orders", new byte[] { 1 }, new PublishOptions(), TestContext.Current.CancellationToken);
        Assert.Equal(1, system.PlainCreations);
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() =>
            registry.PublishAsync("orders", new byte[] { 2 }, new PublishOptions(), new CancellationToken(true)));
        await registry.PublishAsync("orders", new byte[] { 3 }, new PublishOptions(), TestContext.Current.CancellationToken);
        Assert.Equal(1, system.PlainCreations);
    }

    private static StreamProducerRegistry CreateUnstartedRegistry()
    {
        var system = new StubSystemHandle();
        var options = new StreamBusOptions { Vhost = "/", UserName = "app", Password = "secret" };
        var supervisor = new StreamBusConnectionSupervisor(options, new ReadyConnector(system));
        return new StreamProducerRegistry(supervisor, "/");
    }

    [Theory]
    [InlineData(0)]
    [InlineData(-1)]
    public async Task Non_positive_timeout_throws_before_broker_contact(int milliseconds)
    {
        var registry = CreateUnstartedRegistry();
        await Assert.ThrowsAsync<StreamBusConfigurationException>(() =>
            registry.PublishAsync("orders", new byte[] { 1 }, new PublishOptions { Timeout = TimeSpan.FromMilliseconds(milliseconds) }, TestContext.Current.CancellationToken));
    }

    [Theory]
    [InlineData(null)]
    [InlineData("")]
    [InlineData("   ")]
    public async Task Null_or_empty_stream_throws_before_broker_contact(string? stream)
    {
        var registry = CreateUnstartedRegistry();
        await Assert.ThrowsAnyAsync<ArgumentException>(() =>
            registry.PublishAsync(stream!, new byte[] { 1 }, new PublishOptions(), TestContext.Current.CancellationToken));
    }

    [Fact]
    public async Task Null_options_throws_before_broker_contact()
    {
        var registry = CreateUnstartedRegistry();
        await Assert.ThrowsAsync<ArgumentNullException>(() =>
            registry.PublishAsync("orders", new byte[] { 1 }, null!, TestContext.Current.CancellationToken));
    }

    [Fact]
    public async Task Super_publish_requires_routing_key()
    {
        var (registry, _) = CreateRegistry();
        await Assert.ThrowsAsync<StreamBusConfigurationException>(() =>
            registry.PublishAsync("events", new byte[] { 1 }, new PublishOptions { IsSuperStream = true }, TestContext.Current.CancellationToken));
    }

    [Fact]
    public async Task Not_ready_failfast_throws_not_ready()
    {
        var options = new StreamBusOptions { Vhost = "/", UserName = "app", Password = "secret", Host = "127.0.0.1", StreamPort = 59999 };
        await using var supervisor = new StreamBusConnectionSupervisor(options, new StreamSystemConnector());
        await supervisor.StartAsync(CancellationToken.None);
        var registry = new StreamProducerRegistry(supervisor, "/");
        await Assert.ThrowsAsync<StreamBusNotReadyException>(() =>
            registry.PublishAsync("orders", new byte[] { 1 }, new PublishOptions(), TestContext.Current.CancellationToken));
    }
}
