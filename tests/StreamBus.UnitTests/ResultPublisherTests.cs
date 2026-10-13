using RabbitMQ.Stream.Client;

namespace StreamBus.UnitTests;

public sealed class ResultPublisherTests
{
    private sealed class FlakyHandle : IStreamProducerHandle
    {
        private int _failuresLeft;
        public int Calls;
        public FlakyHandle(int failures) => _failuresLeft = failures;
        public Task SendAsync(ReadOnlyMemory<byte> body, string? routingKey, CancellationToken ct) => throw new NotImplementedException();
        public Task SendEnvelopeAsync(ReadOnlyMemory<byte> body, string? routingKey, IReadOnlyDictionary<string, object?> app, string? mid, string? cid, CancellationToken ct) => throw new NotImplementedException();
        public Task PublishReplayAsync(ReadOnlyMemory<byte> body, ulong publishingId, CancellationToken ct) => throw new NotImplementedException();
        public Task PublishResultAsync(ReadOnlyMemory<byte> body, ulong publishingId, string? routingKey, CancellationToken ct)
        {
            Calls++;
            if (Interlocked.Decrement(ref _failuresLeft) >= 0)
                throw new StreamBusPublishTransientException("flaky");
            return Task.CompletedTask;
        }
        public ValueTask DisposeAsync() => ValueTask.CompletedTask;
    }

    private sealed class FlakySystem : IStreamSystemHandle
    {
        private readonly FlakyHandle _handle;
        public FlakySystem(int failures) => _handle = new FlakyHandle(failures);
        public int Calls => _handle.Calls;
        public bool IsClosed => false;
        public Task<IStreamProducerHandle> CreatePlainProducerAsync(string stream, CancellationToken ct) => throw new NotImplementedException();
        public Task<IStreamProducerHandle> CreatePlainProducerWithReferenceAsync(string stream, string reference, CancellationToken ct) => Task.FromResult<IStreamProducerHandle>(_handle);
        public Task<IStreamProducerHandle> CreateSuperStreamProducerAsync(string stream, CancellationToken ct) => throw new NotImplementedException();
        public Task<IStreamProducerHandle> CreateSuperStreamProducerWithReferenceAsync(string stream, string reference, CancellationToken ct) => Task.FromResult<IStreamProducerHandle>(_handle);
        public Task<ulong?> TryQueryOffsetAsync(string reference, string stream, CancellationToken ct) => Task.FromResult<ulong?>(null);
        public Task StoreSystemOffsetAsync(string reference, string stream, ulong offset, CancellationToken ct) => Task.CompletedTask;
        public Task<bool> StreamExistsAsync(string stream, CancellationToken ct) => Task.FromResult(true);
        public Task<bool> SuperStreamExistsAsync(string superStream, CancellationToken ct) => Task.FromResult(false);
        public Task<string[]> QueryPartitionAsync(string superStream, CancellationToken ct) => Task.FromResult(new[] { superStream });
        public Task<ulong> QuerySequenceAsync(string reference, string stream, CancellationToken ct) => Task.FromResult(0UL);
        public Task<RawConsumer> CreateRawConsumerAsync(RawConsumerConfig config, CancellationToken ct) => throw new NotImplementedException();
        public Task CreateStreamAsync(StreamSpec spec, CancellationToken ct) => throw new NotImplementedException("ResultPublisherTests never declares topology.");
        public Task CreateSuperStreamAsync(PartitionsSuperStreamSpec spec, CancellationToken ct) => throw new NotImplementedException("ResultPublisherTests never declares topology.");
        public ValueTask DisposeAsync() => ValueTask.CompletedTask;
    }

    private sealed class ReadyConnector : IStreamSystemConnector
    {
        private readonly FlakySystem _handle;
        public ReadyConnector(FlakySystem handle) => _handle = handle;
        public Task<IStreamSystemHandle> ConnectAsync(StreamBusOptions options, CancellationToken ct) => Task.FromResult<IStreamSystemHandle>(_handle);
    }

    private static (ResultPublisher Publisher, FlakySystem System) Create(int failures)
    {
        var system = new FlakySystem(failures);
        var options = new StreamBusOptions { Vhost = "/", UserName = "app", Password = "secret" };
        var supervisor = new StreamBusConnectionSupervisor(options, new ReadyConnector(system));
        supervisor.StartAsync(CancellationToken.None).GetAwaiter().GetResult();
        return (new ResultPublisher(new ResultProducerRegistry(supervisor, "/")), system);
    }

    private static ResultWork Work() => new()
    {
        TargetStream = "dst", TargetSuper = false, Body = new byte[] { 1 },
        SourceOffset = 9, RoutingKey = null, ProducerName = "grp",
    };

    [Fact]
    public async Task Fail_twice_then_confirms_three_publishes_one_return()
    {
        var (publisher, system) = Create(failures: 2);
        await publisher.PublishForeverAsync(Work(), _ => TimeSpan.Zero, TestContext.Current.CancellationToken);
        Assert.Equal(3, system.Calls);
    }

    [Fact]
    public async Task Cancel_mid_handoff_throws_and_stops()
    {
        var (publisher, system) = Create(failures: int.MaxValue);
        using var cts = new CancellationTokenSource(TimeSpan.FromMilliseconds(200));
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() =>
            publisher.PublishForeverAsync(Work(), _ => TimeSpan.FromMilliseconds(50), cts.Token));
        Assert.True(system.Calls >= 1);
    }

    [Fact]
    public async Task Negative_delay_clamps_to_zero()
    {
        var (publisher, system) = Create(failures: 1);
        await publisher.PublishForeverAsync(Work(), _ => TimeSpan.FromMilliseconds(-5), TestContext.Current.CancellationToken);
        Assert.Equal(2, system.Calls);
    }

    [Fact]
    public async Task Throwing_delay_falls_back_to_zero()
    {
        var (publisher, system) = Create(failures: 1);
        await publisher.PublishForeverAsync(Work(), _ => throw new InvalidOperationException("hostile"), TestContext.Current.CancellationToken);
        Assert.Equal(2, system.Calls);
    }
}
