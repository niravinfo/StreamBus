using RabbitMQ.Stream.Client;

namespace StreamBus.UnitTests;

sealed class ScriptedConsumer : IStreamConsumerHandle
{
    private int _open = 1;
    public int CloseCount;
    public bool IsOpen => Volatile.Read(ref _open) == 1;
    public void MarkClosed() => Volatile.Write(ref _open, 0);
    public Task<ResponseCode> Close()
    {
        Interlocked.Increment(ref CloseCount);
        MarkClosed();
        return Task.FromResult(ResponseCode.Ok);
    }
    public Task StoreOffset(ulong offset) => Task.CompletedTask;
}

sealed class ScriptedConsumerFactory
{
    private readonly object _gate = new();
    private readonly List<RawConsumerConfig> _configs = new();
    private readonly List<ScriptedConsumer> _consumers = new();
    public int CreateCount { get { lock (_gate) return _consumers.Count; } }
    public RawConsumerConfig FirstConfig { get { lock (_gate) return _configs[0]; } }
    public ScriptedConsumer Current { get { lock (_gate) return _consumers[^1]; } }

    public Task<IStreamConsumerHandle> CreateAsync(RawConsumerConfig config, CancellationToken ct)
    {
        lock (_gate)
        {
            _configs.Add(config);
            _consumers.Add(new ScriptedConsumer());
            return Task.FromResult<IStreamConsumerHandle>(_consumers[^1]);
        }
    }
}

file sealed class NeverReadySystem : IStreamSystemHandle
{
    public bool IsClosed => false;
    public Task<ulong?> TryQueryOffsetAsync(string reference, string stream, CancellationToken ct) => Task.FromResult<ulong?>(null);
    public Task<RawConsumer> CreateRawConsumerAsync(RawConsumerConfig config, CancellationToken ct)
        => throw new NotSupportedException("ConsumerDeathTests inject their own consumer factory.");
    public Task<IStreamProducerHandle> CreatePlainProducerAsync(string stream, CancellationToken ct) => throw new NotSupportedException();
    public Task<IStreamProducerHandle> CreatePlainProducerWithReferenceAsync(string stream, string reference, CancellationToken ct) => throw new NotSupportedException();
    public Task<IStreamProducerHandle> CreateSuperStreamProducerAsync(string stream, CancellationToken ct) => throw new NotSupportedException();
    public Task<IStreamProducerHandle> CreateSuperStreamProducerWithReferenceAsync(string stream, string reference, CancellationToken ct) => throw new NotSupportedException();
    public Task<ulong> QuerySequenceAsync(string reference, string stream, CancellationToken ct) => throw new NotSupportedException();
    public Task StoreSystemOffsetAsync(string reference, string stream, ulong offset, CancellationToken ct) => Task.CompletedTask;
    public Task<bool> StreamExistsAsync(string stream, CancellationToken ct) => Task.FromResult(true);
    public Task<bool> SuperStreamExistsAsync(string superStream, CancellationToken ct) => Task.FromResult(false);
    public Task<string[]> QueryPartitionAsync(string superStream, CancellationToken ct) => Task.FromResult(new[] { superStream });
    public Task CreateStreamAsync(StreamSpec spec, CancellationToken ct) => Task.CompletedTask;
    public Task CreateSuperStreamAsync(PartitionsSuperStreamSpec spec, CancellationToken ct) => Task.CompletedTask;
    public ValueTask DisposeAsync() => ValueTask.CompletedTask;
}

file sealed class NeverReadyConnector : IStreamSystemConnector
{
    public Task<IStreamSystemHandle> ConnectAsync(StreamBusOptions options, CancellationToken ct)
        => new TaskCompletionSource<IStreamSystemHandle>().Task;
}

public sealed class ConsumerDeathTests
{
    private static async Task WaitUntilAsync(Func<bool> condition, TimeSpan timeout)
    {
        var deadline = DateTime.UtcNow + timeout;
        while (DateTime.UtcNow < deadline)
        {
            if (condition()) return;
            await Task.Delay(25);
        }
        Assert.True(condition(), $"condition was not met within {timeout}");
    }

    private sealed class Harness : IAsyncDisposable
    {
        public required ScriptedConsumerFactory Factory { get; init; }
        public required StreamPartitionConsumer Consumer { get; init; }
        public required Task RunTask { get; init; }
        public required CancellationTokenSource Cts { get; init; }

        public async ValueTask DisposeAsync()
        {
            await Cts.CancelAsync();
            try { await RunTask; } catch (OperationCanceledException) { }
            await Consumer.DisposeAsync();
            Cts.Dispose();
        }
    }

    private static Harness Create()
    {
        var system = new NeverReadySystem();
        var options = new ConsumeOptions
        {
            Reference = "grp",
            SingleActiveConsumer = false,
            Offset = ConsumeOffset.AlwaysFirst,
        };
        var supervisor = new StreamBusConnectionSupervisor(
            new StreamBusOptions { Vhost = "/", UserName = "app", Password = "secret" },
            new NeverReadyConnector());
        var producers = new StreamProducerRegistry(supervisor, "/");
        var errors = new ErrorPublisher(producers);
        var factory = new ScriptedConsumerFactory();
        var consumer = new StreamPartitionConsumer(
            system, "orders", "orders", 0, "grp", options,
            _ => Task.CompletedTask,
            new MonotonicOffsetGuard(),
            new PartitionState(),
            StreamBusMetrics.Shared,
            "/", "bus", errors,
            logger: null,
            consumerFactory: factory.CreateAsync);
        var cts = new CancellationTokenSource();
        var run = consumer.RunAsync(cts.Token);
        return new Harness { Factory = factory, Consumer = consumer, RunTask = run, Cts = cts };
    }

    [Fact]
    public async Task Connection_closed_callback_fails_attach_and_loop_recreates()
    {
        await using var harness = Create();
        await harness.Consumer.FirstAttachAsync.WaitAsync(TimeSpan.FromSeconds(5), TestContext.Current.CancellationToken);
        var first = harness.Factory.Current;

        await harness.Factory.FirstConfig.ConnectionClosedHandler("connection reset by peer");

        await WaitUntilAsync(() => harness.Factory.CreateCount >= 2, TimeSpan.FromSeconds(5));
        Assert.True(first.CloseCount >= 1);
    }

    [Fact]
    public async Task Metadata_change_on_closed_handle_fails_attach_exactly_once()
    {
        await using var harness = Create();
        await harness.Consumer.FirstAttachAsync.WaitAsync(TimeSpan.FromSeconds(5), TestContext.Current.CancellationToken);
        harness.Factory.Current.MarkClosed();

        var metadata = new MetaDataUpdate("orders", ResponseCode.Ok);
        await harness.Factory.FirstConfig.MetadataHandler(metadata);
        await harness.Factory.FirstConfig.MetadataHandler(metadata);

        await WaitUntilAsync(() => harness.Factory.CreateCount >= 2, TimeSpan.FromSeconds(5));
        await Task.Delay(150, TestContext.Current.CancellationToken);
        Assert.Equal(2, harness.Factory.CreateCount);
    }

    [Fact]
    public async Task Park_watchdog_detects_closed_handle_within_6s()
    {
        await using var harness = Create();
        await harness.Consumer.FirstAttachAsync.WaitAsync(TimeSpan.FromSeconds(5), TestContext.Current.CancellationToken);
        var started = DateTime.UtcNow;
        harness.Factory.Current.MarkClosed();

        await WaitUntilAsync(() => harness.Factory.CreateCount >= 2, TimeSpan.FromSeconds(6.5));

        Assert.True(DateTime.UtcNow - started < TimeSpan.FromSeconds(6.5));
    }

    [Fact]
    public async Task Watchdog_does_not_fail_attach_while_handle_is_open()
    {
        await using var harness = Create();
        await harness.Consumer.FirstAttachAsync.WaitAsync(TimeSpan.FromSeconds(5), TestContext.Current.CancellationToken);

        await Task.Delay(TimeSpan.FromSeconds(12), TestContext.Current.CancellationToken);

        Assert.Equal(1, harness.Factory.CreateCount);
        Assert.Equal(0, harness.Factory.Current.CloseCount);
    }
}
