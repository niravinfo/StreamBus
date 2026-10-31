using System.Text;
using RabbitMQ.Stream.Client;
using RabbitMQ.Stream.Client.AMQP;

namespace StreamBus.UnitTests;

internal sealed class RecordingProducer : IStreamPlainProducerHandle
{
    private readonly object _gate = new();
    private readonly List<(ulong Id, byte[] Body)> _published = new();

    public RecordingProducer(string stream, string reference)
    {
        Stream = stream;
        Reference = reference;
    }

    public string Stream { get; }
    public string Reference { get; }
    public int PublishedCount { get { lock (_gate) return _published.Count; } }

    public (ulong Id, byte[] Body)[] Published()
    {
        lock (_gate) return _published.ToArray();
    }

    public Task SendAsync(ReadOnlyMemory<byte> body, PublishOptions options, CancellationToken ct)
        => throw new NotSupportedException("Replay publishes through PublishReplayAsync.");

    public Task SendEnvelopeAsync(ReadOnlyMemory<byte> body, StreamMessageProperties properties, CancellationToken ct)
        => throw new NotSupportedException("Replay publishes through PublishReplayAsync.");

    public Task PublishReplayAsync(ReadOnlyMemory<byte> body, ulong publishingId, PublishOptions options, CancellationToken ct)
    {
        lock (_gate) _published.Add((publishingId, body.ToArray()));
        return Task.CompletedTask;
    }

    public ValueTask DisposeAsync() => ValueTask.CompletedTask;
}

internal sealed class RecordingSystem : IStreamSystemHandle
{
    private readonly object _gate = new();
    private readonly List<string> _existenceChecks = new();
    private readonly List<RecordingProducer> _producers = new();
    private int _closed;

    public bool IsClosed => Volatile.Read(ref _closed) == 1;
    public void MarkClosed() => Volatile.Write(ref _closed, 1);

    public int ExistenceCheckCount { get { lock (_gate) return _existenceChecks.Count; } }

    public bool CheckedExistence(string stream)
    {
        lock (_gate) return _existenceChecks.Contains(stream);
    }

    public RecordingProducer[] Producers()
    {
        lock (_gate) return _producers.ToArray();
    }

    public string[] ProducerReferences()
    {
        lock (_gate) return _producers.Select(p => p.Reference).ToArray();
    }

    public int PublishedCount
    {
        get
        {
            lock (_gate) return _producers.Sum(p => p.PublishedCount);
        }
    }

    public Task<bool> StreamExistsAsync(string stream, CancellationToken ct)
    {
        lock (_gate) _existenceChecks.Add(stream);
        return Task.FromResult(true);
    }

    public Task<IStreamPlainProducerHandle> CreatePlainProducerWithReferenceAsync(string stream, string reference, CancellationToken ct)
    {
        var producer = new RecordingProducer(stream, reference);
        lock (_gate) _producers.Add(producer);
        return Task.FromResult<IStreamPlainProducerHandle>(producer);
    }

    public Task<IStreamPlainProducerHandle> CreatePlainProducerAsync(string stream, CancellationToken ct)
        => throw new NotSupportedException("Replay uses reference-scoped producers.");

    public Task<RawConsumer> CreateRawConsumerAsync(RawConsumerConfig config, CancellationToken ct)
        => throw new NotSupportedException("Tests inject their own consumer factory.");

    public Task<ulong?> TryQueryOffsetAsync(string reference, string stream, CancellationToken ct)
        => Task.FromResult<ulong?>(null);

    public Task<ulong> QuerySequenceAsync(string reference, string stream, CancellationToken ct)
        => Task.FromResult(0UL);

    public Task StoreSystemOffsetAsync(string reference, string stream, ulong offset, CancellationToken ct)
        => Task.CompletedTask;

    public Task<bool> SuperStreamExistsAsync(string superStream, CancellationToken ct)
        => Task.FromResult(false);

    public Task<string[]> QueryPartitionAsync(string superStream, CancellationToken ct)
        => Task.FromResult(new[] { superStream });

    public Task<IStreamSuperProducerHandle> CreateSuperStreamProducerAsync(string stream, CancellationToken ct)
        => throw new NotSupportedException("These tests use plain targets.");

    public Task<IStreamSuperProducerHandle> CreateSuperStreamProducerWithReferenceAsync(string stream, string reference, CancellationToken ct)
        => throw new NotSupportedException("These tests use plain targets.");

    public Task CreateStreamAsync(StreamSpec spec, CancellationToken ct) => Task.CompletedTask;
    public Task CreateSuperStreamAsync(PartitionsSuperStreamSpec spec, CancellationToken ct) => Task.CompletedTask;
    public ValueTask DisposeAsync() => ValueTask.CompletedTask;
}

internal sealed class SwappingConnector : IStreamSystemConnector
{
    private readonly object _gate = new();
    private readonly IStreamSystemHandle[] _systems;
    private int _next;

    public SwappingConnector(params IStreamSystemHandle[] systems)
    {
        _systems = systems;
    }

    public Task<IStreamSystemHandle> ConnectAsync(StreamBusOptions options, CancellationToken ct)
    {
        lock (_gate)
        {
            var index = Math.Min(_next, _systems.Length - 1);
            _next++;
            return Task.FromResult(_systems[index]);
        }
    }
}

public sealed class ReplayAndEffectivelyOnceReattachTests
{
    private sealed class Harness : IAsyncDisposable
    {
        public required StreamBusConnectionSupervisor Supervisor { get; init; }
        public required StreamConsumerRegistry Registry { get; init; }
        public required ScriptedConsumerFactory Factory { get; init; }
        public required RecordingSystem OldSystem { get; init; }
        public required RecordingSystem NewSystem { get; init; }

        public async ValueTask DisposeAsync() => await Supervisor.DisposeAsync();
    }

    private static async Task<Harness> CreateAsync()
    {
        var oldSystem = new RecordingSystem();
        var newSystem = new RecordingSystem();
        var supervisor = new StreamBusConnectionSupervisor(
            new StreamBusOptions
            {
                Vhost = "/",
                UserName = "app",
                Password = "secret",
                Connection = new StreamBusConnectionOptions { MonitorInterval = TimeSpan.FromMilliseconds(20) },
            },
            new SwappingConnector(oldSystem, newSystem));
        await supervisor.StartAsync(TestContext.Current.CancellationToken);
        var readiness = await supervisor.WaitUntilReadyAsync(
            ReadinessPolicy.UpTo(TimeSpan.FromSeconds(5)),
            TestContext.Current.CancellationToken);
        Assert.Equal(BusReadiness.Ready, readiness);

        var factory = new ScriptedConsumerFactory();
        var registry = new StreamConsumerRegistry(
            supervisor,
            "/",
            new StreamProducerRegistry(supervisor, "/"),
            loggerFactory: null,
            new ResultProducerRegistry(supervisor, "/"),
            consumerFactory: factory.CreateAsync);
        return new Harness
        {
            Supervisor = supervisor,
            Registry = registry,
            Factory = factory,
            OldSystem = oldSystem,
            NewSystem = newSystem,
        };
    }

    private static async Task WaitUntilAsync(Func<bool> condition, TimeSpan timeout)
    {
        var deadline = DateTime.UtcNow + timeout;
        while (DateTime.UtcNow < deadline)
        {
            if (condition()) return;
            await Task.Delay(20);
        }
        Assert.True(condition(), $"condition was not met within {timeout}");
    }

    private static ErrorEnvelope Envelope(string partitionStream, ulong offset, string body)
        => new()
        {
            ErrorStream = "errors",
            SourceStream = "orders",
            SourcePartition = partitionStream.EndsWith("-0", StringComparison.Ordinal) ? 0 : 1,
            SourcePartitionStream = partitionStream,
            SourceOffset = offset,
            Body = Encoding.UTF8.GetBytes(body),
            ExceptionType = "System.InvalidOperationException",
            ExceptionMessage = "poison",
            ExceptionStack = string.Empty,
            Attempts = 1,
            FailedAt = DateTimeOffset.UtcNow,
            MessageId = null,
            CorrelationId = null,
            RoutingValue = null,
        };

    private static Task DeliverAsync(RawConsumerConfig config, ulong offset, ErrorEnvelope envelope)
    {
        var message = new Message(envelope.Body);
        message.ApplicationProperties ??= new ApplicationProperties();
        foreach (var (key, value) in envelope.ToApplicationProperties())
        {
            if (value is not null) message.ApplicationProperties[key] = value;
        }
        return config.MessageHandler(
            null!,
            new MessageContext(offset, TimeSpan.Zero, 0, 0),
            message);
    }

    private static async Task CancelAsync(CancellationTokenSource cts, Task replay)
    {
        await cts.CancelAsync();
        try { await replay; } catch (OperationCanceledException) { }
    }

    [Fact]
    public async Task Effectively_once_reattaches_through_current_system_after_supervisor_swap()
    {
        await using var harness = await CreateAsync();
        var registration = await harness.Registry.ConsumeEffectivelyOnceAsync(
            typeof(DefaultBus),
            "orders",
            "results",
            new EffectivelyOnceOptions(),
            _ => Task.FromResult(ReadOnlyMemory<byte>.Empty),
            TestContext.Current.CancellationToken);
        try
        {
            Assert.Same(harness.OldSystem, harness.Factory.SystemAt(0));

            harness.OldSystem.MarkClosed();
            await WaitUntilAsync(
                () => ReferenceEquals(harness.Supervisor.CurrentHandle, harness.NewSystem),
                TimeSpan.FromSeconds(5));
            await harness.Factory.FirstConfig.ConnectionClosedHandler("supervisor swapped the handle");

            await WaitUntilAsync(() => harness.Factory.CreateCount >= 2, TimeSpan.FromSeconds(5));
            Assert.Same(harness.NewSystem, harness.Factory.SystemAt(1));
        }
        finally
        {
            await registration.DisposeAsync();
        }
    }

    [Fact]
    public async Task Replay_reattaches_through_current_system_after_supervisor_swap()
    {
        await using var harness = await CreateAsync();
        using var cts = new CancellationTokenSource();
        var replay = harness.Registry.ReplayAsync("errors", null, cts.Token);
        try
        {
            await WaitUntilAsync(() => harness.Factory.CreateCount >= 1, TimeSpan.FromSeconds(5));
            Assert.Same(harness.OldSystem, harness.Factory.SystemAt(0));

            harness.OldSystem.MarkClosed();
            await WaitUntilAsync(
                () => ReferenceEquals(harness.Supervisor.CurrentHandle, harness.NewSystem),
                TimeSpan.FromSeconds(5));
            await harness.Factory.FirstConfig.ConnectionClosedHandler("supervisor swapped the handle");

            await WaitUntilAsync(() => harness.Factory.CreateCount >= 2, TimeSpan.FromSeconds(5));
            Assert.Same(harness.NewSystem, harness.Factory.SystemAt(1));
        }
        finally
        {
            await CancelAsync(cts, replay);
        }
    }

    [Fact]
    public async Task Replay_handler_uses_current_system_not_the_handle_captured_at_start()
    {
        await using var harness = await CreateAsync();
        using var cts = new CancellationTokenSource();
        var replay = harness.Registry.ReplayAsync("errors", null, cts.Token);
        try
        {
            await WaitUntilAsync(() => harness.Factory.CreateCount >= 1, TimeSpan.FromSeconds(5));
            harness.OldSystem.MarkClosed();
            await WaitUntilAsync(
                () => ReferenceEquals(harness.Supervisor.CurrentHandle, harness.NewSystem),
                TimeSpan.FromSeconds(5));
            var oldChecks = harness.OldSystem.ExistenceCheckCount;

            await DeliverAsync(harness.Factory.CurrentConfig, 0, Envelope("orders-0", 7, "replayed"));

            await WaitUntilAsync(() => harness.NewSystem.PublishedCount >= 1, TimeSpan.FromSeconds(5));
            Assert.Equal(oldChecks, harness.OldSystem.ExistenceCheckCount);
            Assert.True(harness.NewSystem.CheckedExistence("orders-0"));
            Assert.Single(harness.NewSystem.Producers());
        }
        finally
        {
            await CancelAsync(cts, replay);
        }
    }

    [Fact]
    public async Task Explicit_target_replay_uses_a_producer_per_source_partition()
    {
        await using var harness = await CreateAsync();
        using var cts = new CancellationTokenSource();
        var replay = harness.Registry.ReplayAsync("errors", "results", cts.Token);
        try
        {
            await WaitUntilAsync(() => harness.Factory.CreateCount >= 1, TimeSpan.FromSeconds(5));

            await DeliverAsync(harness.Factory.CurrentConfig, 0, Envelope("orders-0", 0, "first"));
            await DeliverAsync(harness.Factory.CurrentConfig, 1, Envelope("orders-1", 0, "second"));

            await WaitUntilAsync(() => harness.OldSystem.PublishedCount >= 2, TimeSpan.FromSeconds(5));
            var producers = harness.OldSystem.Producers();
            Assert.Equal(2, producers.Length);
            Assert.Equal(2, producers.Select(p => p.Reference).Distinct(StringComparer.Ordinal).Count());
            Assert.Equal(
                new[] { "first", "second" },
                producers.SelectMany(p => p.Published())
                    .Select(x => Encoding.UTF8.GetString(x.Body))
                    .OrderBy(x => x, StringComparer.Ordinal)
                    .ToArray());
        }
        finally
        {
            await CancelAsync(cts, replay);
        }
    }
}
