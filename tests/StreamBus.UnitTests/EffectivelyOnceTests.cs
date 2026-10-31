using RabbitMQ.Stream.Client;

namespace StreamBus.UnitTests;

public sealed class EffectivelyOnceTests
{
    [Fact]
    public void Producer_name_is_stable_and_meaningful()
    {
        var a = ResultNaming.ProducerName("orders-0", "orders.done");
        Assert.Equal(a, ResultNaming.ProducerName("orders-0", "orders.done"));
        Assert.Contains("orders-0", a);
        Assert.Contains("orders.done", a);
    }

    [Fact]
    public void Producer_name_differs_per_partition_and_target()
    {
        Assert.NotEqual(ResultNaming.ProducerName("s-0", "t"), ResultNaming.ProducerName("s-1", "t"));
        Assert.NotEqual(ResultNaming.ProducerName("s-0", "t1"), ResultNaming.ProducerName("s-0", "t2"));
    }

    [Fact]
    public void Producer_name_never_exceeds_255_bytes()
    {
        var longStream = new string('x', 300);
        var name = ResultNaming.ProducerName(longStream, longStream);
        Assert.True(System.Text.Encoding.UTF8.GetByteCount(name) <= 255);
        Assert.StartsWith("streambus-eo-", name);
        Assert.Equal(name, ResultNaming.ProducerName(longStream, longStream));
    }

    [Fact]
    public void Replay_producer_name_is_stable_bounded_and_partition_scoped()
    {
        var name = StreamBusNaming.ReplayProducerName("orders", "orders-2");
        Assert.StartsWith("streambus-replay-", name);
        Assert.Equal(name, StreamBusNaming.ReplayProducerName("orders", "orders-2"));
        Assert.NotEqual(name, StreamBusNaming.ReplayProducerName("orders", "orders-3"));
        Assert.True(System.Text.Encoding.UTF8.GetByteCount(name) <= StreamBusNaming.MaxName);
    }

    [Fact]
    public void Consumer_reference_is_stable_per_group()
    {
        var a = ResultNaming.ConsumerReference(typeof(DefaultBus), "src", "dst");
        Assert.Equal(a, ResultNaming.ConsumerReference(typeof(DefaultBus), "src", "dst"));
        Assert.NotEqual(a, ResultNaming.ConsumerReference(typeof(DefaultBus), "src", "other"));
    }

    [Fact]
    public void Publishing_id_equals_source_offset() => Assert.Equal(41UL, ResultNaming.PublishingId(41));

    [Fact]
    public void Consume_rejects_bounded()
    {
        Assert.Throws<StreamBusConfigurationException>(() =>
            EffectivelyOnceValidation.ValidateConsume(new RetryPolicy { MaxAttempts = 3 }));
    }

    [Fact]
    public void Consume_accepts_default_unbounded()
    {
        EffectivelyOnceValidation.ValidateConsume(new RetryPolicy());
    }

    [Fact]
    public void Consume_rejects_non_positive_base_delay()
    {
        var ex = Assert.Throws<StreamBusConfigurationException>(() =>
            EffectivelyOnceValidation.ValidateConsume(new RetryPolicy { BaseDelay = TimeSpan.Zero }));
        Assert.Contains("BaseDelay", ex.Message);
    }

    [Fact]
    public void Publish_result_rejects_missing_name()
    {
        Assert.ThrowsAny<Exception>(() =>
            EffectivelyOnceValidation.ValidatePublishResult(new PublishResultOptions { ProducerName = "  " }));
    }

    [Fact]
    public void Publish_result_rejects_overlong_name()
    {
        Assert.Throws<StreamBusConfigurationException>(() =>
            EffectivelyOnceValidation.ValidatePublishResult(new PublishResultOptions { ProducerName = new string('y', 300) }));
    }

    [Fact]
    public void Publish_result_rejects_super_without_routing()
    {
        var ex = Assert.Throws<StreamBusConfigurationException>(() =>
            EffectivelyOnceValidation.ValidatePublishResult(new PublishResultOptions { ProducerName = "n", IsSuperStream = true }));
        Assert.Contains("RoutingKey", ex.Message);
    }

    [Fact]
    public void Publish_result_rejects_bounded_retry()
    {
        var ex = Assert.Throws<StreamBusConfigurationException>(() =>
            EffectivelyOnceValidation.ValidatePublishResult(new PublishResultOptions
            {
                ProducerName = "n",
                Retry = new RetryPolicy { MaxAttempts = 3 },
            }));
        Assert.Contains("MaxAttempts", ex.Message);
    }

    [Fact]
    public void Resume_is_max_last_id()
    {
        Assert.Equal(7UL, ResultResume.ResumeOffset(new ulong?[] { 3, 7, 5 }));
    }

    [Fact]
    public void Resume_skips_unknown_partitions()
    {
        Assert.Equal(3UL, ResultResume.ResumeOffset(new ulong?[] { null, 3 }));
    }

    [Fact]
    public void Resume_is_null_when_nothing_published()
    {
        Assert.Null(ResultResume.ResumeOffset(Array.Empty<ulong?>()));
        Assert.Null(ResultResume.ResumeOffset(new ulong?[] { null }));
    }

    private sealed class StubHandle : IStreamSuperProducerHandle
    {
        public readonly List<(ulong Id, string? Routing)> Published = new();
        public Task SendAsync(ReadOnlyMemory<byte> body, PublishOptions options, CancellationToken ct) => throw new NotImplementedException("EO tests publish results only.");
        public Task SendEnvelopeAsync(ReadOnlyMemory<byte> body, StreamMessageProperties properties, CancellationToken ct) => throw new NotImplementedException("EO tests publish results only.");
        public Task PublishReplayAsync(ReadOnlyMemory<byte> body, ulong publishingId, PublishOptions options, CancellationToken ct)
        {
            Record(publishingId, null);
            return Task.CompletedTask;
        }
        public Task PublishWithIdAsync(ulong publishingId, ReadOnlyMemory<byte> body, PublishOptions options, CancellationToken ct)
        {
            Record(publishingId, options.RoutingKey);
            return Task.CompletedTask;
        }
        public Task PublishResultAsync(ReadOnlyMemory<byte> body, ulong publishingId, string? routingKey, CancellationToken ct)
        {
            Record(publishingId, routingKey);
            return Task.CompletedTask;
        }
        private void Record(ulong publishingId, string? routingKey)
        {
            lock (Published) Published.Add((publishingId, routingKey));
        }
        public ValueTask DisposeAsync() => ValueTask.CompletedTask;
    }

    private sealed class StubSystem : IStreamSystemHandle
    {
        public readonly Dictionary<string, StubHandle> Created = new();
        public readonly Dictionary<string, ulong> Sequences = new();
        public string[] Partitions = new[] { "p" };
        public bool IsClosed => false;
        public Task<IStreamPlainProducerHandle> CreatePlainProducerAsync(string stream, CancellationToken ct) => throw new NotImplementedException("EO tests use stable names.");
        public Task<IStreamPlainProducerHandle> CreatePlainProducerWithReferenceAsync(string stream, string reference, CancellationToken ct)
        {
            var h = new StubHandle();
            Created[$"plain:{stream}:{reference}"] = h;
            return Task.FromResult<IStreamPlainProducerHandle>(h);
        }
        public Task<IStreamSuperProducerHandle> CreateSuperStreamProducerAsync(string stream, CancellationToken ct) => throw new NotImplementedException("EO tests use stable names.");
        public Task<IStreamSuperProducerHandle> CreateSuperStreamProducerWithReferenceAsync(string stream, string reference, CancellationToken ct)
        {
            var h = new StubHandle();
            Created[$"super:{stream}:{reference}"] = h;
            return Task.FromResult<IStreamSuperProducerHandle>(h);
        }
        public Task<ulong?> TryQueryOffsetAsync(string reference, string stream, CancellationToken ct) => Task.FromResult<ulong?>(null);
        public Task StoreSystemOffsetAsync(string reference, string stream, ulong offset, CancellationToken ct) => Task.CompletedTask;
        public Task<bool> StreamExistsAsync(string stream, CancellationToken ct) => Task.FromResult(true);
        public Task<bool> SuperStreamExistsAsync(string superStream, CancellationToken ct) => Task.FromResult(false);
        public Task<string[]> QueryPartitionAsync(string superStream, CancellationToken ct) => Task.FromResult(Partitions);
        public Task<ulong> QuerySequenceAsync(string reference, string stream, CancellationToken ct)
            => Sequences.TryGetValue($"{reference}@{stream}", out var v)
                ? Task.FromResult(v)
                : Task.FromException<ulong>(new InvalidOperationException($"no sequence for {reference}@{stream}"));
        public Task<RawConsumer> CreateRawConsumerAsync(RawConsumerConfig config, CancellationToken ct) => throw new NotImplementedException("EO unit tests never attach.");
        public Task CreateStreamAsync(StreamSpec spec, CancellationToken ct) => throw new NotImplementedException("EffectivelyOnceTests never declares topology.");
        public Task CreateSuperStreamAsync(PartitionsSuperStreamSpec spec, CancellationToken ct) => throw new NotImplementedException("EffectivelyOnceTests never declares topology.");
        public ValueTask DisposeAsync() => ValueTask.CompletedTask;
    }

    private static (ResultProducerRegistry Registry, StubSystem System) CreateRegistry()
    {
        var system = new StubSystem();
        var options = new StreamBusOptions { Vhost = "/", UserName = "app", Password = "secret" };
        var supervisor = new StreamBusConnectionSupervisor(options, new ReadyConnector(system));
        supervisor.StartAsync(CancellationToken.None).GetAwaiter().GetResult();
        var readiness = supervisor.WaitUntilReadyAsync(ReadinessPolicy.Indefinitely, new CancellationTokenSource(TimeSpan.FromSeconds(5)).Token).GetAwaiter().GetResult();
        Assert.Equal(BusReadiness.Ready, readiness);
        return (new ResultProducerRegistry(supervisor, "/"), system);
    }

    private sealed class ReadyConnector : IStreamSystemConnector
    {
        private readonly StubSystem _handle;
        public ReadyConnector(StubSystem handle) => _handle = handle;
        public Task<IStreamSystemHandle> ConnectAsync(StreamBusOptions options, CancellationToken ct) => Task.FromResult<IStreamSystemHandle>(_handle);
    }

    [Fact]
    public async Task Stable_producer_is_reused_and_carries_source_offset_as_id()
    {
        var (registry, system) = CreateRegistry();
        var ct = TestContext.Current.CancellationToken;
        await registry.PublishResultAsync("dst", false, new byte[] { 1 }, 7, null, "grp", ct);
        await registry.PublishResultAsync("dst", false, new byte[] { 2 }, 8, null, "grp", ct);
        var handle = Assert.Single(system.Created.Values);
        Assert.Equal(new[] { 7UL, 8UL }, handle.Published.Select(p => p.Id).ToList());
    }

    [Fact]
    public async Task Duplicate_in_flight_id_fails_loudly()
    {
        // FR-6.3 forbids concurrent publishing under one name. The managed path is
        // sequential by construction (dispatch gate + SAC); this pins the loud
        // failure for manual misuse at the level where it lives: the pending-id
        // table in ConfirmationTracker rejects the second same-id publish.
        var tracker = new ConfirmationTracker();
        var gate = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var producer = new GateProducer(gate.Task);
        using var timeout = new CancellationTokenSource();
        var first = tracker.PublishWithIdAsync(producer, "dst", 5, new Message(new byte[] { 1 }), timeout.Token);
        await Task.Delay(200, TestContext.Current.CancellationToken);
        var ex = await Assert.ThrowsAsync<StreamBusPublishException>(() =>
            tracker.PublishWithIdAsync(producer, "dst", 5, new Message(new byte[] { 2 }), timeout.Token));
        Assert.Contains("already in flight", ex.Message);
        gate.TrySetResult();
        // Nothing confirms the first publish (no ConfirmHandler wired in the
        // fake), so cancel its token: the tracker's ct-registration fires and
        // the await propagates OperationCanceledException instead of hanging.
        timeout.Cancel();
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => first);
    }

    private sealed class GateProducer : IProducer
    {
        private readonly Task _gate;
        public GateProducer(Task gate) => _gate = gate;
        public async ValueTask Send(ulong publishingId, Message message) => await _gate;
        public ValueTask Send(List<(ulong, Message)> messages) => throw new NotImplementedException("GateProducer handles single sends only.");
        public ValueTask Send(ulong publishingId, List<Message> subEntryMessages, CompressionType compressionType) => throw new NotImplementedException("GateProducer handles single sends only.");
        public Task<ulong> GetLastPublishingId() => throw new NotImplementedException("GateProducer never queries the last id.");
        public bool IsOpen() => throw new NotImplementedException("GateProducer never reports openness.");
        public void Dispose() => throw new NotImplementedException("GateProducer is never disposed.");
        public Task<ResponseCode> Close() => throw new NotImplementedException("GateProducer is never closed.");
        public int MessagesSent => throw new NotImplementedException("GateProducer never reports counters.");
        public int ConfirmFrames => throw new NotImplementedException("GateProducer never reports counters.");
        public int IncomingFrames => throw new NotImplementedException("GateProducer never reports counters.");
        public int PublishCommandsSent => throw new NotImplementedException("GateProducer never reports counters.");
        public int PendingCount => throw new NotImplementedException("GateProducer never reports counters.");
        public ProducerInfo Info => throw new NotImplementedException("GateProducer never reports info.");
    }

    [Fact]
    public async Task Producer_name_cap_is_enforced_per_target()
    {
        var (registry, _) = CreateRegistry();
        var ct = TestContext.Current.CancellationToken;
        for (var i = 0; i < ResultProducerRegistry.MaxProducerNamesPerStream; i++)
            await registry.GetOrCreateAsync("dst", false, $"name-{i}", ct);
        var ex = await Assert.ThrowsAsync<StreamBusConfigurationException>(() =>
            registry.GetOrCreateAsync("dst", false, "name-overflow", ct));
        Assert.Contains("255", ex.Message);
        // A different target is unaffected.
        await registry.GetOrCreateAsync("other", false, "name-overflow", ct);
    }

    [Fact]
    public async Task Resume_returns_last_plus_one()
    {
        var (registry, system) = CreateRegistry();
        system.Sequences["grp@dst"] = 12;
        Assert.Equal(12UL, await registry.GetResumeOffsetAsync("dst", false, "grp", TestContext.Current.CancellationToken));
    }

    [Fact]
    public async Task Resume_maps_zero_to_never_published()
    {
        var (registry, system) = CreateRegistry();
        system.Sequences["grp@dst"] = 0;
        Assert.Null(await registry.GetResumeOffsetAsync("dst", false, "grp", TestContext.Current.CancellationToken));
    }

    [Fact]
    public async Task Resume_is_null_when_never_published()
    {
        var (registry, _) = CreateRegistry();
        Assert.Null(await registry.GetResumeOffsetAsync("dst", false, "unknown", TestContext.Current.CancellationToken));
    }

    [Fact]
    public async Task Resume_takes_max_across_super_partitions_and_skips_unknown()
    {
        var (registry, system) = CreateRegistry();
        system.Partitions = new[] { "t-0", "t-1", "t-2" };
        system.Sequences["grp@t-0"] = 4;
        system.Sequences["grp@t-2"] = 9;
        Assert.Equal(9UL, await registry.GetResumeOffsetAsync("t", true, "grp", TestContext.Current.CancellationToken));
    }

    [Fact]
    public async Task Resume_is_null_when_no_partition_known()
    {
        var (registry, system) = CreateRegistry();
        system.Partitions = new[] { "t-0", "t-1" };
        Assert.Null(await registry.GetResumeOffsetAsync("t", true, "grp", TestContext.Current.CancellationToken));
    }
}
