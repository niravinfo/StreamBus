using System.Diagnostics.Metrics;
using RabbitMQ.Stream.Client;

namespace StreamBus.UnitTests;

public sealed class AcceptanceTests : IDisposable
{
    private readonly MeterListener _listener = new();
    private readonly List<(string Instrument, double Value, KeyValuePair<string, object?>[] Tags)> _measurements = new();

    public AcceptanceTests()
    {
        _listener.InstrumentPublished = (instrument, listener) =>
        {
            if (instrument.Meter.Name == "StreamBus")
                listener.EnableMeasurementEvents(instrument);
        };
        _listener.SetMeasurementEventCallback<double>(Record);
        _listener.SetMeasurementEventCallback<long>(Record);
        _listener.SetMeasurementEventCallback<int>(Record);
        _listener.Start();
    }

    private void Record<T>(Instrument instrument, T measurement, ReadOnlySpan<KeyValuePair<string, object?>> tags, object? state)
    {
        lock (_measurements) _measurements.Add((instrument.Name, Convert.ToDouble(measurement), tags.ToArray()));
    }

    public void Dispose() => _listener.Dispose();

    [Fact]
    public void Regressed_counter_emits_with_catalog_name()
    {
        StreamBusMetrics.Shared.RecordRegressed("acc-vh", "acc-bus", "acc-s", 0, "acc-r");
        Assert.Contains(_measurements, m => m.Instrument == "streambus_offset_regressed_total"
            && m.Tags.Any(t => t.Key == "vhost" && (string?)t.Value == "acc-vh")
            && m.Tags.Any(t => t.Key == "bus" && (string?)t.Value == "acc-bus"));
    }

    [Fact]
    public void Connection_state_observes_current_state()
    {
        var state = BusConnectionState.Starting;
        StreamBusMetrics.Shared.TrackConnection("acc-vh", "acc-bus", () => state);
        state = BusConnectionState.Ready;
        _listener.RecordObservableInstruments();
        double first;
        lock (_measurements) first = _measurements.Last(m => m.Instrument == "streambus_connection_state"
            && m.Tags.Any(t => t.Key == "vhost" && (string?)t.Value == "acc-vh")
            && m.Tags.Any(t => t.Key == "bus" && (string?)t.Value == "acc-bus")).Value;
        state = BusConnectionState.TransientFailure;
        _listener.RecordObservableInstruments();
        double second;
        lock (_measurements) second = _measurements.Last(m => m.Instrument == "streambus_connection_state"
            && m.Tags.Any(t => t.Key == "vhost" && (string?)t.Value == "acc-vh")
            && m.Tags.Any(t => t.Key == "bus" && (string?)t.Value == "acc-bus")).Value;
        Assert.NotEqual(first, second);
    }

    [Fact]
    public void Offset_store_defaults_match_spec()
    {
        Assert.Equal(1, new OffsetStoreOptions().EveryMessages);
        Assert.Null(new OffsetStoreOptions().MaxInterval);
        Assert.Equal(1, new ConsumeOptions().OffsetStore.EveryMessages);
    }

    private sealed class CountingHandle : IStreamProducerHandle
    {
        public int SendCount;
        public Task SendAsync(ReadOnlyMemory<byte> body, string? routingKey, CancellationToken ct)
        {
            Interlocked.Increment(ref SendCount);
            return Task.CompletedTask;
        }
        public Task SendEnvelopeAsync(ReadOnlyMemory<byte> body, string? routingKey, IReadOnlyDictionary<string, object?> applicationProperties, string? messageId, string? correlationId, CancellationToken ct)
            => throw new NotImplementedException("Acceptance tests publish plain only.");
        public Task PublishReplayAsync(ReadOnlyMemory<byte> body, ulong publishingId, CancellationToken ct)
            => throw new NotImplementedException("Acceptance tests publish plain only.");
        public Task PublishResultAsync(ReadOnlyMemory<byte> body, ulong publishingId, string? routingKey, CancellationToken ct)
            => throw new NotImplementedException("Acceptance tests publish plain only.");
        public ValueTask DisposeAsync() => ValueTask.CompletedTask;
    }

    private sealed class ScriptSystemHandle : IStreamSystemHandle
    {
        private readonly CountingHandle _handle;
        public ScriptSystemHandle(CountingHandle handle) => _handle = handle;
        public bool IsClosed => false;
        public Task<IStreamProducerHandle> CreatePlainProducerAsync(string stream, CancellationToken ct) => Task.FromResult<IStreamProducerHandle>(_handle);
        public Task<IStreamProducerHandle> CreatePlainProducerWithReferenceAsync(string stream, string reference, CancellationToken ct) => throw new NotImplementedException("Acceptance tests use the unique-name path.");
        public Task<IStreamProducerHandle> CreateSuperStreamProducerAsync(string stream, CancellationToken ct) => throw new NotImplementedException("Acceptance tests publish plain only.");
        public Task<IStreamProducerHandle> CreateSuperStreamProducerWithReferenceAsync(string stream, string reference, CancellationToken ct) => throw new NotImplementedException("Acceptance tests publish plain only.");
        public Task<RawConsumer> CreateRawConsumerAsync(RawConsumerConfig config, CancellationToken ct) => throw new NotImplementedException("Acceptance tests never attach.");
        public Task<ulong?> TryQueryOffsetAsync(string reference, string stream, CancellationToken ct) => Task.FromResult<ulong?>(null);
        public Task<ulong> QuerySequenceAsync(string reference, string stream, CancellationToken ct) => Task.FromResult(0UL);
        public Task StoreSystemOffsetAsync(string reference, string stream, ulong offset, CancellationToken ct) => Task.CompletedTask;
        public Task<bool> StreamExistsAsync(string stream, CancellationToken ct) => Task.FromResult(true);
        public Task<bool> SuperStreamExistsAsync(string superStream, CancellationToken ct) => Task.FromResult(false);
        public Task<string[]> QueryPartitionAsync(string superStream, CancellationToken ct) => Task.FromResult(new[] { superStream });
        public Task CreateStreamAsync(StreamSpec spec, CancellationToken ct) => Task.CompletedTask;
        public Task CreateSuperStreamAsync(PartitionsSuperStreamSpec spec, CancellationToken ct) => Task.CompletedTask;
        public ValueTask DisposeAsync() => ValueTask.CompletedTask;
    }

    private sealed class DelayedConnector : IStreamSystemConnector
    {
        private readonly ScriptSystemHandle _handle;
        private readonly TimeSpan _delay;
        public DelayedConnector(ScriptSystemHandle handle, TimeSpan delay) { _handle = handle; _delay = delay; }
        public async Task<IStreamSystemHandle> ConnectAsync(StreamBusOptions options, CancellationToken ct)
        {
            await Task.Delay(_delay, ct);
            return _handle;
        }
    }

    [Fact]
    public async Task WaitUpTo_times_out_at_publish_then_Indefinitely_waits()
    {
        // FR-8.6: per-call-site policies at a publish call site (FailFast is pinned
        // by Not_ready_failfast_throws_not_ready; confirm timeout stays independent —
        // PublishOptions.Timeout is untouched here and no confirm elapses).
        var handle = new CountingHandle();
        var options = new StreamBusOptions { Vhost = "/", UserName = "app", Password = "secret" };
        await using var supervisor = new StreamBusConnectionSupervisor(options, new DelayedConnector(new ScriptSystemHandle(handle), TimeSpan.FromMilliseconds(500)));
        await supervisor.StartAsync(CancellationToken.None);
        var registry = new StreamProducerRegistry(supervisor, "/");
        var ct = TestContext.Current.CancellationToken;
        await Assert.ThrowsAsync<StreamBusNotReadyException>(() =>
            registry.PublishAsync("s", new byte[] { 1 }, new PublishOptions { ReadinessPolicy = ReadinessPolicy.UpTo(TimeSpan.FromMilliseconds(100)) }, ct));
        await registry.PublishAsync("s", new byte[] { 2 }, new PublishOptions { ReadinessPolicy = ReadinessPolicy.Indefinitely }, ct);
        Assert.Equal(1, handle.SendCount);
    }

    private sealed class RecordingSuperProducer : ISuperStreamProducer
    {
        public readonly List<(ulong Id, Message Message)> Sent = new();
        public ValueTask Send(ulong publishingId, Message message)
        {
            lock (Sent) Sent.Add((publishingId, message));
            return ValueTask.CompletedTask;
        }
        // Remaining ISuperStreamProducer members per reflection probe on 1.12.1
        // (only the single-message Send above does work; everything else throws):
        // Send(UInt64, Message); Send(List<ValueTuple<UInt64, Message>>);
        // Send(UInt64, List<Message>, CompressionType); GetLastPublishingId;
        // IsOpen; Dispose; Close; MessagesSent/ConfirmFrames/IncomingFrames/
        // PublishCommandsSent/PendingCount; Info; ReconnectPartition(StreamInfo).
        public ValueTask Send(List<(ulong, Message)> messages) => throw new NotImplementedException("Acceptance routing test sends single messages.");
        public ValueTask Send(ulong publishingId, List<Message> subEntryMessages, CompressionType compressionType) => throw new NotImplementedException("Acceptance routing test sends single messages.");
        public Task<ulong> GetLastPublishingId() => Task.FromResult(0UL);
        public bool IsOpen() => true;
        public void Dispose() { }
        public Task<ResponseCode> Close() => throw new NotImplementedException("Acceptance routing test never closes.");
        public int MessagesSent => throw new NotImplementedException("Acceptance routing test never reports counters.");
        public int ConfirmFrames => throw new NotImplementedException("Acceptance routing test never reports counters.");
        public int IncomingFrames => throw new NotImplementedException("Acceptance routing test never reports counters.");
        public int PublishCommandsSent => throw new NotImplementedException("Acceptance routing test never reports counters.");
        public int PendingCount => throw new NotImplementedException("Acceptance routing test never reports counters.");
        public ProducerInfo Info => throw new NotImplementedException("Acceptance routing test never reports info.");
        public Task ReconnectPartition(StreamInfo streamInfo) => throw new NotImplementedException("Acceptance routing test never reconnects partitions.");
    }

    [Fact]
    public async Task Super_routing_passes_only_the_routing_value()
    {
        // FR-3.2 library half: the outgoing routing equals exactly the passed key
        // (same key + different bodies agree; different keys are preserved as-is).
        // Partition selection itself lives in the client (pinned end to end by
        // Super_stream_keys_land_on_stable_partitions).
        var confirmations = new ConfirmationTracker();
        var producer = new RecordingSuperProducer();
        var handle = new SuperStreamProducerHandle(producer, "events", confirmations);
        var ct = TestContext.Current.CancellationToken;
        await SendAndConfirmAsync(handle, confirmations, producer, new byte[] { 1 }, "key-a", ct);
        await SendAndConfirmAsync(handle, confirmations, producer, new byte[] { 2 }, "key-a", ct);
        await SendAndConfirmAsync(handle, confirmations, producer, new byte[] { 3 }, "key-b", ct);
    }

    private static async Task SendAndConfirmAsync(IStreamProducerHandle handle, ConfirmationTracker tracker, RecordingSuperProducer producer, byte[] body, string routing, CancellationToken ct)
    {
        int before;
        lock (producer.Sent) before = producer.Sent.Count;
        var send = handle.SendAsync(body, routing, ct);
        ulong id;
        Message recorded;
        while (true)
        {
            ct.ThrowIfCancellationRequested();
            lock (producer.Sent)
            {
                if (producer.Sent.Count > before)
                {
                    id = producer.Sent[^1].Id;
                    recorded = producer.Sent[^1].Message;
                    break;
                }
            }
            await Task.Delay(10, ct);
        }
        Assert.Equal(routing, SuperStreamProducerHandle.RoutingFor(recorded));
        tracker.Complete(new Confirmation { PublishingId = id, Code = ResponseCode.Ok });
        await send;
    }
}
