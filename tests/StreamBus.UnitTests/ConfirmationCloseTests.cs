using RabbitMQ.Stream.Client;

namespace StreamBus.UnitTests;

file sealed class SilentProducer : IProducer
{
    public readonly TaskCompletionSource Sent = new(TaskCreationOptions.RunContinuationsAsynchronously);

    public ValueTask Send(ulong publishingId, Message message)
    {
        Sent.TrySetResult();
        return ValueTask.CompletedTask;
    }

    public ValueTask Send(List<(ulong, Message)> messages) => throw new NotSupportedException("ConfirmationCloseTests send one message at a time.");
    public ValueTask Send(ulong publishingId, List<Message> subEntryMessages, CompressionType compressionType)
        => throw new NotSupportedException("ConfirmationCloseTests never send sub-entry batches.");
    public Task<ulong> GetLastPublishingId() => throw new NotSupportedException("ConfirmationCloseTests never query the last publishing id.");
    public Task<ResponseCode> Close() => Task.FromResult(ResponseCode.Ok);
    public bool IsOpen() => true;
    public void Dispose() { }
    public int MessagesSent => 0;
    public int ConfirmFrames => 0;
    public int IncomingFrames => 0;
    public int PublishCommandsSent => 0;
    public int PendingCount => 0;
    public ProducerInfo Info => throw new NotSupportedException("ConfirmationCloseTests never inspect the producer info.");
}

file sealed class NeverConfirmedEnvelopeHandle : IStreamPlainProducerHandle
{
    public int Disposals;

    public Task SendAsync(ReadOnlyMemory<byte> body, PublishOptions options, CancellationToken ct)
        => throw new NotSupportedException("ConfirmationCloseTests only publish envelopes.");

    public async Task SendEnvelopeAsync(ReadOnlyMemory<byte> body, StreamMessageProperties properties, CancellationToken ct)
        => await Task.Delay(Timeout.InfiniteTimeSpan, ct).ConfigureAwait(false);

    public ValueTask DisposeAsync()
    {
        Interlocked.Increment(ref Disposals);
        return ValueTask.CompletedTask;
    }
}

file sealed class FatalEnvelopeHandle : IStreamPlainProducerHandle
{
    public int Sends;

    public Task SendAsync(ReadOnlyMemory<byte> body, PublishOptions options, CancellationToken ct)
        => throw new NotSupportedException("ConfirmationCloseTests only publish envelopes.");

    public Task SendEnvelopeAsync(ReadOnlyMemory<byte> body, StreamMessageProperties properties, CancellationToken ct)
    {
        Sends++;
        return Task.FromException(new GenericProtocolException(ResponseCode.InternalError, "protocol"));
    }

    public ValueTask DisposeAsync() => ValueTask.CompletedTask;
}

file sealed class RecordingEnvelopeHandle : IStreamPlainProducerHandle
{
    public int Sends;

    public Task SendAsync(ReadOnlyMemory<byte> body, PublishOptions options, CancellationToken ct)
        => throw new NotSupportedException("ConfirmationCloseTests only publish envelopes.");

    public Task SendEnvelopeAsync(ReadOnlyMemory<byte> body, StreamMessageProperties properties, CancellationToken ct)
    {
        Sends++;
        return Task.CompletedTask;
    }

    public ValueTask DisposeAsync() => ValueTask.CompletedTask;
}

public sealed class ConfirmationCloseTests
{
    private static readonly IReadOnlyDictionary<string, object?> NoHeaders = new Dictionary<string, object?>();

    [Fact]
    public async Task Connection_lost_mid_wait_fails_all_pending_confirmations_transiently()
    {
        var tracker = new ConfirmationTracker();
        var producer = new SilentProducer();
        var publish = tracker.PublishAsync(producer, "orders", new Message(new byte[] { 1 }), TestContext.Current.CancellationToken);
        await producer.Sent.Task.WaitAsync(TimeSpan.FromSeconds(5), TestContext.Current.CancellationToken);

        tracker.OnConnectionLost();

        var ex = await Assert.ThrowsAsync<StreamBusTransientException>(
            () => publish.WaitAsync(TimeSpan.FromSeconds(5), TestContext.Current.CancellationToken));
        Assert.Contains("outcome unknown", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public async Task Confirmation_idempotent_when_connection_lost_repeatedly()
    {
        var tracker = new ConfirmationTracker();
        var producer = new SilentProducer();
        var publish = tracker.PublishAsync(producer, "orders", new Message(new byte[] { 2 }), TestContext.Current.CancellationToken);
        await producer.Sent.Task.WaitAsync(TimeSpan.FromSeconds(5), TestContext.Current.CancellationToken);

        tracker.OnConnectionLost();
        tracker.OnConnectionLost();

        await Assert.ThrowsAsync<StreamBusTransientException>(
            () => publish.WaitAsync(TimeSpan.FromSeconds(5), TestContext.Current.CancellationToken));
    }

    [Fact]
    public async Task Handoff_returns_transient_after_timeout_and_message_survives_on_fresh_handle()
    {
        var system = new StubSystemHandle();
        var stalled = new NeverConfirmedEnvelopeHandle();
        system.OnCreatePlain = (_, _) => Task.FromResult<IStreamPlainProducerHandle>(stalled);
        var (registry, _) = CreateRegistry(system);

        var ex = await Assert.ThrowsAsync<StreamBusTransientException>(() =>
            registry.PublishEnvelopeAsync(
                "errors",
                new byte[] { 3 },
                new StreamMessageProperties { Headers = NoHeaders },
                confirmTimeout: TimeSpan.FromMilliseconds(50),
                TestContext.Current.CancellationToken));

        Assert.Contains("outcome unknown", ex.Message, StringComparison.OrdinalIgnoreCase);
        Assert.Equal(1, stalled.Disposals);

        var fresh = new RecordingEnvelopeHandle();
        system.OnCreatePlain = (_, _) => Task.FromResult<IStreamPlainProducerHandle>(fresh);
        await registry.PublishEnvelopeAsync(
            "errors",
            new byte[] { 4 },
            new StreamMessageProperties { Headers = NoHeaders },
            confirmTimeout: TimeSpan.FromSeconds(5),
            TestContext.Current.CancellationToken);

        Assert.Equal(1, fresh.Sends);
        Assert.Equal(2, system.PlainCreations);
    }

    [Fact]
    public async Task Fatal_envelope_failure_is_not_wrapped_as_transient()
    {
        var system = new StubSystemHandle();
        var handle = new FatalEnvelopeHandle();
        system.OnCreatePlain = (_, _) => Task.FromResult<IStreamPlainProducerHandle>(handle);
        var (registry, _) = CreateRegistry(system);

        await Assert.ThrowsAsync<GenericProtocolException>(() =>
            registry.PublishEnvelopeAsync(
                "errors",
                new byte[] { 5 },
                new StreamMessageProperties { Headers = NoHeaders },
                confirmTimeout: TimeSpan.FromSeconds(5),
                TestContext.Current.CancellationToken));

        Assert.Equal(1, handle.Sends);
    }

    private static (StreamProducerRegistry Registry, StubSystemHandle System) CreateRegistry(StubSystemHandle system)
    {
        var options = new StreamBusOptions { Vhost = "/", UserName = "app", Password = "secret" };
        var supervisor = new StreamBusConnectionSupervisor(options, new ReadyConnector(system));
        supervisor.StartAsync(CancellationToken.None).GetAwaiter().GetResult();
        var readiness = supervisor.WaitUntilReadyAsync(ReadinessPolicy.Forever, TestContext.Current.CancellationToken).GetAwaiter().GetResult();
        Assert.Equal(BusReadiness.Ready, readiness);
        return (new StreamProducerRegistry(supervisor, "/"), system);
    }
}
