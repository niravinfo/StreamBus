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

file sealed class NeverConfirmedEnvelopeHandle : IStreamProducerHandle
{
    public int Disposals;

    public Task SendAsync(ReadOnlyMemory<byte> body, string? routingKey, CancellationToken ct)
        => throw new NotSupportedException("ConfirmationCloseTests only publish envelopes.");

    public async Task SendEnvelopeAsync(ReadOnlyMemory<byte> body, string? routingKey, IReadOnlyDictionary<string, object?> applicationProperties, string? messageId, string? correlationId, CancellationToken ct)
        => await Task.Delay(Timeout.InfiniteTimeSpan, ct).ConfigureAwait(false);

    public Task PublishReplayAsync(ReadOnlyMemory<byte> body, ulong publishingId, CancellationToken ct)
        => throw new NotSupportedException("ConfirmationCloseTests never replay.");

    public Task PublishResultAsync(ReadOnlyMemory<byte> body, ulong publishingId, string? routingKey, CancellationToken ct)
        => throw new NotSupportedException("ConfirmationCloseTests never publish results.");

    public ValueTask DisposeAsync()
    {
        Interlocked.Increment(ref Disposals);
        return ValueTask.CompletedTask;
    }
}

file sealed class RecordingEnvelopeHandle : IStreamProducerHandle
{
    public int Sends;

    public Task SendAsync(ReadOnlyMemory<byte> body, string? routingKey, CancellationToken ct)
        => throw new NotSupportedException("ConfirmationCloseTests only publish envelopes.");

    public Task SendEnvelopeAsync(ReadOnlyMemory<byte> body, string? routingKey, IReadOnlyDictionary<string, object?> applicationProperties, string? messageId, string? correlationId, CancellationToken ct)
    {
        Sends++;
        return Task.CompletedTask;
    }

    public Task PublishReplayAsync(ReadOnlyMemory<byte> body, ulong publishingId, CancellationToken ct)
        => throw new NotSupportedException("ConfirmationCloseTests never replay.");

    public Task PublishResultAsync(ReadOnlyMemory<byte> body, ulong publishingId, string? routingKey, CancellationToken ct)
        => throw new NotSupportedException("ConfirmationCloseTests never publish results.");

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
        var publish = tracker.PublishWithIdAsync(
            producer, "orders", 42UL, new Message(new byte[] { 1 }), TestContext.Current.CancellationToken);
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
        var publish = tracker.PublishWithIdAsync(
            producer, "orders", 7UL, new Message(new byte[] { 2 }), TestContext.Current.CancellationToken);
        await producer.Sent.Task.WaitAsync(TimeSpan.FromSeconds(5), TestContext.Current.CancellationToken);

        tracker.OnConnectionLost();
        tracker.OnConnectionLost();

        await Assert.ThrowsAsync<StreamBusTransientException>(
            () => publish.WaitAsync(TimeSpan.FromSeconds(5), TestContext.Current.CancellationToken));
    }

    [Fact]
    public async Task Handoff_returns_transient_after_timeout_and_message_stays_replayable()
    {
        var system = new StubSystemHandle();
        var stalled = new NeverConfirmedEnvelopeHandle();
        system.OnCreatePlain = (_, _) => Task.FromResult<IStreamProducerHandle>(stalled);
        var (registry, _) = CreateRegistry(system);

        var ex = await Assert.ThrowsAsync<StreamBusTransientException>(() =>
            registry.PublishEnvelopeAsync(
                "errors",
                new byte[] { 3 },
                routingKey: null,
                NoHeaders,
                messageId: null,
                correlationId: null,
                confirmTimeout: TimeSpan.FromMilliseconds(50),
                TestContext.Current.CancellationToken));

        Assert.Contains("outcome unknown", ex.Message, StringComparison.OrdinalIgnoreCase);
        Assert.Equal(1, stalled.Disposals);

        var replay = new RecordingEnvelopeHandle();
        system.OnCreatePlain = (_, _) => Task.FromResult<IStreamProducerHandle>(replay);
        await registry.PublishEnvelopeAsync(
            "errors",
            new byte[] { 4 },
            routingKey: null,
            NoHeaders,
            messageId: null,
            correlationId: null,
            confirmTimeout: TimeSpan.FromSeconds(5),
            TestContext.Current.CancellationToken);

        Assert.Equal(1, replay.Sends);
        Assert.Equal(2, system.PlainCreations);
    }

    private static (StreamProducerRegistry Registry, StubSystemHandle System) CreateRegistry(StubSystemHandle system)
    {
        var options = new StreamBusOptions { Vhost = "/", UserName = "app", Password = "secret" };
        var supervisor = new StreamBusConnectionSupervisor(options, new ReadyConnector(system));
        supervisor.StartAsync(CancellationToken.None).GetAwaiter().GetResult();
        var readiness = supervisor.WaitUntilReadyAsync(ReadinessPolicy.Indefinitely, TestContext.Current.CancellationToken).GetAwaiter().GetResult();
        Assert.Equal(BusReadiness.Ready, readiness);
        return (new StreamProducerRegistry(supervisor, "/"), system);
    }
}
