using RabbitMQ.Stream.Client;

namespace StreamBus.UnitTests;

// CON-1: a handler that throws OperationCanceledException from its own token (per-delivery
// CTS, DB timeout wrapped as OCE) must be treated as a real handler failure with a visible
// attach recycle — not retried inside the dispatch gate forever.
public sealed class DispatchForeignCancellationTests
{
    private const string Stream = "orders";
    private const int Partition = 0;

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

    private sealed class ScriptedSystem : IStreamSystemHandle
    {
        public bool IsClosed => false;
        public Task<ulong?> TryQueryOffsetAsync(string reference, string stream, CancellationToken ct)
            => Task.FromResult<ulong?>(null);
        public Task<RawConsumer> CreateRawConsumerAsync(RawConsumerConfig config, CancellationToken ct)
            => throw new NotSupportedException("DispatchForeignCancellationTests inject their own consumer factory.");
        public Task<IStreamPlainProducerHandle> CreatePlainProducerAsync(string stream, CancellationToken ct) => throw new NotSupportedException();
        public Task<IStreamPlainProducerHandle> CreatePlainProducerWithReferenceAsync(string stream, string reference, CancellationToken ct) => throw new NotSupportedException();
        public Task<IStreamSuperProducerHandle> CreateSuperStreamProducerAsync(string stream, CancellationToken ct) => throw new NotSupportedException();
        public Task<IStreamSuperProducerHandle> CreateSuperStreamProducerWithReferenceAsync(string stream, string reference, CancellationToken ct) => throw new NotSupportedException();
        public Task<ulong> QuerySequenceAsync(string reference, string stream, CancellationToken ct) => throw new NotSupportedException();
        public Task StoreSystemOffsetAsync(string reference, string stream, ulong offset, CancellationToken ct) => Task.CompletedTask;
        public Task<bool> StreamExistsAsync(string stream, CancellationToken ct) => Task.FromResult(true);
        public Task<bool> SuperStreamExistsAsync(string superStream, CancellationToken ct) => Task.FromResult(false);
        public Task<string[]> QueryPartitionAsync(string superStream, CancellationToken ct) => throw new NotSupportedException();
        public Task CreateStreamAsync(StreamSpec spec, CancellationToken ct) => Task.CompletedTask;
        public Task CreateSuperStreamAsync(PartitionsSuperStreamSpec spec, CancellationToken ct) => Task.CompletedTask;
        public ValueTask DisposeAsync() => ValueTask.CompletedTask;
    }

    private sealed class NeverReadyConnector : IStreamSystemConnector
    {
        public Task<IStreamSystemHandle> ConnectAsync(StreamBusOptions options, CancellationToken ct)
            => new TaskCompletionSource<IStreamSystemHandle>().Task;
    }

    private static (StreamPartitionConsumer Consumer, ScriptedConsumerFactory Factory, Task Run, CancellationTokenSource Cts) Create(
        Func<ConsumeContext, Task> handler, ConsumeOptions options)
    {
        var system = new ScriptedSystem();
        var supervisor = new StreamBusConnectionSupervisor(
            new StreamBusOptions { Vhost = "/", UserName = "app", Password = "secret" },
            new NeverReadyConnector());
        var producers = new StreamProducerRegistry(supervisor, "/");
        var errors = new ErrorPublisher(producers);
        var factory = new ScriptedConsumerFactory();
        var consumer = new StreamPartitionConsumer(
            system, Stream, Stream, Partition, "grp", options,
            handler,
            new MonotonicOffsetGuard(),
            new PartitionState(),
            StreamBusMetrics.Shared,
            "/", "bus", errors,
            logger: null,
            consumerFactory: factory.CreateAsync);
        var cts = new CancellationTokenSource();
        var run = consumer.RunAsync(cts.Token);
        return (consumer, factory, run, cts);
    }

    private static Task DeliverAsync(RawConsumerConfig config, ulong offset)
        => config.MessageHandler(
            null!,
            new MessageContext(offset, TimeSpan.Zero, 0, 0),
            new Message(Array.Empty<byte>()));

    private static async Task DisposeAsync(
        StreamPartitionConsumer consumer, Task run, CancellationTokenSource cts)
    {
        await cts.CancelAsync();
        try { await run; } catch (OperationCanceledException) { }
        await consumer.DisposeAsync();
        cts.Dispose();
    }

    [Fact]
    public async Task Handler_foreign_token_OCE_recycles_attach_instead_of_retrying_forever()
    {
        var options = new ConsumeOptions
        {
            Reference = "grp",
            SingleActiveConsumer = false,
            Offset = ConsumeOffset.AlwaysFirst,
            // Unbounded retry (the dangerous default CON-1 calls out): with the bug, the
            // foreign-token OCE is retried forever and the attach is never recycled.
            Retry = new RetryPolicy { BaseDelay = TimeSpan.FromMilliseconds(10) },
        };
        var first = new TaskCompletionSource();
        Func<ConsumeContext, Task> handler = _ =>
        {
            first.TrySetResult();
            throw new OperationCanceledException(); // neither token is the loop token
        };
        var (consumer, factory, run, cts) = Create(handler, options);
        try
        {
            await consumer.FirstAttachAsync.WaitAsync(TimeSpan.FromSeconds(5), TestContext.Current.CancellationToken);

            // Fire-and-forget: the bugged dispatch gate never completes the delivery.
            _ = DeliverAsync(factory.FirstConfig, 0);
            await first.Task.WaitAsync(TimeSpan.FromSeconds(5), TestContext.Current.CancellationToken);

            // RED expectation: the attach fails visibly (close + backoff + recreate).
            await WaitUntilAsync(() => factory.CreateCount >= 2, TimeSpan.FromSeconds(5));
        }
        finally
        {
            await DisposeAsync(consumer, run, cts);
        }
    }
}
