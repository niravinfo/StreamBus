using Microsoft.Extensions.DependencyInjection;
using RabbitMQ.Stream.Client;

namespace StreamBus.UnitTests;

// CON-3: every non-effectively-once dispatch exit path must release the inflight
// counter and the retry-blocked flag through one finally — including the
// shutdown-raced exit. A stale inflight count after shutdown skews the
// streambus_partition_inflight gauge and the replay drain's idle detection.
public sealed class DispatchShutdownReleaseTests
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
            => throw new NotSupportedException("DispatchShutdownReleaseTests inject their own consumer factory.");
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

    private static Task DeliverAsync(RawConsumerConfig config, ulong offset)
        => config.MessageHandler(
            null!,
            new MessageContext(offset, TimeSpan.Zero, 0, 0),
            new Message(Array.Empty<byte>()));

    [Fact]
    public async Task Shutdown_racing_exhaustion_handoff_releases_the_inflight_decrement()
    {
        var state = new PartitionState();
        var enteredHandoff = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var options = new ConsumeOptions
        {
            Reference = "grp",
            SingleActiveConsumer = false,
            Offset = ConsumeOffset.AlwaysFirst,
            Retry = new RetryPolicy { MaxAttempts = 1, BaseDelay = TimeSpan.FromMilliseconds(10) },
            // The exhaustion handoff signals entry, then blocks on its cancellation
            // token, so cancelling the run token races shutdown straight through the
            // exhaustion branch while the inflight counter is still held.
            OnExhausted = async (_, ct) =>
            {
                enteredHandoff.TrySetResult();
                await Task.Delay(Timeout.Infinite, ct);
            },
        };
        var system = new ScriptedSystem();
        var supervisor = new StreamBusConnectionSupervisor(
            new StreamBusOptions { Vhost = "/", UserName = "app", Password = "secret" },
            new NeverReadyConnector());
        var producers = new StreamProducerRegistry(supervisor, "/");
        var errors = new ErrorPublisher(producers);
        var factory = new ScriptedConsumerFactory();
        using var services = new ServiceCollection().BuildServiceProvider();
        var scopes = services.GetRequiredService<IServiceScopeFactory>();
        var consumer = new StreamPartitionConsumer(
            system, Stream, Stream, Partition, "grp", options,
            _ => Task.FromException(new InvalidOperationException("handler failed")),
            new MonotonicOffsetGuard(),
            state,
            StreamBusMetrics.Shared,
            "/", "bus", errors,
            logger: null,
            scopeFactory: scopes,
            consumerFactory: factory.CreateAsync);
        var cts = new CancellationTokenSource();
        var run = consumer.RunAsync(cts.Token);
        try
        {
            await consumer.FirstAttachAsync.WaitAsync(TimeSpan.FromSeconds(5), TestContext.Current.CancellationToken);

            _ = DeliverAsync(factory.FirstConfig, 0);
            await enteredHandoff.Task.WaitAsync(TimeSpan.FromSeconds(5), TestContext.Current.CancellationToken);
            Assert.Equal(1, Volatile.Read(ref state.Inflight));

            // Shutdown races the blocked handoff: dispatch must still release
            // the inflight counter and the retry-blocked flag on exit.
            await cts.CancelAsync();
            await WaitUntilAsync(
                () => Volatile.Read(ref state.Inflight) == 0 && Volatile.Read(ref state.RetryBlocked) == 0,
                TimeSpan.FromSeconds(5));
        }
        finally
        {
            await cts.CancelAsync();
            try { await run; } catch (OperationCanceledException) { }
            await consumer.DisposeAsync();
            cts.Dispose();
            services.Dispose();
        }
    }
}
