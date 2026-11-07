using Microsoft.Extensions.DependencyInjection;
using RabbitMQ.Stream.Client;

namespace StreamBus.UnitTests;

// CON-13: _dispatchGate serializes dispatches but does not cancel queued waiters.
// A dispatch that was queued before DisposeAsync started can acquire the gate
// after teardown began (the attach is not necessarily drained — it belongs to
// the current generation), so it must observe the disposed state and exit
// before reaching the handler or touching attach/context state.
public sealed class DisposeDispatchTests
{
    private const string Stream = "orders";
    private const int Partition = 0;

    private sealed class ScriptedSystem : NeverReadyConsumerSystem
    {
        public override Task<RawConsumer> CreateRawConsumerAsync(RawConsumerConfig config, CancellationToken ct)
            => throw new NotSupportedException("DisposeDispatchTests inject their own consumer factory.");
        public override Task<string[]> QueryPartitionAsync(string superStream, CancellationToken ct)
            => throw new NotSupportedException("Dispatch tests never list partitions.");
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
    public async Task Queued_dispatch_does_not_run_after_dispose()
    {
        var state = new PartitionState();
        var firstEntered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var releaseFirst = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var secondRan = 0;
        var options = new ConsumeOptions
        {
            Reference = "grp",
            SingleActiveConsumer = false,
            Offset = ConsumeOffset.AlwaysFirst,
            Retry = new RetryPolicy { MaxAttempts = 1, BaseDelay = TimeSpan.FromMilliseconds(10) },
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
            async ctx =>
            {
                if (ctx.Offset == 0)
                {
                    firstEntered.TrySetResult();
                    await releaseFirst.Task.ConfigureAwait(false);
                    return;
                }
                Interlocked.Exchange(ref secondRan, 1);
            },
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
            var config = factory.FirstConfig;

            // Offset 0 enters the handler and holds the dispatch gate.
            var first = DeliverAsync(config, 0);
            await firstEntered.Task.WaitAsync(TimeSpan.FromSeconds(5), TestContext.Current.CancellationToken);

            // Offset 1 is queued on the gate while offset 0 is still running.
            var second = DeliverAsync(config, 1);

            // Dispose snapshots the tracked dispatch and waits for it. Once the
            // first dispatch releases the gate, the queued one would run unless
            // it observes the disposed state first.
            var dispose = consumer.DisposeAsync().AsTask();
            releaseFirst.TrySetResult();

            await Task.WhenAll(first, second, dispose)
                .WaitAsync(TimeSpan.FromSeconds(5), TestContext.Current.CancellationToken);

            Assert.Equal(0, Volatile.Read(ref secondRan));
        }
        finally
        {
            releaseFirst.TrySetResult();
            await cts.CancelAsync();
            try { await run; } catch (OperationCanceledException) { }
            cts.Dispose();
            services.Dispose();
        }
    }
}
