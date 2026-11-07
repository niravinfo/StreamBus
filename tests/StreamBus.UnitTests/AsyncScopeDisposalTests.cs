using Microsoft.Extensions.DependencyInjection;
using RabbitMQ.Stream.Client;

namespace StreamBus.UnitTests;

// CON-14: per-delivery scopes (dispatch path and the OnExhausted handoff)
// must dispose scoped services through IAsyncDisposable (CreateAsyncScope),
// never synchronously — a scoped DbContext-style service would otherwise be
// sync-disposed on the dispatch thread.
public sealed class AsyncScopeDisposalTests
{
    private const string Stream = "orders";
    private const int Partition = 0;

    private sealed class ScriptedSystem : NeverReadyConsumerSystem
    {
        public override Task<RawConsumer> CreateRawConsumerAsync(RawConsumerConfig config, CancellationToken ct)
            => throw new NotSupportedException("AsyncScopeDisposalTests inject their own consumer factory.");
        public override Task<string[]> QueryPartitionAsync(string superStream, CancellationToken ct)
            => throw new NotSupportedException("Scope tests never list partitions.");
    }

    private sealed class NeverReadyConnector : IStreamSystemConnector
    {
        public Task<IStreamSystemHandle> ConnectAsync(StreamBusOptions options, CancellationToken ct)
            => new TaskCompletionSource<IStreamSystemHandle>().Task;
    }

    private sealed class CapturingFilter : IStreamConsumeFilter
    {
        public AsyncDisposeProbe? Probe;
        public Task OnConsume(ConsumeContext context, ConsumeNext next)
        {
            Probe = context.Services.GetRequiredService<AsyncDisposeProbe>();
            return next();
        }
    }

    // Implements both disposal interfaces; records which path the container took.
    private sealed class AsyncDisposeProbe : IDisposable, IAsyncDisposable
    {
        public bool SyncDisposed;
        public bool AsyncDisposed;

        public void Dispose() => SyncDisposed = true;

        public ValueTask DisposeAsync()
        {
            AsyncDisposed = true;
            return ValueTask.CompletedTask;
        }
    }

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

    private static async Task DeliverAsync(RawConsumerConfig config, ulong offset)
        => await config.MessageHandler(
            null!,
            new MessageContext(offset, TimeSpan.Zero, 0, 0),
            new Message(Array.Empty<byte>()));

    private static StreamPartitionConsumer CreateConsumer(
        ConsumeOptions options,
        IStreamConsumeFilter? filter,
        IServiceScopeFactory scopes,
        ScriptedConsumerFactory factory,
        Func<ConsumeContext, Task>? handler = null)
    {
        var system = new ScriptedSystem();
        var supervisor = new StreamBusConnectionSupervisor(
            new StreamBusOptions { Vhost = "/", UserName = "app", Password = "secret" },
            new NeverReadyConnector());
        var producers = new StreamProducerRegistry(supervisor, "/");
        var errors = new ErrorPublisher(producers);
        var pipeline = filter is null
            ? null
            : new ConsumePipeline(new[] { filter });
        return new StreamPartitionConsumer(
            system, Stream, Stream, Partition, "grp", options,
            handler ?? (static _ => Task.CompletedTask),
            new MonotonicOffsetGuard(),
            new PartitionState(),
            StreamBusMetrics.Shared,
            "/", "bus", errors,
            logger: null,
            scopeFactory: scopes,
            consumerFactory: factory.CreateAsync,
            pipeline: pipeline);
    }

    [Fact]
    public async Task Per_delivery_scope_disposes_scoped_services_asynchronously()
    {
        var services = new ServiceCollection()
            .AddScoped<AsyncDisposeProbe>()
            .BuildServiceProvider();
        var scopes = services.GetRequiredService<IServiceScopeFactory>();
        var factory = new ScriptedConsumerFactory();
        var options = new ConsumeOptions
        {
            Reference = "grp",
            SingleActiveConsumer = false,
            Offset = ConsumeOffset.AlwaysFirst,
        };
        var filter = new CapturingFilter();
        var consumer = CreateConsumer(options, filter, scopes, factory);
        var cts = new CancellationTokenSource();
        var run = consumer.RunAsync(cts.Token);
        try
        {
            await consumer.FirstAttachAsync.WaitAsync(TimeSpan.FromSeconds(5), TestContext.Current.CancellationToken);
            await DeliverAsync(factory.FirstConfig, 0);

            var probe = filter.Probe;
            Assert.NotNull(probe);
            await WaitUntilAsync(() => probe.AsyncDisposed || probe.SyncDisposed, TimeSpan.FromSeconds(5));
            Assert.True(probe.AsyncDisposed,
                "the per-delivery scope must dispose scoped services via DisposeAsync");
            Assert.False(probe.SyncDisposed,
                "the per-delivery scope must not fall back to synchronous disposal");
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

    [Fact]
    public async Task OnExhausted_scope_disposes_scoped_services_asynchronously()
    {
        var services = new ServiceCollection()
            .AddScoped<AsyncDisposeProbe>()
            .BuildServiceProvider();
        var scopes = services.GetRequiredService<IServiceScopeFactory>();
        var factory = new ScriptedConsumerFactory();
        AsyncDisposeProbe? resolved = null;
        var exhausted = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var options = new ConsumeOptions
        {
            Reference = "grp",
            SingleActiveConsumer = false,
            Offset = ConsumeOffset.AlwaysFirst,
            Retry = new RetryPolicy { MaxAttempts = 1, BaseDelay = TimeSpan.FromMilliseconds(10) },
            OnExhausted = (context, _) =>
            {
                resolved = context.Services.GetRequiredService<AsyncDisposeProbe>();
                exhausted.TrySetResult();
                return Task.CompletedTask;
            },
        };
        var consumer = CreateConsumer(
            options, null, scopes, factory,
            _ => Task.FromException(new InvalidOperationException("handler failed")));
        var cts = new CancellationTokenSource();
        var run = consumer.RunAsync(cts.Token);
        try
        {
            await consumer.FirstAttachAsync.WaitAsync(TimeSpan.FromSeconds(5), TestContext.Current.CancellationToken);
            await DeliverAsync(factory.FirstConfig, 0);
            await exhausted.Task.WaitAsync(TimeSpan.FromSeconds(5), TestContext.Current.CancellationToken);

            var probe = resolved;
            Assert.NotNull(probe);
            await WaitUntilAsync(() => probe.AsyncDisposed || probe.SyncDisposed, TimeSpan.FromSeconds(5));
            Assert.True(probe.AsyncDisposed,
                "the OnExhausted handoff scope must dispose scoped services via DisposeAsync");
            Assert.False(probe.SyncDisposed,
                "the OnExhausted handoff scope must not fall back to synchronous disposal");
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
