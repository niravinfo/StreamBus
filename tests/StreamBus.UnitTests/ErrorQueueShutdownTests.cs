using System.Collections.Concurrent;
using System.Diagnostics;
using System.Reflection;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging.Abstractions;

namespace StreamBus.UnitTests;

public sealed class ErrorQueueShutdownTests
{
    private static StreamBusOptions Options() => new()
    {
        Vhost = "/",
        UserName = "app",
        Password = "secret",
    };

    // The registry owns its error-queue transports internally; tests observe
    // disposal by pre-seeding a fake-backed transport into the vhost map
    // (via reflection), rather than adding a test-only constructor seam.
    private static StreamBusRegistry RegistryWith(IErrorQueueConnection connection)
    {
        var registry = new StreamBusRegistry(
            _ => Options(),
            _ => throw new InvalidOperationException("shutdown tests never create supervisors"));
        var field = typeof(StreamBusRegistry).GetField("_errorQueues", BindingFlags.Instance | BindingFlags.NonPublic)
            ?? throw new InvalidOperationException("StreamBusRegistry._errorQueues not found; update this test.");
        var map = (ConcurrentDictionary<string, ErrorQueueTransport>?)field.GetValue(registry)
            ?? throw new InvalidOperationException("StreamBusRegistry._errorQueues is null; update this test.");
        map["/"] = ErrorQueueTransport.ForTest(Options(), (_, _) => Task.FromResult<IErrorQueueConnection>(connection));
        return registry;
    }

    private static async Task ForceConnectionAsync(StreamBusRegistry registry)
    {
        var transport = ((IStreamBusRegistry)registry).ErrorQueuesFor("/");
        await transport.DeclareAsync(
            new[] { new DeadLetterOptions { Name = "orders-error", QueueType = DeadLetterQueueType.ClassicQueue } },
            TestContext.Current.CancellationToken);
    }

    [Fact]
    public async Task Registry_dispose_gracefully_closes_error_queue_transport_connections()
    {
        var connection = new RecordingErrorQueueConnection();
        var registry = RegistryWith(connection);
        await ForceConnectionAsync(registry);
        Assert.Equal(0, connection.CloseCount);

        await registry.DisposeAsync();

        Assert.Equal(1, connection.CloseCount);
        Assert.Equal(1, connection.DisposeCount);
    }

    [Fact]
    public async Task Registry_dispose_is_idempotent()
    {
        var connection = new RecordingErrorQueueConnection();
        var registry = RegistryWith(connection);
        await ForceConnectionAsync(registry);

        await registry.DisposeAsync();
        await registry.DisposeAsync();

        Assert.Equal(1, connection.CloseCount);
        Assert.Equal(1, connection.DisposeCount);
    }

    [Fact]
    public async Task StopAsync_disposes_error_queue_transports_after_supervisors()
    {
        var connection = new RecordingErrorQueueConnection();
        var registry = RegistryWith(connection);
        await ForceConnectionAsync(registry);
        using var sp = new ServiceCollection().BuildServiceProvider();
        var hosted = new StreamBusHostedService(
            new StreamBusConfigStore(),
            registry,
            NullLogger<StreamBusHostedService>.Instance,
            sp.GetRequiredService<IServiceScopeFactory>(),
            new StreamBusConsumerStore(),
            new StreamBusEndpointStore(),
            new StreamBusPublishStore(),
            new StreamBusHostOptions { WaitUntilStarted = false });

        await hosted.StopAsync(CancellationToken.None);

        Assert.Equal(1, connection.CloseCount);
        Assert.Equal(1, connection.DisposeCount);
    }

    private static StreamBusHostedService Hosted(StreamBusRegistry registry, StreamBusHostOptions options)
    {
        using var sp = new ServiceCollection().BuildServiceProvider();
        return new StreamBusHostedService(
            new StreamBusConfigStore(),
            registry,
            NullLogger<StreamBusHostedService>.Instance,
            sp.GetRequiredService<IServiceScopeFactory>(),
            new StreamBusConsumerStore(),
            new StreamBusEndpointStore(),
            new StreamBusPublishStore(),
            options);
    }

    // Pre-seeds a consumer registry whose only registration's partition
    // RunTask never completes, so its DisposeAsync (which drains RunTasks)
    // pends forever — the hanging-consumer harness for the stop budgets.
    private static void SeedHangingConsumer(StreamBusRegistry registry)
    {
        var consumers = new StreamConsumerRegistry(null!, "/", null!, null);
        var liveField = typeof(StreamConsumerRegistry)
            .GetField("_live", BindingFlags.Instance | BindingFlags.NonPublic)
            ?? throw new InvalidOperationException("StreamConsumerRegistry._live not found; update this test.");
        var set = (HashSet<StreamConsumerRegistration>?)liveField.GetValue(consumers)
            ?? throw new InvalidOperationException("StreamConsumerRegistry._live is null; update this test.");
        var never = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously).Task;
        set.Add(new StreamConsumerRegistration(
            "orders", "cg",
            new List<(StreamPartitionConsumer Loop, Task RunTask)> { (null!, never) },
            [], new CancellationTokenSource(), _ => { }));
        var field = typeof(StreamBusRegistry).GetField("_consumers", BindingFlags.Instance | BindingFlags.NonPublic)
            ?? throw new InvalidOperationException("StreamBusRegistry._consumers not found; update this test.");
        var map = (ConcurrentDictionary<string, StreamConsumerRegistry>?)field.GetValue(registry)
            ?? throw new InvalidOperationException("StreamBusRegistry._consumers is null; update this test.");
        map["/"] = consumers;
    }

    private sealed class SlowErrorQueueConnection : IErrorQueueConnection
    {
        private readonly TimeSpan _closeDelay;
        public int DisposeCount;
        public SlowErrorQueueConnection(TimeSpan closeDelay) => _closeDelay = closeDelay;
        public bool IsOpen => true;
        public ValueTask DisposeAsync()
        {
            DisposeCount++;
            return ValueTask.CompletedTask;
        }
        public async Task CloseAsync(CancellationToken ct)
        {
            await Task.Delay(_closeDelay, ct);
        }
        public Task<IErrorQueueControlChannel> CreateControlChannelAsync(CancellationToken ct)
            => Task.FromResult<IErrorQueueControlChannel>(new NoopControlChannel());
        public Task<IErrorQueuePublishChannel> CreatePublishChannelAsync(CancellationToken ct)
            => Task.FromResult<IErrorQueuePublishChannel>(new NoopPublishChannel());
    }

    [Fact]
    public async Task StopTimeout_expires_and_proceeds()
    {
        var connection = new RecordingErrorQueueConnection();
        var registry = RegistryWith(connection);
        SeedHangingConsumer(registry);
        await ForceConnectionAsync(registry);
        var hosted = Hosted(registry, new StreamBusHostOptions
        {
            WaitUntilStarted = false,
            StopTimeout = TimeSpan.FromMilliseconds(200),
        });

        var stopwatch = Stopwatch.StartNew();
        await hosted.StopAsync(TestContext.Current.CancellationToken);
        stopwatch.Stop();

        Assert.True(stopwatch.Elapsed < TimeSpan.FromSeconds(5),
            $"StopAsync took {stopwatch.Elapsed} despite the hanging consumer and a 200ms StopTimeout.");
        Assert.Equal(1, connection.CloseCount);
        Assert.Equal(1, connection.DisposeCount);
    }

    [Fact]
    public async Task ConsumerStopTimeout_expires_and_proceeds()
    {
        var connection = new RecordingErrorQueueConnection();
        var registry = RegistryWith(connection);
        SeedHangingConsumer(registry);
        await ForceConnectionAsync(registry);
        var hosted = Hosted(registry, new StreamBusHostOptions
        {
            WaitUntilStarted = false,
            ConsumerStopTimeout = TimeSpan.FromMilliseconds(200),
        });

        var stopwatch = Stopwatch.StartNew();
        await hosted.StopAsync(TestContext.Current.CancellationToken);
        stopwatch.Stop();

        Assert.True(stopwatch.Elapsed < TimeSpan.FromSeconds(5),
            $"StopAsync took {stopwatch.Elapsed} despite the hanging consumer and a 200ms ConsumerStopTimeout.");
        Assert.Equal(1, connection.CloseCount);
        Assert.Equal(1, connection.DisposeCount);
    }

    [Fact]
    public async Task Infinite_stop_waits_for_dispose()
    {
        var connection = new SlowErrorQueueConnection(TimeSpan.FromMilliseconds(300));
        var registry = RegistryWith(connection);
        await ForceConnectionAsync(registry);
        var hosted = Hosted(registry, new StreamBusHostOptions { WaitUntilStarted = false });

        var stopwatch = Stopwatch.StartNew();
        await hosted.StopAsync(TestContext.Current.CancellationToken);
        stopwatch.Stop();

        // StopTimeout defaults to null = infinite, not zero: the 300ms close
        // must be awaited, not abandoned.
        Assert.True(stopwatch.Elapsed >= TimeSpan.FromMilliseconds(250),
            $"StopAsync returned after only {stopwatch.Elapsed}; null StopTimeout must wait for dispose.");
        Assert.Equal(1, connection.DisposeCount);
    }

    [Fact]
    public async Task StopAsync_validates_before_anything()
    {
        var connection = new RecordingErrorQueueConnection();
        var registry = RegistryWith(connection);
        await ForceConnectionAsync(registry);
        var hosted = Hosted(registry, new StreamBusHostOptions
        {
            WaitUntilStarted = false,
            ConsumerStopTimeout = TimeSpan.FromSeconds(6),
            StopTimeout = TimeSpan.FromSeconds(5),
        });

        await Assert.ThrowsAsync<StreamBusConfigurationException>(
            () => hosted.StopAsync(TestContext.Current.CancellationToken));
        // Plus the Task 1-review ruling: a non-positive ConsumerStopTimeout is rejected.
        Assert.Throws<StreamBusConfigurationException>(() =>
            StreamBusHostOptions.ValidateStop(new StreamBusHostOptions { ConsumerStopTimeout = TimeSpan.Zero }));

        Assert.Equal(0, connection.CloseCount);
        Assert.Equal(0, connection.DisposeCount);
    }

    private sealed class NoopControlChannel : IErrorQueueControlChannel
    {
        public bool IsOpen => true;
        public Task DeclareAsync(string queue, IReadOnlyDictionary<string, object?> arguments, CancellationToken ct)
            => Task.CompletedTask;
        public Task DeclarePassiveAsync(string queue, CancellationToken ct) => Task.CompletedTask;
        public ValueTask DisposeAsync() => ValueTask.CompletedTask;
    }

    private sealed class NoopPublishChannel : IErrorQueuePublishChannel
    {
        public bool IsOpen => true;
        public Task PublishAsync(string queue, ErrorEnvelope envelope, TimeSpan confirmTimeout, CancellationToken ct)
            => Task.CompletedTask;
        public ValueTask DisposeAsync() => ValueTask.CompletedTask;
    }

    private sealed class RecordingErrorQueueConnection : IErrorQueueConnection
    {
        public int CloseCount;
        public int DisposeCount;
        public bool IsOpen => true;
        public ValueTask DisposeAsync()
        {
            DisposeCount++;
            return ValueTask.CompletedTask;
        }
        public Task CloseAsync(CancellationToken ct)
        {
            CloseCount++;
            return Task.CompletedTask;
        }
        public Task<IErrorQueueControlChannel> CreateControlChannelAsync(CancellationToken ct)
            => Task.FromResult<IErrorQueueControlChannel>(new NoopControlChannel());
        public Task<IErrorQueuePublishChannel> CreatePublishChannelAsync(CancellationToken ct)
            => Task.FromResult<IErrorQueuePublishChannel>(new NoopPublishChannel());
    }
}
