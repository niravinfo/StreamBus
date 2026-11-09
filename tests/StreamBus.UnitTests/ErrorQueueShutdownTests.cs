using System.Collections.Concurrent;
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
    private static StreamBusRegistry RegistryWith(RecordingErrorQueueConnection connection)
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
            new StreamBusHostingStore(new StreamBusHostingOptions()));

        await hosted.StopAsync(CancellationToken.None);

        Assert.Equal(1, connection.CloseCount);
        Assert.Equal(1, connection.DisposeCount);
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
