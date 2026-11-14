using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using RabbitMQ.Stream.Client;

namespace StreamBus.IntegrationTests;

/// <summary>
/// Broker-backed proof of the options-driven hosting contract
/// (<see cref="StreamBusHostOptions"/>): blocking startup orders hosted
/// services after bus readiness, the default does not block, and an
/// infinite blocking wait never throws on its own but yields to the
/// startup cancellation token.
/// </summary>
public sealed class HostingStartupOrderTests : IAsyncDisposable
{
    public HostingStartupOrderTests() => IntegrationLane.RequireBroker();

    private readonly List<string> _streams = new();

    private static async Task<StreamSystem> ConnectAdminAsync()
    {
        var handle = await TestBroker.StartAsync(TestContext.Current.CancellationToken);
        return await StreamSystem.Create(new StreamSystemConfig
        {
            UserName = handle.Username,
            Password = handle.Password,
            VirtualHost = "/",
            Endpoints = new List<System.Net.EndPoint> { new System.Net.IPEndPoint(System.Net.IPAddress.Parse(handle.Host), handle.StreamPort) },
        });
    }

    private static string Unique(string prefix) => $"{prefix}-{Guid.NewGuid():N}";

    public async ValueTask DisposeAsync()
    {
        if (_streams.Count == 0) return;
        var admin = await ConnectAdminAsync();
        try
        {
            foreach (var s in _streams) { try { await admin.DeleteStream(s); } catch { } }
        }
        finally { await admin.DisposeAsync(); }
    }

    private static ReadOnlyMemory<byte> Body<T>(T message) => SystemTextJsonStreamSerializer.Default.Serialize(message);

    /// <summary>A port on loopback that nothing listens on (an ephemeral
    /// listener is asked for a free port and then stopped).</summary>
    private static int ClosedPort()
    {
        var listener = new System.Net.Sockets.TcpListener(System.Net.IPAddress.Loopback, 0);
        listener.Start();
        try
        {
            return ((System.Net.IPEndPoint)listener.LocalEndpoint).Port;
        }
        finally
        {
            listener.Stop();
        }
    }

    // A hosted service registered after StreamBus's own: runs its
    // StartAsync after StreamBus's, exactly like a real host does.
    private sealed class ProbeHostedService : IHostedService
    {
        private readonly Func<CancellationToken, Task> _onStart;
        public ProbeHostedService(Func<CancellationToken, Task> onStart) => _onStart = onStart;
        public Task StartAsync(CancellationToken cancellationToken) => _onStart(cancellationToken);
        public Task StopAsync(CancellationToken cancellationToken) => Task.CompletedTask;
    }

    private sealed record OrderEvent(string OrderId, int Seq);

    [Fact]
    public async Task Options_blocking_startup_publishes_from_probe_without_waiting()
    {
        // The startup-order claim: with WaitUntilStarted, hosted services
        // after StreamBus's see a ready bus and derived topology, so a
        // probe publishes on entry with no WaitUntilReadyAsync of its own.
        var ct = TestContext.Current.CancellationToken;
        var handle = await TestBroker.StartAsync(ct);
        var stream = Unique("order-startup");
        _streams.Add(stream);
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddOptions<StreamBusHostOptions>().Configure(o => { o.WaitUntilStarted = true; o.StartTimeout = TimeSpan.FromSeconds(30); });
        services.AddStreamBus((_, cfg) =>
        {
            cfg.Host(handle.Host, "/", h => { h.Credentials(handle.Username, handle.Password); h.StreamPort(handle.StreamPort); });
            cfg.Publish<OrderEvent>(stream);
        });
        var probeRan = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
        services.AddSingleton<IHostedService>(sp => new ProbeHostedService(async token =>
        {
            var bus = sp.GetRequiredService<IStreamBus>();
            await bus.PublishRawAsync(stream, Body(new OrderEvent("o", 0)), new PublishOptions(), token);
            probeRan.SetResult(true);
        }));
        await using var sp = services.BuildServiceProvider();
        try
        {
            // Same ordering a real host applies: registration order.
            foreach (var hosted in sp.GetServices<IHostedService>())
                await hosted.StartAsync(ct);
            // The only wait is the probe itself: blocking startup already
            // made bus and derived stream ready before the probe ran.
            var completed = await Task.WhenAny(probeRan.Task, Task.Delay(TimeSpan.FromSeconds(30), ct));
            Assert.Same(probeRan.Task, completed);
            Assert.True(await probeRan.Task);
        }
        finally
        {
            foreach (var hosted in sp.GetServices<IHostedService>().Reverse())
                await hosted.StopAsync(ct);
        }
    }

    [Fact]
    public async Task Default_starts_without_blocking()
    {
        // MassTransit parity proof: AddStreamBus alone (default options)
        // lets a real host reach started in seconds with no broker at all.
        var ct = TestContext.Current.CancellationToken;
        var port = ClosedPort();
        var started = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
        using var host = new HostBuilder()
            .ConfigureServices(s =>
            {
                s.AddLogging();
                s.AddStreamBus((_, cfg) => cfg.Host("127.0.0.1", "/", h => { h.Credentials("guest", "guest"); h.StreamPort(port); }));
                s.AddSingleton<IHostedService>(new ProbeHostedService(_ => { started.SetResult(true); return Task.CompletedTask; }));
            })
            .Build();
        await host.StartAsync(ct).WaitAsync(TimeSpan.FromSeconds(5), ct);
        Assert.True(started.Task.IsCompleted);
        using var stopCts = CancellationTokenSource.CreateLinkedTokenSource(ct);
        stopCts.CancelAfter(TimeSpan.FromSeconds(10));
        await host.StopAsync(stopCts.Token);
    }

    [Fact]
    public async Task WaitUntilStarted_infinite_never_completes_without_throw()
    {
        // Blocking with StartTimeout = null waits forever; the startup
        // cancellation token is the only unblock, and cancelling it must
        // not leave StartAsync stuck.
        var ct = TestContext.Current.CancellationToken;
        var port = ClosedPort();
        using var host = new HostBuilder()
            .ConfigureServices(s =>
            {
                s.AddLogging();
                s.AddStreamBus((_, cfg) => cfg.Host("127.0.0.1", "/", h => { h.Credentials("guest", "guest"); h.StreamPort(port); }));
                s.AddOptions<StreamBusHostOptions>().Configure(o => { o.WaitUntilStarted = true; o.StartTimeout = null; });
            })
            .Build();
        using var startupCts = CancellationTokenSource.CreateLinkedTokenSource(ct);
        var startTask = host.StartAsync(startupCts.Token);
        var afterASecond = await Task.WhenAny(startTask, Task.Delay(TimeSpan.FromSeconds(1), ct));
        // No timeout, no broker: still pending, and no exception thrown.
        Assert.NotSame(startTask, afterASecond);
        Assert.False(startTask.IsCompleted);
        startupCts.Cancel();
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => startTask);
        using var stopCts = CancellationTokenSource.CreateLinkedTokenSource(ct);
        stopCts.CancelAfter(TimeSpan.FromSeconds(10));
        await host.StopAsync(stopCts.Token);
    }
}
