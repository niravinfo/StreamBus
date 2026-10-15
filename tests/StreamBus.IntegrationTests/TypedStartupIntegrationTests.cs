using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using RabbitMQ.Stream.Client;

namespace StreamBus.IntegrationTests;

public sealed class TypedStartupIntegrationTests : IAsyncDisposable
{
    private static string User() => Environment.GetEnvironmentVariable("STREAMBUS_USER") ?? "app";
    private static string Password() => Environment.GetEnvironmentVariable("STREAMBUS_PASSWORD") ?? "password";
    private readonly List<string> _streams = new();
    private readonly List<string> _superStreams = new();

    private async Task<StreamSystem> ConnectAdminAsync()
        => await StreamSystem.Create(new StreamSystemConfig
        {
            UserName = User(),
            Password = Password(),
            VirtualHost = "/",
            Endpoints = new List<System.Net.EndPoint> { new System.Net.IPEndPoint(System.Net.IPAddress.Loopback, 5552) },
        });

    private static string Unique(string prefix) => $"{prefix}-{Guid.NewGuid():N}";

    public async ValueTask DisposeAsync()
    {
        var admin = await ConnectAdminAsync();
        try
        {
            foreach (var s in _streams) { try { await admin.DeleteStream(s); } catch { } }
            foreach (var s in _superStreams) { try { await admin.DeleteSuperStream(s); } catch { } }
        }
        finally { await admin.DisposeAsync(); }
    }

    private sealed class BusInstance : IAsyncDisposable
    {
        public ServiceProvider Provider { get; private set; } = null!;
        public StreamBusHostedService Hosted { get; private set; } = null!;
        public IStreamBus Bus { get; private set; } = null!;
        public static async Task<BusInstance> StartAsync(Action<ServiceCollection>? configure = null, string vhost = "/", Action<IStreamBusConfigurator>? busConfigure = null)
        {
            var services = new ServiceCollection();
            services.AddLogging();
            services.AddStreamBus((_, cfg) =>
            {
                cfg.Host("127.0.0.1", vhost, h => h.Credentials(User(), Password()));
                busConfigure?.Invoke(cfg);
            });
            configure?.Invoke(services);
            var sp = services.BuildServiceProvider();
            var hosted = sp.GetServices<IHostedService>().OfType<StreamBusHostedService>().Single();
            await hosted.StartAsync(TestContext.Current.CancellationToken);
            var bus = sp.GetRequiredService<IStreamBus>();
            Assert.Equal(BusReadiness.Ready, await bus.WaitUntilReadyAsync(ReadinessPolicy.UpTo(TimeSpan.FromSeconds(30)), TestContext.Current.CancellationToken));
            return new BusInstance { Provider = sp, Hosted = hosted, Bus = bus };
        }
        public async ValueTask DisposeAsync()
        {
            try { await Hosted.StopAsync(TestContext.Current.CancellationToken); }
            finally { await Provider.DisposeAsync(); }
        }
    }

    private static ReadOnlyMemory<byte> Body<T>(T message) => SystemTextJsonStreamSerializer.Default.Serialize(message);

    private static async Task WaitForAsync(Func<bool> done, TimeSpan timeout, string what)
    {
        var deadline = DateTime.UtcNow + timeout;
        while (!done())
        {
            if (DateTime.UtcNow >= deadline) Assert.Fail($"Timed out waiting for {what}.");
            await Task.Delay(100, TestContext.Current.CancellationToken);
        }
    }

    private sealed record OrderEvent(string OrderId, int Seq);

    private sealed class OrderConsumer : IConsumer<OrderEvent>
    {
        private readonly List<(string Who, int Seq)> _got;
        public OrderConsumer(List<(string Who, int Seq)> got) => _got = got;
        public Task Consume(ConsumeContext<OrderEvent> context, CancellationToken ct)
        {
            lock (_got) _got.Add((nameof(OrderConsumer), context.Message.Seq));
            return Task.CompletedTask;
        }
    }

    private sealed class AuditConsumer : IConsumer<OrderEvent>
    {
        private readonly List<(string Who, int Seq)> _got;
        public AuditConsumer(List<(string Who, int Seq)> got) => _got = got;
        public Task Consume(ConsumeContext<OrderEvent> context, CancellationToken ct)
        {
            lock (_got) _got.Add((nameof(AuditConsumer), context.Message.Seq));
            return Task.CompletedTask;
        }
    }

    private sealed class GhostBus { }

    [Fact]
    public async Task Startup_with_topology_and_consumer_converges()
    {
        // Consumer registered BEFORE the bus (order independence) + stream declared
        // via Topology in the same config: everything converges after one Start.
        var ct = TestContext.Current.CancellationToken;
        var stream = Unique("typed-orders");
        _streams.Add(stream);
        var got = new List<(string Who, int Seq)>();
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddSingleton(got);
        services.AddStreamBus((_, cfg) =>
        {
            cfg.Host("127.0.0.1", "/", h => h.Credentials(User(), Password()));
            cfg.DeclareStream(stream);
            cfg.ReceiveEndpoint(stream, e => e.Consumer<OrderConsumer>("startup-group"));
        });
        await using var sp = services.BuildServiceProvider();
        var hosted = sp.GetServices<IHostedService>().OfType<StreamBusHostedService>().Single();
        await hosted.StartAsync(ct);
        try
        {
            var bus = sp.GetRequiredService<IStreamBus>();
            Assert.Equal(BusReadiness.Ready, await bus.WaitUntilReadyAsync(ReadinessPolicy.UpTo(TimeSpan.FromSeconds(30)), ct));
            // Topology declaration runs in the background under non-blocking startup;
            // the stream must exist before the first publish (else NotFound).
            var admin = await ConnectAdminAsync();
            await using var _ = admin;
            await WaitForAsync(() => admin.StreamExists(stream).GetAwaiter().GetResult(), TimeSpan.FromSeconds(30), "startup topology declaration");
            await bus.PublishRawAsync(stream, Body(new OrderEvent("o", 0)), new PublishOptions(), ct);
            await WaitForAsync(() => { lock (got) return got.Count >= 1; }, TimeSpan.FromSeconds(30), "typed receipt after startup");
            lock (got) Assert.Single(got);
        }
        finally
        {
            await hosted.StopAsync(ct);
        }
    }

    [Fact]
    public async Task Invalid_options_fail_blocking_startup()
    {
        var ct = TestContext.Current.CancellationToken;
        var stream = Unique("typed-invalid");
        _streams.Add(stream);
        var admin = await ConnectAdminAsync();
        await using var _ = admin;
        await admin.CreateStream(new StreamSpec(stream));
        var got = new List<(string Who, int Seq)>();
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddSingleton(got);
        services.AddStreamBus((_, cfg) =>
        {
            cfg.Host("127.0.0.1", "/", h => h.Credentials(User(), Password()));
            cfg.Hosting(h => h.BlockStartupUntilReady(TimeSpan.FromSeconds(30)));
            cfg.ReceiveEndpoint(stream, e =>
            {
                e.Consumer<OrderConsumer>("invalid-group");
                e.Retry(new RetryPolicy { FailureMode = FailureMode.SkipAfterDeadline, MaxAttempts = 2 });
            });
        });
        await using var sp = services.BuildServiceProvider();
        var hosted = sp.GetServices<IHostedService>().OfType<StreamBusHostedService>().Single();
        var ex = await Assert.ThrowsAsync<StreamBusConfigurationException>(() => hosted.StartAsync(ct));
        Assert.Contains("ErrorStream", ex.Message);
        await hosted.StopAsync(ct);
    }

    [Fact]
    public async Task Unknown_marker_fails_startup_in_both_modes()
    {
        var ct = TestContext.Current.CancellationToken;
        var stream = Unique("typed-ghost");
        _streams.Add(stream);
        var admin = await ConnectAdminAsync();
        await using var _ = admin;
        await admin.CreateStream(new StreamSpec(stream));

        {
            var services = new ServiceCollection();
            services.AddLogging();
            services.AddSingleton(new List<(string Who, int Seq)>());
            services.AddStreamBus((_, cfg) => cfg.Host("127.0.0.1", "/", h => h.Credentials(User(), Password())));
            await using var sp = services.BuildServiceProvider();
            sp.GetRequiredService<StreamBusConsumerStore>().Add(typeof(GhostBus), typeof(OrderEvent), typeof(OrderConsumer), stream, new ConsumeOptions { Reference = "ghost-group" });
            var hosted = sp.GetServices<IHostedService>().OfType<StreamBusHostedService>().Single();
            var ex = await Assert.ThrowsAsync<StreamBusConfigurationException>(() => hosted.StartAsync(ct));
            Assert.Contains("GhostBus", ex.Message);
            await hosted.StopAsync(ct);
        }

        {
            var services = new ServiceCollection();
            services.AddLogging();
            services.AddSingleton(new List<(string Who, int Seq)>());
            services.AddStreamBus((_, cfg) =>
            {
                cfg.Host("127.0.0.1", "/", h => h.Credentials(User(), Password()));
                cfg.Hosting(h => h.BlockStartupUntilReady(TimeSpan.FromSeconds(30)));
            });
            await using var sp = services.BuildServiceProvider();
            sp.GetRequiredService<StreamBusConsumerStore>().Add(typeof(GhostBus), typeof(OrderEvent), typeof(OrderConsumer), stream, new ConsumeOptions { Reference = "ghost-group" });
            var hosted = sp.GetServices<IHostedService>().OfType<StreamBusHostedService>().Single();
            var ex = await Assert.ThrowsAsync<StreamBusConfigurationException>(() => hosted.StartAsync(ct));
            Assert.Contains("GhostBus", ex.Message);
            await hosted.StopAsync(ct);
        }
    }

    [Fact]
    public async Task Two_consumer_types_each_receive_everything()
    {
        var ct = TestContext.Current.CancellationToken;
        var stream = Unique("typed-two");
        _streams.Add(stream);
        var admin = await ConnectAdminAsync();
        await using var _ = admin;
        await admin.CreateStream(new StreamSpec(stream));
        var got = new List<(string Who, int Seq)>();
        await using var inst = await BusInstance.StartAsync(
            s => { s.AddSingleton(got); },
            "/",
            cfg =>
            {
                cfg.ReceiveEndpoint(stream, e => e.Consumer<OrderConsumer>("orders-group"));
                cfg.ReceiveEndpoint(stream, e => e.Consumer<AuditConsumer>("audit-group"));
            });
        for (var seq = 0; seq < 4; seq++)
            await inst.Bus.PublishRawAsync(stream, Body(new OrderEvent("o", seq)), new PublishOptions(), ct);
        await WaitForAsync(() => { lock (got) return got.Count >= 8; }, TimeSpan.FromSeconds(30), "two-type receipt");
        await Task.Delay(500, ct);
        lock (got)
        {
            Assert.Equal(8, got.Count);
            Assert.Equal(new[] { 0, 1, 2, 3 }, got.Where(x => x.Who == nameof(OrderConsumer)).Select(x => x.Seq).OrderBy(x => x).ToList());
            Assert.Equal(new[] { 0, 1, 2, 3 }, got.Where(x => x.Who == nameof(AuditConsumer)).Select(x => x.Seq).OrderBy(x => x).ToList());
        }
    }
}
