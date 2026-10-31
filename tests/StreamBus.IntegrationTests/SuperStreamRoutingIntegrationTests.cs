using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using RabbitMQ.Stream.Client;

namespace StreamBus.IntegrationTests;

public sealed record RoutingEvent(string RoutingKey);

public sealed class SuperStreamRoutingIntegrationTests : IAsyncDisposable
{
    public SuperStreamRoutingIntegrationTests() => IntegrationLane.RequireBroker();

    private static string User() => Environment.GetEnvironmentVariable("STREAMBUS_USER") ?? "app";
    private static string Password() => Environment.GetEnvironmentVariable("STREAMBUS_PASSWORD") ?? "password";
    private readonly List<string> _superStreams = new();

    private static async Task<StreamSystem> ConnectAdminAsync()
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
            foreach (var s in _superStreams) { try { await admin.DeleteSuperStream(s); } catch { } }
        }
        finally { await admin.DisposeAsync(); }
    }

    private sealed class BusInstance : IAsyncDisposable
    {
        public ServiceProvider Provider { get; private set; } = null!;
        public StreamBusHostedService Hosted { get; private set; } = null!;
        public IStreamBus Bus { get; private set; } = null!;
        public string Name { get; private set; } = "";

        public static async Task<BusInstance> StartAsync(string name, string vhost = "/")
        {
            var services = new ServiceCollection();
            services.AddLogging();
            services.AddStreamBus((_, cfg) => cfg.Host("localhost", vhost, h => h.Credentials(User(), Password())));
            var sp = services.BuildServiceProvider();
            var hosted = sp.GetServices<IHostedService>().OfType<StreamBusHostedService>().Single();
            await hosted.StartAsync(TestContext.Current.CancellationToken);
            var bus = sp.GetRequiredService<IStreamBus>();
            Assert.Equal(BusReadiness.Ready, await bus.WaitUntilReadyAsync(ReadinessPolicy.UpTo(TimeSpan.FromSeconds(30)), TestContext.Current.CancellationToken));
            return new BusInstance { Provider = sp, Hosted = hosted, Bus = bus, Name = name };
        }

        public async ValueTask DisposeAsync()
        {
            try { await Hosted.StopAsync(TestContext.Current.CancellationToken); }
            finally { await Provider.DisposeAsync(); }
        }
    }

    private static int Count<T>(List<T> list) { lock (list) return list.Count; }

    private static async Task WaitForAsync(Func<bool> done, TimeSpan timeout, string what)
    {
        var deadline = DateTime.UtcNow + timeout;
        while (!done())
        {
            if (DateTime.UtcNow >= deadline) Assert.Fail($"Timed out waiting for {what}.");
            await Task.Delay(100, TestContext.Current.CancellationToken);
        }
    }

    [Fact]
    public async Task Super_publish_exposes_its_routing_key_on_every_delivery()
    {
        var super = Unique("routing-super");
        var admin = await ConnectAdminAsync();
        await using var _ = admin;
        await admin.CreateSuperStream(new PartitionsSuperStreamSpec(super, 3));
        _superStreams.Add(super);

        await using var publisher = await BusInstance.StartAsync("routing-pub");
        foreach (var key in new[] { "key-a", "key-b", "key-a" })
            await publisher.Bus.PublishSuperAsync(super, new RoutingEvent(key), key, TestContext.Current.CancellationToken);

        var seen = new List<ConsumeContext>();
        await using var consumer = await BusInstance.StartAsync("routing-con");
        await using var subscription = await consumer.Bus.ConsumeAsync(
            super,
            new ConsumeOptions { IsSuperStream = true, Reference = "routing-grp", Offset = ConsumeOffset.AlwaysFirst },
            ctx => { lock (seen) seen.Add(ctx); return Task.CompletedTask; },
            TestContext.Current.CancellationToken);

        await WaitForAsync(() => Count(seen) == 3, TimeSpan.FromSeconds(30), "routed deliveries");
        Assert.All(seen, ctx => Assert.False(string.IsNullOrWhiteSpace(ctx.RoutingValue)));
        Assert.All(seen, ctx => Assert.Equal(ctx.RoutingValue, ctx.Headers.GetString("streambus.routing-key")));
        Assert.Contains(seen, ctx => ctx.RoutingValue == "key-a");
        Assert.All(seen, ctx => Assert.NotEqual(Guid.Empty, ctx.MessageId));
    }

    [Fact]
    public async Task Super_target_result_carries_the_source_routing_key()
    {
        var source = Unique("routing-eo-src");
        var target = Unique("routing-eo-tgt");
        var admin = await ConnectAdminAsync();
        await using var _ = admin;
        await admin.CreateSuperStream(new PartitionsSuperStreamSpec(source, 2));
        _superStreams.Add(source);
        await admin.CreateSuperStream(new PartitionsSuperStreamSpec(target, 2));
        _superStreams.Add(target);

        await using var publisher = await BusInstance.StartAsync("routing-eo-pub");
        await publisher.Bus.PublishSuperAsync(source, new RoutingEvent("key-a"), "key-a",
            TestContext.Current.CancellationToken);

        await using var eo = await BusInstance.StartAsync("routing-eo");
        await using var step = await eo.Bus.ConsumeEffectivelyOnceAsync(
            source, target,
            new EffectivelyOnceOptions
            {
                SourceIsSuperStream = true,
                TargetIsSuperStream = true,
                Retry = new RetryPolicy { BaseDelay = TimeSpan.FromMilliseconds(10), MaxJitter = TimeSpan.Zero },
            },
            ctx => Task.FromResult(ctx.Body),
            TestContext.Current.CancellationToken);

        var seen = new List<ConsumeContext>();
        await using var reader = await BusInstance.StartAsync("routing-eo-read");
        await using var subscription = await reader.Bus.ConsumeAsync(
            target,
            new ConsumeOptions { IsSuperStream = true, Reference = "routing-eo-grp", Offset = ConsumeOffset.AlwaysFirst },
            ctx => { lock (seen) seen.Add(ctx); return Task.CompletedTask; },
            TestContext.Current.CancellationToken);

        await WaitForAsync(() => Count(seen) == 1, TimeSpan.FromSeconds(30), "routed result");
        var result = Assert.Single(seen);
        Assert.Equal("key-a", result.RoutingValue);
        Assert.Equal("key-a", result.Headers.GetString("streambus.routing-key"));
        Assert.NotEqual(Guid.Empty, result.MessageId);
    }
}
