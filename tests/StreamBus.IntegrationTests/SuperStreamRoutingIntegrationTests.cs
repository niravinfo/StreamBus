using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using RabbitMQ.Stream.Client;

namespace StreamBus.IntegrationTests;

public sealed record RoutingEvent(string RoutingKey);

public sealed class SuperStreamRoutingIntegrationTests : IAsyncDisposable
{
    public SuperStreamRoutingIntegrationTests() => IntegrationLane.RequireBroker();

    private readonly List<string> _superStreams = new();

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
            var handle = await TestBroker.StartAsync(TestContext.Current.CancellationToken);
            var services = new ServiceCollection();
            services.AddLogging();
            services.AddStreamBus((_, cfg) => cfg.Host(handle.Host, vhost, h => { h.Credentials(handle.Username, handle.Password); h.StreamPort(handle.StreamPort); }));
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
}

