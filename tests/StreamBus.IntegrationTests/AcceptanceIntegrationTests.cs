using System.Text;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using RabbitMQ.Stream.Client;

namespace StreamBus.IntegrationTests;

public sealed class AcceptanceIntegrationTests : IAsyncDisposable
{
    public AcceptanceIntegrationTests() => IntegrationLane.RequireBroker();

    private static string User() => Environment.GetEnvironmentVariable("STREAMBUS_USER") ?? "app";
    private static string Password() => Environment.GetEnvironmentVariable("STREAMBUS_PASSWORD") ?? "password";
    private readonly List<string> _streams = new();

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
            foreach (var s in _streams) { try { await admin.DeleteStream(s); } catch { } }
        }
        finally { await admin.DisposeAsync(); }
    }

    private sealed class BusInstance : IAsyncDisposable
    {
        public ServiceProvider Provider { get; private set; } = null!;
        public StreamBusHostedService Hosted { get; private set; } = null!;
        public IStreamBus Bus { get; private set; } = null!;
        public static async Task<BusInstance> StartAsync(string vhost = "/")
        {
            var services = new ServiceCollection();
            services.AddLogging();
            services.AddStreamBus((_, cfg) => cfg.Host("localhost", vhost, h => h.Credentials(User(), Password())));
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

    private static ReadOnlyMemory<byte> Body(string s) => Encoding.UTF8.GetBytes(s);

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
    public async Task One_bus_serves_two_streams()
    {
        // FR-8.2: one bus instance serves many streams — both confirmed, bodies uncrossed.
        var ct = TestContext.Current.CancellationToken;
        var first = Unique("acc-s1");
        var second = Unique("acc-s2");
        _streams.Add(first);
        _streams.Add(second);
        var admin = await ConnectAdminAsync();
        await using var _ = admin;
        await admin.CreateStream(new StreamSpec(first));
        await admin.CreateStream(new StreamSpec(second));
        await using var inst = await BusInstance.StartAsync();
        await inst.Bus.PublishRawAsync(first, Body("a:0"), new PublishOptions(), ct);
        await inst.Bus.PublishRawAsync(second, Body("b:0"), new PublishOptions(), ct);
        var gotFirst = new List<string>();
        var gotSecond = new List<string>();
        await using var c1 = await inst.Bus.ConsumeAsync(first,
            new ConsumeOptions { Offset = ConsumeOffset.AlwaysFirst, Reference = Unique("read-1") },
            ctx => { lock (gotFirst) gotFirst.Add(Encoding.UTF8.GetString(ctx.Body.Span)); return Task.CompletedTask; }, ct);
        await using var c2 = await inst.Bus.ConsumeAsync(second,
            new ConsumeOptions { Offset = ConsumeOffset.AlwaysFirst, Reference = Unique("read-2") },
            ctx => { lock (gotSecond) gotSecond.Add(Encoding.UTF8.GetString(ctx.Body.Span)); return Task.CompletedTask; }, ct);
        await WaitForAsync(() =>
        {
            lock (gotFirst) lock (gotSecond) return gotFirst.Count >= 1 && gotSecond.Count >= 1;
        }, TimeSpan.FromSeconds(30), "both streams delivered");
        await Task.Delay(500, ct);
        lock (gotFirst) lock (gotSecond)
        {
            Assert.Equal(new[] { "a:0" }, gotFirst);
            Assert.Equal(new[] { "b:0" }, gotSecond);
        }
    }

    [Fact]
    public async Task OnExhausted_owns_exhaustion_and_advances_past_poison()
    {
        // The raw ConsumeOptions path exposes exactly one exhaustion callback -
        // the same OnExhausted shape as the endpoint API. It owns the outcome:
        // returning advances past the message, with no separate error-stream
        // hand-off and no second observe-only hook.
        var ct = TestContext.Current.CancellationToken;
        var stream = Unique("acc-src");
        _streams.Add(stream);
        var admin = await ConnectAdminAsync();
        await using var _ = admin;
        await admin.CreateStream(new StreamSpec(stream));
        var exhausted = new List<ExhaustedContext>();
        var completed = new List<ulong>();
        await using var inst = await BusInstance.StartAsync();
        await using var consumer = await inst.Bus.ConsumeAsync(stream,
            new ConsumeOptions
            {
                Reference = Unique("grp-acc"),
                Retry = new RetryPolicy { MaxAttempts = 2, BaseDelay = TimeSpan.FromMilliseconds(10), MaxJitter = TimeSpan.Zero },
                OnExhausted = (ctx, _) =>
                {
                    lock (exhausted) exhausted.Add(ctx);
                    return Task.CompletedTask;
                },
            },
            ctx =>
            {
                if (ctx.Offset < 2) throw new InvalidOperationException($"poison {ctx.Offset}");
                lock (completed) completed.Add(ctx.Offset);
                return Task.CompletedTask;
            },
            ct);
        var pub = await BusInstance.StartAsync();
        try
        {
            await pub.Bus.PublishRawAsync(stream, Body("poison-a"), new PublishOptions(), ct);
            await pub.Bus.PublishRawAsync(stream, Body("poison-b"), new PublishOptions(), ct);
            await pub.Bus.PublishRawAsync(stream, Body("good:2"), new PublishOptions(), ct);
        }
        finally { await pub.DisposeAsync(); }
        await WaitForAsync(() => { lock (completed) return completed.Count >= 1; }, TimeSpan.FromSeconds(60), "good processed past both exhausted messages");
        List<ExhaustedContext> seen;
        lock (exhausted) seen = exhausted.OrderBy(e => e.Offset).ToList();
        Assert.Equal(new[] { 0UL, 1UL }, seen.Select(e => e.Offset).ToArray());
        foreach (var context in seen)
        {
            Assert.Equal(stream, context.Stream);
            Assert.Equal(0, context.Partition);
            Assert.Equal(2, context.Attempts);
            Assert.IsType<InvalidOperationException>(context.Exception);
            Assert.Contains($"poison {context.Offset}", context.Exception.Message);
            Assert.True(DateTimeOffset.UtcNow - context.FailedAt < TimeSpan.FromMinutes(1));
            Assert.True(context.FailedAt <= DateTimeOffset.UtcNow);
            Assert.NotNull(context.Services);
            Assert.NotNull(context.Headers);
        }
        lock (completed) Assert.Equal(new[] { 2UL }, completed);
    }
}
