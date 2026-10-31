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
    public async Task OnRetryExhausted_observes_but_never_changes_outcome()
    {
        // FR-4.5 hook half: the hook observes the handoff (all fields) and even a
        // throwing hook cannot change the outcome (advance-once + error publish).
        var ct = TestContext.Current.CancellationToken;
        var stream = Unique("acc-src");
        var error = Unique("acc-src.error");
        _streams.Add(stream);
        _streams.Add(error);
        var admin = await ConnectAdminAsync();
        await using var _ = admin;
        await admin.CreateStream(new StreamSpec(stream));
        await admin.CreateStream(new StreamSpec(error) { MaxAge = TimeSpan.FromDays(7) });
        var handoffs = new List<ErrorHandoff>();
        var hookCalls = 0;
        var completed = new List<ulong>();
        await using var inst = await BusInstance.StartAsync();
        await using var consumer = await inst.Bus.ConsumeAsync(stream,
            new ConsumeOptions
            {
                Reference = Unique("grp-acc"),
                Retry = new RetryPolicy { MaxAttempts = 2, BaseDelay = TimeSpan.FromMilliseconds(10), MaxJitter = TimeSpan.Zero },
                ErrorStream = new ErrorStreamOptions { Stream = error },
                OnRetryExhausted = h =>
                {
                    Interlocked.Increment(ref hookCalls);
                    if (h.SourceOffset == 1) throw new InvalidOperationException("hook boom");
                    lock (handoffs) handoffs.Add(h);
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
        var errors = new List<byte[]>();
        await using var reader = await inst.Bus.ConsumeAsync(error,
            new ConsumeOptions { Offset = ConsumeOffset.AlwaysFirst, Reference = Unique("read") },
            ctx => { lock (errors) errors.Add(ctx.Body.ToArray()); return Task.CompletedTask; }, ct);
        await WaitForAsync(() => { lock (errors) return errors.Count >= 2; }, TimeSpan.FromSeconds(60), "both handoffs");
        await WaitForAsync(() => Volatile.Read(ref hookCalls) >= 2, TimeSpan.FromSeconds(30), "both hook runs");
        await WaitForAsync(() => { lock (completed) return completed.Count >= 1; }, TimeSpan.FromSeconds(30), "good processed past both handoffs");
        ErrorHandoff handoff;
        lock (handoffs) handoff = Assert.Single(handoffs);
        Assert.Equal(error, handoff.ErrorStream);
        Assert.Equal(stream, handoff.SourceStream);
        Assert.Equal(0, handoff.SourcePartition);
        Assert.Equal(0UL, handoff.SourceOffset);
        Assert.Equal(2, handoff.Attempts);
        Assert.IsType<InvalidOperationException>(handoff.Exception);
        Assert.Contains("poison 0", handoff.Exception.Message);
        Assert.True(DateTimeOffset.UtcNow - handoff.FailedAt < TimeSpan.FromMinutes(1));
        Assert.True(handoff.FailedAt <= DateTimeOffset.UtcNow);
        var bodies = new List<string>();
        lock (errors) bodies.AddRange(errors.Select(Encoding.UTF8.GetString));
        Assert.Contains("poison-a", bodies);
        Assert.Contains("poison-b", bodies);
        lock (completed) Assert.Equal(new[] { 2UL }, completed);
    }
}
