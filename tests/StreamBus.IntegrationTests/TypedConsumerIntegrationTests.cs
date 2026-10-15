using System.Text;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using RabbitMQ.Stream.Client;

namespace StreamBus.IntegrationTests;

public sealed class TypedConsumerIntegrationTests : IAsyncDisposable
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

    private sealed record OrderEvent(string OrderId, int Seq);

    private sealed class BusInstance : IAsyncDisposable
    {
        public ServiceProvider Provider { get; private set; } = null!;
        public StreamBusHostedService Hosted { get; private set; } = null!;
        public IStreamBus Bus { get; private set; } = null!;
        public static async Task<BusInstance> StartAsync(Action<ServiceCollection>? configure = null, string vhost = "/")
        {
            var services = new ServiceCollection();
            services.AddLogging();
            services.AddStreamBus((_, cfg) => cfg.Host("127.0.0.1", vhost, h => h.Credentials(User(), Password())));
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

    private sealed class OrderConsumer : IConsumer<OrderEvent>
    {
        private readonly List<(ConsumeContext<OrderEvent> Ctx, byte[] Raw)> _got;
        public OrderConsumer(List<(ConsumeContext<OrderEvent>, byte[])> got) => _got = got;
        public Task Consume(ConsumeContext<OrderEvent> context, CancellationToken ct)
        {
            lock (_got) _got.Add((context, context.Body.ToArray()));
            return Task.CompletedTask;
        }
    }

    private sealed class FlakyTypedConsumer : IConsumer<OrderEvent>
    {
        private readonly Dictionary<int, int> _attempts;
        private readonly List<(ConsumeContext<OrderEvent> Ctx, byte[] Raw)> _got;
        public FlakyTypedConsumer(
            Dictionary<int, int> attempts,
            List<(ConsumeContext<OrderEvent>, byte[])> got)
        {
            _attempts = attempts;
            _got = got;
        }
        public Task Consume(ConsumeContext<OrderEvent> context, CancellationToken ct)
        {
            int attempt;
            lock (_attempts)
            {
                attempt = _attempts.TryGetValue(context.Message.Seq, out var n) ? n + 1 : 1;
                _attempts[context.Message.Seq] = attempt;
            }
            if (context.Message.Seq == 2 && attempt < 3) throw new InvalidOperationException("flaky 2");
            lock (_got) _got.Add((context, context.Body.ToArray()));
            return Task.CompletedTask;
        }
    }

    private sealed class BusA { }
    private sealed class BusB { }

    [Fact]
    public async Task Typed_round_trip_plain()
    {
        var ct = TestContext.Current.CancellationToken;
        var stream = Unique("typed-orders");
        _streams.Add(stream);
        var admin = await ConnectAdminAsync();
        await using var _ = admin;
        await admin.CreateStream(new StreamSpec(stream));
        var got = new List<(ConsumeContext<OrderEvent> Ctx, byte[] Raw)>();
        await using var inst = await BusInstance.StartAsync(s =>
        {
            s.AddSingleton(got);
            s.AddStreamBusConsumer<OrderEvent, OrderConsumer>(stream);
        });
        await using var pub = await BusInstance.StartAsync();
        for (var seq = 0; seq < 3; seq++)
            await pub.Bus.PublishAsync(stream, Body(new OrderEvent("o", seq)), new PublishOptions(), ct);
        await pub.DisposeAsync();
        await WaitForAsync(() => { lock (got) return got.Count >= 3; }, TimeSpan.FromSeconds(30), "typed receipt");
        await Task.Delay(500, ct);
        lock (got)
        {
            Assert.Equal(3, got.Count);
            Assert.Equal(new[] { 0, 1, 2 }, got.Select(x => x.Ctx.Message.Seq).ToList());
            Assert.Equal(new[] { 0UL, 1UL, 2UL }, got.Select(x => x.Ctx.Offset).ToList());
            Assert.All(got, x => Assert.Equal(stream, x.Ctx.Stream));
            Assert.All(got, x => Assert.Equal(0, x.Ctx.Partition));
            Assert.All(got, x => Assert.Equal(SystemTextJsonStreamSerializer.Default.Serialize(x.Ctx.Message), x.Raw));
        }
    }

    [Fact]
    public async Task Multi_bus_isolation()
    {
        // One consumer type serves both buses (references differ by marker);
        // capture shares one list and stream names attribute delivery per bus.
        var ct = TestContext.Current.CancellationToken;
        var streamA = Unique("typed-a");
        var streamB = Unique("typed-b");
        _streams.Add(streamA);
        _streams.Add(streamB);
        var admin = await ConnectAdminAsync();
        await using var _ = admin;
        await admin.CreateStream(new StreamSpec(streamA));
        await admin.CreateStream(new StreamSpec(streamB));
        var got = new List<(ConsumeContext<OrderEvent> Ctx, byte[] Raw)>();
        await using var inst = await BusInstance.StartAsync(s =>
        {
            s.AddStreamBus<BusA>((_, cfg) => cfg.Host("127.0.0.1", "/", h => h.Credentials(User(), Password())));
            s.AddStreamBus<BusB>((_, cfg) => cfg.Host("127.0.0.1", "/", h => h.Credentials(User(), Password())));
            s.AddSingleton(got);
            s.AddStreamBusConsumer<BusA, OrderEvent, OrderConsumer>(streamA);
            s.AddStreamBusConsumer<BusB, OrderEvent, OrderConsumer>(streamB);
        });
        await using var pub = await BusInstance.StartAsync();
        for (var seq = 0; seq < 3; seq++)
        {
            await pub.Bus.PublishAsync(streamA, Body(new OrderEvent($"a-{seq}", seq)), new PublishOptions(), ct);
            await pub.Bus.PublishAsync(streamB, Body(new OrderEvent($"b-{seq}", seq)), new PublishOptions(), ct);
        }
        await pub.DisposeAsync();
        await WaitForAsync(() => { lock (got) return got.Count >= 6; }, TimeSpan.FromSeconds(30), "multi-bus receipt");
        await Task.Delay(500, ct);
        lock (got)
        {
            Assert.Equal(6, got.Count);
            Assert.Equal(new[] { 0, 1, 2 }, got.Where(x => x.Ctx.Stream == streamA).Select(x => x.Ctx.Message.Seq).OrderBy(x => x).ToList());
            Assert.Equal(new[] { 0, 1, 2 }, got.Where(x => x.Ctx.Stream == streamB).Select(x => x.Ctx.Message.Seq).OrderBy(x => x).ToList());
            Assert.All(got.Where(x => x.Ctx.Stream == streamA), x => Assert.StartsWith("a-", x.Ctx.Message.OrderId));
            Assert.All(got.Where(x => x.Ctx.Stream == streamB), x => Assert.StartsWith("b-", x.Ctx.Message.OrderId));
        }
    }

    [Fact]
    public async Task Binding_is_explicit()
    {
        var ct = TestContext.Current.CancellationToken;
        var streamA = Unique("typed-a");
        var streamB = Unique("typed-b");
        _streams.Add(streamA);
        _streams.Add(streamB);
        var admin = await ConnectAdminAsync();
        await using var _ = admin;
        await admin.CreateStream(new StreamSpec(streamA));
        await admin.CreateStream(new StreamSpec(streamB));
        var got = new List<(ConsumeContext<OrderEvent> Ctx, byte[] Raw)>();
        await using var inst = await BusInstance.StartAsync(s =>
        {
            s.AddSingleton(got);
            s.AddStreamBusConsumer<OrderEvent, OrderConsumer>(streamA);
        });
        await using var pub = await BusInstance.StartAsync();
        await pub.Bus.PublishAsync(streamA, Body(new OrderEvent("a", 0)), new PublishOptions(), ct);
        await pub.Bus.PublishAsync(streamA, Body(new OrderEvent("a", 1)), new PublishOptions(), ct);
        await pub.Bus.PublishAsync(streamB, Body(new OrderEvent("b", 0)), new PublishOptions(), ct);
        await pub.Bus.PublishAsync(streamB, Body(new OrderEvent("b", 1)), new PublishOptions(), ct);
        await pub.DisposeAsync();
        await WaitForAsync(() => { lock (got) return got.Count >= 2; }, TimeSpan.FromSeconds(30), "bound-stream receipt");
        await Task.Delay(TimeSpan.FromSeconds(2), ct);
        lock (got)
        {
            Assert.Equal(2, got.Count);
            Assert.All(got, x => Assert.Equal(streamA, x.Ctx.Stream));
        }
    }

    [Fact]
    public async Task Typed_super_stream_ordered_per_key()
    {
        var ct = TestContext.Current.CancellationToken;
        var super = Unique("typed-super");
        _superStreams.Add(super);
        var admin = await ConnectAdminAsync();
        await using var _ = admin;
        await admin.CreateSuperStream(new PartitionsSuperStreamSpec(super, 3));
        var got = new List<(ConsumeContext<OrderEvent> Ctx, byte[] Raw)>();
        await using var inst = await BusInstance.StartAsync(s =>
        {
            s.AddSingleton(got);
            s.AddStreamBusConsumer<OrderEvent, OrderConsumer>(super, o => o.SuperStream());
        });
        await using var pub = await BusInstance.StartAsync();
        for (var seq = 0; seq < 12; seq++)
            await pub.Bus.PublishAsync(super, Body(new OrderEvent($"k{seq % 4}", seq)),
                new PublishOptions { IsSuperStream = true, RoutingKey = $"k{seq % 4}" }, ct);
        await pub.DisposeAsync();
        await WaitForAsync(() => { lock (got) return got.Count >= 12; }, TimeSpan.FromSeconds(60), "keyed receipt");
        await Task.Delay(500, ct);
        lock (got)
        {
            Assert.Equal(12, got.Count);
            foreach (var grp in got.GroupBy(x => x.Ctx.Message.OrderId))
                Assert.Equal(grp.Select(x => x.Ctx.Message.Seq).OrderBy(x => x).ToList(), grp.Select(x => x.Ctx.Message.Seq).ToList());
        }
    }

    [Fact]
    public async Task Poison_skip_goes_to_error_stream()
    {
        var ct = TestContext.Current.CancellationToken;
        var stream = Unique("typed-src");
        var error = Unique("typed-src.error");
        _streams.Add(stream);
        _streams.Add(error);
        var admin = await ConnectAdminAsync();
        await using var _ = admin;
        await admin.CreateStream(new StreamSpec(stream));
        await admin.CreateStream(new StreamSpec(error) { MaxAge = TimeSpan.FromDays(7) });
        var got = new List<(ConsumeContext<OrderEvent> Ctx, byte[] Raw)>();
        await using var inst = await BusInstance.StartAsync(s =>
        {
            s.AddSingleton(got);
            s.AddStreamBusConsumer<OrderEvent, OrderConsumer>(stream, o => o
                .Retry(new RetryPolicy { FailureMode = FailureMode.SkipAfterDeadline, MaxAttempts = 2, BaseDelay = TimeSpan.FromMilliseconds(10), MaxJitter = TimeSpan.Zero })
                .ErrorStream(new ErrorStreamOptions { Stream = error }));
        });
        await using var pub = await BusInstance.StartAsync();
        await pub.Bus.PublishAsync(stream, Encoding.UTF8.GetBytes("{bad json"), new PublishOptions(), ct);
        await pub.Bus.PublishAsync(stream, Body(new OrderEvent("ok", 1)), new PublishOptions(), ct);
        await pub.DisposeAsync();
        await WaitForAsync(() => { lock (got) return got.Count >= 1; }, TimeSpan.FromSeconds(30), "post-poison good message");
        Assert.Equal("ok", got[0].Ctx.Message.OrderId);
        var errors = new List<byte[]>();
        await using var reader = await inst.Bus.ConsumeAsync(error,
            new ConsumeOptions { Offset = ConsumeOffset.AlwaysFirst, Reference = Unique("read") },
            ctx => { lock (errors) errors.Add(ctx.Body.ToArray()); return Task.CompletedTask; }, ct);
        await WaitForAsync(() => { lock (errors) return errors.Count >= 1; }, TimeSpan.FromSeconds(30), "error handoff");
        Assert.Equal("{bad json", Encoding.UTF8.GetString(errors[0]));
    }

    [Fact]
    public async Task Poison_strict_never_skips()
    {
        var ct = TestContext.Current.CancellationToken;
        var stream = Unique("typed-strict");
        _streams.Add(stream);
        var admin = await ConnectAdminAsync();
        await using var _ = admin;
        await admin.CreateStream(new StreamSpec(stream));
        var got = new List<(ConsumeContext<OrderEvent> Ctx, byte[] Raw)>();
        await using var inst = await BusInstance.StartAsync(s =>
        {
            s.AddSingleton(got);
            s.AddStreamBusConsumer<OrderEvent, OrderConsumer>(stream);
        });
        await using var pub = await BusInstance.StartAsync();
        await pub.Bus.PublishAsync(stream, Body(new OrderEvent("good", 0)), new PublishOptions(), ct);
        await pub.Bus.PublishAsync(stream, Encoding.UTF8.GetBytes("{bad json"), new PublishOptions(), ct);
        await pub.Bus.PublishAsync(stream, Body(new OrderEvent("good", 1)), new PublishOptions(), ct);
        await pub.DisposeAsync();
        await WaitForAsync(() => { lock (got) return got.Count >= 1; }, TimeSpan.FromSeconds(30), "first good message");
        await Task.Delay(TimeSpan.FromSeconds(5), ct);
        lock (got)
        {
            var only = Assert.Single(got);
            Assert.Equal(0, only.Ctx.Message.Seq);
        }
    }

    [Fact]
    public async Task Retry_preserves_order_typed()
    {
        var ct = TestContext.Current.CancellationToken;
        var stream = Unique("typed-retry");
        _streams.Add(stream);
        var admin = await ConnectAdminAsync();
        await using var _ = admin;
        await admin.CreateStream(new StreamSpec(stream));
        var got = new List<(ConsumeContext<OrderEvent> Ctx, byte[] Raw)>();
        var attempts = new Dictionary<int, int>();
        await using var inst = await BusInstance.StartAsync(s =>
        {
            s.AddSingleton(got);
            s.AddSingleton(attempts);
            s.AddStreamBusConsumer<OrderEvent, FlakyTypedConsumer>(stream,
                o => o.Retry(new RetryPolicy { BaseDelay = TimeSpan.FromMilliseconds(10), MaxJitter = TimeSpan.Zero }));
        });
        await using var pub = await BusInstance.StartAsync();
        for (var seq = 0; seq < 5; seq++)
            await pub.Bus.PublishAsync(stream, Body(new OrderEvent("o", seq)), new PublishOptions(), ct);
        await pub.DisposeAsync();
        await WaitForAsync(() => { lock (got) return got.Count >= 5; }, TimeSpan.FromSeconds(60), "retry drain");
        await Task.Delay(500, ct);
        lock (got)
        {
            Assert.Equal(5, got.Count);
            Assert.Equal(new[] { 0, 1, 2, 3, 4 }, got.Select(x => x.Ctx.Message.Seq).ToList());
        }
        lock (attempts)
        {
            Assert.Equal(3, attempts[2]);
        }
    }

    [Fact]
    public async Task Type_mismatch_is_loud()
    {
        // Same options both sides is a deployment obligation; a wire-type
        // mismatch must land in the error stream, never misread silently.
        var ct = TestContext.Current.CancellationToken;
        var stream = Unique("typed-mismatch");
        var error = Unique("typed-mismatch.error");
        _streams.Add(stream);
        _streams.Add(error);
        var admin = await ConnectAdminAsync();
        await using var _ = admin;
        await admin.CreateStream(new StreamSpec(stream));
        await admin.CreateStream(new StreamSpec(error) { MaxAge = TimeSpan.FromDays(7) });
        var got = new List<(ConsumeContext<OrderEvent> Ctx, byte[] Raw)>();
        await using var inst = await BusInstance.StartAsync(s =>
        {
            s.AddSingleton(got);
            s.AddStreamBusConsumer<OrderEvent, OrderConsumer>(stream, o => o
                .Retry(new RetryPolicy { FailureMode = FailureMode.SkipAfterDeadline, MaxAttempts = 1, BaseDelay = TimeSpan.FromMilliseconds(10), MaxJitter = TimeSpan.Zero })
                .ErrorStream(new ErrorStreamOptions { Stream = error }));
        });
        await using var pub = await BusInstance.StartAsync();
        var mismatched = Encoding.UTF8.GetBytes("{\"OrderId\":123,\"Seq\":1}");
        await pub.Bus.PublishAsync(stream, mismatched, new PublishOptions(), ct);
        await pub.DisposeAsync();
        var errors = new List<byte[]>();
        await using var reader = await inst.Bus.ConsumeAsync(error,
            new ConsumeOptions { Offset = ConsumeOffset.AlwaysFirst, Reference = Unique("read") },
            ctx => { lock (errors) errors.Add(ctx.Body.ToArray()); return Task.CompletedTask; }, ct);
        await WaitForAsync(() => { lock (errors) return errors.Count >= 1; }, TimeSpan.FromSeconds(30), "mismatch handoff");
        Assert.Equal(mismatched, errors[0]);
        await Task.Delay(TimeSpan.FromSeconds(2), ct);
        lock (got)
        {
            Assert.Empty(got);
        }
    }
}
