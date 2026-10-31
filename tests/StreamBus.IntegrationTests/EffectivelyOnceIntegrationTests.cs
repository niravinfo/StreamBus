using System.Text;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using RabbitMQ.Stream.Client;

namespace StreamBus.IntegrationTests;

internal sealed class EoAnnotatingFilter : IStreamConsumeFilter
{
    public Task OnConsume(StreamConsumeFilterContext context, ConsumeDelegate next, CancellationToken ct)
    {
        context.OutputHeaders.Set("tenant", "acme");
        return next();
    }
}

public sealed class EffectivelyOnceIntegrationTests : IAsyncDisposable
{
    public EffectivelyOnceIntegrationTests() => IntegrationLane.RequireBroker();

    private static string User() => Environment.GetEnvironmentVariable("STREAMBUS_USER") ?? "app";
    private static string Password() => Environment.GetEnvironmentVariable("STREAMBUS_PASSWORD") ?? "password";
    private readonly List<string> _streams = new();
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
        public string Name { get; private set; } = "";
        public static async Task<BusInstance> StartAsync(string name, string vhost = "/", Action<IStreamBusConfigurator>? configureBus = null)
        {
            var services = new ServiceCollection();
            services.AddLogging();
            services.AddStreamBus((_, cfg) =>
            {
                cfg.Host("localhost", vhost, h => h.Credentials(User(), Password()));
                configureBus?.Invoke(cfg);
            });
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

    private static ReadOnlyMemory<byte> Body(string s) => Encoding.UTF8.GetBytes(s);

    private static (string Key, int Seq) Parse(ReadOnlyMemory<byte> body)
    {
        var parts = Encoding.UTF8.GetString(body.Span).Split(':');
        return (parts[0], int.Parse(parts[1], System.Globalization.CultureInfo.InvariantCulture));
    }

    private static async Task WaitForAsync(Func<bool> done, TimeSpan timeout, string what)
    {
        var deadline = DateTime.UtcNow + timeout;
        while (!done())
        {
            if (DateTime.UtcNow >= deadline) Assert.Fail($"Timed out waiting for {what}.");
            await Task.Delay(100, TestContext.Current.CancellationToken);
        }
    }

    private static Task<ReadOnlyMemory<byte>> Echo(ConsumeContext ctx)
    {
        var (key, seq) = Parse(ctx.Body);
        return Task.FromResult(Body($"{key}:{seq}"));
    }

    private static async Task<List<(string Key, int Seq)>> ReadTargetAsync(IStreamBus bus, string target, bool isSuper, int expected, TimeSpan timeout)
    {
        var got = new List<(string Key, int Seq)>();
        await using var c = await bus.ConsumeAsync(target,
            new ConsumeOptions { Offset = ConsumeOffset.AlwaysFirst, Reference = Unique("read"), IsSuperStream = isSuper },
            ctx => { lock (got) got.Add(Parse(ctx.Body)); return Task.CompletedTask; },
            TestContext.Current.CancellationToken);
        await WaitForAsync(() => { lock (got) return got.Count >= expected; }, timeout, $"target {target} to reach {expected}");
        await Task.Delay(500, TestContext.Current.CancellationToken);
        lock (got) return got.ToList();
    }

    [Fact]
    public async Task Plain_to_plain_maps_publishing_id_to_source_offset()
    {
        var ct = TestContext.Current.CancellationToken;
        var source = Unique("eo-src");
        var target = Unique("eo-dst");
        var admin = await ConnectAdminAsync();
        await using var _ = admin;
        await admin.CreateStream(new StreamSpec(source));
        _streams.Add(source);
        await admin.CreateStream(new StreamSpec(target));
        _streams.Add(target);
        await using var pub = await BusInstance.StartAsync("pub");
        for (var seq = 0; seq < 5; seq++)
            await pub.Bus.PublishRawAsync(source, Body($"k:{seq}"), new PublishOptions(), ct);
        await pub.DisposeAsync();
        await using var eo = await BusInstance.StartAsync("eo");
        await using var consumer = await eo.Bus.ConsumeEffectivelyOnceAsync(source, target, new EffectivelyOnceOptions(), Echo, ct);
        var got = await ReadTargetAsync(eo.Bus, target, isSuper: false, expected: 5, TimeSpan.FromSeconds(30));
        Assert.Equal(5, got.Count);
        Assert.Equal(Enumerable.Range(0, 5).Select(i => ("k", i)).ToList(), got);
        var producerName = ResultNaming.ProducerName(source, target);
        Assert.Equal(4UL, await admin.QuerySequence(producerName, target));
        var eoRef = ResultNaming.ConsumerReference(typeof(DefaultBus), source, target);
        Assert.Null(await eo.Bus.GetConsumerOffsetAsync(source, eoRef, 0, ct));
    }

    [Fact]
    public async Task Republish_same_ids_keeps_first_write()
    {
        var ct = TestContext.Current.CancellationToken;
        var source = Unique("eo-src");
        var target = Unique("eo-dst");
        var admin = await ConnectAdminAsync();
        await using var _ = admin;
        await admin.CreateStream(new StreamSpec(source));
        _streams.Add(source);
        await admin.CreateStream(new StreamSpec(target));
        _streams.Add(target);
        await using var pub = await BusInstance.StartAsync("pub");
        for (var seq = 0; seq < 3; seq++)
            await pub.Bus.PublishRawAsync(source, Body($"k:{seq}"), new PublishOptions(), ct);
        await pub.DisposeAsync();
        await using var eo = await BusInstance.StartAsync("eo");
        await using var consumer = await eo.Bus.ConsumeEffectivelyOnceAsync(source, target, new EffectivelyOnceOptions(), Echo, ct);
        Assert.Equal(3, (await ReadTargetAsync(eo.Bus, target, false, 3, TimeSpan.FromSeconds(30))).Count);
        var producerName = ResultNaming.ProducerName(source, target);
        for (var seq = 0; seq < 3; seq++)
            await eo.Bus.PublishResultAsync(target, Body($"POISON:{seq}"), (ulong)seq,
                new PublishResultOptions { ProducerName = producerName }, ct);
        await Task.Delay(TimeSpan.FromSeconds(2), ct);
        var got = await ReadTargetAsync(eo.Bus, target, false, 3, TimeSpan.FromSeconds(30));
        Assert.Equal(3, got.Count);
        Assert.DoesNotContain(got, x => x.Key == "POISON");
        Assert.Equal(Enumerable.Range(0, 3).Select(i => ("k", i)).ToList(), got);
    }

    [Fact]
    public async Task Kill_mid_run_resumes_without_dupes_or_gaps()
    {
        var ct = TestContext.Current.CancellationToken;
        var source = Unique("eo-src");
        var target = Unique("eo-dst");
        var admin = await ConnectAdminAsync();
        await using var _ = admin;
        await admin.CreateStream(new StreamSpec(source));
        _streams.Add(source);
        await admin.CreateStream(new StreamSpec(target));
        _streams.Add(target);
        await using var pub = await BusInstance.StartAsync("pub");
        for (var seq = 0; seq < 10; seq++)
            await pub.Bus.PublishRawAsync(source, Body($"k:{seq}"), new PublishOptions(), ct);
        await pub.DisposeAsync();
        var gate = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
        var seen = 0;
        await using var eo1 = await BusInstance.StartAsync("eo1");
        await using var c1 = await eo1.Bus.ConsumeEffectivelyOnceAsync(source, target, new EffectivelyOnceOptions(),
            async ctx =>
            {
                if (Interlocked.Increment(ref seen) == 4) gate.TrySetResult(true);
                await Task.Delay(50, ctx.CancellationToken);
                var (key, seq) = Parse(ctx.Body);
                return Body($"{key}:{seq}");
            }, ct);
        Assert.True(await gate.Task.WaitAsync(TimeSpan.FromSeconds(30), ct));
        // Crash-like kill: the bus (not the consumer) is disposed, so in-flight
        // dispatches die without any bookkeeping to reconcile. Redelivery plus
        // (name, id) dedup must converge to exactly-once.
        await eo1.DisposeAsync();
        await using var eo2 = await BusInstance.StartAsync("eo2");
        await using var c2 = await eo2.Bus.ConsumeEffectivelyOnceAsync(source, target, new EffectivelyOnceOptions(), Echo, ct);
        var got = await ReadTargetAsync(eo2.Bus, target, false, 10, TimeSpan.FromSeconds(30));
        Assert.Equal(10, got.Count);
        Assert.Equal(Enumerable.Range(0, 10).Select(i => ("k", i)).ToList(), got);
    }

    [Fact]
    public async Task Handler_retry_preserves_partition_order()
    {
        var ct = TestContext.Current.CancellationToken;
        var source = Unique("eo-src");
        var target = Unique("eo-dst");
        var admin = await ConnectAdminAsync();
        await using var _ = admin;
        await admin.CreateStream(new StreamSpec(source));
        _streams.Add(source);
        await admin.CreateStream(new StreamSpec(target));
        _streams.Add(target);
        await using var pub = await BusInstance.StartAsync("pub");
        for (var seq = 0; seq < 5; seq++)
            await pub.Bus.PublishRawAsync(source, Body($"k:{seq}"), new PublishOptions(), ct);
        await pub.DisposeAsync();
        await using var eo = await BusInstance.StartAsync("eo");
        var attempts = new Dictionary<int, int>();
        await using var consumer = await eo.Bus.ConsumeEffectivelyOnceAsync(source, target,
            new EffectivelyOnceOptions { Retry = new RetryPolicy { BaseDelay = TimeSpan.FromMilliseconds(10), MaxJitter = TimeSpan.Zero } },
            ctx =>
            {
                var (key, seq) = Parse(ctx.Body);
                lock (attempts) attempts[seq] = attempts.TryGetValue(seq, out var n) ? n + 1 : 1;
                if (seq == 2 && attempts[seq] < 3) throw new InvalidOperationException("flaky 2");
                return Task.FromResult(Body($"{key}:{seq}"));
            }, ct);
        var got = await ReadTargetAsync(eo.Bus, target, false, 5, TimeSpan.FromSeconds(30));
        Assert.Equal(Enumerable.Range(0, 5).Select(i => ("k", i)).ToList(), got);
        Assert.Equal(3, attempts[2]);
    }

    [Fact]
    public async Task Super_source_to_plain_target_keeps_per_key_order()
    {
        var ct = TestContext.Current.CancellationToken;
        var source = Unique("eo-ssrc");
        var target = Unique("eo-dst");
        var admin = await ConnectAdminAsync();
        await using var _ = admin;
        await admin.CreateSuperStream(new PartitionsSuperStreamSpec(source, 3));
        _superStreams.Add(source);
        await admin.CreateStream(new StreamSpec(target));
        _streams.Add(target);
        await using var pub = await BusInstance.StartAsync("pub");
        for (var seq = 0; seq < 12; seq++)
            await pub.Bus.PublishRawAsync(source, Body($"k{seq % 4}:{seq}"),
                new PublishOptions { IsSuperStream = true, RoutingKey = $"k{seq % 4}" }, ct);
        await pub.DisposeAsync();
        await using var eo = await BusInstance.StartAsync("eo");
        await using var consumer = await eo.Bus.ConsumeEffectivelyOnceAsync(source, target,
            new EffectivelyOnceOptions { SourceIsSuperStream = true }, Echo, ct);
        var got = await ReadTargetAsync(eo.Bus, target, false, 12, TimeSpan.FromSeconds(60));
        Assert.Equal(12, got.Count);
        foreach (var grp in got.GroupBy(x => x.Key))
            Assert.Equal(grp.Select(x => x.Seq).OrderBy(x => x).ToList(), grp.Select(x => x.Seq).ToList());
    }

    [Fact]
    public async Task Plain_to_super_target_resumes_from_max()
    {
        var ct = TestContext.Current.CancellationToken;
        var source = Unique("eo-src");
        var target = Unique("eo-stgt");
        var admin = await ConnectAdminAsync();
        await using var _ = admin;
        await admin.CreateStream(new StreamSpec(source));
        _streams.Add(source);
        await admin.CreateSuperStream(new PartitionsSuperStreamSpec(target, 2));
        _superStreams.Add(target);
        await using var pub = await BusInstance.StartAsync("pub");
        for (var seq = 0; seq < 6; seq++)
            await pub.Bus.PublishRawAsync(source, Body($"k{seq % 2}:{seq}"), new PublishOptions(), ct);
        await pub.DisposeAsync();
        var eoOptions = new EffectivelyOnceOptions
        {
            TargetIsSuperStream = true,
            TargetRouting = ctx => Parse(ctx.Body).Key,
        };
        await using var eo1 = await BusInstance.StartAsync("eo1");
        await using var c1 = await eo1.Bus.ConsumeEffectivelyOnceAsync(source, target, eoOptions, Echo, ct);
        Assert.Equal(6, (await ReadTargetAsync(eo1.Bus, target, true, 6, TimeSpan.FromSeconds(60))).Count);
        await eo1.DisposeAsync();
        // Second generation publishes more, then resumes: only the new offsets flow.
        await using var pub2 = await BusInstance.StartAsync("pub2");
        for (var seq = 6; seq < 8; seq++)
            await pub2.Bus.PublishRawAsync(source, Body($"k{seq % 2}:{seq}"), new PublishOptions(), ct);
        await pub2.DisposeAsync();
        await using var eo2 = await BusInstance.StartAsync("eo2");
        await using var c2 = await eo2.Bus.ConsumeEffectivelyOnceAsync(source, target, eoOptions, Echo, ct);
        var got = await ReadTargetAsync(eo2.Bus, target, true, 8, TimeSpan.FromSeconds(60));
        Assert.Equal(8, got.Count);
        Assert.Equal(8, got.Select(x => (x.Key, x.Seq)).Distinct().Count());
    }

    [Fact]
    public async Task Super_target_without_routing_redelivers_and_writes_nothing()
    {
        var ct = TestContext.Current.CancellationToken;
        var source = Unique("eo-src");
        var target = Unique("eo-stgt");
        var admin = await ConnectAdminAsync();
        await using var _ = admin;
        await admin.CreateStream(new StreamSpec(source));
        _streams.Add(source);
        await admin.CreateSuperStream(new PartitionsSuperStreamSpec(target, 2));
        _superStreams.Add(target);
        await using var pub = await BusInstance.StartAsync("pub");
        await pub.Bus.PublishRawAsync(source, Body("k:0"), new PublishOptions(), ct);
        await pub.DisposeAsync();
        await using var eo = await BusInstance.StartAsync("eo");
        var entries = 0;
        await using var consumer = await eo.Bus.ConsumeEffectivelyOnceAsync(source, target,
            new EffectivelyOnceOptions
            {
                TargetIsSuperStream = true,
                Retry = new RetryPolicy { BaseDelay = TimeSpan.FromMilliseconds(10), MaxJitter = TimeSpan.Zero },
            },
            ctx => { Interlocked.Increment(ref entries); return Echo(ctx); }, ct);
        // No routing anywhere (unrouted source + default TargetRouting): the step
        // must redeliver (entries grow) and must never write the target.
        await WaitForAsync(() => Volatile.Read(ref entries) >= 3, TimeSpan.FromSeconds(30), "redeliveries");
        var got = new List<(string Key, int Seq)>();
        await using var reader = await eo.Bus.ConsumeAsync(target,
            new ConsumeOptions { Offset = ConsumeOffset.AlwaysFirst, Reference = Unique("read"), IsSuperStream = true },
            ctx => { lock (got) got.Add(Parse(ctx.Body)); return Task.CompletedTask; }, ct);
        await Task.Delay(TimeSpan.FromSeconds(2), ct);
        lock (got) Assert.Empty(got);
    }

    [Fact]
    public async Task Two_instances_share_one_group_without_corruption()
    {
        var ct = TestContext.Current.CancellationToken;
        var source = Unique("eo-ssrc");
        var target = Unique("eo-dst");
        var admin = await ConnectAdminAsync();
        await using var _ = admin;
        await admin.CreateSuperStream(new PartitionsSuperStreamSpec(source, 2));
        _superStreams.Add(source);
        await admin.CreateStream(new StreamSpec(target));
        _streams.Add(target);
        await using var pub = await BusInstance.StartAsync("pub");
        for (var seq = 0; seq < 8; seq++)
            await pub.Bus.PublishRawAsync(source, Body($"k{seq % 2}:{seq}"),
                new PublishOptions { IsSuperStream = true, RoutingKey = $"k{seq % 2}" }, ct);
        await pub.DisposeAsync();
        await using var a = await BusInstance.StartAsync("a");
        await using var b = await BusInstance.StartAsync("b");
        // Same derived reference (same bus type + streams) = one SAC group:
        // partitions split across instances, every message exactly once.
        await using var ca = await a.Bus.ConsumeEffectivelyOnceAsync(source, target,
            new EffectivelyOnceOptions { SourceIsSuperStream = true }, Echo, ct);
        await using var cb = await b.Bus.ConsumeEffectivelyOnceAsync(source, target,
            new EffectivelyOnceOptions { SourceIsSuperStream = true }, Echo, ct);
        var got = await ReadTargetAsync(a.Bus, target, false, 8, TimeSpan.FromSeconds(60));
        Assert.Equal(8, got.Count);
        Assert.Equal(8, got.Select(x => (x.Key, x.Seq)).Distinct().Count());
    }

    [Fact]
    public async Task Full_cycle_super_to_super_with_flakes_and_kill_G2()
    {
        var ct = TestContext.Current.CancellationToken;
        var source = Unique("eo-ssrc");
        var target = Unique("eo-stgt");
        var admin = await ConnectAdminAsync();
        await using var _ = admin;
        await admin.CreateSuperStream(new PartitionsSuperStreamSpec(source, 3));
        _superStreams.Add(source);
        await admin.CreateSuperStream(new PartitionsSuperStreamSpec(target, 2));
        _superStreams.Add(target);
        await using var pub = await BusInstance.StartAsync("pub");
        for (var seq = 0; seq < 20; seq++)
            await pub.Bus.PublishRawAsync(source, Body($"k{seq % 5}:{seq}"),
                new PublishOptions { IsSuperStream = true, RoutingKey = $"k{seq % 5}" }, ct);
        await pub.DisposeAsync();
        var eoOptions = new EffectivelyOnceOptions
        {
            SourceIsSuperStream = true,
            TargetIsSuperStream = true,
            TargetRouting = ctx => Parse(ctx.Body).Key,
            Retry = new RetryPolicy { BaseDelay = TimeSpan.FromMilliseconds(10), MaxJitter = TimeSpan.Zero },
        };
        Task<ReadOnlyMemory<byte>> Flaky(ConsumeContext ctx)
        {
            var (key, seq) = Parse(ctx.Body);
            if (seq % 5 == 0 && Interlocked.Increment(ref _flakes) % 2 == 1)
                throw new InvalidOperationException($"flake {seq}");
            return Task.FromResult(Body($"{key}:{seq}"));
        }
        var gate = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
        var seen = 0;
        await using var eo1 = await BusInstance.StartAsync("eo1");
        await using var c1 = await eo1.Bus.ConsumeEffectivelyOnceAsync(source, target, eoOptions,
            async ctx =>
            {
                if (Interlocked.Increment(ref seen) == 8) gate.TrySetResult(true);
                return await Flaky(ctx);
            }, ct);
        Assert.True(await gate.Task.WaitAsync(TimeSpan.FromSeconds(60), ct));
        await eo1.DisposeAsync();
        await using var eo2 = await BusInstance.StartAsync("eo2");
        await using var c2 = await eo2.Bus.ConsumeEffectivelyOnceAsync(source, target, eoOptions, Flaky, ct);
        var got = await ReadTargetAsync(eo2.Bus, target, true, 20, TimeSpan.FromSeconds(90));
        Assert.Equal(20, got.Count);
        Assert.Equal(20, got.Select(x => (x.Key, x.Seq)).Distinct().Count());
        foreach (var grp in got.GroupBy(x => x.Key))
            Assert.Equal(grp.Select(x => x.Seq).OrderBy(x => x).ToList(), grp.Select(x => x.Seq).ToList());
        // FR-6.6 spot-check: every target partition holds a sequence for at least
        // one stable producer (resume-from-max had something to read).
        var parts = await admin.QueryPartition(target);
        Assert.Equal(2, parts.Length);
    }

    [Fact]
    public async Task Pipeline_output_headers_are_published_on_the_result_target()
    {
        var ct = TestContext.Current.CancellationToken;
        var source = Unique("eo-src");
        var target = Unique("eo-dst");
        var admin = await ConnectAdminAsync();
        await using var _ = admin;
        await admin.CreateStream(new StreamSpec(source));
        _streams.Add(source);
        await admin.CreateStream(new StreamSpec(target));
        _streams.Add(target);
        await using var pub = await BusInstance.StartAsync("pub");
        for (var seq = 0; seq < 3; seq++)
            await pub.Bus.PublishRawAsync(source, Body($"k:{seq}"), new PublishOptions(), ct);
        await pub.DisposeAsync();

        await using var eo = await BusInstance.StartAsync("eo", configureBus: cfg => cfg.UseConsumeFilter<EoAnnotatingFilter>());
        await using var consumer = await eo.Bus.ConsumeEffectivelyOnceAsync(source, target, new EffectivelyOnceOptions(), Echo, ct);

        var got = new List<(int Seq, string? Tenant)>();
        await using var reader = await eo.Bus.ConsumeAsync(target,
            new ConsumeOptions { Offset = ConsumeOffset.AlwaysFirst, Reference = Unique("read") },
            ctx =>
            {
                var (_, seq) = Parse(ctx.Body);
                var tenant = ctx.Metadata.ApplicationProperties.TryGetValue("tenant", out var value) ? value?.ToString() : null;
                lock (got) got.Add((seq, tenant));
                return Task.CompletedTask;
            }, ct);
        await WaitForAsync(() => { lock (got) return got.Count >= 3; }, TimeSpan.FromSeconds(30), "the annotated result target");
        lock (got)
        {
            Assert.Equal(new[] { 0, 1, 2 }, got.Select(x => x.Seq).ToArray());
            Assert.All(got, x => Assert.Equal("acme", x.Tenant));
        }
    }

    private int _flakes;
}
