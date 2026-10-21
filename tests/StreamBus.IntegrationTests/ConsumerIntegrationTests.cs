using System.Diagnostics.Metrics;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using RabbitMQ.Stream.Client;

namespace StreamBus.IntegrationTests;

public sealed record PropertyHeaderPayload(int Id);

public sealed class ConsumerIntegrationTests : IAsyncDisposable
{
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

    private static ReadOnlyMemory<byte> Body(string s) => System.Text.Encoding.UTF8.GetBytes(s);

    private static (string Key, int Seq) Parse(ReadOnlyMemory<byte> body)
    {
        var parts = System.Text.Encoding.UTF8.GetString(body.Span).Split(':');
        return (parts[0], int.Parse(parts[1], System.Globalization.CultureInfo.InvariantCulture));
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

    private static Func<ConsumeContext, Task> Rec(List<int> into) => ctx =>
    {
        var (_, seq) = Parse(ctx.Body);
        lock (into) into.Add(seq);
        return Task.CompletedTask;
    };

    [Fact]
    public async Task Super_stream_splits_across_instances_exactly_once_ordered()
    {
        var super = Unique("con-super");
        var admin = await ConnectAdminAsync();
        await using var _ = admin;
        await admin.CreateSuperStream(new PartitionsSuperStreamSpec(super, 4));
        _superStreams.Add(super);
        var keys = new[] { "k0", "k1", "k2", "k3", "k4", "k5" };
        // backlog first (no settle flake): publish 4 seq per key via a throwaway bus
        await using var pub = await BusInstance.StartAsync("pub");
        try
        {
            foreach (var seq in Enumerable.Range(1, 4))
                foreach (var key in keys)
                    await pub.Bus.PublishRawAsync(super, Body($"{key}:{seq}"),
                        new PublishOptions { IsSuperStream = true, RoutingKey = key }, TestContext.Current.CancellationToken);
        }
        finally { await pub.DisposeAsync(); }
        // three same-reference instances consume the 24-message backlog
        var instances = new List<BusInstance>();
        var all = new List<(string Key, int Seq, string Instance, int Partition)>();
        try
        {
            foreach (var name in new[] { "i0", "i1", "i2" })
            {
                var inst = await BusInstance.StartAsync(name);
                instances.Add(inst);
                await inst.Bus.ConsumeAsync(super,
                    new ConsumeOptions { IsSuperStream = true, Reference = "grp-a" },
                    ctx =>
                    {
                        var (key, seq) = Parse(ctx.Body);
                        lock (all) all.Add((key, seq, name, ctx.Partition));
                        return Task.CompletedTask;
                    }, TestContext.Current.CancellationToken);
            }
            await WaitForAsync(() => Count(all) == 24, TimeSpan.FromSeconds(60), "backlog drain");
        }
        finally { foreach (var inst in instances) await inst.DisposeAsync(); }
        Assert.Equal(24, all.Count);
        Assert.Equal(24, all.Select(x => (x.Key, x.Seq)).Distinct().Count()); // exactly once
        foreach (var key in keys)
            Assert.Equal(new[] { 1, 2, 3, 4 }, all.Where(x => x.Key == key).Select(x => x.Seq));
        // Partition enumeration sanity (the registry depends on stable QueryPartition order).
        var partitions = await admin.QueryPartition(super);
        Assert.Equal(4, partitions.Length);
    }

    [Fact]
    public async Task Failover_preserves_offsets_and_order()
    {
        var super = Unique("con-fail");
        var admin = await ConnectAdminAsync();
        await using var _ = admin;
        await admin.CreateSuperStream(new PartitionsSuperStreamSpec(super, 2));
        _superStreams.Add(super);
        var keys = new[] { "a", "b" };
        await using var pub = await BusInstance.StartAsync("pub");
        try
        {
            foreach (var seq in Enumerable.Range(1, 6))
                foreach (var key in keys)
                    await pub.Bus.PublishRawAsync(super, Body($"{key}:{seq}"),
                        new PublishOptions { IsSuperStream = true, RoutingKey = key }, TestContext.Current.CancellationToken);
        }
        finally { await pub.DisposeAsync(); }
        var all = new List<(string Key, int Seq, string Instance, int Partition, ulong Offset)>();
        var a = await BusInstance.StartAsync("fa");
        var b = await BusInstance.StartAsync("fb");
        var aDisposed = false;
        try
        {
            foreach (var (inst, name) in new[] { (a, "fa"), (b, "fb") })
                await inst.Bus.ConsumeAsync(super,
                    new ConsumeOptions { IsSuperStream = true, Reference = "grp-b" },
                    ctx =>
                    {
                        var (key, seq) = Parse(ctx.Body);
                        lock (all) all.Add((key, seq, name, ctx.Partition, ctx.Offset));
                        return Task.CompletedTask;
                    }, TestContext.Current.CancellationToken);
            await WaitForAsync(() => all.Any(x => x.Instance == "fa") && Count(all) >= 2, TimeSpan.FromSeconds(60), "A to own a partition");
            await a.DisposeAsync();
            aDisposed = true;
            await using var pub2 = await BusInstance.StartAsync("pub2");
            try
            {
                foreach (var seq in Enumerable.Range(7, 6))
                    foreach (var key in keys)
                        await pub2.Bus.PublishRawAsync(super, Body($"{key}:{seq}"),
                            new PublishOptions { IsSuperStream = true, RoutingKey = key }, TestContext.Current.CancellationToken);
            }
            finally { await pub2.DisposeAsync(); }
            await WaitForAsync(() => Count(all) == 24, TimeSpan.FromSeconds(90), "post-failover drain");
            Assert.Equal(24, all.Count);
            Assert.Equal(24, all.Select(x => (x.Key, x.Seq)).Distinct().Count());
            foreach (var key in keys)
                Assert.Equal(Enumerable.Range(1, 12), all.Where(x => x.Key == key).Select(x => x.Seq));
            foreach (var p in new[] { 0, 1 })
            {
                var received = all.Where(x => x.Partition == p).Select(x => x.Offset).ToList();
                var stored = await b.Bus.GetConsumerOffsetAsync(super, "grp-b", p, TestContext.Current.CancellationToken);
                if (received.Count == 0) Assert.Null(stored);
                else Assert.Equal(received.Max(), stored);
            }
        }
        finally
        {
            if (!aDisposed) await a.DisposeAsync();
            await b.DisposeAsync();
        }
    }

    [Fact]
    public async Task Distinct_references_each_receive_everything()
    {
        var stream = Unique("con-bcast");
        var admin = await ConnectAdminAsync();
        await using var _ = admin;
        await admin.CreateStream(new StreamSpec(stream));
        _streams.Add(stream);
        await using var pub = await BusInstance.StartAsync("pub");
        try
        {
            foreach (var seq in Enumerable.Range(0, 6))
                await pub.Bus.PublishRawAsync(stream, Body($"k:{seq}"), new PublishOptions(), TestContext.Current.CancellationToken);
        }
        finally { await pub.DisposeAsync(); }
        var g1 = new List<(int Seq, int Partition)>();
        var g2 = new List<(int Seq, int Partition)>();
        await using var i1 = await BusInstance.StartAsync("i1");
        await using var i2 = await BusInstance.StartAsync("i2");
        await i1.Bus.ConsumeAsync(stream, new ConsumeOptions { Reference = "grp-c1" }, ctx =>
        {
            var (_, seq) = Parse(ctx.Body);
            lock (g1) g1.Add((seq, ctx.Partition));
            return Task.CompletedTask;
        }, TestContext.Current.CancellationToken);
        await i2.Bus.ConsumeAsync(stream, new ConsumeOptions { Reference = "grp-c2" }, ctx =>
        {
            var (_, seq) = Parse(ctx.Body);
            lock (g2) g2.Add((seq, ctx.Partition));
            return Task.CompletedTask;
        }, TestContext.Current.CancellationToken);
        await WaitForAsync(() => Count(g1) == 6 && Count(g2) == 6, TimeSpan.FromSeconds(60), "broadcast drain");
        Assert.Equal(Enumerable.Range(0, 6), g1.Select(x => x.Seq));
        Assert.Equal(Enumerable.Range(0, 6), g2.Select(x => x.Seq));
    }

    [Fact]
    public async Task Recreate_resumes_from_broker_stored_offset()
    {
        var stream = Unique("con-resume");
        var admin = await ConnectAdminAsync();
        await using var _ = admin;
        await admin.CreateStream(new StreamSpec(stream));
        _streams.Add(stream);
        // Two backlog phases (deterministic: a plain-stream backlog drains in <200ms, so an
        // intermediate Count==5 wait overshoots and flakes — never wait mid-drain).
        await using var pub = await BusInstance.StartAsync("pub");
        try
        {
            foreach (var seq in Enumerable.Range(0, 5))
                await pub.Bus.PublishRawAsync(stream, Body($"k:{seq}"), new PublishOptions(), TestContext.Current.CancellationToken);
        }
        finally { await pub.DisposeAsync(); }
        var first = new List<int>();
        await using (var inst = await BusInstance.StartAsync("i"))
        {
            var c1 = await inst.Bus.ConsumeAsync(stream, new ConsumeOptions { Reference = "grp-d" }, Rec(first), TestContext.Current.CancellationToken);
            await WaitForAsync(() => Count(first) == 5, TimeSpan.FromSeconds(60), "first drain");
            await c1.DisposeAsync();
        }
        await using var pub2 = await BusInstance.StartAsync("pub2");
        try
        {
            foreach (var seq in Enumerable.Range(5, 5))
                await pub2.Bus.PublishRawAsync(stream, Body($"k:{seq}"), new PublishOptions(), TestContext.Current.CancellationToken);
        }
        finally { await pub2.DisposeAsync(); }
        // Fresh instance => fresh guard water: only a broker resume passes this test.
        var second = new List<int>();
        await using var inst2 = await BusInstance.StartAsync("i2");
        var c2 = await inst2.Bus.ConsumeAsync(stream, new ConsumeOptions { Reference = "grp-d" }, Rec(second), TestContext.Current.CancellationToken);
        await WaitForAsync(() => Count(second) == 5, TimeSpan.FromSeconds(60), "second drain");
        await c2.DisposeAsync();
        Assert.Equal(new[] { 0, 1, 2, 3, 4 }, first);
        Assert.Equal(new[] { 5, 6, 7, 8, 9 }, second);
        Assert.Equal(10, first.Concat(second).Distinct().Count());
    }

    [Fact]
    public async Task Offset_branches_behave()
    {
        var admin = await ConnectAdminAsync();
        await using var _ = admin;
        await using var inst = await BusInstance.StartAsync("i");
        // e1: resume, not replay (promotion-fix pin)
        var e1 = Unique("con-e1");
        await admin.CreateStream(new StreamSpec(e1));
        _streams.Add(e1);
        foreach (var seq in Enumerable.Range(0, 3))
            await inst.Bus.PublishRawAsync(e1, Body($"k:{seq}"), new PublishOptions(), TestContext.Current.CancellationToken);
        var first = new List<int>();
        var c1 = await inst.Bus.ConsumeAsync(e1, new ConsumeOptions { Reference = "grp-e1" }, Rec(first), TestContext.Current.CancellationToken);
        await WaitForAsync(() => Count(first) == 3, TimeSpan.FromSeconds(60), "e1 first drain");
        await c1.DisposeAsync();
        Assert.Equal(new[] { 0, 1, 2 }, first);
        foreach (var seq in Enumerable.Range(3, 3))
            await inst.Bus.PublishRawAsync(e1, Body($"k:{seq}"), new PublishOptions(), TestContext.Current.CancellationToken);
        var second = new List<int>();
        var c2 = await inst.Bus.ConsumeAsync(e1, new ConsumeOptions { Reference = "grp-e1" }, Rec(second), TestContext.Current.CancellationToken);
        await WaitForAsync(() => Count(second) == 3, TimeSpan.FromSeconds(60), "e1 resume drain");
        await c2.DisposeAsync();
        Assert.Equal(new[] { 3, 4, 5 }, second);
        // e2: ResumeOrFail on a fresh reference throws
        var e2 = Unique("con-e2");
        await admin.CreateStream(new StreamSpec(e2));
        _streams.Add(e2);
        await Assert.ThrowsAsync<StreamBusConfigurationException>(() =>
            inst.Bus.ConsumeAsync(e2, new ConsumeOptions { Reference = "grp-e2", Offset = ConsumeOffset.ResumeOrFail }, Rec(new List<int>()), TestContext.Current.CancellationToken));
        // e3: AlwaysFirst replays; AlwaysNext tails (attach point fixed at creation, so
        // publishing after ConsumeAsync returns is deterministically post-attach)
        var e3 = Unique("con-e3");
        await admin.CreateStream(new StreamSpec(e3));
        _streams.Add(e3);
        foreach (var seq in Enumerable.Range(0, 2))
            await inst.Bus.PublishRawAsync(e3, Body($"k:{seq}"), new PublishOptions(), TestContext.Current.CancellationToken);
        var replay = new List<int>();
        var c3 = await inst.Bus.ConsumeAsync(e3, new ConsumeOptions { Reference = "grp-e3", Offset = ConsumeOffset.AlwaysFirst }, Rec(replay), TestContext.Current.CancellationToken);
        await WaitForAsync(() => Count(replay) == 2, TimeSpan.FromSeconds(60), "e3 replay drain");
        await c3.DisposeAsync();
        Assert.Equal(new[] { 0, 1 }, replay);
        var tail = new List<int>();
        var c4 = await inst.Bus.ConsumeAsync(e3, new ConsumeOptions { Reference = "grp-e3b", Offset = ConsumeOffset.AlwaysNext }, Rec(tail), TestContext.Current.CancellationToken);
        foreach (var seq in Enumerable.Range(2, 2))
            await inst.Bus.PublishRawAsync(e3, Body($"k:{seq}"), new PublishOptions(), TestContext.Current.CancellationToken);
        await WaitForAsync(() => Count(tail) == 2, TimeSpan.FromSeconds(60), "e3 tail drain");
        await c4.DisposeAsync();
        Assert.Equal(new[] { 2, 3 }, tail);
    }

    [Fact]
    public async Task Set_then_get_round_trip_and_resume()
    {
        var stream = Unique("con-f");
        var admin = await ConnectAdminAsync();
        await using var _ = admin;
        await admin.CreateStream(new StreamSpec(stream));
        _streams.Add(stream);
        await using var pub = await BusInstance.StartAsync("pub");
        try
        {
            foreach (var s in Enumerable.Range(0, 10))
                await pub.Bus.PublishRawAsync(stream, Body($"k:{s}"), new PublishOptions(), TestContext.Current.CancellationToken);
        }
        finally { await pub.DisposeAsync(); }
        await using var inst = await BusInstance.StartAsync("i");
        await inst.Bus.SetConsumerOffsetAsync(stream, "grp-f", 0, 7, TestContext.Current.CancellationToken);
        Assert.Equal(7UL, await inst.Bus.GetConsumerOffsetAsync(stream, "grp-f", 0, TestContext.Current.CancellationToken));
        var got = new List<int>();
        var c = await inst.Bus.ConsumeAsync(stream, new ConsumeOptions { Reference = "grp-f" }, Rec(got), TestContext.Current.CancellationToken);
        await WaitForAsync(() => Count(got) == 2, TimeSpan.FromSeconds(60), "post-set drain");
        await c.DisposeAsync();
        Assert.Equal(new[] { 8, 9 }, got);
    }

    [Fact]
    public async Task Handler_failure_redelivers_without_advancing()
    {
        var stream = Unique("con-g");
        var admin = await ConnectAdminAsync();
        await using var _ = admin;
        await admin.CreateStream(new StreamSpec(stream));
        _streams.Add(stream);
        await using var pub = await BusInstance.StartAsync("pub");
        try
        {
            foreach (var seq in Enumerable.Range(0, 5))
                await pub.Bus.PublishRawAsync(stream, Body($"k:{seq}"), new PublishOptions(), TestContext.Current.CancellationToken);
        }
        finally { await pub.DisposeAsync(); }
        await using var inst = await BusInstance.StartAsync("i");
        var got = new List<int>();
        var attempts = new Dictionary<int, int>();
        var c = await inst.Bus.ConsumeAsync(stream, new ConsumeOptions { Reference = "grp-g" }, ctx =>
        {
            var (_, seq) = Parse(ctx.Body);
            lock (attempts) attempts[seq] = attempts.TryGetValue(seq, out var n) ? n + 1 : 1;
            if (seq == 2 && attempts[seq] <= 2) throw new InvalidOperationException("boom on 2");
            lock (got) got.Add(seq);
            return Task.CompletedTask;
        }, TestContext.Current.CancellationToken);
        await WaitForAsync(() => Count(got) == 5, TimeSpan.FromSeconds(90), "redelivery drain");
        await c.DisposeAsync();
        // Failures throw before recording, so only successes land in `got`; the attempts map
        // is what proves redelivery (offset 2 attempted 3 times: fail, fail, succeed).
        Assert.Equal(new[] { 0, 1, 2, 3, 4 }, got);
        Assert.Equal(3, attempts[2]);
        Assert.Equal(4UL, await inst.Bus.GetConsumerOffsetAsync(stream, "grp-g", 0, TestContext.Current.CancellationToken));
    }

    [Fact]
    public async Task Inflight_gauge_tracks_dispatched_uncompleted()
    {
        var stream = Unique("con-h");
        var admin = await ConnectAdminAsync();
        await using var _ = admin;
        await admin.CreateStream(new StreamSpec(stream));
        _streams.Add(stream);
        await using var pub = await BusInstance.StartAsync("pub");
        try
        {
            await pub.Bus.PublishRawAsync(stream, Body("k:0"), new PublishOptions(), TestContext.Current.CancellationToken);
        }
        finally { await pub.DisposeAsync(); }
        await using var inst = await BusInstance.StartAsync("i");
        using var listener = new MeterListener();
        var observed = new List<(string Name, double Value, string Partition)>();
        listener.InstrumentPublished = (instrument, listener2) =>
        {
            if (instrument.Meter.Name == "StreamBus") listener2.EnableMeasurementEvents(instrument);
        };
        listener.SetMeasurementEventCallback<long>((measurement, value, tags, _) =>
        {
            if (measurement.Name == "streambus_partition_inflight")
            {
                string? part = null;
                foreach (var tag in tags) if (tag.Key == "partition") part = tag.Value?.ToString();
                lock (observed) observed.Add((measurement.Name, value, part ?? "?"));
            }
        });
        listener.Start();
        var entered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var c = await inst.Bus.ConsumeAsync(stream, new ConsumeOptions { Reference = "grp-h" }, async ctx =>
        {
            entered.TrySetResult();
            await release.Task;
        }, TestContext.Current.CancellationToken);
        try
        {
            Assert.True(await Task.WhenAny(entered.Task, Task.Delay(TimeSpan.FromSeconds(30), TestContext.Current.CancellationToken)) == entered.Task, "handler entered");
            listener.RecordObservableInstruments();
            Assert.Contains(observed, m => m.Value == 1 && m.Partition == "0");
        }
        finally { release.TrySetResult(); }
        await WaitForAsync(() =>
        {
            listener.RecordObservableInstruments();
            lock (observed) return observed.Any(m => m.Value == 0);
        }, TimeSpan.FromSeconds(30), "inflight back to 0");
        await c.DisposeAsync();
    }

    [Fact]
    public async Task Properties_survive_to_consume_side_on_plain_and_super_streams()
    {
        var ct = TestContext.Current.CancellationToken;
        var stream = Unique("props-plain");
        var super = Unique("props-super");
        var admin = await ConnectAdminAsync();
        await using var _ = admin;
        await admin.CreateStream(new StreamSpec(stream));
        await admin.CreateSuperStream(new PartitionsSuperStreamSpec(super, 2));
        _streams.Add(stream);
        _superStreams.Add(super);

        var properties = new StreamMessageProperties
        {
            MessageId = "m-props",
            CorrelationId = "c-props",
            Headers = new Dictionary<string, object?>
            {
                ["tenant"] = "acme",
                ["attempt"] = 3,
                ["sequence"] = 9_000_000_000L,
                ["flag"] = true,
                ["payload"] = "{\"Id\":7}",
            },
        };

        var pub = await BusInstance.StartAsync("props-pub");
        try
        {
            await pub.Bus.PublishRawAsync(stream, Body("plain"), new PublishOptions { Properties = properties }, ct);
            await pub.Bus.PublishRawAsync(super, Body("super"),
                new PublishOptions { IsSuperStream = true, RoutingKey = "key-a", Properties = properties }, ct);
        }
        finally { await pub.DisposeAsync(); }

        var seen = new List<(string Kind, ConsumeContext Context)>();
        var consumer = await BusInstance.StartAsync("props-con");
        try
        {
            await using var plain = await consumer.Bus.ConsumeAsync(
                stream,
                new ConsumeOptions { Reference = "props-plain-grp", Offset = ConsumeOffset.AlwaysFirst },
                ctx =>
                {
                    lock (seen) seen.Add(("plain", ctx));
                    return Task.CompletedTask;
                },
                ct);
            await using var superSub = await consumer.Bus.ConsumeAsync(
                super,
                new ConsumeOptions { IsSuperStream = true, Reference = "props-super-grp", Offset = ConsumeOffset.AlwaysFirst },
                ctx =>
                {
                    lock (seen) seen.Add(("super", ctx));
                    return Task.CompletedTask;
                },
                ct);
            await WaitForAsync(() => Count(seen) >= 2, TimeSpan.FromSeconds(30), "plain and super property messages");
        }
        finally { await consumer.DisposeAsync(); }

        Assert.Contains(seen, x => x.Kind == "plain");
        Assert.Contains(seen, x => x.Kind == "super");
        foreach (var (_, context) in seen)
        {
            Assert.Equal("m-props", context.Metadata.MessageId);
            Assert.Equal("c-props", context.Metadata.CorrelationId);
            Assert.Equal("acme", context.Metadata.GetString("tenant"));
            Assert.Equal(3, context.Metadata.GetInt("attempt"));
            Assert.Equal(9_000_000_000L, context.Metadata.GetLong("sequence"));
            Assert.True(context.Metadata.GetBool("flag"));
            var payload = context.Metadata.GetJson<PropertyHeaderPayload>("payload");
            Assert.NotNull(payload);
            Assert.Equal(7, payload.Id);
        }
    }
}
