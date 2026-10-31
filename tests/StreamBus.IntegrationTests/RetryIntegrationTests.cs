using System.Diagnostics.Metrics;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using RabbitMQ.Stream.Client;

namespace StreamBus.IntegrationTests;

public sealed class RetryIntegrationTests : IAsyncDisposable
{
    public RetryIntegrationTests() => IntegrationLane.RequireBroker();

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

    // Error streams carry mandatory x-max-age retention (FR-5.2); the retention is
    // declared here at creation (test-only admin — the library never creates streams).
    private static StreamSpec ErrorSpec(string stream) => new(stream) { MaxAge = TimeSpan.FromDays(7) };

    private static string Env(Dictionary<string, string?> app, string key)
        => app.TryGetValue("streambus.error." + key, out var v) ? v ?? "" : "";

    private async Task RunSkipCaseAsync(int maxAttempts, int expectedAttempts)
    {
        var stream = Unique("retry-src");
        var error = Unique("retry-src.error");
        var admin = await ConnectAdminAsync();
        await using var _ = admin;
        await admin.CreateStream(new StreamSpec(stream));
        _streams.Add(stream);
        await admin.CreateStream(ErrorSpec(error));
        _streams.Add(error);
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
        var c = await inst.Bus.ConsumeAsync(stream,
            new ConsumeOptions
            {
                Reference = "grp-r1",
                Retry = new RetryPolicy { MaxAttempts = maxAttempts },
                ErrorStream = new ErrorStreamOptions { Stream = error },
            },
            ctx =>
            {
                var (_, seq) = Parse(ctx.Body);
                lock (attempts) attempts[seq] = attempts.TryGetValue(seq, out var n) ? n + 1 : 1;
                if (seq == 2) throw new InvalidOperationException("poison 2");
                lock (got) got.Add(seq);
                return Task.CompletedTask;
            }, TestContext.Current.CancellationToken);
        await WaitForAsync(() => Count(got) == 4, TimeSpan.FromSeconds(90), "post-handoff drain");
        await c.DisposeAsync();
        Assert.Equal(new[] { 0, 1, 3, 4 }, got);
        Assert.Equal(expectedAttempts, attempts[2]);
        Assert.All(attempts.Where(kv => kv.Key != 2), kv => Assert.Equal(1, kv.Value));
        Assert.Equal(4UL, await inst.Bus.GetConsumerOffsetAsync(stream, "grp-r1", 0, TestContext.Current.CancellationToken));
        var errors = new List<(string Body, Dictionary<string, string?> App)>();
        var ec = await inst.Bus.ConsumeAsync(error,
            new ConsumeOptions { Reference = "grp-r1-err", Offset = ConsumeOffset.AlwaysFirst },
            ctx =>
            {
                var app = ctx.Headers.All.ToDictionary(kv => kv.Key, kv => kv.Value?.ToString());
                lock (errors) errors.Add((System.Text.Encoding.UTF8.GetString(ctx.Body.Span), app));
                return Task.CompletedTask;
            },
            TestContext.Current.CancellationToken);
        await WaitForAsync(() => Count(errors) == 1, TimeSpan.FromSeconds(60), "error drain");
        await ec.DisposeAsync();
        var (eBody, eApp) = errors[0];
        Assert.Equal("k:2", eBody);
        Assert.Equal(stream, Env(eApp, "source-stream"));
        Assert.Equal("0", Env(eApp, "source-partition"));
        Assert.Equal("2", Env(eApp, "source-offset"));
        Assert.Equal(expectedAttempts.ToString(System.Globalization.CultureInfo.InvariantCulture), Env(eApp, "attempts"));
        Assert.Contains("InvalidOperationException", Env(eApp, "exception-type"));
        Assert.Equal("poison 2", Env(eApp, "exception-message"));
        Assert.False(string.IsNullOrWhiteSpace(Env(eApp, "exception-stack")));
        Assert.False(string.IsNullOrWhiteSpace(Env(eApp, "failed-at")));
    }

    [Fact]
    public async Task Skip_handoff_publishes_envelope_and_advances_once()
        => await RunSkipCaseAsync(3, 3);

    [Fact]
    public async Task Skip_with_max_attempts_1_hands_off_after_one_attempt()
        => await RunSkipCaseAsync(1, 1);

    [Fact]
    public async Task Strict_retry_window_survives_kill_without_skip()
    {
        var stream = Unique("retry-g5b");
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
        var attempts = new Dictionary<int, int>();
        var entered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var first = new List<int>();
        await using (var inst = await BusInstance.StartAsync("i"))
        {
            var c1 = await inst.Bus.ConsumeAsync(stream, new ConsumeOptions { Reference = "grp-r2" }, async ctx =>
            {
                var (_, seq) = Parse(ctx.Body);
                int n;
                lock (attempts) { attempts[seq] = attempts.TryGetValue(seq, out var m) ? m + 1 : 1; n = attempts[seq]; }
                if (seq == 1 && n < 3) throw new InvalidOperationException("flaky 1");
                // Third attempt blocks inside dispatch: the Strict retry window is now held open.
                if (seq == 1 && n == 3) { entered.TrySetResult(); await Task.Delay(Timeout.Infinite, ctx.CancellationToken); }
                lock (first) first.Add(seq);
            }, TestContext.Current.CancellationToken);
            Assert.True(await Task.WhenAny(entered.Task, Task.Delay(TimeSpan.FromSeconds(30), TestContext.Current.CancellationToken)) == entered.Task, "third attempt entered");
            // Kill mid-retry-window: disposal cancels the attach token, aborting the wait.
            await c1.DisposeAsync();
        }
        // Fresh instance => fresh guard water: only a broker resume passes this test.
        var second = new List<int>();
        await using var inst2 = await BusInstance.StartAsync("i2");
        var c2 = await inst2.Bus.ConsumeAsync(stream, new ConsumeOptions { Reference = "grp-r2" }, ctx =>
        {
            var (_, seq) = Parse(ctx.Body);
            lock (second) second.Add(seq);
            return Task.CompletedTask;
        }, TestContext.Current.CancellationToken);
        await WaitForAsync(() => Count(second) == 4, TimeSpan.FromSeconds(60), "post-kill drain");
        await c2.DisposeAsync();
        // `first` documents the teardown race, it does not pin it: killing the consumer
        // mid-retry-window lets already-buffered dispatches (2, 3, 4) drain in-memory
        // while teardown runs, but their stores are dropped by the cancellation check
        // so nothing is falsely advanced. Deterministic pins: msg 0 processed first,
        // msg 1 never recorded here (both attempts threw, the third blocked), attempts
        // total exactly 3 (no redelivery is possible once the loop exits).
        Assert.Equal(0, first[0]);
        Assert.DoesNotContain(1, first);
        Assert.Equal(first.Skip(1).OrderBy(x => x), first.Skip(1));
        Assert.All(first.Skip(1), x => Assert.InRange(x, 2, 4));
        Assert.Equal(first.Skip(1).Distinct().Count(), first.Count - 1);
        Assert.Equal(new[] { 1, 2, 3, 4 }, second);
        Assert.Equal(3, attempts[1]);
        Assert.Equal(4UL, await inst2.Bus.GetConsumerOffsetAsync(stream, "grp-r2", 0, TestContext.Current.CancellationToken));
    }

    [Fact]
    public async Task Retry_preserves_order_under_load_and_failover()
    {
        var super = Unique("retry-g1");
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
        var attempts = new Dictionary<(string Key, int Seq), int>();
        Func<string, Func<ConsumeContext, Task>> handler = name => ctx =>
        {
            var (key, seq) = Parse(ctx.Body);
            bool fail;
            lock (attempts)
            {
                var k = (key, seq);
                var n = attempts.TryGetValue(k, out var m) ? m + 1 : 1;
                attempts[k] = n;
                fail = n == 1;
            }
            if (fail) throw new InvalidOperationException($"first-attempt flake {key}:{seq}");
            lock (all) all.Add((key, seq, name, ctx.Partition, ctx.Offset));
            return Task.CompletedTask;
        };
        var a = await BusInstance.StartAsync("ga");
        var b = await BusInstance.StartAsync("gb");
        var aDisposed = false;
        try
        {
            await a.Bus.ConsumeAsync(super,
                new ConsumeOptions { IsSuperStream = true, Reference = "grp-r3" }, handler("ga"), TestContext.Current.CancellationToken);
            await b.Bus.ConsumeAsync(super,
                new ConsumeOptions { IsSuperStream = true, Reference = "grp-r3" }, handler("gb"), TestContext.Current.CancellationToken);
            await WaitForAsync(() => all.Any(x => x.Instance == "ga") && Count(all) >= 2, TimeSpan.FromSeconds(60), "A to own a partition");
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
            await WaitForAsync(() => Count(all) == 24, TimeSpan.FromSeconds(120), "post-failover drain");
            Assert.Equal(24, all.Count);
            Assert.Equal(24, all.Select(x => (x.Key, x.Seq)).Distinct().Count());
            foreach (var key in keys)
                Assert.Equal(Enumerable.Range(1, 12), all.Where(x => x.Key == key).Select(x => x.Seq));
            foreach (var p in new[] { 0, 1 })
            {
                var received = all.Where(x => x.Partition == p).ToList();
                var stored = await b.Bus.GetConsumerOffsetAsync(super, "grp-r3", p, TestContext.Current.CancellationToken);
                if (received.Count == 0) Assert.Null(stored);
                else Assert.Equal(received.Max(x => x.Offset), stored);
            }
        }
        finally
        {
            if (!aDisposed) await a.DisposeAsync();
            await b.DisposeAsync();
        }
    }

    [Fact]
    public async Task Replay_twice_stores_once()
    {
        var stream = Unique("retry-replay");
        var error = Unique("retry-replay.error");
        var admin = await ConnectAdminAsync();
        await using var _ = admin;
        await admin.CreateStream(new StreamSpec(stream));
        _streams.Add(stream);
        await admin.CreateStream(ErrorSpec(error));
        _streams.Add(error);
        await using var pub = await BusInstance.StartAsync("pub");
        var replayedId = Guid.NewGuid();
        var replayedSentTime = new DateTime(2026, 10, 31, 9, 12, 3, 284, DateTimeKind.Utc);
        try
        {
            foreach (var seq in Enumerable.Range(0, 3))
                await pub.Bus.PublishRawAsync(stream, Body($"k:{seq}"), new PublishOptions
                {
                    Properties = seq == 1
                        ? new StreamMessageProperties { MessageId = replayedId, SentTime = replayedSentTime }
                        : new StreamMessageProperties(),
                }, TestContext.Current.CancellationToken);
        }
        finally { await pub.DisposeAsync(); }
        await using var inst = await BusInstance.StartAsync("i");
        var c = await inst.Bus.ConsumeAsync(stream,
            new ConsumeOptions
            {
                Reference = "grp-rd",
                Retry = new RetryPolicy { MaxAttempts = 2 },
                ErrorStream = new ErrorStreamOptions { Stream = error },
            },
            ctx =>
            {
                var (_, seq) = Parse(ctx.Body);
                if (seq == 1) throw new InvalidOperationException("poison 1");
                return Task.CompletedTask;
            }, TestContext.Current.CancellationToken);
        var handoffDeadline = DateTime.UtcNow + TimeSpan.FromSeconds(90);
        while (await inst.Bus.GetConsumerOffsetAsync(stream, "grp-rd", 0, TestContext.Current.CancellationToken) != 2UL)
        {
            if (DateTime.UtcNow >= handoffDeadline) Assert.Fail("Timed out waiting for handoff advance.");
            await Task.Delay(100, TestContext.Current.CancellationToken);
        }
        await c.DisposeAsync();
        var read = new List<ConsumeContext>();
        var rc = await inst.Bus.ConsumeAsync(stream,
            new ConsumeOptions { Reference = "grp-rd-read", Offset = ConsumeOffset.AlwaysFirst },
            ctx => { lock (read) read.Add(ctx); return Task.CompletedTask; },
            TestContext.Current.CancellationToken);
        Assert.Equal(1, await inst.Bus.ReplayErrorStreamAsync(error, null, TestContext.Current.CancellationToken));
        await WaitForAsync(() => Count(read) == 4, TimeSpan.FromSeconds(60), "replayed read");
        Assert.Equal(new[] { "k:0", "k:1", "k:2", "k:1" },
            read.Select(ctx => System.Text.Encoding.UTF8.GetString(ctx.Body.Span)).ToArray());
        Assert.All(read.Where(ctx => System.Text.Encoding.UTF8.GetString(ctx.Body.Span) == "k:1"), ctx =>
        {
            Assert.Equal(replayedId, ctx.MessageId);
            Assert.Equal(replayedSentTime, ctx.SentTime);
        });
        // Garbage on the error stream is skipped without throwing; the real envelope
        // replays again but broker dedup (stable name + id = source offset) stores nothing new.
        await inst.Bus.PublishRawAsync(error, Body("not-an-envelope"), new PublishOptions(), TestContext.Current.CancellationToken);
        Assert.Equal(1, await inst.Bus.ReplayErrorStreamAsync(error, null, TestContext.Current.CancellationToken));
        await Task.Delay(TimeSpan.FromSeconds(3), TestContext.Current.CancellationToken);
        await rc.DisposeAsync();
        Assert.Equal(new[] { "k:0", "k:1", "k:2", "k:1" },
            read.Select(ctx => System.Text.Encoding.UTF8.GetString(ctx.Body.Span)).ToArray());
    }

    [Fact]
    public async Task Shared_and_per_stream_error_forms_coexist()
    {
        var s1 = Unique("retry-e1");
        var s2 = Unique("retry-e2");
        var shared = Unique("retry-shared.error");
        var admin = await ConnectAdminAsync();
        await using var _ = admin;
        await admin.CreateStream(new StreamSpec(s1));
        _streams.Add(s1);
        await admin.CreateStream(new StreamSpec(s2));
        _streams.Add(s2);
        await admin.CreateStream(ErrorSpec(shared));
        _streams.Add(shared);
        await using var pub = await BusInstance.StartAsync("pub");
        try
        {
            await pub.Bus.PublishRawAsync(s1, Body("a:0"), new PublishOptions(), TestContext.Current.CancellationToken);
            await pub.Bus.PublishRawAsync(s2, Body("b:0"), new PublishOptions(), TestContext.Current.CancellationToken);
        }
        finally { await pub.DisposeAsync(); }
        await using var inst = await BusInstance.StartAsync("i");
        var c1 = await inst.Bus.ConsumeAsync(s1,
            new ConsumeOptions
            {
                Reference = "grp-re1",
                Retry = new RetryPolicy { MaxAttempts = 2 },
                ErrorStream = new ErrorStreamOptions { Stream = shared },
            },
            _ => throw new InvalidOperationException("poison a"),
            TestContext.Current.CancellationToken);
        var c2 = await inst.Bus.ConsumeAsync(s2,
            new ConsumeOptions
            {
                Reference = "grp-re2",
                Retry = new RetryPolicy { MaxAttempts = 2 },
                ErrorStream = new ErrorStreamOptions { Stream = shared },
            },
            _ => throw new InvalidOperationException("poison b"),
            TestContext.Current.CancellationToken);
        var errors = new List<(string Body, Dictionary<string, string?> App)>();
        var ec = await inst.Bus.ConsumeAsync(shared,
            new ConsumeOptions { Reference = "grp-re-read", Offset = ConsumeOffset.AlwaysFirst },
            ctx =>
            {
                var app = ctx.Headers.All.ToDictionary(kv => kv.Key, kv => kv.Value?.ToString());
                lock (errors) errors.Add((System.Text.Encoding.UTF8.GetString(ctx.Body.Span), app));
                return Task.CompletedTask;
            },
            TestContext.Current.CancellationToken);
        await WaitForAsync(() => Count(errors) == 2, TimeSpan.FromSeconds(90), "shared error drain");
        await c1.DisposeAsync();
        await c2.DisposeAsync();
        await ec.DisposeAsync();
        Assert.Equal(
            new[] { "a:0", "b:0" },
            errors.Select(e => e.Body).OrderBy(static body => body, StringComparer.Ordinal));
    }

    [Fact]
    public async Task Strict_and_skip_modes_coexist_per_consumer()
    {
        var strictStream = Unique("retry-co-s");
        var skipStream = Unique("retry-co-k");
        var error = Unique("retry-co.error");
        var admin = await ConnectAdminAsync();
        await using var _ = admin;
        await admin.CreateStream(new StreamSpec(strictStream));
        _streams.Add(strictStream);
        await admin.CreateStream(new StreamSpec(skipStream));
        _streams.Add(skipStream);
        await admin.CreateStream(ErrorSpec(error));
        _streams.Add(error);
        await using var pub = await BusInstance.StartAsync("pub");
        try
        {
            foreach (var seq in Enumerable.Range(0, 3))
                await pub.Bus.PublishRawAsync(strictStream, Body($"s:{seq}"), new PublishOptions(), TestContext.Current.CancellationToken);
            foreach (var seq in Enumerable.Range(0, 2))
                await pub.Bus.PublishRawAsync(skipStream, Body($"k:{seq}"), new PublishOptions(), TestContext.Current.CancellationToken);
        }
        finally { await pub.DisposeAsync(); }
        await using var inst = await BusInstance.StartAsync("i");
        var strictGot = new List<int>();
        var strictAttempts = new Dictionary<int, int>();
        var cs = await inst.Bus.ConsumeAsync(strictStream, new ConsumeOptions { Reference = "grp-rf-s" }, ctx =>
        {
            var (_, seq) = Parse(ctx.Body);
            bool fail;
            lock (strictAttempts)
            {
                var n = strictAttempts.TryGetValue(seq, out var m) ? m + 1 : 1;
                strictAttempts[seq] = n;
                fail = n == 1;
            }
            if (fail) throw new InvalidOperationException($"flake {seq}");
            lock (strictGot) strictGot.Add(seq);
            return Task.CompletedTask;
        }, TestContext.Current.CancellationToken);
        var skipGot = new List<int>();
        var ck = await inst.Bus.ConsumeAsync(skipStream,
            new ConsumeOptions
            {
                Reference = "grp-rf-k",
                Retry = new RetryPolicy { MaxAttempts = 2 },
                ErrorStream = new ErrorStreamOptions { Stream = error },
            },
            ctx =>
            {
                var (_, seq) = Parse(ctx.Body);
                if (seq == 0) throw new InvalidOperationException("poison 0");
                lock (skipGot) skipGot.Add(seq);
                return Task.CompletedTask;
            }, TestContext.Current.CancellationToken);
        await WaitForAsync(() => Count(strictGot) == 3 && Count(skipGot) == 1, TimeSpan.FromSeconds(90), "coexist drain");
        await cs.DisposeAsync();
        await ck.DisposeAsync();
        Assert.Equal(new[] { 0, 1, 2 }, strictGot);
        Assert.Equal(new[] { 1 }, skipGot);
        Assert.Equal(1UL, await inst.Bus.GetConsumerOffsetAsync(skipStream, "grp-rf-k", 0, TestContext.Current.CancellationToken));
    }

    [Fact]
    public async Task Retry_blocked_and_lag_expose_a_stalled_partition()
    {
        var stream = Unique("retry-vis");
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
            if (measurement.Name is "streambus_partition_retry_blocked" or "streambus_partition_lag")
            {
                string? part = null;
                foreach (var tag in tags) if (tag.Key == "partition") part = tag.Value?.ToString();
                lock (observed) observed.Add((measurement.Name, value, part ?? "?"));
            }
        });
        listener.Start();
        var entered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var attempts = 0;
        var attemptsGate = new object();
        var got = new List<int>();
        var c = await inst.Bus.ConsumeAsync(stream, new ConsumeOptions { Reference = "grp-rv" }, async ctx =>
        {
            int n;
            lock (attemptsGate) { attempts++; n = attempts; }
            if (n == 1) throw new InvalidOperationException("first flake");
            if (n == 2)
            {
                // Hold the second attempt open until the test has observed the stall.
                // The context-token wait aborts the hold if disposal wins the race.
                entered.TrySetResult();
                await Task.WhenAny(release.Task, Task.Delay(Timeout.Infinite, ctx.CancellationToken));
                ctx.CancellationToken.ThrowIfCancellationRequested();
            }
            lock (got) got.Add(0);
        }, TestContext.Current.CancellationToken);
        try
        {
            Assert.True(await Task.WhenAny(entered.Task, Task.Delay(TimeSpan.FromSeconds(30), TestContext.Current.CancellationToken)) == entered.Task, "retry entered");
            await WaitForAsync(() =>
            {
                listener.RecordObservableInstruments();
                lock (observed) return observed.Any(m => m.Name == "streambus_partition_retry_blocked" && m.Value == 1 && m.Partition == "0");
            }, TimeSpan.FromSeconds(30), "retry_blocked == 1");
            listener.RecordObservableInstruments();
            lock (observed) Assert.Contains(observed, m => m.Name == "streambus_partition_lag" && m.Value > 0 && m.Partition == "0");
            release.TrySetResult();
            await WaitForAsync(() => Count(got) == 1, TimeSpan.FromSeconds(60), "post-release success");
            await WaitForAsync(() =>
            {
                listener.RecordObservableInstruments();
                lock (observed) return observed.Any(m => m.Name == "streambus_partition_retry_blocked" && m.Value == 0);
            }, TimeSpan.FromSeconds(30), "retry_blocked back to 0");
            Assert.Equal(0UL, await inst.Bus.GetConsumerOffsetAsync(stream, "grp-rv", 0, TestContext.Current.CancellationToken));
        }
        finally { await c.DisposeAsync(); }
    }
}
