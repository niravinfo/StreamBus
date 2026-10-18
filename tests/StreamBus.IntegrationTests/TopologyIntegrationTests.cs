using System.Net;
using System.Net.Http.Headers;
using System.Text;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using RabbitMQ.Stream.Client;

namespace StreamBus.IntegrationTests;

public sealed class TopologyIntegrationTests : IAsyncDisposable
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
        public static async Task<BusInstance> StartAsync(string vhost = "/", Action<IStreamRegistrationContext, IStreamBusConfigurator>? configure = null)
        {
            var services = new ServiceCollection();
            services.AddLogging();
            services.AddStreamBus((ctx, cfg) => { cfg.Host("localhost", vhost, h => h.Credentials(User(), Password())); configure?.Invoke(ctx, cfg); });
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
    public async Task Repeat_and_concurrent_declares_converge()
    {
        var ct = TestContext.Current.CancellationToken;
        var plain = Unique("topo-plain");
        var super = Unique("topo-super");
        _streams.Add(plain);
        _superStreams.Add(super);
        await using var inst = await BusInstance.StartAsync();
        await inst.Bus.Topology.EnsureStreamAsync(new StreamDeclaration { Name = plain }, ct);
        await inst.Bus.Topology.EnsureStreamAsync(new StreamDeclaration { Name = plain }, ct);
        var sdecl = new SuperStreamDeclaration { Name = super, Partitions = 3 };
        await Task.WhenAll(Enumerable.Range(0, 4).Select(_ => inst.Bus.Topology.EnsureSuperStreamAsync(sdecl, ct)));
        var admin = await ConnectAdminAsync();
        await using var _ = admin;
        Assert.True(await admin.StreamExists(plain));
        Assert.True(await admin.SuperStreamExists(super));
        Assert.Equal(3, (await admin.QueryPartition(super)).Length);
        await inst.Bus.PublishRawAsync(plain, Body("k:0"), new PublishOptions(), ct);
        await inst.Bus.PublishRawAsync(super, Body("k:0"), new PublishOptions { IsSuperStream = true, RoutingKey = "k" }, ct);
    }

    [Fact]
    public async Task Super_partition_conflict_names_expected_and_actual()
    {
        var ct = TestContext.Current.CancellationToken;
        var super = Unique("topo-super");
        _superStreams.Add(super);
        await using var inst = await BusInstance.StartAsync();
        await inst.Bus.Topology.EnsureSuperStreamAsync(new SuperStreamDeclaration { Name = super, Partitions = 3 }, ct);
        var ex = await Assert.ThrowsAsync<StreamConfigurationConflictException>(() =>
            inst.Bus.Topology.EnsureSuperStreamAsync(new SuperStreamDeclaration { Name = super, Partitions = 5 }, ct));
        Assert.Contains("5", ex.Message);
        Assert.Contains("3", ex.Message);
        Assert.Contains("5", ex.Expected);
        Assert.Contains("3", ex.Actual);
    }

    [Fact]
    public async Task Kind_mismatch_conflicts()
    {
        var ct = TestContext.Current.CancellationToken;
        var plain = Unique("topo-plain");
        var super = Unique("topo-super");
        _streams.Add(plain);
        _superStreams.Add(super);
        await using var inst = await BusInstance.StartAsync();
        await inst.Bus.Topology.EnsureStreamAsync(new StreamDeclaration { Name = plain }, ct);
        var ex = await Assert.ThrowsAsync<StreamConfigurationConflictException>(() =>
            inst.Bus.Topology.EnsureSuperStreamAsync(new SuperStreamDeclaration { Name = plain, Partitions = 2 }, ct));
        Assert.Contains("plain stream", ex.Actual);
        await inst.Bus.Topology.EnsureSuperStreamAsync(new SuperStreamDeclaration { Name = super, Partitions = 2 }, ct);
        var ex2 = await Assert.ThrowsAsync<StreamConfigurationConflictException>(() =>
            inst.Bus.Topology.EnsureStreamAsync(new StreamDeclaration { Name = super }, ct));
        Assert.Contains("2", ex2.Actual);
    }

    [Fact]
    public async Task Restart_with_identical_spec_succeeds()
    {
        var ct = TestContext.Current.CancellationToken;
        var plain = Unique("topo-plain");
        var super = Unique("topo-super");
        _streams.Add(plain);
        _superStreams.Add(super);
        await using var first = await BusInstance.StartAsync();
        await first.Bus.Topology.EnsureStreamAsync(new StreamDeclaration { Name = plain, MaxAge = TimeSpan.FromDays(7) }, ct);
        await first.Bus.Topology.EnsureSuperStreamAsync(new SuperStreamDeclaration { Name = super, Partitions = 2 }, ct);
        await first.DisposeAsync();
        await using var second = await BusInstance.StartAsync();
        await second.Bus.Topology.EnsureStreamAsync(new StreamDeclaration { Name = plain, MaxAge = TimeSpan.FromDays(7) }, ct);
        await second.Bus.Topology.EnsureSuperStreamAsync(new SuperStreamDeclaration { Name = super, Partitions = 2 }, ct);
        var admin = await ConnectAdminAsync();
        await using var _ = admin;
        Assert.True(await admin.StreamExists(plain));
        Assert.Equal(2, (await admin.QueryPartition(super)).Length);
    }

    [Fact]
    public async Task Plain_knob_redclare_succeeds_with_warning()
    {
        // Plain-stream argument compatibility is not readable via the stream
        // protocol (Review Focus 5): identical-name knob differences converge
        // instead of throwing. Keep declarations in sync across processes.
        var ct = TestContext.Current.CancellationToken;
        var plain = Unique("topo-plain");
        _streams.Add(plain);
        await using var inst = await BusInstance.StartAsync();
        await inst.Bus.Topology.EnsureStreamAsync(new StreamDeclaration { Name = plain }, ct);
        await inst.Bus.Topology.EnsureStreamAsync(new StreamDeclaration { Name = plain, MaxAge = TimeSpan.FromDays(7) }, ct);
        var admin = await ConnectAdminAsync();
        await using var _ = admin;
        Assert.True(await admin.StreamExists(plain));
    }

    [Fact]
    public async Task Nonblocking_startup_declares_in_background()
    {
        var ct = TestContext.Current.CancellationToken;
        var plain = Unique("topo-plain");
        var super = Unique("topo-super");
        _streams.Add(plain);
        _superStreams.Add(super);
        await using var inst = await BusInstance.StartAsync("/", (_, cfg) =>
        {
            cfg.Publish<TopologyPlainEvent>(plain);
            cfg.PublishSuper<TopologySuperEvent>(super, s => s.Partitions(2));
        });
        var admin = await ConnectAdminAsync();
        await using var _ = admin;
        await WaitForAsync(() => admin.StreamExists(plain).GetAwaiter().GetResult()
            && admin.SuperStreamExists(super).GetAwaiter().GetResult(), TimeSpan.FromSeconds(30), "startup declaration");
        Assert.Equal(2, (await admin.QueryPartition(super)).Length);
    }

    [Fact]
    public async Task Blocking_startup_declares_before_return()
    {
        var ct = TestContext.Current.CancellationToken;
        var plain = Unique("topo-plain");
        var super = Unique("topo-super");
        _streams.Add(plain);
        _superStreams.Add(super);
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddStreamBus((_, cfg) =>
        {
            cfg.Host("localhost", "/", h => h.Credentials(User(), Password()));
            cfg.Publish<TopologyPlainEvent>(plain);
            cfg.PublishSuper<TopologySuperEvent>(super, s => s.Partitions(2));
        });
        services.AddStreamBusHosting(h => h.BlockStartupUntilReady(TimeSpan.FromSeconds(30)));
        await using var sp = services.BuildServiceProvider();
        var hosted = sp.GetServices<IHostedService>().OfType<StreamBusHostedService>().Single();
        await hosted.StartAsync(ct);
        try
        {
            var admin = await ConnectAdminAsync();
            await using var _ = admin;
            Assert.True(await admin.StreamExists(plain));
            Assert.Equal(2, (await admin.QueryPartition(super)).Length);
        }
        finally
        {
            await hosted.StopAsync(ct);
        }
    }

    [Fact]
    public async Task Blocking_startup_with_conflict_fails_loudly()
    {
        var ct = TestContext.Current.CancellationToken;
        var super = Unique("topo-super");
        _superStreams.Add(super);
        var admin = await ConnectAdminAsync();
        await using var _ = admin;
        await admin.CreateSuperStream(new PartitionsSuperStreamSpec(super, 3));
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddStreamBus((_, cfg) =>
        {
            cfg.Host("localhost", "/", h => h.Credentials(User(), Password()));
            cfg.PublishSuper<TopologySuperEvent>(super, s => s.Partitions(5));
        });
        services.AddStreamBusHosting(h => h.BlockStartupUntilReady(TimeSpan.FromSeconds(30)));
        await using var sp = services.BuildServiceProvider();
        var hosted = sp.GetServices<IHostedService>().OfType<StreamBusHostedService>().Single();
        var ex = await Assert.ThrowsAsync<StreamConfigurationConflictException>(() => hosted.StartAsync(ct));
        Assert.Contains("5", ex.Message);
        Assert.Contains("3", ex.Message);
        await hosted.StopAsync(ct);
    }

    [Fact]
    public async Task Retention_declaration_is_usable()
    {
        // Effective retention is not readable via the stream protocol; this pins
        // that a retention-carrying declaration succeeds and the stream works.
        var ct = TestContext.Current.CancellationToken;
        var error = Unique("topo-error");
        _streams.Add(error);
        await using var inst = await BusInstance.StartAsync();
        await inst.Bus.Topology.EnsureStreamAsync(new StreamDeclaration { Name = error, MaxAge = TimeSpan.FromDays(7) }, ct);
        await inst.Bus.PublishRawAsync(error, Body("k:0"), new PublishOptions(), ct);
        var got = new List<string>();
        await using var c = await inst.Bus.ConsumeAsync(error,
            new ConsumeOptions { Offset = ConsumeOffset.AlwaysFirst, Reference = Unique("read") },
            ctx => { lock (got) got.Add(Encoding.UTF8.GetString(ctx.Body.Span)); return Task.CompletedTask; }, ct);
        await WaitForAsync(() => { lock (got) return got.Count >= 1; }, TimeSpan.FromSeconds(30), "retained message");
    }

    private static async Task<bool> ManagementReachableAsync()
    {
        try
        {
            using var http = new HttpClient { Timeout = TimeSpan.FromSeconds(5) };
            http.DefaultRequestHeaders.Authorization = new AuthenticationHeaderValue("Basic",
                Convert.ToBase64String(Encoding.UTF8.GetBytes($"{User()}:{Password()}")));
            using var resp = await http.GetAsync("http://127.0.0.1:15672/api/overview", TestContext.Current.CancellationToken);
            return resp.IsSuccessStatusCode;
        }
        catch
        {
            return false;
        }
    }

    [Fact]
    public async Task Policy_applied_tuning_is_effective()
    {
        // FR-7.4 operator path: production tuning belongs in policies, not queue
        // arguments. Runs only where the management plugin is reachable.
        if (!await ManagementReachableAsync()) Assert.Skip("management API not usable with stream credentials (unreachable or unauthorized); FR-7.4 operator path not exercised here.");
        var ct = TestContext.Current.CancellationToken;
        var stream = Unique("topo-pol");
        _streams.Add(stream);
        await using var inst = await BusInstance.StartAsync();
        await inst.Bus.Topology.EnsureStreamAsync(new StreamDeclaration { Name = stream }, ct);
        var policy = Unique("pol");
        using var http = new HttpClient { Timeout = TimeSpan.FromSeconds(10) };
        http.DefaultRequestHeaders.Authorization = new AuthenticationHeaderValue("Basic",
            Convert.ToBase64String(Encoding.UTF8.GetBytes($"{User()}:{Password()}")));
        var put = await http.PutAsync(
            $"http://127.0.0.1:15672/api/policies/%2F/{policy}",
            new StringContent(
                $"{{\"pattern\":\"^{stream}$\",\"definition\":{{\"max-age\":3600}},\"priority\":0}}",
                Encoding.UTF8, "application/json"), ct);
        Assert.True(put.IsSuccessStatusCode, $"PUT policy failed: {(int)put.StatusCode}");
        try
        {
            using var get = await http.GetAsync($"http://127.0.0.1:15672/api/queues/%2F/{stream}", ct);
            var payload = await get.Content.ReadAsStringAsync(ct);
            Assert.Contains("max-age", payload);
        }
        finally
        {
            await http.DeleteAsync($"http://127.0.0.1:15672/api/policies/%2F/{policy}", ct);
        }
    }

    [Fact]
    public async Task Leader_placement_gate_G3()
    {
        // G-3: leader-locator and replica placement need a 3-node cluster. This
        // test proves the gate is WIRED (declaration + StreamInfo observation) and
        // skips on a single node instead of asserting unmeasured behavior.
        var ct = TestContext.Current.CancellationToken;
        var super = Unique("topo-super");
        _superStreams.Add(super);
        await using var inst = await BusInstance.StartAsync();
        await inst.Bus.Topology.EnsureSuperStreamAsync(new SuperStreamDeclaration { Name = super, Partitions = 3 }, ct);
        var admin = await ConnectAdminAsync();
        await using var _ = admin;
        var info = await admin.StreamInfo(super);
        // Single-node brokers report replicas=1 here (Task 1 probe) — placement is
        // only observable with real replicas, so gate on > 1, not > 0.
        if (info.Replicas.Count <= 1) Assert.Skip("fewer than 2 replicas; G-3 requires a 3-node cluster.");
        // NB: client 1.12.1 models Leader as a Broker struct, so NotNull does
        // not apply (xUnit2002) — assert the leader endpoint is populated.
        Assert.False(string.IsNullOrEmpty(info.Leader.Host));
    }

    private sealed record TopologyPlainEvent;

    private sealed record TopologySuperEvent;
}
