using System.Globalization;
using System.Text;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using RabbitMQ.Stream.Client;

namespace StreamBus.IntegrationTests;

[Collection(ChaosCollection.Name)]
public sealed class ConsumerDeathTests : IAsyncDisposable
{
    public ConsumerDeathTests() => IntegrationLane.RequireBroker();

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
        public static async Task<BusInstance> StartAsync()
        {
            var services = new ServiceCollection();
            services.AddLogging();
            services.AddStreamBus((_, cfg) => cfg.Host("localhost", "/", h => h.Credentials(User(), Password())));
            var sp = services.BuildServiceProvider();
            var hosted = sp.GetServices<IHostedService>().OfType<StreamBusHostedService>().Single();
            await hosted.StartAsync(TestContext.Current.CancellationToken);
            var bus = sp.GetRequiredService<IStreamBus>();
            Assert.Equal(BusReadiness.Ready, await bus.WaitUntilReadyAsync(
                ReadinessPolicy.UpTo(TimeSpan.FromSeconds(30)), TestContext.Current.CancellationToken));
            return new BusInstance { Provider = sp, Hosted = hosted, Bus = bus };
        }
        public async ValueTask DisposeAsync()
        {
            try { await Hosted.StopAsync(TestContext.Current.CancellationToken); }
            finally { await Provider.DisposeAsync(); }
        }
    }

    private static ReadOnlyMemory<byte> Body(int seq)
        => Encoding.UTF8.GetBytes($"k:{seq.ToString(CultureInfo.InvariantCulture)}");

    private static int ParseSeq(ReadOnlyMemory<byte> body)
        => int.Parse(Encoding.UTF8.GetString(body.Span).Split(':')[1], CultureInfo.InvariantCulture);

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

    private static int PartitionOf(StreamConsumerInfo consumer)
    {
        var queue = consumer.Queue;
        var dash = queue.LastIndexOf('-');
        return dash < 0
            ? -1
            : int.Parse(queue[(dash + 1)..], CultureInfo.InvariantCulture);
    }

    [Fact]
    public async Task Connection_closed_while_idle_reattaches_within_bounds()
    {
        var ct = TestContext.Current.CancellationToken;
        var stream = Unique("chaos-idle");
        var reference = $"chaos-idle-{Guid.NewGuid():N}";
        var admin = await ConnectAdminAsync();
        await using var _ = admin;
        await admin.CreateStream(new StreamSpec(stream));
        _streams.Add(stream);
        using var management = await StreamManagementApi.ConnectAsync(ct);

        var received = new List<int>();
        await using var instance = await BusInstance.StartAsync();
        await using var consumer = await instance.Bus.ConsumeAsync(
            stream,
            new ConsumeOptions { Reference = reference },
            ctx =>
            {
                var seq = ParseSeq(ctx.Body);
                lock (received) received.Add(seq);
                return Task.CompletedTask;
            },
            ct);

        // Management stats lag the actual subscription by a few seconds. Wait
        // until the broker reports the connection before deleting it.
        var attached = await StreamManagementApi.WaitForAsync(
            () => management.GetStreamConsumersAsync(ct),
            consumers => consumers.Any(c => c.Queue == stream
                && c.Reference == reference
                && c.ConnectionName is not null),
            TimeSpan.FromSeconds(30),
            "idle consumer connection in the management API");
        var originalConnection = attached
            .First(c => c.Queue == stream && c.Reference == reference && c.ConnectionName is not null)
            .ConnectionName!;

        Assert.True(
            await management.CloseStreamConnectionAsync(originalConnection, ct),
            $"Expected to close stream connection '{originalConnection}'.");

        // Publish immediately after the forced close: the subscription was idle,
        // so all three messages land after the broker dropped its consumer.
        foreach (var seq in Enumerable.Range(0, 3))
            await instance.Bus.PublishRawAsync(stream, Body(seq), new PublishOptions(), ct);

        await WaitForAsync(() => Count(received) == 3, TimeSpan.FromSeconds(10), "three messages after forced close");
        int[] observed;
        lock (received) observed = received.ToArray();
        Assert.Equal(new[] { 0, 1, 2 }, observed);

        // GetConsumerOffsetAsync returns the last stored offset, not a count:
        // three messages at offsets 0..2 store offset 2.
        var stored = await StreamManagementApi.WaitForAsync(
            () => admin.TryQueryOffset(reference, stream),
            offset => offset == 2UL,
            TimeSpan.FromSeconds(10),
            "stored offset 2 after the redelivery");
        Assert.Equal(2UL, stored);

        var reattached = await StreamManagementApi.WaitForAsync(
            () => management.GetStreamConsumersAsync(ct),
            consumers => consumers.Any(c => c.Queue == stream
                && c.Reference == reference
                && c.ConnectionName is { } name
                && name != originalConnection),
            TimeSpan.FromSeconds(30),
            "consumer reattachment on a new connection");
        Assert.Contains(reattached, c => c.Queue == stream
            && c.Reference == reference
            && c.ConnectionName is { } name
            && name != originalConnection);

        Assert.Equal(2UL, await instance.Bus.GetConsumerOffsetAsync(stream, reference, 0, ct));
    }

    [Fact]
    public async Task Metadata_event_close_reattaches_and_promotes_correctly()
    {
        var ct = TestContext.Current.CancellationToken;
        var super = Unique("chaos-sac");
        var reference = $"chaos-sac-{Guid.NewGuid():N}";
        var admin = await ConnectAdminAsync();
        await using var _ = admin;
        await admin.CreateSuperStream(new PartitionsSuperStreamSpec(super, 2));
        _superStreams.Add(super);
        var partitionStreams = await admin.QueryPartition(super);
        var partitionZero = partitionStreams[0];
        using var management = await StreamManagementApi.ConnectAsync(ct);

        var all = new List<(string Instance, int Partition, int Seq)>();
        await using var first = await BusInstance.StartAsync();
        await using var second = await BusInstance.StartAsync();
        await first.Bus.ConsumeAsync(
            super,
            new ConsumeOptions { IsSuperStream = true, Reference = reference, SingleActiveConsumer = true },
            ctx =>
            {
                var seq = ParseSeq(ctx.Body);
                lock (all) all.Add(("first", ctx.Partition, seq));
                return Task.CompletedTask;
            },
            ct);

        var initial = await StreamManagementApi.WaitForAsync(
            () => management.GetStreamConsumersAsync(ct),
            consumers => consumers.Count(c => c.Reference == reference
                && c.Active
                && c.ConnectionName is not null) >= 2,
            TimeSpan.FromSeconds(30),
            "both SAC partitions active on the first instance");
        var firstConnections = initial
            .Where(c => c.Reference == reference && c.Active && c.ConnectionName is not null)
            .Select(c => c.ConnectionName!)
            .ToHashSet(StringComparer.Ordinal);
        var firstPartitionZero = initial
            .Single(c => c.Reference == reference && PartitionOf(c) == 0 && c.Active && c.ConnectionName is not null)
            .ConnectionName!;

        await second.Bus.ConsumeAsync(
            super,
            new ConsumeOptions { IsSuperStream = true, Reference = reference, SingleActiveConsumer = true },
            ctx =>
            {
                var seq = ParseSeq(ctx.Body);
                lock (all) all.Add(("second", ctx.Partition, seq));
                return Task.CompletedTask;
            },
            ct);

        // SAC keeps the second instance's subscriptions waiting on their own
        // connections. Capture partition 0's waiting connection before the kill.
        var waiting = await StreamManagementApi.WaitForAsync(
            () => management.GetStreamConsumersAsync(ct),
            consumers =>
            {
                var partition = consumers
                    .Where(c => c.Reference == reference && PartitionOf(c) == 0 && c.ConnectionName is not null)
                    .ToList();
                return partition.Any(c => c.Active && firstConnections.Contains(c.ConnectionName!))
                    && partition.Any(c => !c.Active && !firstConnections.Contains(c.ConnectionName!));
            },
            TimeSpan.FromSeconds(30),
            "second instance waiting on partition 0");
        var secondPartitionZero = waiting
            .First(c => c.Reference == reference
                && PartitionOf(c) == 0
                && !c.Active
                && c.ConnectionName is not null
                && !firstConnections.Contains(c.ConnectionName))
            .ConnectionName!;

        Assert.True(
            await management.CloseStreamConnectionAsync(firstPartitionZero, ct),
            $"Expected to close the active SAC connection '{firstPartitionZero}'.");

        await StreamManagementApi.WaitForAsync(
            () => management.GetStreamConsumersAsync(ct),
            consumers => consumers.Any(c => c.Reference == reference
                && PartitionOf(c) == 0
                && c.Active
                && c.ConnectionName == secondPartitionZero),
            TimeSpan.FromSeconds(30),
            "partition 0 promotion to the surviving instance");

        // Publish directly to the partition to remove routing ambiguity. Only
        // the promoted surviving instance may receive it.
        await second.Bus.PublishRawAsync(partitionZero, Body(7), new PublishOptions(), ct);
        await WaitForAsync(() => Count(all) >= 1, TimeSpan.FromSeconds(30), "post-promotion delivery");
        await Task.Delay(TimeSpan.FromSeconds(3), ct);

        (string Instance, int Partition, int Seq)[] observed;
        lock (all) observed = all.ToArray();
        var delivery = Assert.Single(observed);
        Assert.Equal("second", delivery.Instance);
        Assert.Equal(0, delivery.Partition);
        Assert.Equal(7, delivery.Seq);
    }
}
