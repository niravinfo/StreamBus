using System.Buffers;
using System.Text;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using RabbitMQ.Stream.Client;
using StreamBus;

namespace StreamBus.IntegrationTests;

public sealed class PublishIntegrationTests : IAsyncDisposable
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
        }).ConfigureAwait(false);

    private static string Unique(string prefix) => $"{prefix}-{Guid.NewGuid():N}";

    private static byte[] Body(RabbitMQ.Stream.Client.Message message)
        => message.Data.Contents.ToArray();

    public async ValueTask DisposeAsync()
    {
        var admin = await ConnectAdminAsync().ConfigureAwait(false);
        try
        {
            foreach (var s in _streams) { try { await admin.DeleteStream(s).ConfigureAwait(false); } catch { } }
            foreach (var s in _superStreams) { try { await admin.DeleteSuperStream(s).ConfigureAwait(false); } catch { } }
        }
        finally { await admin.DisposeAsync().ConfigureAwait(false); }
    }

    [Fact]
    public async Task Plain_round_trip_delivers_ordered_offsets()
    {
        var stream = Unique("pub-plain");
        await using var admin = await ConnectAdminAsync();
        await admin.CreateStream(new StreamSpec(stream));
        _streams.Add(stream);
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddStreamBus((_, cfg) => cfg.Host("127.0.0.1", "/", h => h.Credentials(User(), Password())));
        await using var sp = services.BuildServiceProvider();
        var hosted = sp.GetServices<IHostedService>().OfType<StreamBusHostedService>().Single();
        await hosted.StartAsync(CancellationToken.None);
        try
        {
            var bus = sp.GetRequiredService<IStreamBus>();
            Assert.Equal(BusReadiness.Ready, await bus.WaitUntilReadyAsync(ReadinessPolicy.UpTo(TimeSpan.FromSeconds(30)), TestContext.Current.CancellationToken));
            for (byte i = 0; i < 3; i++)
                await bus.PublishRawAsync(stream, new ReadOnlyMemory<byte>(new[] { i }), new PublishOptions(), TestContext.Current.CancellationToken);
            var received = new List<byte>();
            var done = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            var consumer = await admin.CreateRawConsumer(new RawConsumerConfig(stream)
            {
                Reference = $"probe-{Guid.NewGuid():N}",
                OffsetSpec = new OffsetTypeFirst(),
                MessageHandler = (_, _, message) =>
                {
                    var first = Body(message)[0];
                    lock (received)
                    {
                        received.Add(first);
                        if (received.Count == 3) done.TrySetResult();
                    }
                    return Task.CompletedTask;
                },
            });
            await done.Task.WaitAsync(TimeSpan.FromSeconds(30), TestContext.Current.CancellationToken);
            consumer.Dispose();
            Assert.Equal(new byte[] { 0, 1, 2 }, received.ToArray());
        }
        finally { await hosted.StopAsync(CancellationToken.None); }
    }

    [Fact]
    public async Task Missing_stream_fails_and_stays_absent()
    {
        var stream = Unique("pub-missing");
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddStreamBus((_, cfg) => cfg.Host("127.0.0.1", "/", h => h.Credentials(User(), Password())));
        await using var sp = services.BuildServiceProvider();
        var hosted = sp.GetServices<IHostedService>().OfType<StreamBusHostedService>().Single();
        await hosted.StartAsync(CancellationToken.None);
        try
        {
            var bus = sp.GetRequiredService<IStreamBus>();
            Assert.Equal(BusReadiness.Ready, await bus.WaitUntilReadyAsync(ReadinessPolicy.UpTo(TimeSpan.FromSeconds(30)), TestContext.Current.CancellationToken));
            var ex = await Assert.ThrowsAsync<StreamBusStreamNotFoundException>(() =>
                bus.PublishRawAsync(stream, new ReadOnlyMemory<byte>(new byte[] { 9 }), new PublishOptions(), TestContext.Current.CancellationToken));
            Assert.Equal(stream, ex.Stream);
            await using var admin = await ConnectAdminAsync();
            Assert.False(await admin.StreamExists(stream));
        }
        finally { await hosted.StopAsync(CancellationToken.None); }
    }

    [Fact]
    public async Task Super_stream_keys_land_on_stable_partitions()
    {
        var super = Unique("pub-super");
        await using var admin = await ConnectAdminAsync();
        await admin.CreateSuperStream(new PartitionsSuperStreamSpec(super, 3));
        _superStreams.Add(super);
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddStreamBus((_, cfg) => cfg.Host("127.0.0.1", "/", h => h.Credentials(User(), Password())));
        await using var sp = services.BuildServiceProvider();
        var hosted = sp.GetServices<IHostedService>().OfType<StreamBusHostedService>().Single();
        await hosted.StartAsync(CancellationToken.None);
        try
        {
            var bus = sp.GetRequiredService<IStreamBus>();
            Assert.Equal(BusReadiness.Ready, await bus.WaitUntilReadyAsync(ReadinessPolicy.UpTo(TimeSpan.FromSeconds(30)), TestContext.Current.CancellationToken));
            foreach (var key in new[] { "alpha", "beta", "alpha", "gamma", "beta", "alpha" })
                await bus.PublishRawAsync(super,
                    new ReadOnlyMemory<byte>(Encoding.UTF8.GetBytes(key)),
                    new PublishOptions { IsSuperStream = true, RoutingKey = key },
                    TestContext.Current.CancellationToken);
            var partitions = await admin.QueryPartition(super);
            Assert.Equal(3, partitions.Length);
            var perPartition = partitions.ToDictionary(p => p, _ => new List<string>());
            var total = 0;
            var done = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            var closers = new List<Func<Task>>();
            foreach (var partition in partitions)
            {
                var list = perPartition[partition];
                var consumer = await admin.CreateRawConsumer(new RawConsumerConfig(partition)
                {
                    Reference = $"probe-{Guid.NewGuid():N}",
                    OffsetSpec = new OffsetTypeFirst(),
                    MessageHandler = (_, _, message) =>
                    {
                        var text = Encoding.UTF8.GetString(Body(message));
                        lock (list) list.Add(text);
                        if (Interlocked.Increment(ref total) == 6) done.TrySetResult();
                        return Task.CompletedTask;
                    },
                });
                closers.Add(() => { consumer.Dispose(); return Task.CompletedTask; });
            }
            var winner = await Task.WhenAny(done.Task, Task.Delay(TimeSpan.FromSeconds(30), TestContext.Current.CancellationToken));
            Assert.True(winner == done.Task, "Timed out waiting for all 6 super-stream messages.");
            foreach (var close in closers)
                await close();
            var all = perPartition.Values.SelectMany(v => v).OrderBy(x => x).ToList();
            Assert.Equal(6, all.Count);
            foreach (var key in new[] { "alpha", "beta", "gamma" })
                Assert.Single(perPartition.Values, v => v.Contains(key));
        }
        finally { await hosted.StopAsync(CancellationToken.None); }
    }
}
