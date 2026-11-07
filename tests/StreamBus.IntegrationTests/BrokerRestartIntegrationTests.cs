using System.Globalization;
using System.Text;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Diagnostics.HealthChecks;
using Microsoft.Extensions.Hosting;
using RabbitMQ.Stream.Client;

namespace StreamBus.IntegrationTests;

[Collection(ChaosCollection.Name)]
public sealed class BrokerRestartTests : IAsyncDisposable
{
    private const int LoadMessages = 40;

    public BrokerRestartTests() => IntegrationLane.RequireBroker();

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

    private static bool IsRetryable(Exception ex)
        => ex is StreamBusPublishTransientException
            or StreamBusNotReadyException
            or StreamBusTimeoutException;

    private static async Task SampleHealthAsync(
        StreamBusHealthCheck check,
        List<HealthStatus> statuses,
        CancellationToken ct)
    {
        while (!ct.IsCancellationRequested)
        {
            var result = await check.CheckHealthAsync(new HealthCheckContext(), ct);
            lock (statuses) statuses.Add(result.Status);
            try { await Task.Delay(20, ct); }
            catch (OperationCanceledException) { return; }
        }
    }

    private static bool LastIsHealthy(List<HealthStatus> statuses)
    {
        lock (statuses) return statuses.Count > 0 && statuses[^1] == HealthStatus.Healthy;
    }

    [Fact]
    public async Task Connections_kill_does_not_skip_or_gap()
    {
        var ct = TestContext.Current.CancellationToken;
        var stream = Unique("chaos-kill");
        var reference = $"chaos-kill-{Guid.NewGuid():N}";
        var admin = await ConnectAdminAsync();
        await using var _ = admin;
        await admin.CreateStream(new StreamSpec(stream));
        _streams.Add(stream);
        using var management = await StreamManagementApi.ConnectAsync(ct);

        var received = new List<(int Seq, ulong Offset)>();
        await using var publisher = await BusInstance.StartAsync();
        await using var consumerBus = await BusInstance.StartAsync();
        await using var consumer = await consumerBus.Bus.ConsumeAsync(
            stream,
            new ConsumeOptions { Reference = reference },
            ctx =>
            {
                var seq = ParseSeq(ctx.Body);
                lock (received) received.Add((seq, ctx.Offset));
                return Task.CompletedTask;
            },
            ct);

        // Warm-up pins the subscription before the load starts.
        await publisher.Bus.PublishRawAsync(stream, Body(0), new PublishOptions(), ct);
        await WaitForAsync(() => Count(received) >= 1, TimeSpan.FromSeconds(30), "warm-up delivery");

        // Management stats lag by a few seconds; wait for the producer,
        // consumer and locator connections before the one-shot kill so the
        // snapshot is complete.
        await StreamManagementApi.WaitForAsync(
            () => management.GetStreamConnectionsAsync(ct),
            connections => connections.Count >= 4,
            TimeSpan.FromSeconds(30),
            "stream connections visible in the management API");

        var publisherCheck = publisher.Provider.GetServices<IHealthCheck>().OfType<StreamBusHealthCheck>().Single();
        var consumerCheck = consumerBus.Provider.GetServices<IHealthCheck>().OfType<StreamBusHealthCheck>().Single();
        var publisherStatuses = new List<HealthStatus>();
        var consumerStatuses = new List<HealthStatus>();
        using var samplingCts = CancellationTokenSource.CreateLinkedTokenSource(ct);
        var sampling = Task.WhenAll(
            SampleHealthAsync(publisherCheck, publisherStatuses, samplingCts.Token),
            SampleHealthAsync(consumerCheck, consumerStatuses, samplingCts.Token));

        var publishErrors = new List<Exception>();
        var published = 0;
        try
        {
            var load = Task.Run(async () =>
            {
                for (var seq = 1; seq <= LoadMessages; seq++)
                {
                    var deadline = DateTime.UtcNow + TimeSpan.FromSeconds(45);
                    while (true)
                    {
                        try
                        {
                            await publisher.Bus.PublishRawAsync(
                                stream,
                                Body(seq),
                                new PublishOptions { Timeout = TimeSpan.FromSeconds(2) },
                                ct);
                            break;
                        }
                        catch (Exception ex) when (IsRetryable(ex))
                        {
                            lock (publishErrors) publishErrors.Add(ex);
                            if (DateTime.UtcNow >= deadline)
                            {
                                throw new TimeoutException(
                                    $"Publishing sequence {seq} did not recover within 45s.", ex);
                            }
                            await Task.Delay(100, ct);
                        }
                    }
                    Interlocked.Increment(ref published);
                    await Task.Delay(200, ct);
                }
            }, ct);

            await WaitForAsync(() => Volatile.Read(ref published) >= 15, TimeSpan.FromSeconds(20), "load mid-run");
            var closed = await management.CloseAllStreamConnectionsAsync(ct);
            Assert.True(closed >= 1, "Expected the management API to close at least one stream connection.");

            await load;
            try
            {
                await WaitForAsync(
                    () => Count(received) >= LoadMessages + 1,
                    TimeSpan.FromSeconds(60),
                    "all messages consumed after the connection kill");
            }
            catch (Exception ex)
            {
                var publisherHealth = await publisherCheck.CheckHealthAsync(new HealthCheckContext(), ct);
                var consumerHealth = await consumerCheck.CheckHealthAsync(new HealthCheckContext(), ct);
                var consumers = await management.GetStreamConsumersAsync(ct);
                var connections = await management.GetStreamConnectionsAsync(ct);
                var rows = string.Join("; ", consumers
                    .Where(c => c.Reference == reference)
                    .Select(c => $"{c.Queue} active={c.Active} conn={c.ConnectionName ?? "<none>"}"));
                var errorCount = publishErrors.Count;
                Assert.Fail(
                    $"{ex.Message} received={Count(received)}/{LoadMessages + 1} " +
                    $"publisherState={publisher.Bus.State} consumerState={consumerBus.Bus.State} " +
                    $"publisherHealth={publisherHealth.Status} consumerHealth={consumerHealth.Status} " +
                    $"publishErrors={errorCount} streamConnections={connections.Count} " +
                    $"consumerRows=[{rows}]");
            }

            await WaitForAsync(
                () => LastIsHealthy(publisherStatuses) && LastIsHealthy(consumerStatuses),
                TimeSpan.FromSeconds(30),
                "publisher and consumer health to return to Healthy");
        }
        finally
        {
            await samplingCts.CancelAsync();
            try { await sampling; }
            catch (OperationCanceledException) { }
        }

        (int Seq, ulong Offset)[] snapshot;
        lock (received) snapshot = received.ToArray();
        var sequences = snapshot.Select(x => x.Seq).Distinct().OrderBy(x => x).ToArray();
        Assert.Equal(Enumerable.Range(0, LoadMessages + 1), sequences);

        // Offsets are not numerically contiguous under live consumption: broker
        // offset-tracking writes occupy stream positions between delivered
        // messages (observed empirically as 0,2,4... with every-message stores).
        // A "gap" therefore means a missing sequence, not an adjacent integer.
        var offsets = snapshot.Select(x => x.Offset).Distinct().OrderBy(x => x).ToArray();
        Assert.NotEmpty(offsets);
        for (var i = 1; i < snapshot.Length; i++)
            Assert.True(snapshot[i].Offset >= snapshot[i - 1].Offset, "delivery order must follow offset order");

        var stored = await StreamManagementApi.WaitForAsync(
            () => admin.TryQueryOffset(reference, stream),
            offset => offset == offsets[^1],
            TimeSpan.FromSeconds(30),
            "stored offset to reach the last delivered position");
        Assert.Equal(offsets[^1], stored);

        Exception[] errors;
        lock (publishErrors) errors = publishErrors.ToArray();
        Assert.NotEmpty(errors);
        Assert.All(errors, ex => Assert.True(
            IsRetryable(ex),
            $"Publish failure was not classified transient: {ex.GetType().Name}: {ex.Message}"));

        HealthStatus[] publisherObserved;
        HealthStatus[] consumerObserved;
        lock (publisherStatuses) publisherObserved = publisherStatuses.ToArray();
        lock (consumerStatuses) consumerObserved = consumerStatuses.ToArray();

        // The client's StreamSystem transparently reconnects its locator after a
        // connection-only kill, so the publisher supervisor often stays Ready and
        // its health is not a stable outage signal. The consumer aggregate is:
        // with one consumer, Task 5's all-consumers-failed rule reports Unhealthy
        // (Degraded is the partial-failure state), then Healthy once reattached.
        Assert.Equal(HealthStatus.Healthy, publisherObserved[^1]);
        Assert.Contains(consumerObserved, status => status != HealthStatus.Healthy);
        Assert.Equal(HealthStatus.Healthy, consumerObserved[^1]);
    }
}
