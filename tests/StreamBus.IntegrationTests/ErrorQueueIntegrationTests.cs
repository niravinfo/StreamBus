using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using RabbitMQ.Client;
using RabbitMQ.Stream.Client;

namespace StreamBus.IntegrationTests;

public sealed class ErrorQueueIntegrationTests : IAsyncDisposable
{
    public ErrorQueueIntegrationTests() => IntegrationLane.RequireBroker();

    private static string User() => Environment.GetEnvironmentVariable("STREAMBUS_USER") ?? "app";
    private static string Password() => Environment.GetEnvironmentVariable("STREAMBUS_PASSWORD") ?? "password";
    private readonly List<string> _streams = new();
    private readonly List<string> _errorQueues = new();

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
        if (_errorQueues.Count > 0)
        {
            try
            {
                var factory = new ConnectionFactory
                {
                    HostName = "localhost",
                    Port = 5672,
                    UserName = User(),
                    Password = Password(),
                    VirtualHost = "/",
                };
                using var connection = await factory.CreateConnectionAsync(TestContext.Current.CancellationToken);
                await using var channel = await connection.CreateChannelAsync(new CreateChannelOptions(false, false, null, null), TestContext.Current.CancellationToken);
                foreach (var q in _errorQueues) { try { await channel.QueueDeleteAsync(q, false, false, TestContext.Current.CancellationToken); } catch { } }
            }
            catch { }
        }
    }

    private sealed record OrderEvent(string OrderId, int Sequence);

    private sealed class FailingConsumer : IConsumer<OrderEvent>
    {
        public Task Consume(ConsumeContext<OrderEvent> context)
            => Task.FromException(new InvalidOperationException($"poison {context.Message.Sequence}"));
    }

    private sealed class BusInstance : IAsyncDisposable
    {
        public ServiceProvider Provider { get; private set; } = null!;
        public StreamBusHostedService Hosted { get; private set; } = null!;
        public IStreamBus Bus { get; private set; } = null!;
        public static async Task<BusInstance> StartAsync(Action<ServiceCollection>? configure = null, string vhost = "/", Action<IStreamBusConfigurator>? busConfigure = null, TimeSpan? blockTimeout = null)
        {
            var services = new ServiceCollection();
            services.AddLogging();
            services.AddStreamBus((_, cfg) =>
            {
                cfg.Host("localhost", vhost, h => h.Credentials(User(), Password()));
                busConfigure?.Invoke(cfg);
            });
            if (blockTimeout is { } timeout)
                services.AddStreamBusHosting(h => h.BlockStartupUntilReady(timeout));
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

    private static ReadOnlyMemory<byte> Body(OrderEvent message) => SystemTextJsonStreamSerializer.Default.Serialize(message);

    private static async Task WaitForAsync(Func<bool> done, TimeSpan timeout, string what)
    {
        var deadline = DateTime.UtcNow + timeout;
        while (!done())
        {
            if (DateTime.UtcNow >= deadline) Assert.Fail($"Timed out waiting for {what}.");
            await Task.Delay(200, TestContext.Current.CancellationToken);
        }
    }

    private static async Task<(byte[] Body, Dictionary<string, object?> Headers)> PeekQueueAsync(string queue)
    {
        var ct = TestContext.Current.CancellationToken;
        var factory = new ConnectionFactory
        {
            HostName = "localhost",
            Port = 5672,
            UserName = User(),
            Password = Password(),
            VirtualHost = "/",
        };
        using var connection = await factory.CreateConnectionAsync(ct);
        await using var channel = await connection.CreateChannelAsync(new CreateChannelOptions(false, false, null, null), ct);
        var deadline = DateTime.UtcNow + TimeSpan.FromSeconds(60);
        while (true)
        {
            var delivery = await channel.BasicGetAsync(queue, autoAck: false, cancellationToken: ct);
            if (delivery is not null)
            {
                var headers = delivery.BasicProperties?.Headers is { } raw
                    ? new Dictionary<string, object?>(raw, StringComparer.Ordinal)
                    : new Dictionary<string, object?>(StringComparer.Ordinal);
                return (delivery.Body.ToArray(), headers);
            }
            if (DateTime.UtcNow >= deadline) Assert.Fail($"Timed out waiting for a message in error queue '{queue}'.");
            await Task.Delay(200, ct);
        }
    }

    private async Task RunErrorQueueCaseAsync(bool explicitQueue)
    {
        var ct = TestContext.Current.CancellationToken;
        var stream = Unique("eq-orders");
        var errorQueue = explicitQueue ? Unique("orders-errors") : stream + "_error";
        _streams.Add(stream);
        _errorQueues.Add(errorQueue);
        var poison = new OrderEvent("o-1", 1);
        var messageId = Guid.NewGuid();
        var sentTime = new DateTime(2026, 10, 31, 9, 12, 3, 284, DateTimeKind.Utc);

        await using var inst = await BusInstance.StartAsync(
            null,
            "/",
            cfg =>
            {
                cfg.Publish<OrderEvent>(stream);
                cfg.ReceiveEndpoint(stream, e =>
                {
                    e.Consumer<FailingConsumer>("eq-group");
                    e.UseMessageRetry(r => r.None());
                    if (explicitQueue)
                        e.DeadLetterToQueue(errorQueue, DeadLetterQueueType.ClassicQueue);
                });
            },
            TimeSpan.FromSeconds(60));
        await using var pub = await BusInstance.StartAsync();
        await pub.Bus.PublishRawAsync(stream, Body(poison), new PublishOptions
        {
            Properties = new StreamMessageProperties { MessageId = messageId, SentTime = sentTime },
        }, ct);

        var (queuedBody, queuedHeaders) = await PeekQueueAsync(errorQueue);
        Assert.Equal(Body(poison).ToArray(), queuedBody);
        Assert.True(ErrorEnvelope.TryParseHeaders(errorQueue, queuedBody, queuedHeaders, out var envelope));
        Assert.NotNull(envelope);
        Assert.Equal(stream, envelope.SourceStream);
        Assert.Equal(0, envelope.SourcePartition);
        Assert.Equal(0UL, envelope.SourceOffset);
        Assert.Equal(1, envelope.Attempts);
        Assert.Contains("InvalidOperationException", envelope.ExceptionType, StringComparison.Ordinal);

        await inst.DisposeAsync();
        var requeued = await pub.Bus.RequeueErrorsAsync(stream, errorQueue, ct);
        Assert.Equal(1, requeued);

        var seen = new List<ConsumeContext>();
        await using var reader = await pub.Bus.ConsumeAsync(stream,
            new ConsumeOptions { Offset = ConsumeOffset.AlwaysFirst, Reference = Unique("read") },
            ctx => { lock (seen) seen.Add(ctx); return Task.CompletedTask; }, ct);
        await WaitForAsync(() => { lock (seen) return seen.Count >= 2; }, TimeSpan.FromSeconds(30), "redelivered copy on source stream");
        Assert.All(seen, context =>
        {
            Assert.Equal(Body(poison).ToArray(), context.Body.ToArray());
            Assert.Equal(messageId, context.MessageId);
            Assert.Equal(sentTime, context.SentTime);
        });
    }

    [Fact]
    public async Task Explicit_classic_queue_handoff_and_requeue() => await RunErrorQueueCaseAsync(explicitQueue: true);

    [Fact]
    public async Task Derived_default_queue_handoff_and_requeue() => await RunErrorQueueCaseAsync(explicitQueue: false);
}
