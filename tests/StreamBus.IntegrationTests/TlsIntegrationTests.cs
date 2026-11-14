using System.Net.Security;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using RabbitMQ.Client;
using RabbitMQ.Stream.Client;

namespace StreamBus.IntegrationTests;

/// <summary>
/// One end-to-end pass against a real RabbitMQ with TLS listeners: a typed
/// stream round trip (stream protocol over TLS) plus an error-queue handoff
/// (AMQP 0-9-1 over TLS), proving both transports' TLS wiring against a broker.
/// </summary>
public sealed class TlsIntegrationTests : IAsyncDisposable
{
    public TlsIntegrationTests() => IntegrationLane.RequireBroker();

    private readonly List<string> _streams = new();
    private readonly List<string> _errorQueues = new();

    private static async Task<TlsBroker.TlsBrokerHandle> BrokerAsync()
        => await TlsBroker.StartAsync(TestContext.Current.CancellationToken);

    private static async Task<StreamSystem> ConnectAdminAsync()
    {
        var handle = await BrokerAsync();
        return await StreamSystem.Create(new StreamSystemConfig
        {
            UserName = handle.Username,
            Password = handle.Password,
            VirtualHost = "/",
            Endpoints = new List<System.Net.EndPoint> { new System.Net.IPEndPoint(System.Net.IPAddress.Parse(handle.Host), handle.StreamTlsPort) },
            Ssl = new RabbitMQ.Stream.Client.SslOption
            {
                Enabled = true,
                ServerName = "localhost",
                AcceptablePolicyErrors = SslPolicyErrors.RemoteCertificateChainErrors,
            },
        });
    }

    private static string Unique(string prefix) => $"{prefix}-{Guid.NewGuid():N}";

    public async ValueTask DisposeAsync()
    {
        var ct = TestContext.Current.CancellationToken;
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
                var handle = await BrokerAsync();
                var factory = new ConnectionFactory
                {
                    HostName = handle.Host,
                    Port = handle.AmqpTlsPort,
                    UserName = handle.Username,
                    Password = handle.Password,
                    VirtualHost = "/",
                    Ssl = new RabbitMQ.Client.SslOption
                    {
                        Enabled = true,
                        ServerName = "localhost",
                        AcceptablePolicyErrors = SslPolicyErrors.RemoteCertificateChainErrors,
                    },
                };
                using var connection = await factory.CreateConnectionAsync(ct);
                await using var channel = await connection.CreateChannelAsync(new CreateChannelOptions(false, false, null, null), ct);
                foreach (var q in _errorQueues) { try { await channel.QueueDeleteAsync(q, false, false, ct); } catch { } }
            }
            catch { }
        }
    }

    private sealed record OrderEvent(string OrderId, int Sequence);

    private sealed class OrderConsumer : IConsumer<OrderEvent>
    {
        private readonly List<(ConsumeContext<OrderEvent> Ctx, byte[] Raw)> _got;
        public OrderConsumer(List<(ConsumeContext<OrderEvent>, byte[])> got) => _got = got;
        public Task Consume(ConsumeContext<OrderEvent> context)
        {
            lock (_got) _got.Add((context, context.Body.ToArray()));
            return Task.CompletedTask;
        }
    }

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
        public static async Task<BusInstance> StartAsync(Action<ServiceCollection>? configureServices = null, Action<IStreamBusConfigurator>? busConfigure = null)
        {
            var handle = await BrokerAsync();
            var services = new ServiceCollection();
            services.AddLogging();
            services.AddStreamBus((_, cfg) =>
            {
                cfg.Host(handle.Host, "/", h =>
                {
                    h.Credentials(handle.Username, handle.Password);
                    h.StreamPort(handle.StreamTlsPort);
                    h.AmqpPort(handle.AmqpTlsPort);
                    // The fixture's CA is a throwaway from the test temp dir, so
                    // only the untrusted-chain error is acceptable; the server
                    // certificate is minted for localhost, so name matching
                    // must succeed.
                    h.UseTls(t => t.AcceptableCertificateErrors(SslPolicyErrors.RemoteCertificateChainErrors));
                });
                busConfigure?.Invoke(cfg);
            });
            configureServices?.Invoke(services);
            services.AddOptions<StreamBusHostOptions>().Configure(o => { o.WaitUntilStarted = true; o.StartTimeout = TimeSpan.FromSeconds(60); });
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
        var handle = await BrokerAsync();
        var factory = new ConnectionFactory
        {
            HostName = handle.Host,
            Port = handle.AmqpTlsPort,
            UserName = handle.Username,
            Password = handle.Password,
            VirtualHost = "/",
            Ssl = new RabbitMQ.Client.SslOption
            {
                Enabled = true,
                ServerName = "localhost",
                AcceptablePolicyErrors = SslPolicyErrors.RemoteCertificateChainErrors,
            },
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

    [Fact]
    public async Task Stream_round_trip_and_error_queue_handoff_both_work_over_tls()
    {
        var ct = TestContext.Current.CancellationToken;
        var stream = Unique("tls-round-orders");
        var errorStream = Unique("tls-error-orders");
        var errorQueue = errorStream + "_error";
        _streams.Add(stream);
        _streams.Add(errorStream);
        _errorQueues.Add(errorQueue);
        var good = new OrderEvent("o-tls", 1);
        var poison = new OrderEvent("poison-tls", 1);

        await using var admin = await ConnectAdminAsync();
        await admin.CreateStream(new StreamSpec(stream));
        await admin.CreateStream(new StreamSpec(errorStream));

        var got = new List<(ConsumeContext<OrderEvent> Ctx, byte[] Raw)>();
        await using var inst = await BusInstance.StartAsync(
            s => s.AddSingleton(got),
            cfg =>
        {
            cfg.ReceiveEndpoint(stream, e => e.Consumer<OrderConsumer>("tls-round-group"));
            cfg.ReceiveEndpoint(errorStream, e =>
            {
                e.Consumer<FailingConsumer>("tls-error-group");
                e.UseRetry(r => r.None());
            });
        });

        // Stream transport over TLS: typed publish, typed consume.
        await inst.Bus.PublishAsync(stream, good, ct);
        await WaitForAsync(() => { lock (got) return got.Count >= 1; }, TimeSpan.FromSeconds(30), "TLS round-trip receipt");
        await Task.Delay(500, ct);
        lock (got)
        {
            Assert.Single(got);
            Assert.Equal(1, got[0].Ctx.Message.Sequence);
            Assert.Equal(stream, got[0].Ctx.Stream);
        }

        // Error transport over TLS: the FailingConsumer drives the poison
        // message onto the AMQP dead-letter queue via the bus's own wiring.
        await inst.Bus.PublishAsync(errorStream, poison, ct);
        var (queuedBody, queuedHeaders) = await PeekQueueAsync(errorQueue);
        Assert.Equal(Body(poison).ToArray(), queuedBody);
        Assert.True(ErrorEnvelope.TryParseHeaders(errorQueue, queuedBody, queuedHeaders, out var envelope));
        Assert.NotNull(envelope);
        Assert.Equal(errorStream, envelope.SourceStream);
        Assert.Contains("InvalidOperationException", envelope.ExceptionType, StringComparison.Ordinal);
    }
}
