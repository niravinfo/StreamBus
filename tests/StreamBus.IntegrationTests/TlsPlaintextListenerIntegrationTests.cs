using System.Diagnostics;
using System.Net.Security;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using RabbitMQ.Stream.Client;

namespace StreamBus.IntegrationTests;

/// <summary>
/// A bus configured with <c>UseTls</c> whose ports point at a broker's plain
/// TCP listeners: TLS can never succeed there, so the bus must surface a
/// classified Fatal error quickly instead of hanging or retrying forever.
/// </summary>
/// <remarks>
/// Uses the plain-TCP <see cref="TestBroker"/> fixture; no broker-side TLS
/// config is involved. Both transports are exercised: the stream-protocol
/// locator connect (via the full hosted bus) and the AMQP error-queue connect
/// (via the error-queue connection factory). Observed against RabbitMQ 4.2 /
/// RabbitMQ.Stream.Client + RabbitMQ.Client 7.2.2 the failure shapes are:
/// stream locator — a bare <c>SslException</c> ("The SSL handshake failed"),
/// fatal per <see cref="StreamBusErrorClassification"/>; AMQP —
/// <c>BrokerUnreachableException</c> wrapping <c>AuthenticationException</c>
/// ("Cannot determine the frame size or a corrupted frame was received"),
/// which <see cref="AmqpConnectionFactory"/> translates to a classified
/// <see cref="StreamBusFatalException"/>.
/// </remarks>
public sealed class TlsPlaintextListenerIntegrationTests
{
    public TlsPlaintextListenerIntegrationTests() => IntegrationLane.RequireBroker();

    private const SslPolicyErrors Acceptable = SslPolicyErrors.RemoteCertificateChainErrors
        | SslPolicyErrors.RemoteCertificateNameMismatch;

    private static async Task<TestBroker.BrokerHandle> BrokerAsync()
        => await TestBroker.StartAsync(TestContext.Current.CancellationToken);

    private static string Describe(Exception ex)
    {
        var chain = new List<string>();
        for (var current = ex; current is not null; current = current.InnerException)
            chain.Add($"{current.GetType().Name}: {current.Message}");
        return string.Join(" -> ", chain);
    }

    [Fact]
    public async Task Stream_locator_tls_against_plain_listener_is_classified_fatal()
    {
        var handle = await BrokerAsync();
        var config = new StreamSystemConfig
        {
            UserName = handle.Username,
            Password = handle.Password,
            VirtualHost = "/",
            Endpoints = new List<System.Net.EndPoint>
            {
                new System.Net.IPEndPoint(System.Net.IPAddress.Parse(handle.Host), handle.StreamPort),
            },
            Ssl = new SslOption
            {
                Enabled = true,
                ServerName = "localhost",
                AcceptablePolicyErrors = Acceptable,
            },
        };

        var clock = Stopwatch.StartNew();
        var ex = await Assert.ThrowsAnyAsync<Exception>(() => StreamSystem.Create(config));
        clock.Stop();

        TestContext.Current.TestOutputHelper!.WriteLine(
            $"stream locator exception after {clock.ElapsedMilliseconds} ms: {Describe(ex)}");
        Assert.True(StreamBusErrorClassification.IsFatal(ex),
            $"Stream-locator failure was NOT classified fatal: {Describe(ex)}");
        Assert.True(clock.Elapsed < TimeSpan.FromSeconds(10),
            $"Stream-locator TLS failure took {clock.Elapsed} to surface; expected a fast classified failure.");
    }

    [Fact]
    public async Task Amqp_error_queue_tls_against_plain_listener_is_classified_fatal()
    {
        var ct = TestContext.Current.CancellationToken;
        var handle = await BrokerAsync();
        var options = new StreamBusOptions
        {
            Vhost = "/",
            UserName = handle.Username,
            Password = handle.Password,
            Host = handle.Host,
            AmqpPort = handle.AmqpPort,
            Tls = new StreamBusTlsOptions { Enabled = true, AcceptableCertificateErrors = Acceptable },
        };
        var connector = new AmqpConnectionFactory();

        var clock = Stopwatch.StartNew();
        var ex = await Assert.ThrowsAsync<StreamBusFatalException>(() => connector.ConnectAsync(options, ct));
        clock.Stop();

        TestContext.Current.TestOutputHelper!.WriteLine($"amqp connect fatal after {clock.ElapsedMilliseconds} ms");
        Assert.Contains("TLS", ex.Message, StringComparison.Ordinal);
        Assert.True(clock.Elapsed < TimeSpan.FromSeconds(10),
            $"AMQP TLS failure took {clock.Elapsed} to surface; expected a fast classified failure.");
    }

    [Fact]
    public async Task Bus_with_tls_against_plain_listener_surfaces_fatal_quickly()
    {
        var ct = TestContext.Current.CancellationToken;
        var handle = await BrokerAsync();
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddStreamBus((_, cfg) => cfg.Host(handle.Host, "/", h =>
        {
            h.Credentials(handle.Username, handle.Password);
            h.StreamPort(handle.StreamPort);
            h.AmqpPort(handle.AmqpPort);
            h.UseTls(t => t.AcceptableCertificateErrors(Acceptable));
        }));
        await using var sp = services.BuildServiceProvider();
        var hosted = sp.GetServices<IHostedService>().OfType<StreamBusHostedService>().Single();
        await hosted.StartAsync(ct);
        try
        {
            var bus = sp.GetRequiredService<IStreamBus>();

            var clock = Stopwatch.StartNew();
            var readiness = await bus.WaitUntilReadyAsync(ReadinessPolicy.UpTo(TimeSpan.FromSeconds(30)), ct);
            clock.Stop();

            // The bus must terminate its readiness wait with the classified
            // Fatal verdict — not Ready, and not a 30s-budget burn ending in
            // TimedOut: the first connect attempt fails fast and fatally.
            Assert.Equal(BusReadiness.Fatal, readiness);
            Assert.True(clock.Elapsed < TimeSpan.FromSeconds(20),
                $"Fatal took {clock.Elapsed} to surface; expected a classified failure within seconds, not a budget burn.");
        }
        finally
        {
            await hosted.StopAsync(ct);
        }
    }
}
