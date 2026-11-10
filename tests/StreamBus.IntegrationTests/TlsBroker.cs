using System.Net;
using System.Net.Security;
using RabbitMQ.Client;
using RabbitMQ.Stream.Client;
using Testcontainers.RabbitMq;

namespace StreamBus.IntegrationTests;

/// <summary>
/// Lazily starts one RabbitMQ Testcontainers broker with TLS listeners (AMQP on
/// 5671, stream protocol on 5551) and hands out its connection details.
/// </summary>
/// <remarks>
/// Mirrors <see cref="TestBroker"/> (which stays untouched for the plain-TCP
/// tests). Differences:
/// <list type="bullet">
/// <item>The TLS listeners and certificate paths come from a <c>conf.d</c>
/// fragment and the generated test certificates, resource-mapped into the
/// container at build time.</item>
/// <item>The stream plugins are enabled via container exec after the first
/// start, then the container is restarted so the enabled plugins open the TLS
/// listeners from the mounted configuration at boot.</item>
/// <item>Both mapped host ports are pre-allocated free ports, and only the
/// TLS stream listener is advertised (the plain stream listener also
/// advertises that port, but it is never bound to the host).</item>
/// <item>Readiness polling opens real TLS client connections on both
/// transports — a plain TCP connect would succeed while the TLS listener is
/// still missing.</item>
/// </list>
/// The test CA is not trusted by the host OS, so probes and tests accept
/// <see cref="SslPolicyErrors.RemoteCertificateChainErrors"/> only; the server
/// certificate is minted for <c>localhost</c> (SAN), so name matching must
/// still succeed.
/// </remarks>
internal static class TlsBroker
{
    private static Task<TlsBrokerHandle>? _start;

    /// <summary>Starts (or joins the in-flight start of) the shared TLS broker.</summary>
    public static Task<TlsBrokerHandle> StartAsync(CancellationToken ct = default)
    {
        var task = _start;
        if (task is not null)
        {
            return task.WaitAsync(ct);
        }

        lock (typeof(TlsBroker))
        {
            _start ??= StartCoreAsync();
            var start = _start;
            _ = start.ContinueWith(OnStartFaulted, TaskScheduler.Default);
            return start.WaitAsync(ct);
        }
    }

    private static void OnStartFaulted(Task<TlsBrokerHandle> task)
    {
        if (!task.IsFaulted)
            return;
        lock (typeof(TlsBroker))
        {
            if (ReferenceEquals(_start, task))
                _start = null;
        }
    }

    /// <summary>Picks a free TCP port on the loopback interface; mirrors TestBroker's helper.</summary>
    private static int GetFreeHostPort()
    {
        var listener = new System.Net.Sockets.TcpListener(IPAddress.Loopback, 0);
        listener.Start();
        try
        {
            return ((IPEndPoint)listener.LocalEndpoint).Port;
        }
        finally
        {
            listener.Stop();
        }
    }

    private static async Task<TlsBrokerHandle> StartCoreAsync()
    {
        var certDir = Path.Combine(Path.GetTempPath(), $"streambus-tls-{Guid.NewGuid():N}");
        var certs = TlsTestCertificates.Generate(certDir);
        var confPath = Path.Combine(certDir, "90-tls.conf");
        await File.WriteAllTextAsync(confPath, string.Join("\n",
        [
            "listeners.ssl.default = 5671",
            "ssl_options.cacertfile = /etc/rabbitmq/certs/ca_certificate.pem",
            "ssl_options.certfile = /etc/rabbitmq/certs/server_certificate.pem",
            "ssl_options.keyfile = /etc/rabbitmq/certs/server_key.pem",
            "ssl_options.verify = verify_none",
            "ssl_options.fail_if_no_peer_cert = false",
            "stream.listeners.ssl.default = 5551",
        ]) + "\n");

        // See TestBroker: the mapped host port must equal the advertised stream
        // port, because the stream client connects to producers/consumers via
        // the endpoint the broker advertises.
        var streamTlsHostPort = GetFreeHostPort();
        var amqpTlsHostPort = GetFreeHostPort();
        var container = new RabbitMqBuilder("rabbitmq:4.2-management")
            .WithUsername("app")
            .WithPassword("password")
            .WithResourceMapping(confPath, "/etc/rabbitmq/conf.d/")
            .WithResourceMapping(certs.CaCertificate, "/etc/rabbitmq/certs/")
            .WithResourceMapping(certs.ServerCertificate, "/etc/rabbitmq/certs/")
            .WithResourceMapping(certs.ServerKey, "/etc/rabbitmq/certs/")
            .WithPortBinding(streamTlsHostPort, 5551)
            .WithPortBinding(amqpTlsHostPort, 5671)
            .WithEnvironment("RABBITMQ_SERVER_ADDITIONAL_ERL_ARGS",
                $"-rabbitmq_stream advertised_host localhost advertised_port {streamTlsHostPort}")
            .WithCleanUp(true)
            .Build();

        await container.StartAsync();

        // Enable the stream plugins, then restart so the plugins — already
        // persisted in the container's enabled_plugins file — open the TLS
        // listeners from the mounted conf.d fragment at boot. (RabbitMQ 4.x can
        // also apply these keys when a plugin starts at runtime; restarting
        // makes that question moot instead of relying on it.)
        await container.ExecAsync(new[]
        {
            "rabbitmq-plugins",
            "enable",
            "rabbitmq_stream",
            "rabbitmq_stream_management",
        });
        await container.StopAsync();
        await container.StartAsync();

        var handle = new TlsBrokerHandle(
            container.Hostname,
            streamTlsHostPort,
            amqpTlsHostPort,
            "app",
            "password");
        await WaitForTlsListenersAsync(handle);
        return handle;
    }

    /// <summary>
    /// Polls (up to 120 s) until real TLS client connections succeed against
    /// BOTH the stream listener and the AMQP listener. TLS handshakes fail
    /// while the listeners are still coming up, so this is a genuine readiness
    /// gate, not just a TCP check.
    /// </summary>
    private static async Task WaitForTlsListenersAsync(TlsBrokerHandle handle)
    {
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(120));
        Exception? lastError = null;

        string Describe(string what)
        {
            if (lastError is not null)
                what += ". Last probe error: " + lastError;
            return what;
        }

        while (true)
        {
            try
            {
                await ProbeStreamAsync(handle);
                await ProbeAmqpAsync(handle, timeout.Token);
                return;
            }
            catch (OperationCanceledException)
            {
                throw new TimeoutException(
                    Describe($"RabbitMQ TLS listeners did not accept connections within 120s on {handle.Host}:stream {handle.StreamTlsPort}, amqp {handle.AmqpTlsPort}."));
            }
            catch (Exception ex)
            {
                lastError = ex;
                // Delay outside the catch so the timeout token's cancellation
                // escapes the generic handler and surfaces as the TimeoutException
                // below instead of bubbling up as OperationCanceledException.
                try
                {
                    await Task.Delay(500, timeout.Token);
                }
                catch (OperationCanceledException)
                {
                    throw new TimeoutException(
                        Describe($"RabbitMQ TLS listeners did not accept connections within 120s on {handle.Host}:stream {handle.StreamTlsPort}, amqp {handle.AmqpTlsPort}."));
                }
            }
        }
    }

    private static async Task ProbeStreamAsync(TlsBrokerHandle handle)
    {
        await using var system = await StreamSystem.Create(new StreamSystemConfig
        {
            Endpoints = new List<EndPoint> { new IPEndPoint(IPAddress.Parse(handle.Host), handle.StreamTlsPort) },
            UserName = handle.Username,
            Password = handle.Password,
            VirtualHost = "/",
            Ssl = new RabbitMQ.Stream.Client.SslOption
            {
                Enabled = true,
                ServerName = "localhost",
                AcceptablePolicyErrors = SslPolicyErrors.RemoteCertificateChainErrors,
            },
        });
        BrokerProbe.StreamExists(system, "readiness-probe");
    }

    private static async Task ProbeAmqpAsync(TlsBrokerHandle handle, CancellationToken ct)
    {
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
        await using var connection = await factory.CreateConnectionAsync(ct);
    }

    public sealed record TlsBrokerHandle(
        string Host,
        int StreamTlsPort,
        int AmqpTlsPort,
        string Username,
        string Password);
}
