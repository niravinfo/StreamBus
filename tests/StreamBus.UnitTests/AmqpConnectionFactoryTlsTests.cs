using System.Diagnostics;
using System.Net.Security;
using System.Security.Authentication;
using RabbitMQ.Client;

namespace StreamBus.UnitTests;

public sealed class AmqpConnectionFactoryTlsTests
{
    private static StreamBusOptions ValidOptions() => new() { Vhost = "/" };

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public void Null_tls_options_keep_plain_factory_shape(bool useNullTlsSection)
    {
        var options = ValidOptions() with
        {
            Host = "broker.example",
            AmqpPort = 5672,
            UserName = "user",
            Password = "pass",
            Heartbeat = TimeSpan.FromSeconds(30),
            Tls = useNullTlsSection ? null : new StreamBusTlsOptions { Enabled = false },
        };

        var factory = AmqpConnectionFactory.BuildFactory(options);

        Assert.Equal("broker.example", factory.HostName);
        Assert.Equal(5672, factory.Port);
        Assert.Equal("user", factory.UserName);
        Assert.Equal("pass", factory.Password);
        Assert.Equal("/", factory.VirtualHost);
        Assert.Equal("streambus-error-queues", factory.ClientProvidedName);
        Assert.Equal(TimeSpan.FromSeconds(30), factory.RequestedHeartbeat);
        Assert.False(factory.Ssl.Enabled);
    }

    [Fact]
    public void Enabled_tls_maps_server_name_policy_errors_and_protocol_version()
    {
        var options = ValidOptions() with
        {
            Host = "broker.example",
            AmqpPort = 5671,
            Tls = new StreamBusTlsOptions
            {
                Enabled = true,
                ServerName = "broker.alt.example",
                AcceptableCertificateErrors = SslPolicyErrors.RemoteCertificateChainErrors,
                ProtocolVersion = SslProtocols.Tls12,
            },
        };

        var factory = AmqpConnectionFactory.BuildFactory(options);

        Assert.Equal(5671, factory.Port);
        Assert.True(factory.Ssl.Enabled);
        Assert.Equal("broker.alt.example", factory.Ssl.ServerName);
        Assert.Equal(SslPolicyErrors.RemoteCertificateChainErrors, factory.Ssl.AcceptablePolicyErrors);
        Assert.Equal(SslProtocols.Tls12, factory.Ssl.Version);
    }

    [Fact]
    public void Enabled_tls_without_server_name_falls_back_to_host()
    {
        var options = ValidOptions() with
        {
            Host = "broker.internal",
            Tls = new StreamBusTlsOptions { Enabled = true },
        };

        var factory = AmqpConnectionFactory.BuildFactory(options);

        Assert.True(factory.Ssl.Enabled);
        Assert.Equal("broker.internal", factory.Ssl.ServerName);
    }

    [Fact]
    public void Enabled_tls_with_missing_cert_path_throws_with_path_in_message()
    {
        var missingPath = Path.Combine(Path.GetTempPath(), $"streambus-missing-{Guid.NewGuid():N}.pfx");
        var options = ValidOptions() with
        {
            Tls = new StreamBusTlsOptions { Enabled = true, ClientCertPath = missingPath },
        };

        var ex = Assert.Throws<StreamBusConfigurationException>(() => AmqpConnectionFactory.BuildFactory(options));
        Assert.Contains(missingPath, ex.Message);
    }

    [Fact]
    public void Enabled_tls_with_existing_cert_file_sets_cert_path()    {
        var certPath = Path.Combine(Path.GetTempPath(), $"streambus-cert-{Guid.NewGuid():N}.pfx");
        try
        {
            File.WriteAllText(certPath, "not a real cert");
            var options = ValidOptions() with
            {
                Tls = new StreamBusTlsOptions { Enabled = true, ClientCertPath = certPath },
            };

            var factory = AmqpConnectionFactory.BuildFactory(options);

            Assert.True(factory.Ssl.Enabled);
            Assert.Equal(certPath, factory.Ssl.CertPath);
        }
        finally
        {
            File.Delete(certPath);
        }
    }

    /// <summary>
    /// Pins the observed failure shape of RabbitMQ.Client 7.2.2 when a
    /// TLS-enabled AMQP connect meets a plain TCP listener (observed live
    /// against a real plaintext broker port): the handshake dies with
    /// <see cref="AuthenticationException"/> "Cannot determine the frame size or
    /// a corrupted frame was received", wrapped in
    /// <c>BrokerUnreachableException</c>. The factory must translate that to a
    /// classified fatal error so the queue transport, declare loop, and handoff
    /// retry paths never retry a permanent misconfiguration forever.
    /// </summary>
    [Fact]
    public async Task Tls_handshake_failure_against_plain_listener_is_fatal_not_unclassified_retriable()
    {
        using var listener = new System.Net.Sockets.TcpListener(System.Net.IPAddress.Loopback, 0);
        listener.Start();
        var port = ((System.Net.IPEndPoint)listener.LocalEndpoint).Port;
        var accept = Task.Run(async () =>
        {            try
            {
                using var client = await listener.AcceptTcpClientAsync(TestContext.Current.CancellationToken);
                // Speak plaintext AMQP: the protocol header is not a TLS record,
                // so the TLS client's handshake cannot succeed.
                var stream = client.GetStream();
                await stream.WriteAsync("AMQP\x00\x00\x09\x01"u8.ToArray(), TestContext.Current.CancellationToken);
                await Task.Delay(TimeSpan.FromSeconds(30), TestContext.Current.CancellationToken);
            }
            catch (Exception)
            {
                // Listener stopped once the observation completed.
            }
        },
            TestContext.Current.CancellationToken);
        try
        {
            var options = ValidOptions() with
            {
                Host = "127.0.0.1",
                AmqpPort = port,
                UserName = "user",
                Password = "pass",
                Tls = new StreamBusTlsOptions { Enabled = true },
            };
            var connector = new AmqpConnectionFactory();

            var clock = Stopwatch.StartNew();
            var ex = await Assert.ThrowsAsync<StreamBusFatalException>(
                () => connector.ConnectAsync(options, TestContext.Current.CancellationToken));
            clock.Stop();

            Assert.True(clock.Elapsed < TimeSpan.FromSeconds(10),
                $"TLS handshake failure took {clock.Elapsed} to surface; expected a fast classified failure.");
            Assert.Contains("TLS", ex.Message, StringComparison.Ordinal);
        }
        finally
        {
            listener.Stop();
            try { await accept; } catch (Exception) { }
        }
    }

    /// <summary>
    /// Pins the aggregate traversal in HasTlsHandshakeFailure: RabbitMQ.Client's
    /// IEndpointResolverExtensions.SelectOneAsync collects per-endpoint failures
    /// into AggregateException(exceptions), and AggregateException.InnerException
    /// exposes only the first child — an AuthenticationException in any other slot
    /// (multi-endpoint or nested-aggregate shapes) must still classify the
    /// connect as a fatal TLS handshake failure, not fall back to retriable.
    /// </summary>
    [Fact]
    public void Handshake_failure_in_non_first_aggregate_slot_is_detected()
    {
        var handshake = new AuthenticationException(
            "Cannot determine the frame size or a corrupted frame was received.");
        var aggregate = new AggregateException(new System.IO.IOException("endpoint down"), handshake);

        Assert.True(AmqpConnectionFactory.HasTlsHandshakeFailure(
            new RabbitMQ.Client.Exceptions.BrokerUnreachableException(aggregate)));
    }

    [Fact]
    public void Handshake_failure_in_nested_aggregate_is_detected()
    {
        var handshake = new AuthenticationException("handshake");
        var nested = new AggregateException(
            new System.IO.IOException("first"),
            new AggregateException(new System.IO.IOException("second"), handshake));

        Assert.True(AmqpConnectionFactory.HasTlsHandshakeFailure(
            new RabbitMQ.Client.Exceptions.BrokerUnreachableException(
                new AggregateException(nested))));
    }

    [Fact]
    public void Aggregate_without_handshake_failure_is_not_detected()
    {
        var aggregate = new AggregateException(
            new System.IO.IOException("endpoint down"),
            new System.Net.Sockets.SocketException(10061));

        Assert.False(AmqpConnectionFactory.HasTlsHandshakeFailure(
            new RabbitMQ.Client.Exceptions.BrokerUnreachableException(aggregate)));
        Assert.False(AmqpConnectionFactory.HasTlsHandshakeFailure(
            new RabbitMQ.Client.Exceptions.BrokerUnreachableException(new System.IO.IOException("plain"))));
    }
}
