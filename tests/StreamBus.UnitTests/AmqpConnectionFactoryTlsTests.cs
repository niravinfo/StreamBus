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
    public void Enabled_tls_with_existing_cert_file_sets_cert_path()
    {
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
}
