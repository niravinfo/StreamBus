using System.Net;
using RabbitMQ.Stream.Client;

namespace StreamBus.UnitTests;

public sealed class StreamConnectorTlsTests
{
    private static StreamBusOptions ValidOptions() => new() { Vhost = "/" };

    [Fact]
    public void Null_tls_options_keep_plain_config_shape()
    {
        var options = ValidOptions() with
        {
            Host = "broker.example",
            StreamPort = 5552,
            UserName = "user",
            Password = "pass",
            Heartbeat = TimeSpan.FromSeconds(30),
        };

        var config = StreamSystemConnector.BuildLocatorConfig(options);

        Assert.Equal("user", config.UserName);
        Assert.Equal("pass", config.Password);
        Assert.Equal("/", config.VirtualHost);
        Assert.Equal(TimeSpan.FromSeconds(30), config.Heartbeat);
        var endpoint = Assert.IsType<DnsEndPoint>(Assert.Single(config.Endpoints));
        Assert.Equal("broker.example", endpoint.Host);
        Assert.Equal(5552, endpoint.Port);
        Assert.True(config.Ssl is null || !config.Ssl.Enabled);
    }

    [Fact]
    public void Enabled_tls_with_server_name_sets_ssl_server_name()
    {
        var options = ValidOptions() with
        {
            Host = "localhost",
            Tls = new StreamBusTlsOptions { Enabled = true, ServerName = "broker.example" },
        };

        var config = StreamSystemConnector.BuildLocatorConfig(options);

        Assert.NotNull(config.Ssl);
        Assert.True(config.Ssl.Enabled);
        Assert.Equal("broker.example", config.Ssl.ServerName);
    }

    [Fact]
    public void Enabled_tls_without_server_name_falls_back_to_host()
    {
        var options = ValidOptions() with
        {
            Host = "broker.internal",
            Tls = new StreamBusTlsOptions { Enabled = true },
        };

        var config = StreamSystemConnector.BuildLocatorConfig(options);

        Assert.NotNull(config.Ssl);
        Assert.Equal("broker.internal", config.Ssl.ServerName);
    }

    [Fact]
    public void Enabled_tls_with_missing_cert_path_throws_with_path_in_message()
    {
        var missingPath = Path.Combine(Path.GetTempPath(), $"streambus-missing-{Guid.NewGuid():N}.pfx");
        var options = ValidOptions() with
        {
            Tls = new StreamBusTlsOptions { Enabled = true, ClientCertPath = missingPath },
        };

        var ex = Assert.Throws<StreamBusConfigurationException>(() => StreamSystemConnector.BuildLocatorConfig(options));
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

            var config = StreamSystemConnector.BuildLocatorConfig(options);

            Assert.NotNull(config.Ssl);
            Assert.Equal(certPath, config.Ssl.CertPath);
        }
        finally
        {
            File.Delete(certPath);
        }
    }
}
