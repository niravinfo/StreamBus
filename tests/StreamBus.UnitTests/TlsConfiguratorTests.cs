using System.Net.Security;
using System.Security.Authentication;

namespace StreamBus.UnitTests;

public sealed class TlsConfiguratorTests
{
    private sealed class TlsTestBus;

    private static StreamBusOptions BuildCore(Action<IHostConfigurator> host)
    {
        var configurator = new StreamBusConfigurator(typeof(TlsTestBus));
        configurator.Host("localhost", "/", host);
        return configurator.Build(typeof(TlsTestBus));
    }

    private static StreamBusOptions Build(Action<IHostConfigurator> host) => BuildCore(host);

    [Fact]
    public void Without_UseTls_Tls_is_null_and_ports_are_plain_defaults()
    {
        var options = Build(h => h.Credentials("u", "p"));

        Assert.Null(options.Tls);
        Assert.Equal(5552, options.StreamPort);
        Assert.Equal(5672, options.AmqpPort);
    }

    [Fact]
    public void UseTls_alone_enables_tls_and_switches_default_ports()
    {
        var options = Build(h =>
        {
            h.Credentials("u", "p");
            h.UseTls();
        });

        Assert.NotNull(options.Tls);
        Assert.True(options.Tls!.Enabled);
        Assert.Equal(5551, options.StreamPort);
        Assert.Equal(5671, options.AmqpPort);
    }

    [Fact]
    public void Explicit_ports_set_after_UseTls_win()
    {
        var options = Build(h =>
        {
            h.Credentials("u", "p");
            h.UseTls();
            h.StreamPort(1234);
            h.AmqpPort(15672);
        });

        Assert.Equal(1234, options.StreamPort);
        Assert.Equal(15672, options.AmqpPort);
    }

    [Fact]
    public void Explicit_ports_set_before_UseTls_still_win()
    {
        var options = Build(h =>
        {
            h.Credentials("u", "p");
            h.StreamPort(1234);
            h.AmqpPort(15672);
            h.UseTls();
        });

        Assert.Equal(1234, options.StreamPort);
        Assert.Equal(15672, options.AmqpPort);
    }

    [Fact]
    public void UseTls_callback_carries_all_settings()
    {
        var options = Build(h =>
        {
            h.Credentials("u", "p");
            h.UseTls(t =>
            {
                t.ServerName("broker.example");
                t.ClientCertificate("cert.pfx", "pw");
                t.AcceptableCertificateErrors(SslPolicyErrors.RemoteCertificateChainErrors);
                t.ProtocolVersion(SslProtocols.Tls13);
            });
        });

        Assert.NotNull(options.Tls);
        Assert.True(options.Tls!.Enabled);
        Assert.Equal("broker.example", options.Tls.ServerName);
        Assert.Equal("cert.pfx", options.Tls.ClientCertPath);
        Assert.Equal("pw", options.Tls.ClientCertPassword);
        Assert.Equal(SslPolicyErrors.RemoteCertificateChainErrors, options.Tls.AcceptableCertificateErrors);
        Assert.Equal(SslProtocols.Tls13, options.Tls.ProtocolVersion);
    }

    [Fact]
    public void Second_UseTls_call_replaces_previous_settings()
    {
        var options = Build(h =>
        {
            h.Credentials("u", "p");
            h.UseTls(t => t.ServerName("first"));
            h.UseTls(t => t.ServerName("second"));
        });

        Assert.NotNull(options.Tls);
        Assert.Equal("second", options.Tls!.ServerName);
    }

    [Fact]
    public void UseTls_defaults_have_no_client_cert_and_no_accepted_errors()
    {
        var options = Build(h =>
        {
            h.Credentials("u", "p");
            h.UseTls();
        });

        Assert.NotNull(options.Tls);
        Assert.Null(options.Tls!.ClientCertPath);
        Assert.Null(options.Tls.ClientCertPassword);
        Assert.Equal(SslPolicyErrors.None, options.Tls.AcceptableCertificateErrors);
    }
}
