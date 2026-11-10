namespace StreamBus.UnitTests;

public sealed class TlsOptionsTests
{
    private static StreamBusOptions ValidOptions() => new() { Vhost = "/" };

    [Fact]
    public void Default_options_have_no_tls()
    {
        var options = ValidOptions();

        Assert.Null(options.Tls);
        Assert.Empty(options.Validate());
    }

    [Fact]
    public void Enabled_tls_without_server_name_validates_clean()
    {
        var options = ValidOptions() with
        {
            Tls = new StreamBusTlsOptions { Enabled = true }
        };

        Assert.Empty(options.Validate());
    }

    [Fact]
    public void Enabled_tls_with_whitespace_server_name_is_rejected()
    {
        var options = ValidOptions() with
        {
            Tls = new StreamBusTlsOptions { Enabled = true, ServerName = "   " }
        };

        Assert.Contains(options.Validate(), error => error.Contains("ServerName"));
    }

    [Fact]
    public void Disabled_tls_ignores_server_name()
    {
        var options = ValidOptions() with
        {
            Tls = new StreamBusTlsOptions { Enabled = false, ServerName = "   " }
        };

        Assert.Empty(options.Validate());
    }

    // The deprecated members are named deliberately: the tests assert that
    // exactly these protocols are rejected by validation. CA5397 is disabled
    // for test assemblies only (see Directory.Build.props); the shipped
    // library never names them.
    public static TheoryData<System.Security.Authentication.SslProtocols> InsecureProtocols => new()
    {
        System.Security.Authentication.SslProtocols.Ssl3,
        System.Security.Authentication.SslProtocols.Tls,
        System.Security.Authentication.SslProtocols.Tls11,
        System.Security.Authentication.SslProtocols.Ssl3 | System.Security.Authentication.SslProtocols.Tls12,
        System.Security.Authentication.SslProtocols.Ssl2,
    };

    [Theory]
    [MemberData(nameof(InsecureProtocols))]
    public void Enabled_tls_with_pre_tls12_protocol_is_rejected(
        System.Security.Authentication.SslProtocols protocol)
    {
        var options = ValidOptions() with
        {
            Tls = new StreamBusTlsOptions { Enabled = true, ProtocolVersion = protocol }
        };

        Assert.Contains(options.Validate(), error => error.Contains("ProtocolVersion"));
    }

    [Theory]
    [InlineData(System.Security.Authentication.SslProtocols.None)]
    [InlineData(System.Security.Authentication.SslProtocols.Tls12)]
    [InlineData(System.Security.Authentication.SslProtocols.Tls13)]
    public void Enabled_tls_with_tls12_or_newer_protocol_validates_clean(
        System.Security.Authentication.SslProtocols protocol)
    {
        var options = ValidOptions() with
        {
            Tls = new StreamBusTlsOptions { Enabled = true, ProtocolVersion = protocol }
        };

        Assert.Empty(options.Validate());
    }

    [Fact]
    public void Disabled_tls_ignores_insecure_protocol()
    {
        var options = ValidOptions() with
        {
            Tls = new StreamBusTlsOptions
            {
                Enabled = false,
                ProtocolVersion = System.Security.Authentication.SslProtocols.Ssl3,
            }
        };

        Assert.Empty(options.Validate());
    }
}
