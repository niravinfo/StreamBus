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
}
