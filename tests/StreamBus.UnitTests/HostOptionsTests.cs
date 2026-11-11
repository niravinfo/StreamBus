namespace StreamBus.UnitTests;

public sealed class HostOptionsTests
{
    [Fact]
    public void Defaults_match_MassTransit()
    {
        var options = new StreamBusHostOptions();

        Assert.False(options.WaitUntilStarted);
        Assert.Null(options.StartTimeout);
        Assert.True(options.WaitUntilConsumersAttached);
        Assert.Null(options.StopTimeout);
        Assert.Null(options.ConsumerStopTimeout);
    }

    [Fact]
    public void ValidateStart_rejects_non_positive_StartTimeout()
    {
        Assert.Throws<StreamBusConfigurationException>(() =>
            StreamBusHostOptions.ValidateStart(new StreamBusHostOptions { StartTimeout = TimeSpan.Zero }));
        Assert.Throws<StreamBusConfigurationException>(() =>
            StreamBusHostOptions.ValidateStart(new StreamBusHostOptions { StartTimeout = TimeSpan.FromSeconds(-1) }));

        StreamBusHostOptions.ValidateStart(new StreamBusHostOptions { StartTimeout = null });
        StreamBusHostOptions.ValidateStart(new StreamBusHostOptions { StartTimeout = TimeSpan.FromSeconds(30) });
    }

    [Fact]
    public void ValidateStop_rejects_non_positive_StopTimeout()
    {
        Assert.Throws<StreamBusConfigurationException>(() =>
            StreamBusHostOptions.ValidateStop(new StreamBusHostOptions { StopTimeout = TimeSpan.Zero }));

        StreamBusHostOptions.ValidateStop(new StreamBusHostOptions { StopTimeout = null });
    }

    [Fact]
    public void ValidateStop_rejects_ConsumerStopTimeout_greater_than_StopTimeout()
    {
        Assert.Throws<StreamBusConfigurationException>(() =>
            StreamBusHostOptions.ValidateStop(new StreamBusHostOptions
            {
                StopTimeout = TimeSpan.FromSeconds(5),
                ConsumerStopTimeout = TimeSpan.FromSeconds(6),
            }));

        StreamBusHostOptions.ValidateStop(new StreamBusHostOptions
        {
            StopTimeout = TimeSpan.FromSeconds(5),
            ConsumerStopTimeout = TimeSpan.FromSeconds(5),
        });
    }

    [Fact]
    public void ValidateStop_allows_ConsumerStopTimeout_without_StopTimeout()
    {
        StreamBusHostOptions.ValidateStop(new StreamBusHostOptions
        {
            StopTimeout = null,
            ConsumerStopTimeout = TimeSpan.FromSeconds(1),
        });
    }
}
