namespace StreamBus;

public sealed record StreamBusConnectionOptions
{
    public int ProducersPerConnection { get; init; } = 1;
    public int ConsumersPerConnection { get; init; } = 1;

    public IEnumerable<string> Validate()
    {
        if (ProducersPerConnection < 1)
            yield return "ProducersPerConnection must be >= 1.";
        if (ConsumersPerConnection < 1)
            yield return "ConsumersPerConnection must be >= 1.";
    }
}

public sealed record StreamBusHostOptions
{
    public bool BlockStartupUntilReady { get; init; } = false;
    public TimeSpan? StartupTimeout { get; init; }
}

public sealed record StreamBusOptions
{
    public required string Vhost { get; init; }
    public required string UserName { get; init; }
    public required string Password { get; init; }
    public string Host { get; init; } = "127.0.0.1";
    public int Port { get; init; } = 5552;
    public StreamBusConnectionOptions Connection { get; init; } = new();
    public StreamBusHostOptions Hosting { get; init; } = new();
}
