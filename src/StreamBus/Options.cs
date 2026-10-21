namespace StreamBus;

public sealed record StreamBusConnectionOptions
{
    public int ProducersPerConnection { get; init; } = 1;
    public int ConsumersPerConnection { get; init; } = 1;
    public TimeSpan MonitorInterval { get; init; } = TimeSpan.FromSeconds(5);

    public IEnumerable<string> Validate()
    {
        if (ProducersPerConnection < 1)
            yield return "ProducersPerConnection must be >= 1.";
        if (ProducersPerConnection > byte.MaxValue)
            yield return $"ProducersPerConnection must be <= {byte.MaxValue} (client pool limit).";
        if (ConsumersPerConnection < 1)
            yield return "ConsumersPerConnection must be >= 1.";
        if (ConsumersPerConnection > byte.MaxValue)
            yield return $"ConsumersPerConnection must be <= {byte.MaxValue} (client pool limit).";
        if (MonitorInterval <= TimeSpan.Zero)
            yield return "MonitorInterval must be positive.";
        if (MonitorInterval > TimeSpan.FromSeconds(60))
            yield return "MonitorInterval must be <= 60s.";
    }
}

public sealed record StreamBusOptions
{
    public required string Vhost { get; init; }
    public string UserName { get; init; } = "guest";
    public string Password { get; init; } = "guest";
    public string Host { get; init; } = "localhost";
    public int StreamPort { get; init; } = 5552;
    public int AmqpPort { get; init; } = 5672;
    public TimeSpan Heartbeat { get; init; } = TimeSpan.FromSeconds(60);
    public StreamBusConnectionOptions Connection { get; init; } = new();
    public IStreamSerializer Serializer { get; init; } = SystemTextJsonStreamSerializer.Default;
}
