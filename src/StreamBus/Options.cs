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

    /// <summary>
    /// The single validation boundary for connection options. Configurators,
    /// the supervisor, and the connector all delegate here instead of
    /// re-implementing range or member checks.
    /// </summary>
    public IEnumerable<string> Validate()
    {
        foreach (var error in ValidateCredentials())
            yield return error;
        if (string.IsNullOrWhiteSpace(Host))
            yield return "Host host must be supplied.";
        if (StreamPort is < 1 or > 65535)
            yield return "StreamPort must be in 1..65535.";
        if (AmqpPort is < 1 or > 65535)
            yield return "AmqpPort must be in 1..65535.";
        if (Heartbeat <= TimeSpan.Zero)
            yield return "Heartbeat must be positive.";
        foreach (var error in Connection.Validate())
            yield return error;
    }

    internal IEnumerable<string> ValidateCredentials()
    {
        if (string.IsNullOrWhiteSpace(UserName))
            yield return "UserName must be supplied.";
        if (string.IsNullOrWhiteSpace(Password))
            yield return "Password must be supplied.";
        if (string.IsNullOrWhiteSpace(Vhost))
            yield return "Vhost must be supplied.";
    }
}
