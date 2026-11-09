namespace StreamBus;

/// <summary>Per-connection pool sizing and supervision timing for a bus.</summary>
public sealed record StreamBusConnectionOptions
{
    /// <summary>Producers multiplexed onto each connection; 1..255.</summary>
    public int ProducersPerConnection { get; init; } = 1;

    /// <summary>Consumers multiplexed onto each connection; 1..255.</summary>
    public int ConsumersPerConnection { get; init; } = 1;

    /// <summary>Interval between connection monitor passes; positive, at most 60 seconds.</summary>
    public TimeSpan MonitorInterval { get; init; } = TimeSpan.FromSeconds(5);

    internal IEnumerable<string> Validate()
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

/// <summary>Connection settings for a StreamBus bus; credentials, host, ports, and serializer.</summary>
public sealed record StreamBusOptions
{
    /// <summary>RabbitMQ virtual host to open.</summary>
    public required string Vhost { get; init; }

    /// <summary>Broker user name.</summary>
    public string UserName { get; init; } = "guest";

    /// <summary>Broker password.</summary>
    public string Password { get; init; } = "guest";

    /// <summary>Broker host name or address.</summary>
    public string Host { get; init; } = "localhost";

    /// <summary>Stream protocol port; 1..65535.</summary>
    public int StreamPort { get; init; } = 5552;

    /// <summary>AMQP port used for dead-letter queues and topology; 1..65535.</summary>
    public int AmqpPort { get; init; } = 5672;

    /// <summary>TLS settings for both the stream protocol and AMQP connections; null means plain TCP (TLS off, the default).</summary>
    public StreamBusTlsOptions? Tls { get; init; }

    /// <summary>Requested heartbeat interval; must be positive.</summary>
    public TimeSpan Heartbeat { get; init; } = TimeSpan.FromSeconds(60);

    /// <summary>Per-connection pool sizing and supervision timing.</summary>
    public StreamBusConnectionOptions Connection { get; init; } = new();

    /// <summary>Message serializer used by typed publish and consume.</summary>
    public IStreamSerializer Serializer { get; init; } = SystemTextJsonStreamSerializer.Default;

    /// <summary>
    /// The single validation boundary for connection options. Configurators,
    /// the supervisor, and the connector all delegate here instead of
    /// re-implementing range or member checks.
    /// </summary>
    internal IEnumerable<string> Validate()
    {
        foreach (var error in ValidateCredentials())
            yield return error;
        if (string.IsNullOrWhiteSpace(Host))
            yield return "Host must be supplied.";
        if (StreamPort is < 1 or > 65535)
            yield return "StreamPort must be in 1..65535.";
        if (AmqpPort is < 1 or > 65535)
            yield return "AmqpPort must be in 1..65535.";
        if (Heartbeat <= TimeSpan.Zero)
            yield return "Heartbeat must be positive.";
        if (Tls?.Enabled == true && Tls.ServerName is not null && string.IsNullOrWhiteSpace(Tls.ServerName))
            yield return "Tls.ServerName must not be whitespace.";
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
