namespace StreamBus;

public sealed record PublishOptions
{
    public ReadinessPolicy ReadinessPolicy { get; init; } = ReadinessPolicy.FailFastPolicy;
    public bool IsSuperStream { get; init; }
    public string? RoutingKey { get; init; }
    public TimeSpan Timeout { get; init; } = TimeSpan.FromSeconds(10);
}
