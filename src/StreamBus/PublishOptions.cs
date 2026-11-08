namespace StreamBus;

/// <summary>
/// Per-call options for a raw publish. Defaults mirror the typed publish
/// path: fail-fast readiness, plain stream, no routing key, a 10-second
/// confirm timeout, and no message properties.
/// </summary>
public sealed record PublishOptions
{
    /// <summary>How the publish waits for a ready connection; bounds only
    /// the readiness wait, not the confirm.</summary>
    public ReadinessPolicy ReadinessPolicy { get; init; } = ReadinessPolicy.FailFastPolicy;

    /// <summary>Whether the target is a super stream (publish routes to one
    /// partition per routing key).</summary>
    public bool IsSuperStream { get; init; }

    /// <summary>Routing key for a super-stream publish; ignored for a plain
    /// stream.</summary>
    public string? RoutingKey { get; init; }

    /// <summary>Confirm timeout bounding the broker acknowledgment; a
    /// timeout leaves the outcome unknown.</summary>
    public TimeSpan Timeout { get; init; } = TimeSpan.FromSeconds(10);

    /// <summary>AMQP message properties (ids, sent time, content type,
    /// headers) for this publish; null sends a body with no properties.</summary>
    public StreamMessageProperties? Properties { get; init; }
}
