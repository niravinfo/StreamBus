namespace StreamBus;

/// <summary>
/// Per-call options for a raw publish. Defaults mirror the typed publish
/// path: fail-fast readiness, plain stream, no routing key, no confirm
/// timeout (waits indefinitely for the broker confirm), and no message
/// properties.
/// </summary>
public sealed record PublishOptions
{
    /// <summary>How the publish waits for a ready connection; bounds only
    /// the readiness wait, not the confirm.</summary>
    public ReadinessPolicy Readiness { get; init; } = ReadinessPolicy.Default;

    /// <summary>Whether the target is a super stream (publish routes to one
    /// partition per routing key).</summary>
    public bool IsSuperStream { get; init; }

    /// <summary>Routing key for a super-stream publish; ignored for a plain
    /// stream.</summary>
    public string? RoutingKey { get; init; }

    /// <summary>Timeout for the broker confirm; null waits indefinitely
    /// (the caller's cancellation token is the only bound). A timeout leaves
    /// the outcome unknown.</summary>
    public TimeSpan? ConfirmTimeout { get; init; }

    /// <summary>AMQP message properties (ids, sent time, content type,
    /// headers) for this publish; null sends a body with no properties.</summary>
    public StreamMessageProperties? Properties { get; init; }
}
