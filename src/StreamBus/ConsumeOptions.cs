namespace StreamBus;

/// <summary>
/// Where a consumer group starts on a partition when no stored offset exists,
/// and how a stored offset is treated.
/// </summary>
public enum StartOffset
{
    /// <summary>Resume from the stored offset, or the very first offset when none is stored.</summary>
    ResumeOrFirst = 0,
    /// <summary>Resume from the stored offset, or the next offset after attach when none is stored.</summary>
    ResumeOrNext = 1,
    /// <summary>Resume from the stored offset; fail attachment when none is stored.</summary>
    ResumeOrFail = 2,
    /// <summary>Ignore any stored offset and start from the very first offset.</summary>
    AlwaysFirst = 3,
    /// <summary>Ignore any stored offset and start from the next offset after attach.</summary>
    AlwaysNext = 4,
}

/// <summary>Broker-side offset persistence cadence for a consumer group.</summary>
public sealed record OffsetStoreOptions
{
    /// <summary>Store the offset after this many messages; 1 stores every delivery (default).</summary>
    public int StoreEveryMessages { get; init; } = 1;

    /// <summary>Also flush the stored offset at least every this often; null disables the time-based flush.</summary>
    public TimeSpan? MaxFlushInterval { get; init; }
}

/// <summary>
/// Raw consume settings: apply whether the stream is a super stream, the
/// consumer-group identity, the start offset, retry/error-handoff behavior, and
/// the attach readiness policy.
/// </summary>
public sealed record ConsumeOptions
{
    /// <summary><see langword="true"/> when the stream is a super stream (all partitions are consumed).</summary>
    public bool IsSuperStream { get; init; }

    /// <summary>
    /// Consumer-group consumerGroup; the group is the cursor identity and is used
    /// verbatim. Required: a missing group throws
    /// <see cref="StreamBusConfigurationException"/> at attach instead of
    /// guessing; over the broker's 255-byte limit is also an error.
    /// </summary>
    public string? ConsumerGroup { get; init; }

    /// <summary>
    /// Single-active-consumer per (stream, consumerGroup): only one instance in the
    /// group receives deliveries. Set <see langword="false"/> to fan out to every
    /// instance (never run two offset writers on one consumerGroup outside SAC).
    /// </summary>
    public bool SingleActiveConsumer { get; init; } = true;

    /// <summary>Start-offset policy for the group; defaults to resume-or-first.</summary>
    public StartOffset StartOffset { get; init; } = StartOffset.ResumeOrFirst;

    /// <summary>Broker-side offset persistence cadence.</summary>
    public OffsetStoreOptions OffsetStore { get; init; } = new();

    /// <summary>
    /// Retry policy. The default sets no attempt limit: a failing handler is
    /// retried forever and its partition waits, so nothing is skipped. To cap
    /// attempts and hand the message off instead, set
    /// <see cref="RetryPolicy.MaxAttempts"/> and configure an error destination:
    /// <see cref="ErrorStream"/> (an error stream) or <see cref="OnExhausted"/> (a
    /// callback). The endpoint API also offers a dead-letter queue
    /// (<c>DeadLetterToQueue</c>).
    /// </summary>
    public RetryPolicy Retry { get; init; } = new();

    /// <summary>
    /// Error-stream destination for exhausted messages (an alternative to
    /// <see cref="OnExhausted"/>, which it is mutually exclusive with). The
    /// stream is declared from the destination's stream spec; consuming or
    /// draining it afterwards is the application's responsibility.
    /// </summary>
    public ErrorStreamOptions? ErrorStream { get; init; }

    /// <summary>
    /// Terminal handler for exhausted messages. The callback owns the outcome:
    /// it replaces any hand-off destination (on the raw <see cref="ConsumeOptions"/>
    /// path <see cref="ErrorStream"/> and <see cref="OnExhausted"/> are mutually
    /// exclusive). Returning advances past the message; throwing recycles the
    /// attach (redeliver, never skip). Resolve services from
    /// <see cref="ExhaustedContext.Services"/>; the endpoint configurator exposes
    /// the same callback as <c>OnExhausted</c>.
    /// </summary>
    public Func<ExhaustedContext, CancellationToken, Task>? OnExhausted { get; init; }

    /// <summary>Gates the consumer attach: fail fast, wait bounded, or wait indefinitely (default fail fast).</summary>
    public ReadinessPolicy Readiness { get; init; } = ReadinessPolicy.Default;
    /// <summary>Timeout for the confirmed publish to the error stream;
    /// null waits indefinitely (the partition loop's cancellation token is the
    /// only bound). A lost confirmation surfaces as transient so the partition
    /// dispatch gate is never wedged; the hand-off is retried without advancing.</summary>
    public TimeSpan? ErrorPublishTimeout { get; init; }
    internal DeadLetterOptions? DeadLetter { get; init; }
    internal DeadLetterOptions? ShadowedDeadLetter { get; init; }
    internal IReadOnlyList<ConsumeFilterRegistration> ConsumeFilters { get; init; } = Array.Empty<ConsumeFilterRegistration>();
    internal Type? MessageType { get; init; }
}
