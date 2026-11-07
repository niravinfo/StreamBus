namespace StreamBus;

public enum ConsumeOffset
{
    ResumeOrFirst = 0,
    ResumeOrNext = 1,
    ResumeOrFail = 2,
    AlwaysFirst = 3,
    AlwaysNext = 4,
}

public sealed record OffsetStoreOptions
{
    public int EveryMessages { get; init; } = 1;
    public TimeSpan? MaxInterval { get; init; }
}

public sealed record ConsumeOptions
{
    public bool IsSuperStream { get; init; }
    public string? Reference { get; init; }
    public bool SingleActiveConsumer { get; init; } = true;
    public ConsumeOffset Offset { get; init; } = ConsumeOffset.ResumeOrFirst;
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

    public ReadinessPolicy ReadinessPolicy { get; init; } = ReadinessPolicy.FailFastPolicy;
    /// <summary>Bound on the confirmed error-stream hand-off publish. A lost
    /// confirmation surfaces as transient so the partition dispatch gate is
    /// never wedged; the envelope stays replayable.</summary>
    public TimeSpan HandoffConfirmTimeout { get; init; } = TimeSpan.FromSeconds(10);
    internal DeadLetterOptions? DeadLetter { get; init; }
    internal DeadLetterOptions? ShadowedDeadLetter { get; init; }
    internal IReadOnlyList<ConsumeFilterRegistration> ConsumeFilters { get; init; } = Array.Empty<ConsumeFilterRegistration>();
    internal Type? MessageType { get; init; }
}
