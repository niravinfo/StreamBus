namespace StreamBus;

public enum ConsumeOffset
{
    ResumeOrFirst = 0,
    ResumeOrNext = 1,
    ResumeOrFail = 2,
    AlwaysFirst = 3,
    AlwaysNext = 4,
}

public enum ConsumeOrdering
{
    Strict = 0,
}

public sealed record OffsetStoreOptions
{
    public int EveryMessages { get; init; } = 1;
    public TimeSpan? MaxInterval { get; init; }
}

public sealed record ConsumeOptions
{
    public bool IsSuperStream { get; init; } = false;
    public string? Reference { get; init; }
    public bool SingleActiveConsumer { get; init; } = true;
    public ConsumeOrdering Ordering { get; init; } = ConsumeOrdering.Strict;
    public ConsumeOffset Offset { get; init; } = ConsumeOffset.ResumeOrFirst;
    public OffsetStoreOptions OffsetStore { get; init; } = new();
    public RetryPolicy Retry { get; init; } = new();
    public ErrorStreamOptions? ErrorStream { get; init; }
    public Func<ErrorHandoff, Task>? OnRetryExhausted { get; init; }
    public ReadinessPolicy ReadinessPolicy { get; init; } = ReadinessPolicy.FailFastPolicy;
    internal EffectivelyOnceConfig? EffectivelyOnce { get; init; }
    internal Func<ExhaustedContext, CancellationToken, Task>? ExhaustionCallback { get; init; }
    internal DeadLetterOptions? DeadLetter { get; init; }
    internal DeadLetterOptions? ShadowedDeadLetter { get; init; }
}
