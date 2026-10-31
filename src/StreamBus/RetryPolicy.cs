using Microsoft.Extensions.Logging;

namespace StreamBus;

/// <summary>
/// Retry timing and boundedness. A <see langword="null"/>
/// <see cref="MaxAttempts"/> means unbounded retry.
/// </summary>
public sealed record RetryPolicy
{
    /// <summary>
    /// Total attempts including the initial one. <see langword="null"/> is
    /// unbounded; <c>1</c> means no retry.
    /// </summary>
    public int? MaxAttempts { get; init; }

    /// <summary>Base delay for the exponential schedule.</summary>
    public TimeSpan BaseDelay { get; init; } = TimeSpan.FromMilliseconds(200);

    /// <summary>Exponential backoff multiplier.</summary>
    public double Multiplier { get; init; } = 2.0;

    /// <summary>Upper bound for a computed delay.</summary>
    public TimeSpan MaxDelay { get; init; } = TimeSpan.FromSeconds(30);

    /// <summary>Maximum random jitter added to a computed delay.</summary>
    public TimeSpan MaxJitter { get; init; } = TimeSpan.FromSeconds(1);

    /// <summary>
    /// Optional caller-supplied delay. When set, it overrides the exponential
    /// schedule; throwing or negative results fall back safely.
    /// </summary>
    public Func<int, Exception, TimeSpan>? DelayCalculator { get; init; }
}

/// <summary>
/// Stream destination for exhausted messages.
/// </summary>
public sealed record ErrorStreamOptions
{
    /// <summary>Error stream name.</summary>
    public required string Stream { get; init; }
}

public sealed record ErrorHandoff
{
    public required string ErrorStream { get; init; }
    public required string SourceStream { get; init; }
    public required int SourcePartition { get; init; }
    public required ulong SourceOffset { get; init; }
    public required int Attempts { get; init; }
    public required DateTimeOffset FailedAt { get; init; }
    public required Exception Exception { get; init; }
}

internal static class RetryDelays
{
    public static TimeSpan ResolveDelay(RetryPolicy policy, int attempt, Exception ex, ILogger logger)
    {
        ArgumentNullException.ThrowIfNull(policy);
        ArgumentNullException.ThrowIfNull(ex);
        ArgumentNullException.ThrowIfNull(logger);
        ArgumentOutOfRangeException.ThrowIfLessThan(attempt, 1);
        if (policy.DelayCalculator is { } calc)
        {
            try
            {
                return Clamp(calc(attempt, ex), attempt, logger);
            }
            catch (Exception cex)
            {
                logger.LogWarning(cex,
                    "Retry DelayCalculator threw for attempt {Attempt}; falling back to exponential knobs.",
                    attempt);
            }
        }
        return StreamBusDelays.ExponentialCapped(
            attempt, policy.BaseDelay, policy.MaxDelay, policy.Multiplier, policy.MaxJitter);
    }

    public static bool ShouldLogRetryWarning(int attempt)
        => attempt <= 5 || attempt % 100 == 0;

    private static TimeSpan Clamp(TimeSpan computed, int attempt, ILogger logger)
    {
        if (computed >= TimeSpan.Zero) return computed;
        logger.LogWarning(
            "Retry DelayCalculator returned negative {Delay} for attempt {Attempt}; clamped to zero.",
            computed, attempt);
        return TimeSpan.Zero;
    }
}
