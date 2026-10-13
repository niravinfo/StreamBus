using Microsoft.Extensions.Logging;

namespace StreamBus;

public enum FailureMode
{
    Strict = 0,
    SkipAfterDeadline = 1,
}

public sealed record RetryPolicy
{
    public FailureMode FailureMode { get; init; } = FailureMode.Strict;
    public int? MaxAttempts { get; init; }
    public TimeSpan BaseDelay { get; init; } = TimeSpan.FromMilliseconds(200);
    public double Multiplier { get; init; } = 2.0;
    public TimeSpan MaxDelay { get; init; } = TimeSpan.FromSeconds(30);
    public TimeSpan MaxJitter { get; init; } = TimeSpan.FromSeconds(1);
    public Func<int, Exception, TimeSpan>? DelayCalculator { get; init; }
}

public sealed record ErrorStreamOptions
{
    public required string Stream { get; init; }
    public string? RoutingValue { get; init; }
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
