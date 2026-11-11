namespace StreamBus;

internal static class StreamBusDelays
{
    public static TimeSpan ExponentialCapped(int attempt, TimeSpan baseDelay, TimeSpan maxDelay, int jitterCapMs = 1000)
    {
        ArgumentOutOfRangeException.ThrowIfLessThan(attempt, 1);
        var grown = TimeSpan.FromMilliseconds(baseDelay.TotalMilliseconds * Math.Pow(2, Math.Min(attempt - 1, 8)));
        if (grown > maxDelay) grown = maxDelay;
        var jitter = TimeSpan.FromMilliseconds(Random.Shared.Next(0, (int)Math.Min(jitterCapMs, grown.TotalMilliseconds / 2 + 1)));
        return grown + jitter;
    }

    public static TimeSpan ExponentialCapped(int attempt, TimeSpan baseDelay, TimeSpan maxDelay, TimeSpan maxJitter)
    {
        ArgumentOutOfRangeException.ThrowIfLessThan(attempt, 1);
        // Overflow-proof: astronomical knobs (or a hostile direct call) clamp to
        // maxDelay instead of throwing out of the dispatch loop. Growth is fixed
        // at ×2 per attempt; custom growth goes through RetryPolicy.DelayCalculator.
        var raw = baseDelay.TotalMilliseconds * Math.Pow(2, Math.Min(attempt - 1, 16));
        var grown = double.IsNaN(raw) || raw <= 0
            ? TimeSpan.Zero
            : raw >= maxDelay.TotalMilliseconds
                ? maxDelay
                : TimeSpan.FromMilliseconds(raw);
        if (grown < TimeSpan.Zero) grown = TimeSpan.Zero;
        var cap = TimeSpan.FromMilliseconds(Math.Min(maxJitter.TotalMilliseconds, grown.TotalMilliseconds / 2));
        var jitter = cap <= TimeSpan.Zero ? TimeSpan.Zero : TimeSpan.FromMilliseconds(Random.Shared.NextDouble() * cap.TotalMilliseconds);
        return grown + jitter;
    }
}
