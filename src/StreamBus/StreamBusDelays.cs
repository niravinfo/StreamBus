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
}
