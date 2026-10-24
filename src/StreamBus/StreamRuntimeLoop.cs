using Microsoft.Extensions.Logging;

namespace StreamBus;

internal enum StreamBusRetryKind
{
    Transient,
    Fatal,
}

internal static class StreamRuntimeLoop
{
    /// <summary>One skeleton for every bounded start/declare retry: attempt,
    /// classify, delay with exponential-capped backoff, rethrow non-transients.</summary>
    public static async Task<T> RetryUntilAsync<T>(
        Func<CancellationToken, Task<T>> attempt,
        Func<Exception, StreamBusRetryKind> classify,
        TimeSpan baseDelay,
        TimeSpan maxDelay,
        int maxAttempts,
        ILogger logger,
        string operation,
        CancellationToken ct)
    {
        ArgumentNullException.ThrowIfNull(attempt);
        ArgumentNullException.ThrowIfNull(classify);
        ArgumentNullException.ThrowIfNull(logger);
        ArgumentException.ThrowIfNullOrWhiteSpace(operation);
        ArgumentOutOfRangeException.ThrowIfLessThan(maxAttempts, 1);

        var currentAttempt = 0;
        while (true)
        {
            try
            {
                return await attempt(ct).ConfigureAwait(false);
            }
            catch (OperationCanceledException)
            {
                throw;
            }
            catch (Exception ex)
            {
                if (classify(ex) == StreamBusRetryKind.Fatal) throw;
                currentAttempt++;
                if (currentAttempt >= maxAttempts) throw;
                logger.LogWarning(ex, "{Operation} attempt {Attempt} failed; retrying.", operation, currentAttempt);
                await Task.Delay(StreamBusDelays.ExponentialCapped(currentAttempt, baseDelay, maxDelay), ct).ConfigureAwait(false);
            }
        }
    }
}
