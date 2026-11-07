using Microsoft.Extensions.DependencyInjection;

namespace StreamBus;

/// <summary>AMQP queue type for a dead-letter queue destination.</summary>
public enum DeadLetterQueueType
{
    /// <summary>A durable classic AMQP queue.</summary>
    ClassicQueue = 1,

    /// <summary>A quorum AMQP queue.</summary>
    QuorumQueue = 2,
}

/// <summary>
/// AMQP error-queue options. <see cref="ConfirmTimeout"/> is the single
/// confirmation budget for a dead-letter publish; it is independent of the
/// stream publish timeout because the two paths have different failure modes.
/// </summary>
public sealed record ErrorQueueOptions
{
    /// <summary>Confirmation budget for a dead-letter publish when the queue
    /// is confirmed by the broker; independent of the stream publish timeout.</summary>
    public TimeSpan ConfirmTimeout { get; init; } = TimeSpan.FromSeconds(10);
}

/// <summary>Builds AMQP error-queue options inside a dead-letter-to-queue declaration.</summary>
public interface IErrorQueueOptionsBuilder
{
    /// <summary>Sets the broker-confirmation budget for a dead-letter publish.</summary>
    /// <param name="timeout">The confirmation timeout; must be positive.</param>
    /// <exception cref="StreamBusConfigurationException"><paramref name="timeout"/> is not positive.</exception>
    void ConfirmTimeout(TimeSpan timeout);
}

internal sealed class ErrorQueueOptionsBuilder : IErrorQueueOptionsBuilder
{
    private TimeSpan _confirmTimeout = TimeSpan.FromSeconds(10);

    public void ConfirmTimeout(TimeSpan timeout)
    {
        if (timeout <= TimeSpan.Zero)
            throw new StreamBusConfigurationException("ErrorQueue.ConfirmTimeout must be positive.");
        _confirmTimeout = timeout;
    }

    public ErrorQueueOptions Build() => new() { ConfirmTimeout = _confirmTimeout };
}

/// <summary>
/// Kind-based dead-letter destination. A stream destination sets
/// <see cref="Spec"/> (queue type unset); a queue destination sets
/// <see cref="QueueType"/> (stream spec unset). Setting both is
/// unrepresentable by construction — <see cref="IsStream"/> is
/// <see cref="QueueType"/> being null.
/// </summary>
public sealed record DeadLetterOptions
{
    /// <summary>Dead-letter destination name: a stream or queue name.</summary>
    public required string Name { get; init; }

    /// <summary>Queue type when the destination is a queue; null for a stream destination.</summary>
    public DeadLetterQueueType? QueueType { get; init; }

    /// <summary>Stream declaration spec when the destination is a stream; null for a queue destination.</summary>
    public StreamOptions? Spec { get; init; }

    /// <summary>Options for a queue destination; ignored for stream destinations.</summary>
    public ErrorQueueOptions QueueOptions { get; init; } = new();

    /// <summary><see langword="true"/> when the destination is a stream (no queue type set).</summary>
    public bool IsStream => QueueType is null;
}

/// <summary>
/// The failure handed to an <c>OnExhausted</c> callback after the retry
/// schedule is exhausted. Returning from the callback advances past the
/// message; throwing recycles the attach for redelivery.
/// </summary>
public sealed record ExhaustedContext
{
    /// <summary>Source stream name.</summary>
    public required string Stream { get; init; }

    /// <summary>Source partition index.</summary>
    public required int Partition { get; init; }

    /// <summary>Source offset for this delivery.</summary>
    public required ulong Offset { get; init; }

    /// <summary>Serialized message body.</summary>
    public required ReadOnlyMemory<byte> Body { get; init; }

    /// <summary>AMQP <c>message-id</c>; null when absent or not a Guid.</summary>
    public Guid? MessageId { get; init; }

    /// <summary>AMQP <c>correlation-id</c>; null when absent.</summary>
    public Guid? CorrelationId { get; init; }

    /// <summary>AMQP <c>creation-time</c> of the original message, UTC.</summary>
    public DateTime? SentTime { get; init; }

    /// <summary>Application properties of the failed delivery.</summary>
    public required IStreamHeaders Headers { get; init; }

    /// <summary>Total delivery attempts, including the first.</summary>
    public required int Attempts { get; init; }

    /// <summary>The exception that exhausted the schedule.</summary>
    public required Exception Exception { get; init; }

    /// <summary>When the schedule was exhausted, UTC.</summary>
    public required DateTimeOffset FailedAt { get; init; }

    /// <summary>Per-delivery service scope shared with the typed consumer.</summary>
    public required IServiceProvider Services { get; init; }

    /// <summary>
    /// Resolves a required service from the exhaustion scope. A missing
    /// registration is a runtime DI miss, not invalid bus configuration, so
    /// this surfaces the standard <see cref="InvalidOperationException"/>
    /// from <see cref="ServiceProviderServiceExtensions.GetRequiredService{T}(IServiceProvider)"/>.
    /// </summary>
    /// <exception cref="InvalidOperationException">No service of type <typeparamref name="T"/> is registered.</exception>
    public T GetService<T>() where T : notnull
        => Services.GetRequiredService<T>();
}

/// <summary>
/// Configures a retry schedule. A schedule sets a total attempt limit
/// (<see cref="RetryPolicy.MaxAttempts"/> includes the first attempt), which a
/// dead-letter destination or <c>OnExhausted</c> callback requires; without
/// one the endpoint retries forever. Schedule methods overwrite each other;
/// the last call wins.
/// </summary>
public interface IRetryConfigurator
{
    /// <summary>Retries <paramref name="retries"/> times with no delay between attempts.</summary>
    /// <param name="retries">Retry count after the first attempt; zero allows no retries.</param>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="retries"/> is negative.</exception>
    void Immediate(int retries);

    /// <summary>Retries <paramref name="retries"/> times, waiting <paramref name="delay"/> between attempts.</summary>
    /// <param name="retries">Retry count after the first attempt; zero allows no retries.</param>
    /// <param name="delay">Fixed delay between attempts; must be non-negative.</param>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="retries"/> is negative.</exception>
    /// <exception cref="StreamBusConfigurationException"><paramref name="delay"/> is negative.</exception>
    void Interval(int retries, TimeSpan delay);

    /// <summary>Retries once per delay in <paramref name="delays"/>, waiting
    /// <paramref name="delays"/>[i] before retry i + 1.</summary>
    /// <param name="delays">Delay before each retry, in attempt order; at least one, each non-negative.</param>
    /// <exception cref="ArgumentNullException"><paramref name="delays"/> is null.</exception>
    /// <exception cref="StreamBusConfigurationException"><paramref name="delays"/> is empty or contains a negative value.</exception>
    void Intervals(params TimeSpan[] delays);

    /// <summary>
    /// Retries with exponentially growing delays:
    /// <paramref name="baseDelay"/> × <paramref name="multiplier"/>^(attempt − 1),
    /// capped at <paramref name="maxDelay"/>, without jitter.
    /// </summary>
    /// <param name="retryCount">Retry count after the first attempt; zero allows no retries.</param>
    /// <param name="baseDelay">Delay before the first retry; must be positive.</param>
    /// <param name="maxDelay">Upper bound for every delay; must be positive.</param>
    /// <param name="multiplier">Per-attempt growth factor; a finite number ≥ 1.</param>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="retryCount"/> is negative.</exception>
    /// <exception cref="StreamBusConfigurationException"><paramref name="baseDelay"/>, <paramref name="maxDelay"/>, or <paramref name="multiplier"/> is out of range.</exception>
    void Exponential(int retryCount, TimeSpan baseDelay, TimeSpan maxDelay, double multiplier);

    /// <summary>Disables retry: the first failure exhausts the schedule (a single attempt).</summary>
    void None();

    internal RetryPolicy BuildPolicy();
}

internal sealed class RetryConfigurator : IRetryConfigurator
{
    private RetryPolicy? _pending;

    public void Immediate(int retries)
    {
        ArgumentOutOfRangeException.ThrowIfNegative(retries);
        _pending = new RetryPolicy
        {
            MaxAttempts = retries + 1,
            DelayCalculator = static (_, _) => TimeSpan.Zero,
        };
    }

    public void Interval(int retries, TimeSpan delay)
    {
        ArgumentOutOfRangeException.ThrowIfNegative(retries);
        if (delay < TimeSpan.Zero)
            throw new StreamBusConfigurationException("Interval delay must be non-negative.");
        _pending = new RetryPolicy
        {
            MaxAttempts = retries + 1,
            DelayCalculator = (_, _) => delay,
        };
    }

    public void Intervals(params TimeSpan[] delays)
    {
        ArgumentNullException.ThrowIfNull(delays);
        if (delays.Length == 0)
            throw new StreamBusConfigurationException("Intervals requires at least one delay.");
        if (Array.Exists(delays, static d => d < TimeSpan.Zero))
            throw new StreamBusConfigurationException("Intervals delays must be non-negative.");
        var table = delays.ToArray();
        _pending = new RetryPolicy
        {
            MaxAttempts = table.Length + 1,
            DelayCalculator = (attempt, _) => table[Math.Min(attempt - 1, table.Length - 1)],
        };
    }

    public void Exponential(int retryCount, TimeSpan baseDelay, TimeSpan maxDelay, double multiplier)
    {
        ArgumentOutOfRangeException.ThrowIfNegative(retryCount);
        if (baseDelay <= TimeSpan.Zero)
            throw new StreamBusConfigurationException("Exponential baseDelay must be positive.");
        if (maxDelay <= TimeSpan.Zero)
            throw new StreamBusConfigurationException("Exponential maxDelay must be positive.");
        if (multiplier < 1.0 || !double.IsFinite(multiplier))
            throw new StreamBusConfigurationException("Exponential multiplier must be a finite number >= 1.");
        _pending = new RetryPolicy
        {
            MaxAttempts = retryCount + 1,
            BaseDelay = baseDelay,
            MaxDelay = maxDelay,
            Multiplier = multiplier,
            MaxJitter = TimeSpan.Zero,
        };
    }

    public void None()
    {
        _pending = new RetryPolicy
        {
            MaxAttempts = 1,
            DelayCalculator = static (_, _) => TimeSpan.Zero,
        };
    }

    RetryPolicy IRetryConfigurator.BuildPolicy()
        => _pending ?? throw new StreamBusConfigurationException("UseMessageRetry requires a schedule: Immediate, Interval, Intervals, Exponential, or None.");
}

internal enum ExhaustionKind
{
    StrictForever,
    StreamHandoff,
    QueueHandoff,
    Callback,
}

internal sealed record DerivedExhaustion(
    RetryPolicy Policy,
    ExhaustionKind Kind,
    ErrorStreamOptions? Stream,
    DeadLetterOptions? Queue,
    bool DestinationShadowed);

internal static class RetryDerivation
{
    public static DerivedExhaustion Derive(
        RetryPolicy? global,
        RetryPolicy? endpoint,
        DeadLetterOptions? destination,
        bool hasCallback,
        string stream,
        string busName)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(stream);
        ArgumentException.ThrowIfNullOrWhiteSpace(busName);
        var effective = endpoint ?? global ?? new RetryPolicy();
        var bounded = effective.MaxAttempts.HasValue;
        if (!bounded)
        {
            if (destination is not null || hasCallback)
                throw new StreamBusConfigurationException($"Bus {busName}: endpoint '{stream}' sets a dead-letter destination or OnExhausted callback with a retry schedule that has no attempt limit. A hand-off needs an attempt limit: configure UseMessageRetry with Immediate, Interval, Intervals, Exponential, or None.");
            return new DerivedExhaustion(effective, ExhaustionKind.StrictForever, null, null, false);
        }
        if (hasCallback)
            return new DerivedExhaustion(effective, ExhaustionKind.Callback, null, null, destination is not null);
        if (destination is not null)
        {
            if (destination.IsStream)
                return new DerivedExhaustion(effective, ExhaustionKind.StreamHandoff, new ErrorStreamOptions { Stream = destination.Name }, null, false);
            return new DerivedExhaustion(effective, ExhaustionKind.QueueHandoff, null, destination, false);
        }
        return new DerivedExhaustion(effective, ExhaustionKind.QueueHandoff, null, new DeadLetterOptions { Name = $"{stream}_error", QueueType = DeadLetterQueueType.ClassicQueue }, false);
    }
}

internal static class ExhaustionCallbackRunner
{
    public static async Task<bool> RunAsync(
        Func<ExhaustedContext, CancellationToken, Task> callback,
        ExhaustedContext context,
        Func<Task> commitStore,
        Action<Exception> recycle,
        CancellationToken ct)
    {
        ArgumentNullException.ThrowIfNull(callback);
        ArgumentNullException.ThrowIfNull(context);
        ArgumentNullException.ThrowIfNull(commitStore);
        ArgumentNullException.ThrowIfNull(recycle);
        try
        {
            await callback(context, ct).ConfigureAwait(false);
        }
        catch (OperationCanceledException) when (ct.IsCancellationRequested)
        {
            throw;
        }
        catch (Exception ex)
        {
            recycle(ex);
            return false;
        }
        await commitStore().ConfigureAwait(false);
        return true;
    }
}
