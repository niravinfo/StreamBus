namespace StreamBus;

public enum DeadLetterQueueType
{
    ClassicQueue = 1,
    QuorumQueue = 2,
}

/// <summary>
/// AMQP error-queue options. <see cref="ConfirmTimeout"/> is the single
/// confirmation budget for a dead-letter publish; it is independent of the
/// stream publish timeout because the two paths have different failure modes.
/// </summary>
public sealed record ErrorQueueOptions
{
    public TimeSpan ConfirmTimeout { get; init; } = TimeSpan.FromSeconds(10);
}

public interface IErrorQueueOptionsBuilder
{
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
    public required string Name { get; init; }
    public DeadLetterQueueType? QueueType { get; init; }
    public StreamOptions? Spec { get; init; }

    /// <summary>Options for a queue destination; ignored for stream destinations.</summary>
    public ErrorQueueOptions QueueOptions { get; init; } = new();

    public bool IsStream => QueueType is null;
}

public sealed record ExhaustedContext
{
    public required string Stream { get; init; }
    public required int Partition { get; init; }
    public required ulong Offset { get; init; }
    public required ReadOnlyMemory<byte> Body { get; init; }
    public required MessageMetadata Metadata { get; init; }
    public required int Attempts { get; init; }
    public required Exception Exception { get; init; }
    public required DateTimeOffset FailedAt { get; init; }
    public required IServiceProvider Services { get; init; }

    public T GetService<T>() where T : notnull
        => (T)(Services.GetService(typeof(T))
            ?? throw new StreamBusConfigurationException($"ExhaustedContext has no service of type {typeof(T).FullName} registered."));
}

public interface IRetryConfigurator
{
    void Immediate(int retries);
    void Interval(int retries, TimeSpan delay);
    void Intervals(params TimeSpan[] delays);
    void Exponential(int retryCount, TimeSpan baseDelay, TimeSpan maxDelay, double multiplier);
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
                throw new StreamBusConfigurationException($"Bus {busName}: endpoint '{stream}' sets a dead-letter destination or OnExhausted callback with an unbounded retry schedule. Handoff needs a bound: configure UseMessageRetry with Immediate, Interval, Intervals, Exponential, or None.");
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
