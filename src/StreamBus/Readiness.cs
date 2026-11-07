namespace StreamBus;

/// <summary>
/// Outcome of a readiness wait (<see cref="IStreamBus.WaitUntilReadyAsync"/>):
/// the waiting policy either saw the connection become ready, gave up on its
/// budget, or observed a fatal connection state.
/// </summary>
public enum BusReadiness
{
    /// <summary>The bus connection is ready for the operation.</summary>
    Ready = 0,

    /// <summary>The wait budget expired (fail-fast or bounded wait) without
    /// reaching Ready; the operation was not attempted. The connection
    /// keeps retrying in the background.</summary>
    TimedOut = 1,

    /// <summary>The connection is in a fatal state; the operation is refused
    /// until reconnect or operator action.</summary>
    Fatal = 2,
}

/// <summary>
/// Latched connection state of a bus, reported by the supervisor and
/// surfaced through health checks.
/// </summary>
public enum BusConnectionState
{
    /// <summary>Initial state while the first connection attempt is in flight.</summary>
    Starting = 0,

    /// <summary>Connected and ready for work.</summary>
    Ready = 1,

    /// <summary>A transient failure occurred; reconnecting with backoff.</summary>
    TransientFailure = 2,

    /// <summary>An unrecoverable error (e.g. protocol/SSL/auth failure);
    /// no automatic retry.</summary>
    Fatal = 3,
}

/// <summary>
/// How a publish, consume, or readiness call waits for the connection to
/// become ready. Selectable per call site; only the readiness wait is
/// bounded by this policy — the publish confirm timeout is separate.
/// </summary>
public abstract record ReadinessPolicy
{
    private ReadinessPolicy() { }

    /// <summary>Do not wait: return immediately, yielding
    /// <see cref="BusReadiness.TimedOut"/> when the connection is not
    /// already ready.</summary>
    public sealed record FailFast : ReadinessPolicy;

    /// <summary>Wait for readiness for at most <see cref="Timeout"/>,
    /// yielding <see cref="BusReadiness.TimedOut"/> when the budget
    /// expires.</summary>
    /// <param name="Timeout">Maximum time to wait for readiness.</param>
    public sealed record WaitUpTo(TimeSpan Timeout) : ReadinessPolicy;

    /// <summary>Wait until the connection becomes ready, with no time
    /// bound; only cancellation stops the wait.</summary>
    public sealed record WaitIndefinitely : ReadinessPolicy;

    /// <summary>Shared <see cref="FailFast"/> instance, the default policy.</summary>
    public static ReadinessPolicy FailFastPolicy { get; } = new FailFast();

    /// <summary>Shared <see cref="WaitIndefinitely"/> instance.</summary>
    public static ReadinessPolicy Indefinitely { get; } = new WaitIndefinitely();

    /// <summary>Creates a bounded wait policy.</summary>
    /// <param name="timeout">Maximum time to wait for readiness.</param>
    /// <returns>A policy that waits at most <paramref name="timeout"/>.</returns>
    public static ReadinessPolicy UpTo(TimeSpan timeout) => new WaitUpTo(timeout);
}
