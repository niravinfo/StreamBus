namespace StreamBus;

public enum BusReadiness
{
    Ready = 0,
    TimedOut = 1,
    Fatal = 2,
}

public enum BusConnectionState
{
    Starting = 0,
    Ready = 1,
    TransientFailure = 2,
    Fatal = 3,
}

public abstract record ReadinessPolicy
{
    private ReadinessPolicy() { }

    public sealed record FailFast : ReadinessPolicy;
    public sealed record WaitUpTo(TimeSpan Timeout) : ReadinessPolicy;
    public sealed record WaitIndefinitely : ReadinessPolicy;

    public static ReadinessPolicy FailFastPolicy { get; } = new FailFast();
    public static ReadinessPolicy Indefinitely { get; } = new WaitIndefinitely();
    public static ReadinessPolicy UpTo(TimeSpan timeout) => new WaitUpTo(timeout);
}
