namespace StreamBus;

public interface IStreamBus
{
    string Vhost { get; }
    BusConnectionState State { get; }
    Task<BusReadiness> WaitUntilReadyAsync(ReadinessPolicy policy, CancellationToken ct = default);
}

public interface IStreamBus<TBus> : IStreamBus
{
}

public sealed class DefaultBus;

internal sealed class StreamBus : IStreamBus
{
    private readonly StreamBusConnectionSupervisor _supervisor;
    public StreamBus(string vhost, StreamBusConnectionSupervisor supervisor)
    {
        Vhost = vhost;
        _supervisor = supervisor;
    }
    public string Vhost { get; }
    public BusConnectionState State => _supervisor.State;
    public Task<BusReadiness> WaitUntilReadyAsync(ReadinessPolicy policy, CancellationToken ct = default)
        => _supervisor.WaitUntilReadyAsync(policy, ct);
    internal Task StartAsync(CancellationToken ct) => _supervisor.StartAsync(ct);
}

internal sealed class StreamBus<TBus> : IStreamBus<TBus>
{
    private readonly StreamBusConnectionSupervisor _supervisor;
    public StreamBus(string vhost, StreamBusConnectionSupervisor supervisor)
    {
        Vhost = vhost;
        _supervisor = supervisor;
    }
    public string Vhost { get; }
    public BusConnectionState State => _supervisor.State;
    public Task<BusReadiness> WaitUntilReadyAsync(ReadinessPolicy policy, CancellationToken ct = default)
        => _supervisor.WaitUntilReadyAsync(policy, ct);
    internal Task StartAsync(CancellationToken ct) => _supervisor.StartAsync(ct);
}
