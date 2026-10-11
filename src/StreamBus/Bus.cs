namespace StreamBus;

public interface IStreamBus
{
    string Vhost { get; }
    BusConnectionState State { get; }
    Task<BusReadiness> WaitUntilReadyAsync(ReadinessPolicy policy, CancellationToken ct = default);
    Task PublishAsync(string stream, ReadOnlyMemory<byte> body, PublishOptions? options = null, CancellationToken ct = default);
}

public interface IStreamBus<TBus> : IStreamBus
{
}

public sealed class DefaultBus;

internal sealed class StreamBus : IStreamBus
{
    private readonly StreamBusConnectionSupervisor _supervisor;
    private readonly StreamProducerRegistry _producers;
    public StreamBus(string vhost, StreamBusConnectionSupervisor supervisor, StreamProducerRegistry producers)
    {
        Vhost = vhost;
        _supervisor = supervisor;
        _producers = producers;
    }
    public string Vhost { get; }
    public BusConnectionState State => _supervisor.State;
    public Task<BusReadiness> WaitUntilReadyAsync(ReadinessPolicy policy, CancellationToken ct = default)
        => _supervisor.WaitUntilReadyAsync(policy, ct);
    public Task PublishAsync(string stream, ReadOnlyMemory<byte> body, PublishOptions? options = null, CancellationToken ct = default)
        => _producers.PublishAsync(stream, body, options ?? new PublishOptions(), ct);
    internal Task StartAsync(CancellationToken ct) => _supervisor.StartAsync(ct);
}

internal sealed class StreamBus<TBus> : IStreamBus<TBus>
{
    private readonly StreamBusConnectionSupervisor _supervisor;
    private readonly StreamProducerRegistry _producers;
    public StreamBus(string vhost, StreamBusConnectionSupervisor supervisor, StreamProducerRegistry producers)
    {
        Vhost = vhost;
        _supervisor = supervisor;
        _producers = producers;
    }
    public string Vhost { get; }
    public BusConnectionState State => _supervisor.State;
    public Task<BusReadiness> WaitUntilReadyAsync(ReadinessPolicy policy, CancellationToken ct = default)
        => _supervisor.WaitUntilReadyAsync(policy, ct);
    public Task PublishAsync(string stream, ReadOnlyMemory<byte> body, PublishOptions? options = null, CancellationToken ct = default)
        => _producers.PublishAsync(stream, body, options ?? new PublishOptions(), ct);
    internal Task StartAsync(CancellationToken ct) => _supervisor.StartAsync(ct);
}
