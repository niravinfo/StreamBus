namespace StreamBus;

public interface IStreamBus
{
    string Vhost { get; }
    BusConnectionState State { get; }
    Task<BusReadiness> WaitUntilReadyAsync(ReadinessPolicy policy, CancellationToken ct = default);
    Task PublishAsync(string stream, ReadOnlyMemory<byte> body, PublishOptions? options = null, CancellationToken ct = default);
    Task<IStreamConsumer> ConsumeAsync(string stream, ConsumeOptions options, Func<ConsumeContext, Task> handler, CancellationToken ct = default);
    Task<ulong?> GetConsumerOffsetAsync(string stream, string reference, int partition, CancellationToken ct = default);
    Task SetConsumerOffsetAsync(string stream, string reference, int partition, ulong offset, CancellationToken ct = default);
    Task<int> ReplayErrorStreamAsync(string errorStream, string? targetStream = null, CancellationToken ct = default);
    /// <summary>
    /// Publishes a processing result with publishing-id = <paramref name="sourceOffset"/>
    /// under <c>options.ProducerName</c>, retrying until confirmed or cancelled. There is
    /// deliberately no confirm timeout: a result is never skipped silently (FR-6.5).
    /// Concurrent use of one producer name is forbidden (FR-6.3) — the managed
    /// <see cref="ConsumeEffectivelyOnceAsync"/> path serializes per name; manual
    /// callers must do the same. A manual publish combined with a storing consumer
    /// is at-least-once; effectively-once holds for the managed cycle only.
    /// </summary>
    Task PublishResultAsync(string targetStream, ReadOnlyMemory<byte> result, ulong sourceOffset, PublishResultOptions? options = null, CancellationToken ct = default);
    /// <summary>
    /// Consumes <paramref name="sourceStream"/> without storing source offsets
    /// (FR-10.14) and publishes each handler result to <paramref name="targetStream"/>
    /// with publishing-id = source offset. The resume point is recomputed from the
    /// target's last publishing id on every attach; operator <c>SetConsumerOffset</c>
    /// calls on this reference do not affect the resume.
    /// </summary>
    Task<IStreamConsumer> ConsumeEffectivelyOnceAsync(string sourceStream, string targetStream, EffectivelyOnceOptions options, Func<ConsumeContext, Task<ReadOnlyMemory<byte>>> handler, CancellationToken ct = default);
}

public interface IStreamBus<TBus> : IStreamBus
{
}

public sealed class DefaultBus;

internal sealed class StreamBus : IStreamBus
{
    private readonly StreamBusConnectionSupervisor _supervisor;
    private readonly StreamProducerRegistry _producers;
    private readonly StreamConsumerRegistry _consumers;
    public StreamBus(string vhost, StreamBusConnectionSupervisor supervisor, StreamProducerRegistry producers, StreamConsumerRegistry consumers)
    {
        Vhost = vhost;
        _supervisor = supervisor;
        _producers = producers;
        _consumers = consumers;
    }
    public string Vhost { get; }
    public BusConnectionState State => _supervisor.State;
    public Task<BusReadiness> WaitUntilReadyAsync(ReadinessPolicy policy, CancellationToken ct = default)
        => _supervisor.WaitUntilReadyAsync(policy, ct);
    public Task PublishAsync(string stream, ReadOnlyMemory<byte> body, PublishOptions? options = null, CancellationToken ct = default)
        => _producers.PublishAsync(stream, body, options ?? new PublishOptions(), ct);
    public Task<IStreamConsumer> ConsumeAsync(string stream, ConsumeOptions options, Func<ConsumeContext, Task> handler, CancellationToken ct = default)
        => _consumers.ConsumeAsync(typeof(DefaultBus), stream, options, handler, ct);
    public Task<ulong?> GetConsumerOffsetAsync(string stream, string reference, int partition, CancellationToken ct = default)
        => _consumers.GetOffsetAsync(stream, reference, partition, ct);
    public Task SetConsumerOffsetAsync(string stream, string reference, int partition, ulong offset, CancellationToken ct = default)
        => _consumers.SetOffsetAsync(stream, reference, partition, offset, ct);
    public Task<int> ReplayErrorStreamAsync(string errorStream, string? targetStream = null, CancellationToken ct = default)
        => _consumers.ReplayAsync(errorStream, targetStream, ct);
    public Task PublishResultAsync(string targetStream, ReadOnlyMemory<byte> result, ulong sourceOffset, PublishResultOptions? options = null, CancellationToken ct = default)
        => _consumers.PublishResultAsync(targetStream, result, sourceOffset, options, ct);
    public Task<IStreamConsumer> ConsumeEffectivelyOnceAsync(string sourceStream, string targetStream, EffectivelyOnceOptions options, Func<ConsumeContext, Task<ReadOnlyMemory<byte>>> handler, CancellationToken ct = default)
        => _consumers.ConsumeEffectivelyOnceAsync(typeof(DefaultBus), sourceStream, targetStream, options, handler, ct);
    internal Task StartAsync(CancellationToken ct) => _supervisor.StartAsync(ct);
}

internal sealed class StreamBus<TBus> : IStreamBus<TBus>
{
    private readonly StreamBusConnectionSupervisor _supervisor;
    private readonly StreamProducerRegistry _producers;
    private readonly StreamConsumerRegistry _consumers;
    public StreamBus(string vhost, StreamBusConnectionSupervisor supervisor, StreamProducerRegistry producers, StreamConsumerRegistry consumers)
    {
        Vhost = vhost;
        _supervisor = supervisor;
        _producers = producers;
        _consumers = consumers;
    }
    public string Vhost { get; }
    public BusConnectionState State => _supervisor.State;
    public Task<BusReadiness> WaitUntilReadyAsync(ReadinessPolicy policy, CancellationToken ct = default)
        => _supervisor.WaitUntilReadyAsync(policy, ct);
    public Task PublishAsync(string stream, ReadOnlyMemory<byte> body, PublishOptions? options = null, CancellationToken ct = default)
        => _producers.PublishAsync(stream, body, options ?? new PublishOptions(), ct);
    public Task<IStreamConsumer> ConsumeAsync(string stream, ConsumeOptions options, Func<ConsumeContext, Task> handler, CancellationToken ct = default)
        => _consumers.ConsumeAsync(typeof(TBus), stream, options, handler, ct);
    public Task<ulong?> GetConsumerOffsetAsync(string stream, string reference, int partition, CancellationToken ct = default)
        => _consumers.GetOffsetAsync(stream, reference, partition, ct);
    public Task SetConsumerOffsetAsync(string stream, string reference, int partition, ulong offset, CancellationToken ct = default)
        => _consumers.SetOffsetAsync(stream, reference, partition, offset, ct);
    public Task<int> ReplayErrorStreamAsync(string errorStream, string? targetStream = null, CancellationToken ct = default)
        => _consumers.ReplayAsync(errorStream, targetStream, ct);
    public Task PublishResultAsync(string targetStream, ReadOnlyMemory<byte> result, ulong sourceOffset, PublishResultOptions? options = null, CancellationToken ct = default)
        => _consumers.PublishResultAsync(targetStream, result, sourceOffset, options, ct);
    public Task<IStreamConsumer> ConsumeEffectivelyOnceAsync(string sourceStream, string targetStream, EffectivelyOnceOptions options, Func<ConsumeContext, Task<ReadOnlyMemory<byte>>> handler, CancellationToken ct = default)
        => _consumers.ConsumeEffectivelyOnceAsync(typeof(TBus), sourceStream, targetStream, options, handler, ct);
    internal Task StartAsync(CancellationToken ct) => _supervisor.StartAsync(ct);
}
