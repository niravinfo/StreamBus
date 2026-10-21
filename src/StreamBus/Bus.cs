namespace StreamBus;

public interface IStreamBus
{
    string Vhost { get; }
    BusConnectionState State { get; }
    Task<BusReadiness> WaitUntilReadyAsync(ReadinessPolicy policy, CancellationToken ct = default);
    Task PublishAsync<T>(T message, PublishOptions? options = null, CancellationToken ct = default);
    Task PublishAsync<T>(T message, CancellationToken ct);
    Task PublishAsync<T>(string stream, T message, PublishOptions? options = null, CancellationToken ct = default);
    Task PublishAsync<T>(string stream, T message, CancellationToken ct);
    Task PublishSuperAsync<T>(T message, string routingKey, PublishOptions? options = null, CancellationToken ct = default);
    Task PublishSuperAsync<T>(T message, string routingKey, CancellationToken ct);
    Task PublishSuperAsync<T>(string stream, T message, string routingKey, PublishOptions? options = null, CancellationToken ct = default);
    Task PublishSuperAsync<T>(string stream, T message, string routingKey, CancellationToken ct);
    Task PublishRawAsync(string stream, ReadOnlyMemory<byte> body, PublishOptions? options = null, CancellationToken ct = default);
    Task<int> RequeueErrorsAsync(string stream, string errorQueue, CancellationToken ct = default);
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
    Task PublishResultAsync(string targetStream, ReadOnlyMemory<byte> result, ulong sourceOffset, PublishResultOptions options, CancellationToken ct = default);
    /// <summary>
    /// Consumes <paramref name="sourceStream"/> without storing source offsets
    /// (FR-10.14) and publishes each handler result to <paramref name="targetStream"/>
    /// with publishing-id = source offset. The resume point is recomputed from the
    /// target's last publishing id on every attach; operator <c>SetConsumerOffset</c>
    /// calls on this reference do not affect the resume. The per-partition lag
    /// gauge is not meaningful on effectively-once references: the source never
    /// stores offsets (FR-10.14), so there is no stored offset to compare against.
    /// Run at most one effectively-once pipeline per (source, target) pair:
    /// producer names are scoped per source-partition/target, so two pipelines
    /// sharing a pair would publish under identical names from different
    /// single-active-consumer groups.
    /// </summary>
    Task<IStreamConsumer> ConsumeEffectivelyOnceAsync(string sourceStream, string targetStream, EffectivelyOnceOptions options, Func<ConsumeContext, Task<ReadOnlyMemory<byte>>> handler, CancellationToken ct = default);
    /// <summary>
    /// Declares streams and super streams idempotently (FR-7). Startup
    /// declarations configured via topology are declared automatically;
    /// these methods cover explicit/operator-driven declaration anytime.
    /// </summary>
    IStreamTopology Topology { get; }
}

public interface IStreamBus<TBus> : IStreamBus
{
}

public sealed class DefaultBus;

internal abstract class StreamBusCore<TBus> : IStreamBus
{
    private readonly StreamBusConnectionSupervisor _supervisor;
    private readonly StreamProducerRegistry _producers;
    private readonly StreamConsumerRegistry _consumers;
    private readonly IStreamTopology _topology;
    private readonly IStreamSerializer _serializer;
    private readonly StreamPublishRouter _router;
    private readonly ErrorQueueTransport _errorQueues;
    private readonly Func<string, DeclaredStream?> _findDeclaration;
    protected StreamBusCore(string vhost, StreamBusConnectionSupervisor supervisor, StreamProducerRegistry producers, StreamConsumerRegistry consumers, IStreamTopology topology, IStreamSerializer serializer, StreamPublishRouter router, ErrorQueueTransport errorQueues, Func<string, DeclaredStream?>? findDeclaration = null)
    {
        Vhost = vhost;
        _supervisor = supervisor;
        _producers = producers;
        _consumers = consumers;
        _topology = topology;
        _serializer = serializer ?? throw new ArgumentNullException(nameof(serializer));
        _router = router ?? throw new ArgumentNullException(nameof(router));
        _errorQueues = errorQueues ?? throw new ArgumentNullException(nameof(errorQueues));
        _findDeclaration = findDeclaration ?? (_ => null);
    }
    public string Vhost { get; }
    public IStreamTopology Topology => _topology;
    public BusConnectionState State => _supervisor.State;
    public Task<BusReadiness> WaitUntilReadyAsync(ReadinessPolicy policy, CancellationToken ct = default)
        => _supervisor.WaitUntilReadyAsync(policy, ct);
    public Task PublishAsync<T>(T message, PublishOptions? options = null, CancellationToken ct = default)
    {
        ArgumentNullException.ThrowIfNull(message);
        var reg = _router.Resolve(typeof(TBus), typeof(T));
        if (reg.IsSuper)
            throw new StreamBusConfigurationException($"Message {typeof(T).FullName} is registered with PublishSuper for stream '{reg.Stream}'. Use PublishSuperAsync (routing key required).");
        var body = _serializer.Serialize(message);
        var effective = (options ?? new PublishOptions()) with { IsSuperStream = false, RoutingKey = null };
        return _producers.PublishAsync(reg.Stream, body, effective, ct);
    }
    public Task PublishAsync<T>(T message, CancellationToken ct)
        => PublishAsync(message, null, ct);
    public Task PublishAsync<T>(string stream, T message, PublishOptions? options = null, CancellationToken ct = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(stream);
        ArgumentNullException.ThrowIfNull(message);
        DeclareSetBuilder.ValidateCallTarget(_findDeclaration(stream), stream, wantSuper: false);
        var body = _serializer.Serialize(message);
        var effective = (options ?? new PublishOptions()) with { IsSuperStream = false, RoutingKey = null };
        return _producers.PublishAsync(stream, body, effective, ct);
    }
    public Task PublishAsync<T>(string stream, T message, CancellationToken ct)
        => PublishAsync(stream, message, null, ct);
    public Task PublishSuperAsync<T>(T message, string routingKey, PublishOptions? options = null, CancellationToken ct = default)
    {
        ArgumentNullException.ThrowIfNull(message);
        ArgumentException.ThrowIfNullOrWhiteSpace(routingKey);
        var reg = _router.Resolve(typeof(TBus), typeof(T));
        if (!reg.IsSuper)
            throw new StreamBusConfigurationException($"Message {typeof(T).FullName} is registered with Publish for stream '{reg.Stream}'. Use PublishAsync (no routing key).");
        var body = _serializer.Serialize(message);
        var effective = (options ?? new PublishOptions()) with { IsSuperStream = true, RoutingKey = routingKey };
        return _producers.PublishAsync(reg.Stream, body, effective, ct);
    }
    public Task PublishSuperAsync<T>(T message, string routingKey, CancellationToken ct)
        => PublishSuperAsync(message, routingKey, null, ct);
    public Task PublishSuperAsync<T>(string stream, T message, string routingKey, PublishOptions? options = null, CancellationToken ct = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(stream);
        ArgumentNullException.ThrowIfNull(message);
        ArgumentException.ThrowIfNullOrWhiteSpace(routingKey);
        DeclareSetBuilder.ValidateCallTarget(_findDeclaration(stream), stream, wantSuper: true);
        var body = _serializer.Serialize(message);
        var effective = (options ?? new PublishOptions()) with { IsSuperStream = true, RoutingKey = routingKey };
        return _producers.PublishAsync(stream, body, effective, ct);
    }
    public Task PublishSuperAsync<T>(string stream, T message, string routingKey, CancellationToken ct)
        => PublishSuperAsync(stream, message, routingKey, null, ct);
    public Task PublishRawAsync(string stream, ReadOnlyMemory<byte> body, PublishOptions? options = null, CancellationToken ct = default)
        => _producers.PublishAsync(stream, body, options ?? new PublishOptions(), ct);
    public Task<int> RequeueErrorsAsync(string stream, string errorQueue, CancellationToken ct = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(stream);
        ArgumentException.ThrowIfNullOrWhiteSpace(errorQueue);
        return _errorQueues.DrainAsync(errorQueue, (envelope, ct2) =>
            _producers.PublishAsync(stream, envelope.Body, new PublishOptions(), ct2), ct);
    }
    public Task<IStreamConsumer> ConsumeAsync(string stream, ConsumeOptions options, Func<ConsumeContext, Task> handler, CancellationToken ct = default)
        => _consumers.ConsumeAsync(typeof(TBus), stream, options, handler, ct);
    public Task<ulong?> GetConsumerOffsetAsync(string stream, string reference, int partition, CancellationToken ct = default)
        => _consumers.GetOffsetAsync(stream, reference, partition, ct);
    public Task SetConsumerOffsetAsync(string stream, string reference, int partition, ulong offset, CancellationToken ct = default)
        => _consumers.SetOffsetAsync(stream, reference, partition, offset, ct);
    public Task<int> ReplayErrorStreamAsync(string errorStream, string? targetStream = null, CancellationToken ct = default)
        => _consumers.ReplayAsync(errorStream, targetStream, ct);
    public Task PublishResultAsync(string targetStream, ReadOnlyMemory<byte> result, ulong sourceOffset, PublishResultOptions options, CancellationToken ct = default)
        => _consumers.PublishResultAsync(targetStream, result, sourceOffset, options, ct);
    public Task<IStreamConsumer> ConsumeEffectivelyOnceAsync(string sourceStream, string targetStream, EffectivelyOnceOptions options, Func<ConsumeContext, Task<ReadOnlyMemory<byte>>> handler, CancellationToken ct = default)
        => _consumers.ConsumeEffectivelyOnceAsync(typeof(TBus), sourceStream, targetStream, options, handler, ct);
    internal Task StartAsync(CancellationToken ct) => _supervisor.StartAsync(ct);
}

internal sealed class StreamBus : StreamBusCore<DefaultBus>, IStreamBus
{
    public StreamBus(string vhost, StreamBusConnectionSupervisor supervisor, StreamProducerRegistry producers,
        StreamConsumerRegistry consumers, IStreamTopology topology, IStreamSerializer serializer,
        StreamPublishRouter router, ErrorQueueTransport errorQueues, Func<string, DeclaredStream?>? findDeclaration = null)
        : base(vhost, supervisor, producers, consumers, topology, serializer, router, errorQueues, findDeclaration) { }
}

internal sealed class StreamBus<TBus> : StreamBusCore<TBus>, IStreamBus<TBus>
{
    public StreamBus(string vhost, StreamBusConnectionSupervisor supervisor, StreamProducerRegistry producers,
        StreamConsumerRegistry consumers, IStreamTopology topology, IStreamSerializer serializer,
        StreamPublishRouter router, ErrorQueueTransport errorQueues, Func<string, DeclaredStream?>? findDeclaration = null)
        : base(vhost, supervisor, producers, consumers, topology, serializer, router, errorQueues, findDeclaration) { }
}
