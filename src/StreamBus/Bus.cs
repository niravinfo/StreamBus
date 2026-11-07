using System.Diagnostics;
using Microsoft.Extensions.DependencyInjection;

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
    private readonly PublishPipelineCache _publishPipelines;
    private readonly IServiceScopeFactory? _publishScopeFactory;
    protected StreamBusCore(string vhost, StreamBusConnectionSupervisor supervisor, StreamProducerRegistry producers, StreamConsumerRegistry consumers, IStreamTopology topology, IStreamSerializer serializer, StreamPublishRouter router, ErrorQueueTransport errorQueues, Func<string, DeclaredStream?>? findDeclaration = null, PublishPipelineCache? publishPipelines = null, IServiceScopeFactory? publishScopeFactory = null)
    {
        Vhost = vhost;
        _supervisor = supervisor;
        _producers = producers;
        _consumers = consumers;
        _topology = topology;
        _serializer = serializer ?? throw new ArgumentNullException(nameof(serializer));
        if (string.IsNullOrWhiteSpace(_serializer.ContentType))
            throw new StreamBusConfigurationException(
                "The configured IStreamSerializer returned a null or blank ContentType; every serialized publish needs a media type.");
        _router = router ?? throw new ArgumentNullException(nameof(router));
        _errorQueues = errorQueues ?? throw new ArgumentNullException(nameof(errorQueues));
        _findDeclaration = findDeclaration ?? (_ => null);
        _publishPipelines = publishPipelines ?? new PublishPipelineCache(
            typeof(TBus),
            Array.Empty<PublishFilterRegistration>(),
            null);
        _publishScopeFactory = publishScopeFactory;
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
        var effective = (options ?? new PublishOptions()) with { IsSuperStream = false, RoutingKey = null };
        return PublishThroughPipelineAsync(reg.Stream, routingKey: null, message, effective, ct);
    }
    public Task PublishAsync<T>(T message, CancellationToken ct)
        => PublishAsync(message, null, ct);
    public Task PublishAsync<T>(string stream, T message, PublishOptions? options = null, CancellationToken ct = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(stream);
        ArgumentNullException.ThrowIfNull(message);
        DeclareSetBuilder.ValidateCallTarget(_findDeclaration(stream), stream, wantSuper: false);
        var effective = (options ?? new PublishOptions()) with { IsSuperStream = false, RoutingKey = null };
        return PublishThroughPipelineAsync(stream, routingKey: null, message, effective, ct);
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
        var effective = (options ?? new PublishOptions()) with { IsSuperStream = true, RoutingKey = routingKey };
        return PublishThroughPipelineAsync(reg.Stream, routingKey, message, effective, ct);
    }
    public Task PublishSuperAsync<T>(T message, string routingKey, CancellationToken ct)
        => PublishSuperAsync(message, routingKey, null, ct);
    public Task PublishSuperAsync<T>(string stream, T message, string routingKey, PublishOptions? options = null, CancellationToken ct = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(stream);
        ArgumentNullException.ThrowIfNull(message);
        ArgumentException.ThrowIfNullOrWhiteSpace(routingKey);
        DeclareSetBuilder.ValidateCallTarget(_findDeclaration(stream), stream, wantSuper: true);
        var effective = (options ?? new PublishOptions()) with { IsSuperStream = true, RoutingKey = routingKey };
        return PublishThroughPipelineAsync(stream, routingKey, message, effective, ct);
    }
    public Task PublishSuperAsync<T>(string stream, T message, string routingKey, CancellationToken ct)
        => PublishSuperAsync(stream, message, routingKey, null, ct);
    private async Task PublishThroughPipelineAsync<T>(
        string stream,
        string? routingKey,
        T message,
        PublishOptions effective,
        CancellationToken ct)
    {
        using var activity = StreamBusActivity.StartPublish(stream, routingKey);
        StreamMessageProperties.ValidateHeaders(effective.Properties?.Headers);
        var seed = effective.Properties ?? new StreamMessageProperties();
        var messageId = seed.MessageId ?? Guid.NewGuid();
        var sentTime = StreamWire.NormalizeSentTime(seed.SentTime ?? DateTime.UtcNow);
        effective = effective with { Properties = seed with { MessageId = messageId, SentTime = sentTime } };

        var pipeline = _publishPipelines.GetOrCreate(typeof(T));
        try
        {
            if (pipeline.Count == 0)
            {
                await SendTypedAsync(stream, message, effective, ct, activity).ConfigureAwait(false);
                return;
            }

            var scopeFactory = _publishScopeFactory
                ?? throw new StreamBusConfigurationException(
                    "Publish filters are configured, but no IServiceScopeFactory is available (internal error).");
            await using var publishScope = scopeFactory.CreateAsyncScope();
            var headers = new StreamSendHeaders(effective.Properties?.Headers, _serializer);
            var context = new PublishContext<T>
            {
                Stream = stream,
                RoutingKey = routingKey,
                Payload = message!,
                MessageId = messageId,
                CorrelationId = seed.CorrelationId,
                SentTime = sentTime,
                Headers = headers,
                Services = publishScope.ServiceProvider,
                CancellationToken = ct,
            };

            await pipeline.RunAsync(
                context,
                () => SendTypedAsync(
                    stream,
                    message,
                    effective with { Properties = headers.Materialize(effective.Properties) },
                    ct,
                    activity)).ConfigureAwait(false);
        }
        catch (OperationCanceledException)
        {
            // Never wrap cancellation (caller- or foreign-token) as a failure.
            throw;
        }
        catch (Exception ex)
        {
            // The transport layer already classifies genuine transport failures as
            // StreamBusException; anything reaching here is filter or pipeline code
            // failing deterministically and surfaces unwrapped so callers do not
            // retry-burn on their own bugs. Both the fast path and the filtered
            // path record the failure identically on the publish activity.
            StreamBusActivity.RecordException(activity, ex);
            throw;
        }
    }

    private async Task SendTypedAsync<T>(
        string stream,
        T message,
        PublishOptions effective,
        CancellationToken ct,
        Activity? activity = null)
    {
        var body = _serializer.Serialize(message);
        StreamBusActivity.SetSize(activity, body.Length);
        var properties = (effective.Properties ?? new StreamMessageProperties()) with { ContentType = _serializer.ContentType };
        await _producers.PublishAsync(stream, body, effective with { Properties = properties }, ct).ConfigureAwait(false);
    }

    public async Task PublishRawAsync(string stream, ReadOnlyMemory<byte> body, PublishOptions? options = null, CancellationToken ct = default)
    {
        using var activity = StreamBusActivity.StartPublish(stream, options?.RoutingKey);
        StreamMessageProperties.ValidateHeaders(options?.Properties?.Headers);
        StreamBusActivity.SetSize(activity, body.Length);
        await _producers.PublishAsync(stream, body, options ?? new PublishOptions(), ct).ConfigureAwait(false);
    }
    public Task<int> RequeueErrorsAsync(string stream, string errorQueue, CancellationToken ct = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(stream);
        ArgumentException.ThrowIfNullOrWhiteSpace(errorQueue);
        return _errorQueues.DrainAsync(errorQueue, (envelope, ct2) =>
            _producers.PublishAsync(stream, envelope.Body, envelope.ToReplayOptions(), ct2), ct);
    }
    public Task<IStreamConsumer> ConsumeAsync(string stream, ConsumeOptions options, Func<ConsumeContext, Task> handler, CancellationToken ct = default)
        => _consumers.ConsumeAsync(typeof(TBus), stream, options, handler, ct, serializer: _serializer);
    public Task<ulong?> GetConsumerOffsetAsync(string stream, string reference, int partition, CancellationToken ct = default)
        => _consumers.GetOffsetAsync(stream, reference, partition, ct);
    public Task SetConsumerOffsetAsync(string stream, string reference, int partition, ulong offset, CancellationToken ct = default)
        => _consumers.SetOffsetAsync(stream, reference, partition, offset, ct);
    public Task<int> ReplayErrorStreamAsync(string errorStream, string? targetStream = null, CancellationToken ct = default)
        => _consumers.ReplayAsync(errorStream, targetStream, ct);
    internal Task StartAsync(CancellationToken ct) => _supervisor.StartAsync(ct);
}

internal sealed class StreamBus : StreamBusCore<DefaultBus>, IStreamBus
{
    public StreamBus(string vhost, StreamBusConnectionSupervisor supervisor, StreamProducerRegistry producers,
        StreamConsumerRegistry consumers, IStreamTopology topology, IStreamSerializer serializer,
        StreamPublishRouter router, ErrorQueueTransport errorQueues, Func<string, DeclaredStream?>? findDeclaration = null,
        PublishPipelineCache? publishPipelines = null, IServiceScopeFactory? publishScopeFactory = null)
        : base(vhost, supervisor, producers, consumers, topology, serializer, router, errorQueues, findDeclaration, publishPipelines, publishScopeFactory) { }
}

internal sealed class StreamBus<TBus> : StreamBusCore<TBus>, IStreamBus<TBus>
{
    public StreamBus(string vhost, StreamBusConnectionSupervisor supervisor, StreamProducerRegistry producers,
        StreamConsumerRegistry consumers, IStreamTopology topology, IStreamSerializer serializer,
        StreamPublishRouter router, ErrorQueueTransport errorQueues, Func<string, DeclaredStream?>? findDeclaration = null,
        PublishPipelineCache? publishPipelines = null, IServiceScopeFactory? publishScopeFactory = null)
        : base(vhost, supervisor, producers, consumers, topology, serializer, router, errorQueues, findDeclaration, publishPipelines, publishScopeFactory) { }
}
