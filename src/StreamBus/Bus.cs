using System.Diagnostics;
using Microsoft.Extensions.DependencyInjection;

namespace StreamBus;

/// <summary>
/// Publish/consume surface for one bus. A message type resolves its target
/// stream from the publish registration unless an explicit stream name
/// overrides it; wrong-kind calls (plain vs super) throw
/// <see cref="StreamBusConfigurationException"/> naming the method to use.
/// </summary>
public interface IStreamBus
{
    /// <summary>Virtual host this bus is registered against.</summary>
    string Vhost { get; }

    /// <summary>Live connection state of the underlying supervisor.</summary>
    BusConnectionState State { get; }

    /// <summary>Waits until the connection is ready, per <paramref name="policy"/>.</summary>
    /// <param name="policy">How long/whether to wait (fail fast, bounded, or forever).</param>
    /// <param name="cancellationToken">Cancels the wait.</param>
    /// <returns>The outcome: <see cref="BusReadiness.Ready"/>, <see cref="BusReadiness.TimedOut"/>, or <see cref="BusReadiness.Fatal"/>.</returns>
    Task<BusReadiness> WaitUntilReadyAsync(ReadinessPolicy policy, CancellationToken cancellationToken = default);

    /// <summary>Publishes to the stream registered for <typeparamref name="T"/>, with options.</summary>
    /// <typeparam name="T">Registered message type; resolves the target stream.</typeparam>
    /// <param name="message">The message to serialize and send.</param>
    /// <param name="options">Optional per-call publish options.</param>
    /// <param name="cancellationToken">Cancels the publish.</param>
    Task PublishAsync<T>(T message, PublishOptions? options = null, CancellationToken cancellationToken = default);

    /// <summary>Publishes to the stream registered for <typeparamref name="T"/>.</summary>
    /// <typeparam name="T">Registered message type; resolves the target stream.</typeparam>
    /// <param name="message">The message to serialize and send.</param>
    /// <param name="cancellationToken">Cancels the publish.</param>
    Task PublishAsync<T>(T message, CancellationToken cancellationToken);

    /// <summary>Publishes to an explicitly named plain stream, with options. The target must be declared or <see cref="StreamBusStreamNotFoundException"/> surfaces.</summary>
    /// <typeparam name="T">The message type; not used for routing.</typeparam>
    /// <param name="stream">Explicit plain-stream target (overrides the registration).</param>
    /// <param name="message">The message to serialize and send.</param>
    /// <param name="options">Optional per-call publish options.</param>
    /// <param name="cancellationToken">Cancels the publish.</param>
    Task PublishAsync<T>(string stream, T message, PublishOptions? options = null, CancellationToken cancellationToken = default);

    /// <summary>Publishes to an explicitly named plain stream.</summary>
    /// <typeparam name="T">The message type; not used for routing.</typeparam>
    /// <param name="stream">Explicit plain-stream target (overrides the registration).</param>
    /// <param name="message">The message to serialize and send.</param>
    /// <param name="cancellationToken">Cancels the publish.</param>
    Task PublishAsync<T>(string stream, T message, CancellationToken cancellationToken);

    /// <summary>Routed super-stream publish to the stream registered for <typeparamref name="T"/>, with options.</summary>
    /// <typeparam name="T">Message type registered with PublishSuper.</typeparam>
    /// <param name="message">The message to serialize and send.</param>
    /// <param name="routingKey">Required routing value; the broker routes it to one partition.</param>
    /// <param name="options">Optional per-call publish options.</param>
    /// <param name="cancellationToken">Cancels the publish.</param>
    Task PublishSuperAsync<T>(T message, string routingKey, PublishOptions? options = null, CancellationToken cancellationToken = default);

    /// <summary>Routed super-stream publish to the stream registered for <typeparamref name="T"/>.</summary>
    /// <typeparam name="T">Message type registered with PublishSuper.</typeparam>
    /// <param name="message">The message to serialize and send.</param>
    /// <param name="routingKey">Required routing value; the broker routes it to one partition.</param>
    /// <param name="cancellationToken">Cancels the publish.</param>
    Task PublishSuperAsync<T>(T message, string routingKey, CancellationToken cancellationToken);

    /// <summary>Routed super-stream publish to an explicitly named super stream, with options.</summary>
    /// <typeparam name="T">The message type; not used for routing.</typeparam>
    /// <param name="stream">Explicit super-stream target (overrides the registration).</param>
    /// <param name="message">The message to serialize and send.</param>
    /// <param name="routingKey">Required routing value; the broker routes it to one partition.</param>
    /// <param name="options">Optional per-call publish options.</param>
    /// <param name="cancellationToken">Cancels the publish.</param>
    Task PublishSuperAsync<T>(string stream, T message, string routingKey, PublishOptions? options = null, CancellationToken cancellationToken = default);

    /// <summary>Routed super-stream publish to an explicitly named super stream.</summary>
    /// <typeparam name="T">The message type; not used for routing.</typeparam>
    /// <param name="stream">Explicit super-stream target (overrides the registration).</param>
    /// <param name="message">The message to serialize and send.</param>
    /// <param name="routingKey">Required routing value; the broker routes it to one partition.</param>
    /// <param name="cancellationToken">Cancels the publish.</param>
    Task PublishSuperAsync<T>(string stream, T message, string routingKey, CancellationToken cancellationToken);

    /// <summary>Sends pre-serialized bytes to <paramref name="stream"/>, bypassing serialization and the publish pipeline.</summary>
    /// <param name="stream">Target stream.</param>
    /// <param name="body">Already-serialized message body.</param>
    /// <param name="options">Optional publish options (properties, routing key for super streams).</param>
    /// <param name="cancellationToken">Cancels the publish.</param>
    Task PublishRawAsync(string stream, ReadOnlyMemory<byte> body, PublishOptions? options = null, CancellationToken cancellationToken = default);


    /// <summary>Attaches a raw (untyped) consumer to <paramref name="stream"/>; dispose the returned consumer to detach.</summary>
    /// <param name="stream">Stream (or super stream) to consume.</param>
    /// <param name="options">Consume settings; <see cref="ConsumeOptions.Reference"/> is required.</param>
    /// <param name="handler">Invoked per delivery; exceptions drive the retry/error pipeline.</param>
    /// <param name="cancellationToken">Cancels the attach.</param>
    Task<IStreamConsumer> ConsumeAsync(string stream, ConsumeOptions options, Func<ConsumeContext, Task> handler, CancellationToken cancellationToken = default);

    /// <summary>Queries the stored broker-side offset for a consumer group on one partition.</summary>
    /// <param name="stream">Stream name.</param>
    /// <param name="reference">Consumer group (reference) name.</param>
    /// <param name="partition">Partition index; 0 for plain streams.</param>
    /// <param name="cancellationToken">Cancels the query.</param>
    /// <returns>The stored offset, or null when the group has no stored offset.</returns>
    Task<ulong?> GetConsumerOffsetAsync(string stream, string reference, int partition, CancellationToken cancellationToken = default);

    /// <summary>Stores the broker-side offset for a consumer group on one partition.</summary>
    /// <param name="stream">Stream name.</param>
    /// <param name="reference">Consumer group (reference) name.</param>
    /// <param name="partition">Partition index; 0 for plain streams.</param>
    /// <param name="offset">Offset to store; the group resumes after it.</param>
    /// <param name="cancellationToken">Cancels the store.</param>
    Task SetConsumerOffsetAsync(string stream, string reference, int partition, ulong offset, CancellationToken cancellationToken = default);

    /// <summary>
    /// Idempotent topology declaration surface. Startup declarations configured
    /// via topology are declared automatically when the bus starts; use this
    /// property for explicit or operator-driven declaration at any time.
    /// </summary>
    IStreamTopology Topology { get; }
}

/// <summary>
/// A bus keyed by a marker type <typeparamref name="TBus"/>: the same surface as
/// <see cref="IStreamBus"/>, typed so several independently configured buses
/// (each with its own registrations) resolve distinctly from DI.
/// </summary>
/// <typeparam name="TBus">Marker type identifying this bus registration.</typeparam>
public interface IStreamBus<TBus> : IStreamBus
{
}

/// <summary>Default marker used by the non-generic (<c>AddStreamBus</c>) registration; inject <see cref="IStreamBus"/> for that bus.</summary>
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
    /// <summary>Virtual host this bus is registered against.</summary>
    public string Vhost { get; }
    /// <summary>Idempotent topology declaration surface (see <see cref="IStreamBus.Topology"/>).</summary>
    public IStreamTopology Topology => _topology;
    /// <summary>Live connection state of the underlying supervisor.</summary>
    public BusConnectionState State => _supervisor.State;
    /// <summary>Waits until the connection is ready, per <paramref name="policy"/>.</summary>
    /// <param name="policy">How long/whether to wait (fail fast, bounded, or forever).</param>
    /// <param name="cancellationToken">Cancels the wait.</param>
    /// <returns>The outcome: <see cref="BusReadiness.Ready"/>, <see cref="BusReadiness.TimedOut"/>, or <see cref="BusReadiness.Fatal"/>.</returns>
    public Task<BusReadiness> WaitUntilReadyAsync(ReadinessPolicy policy, CancellationToken cancellationToken = default)
        => _supervisor.WaitUntilReadyAsync(policy, cancellationToken);
    /// <summary>Publishes to the stream registered for <typeparamref name="T"/>, with options. Throws <see cref="StreamBusConfigurationException"/> for a PublishSuper registration.</summary>
    public Task PublishAsync<T>(T message, PublishOptions? options = null, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(message);
        var reg = _router.Resolve(typeof(TBus), typeof(T));
        if (reg.IsSuper)
            throw new StreamBusConfigurationException($"Message {typeof(T).FullName} is registered with PublishSuper for stream '{reg.Stream}'. Use PublishSuperAsync (routing key required).");
        var effective = (options ?? new PublishOptions()) with { IsSuperStream = false, RoutingKey = null };
        return PublishThroughPipelineAsync(reg.Stream, routingKey: null, message, effective, cancellationToken);
    }
    /// <summary>Publishes to the stream registered for <typeparamref name="T"/>.</summary>
    public Task PublishAsync<T>(T message, CancellationToken cancellationToken)
        => PublishAsync(message, null, cancellationToken);
    /// <summary>Publishes to an explicitly named plain stream, with options. The target must be declared or <see cref="StreamBusStreamNotFoundException"/> surfaces.</summary>
    public Task PublishAsync<T>(string stream, T message, PublishOptions? options = null, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(stream);
        ArgumentNullException.ThrowIfNull(message);
        DeclareSetBuilder.ValidateCallTarget(_findDeclaration(stream), stream, wantSuper: false);
        var effective = (options ?? new PublishOptions()) with { IsSuperStream = false, RoutingKey = null };
        return PublishThroughPipelineAsync(stream, routingKey: null, message, effective, cancellationToken);
    }
    /// <summary>Publishes to an explicitly named plain stream.</summary>
    public Task PublishAsync<T>(string stream, T message, CancellationToken cancellationToken)
        => PublishAsync(stream, message, null, cancellationToken);
    /// <summary>Routed super-stream publish to the stream registered for <typeparamref name="T"/>, with options. Throws <see cref="StreamBusConfigurationException"/> for a plain Publish registration.</summary>
    public Task PublishSuperAsync<T>(T message, string routingKey, PublishOptions? options = null, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(message);
        ArgumentException.ThrowIfNullOrWhiteSpace(routingKey);
        var reg = _router.Resolve(typeof(TBus), typeof(T));
        if (!reg.IsSuper)
            throw new StreamBusConfigurationException($"Message {typeof(T).FullName} is registered with Publish for stream '{reg.Stream}'. Use PublishAsync (no routing key).");
        var effective = (options ?? new PublishOptions()) with { IsSuperStream = true, RoutingKey = routingKey };
        return PublishThroughPipelineAsync(reg.Stream, routingKey, message, effective, cancellationToken);
    }
    /// <summary>Routed super-stream publish to the stream registered for <typeparamref name="T"/>.</summary>
    public Task PublishSuperAsync<T>(T message, string routingKey, CancellationToken cancellationToken)
        => PublishSuperAsync(message, routingKey, null, cancellationToken);
    /// <summary>Routed super-stream publish to an explicitly named super stream, with options.</summary>
    public Task PublishSuperAsync<T>(string stream, T message, string routingKey, PublishOptions? options = null, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(stream);
        ArgumentNullException.ThrowIfNull(message);
        ArgumentException.ThrowIfNullOrWhiteSpace(routingKey);
        DeclareSetBuilder.ValidateCallTarget(_findDeclaration(stream), stream, wantSuper: true);
        var effective = (options ?? new PublishOptions()) with { IsSuperStream = true, RoutingKey = routingKey };
        return PublishThroughPipelineAsync(stream, routingKey, message, effective, cancellationToken);
    }
    /// <summary>Routed super-stream publish to an explicitly named super stream.</summary>
    public Task PublishSuperAsync<T>(string stream, T message, string routingKey, CancellationToken cancellationToken)
        => PublishSuperAsync(stream, message, routingKey, null, cancellationToken);
    private async Task PublishThroughPipelineAsync<T>(
        string stream,
        string? routingKey,
        T message,
        PublishOptions effective,
        CancellationToken cancellationToken)
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
                await SendTypedAsync(stream, message, effective, cancellationToken, activity).ConfigureAwait(false);
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
                TypedPayload = message,
                MessageId = messageId,
                CorrelationId = seed.CorrelationId,
                SentTime = sentTime,
                Headers = headers,
                Services = publishScope.ServiceProvider,
                CancellationToken = cancellationToken,
            };

            await pipeline.RunAsync(
                context,
                () => SendTypedAsync(
                    stream,
                    message,
                    effective with { Properties = headers.Materialize(effective.Properties) },
                    cancellationToken,
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
        CancellationToken cancellationToken,
        Activity? activity = null)
    {
        var body = _serializer.Serialize(message);
        StreamBusActivity.SetSize(activity, body.Length);
        var properties = (effective.Properties ?? new StreamMessageProperties()) with { ContentType = _serializer.ContentType };
        await _producers.PublishAsync(stream, body, effective with { Properties = properties }, cancellationToken).ConfigureAwait(false);
    }

    /// <summary>Sends pre-serialized bytes to <paramref name="stream"/>, bypassing serialization and the publish pipeline.</summary>
    public async Task PublishRawAsync(string stream, ReadOnlyMemory<byte> body, PublishOptions? options = null, CancellationToken cancellationToken = default)
    {
        using var activity = StreamBusActivity.StartPublish(stream, options?.RoutingKey);
        StreamMessageProperties.ValidateHeaders(options?.Properties?.Headers);
        StreamBusActivity.SetSize(activity, body.Length);
        await _producers.PublishAsync(stream, body, options ?? new PublishOptions(), cancellationToken).ConfigureAwait(false);
    }
    /// <summary>Attaches a raw (untyped) consumer to <paramref name="stream"/>; dispose the returned consumer to detach.</summary>
    public Task<IStreamConsumer> ConsumeAsync(string stream, ConsumeOptions options, Func<ConsumeContext, Task> handler, CancellationToken cancellationToken = default)
        => _consumers.ConsumeAsync(typeof(TBus), stream, options, handler, cancellationToken, serializer: _serializer);
    /// <summary>Queries the stored broker-side offset for a consumer group on one partition; null when none is stored.</summary>
    public Task<ulong?> GetConsumerOffsetAsync(string stream, string reference, int partition, CancellationToken cancellationToken = default)
        => _consumers.GetOffsetAsync(stream, reference, partition, cancellationToken);
    /// <summary>Stores the broker-side offset for a consumer group on one partition.</summary>
    public Task SetConsumerOffsetAsync(string stream, string reference, int partition, ulong offset, CancellationToken cancellationToken = default)
        => _consumers.SetOffsetAsync(stream, reference, partition, offset, cancellationToken);
    internal Task StartAsync(CancellationToken cancellationToken) => _supervisor.StartAsync(cancellationToken);
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
