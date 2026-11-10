using System.Diagnostics.CodeAnalysis;
using System.Net.Security;
using System.Security.Authentication;
using System.Text;
using System.Text.Json;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;

namespace StreamBus;

/// <summary>Service collection passed to the bus configuration callback.</summary>
public interface IStreamRegistrationContext
{
    /// <summary>The service collection the bus is being added to.</summary>
    IServiceCollection Services { get; }
}

/// <summary>Configures broker connection settings inside <c>Host</c>.</summary>
public interface IHostConfigurator
{
    /// <summary>Sets the broker credentials.</summary>
    /// <param name="user">User name; must not be null.</param>
    /// <param name="password">Password; must not be null.</param>
    void Credentials(string user, string password);

    /// <summary>Sets the stream protocol port.</summary>
    /// <param name="port">Stream port; default 5552.</param>
    void StreamPort(int port = 5552);

    /// <summary>Sets the AMQP port used for dead-letter queues and topology.</summary>
    /// <param name="port">AMQP port; default 5672.</param>
    void AmqpPort(int port = 5672);

    /// <summary>Sets the requested heartbeat interval.</summary>
    /// <param name="heartbeat">Heartbeat interval; must be positive.</param>
    void Heartbeat(TimeSpan heartbeat);

    /// <summary>Sets the connection monitor interval.</summary>
    /// <param name="interval">Interval between monitor passes; positive, at most 60 seconds.</param>
    void MonitorInterval(TimeSpan interval);

    /// <summary>Sets producers multiplexed per connection.</summary>
    /// <param name="count">Producer count; 1..255.</param>
    void ProducersPerConnection(int count = 1);

    /// <summary>Sets consumers multiplexed per connection.</summary>
    /// <param name="count">Consumer count; 1..255.</param>
    void ConsumersPerConnection(int count = 1);

    /// <summary>Enables TLS for both the stream protocol and AMQP connections. Optional; off by default.</summary>
    /// <param name="configure">Optional TLS settings callback.</param>
    /// <remarks>
    /// Without explicit <see cref="StreamPort"/>/<see cref="AmqpPort"/> calls, enabling TLS switches the
    /// default ports to the conventional RabbitMQ TLS ports (5551 stream, 5671 AMQP). Explicit port calls
    /// always win, regardless of call order. Calling UseTls again replaces the previous TLS settings.
    /// </remarks>
    void UseTls(Action<ITlsConfigurator>? configure = null);
}

/// <summary>Configures TLS settings inside <c>UseTls</c>.</summary>
public interface ITlsConfigurator
{
    /// <summary>Sets the server name used for TLS certificate validation (SNI and name matching); null falls back to the host name.</summary>
    /// <param name="name">Server name; must not be null.</param>
    void ServerName(string name);

    /// <summary>Sets a PFX client certificate file presented to the broker.</summary>
    /// <param name="path">Path to the PFX certificate file; must not be null.</param>
    /// <param name="password">Optional password for the certificate file.</param>
    void ClientCertificate(string path, string? password = null);

    /// <summary>Sets certificate policy errors accepted despite failing validation.</summary>
    /// <param name="errors">The accepted policy errors; default <see cref="SslPolicyErrors.None"/>.</param>
    void AcceptableCertificateErrors(SslPolicyErrors errors);

    /// <summary>Sets the allowed TLS protocol versions; <see cref="SslProtocols.None"/> means the operating system default.</summary>
    /// <param name="version">Allowed TLS protocol versions.</param>
    void ProtocolVersion(SslProtocols version);
}

/// <summary>Configures one StreamBus bus: connection, topology, and retry.</summary>
public interface IStreamBusConfigurator
{
    /// <summary>Sets the broker connection; exactly one per bus.</summary>
    /// <param name="host">Broker host name or address; must not be empty.</param>
    /// <param name="vhost">RabbitMQ virtual host; must not be empty.</param>
    /// <param name="configure">Connection settings callback.</param>
    /// <exception cref="StreamBusConfigurationException">Called more than once for this bus.</exception>
    void Host(string host, string vhost, Action<IHostConfigurator> configure);

    /// <summary>Replaces the message serializer.</summary>
    /// <param name="serializer">Serializer used by typed publish and consume.</param>
    void Serializer(IStreamSerializer serializer);

    /// <summary>
    /// Replaces the message serializer with a System.Text.Json serializer built
    /// from a fresh <see cref="JsonSerializerOptions"/> configured by
    /// <paramref name="configure"/>.
    /// </summary>
    /// <param name="configure">Callback that mutates the new options.</param>
    void Serializer(Action<JsonSerializerOptions> configure);

    /// <summary>Declares a publish stream for <typeparamref name="TMessage"/>.</summary>
    /// <typeparam name="TMessage">The publish message type.</typeparam>
    /// <param name="stream">Stream name; must not be empty.</param>
    /// <param name="configure">Optional stream declaration callback.</param>
    void Publish<TMessage>(string stream, Action<IStreamOptionsBuilder>? configure = null);

    /// <summary>Declares a publish stream for <typeparamref name="TMessage"/> with an explicit declaration spec.</summary>
    /// <typeparam name="TMessage">The publish message type.</typeparam>
    /// <param name="stream">Stream name; must not be empty.</param>
    /// <param name="spec">Stream declaration spec.</param>
    void Publish<TMessage>(string stream, StreamOptions spec);

    /// <summary>Declares a super-stream publish for <typeparamref name="TMessage"/>.</summary>
    /// <typeparam name="TMessage">The publish message type.</typeparam>
    /// <param name="stream">Super-stream name; must not be empty.</param>
    /// <param name="configure">Super-stream declaration callback.</param>
    void PublishSuper<TMessage>(string stream, Action<ISuperStreamOptionsBuilder> configure);

    /// <summary>Declares a super-stream publish for <typeparamref name="TMessage"/> with an explicit declaration spec.</summary>
    /// <typeparam name="TMessage">The publish message type.</typeparam>
    /// <param name="stream">Super-stream name; must not be empty.</param>
    /// <param name="spec">Super-stream declaration spec.</param>
    void PublishSuper<TMessage>(string stream, StreamOptions spec);

    /// <summary>Declares a receive endpoint on <paramref name="stream"/> for one consumer and consumer group.</summary>
    /// <param name="stream">Stream name; must not be empty.</param>
    /// <param name="configure">Endpoint configuration callback.</param>
    void ReceiveEndpoint(string stream, Action<IReceiveEndpointConfigurator> configure);

    /// <summary>
    /// Configures the bus-wide retry schedule; endpoints inherit it unless they
    /// override it with their own schedule.
    /// </summary>
    /// <param name="configure">Retry configuration callback.</param>
    /// <exception cref="StreamBusConfigurationException">The callback sets no schedule.</exception>
    void UseRetry(Action<IRetryConfigurator> configure);

    /// <summary>
    /// Sets the bus-wide retry schedule from a raw policy, an escape hatch for
    /// schedules the fluent configurator cannot express.
    /// </summary>
    /// <param name="policy">The retry policy to inherit by every endpoint without its own schedule.</param>
    void UseRetry(RetryPolicy policy);
    /// <summary>
    /// Registers a publish filter, discovering its typed message interfaces at
    /// runtime. Use
    /// <see cref="UsePublishFilter{TFilter,TMessage}()"/> to avoid closing the
    /// adapter at runtime. The filter type is registered as a singleton because
    /// publish filters are resolved once per bus and cached; register the type
    /// as a singleton yourself if you add it before this call.
    /// </summary>
    [RequiresUnreferencedCode(RuntimeGeneric.TrimMessage)]
    [RequiresDynamicCode(RuntimeGeneric.AotMessage)]
    void UsePublishFilter<[DynamicallyAccessedMembers(RuntimeGeneric.FilterMembers)] TFilter>() where TFilter : class;
    /// <summary>
    /// Registers a typed publish filter whose declared pipe type is
    /// <typeparamref name="TMessage"/>. This overload constructs the typed
    /// adapter at the call site and therefore avoids closing a generic type at
    /// runtime. It removes one trim/AOT hazard; it does not by itself make
    /// trimming or NativeAOT publish supported (the default serializer and
    /// type-based configuration are still reflection-based). The filter type is
    /// registered as a singleton because publish filters are resolved once per
    /// bus and cached.
    /// </summary>
    void UsePublishFilter<[DynamicallyAccessedMembers(RuntimeGeneric.FilterMembers)] TFilter, TMessage>() where TFilter : class, IStreamPublishFilter<TMessage>;
    /// <summary>
    /// Registers a publish filter by type. The type is registered as a
    /// singleton because publish filters are resolved once per bus and cached.
    /// </summary>
    [RequiresUnreferencedCode(RuntimeGeneric.TrimMessage)]
    [RequiresDynamicCode(RuntimeGeneric.AotMessage)]
    void UsePublishFilter([DynamicallyAccessedMembers(RuntimeGeneric.FilterMembers)] Type filterType);
    /// <summary>
    /// Registers a publish filter created by <paramref name="factory"/>. The
    /// factory runs once per bus; its result is cached for the bus lifetime.
    /// </summary>
    void UsePublishFilter(Func<IServiceProvider, IStreamPublishFilter> factory);
    /// <summary>
    /// Registers an existing publish filter instance, which is used for every
    /// publish on the bus.
    /// </summary>
    void UsePublishFilter(IStreamPublishFilter instance);
    /// <summary>
    /// Registers a consume filter, discovering its typed message interfaces at
    /// runtime. Use
    /// <see cref="UseConsumeFilter{TFilter,TMessage}()"/> to avoid closing the
    /// adapter at runtime. The filter type is registered as a singleton because
    /// consume filters are resolved once per bus and cached; register the type
    /// as a singleton yourself if you add it before this call.
    /// </summary>
    [RequiresUnreferencedCode(RuntimeGeneric.TrimMessage)]
    [RequiresDynamicCode(RuntimeGeneric.AotMessage)]
    void UseConsumeFilter<[DynamicallyAccessedMembers(RuntimeGeneric.FilterMembers)] TFilter>() where TFilter : class;
    /// <summary>
    /// Registers a typed consume filter whose endpoint message type is
    /// <typeparamref name="TMessage"/>. Unlike
    /// <see cref="UseConsumeFilter{TFilter}()"/>, this overload closes the typed
    /// adapter at the call site and needs no runtime generic closing. The
    /// filter type is registered as a singleton because consume filters are
    /// resolved once per bus and cached.
    /// </summary>
    void UseConsumeFilter<[DynamicallyAccessedMembers(RuntimeGeneric.FilterMembers)] TFilter, TMessage>() where TFilter : class, IStreamConsumeFilter<TMessage>;
    /// <summary>
    /// Registers a consume filter by type. The type is registered as a
    /// singleton because consume filters are resolved once per bus and cached.
    /// </summary>
    [RequiresUnreferencedCode(RuntimeGeneric.TrimMessage)]
    [RequiresDynamicCode(RuntimeGeneric.AotMessage)]
    void UseConsumeFilter([DynamicallyAccessedMembers(RuntimeGeneric.FilterMembers)] Type filterType);
    /// <summary>
    /// Registers a consume filter created by <paramref name="factory"/>. The
    /// factory runs once per bus; its result is cached for the bus lifetime.
    /// </summary>
    void UseConsumeFilter(Func<IServiceProvider, IStreamConsumeFilter> factory);
    /// <summary>
    /// Registers an existing consume filter instance, which is used for every
    /// delivery on the bus.
    /// </summary>
    void UseConsumeFilter(IStreamConsumeFilter instance);
}

/// <summary>Configures one receive endpoint: consumer, offset, retry, and exhaustion behavior.</summary>
public interface IReceiveEndpointConfigurator
{
    /// <summary>
    /// Sets the endpoint's typed consumer and its consumer group; exactly once
    /// per endpoint. The consumer class must implement
    /// <c>IConsumer&lt;T&gt;</c> for exactly one message type.
    /// </summary>
    /// <typeparam name="TConsumer">The consumer class; registered as transient.</typeparam>
    /// <param name="consumerGroup">Consumer group name; required, at most 255 UTF-8 bytes.</param>
    /// <exception cref="StreamBusConfigurationException">Called twice, the group name is empty, the group name exceeds the broker's 255-byte limit, or the consumer type implements <c>IConsumer&lt;T&gt;</c> for zero or multiple message types.</exception>
    void Consumer<TConsumer>(string consumerGroup) where TConsumer : class;

    /// <summary>Marks the endpoint's stream as a super-stream.</summary>
    /// <param name="enabled">Whether the stream is a super-stream.</param>
    void SuperStream(bool enabled = true);

    /// <summary>Enables or disables single-active-consumer semantics for the consumer group.</summary>
    /// <param name="enabled">Whether only one consumer instance in the group is active.</param>
    void SingleActiveConsumer(bool enabled = true);

    /// <summary>Sets where the consumer starts when no stored offset exists.</summary>
    /// <param name="startOffset">The start-offset policy.</param>
    void StartOffset(StartOffset startOffset);

    /// <summary>Sets the offset-tracking store options.</summary>
    /// <param name="options">Offset store configuration.</param>
    void OffsetStore(OffsetStoreOptions options);

    /// <summary>Sets the readiness policy for the consumer.</summary>
    /// <param name="policy">The readiness policy.</param>
    void Readiness(ReadinessPolicy policy);

    /// <summary>
    /// Configures this endpoint's retry schedule, overriding the bus-wide
    /// schedule for messages delivered to this endpoint.
    /// </summary>
    /// <param name="configure">Retry configuration callback.</param>
    /// <exception cref="StreamBusConfigurationException">The callback sets no schedule.</exception>
    void UseRetry(Action<IRetryConfigurator> configure);

    /// <summary>
    /// Sets this endpoint's retry schedule from a raw policy, overriding the
    /// bus-wide schedule.
    /// </summary>
    /// <param name="policy">The retry policy for this endpoint.</param>
    void UseRetry(RetryPolicy policy);
    /// <summary>
    /// Registers a consume filter on this endpoint and discovers its typed
    /// message interfaces at runtime. Use
    /// <see cref="UseConsumeFilter{TFilter,TMessage}()"/> to avoid closing the
    /// adapter at runtime. The filter type is registered as a singleton
    /// because consume filters are resolved once per bus and cached; register
    /// the type as a singleton yourself if you add it before this call.
    /// </summary>
    [RequiresUnreferencedCode(RuntimeGeneric.TrimMessage)]
    [RequiresDynamicCode(RuntimeGeneric.AotMessage)]
    void UseConsumeFilter<[DynamicallyAccessedMembers(RuntimeGeneric.FilterMembers)] TFilter>() where TFilter : class;
    /// <summary>
    /// Registers a typed consume filter on this endpoint whose message type is
    /// <typeparamref name="TMessage"/>. This overload constructs the typed
    /// adapter at the call site and therefore avoids closing a generic type at
    /// runtime. It removes one trim/AOT hazard; it does not by itself make
    /// trimming or NativeAOT consume supported (the typed consumer and default
    /// serializer are still reflection-based). The filter type is registered as
    /// a singleton because consume filters are resolved once per bus and
    /// cached.
    /// </summary>
    void UseConsumeFilter<[DynamicallyAccessedMembers(RuntimeGeneric.FilterMembers)] TFilter, TMessage>() where TFilter : class, IStreamConsumeFilter<TMessage>;
    /// <summary>
    /// Registers a consume filter on this endpoint by type. The type is
    /// registered as a singleton because consume filters are resolved once per
    /// bus and cached.
    /// </summary>
    [RequiresUnreferencedCode(RuntimeGeneric.TrimMessage)]
    [RequiresDynamicCode(RuntimeGeneric.AotMessage)]
    void UseConsumeFilter([DynamicallyAccessedMembers(RuntimeGeneric.FilterMembers)] Type filterType);
    /// <summary>
    /// Registers a consume filter on this endpoint created by
    /// <paramref name="factory"/>. The factory runs once per bus; its result
    /// is cached for the bus lifetime.
    /// </summary>
    void UseConsumeFilter(Func<IServiceProvider, IStreamConsumeFilter> factory);
    /// <summary>
    /// Registers an existing consume filter instance, which is used for every
    /// delivery on this endpoint.
    /// </summary>
    void UseConsumeFilter(IStreamConsumeFilter instance);
    /// <summary>
    /// Declares a stream dead-letter destination for messages whose retry
    /// schedule is exhausted; at most one dead-letter destination per endpoint.
    /// The destination is declared at startup with the given spec (partitions
    /// are not allowed).
    /// </summary>
    /// <param name="name">Destination stream name; must not be empty.</param>
    /// <param name="configure">Optional stream declaration callback.</param>
    /// <exception cref="StreamBusConfigurationException">A dead-letter destination is already configured, or the declaration spec is invalid.</exception>
    void DeadLetterToStream(string name, Action<IStreamOptionsBuilder>? configure = null);

    /// <summary>
    /// Declares a stream dead-letter destination with an explicit declaration
    /// spec; at most one dead-letter destination per endpoint.
    /// </summary>
    /// <param name="name">Destination stream name; must not be empty.</param>
    /// <param name="spec">Destination stream declaration spec; partitions are not allowed.</param>
    /// <exception cref="StreamBusConfigurationException">A dead-letter destination is already configured, or the declaration spec is invalid.</exception>
    void DeadLetterToStream(string name, StreamOptions spec);

    /// <summary>
    /// Declares a durable AMQP queue dead-letter destination; at most one
    /// dead-letter destination per endpoint.
    /// </summary>
    /// <param name="name">Destination queue name; must not be empty.</param>
    /// <param name="type">Queue type; must be a defined <see cref="DeadLetterQueueType"/> value.</param>
    /// <exception cref="StreamBusConfigurationException">A dead-letter destination is already configured, or <paramref name="type"/> is undefined.</exception>
    void DeadLetterToQueue(string name, DeadLetterQueueType type);

    /// <summary>
    /// Declares a durable AMQP queue dead-letter destination with configured
    /// error-queue options; at most one dead-letter destination per endpoint.
    /// </summary>
    /// <param name="name">Destination queue name; must not be empty.</param>
    /// <param name="type">Queue type; must be a defined <see cref="DeadLetterQueueType"/> value.</param>
    /// <param name="configure">Error-queue options callback (e.g. confirm timeout).</param>
    /// <exception cref="StreamBusConfigurationException">A dead-letter destination is already configured, or <paramref name="type"/> is undefined.</exception>
    void DeadLetterToQueue(string name, DeadLetterQueueType type, Action<IErrorQueueOptionsBuilder> configure);

    /// <summary>
    /// Sets the timeout for the confirmed publish to the error stream
    /// hand-off. Null waits indefinitely; values below one second are raised
    /// to one second.
    /// </summary>
    /// <param name="timeout">The error-publish timeout; must be positive when set.</param>
    /// <exception cref="StreamBusConfigurationException"><paramref name="timeout"/> is not positive.</exception>
    void ErrorPublishTimeout(TimeSpan? timeout);

    /// <summary>
    /// Registers a callback that owns the outcome for messages whose retry
    /// schedule is exhausted. It shadows a declared dead-letter destination
    /// with a startup warning; returning advances past the message, throwing
    /// recycles the attach for redelivery. Last call wins.
    /// </summary>
    /// <param name="handler">The exhaustion callback.</param>
    void OnExhausted(Func<ExhaustedContext, CancellationToken, Task> handler);

    /// <summary>
    /// Marks the endpoint's stream as a super-stream partitioned into
    /// <paramref name="partitions"/> partitions.
    /// </summary>
    /// <param name="partitions">Partition count for the super-stream; must be >= 1.</param>
    void Partitions(int partitions);

    /// <summary>Sets the stream's maximum age before segment truncation.</summary>
    /// <param name="maxAge">The maximum age.</param>
    void MaxAge(TimeSpan maxAge);

    /// <summary>Sets the stream's maximum length in bytes before segment truncation.</summary>
    /// <param name="maxLengthBytes">The maximum length in bytes.</param>
    void MaxLengthBytes(ulong maxLengthBytes);

    /// <summary>Sets the maximum size of a single stream segment in bytes.</summary>
    /// <param name="maxSegmentSizeBytes">The segment size limit in bytes.</param>
    void MaxSegmentSizeBytes(int maxSegmentSizeBytes);

    /// <summary>Sets the stream's leader locator strategy.</summary>
    /// <param name="locator">The leader locator.</param>
    void LeaderLocator(StreamLeaderLocator locator);
}

internal sealed record ReceiveEndpointDescriptor
{
    public required string Stream { get; init; }
    public required Type MessageType { get; init; }
    public required Type ConsumerType { get; init; }
    public required string ConsumerGroup { get; init; }
    public bool IsSuperStream { get; init; }
    public int? Partitions { get; init; }
    public TimeSpan? MaxAge { get; init; }
    public ulong? MaxLengthBytes { get; init; }
    public int? MaxSegmentSizeBytes { get; init; }
    public StreamLeaderLocator? LeaderLocator { get; init; }
    public bool SingleActiveConsumer { get; init; } = true;
    public StartOffset StartOffset { get; init; } = StartOffset.ResumeOrFirst;
    public OffsetStoreOptions OffsetStore { get; init; } = new();
    public RetryPolicy? RetryOverride { get; init; }
    public DeadLetterOptions? DeadLetter { get; init; }
    public TimeSpan? ErrorPublishTimeout { get; init; }
    public Func<ExhaustedContext, CancellationToken, Task>? OnExhausted { get; init; }
    public ReadinessPolicy Readiness { get; init; } = ReadinessPolicy.Default;
    internal IReadOnlyList<ConsumeFilterRegistration> ConsumeFilters { get; init; } = Array.Empty<ConsumeFilterRegistration>();

    internal void Validate()
        => StreamOptionsValidation.Validate(
            new StreamOptions
            {
                Partitions = IsSuperStream ? Partitions : null,
                MaxAge = MaxAge,
                MaxLengthBytes = MaxLengthBytes,
                MaxSegmentSizeBytes = MaxSegmentSizeBytes,
                LeaderLocator = LeaderLocator,
            },
            $"ReceiveEndpoint '{Stream}'",
            allowPartitions: IsSuperStream,
            partitionsError: "super-stream partitions must be >= 1.");
}

internal sealed class StreamBusEndpointStore
{
    private readonly List<(Type BusType, ReceiveEndpointDescriptor Descriptor)> _endpoints = new();

    internal void Add(Type busType, ReceiveEndpointDescriptor descriptor)
    {
        ArgumentNullException.ThrowIfNull(busType);
        ArgumentNullException.ThrowIfNull(descriptor);
        if (_endpoints.Any(e => e.BusType == busType && e.Descriptor.Stream == descriptor.Stream && e.Descriptor.ConsumerType == descriptor.ConsumerType && e.Descriptor.ConsumerGroup == descriptor.ConsumerGroup))
            throw new StreamBusConfigurationException($"Duplicate receive endpoint for {descriptor.ConsumerType.FullName} on stream '{descriptor.Stream}' with group '{descriptor.ConsumerGroup}' (marker {busType.FullName}).");
        _endpoints.Add((busType, descriptor));
    }

    public IReadOnlyList<(Type BusType, ReceiveEndpointDescriptor Descriptor)> Endpoints => _endpoints;
}

internal sealed class StreamRegistrationContext : IStreamRegistrationContext
{
    public StreamRegistrationContext(IServiceCollection services) => Services = services;
    public IServiceCollection Services { get; }
}

internal sealed class StreamBusConfigurator : IStreamBusConfigurator
{
    private readonly Type _busType;
    private string? _host;
    private string? _vhost;
    private string _user = "";
    private string _password = "";
    private int _streamPort = 5552;
    private int _amqpPort = 5672;
    private int? _streamPortUser;
    private int? _amqpPortUser;
    private bool _tlsEnabled;
    private StreamBusTlsOptions? _tls;
    private TimeSpan _heartbeat = TimeSpan.FromSeconds(60);
    private TimeSpan _monitorInterval = TimeSpan.FromSeconds(5);
    private int _producers = 1;
    private int _consumers = 1;
    private IStreamSerializer _serializer = SystemTextJsonStreamSerializer.Default;
    private readonly List<(Type MessageType, string Stream, bool IsSuperStream, StreamOptions StreamSpec)> _publishes = new();
    private readonly List<PublishFilterRegistration> _publishFilters = new();
    private readonly List<ConsumeFilterRegistration> _consumeFilters = new();
    private readonly List<ReceiveEndpointDescriptor> _endpoints = new();
    private RetryPolicy? _globalRetry;
    private bool _hostCalled;

    internal StreamBusConfigurator(Type busType)
    {
        ArgumentNullException.ThrowIfNull(busType);
        _busType = busType;
    }

    internal IReadOnlyList<(Type MessageType, string Stream, bool IsSuperStream, StreamOptions StreamSpec)> Publishes => _publishes;

    internal IReadOnlyList<PublishFilterRegistration> PublishFilters => _publishFilters;

    internal IReadOnlyList<ConsumeFilterRegistration> ConsumeFilters => _consumeFilters;

    internal IReadOnlyList<ReceiveEndpointDescriptor> Endpoints => _endpoints;

    internal RetryPolicy? GlobalRetry => _globalRetry;

    public void Host(string host, string vhost, Action<IHostConfigurator> configure)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(host);
        ArgumentException.ThrowIfNullOrWhiteSpace(vhost);
        ArgumentNullException.ThrowIfNull(configure);
        if (_hostCalled)
            throw new StreamBusConfigurationException("Host may be called exactly once per bus.");
        _host = host.Trim();
        _vhost = vhost.Trim();
        var inner = new HostConfigurator(this);
        configure(inner);
        _hostCalled = true;
    }

    public void Serializer(IStreamSerializer serializer)
    {
        ArgumentNullException.ThrowIfNull(serializer);
        _serializer = serializer;
    }

    public void Serializer(Action<JsonSerializerOptions> configure)
    {
        ArgumentNullException.ThrowIfNull(configure);
        var options = new JsonSerializerOptions();
        configure(options);
        _serializer = new SystemTextJsonStreamSerializer(options);
    }

    public void Publish<TMessage>(string stream, Action<IStreamOptionsBuilder>? configure = null)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(stream);
        var builder = new StreamOptionsBuilder();
        configure?.Invoke(builder);
        AddPublish(typeof(TMessage), stream, isSuper: false, spec: builder.Build());
    }

    public void Publish<TMessage>(string stream, StreamOptions spec)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(stream);
        ArgumentNullException.ThrowIfNull(spec);
        AddPublish(typeof(TMessage), stream, isSuper: false, spec: spec);
    }

    public void PublishSuper<TMessage>(string stream, Action<ISuperStreamOptionsBuilder> configure)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(stream);
        ArgumentNullException.ThrowIfNull(configure);
        var builder = new StreamOptionsBuilder();
        configure(builder);
        AddPublish(typeof(TMessage), stream, isSuper: true, spec: builder.Build());
    }

    public void PublishSuper<TMessage>(string stream, StreamOptions spec)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(stream);
        ArgumentNullException.ThrowIfNull(spec);
        AddPublish(typeof(TMessage), stream, isSuper: true, spec: spec);
    }

    private void AddPublish(Type messageType, string stream, bool isSuper, StreamOptions spec)
    {
        StreamOptionsValidation.Validate(spec, $"Publish '{stream.Trim()}'", allowPartitions: isSuper);
        _publishes.Add((messageType, stream.Trim(), isSuper, spec));
    }

    public void ReceiveEndpoint(string stream, Action<IReceiveEndpointConfigurator> configure)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(stream);
        ArgumentNullException.ThrowIfNull(configure);
        var endpoint = new ReceiveEndpointConfigurator(stream.Trim());
        configure(endpoint);
        _endpoints.Add(endpoint.Build());
    }

    public void UseRetry(Action<IRetryConfigurator> configure)
    {
        ArgumentNullException.ThrowIfNull(configure);
        var retry = new RetryConfigurator();
        configure(retry);
        _globalRetry = ((IRetryConfigurator)retry).BuildPolicy();
    }

    public void UseRetry(RetryPolicy policy)
    {
        ArgumentNullException.ThrowIfNull(policy);
        _globalRetry = policy;
    }

    [RequiresUnreferencedCode(RuntimeGeneric.TrimMessage)]
    [RequiresDynamicCode(RuntimeGeneric.AotMessage)]
    public void UsePublishFilter<[DynamicallyAccessedMembers(RuntimeGeneric.FilterMembers)] TFilter>() where TFilter : class
    {
        AddPublishFilter(PublishFilterRegistration.ForType(typeof(TFilter)));
    }

    public void UsePublishFilter<[DynamicallyAccessedMembers(RuntimeGeneric.FilterMembers)] TFilter, TMessage>() where TFilter : class, IStreamPublishFilter<TMessage>
    {
        AddPublishFilter(PublishFilterRegistration.ForTyped<TFilter, TMessage>());
    }

    [RequiresUnreferencedCode(RuntimeGeneric.TrimMessage)]
    [RequiresDynamicCode(RuntimeGeneric.AotMessage)]
    public void UsePublishFilter([DynamicallyAccessedMembers(RuntimeGeneric.FilterMembers)] Type filterType)
    {
        ArgumentNullException.ThrowIfNull(filterType);
        AddPublishFilter(PublishFilterRegistration.ForType(filterType));
    }

    public void UsePublishFilter(Func<IServiceProvider, IStreamPublishFilter> factory)
    {
        ArgumentNullException.ThrowIfNull(factory);
        AddPublishFilter(PublishFilterRegistration.ForFactory(factory));
    }

    public void UsePublishFilter(IStreamPublishFilter instance)
    {
        ArgumentNullException.ThrowIfNull(instance);
        AddPublishFilter(PublishFilterRegistration.ForInstance(instance));
    }

    private void AddPublishFilter(PublishFilterRegistration filter)
    {
        if (filter.FilterType is { } type && _publishFilters.Any(filter.ConflictsWith))
        {
            throw new StreamBusConfigurationException(
                $"Publish filter '{type}' is already registered on this bus for one of its message types; duplicate registration would run it twice.");
        }
        _publishFilters.Add(filter);
    }

    [RequiresUnreferencedCode(RuntimeGeneric.TrimMessage)]
    [RequiresDynamicCode(RuntimeGeneric.AotMessage)]
    public void UseConsumeFilter<[DynamicallyAccessedMembers(RuntimeGeneric.FilterMembers)] TFilter>() where TFilter : class
    {
        AddConsumeFilter(ConsumeFilterRegistration.ForType(typeof(TFilter)));
    }

    public void UseConsumeFilter<[DynamicallyAccessedMembers(RuntimeGeneric.FilterMembers)] TFilter, TMessage>() where TFilter : class, IStreamConsumeFilter<TMessage>
    {
        AddConsumeFilter(ConsumeFilterRegistration.ForTyped<TFilter, TMessage>());
    }

    [RequiresUnreferencedCode(RuntimeGeneric.TrimMessage)]
    [RequiresDynamicCode(RuntimeGeneric.AotMessage)]
    public void UseConsumeFilter([DynamicallyAccessedMembers(RuntimeGeneric.FilterMembers)] Type filterType)
    {
        ArgumentNullException.ThrowIfNull(filterType);
        AddConsumeFilter(ConsumeFilterRegistration.ForType(filterType));
    }

    public void UseConsumeFilter(Func<IServiceProvider, IStreamConsumeFilter> factory)
    {
        ArgumentNullException.ThrowIfNull(factory);
        AddConsumeFilter(ConsumeFilterRegistration.ForFactory(factory));
    }

    public void UseConsumeFilter(IStreamConsumeFilter instance)
    {
        ArgumentNullException.ThrowIfNull(instance);
        AddConsumeFilter(ConsumeFilterRegistration.ForInstance(instance));
    }

    private void AddConsumeFilter(ConsumeFilterRegistration filter)
    {
        if (filter.FilterType is { } type && _consumeFilters.Any(filter.ConflictsWith))
        {
            throw new StreamBusConfigurationException(
                $"Consume filter '{type}' is already registered on this bus for one of its message types; duplicate registration would run it twice.");
        }
        _consumeFilters.Add(filter);
    }

    internal StreamBusOptions Build(Type busType)
    {
        ArgumentNullException.ThrowIfNull(busType);
        if (busType != _busType)
            throw new StreamBusConfigurationException($"Internal error: configurator for {_busType.FullName} built as {busType.FullName}.");
        if (!_hostCalled)
            throw new StreamBusConfigurationException($"Bus {busType.FullName}: Host must be configured exactly once.");

        var options = new StreamBusOptions
        {
            Vhost = _vhost!.Trim(),
            UserName = _user,
            Password = _password,
            Host = _host!.Trim(),
            StreamPort = _streamPortUser ?? (_tlsEnabled ? 5551 : _streamPort),
            AmqpPort = _amqpPortUser ?? (_tlsEnabled ? 5671 : _amqpPort),
            Tls = _tls,
            Heartbeat = _heartbeat,
            Connection = new StreamBusConnectionOptions
            {
                ProducersPerConnection = _producers,
                ConsumersPerConnection = _consumers,
                MonitorInterval = _monitorInterval,
            },
            Serializer = _serializer,
        };
        foreach (var error in options.Validate())
            throw new StreamBusConfigurationException(error);
        return options;
    }

    private sealed class HostConfigurator : IHostConfigurator
    {
        private readonly StreamBusConfigurator _owner;
        public HostConfigurator(StreamBusConfigurator owner) => _owner = owner;

        public void Credentials(string user, string password)
        {
            ArgumentNullException.ThrowIfNull(user);
            ArgumentNullException.ThrowIfNull(password);
            _owner._user = user;
            _owner._password = password;
        }

        public void StreamPort(int port = 5552)
        {
            _owner._streamPort = port;
            _owner._streamPortUser = port;
        }

        public void AmqpPort(int port = 5672)
        {
            _owner._amqpPort = port;
            _owner._amqpPortUser = port;
        }

        public void UseTls(Action<ITlsConfigurator>? configure = null)
        {
            _owner._tlsEnabled = true;
            var tls = new TlsConfigurator();
            configure?.Invoke(tls);
            _owner._tls = tls.Build();
        }

        public void Heartbeat(TimeSpan heartbeat) => _owner._heartbeat = heartbeat;

        public void MonitorInterval(TimeSpan interval) => _owner._monitorInterval = interval;

        public void ProducersPerConnection(int count = 1) => _owner._producers = count;

        public void ConsumersPerConnection(int count = 1) => _owner._consumers = count;
    }

    private sealed class TlsConfigurator : ITlsConfigurator
    {
        private string? _serverName;
        private string? _clientCertPath;
        private string? _clientCertPassword;
        private SslPolicyErrors _acceptableErrors;
        private SslProtocols _protocolVersion;

        public void ServerName(string name)
        {
            ArgumentNullException.ThrowIfNull(name);
            _serverName = name;
        }

        public void ClientCertificate(string path, string? password = null)
        {
            ArgumentException.ThrowIfNullOrWhiteSpace(path);
            _clientCertPath = path;
            _clientCertPassword = password;
        }

        public void AcceptableCertificateErrors(SslPolicyErrors errors) => _acceptableErrors = errors;

        public void ProtocolVersion(SslProtocols version) => _protocolVersion = version;

        public StreamBusTlsOptions Build() => new()
        {
            Enabled = true,
            ServerName = _serverName,
            ClientCertPath = _clientCertPath,
            ClientCertPassword = _clientCertPassword,
            AcceptableCertificateErrors = _acceptableErrors,
            ProtocolVersion = _protocolVersion,
        };
    }

    internal static Type ResolveConsumerMessageType(Type consumerType)
    {
        ArgumentNullException.ThrowIfNull(consumerType);
        var matches = consumerType.GetInterfaces()
            .Where(i => i.IsGenericType && i.GetGenericTypeDefinition() == typeof(IConsumer<>))
            .Select(i => i.GetGenericArguments()[0])
            .Distinct()
            .ToList();
        if (matches.Count == 0)
            throw new StreamBusConfigurationException($"Consumer {consumerType.FullName} must implement IConsumer<T> for some message type T.");
        if (matches.Count > 1)
            throw new StreamBusConfigurationException($"Consumer {consumerType.FullName} implements IConsumer<T> for multiple message types ({string.Join(", ", matches.Select(t => t.FullName))}). One consumer class handles one message type.");
        return matches[0];
    }

    private sealed class ReceiveEndpointConfigurator : IReceiveEndpointConfigurator
    {
        private readonly string _stream;
        private Type? _consumerType;
        private Type? _messageType;
        private string? _consumerGroup;
        private bool _super;
        private int? _superPartitions;
        private TimeSpan? _maxAge;
        private ulong? _maxLengthBytes;
        private int? _maxSegmentSizeBytes;
        private StreamLeaderLocator? _locator;
        private bool _sac = true;
        private StartOffset _offset = global::StreamBus.StartOffset.ResumeOrFirst;
        private OffsetStoreOptions _store = new();
        private RetryPolicy? _retryOverride;
        private DeadLetterOptions? _deadLetter;
        private TimeSpan? _errorPublishTimeout;
        private Func<ExhaustedContext, CancellationToken, Task>? _onExhausted;
        private ReadinessPolicy _readiness = ReadinessPolicy.Default;
        private readonly List<ConsumeFilterRegistration> _consumeFilters = new();

        public ReceiveEndpointConfigurator(string stream) => _stream = stream;

        public void Consumer<TConsumer>(string consumerGroup) where TConsumer : class
        {
            if (_consumerType is not null)
                throw new StreamBusConfigurationException($"ReceiveEndpoint '{_stream}': Consumer<T> may be called exactly once per endpoint.");
            if (string.IsNullOrWhiteSpace(consumerGroup))
                throw new StreamBusConfigurationException($"ReceiveEndpoint '{_stream}': consumer group must be supplied (no default exists to collide on).");
            if (Encoding.UTF8.GetByteCount(consumerGroup) > StreamBusLimits.MaxConsumerGroupBytes)
                throw new StreamBusConfigurationException($"ReceiveEndpoint '{_stream}': consumer group exceeds the broker's 255-byte limit.");
            var consumerType = typeof(TConsumer);
            var messageType = ResolveConsumerMessageType(consumerType);
            _consumerType = consumerType;
            _messageType = messageType;
            _consumerGroup = consumerGroup;
        }

        public void SuperStream(bool enabled = true) => _super = enabled;
        public void SingleActiveConsumer(bool enabled = true) => _sac = enabled;
        public void StartOffset(StartOffset startOffset) => _offset = startOffset;
        public void OffsetStore(OffsetStoreOptions options)
        {
            ArgumentNullException.ThrowIfNull(options);
            _store = options;
        }
        public void Readiness(ReadinessPolicy policy)
        {
            ArgumentNullException.ThrowIfNull(policy);
            _readiness = policy;
        }
        public void OnExhausted(Func<ExhaustedContext, CancellationToken, Task> handler)
        {
            ArgumentNullException.ThrowIfNull(handler);
            _onExhausted = handler;
        }
        public void UseRetry(Action<IRetryConfigurator> configure)
        {
            ArgumentNullException.ThrowIfNull(configure);
            var retry = new RetryConfigurator();
            configure(retry);
            _retryOverride = ((IRetryConfigurator)retry).BuildPolicy();
        }
        public void UseRetry(RetryPolicy policy)
        {
            ArgumentNullException.ThrowIfNull(policy);
            _retryOverride = policy;
        }
        [RequiresUnreferencedCode(RuntimeGeneric.TrimMessage)]
        [RequiresDynamicCode(RuntimeGeneric.AotMessage)]
        public void UseConsumeFilter<[DynamicallyAccessedMembers(RuntimeGeneric.FilterMembers)] TFilter>() where TFilter : class
        {
            AddConsumeFilter(ConsumeFilterRegistration.ForType(typeof(TFilter)));
        }
        public void UseConsumeFilter<[DynamicallyAccessedMembers(RuntimeGeneric.FilterMembers)] TFilter, TMessage>() where TFilter : class, IStreamConsumeFilter<TMessage>
        {
            AddConsumeFilter(ConsumeFilterRegistration.ForTyped<TFilter, TMessage>());
        }
        [RequiresUnreferencedCode(RuntimeGeneric.TrimMessage)]
        [RequiresDynamicCode(RuntimeGeneric.AotMessage)]
        public void UseConsumeFilter([DynamicallyAccessedMembers(RuntimeGeneric.FilterMembers)] Type filterType)
        {
            ArgumentNullException.ThrowIfNull(filterType);
            AddConsumeFilter(ConsumeFilterRegistration.ForType(filterType));
        }
        public void UseConsumeFilter(Func<IServiceProvider, IStreamConsumeFilter> factory)
        {
            ArgumentNullException.ThrowIfNull(factory);
            AddConsumeFilter(ConsumeFilterRegistration.ForFactory(factory));
        }
        public void UseConsumeFilter(IStreamConsumeFilter instance)
        {
            ArgumentNullException.ThrowIfNull(instance);
            AddConsumeFilter(ConsumeFilterRegistration.ForInstance(instance));
        }
        private void AddConsumeFilter(ConsumeFilterRegistration filter)
        {
            if (filter.FilterType is { } type && _consumeFilters.Any(filter.ConflictsWith))
            {
                throw new StreamBusConfigurationException(
                    $"Consume filter '{type}' is already registered on this endpoint for one of its message types; duplicate registration would run it twice.");
            }
            _consumeFilters.Add(filter);
        }
        public void DeadLetterToStream(string name, Action<IStreamOptionsBuilder>? configure = null)
        {
            EnsureDeadLetterSlot("stream");
            ArgumentException.ThrowIfNullOrWhiteSpace(name);
            var builder = new StreamOptionsBuilder();
            configure?.Invoke(builder);
            var spec = builder.Build();
            StreamOptionsValidation.Validate(spec, $"ReceiveEndpoint '{_stream}' error-stream destination '{name.Trim()}'", allowPartitions: false);
            _deadLetter = new DeadLetterOptions { Name = name.Trim(), StreamSpec = spec };
        }
        public void DeadLetterToStream(string name, StreamOptions spec)
        {
            EnsureDeadLetterSlot("stream");
            ArgumentException.ThrowIfNullOrWhiteSpace(name);
            ArgumentNullException.ThrowIfNull(spec);
            StreamOptionsValidation.Validate(spec, $"ReceiveEndpoint '{_stream}' error-stream destination '{name.Trim()}'", allowPartitions: false);
            _deadLetter = new DeadLetterOptions { Name = name.Trim(), StreamSpec = spec };
        }
        public void DeadLetterToQueue(string name, DeadLetterQueueType type)
            => SetDeadLetterQueue(name, type, new ErrorQueueOptions());

        public void DeadLetterToQueue(string name, DeadLetterQueueType type, Action<IErrorQueueOptionsBuilder> configure)
        {
            ArgumentNullException.ThrowIfNull(configure);
            var builder = new ErrorQueueOptionsBuilder();
            configure(builder);
            SetDeadLetterQueue(name, type, builder.Build());
        }

        private void SetDeadLetterQueue(string name, DeadLetterQueueType type, ErrorQueueOptions options)
        {
            EnsureDeadLetterSlot("queue");
            ArgumentException.ThrowIfNullOrWhiteSpace(name);
            if (!Enum.IsDefined(type))
                throw new StreamBusConfigurationException($"ReceiveEndpoint '{_stream}': DeadLetterQueueType has an undefined value.");
            _deadLetter = new DeadLetterOptions { Name = name.Trim(), QueueType = type, QueueOptions = options };
        }
        public void ErrorPublishTimeout(TimeSpan? timeout)
        {
            if (timeout is not { } value)
            {
                _errorPublishTimeout = null;
                return;
            }
            if (value <= TimeSpan.Zero)
                throw new StreamBusConfigurationException($"ReceiveEndpoint '{_stream}': ErrorPublishTimeout must be positive.");
            _errorPublishTimeout = value < TimeSpan.FromSeconds(1) ? TimeSpan.FromSeconds(1) : value;
        }
        private void EnsureDeadLetterSlot(string attempted)
        {
            if (_deadLetter is not { } existing)
                return;
            throw new StreamBusConfigurationException(
                $"ReceiveEndpoint '{_stream}': a dead-letter destination is already configured ({Describe(existing)}); at most one is allowed per endpoint. Remove the duplicate {attempted} call.");
        }
        private static string Describe(DeadLetterOptions destination)
            => destination.QueueType is { } queueType
                ? $"queue ({queueType}) '{destination.Name}'"
                : $"stream '{destination.Name}'";
        public void Partitions(int partitions)
        {
            _super = true;
            _superPartitions = partitions;
        }
        public void MaxAge(TimeSpan maxAge) => _maxAge = maxAge;
        public void MaxLengthBytes(ulong maxLengthBytes) => _maxLengthBytes = maxLengthBytes;
        public void MaxSegmentSizeBytes(int maxSegmentSizeBytes) => _maxSegmentSizeBytes = maxSegmentSizeBytes;
        public void LeaderLocator(StreamLeaderLocator locator) => _locator = locator;

        private void ValidateEndpointTypedFilters(Type messageType)
        {
            foreach (var filter in _consumeFilters)
            {
                if (filter.Kind != FilterRegistrationKind.ConcreteTyped)
                    continue;
                if (filter.TypedMessageTypes.Contains(messageType))
                    continue;
                throw new StreamBusConfigurationException(
                    $"ReceiveEndpoint '{_stream}': consume filter '{filter.FilterType}' does not implement IStreamConsumeFilter<{messageType.Name}> for this endpoint's message type '{messageType.Name}'.");
            }
        }

        public ReceiveEndpointDescriptor Build()
        {
            if (_consumerType is null || _messageType is null || _consumerGroup is null)
                throw new StreamBusConfigurationException($"ReceiveEndpoint '{_stream}': Consumer<T>(\"group\") must be called exactly once per endpoint.");
            ValidateEndpointTypedFilters(_messageType);
            var descriptor = new ReceiveEndpointDescriptor
            {
                Stream = _stream,
                MessageType = _messageType,
                ConsumerType = _consumerType,
                ConsumerGroup = _consumerGroup,
                IsSuperStream = _super,
                Partitions = _superPartitions,
                MaxAge = _maxAge,
                MaxLengthBytes = _maxLengthBytes,
                MaxSegmentSizeBytes = _maxSegmentSizeBytes,
                LeaderLocator = _locator,
                SingleActiveConsumer = _sac,
                StartOffset = _offset,
                OffsetStore = _store,
                RetryOverride = _retryOverride,
                DeadLetter = _deadLetter,
                ErrorPublishTimeout = _errorPublishTimeout,
                OnExhausted = _onExhausted,
                Readiness = _readiness,
                ConsumeFilters = _consumeFilters.ToArray(),
            };
            descriptor.Validate();
            return descriptor;
        }
    }
}

/// <summary>AddStreamBus entry points for the service collection.</summary>
public static class StreamBusConfiguratorExtensions
{
    /// <summary>
    /// Adds the default bus (<see cref="IStreamBus"/>) configured by
    /// <paramref name="configure"/>: topology declarations, retry schedules,
    /// and receive endpoints are validated and registered immediately.
    /// </summary>
    /// <param name="services">The service collection.</param>
    /// <param name="configure">The bus configuration callback.</param>
    /// <exception cref="StreamBusConfigurationException">The configuration is invalid or contradictory.</exception>
    public static IServiceCollection AddStreamBus(
        this IServiceCollection services,
        Action<IStreamRegistrationContext, IStreamBusConfigurator> configure)
    {
        ArgumentNullException.ThrowIfNull(services);
        ArgumentNullException.ThrowIfNull(configure);
        return AddCore(services, typeof(DefaultBus), configure);
    }

    /// <summary>
    /// Adds a named bus (<typeparamref name="TBus"/>, exposed as
    /// <c>IStreamBus&lt;TBus&gt;</c>) configured by <paramref name="configure"/>:
    /// topology declarations, retry schedules, and receive endpoints are
    /// validated and registered immediately.
    /// </summary>
    /// <typeparam name="TBus">Marker type that identifies this bus.</typeparam>
    /// <param name="services">The service collection.</param>
    /// <param name="configure">The bus configuration callback.</param>
    /// <exception cref="StreamBusConfigurationException">The configuration is invalid or contradictory.</exception>
    public static IServiceCollection AddStreamBus<TBus>(
        this IServiceCollection services,
        Action<IStreamRegistrationContext, IStreamBusConfigurator> configure)
    {
        ArgumentNullException.ThrowIfNull(services);
        ArgumentNullException.ThrowIfNull(configure);
        return AddCore(services, typeof(TBus), configure);
    }

    private static IServiceCollection AddCore(
        IServiceCollection services,
        Type busType,
        Action<IStreamRegistrationContext, IStreamBusConfigurator>? configure)
    {
        var configurator = new StreamBusConfigurator(busType);
        var context = new StreamRegistrationContext(services);
        configure?.Invoke(context, configurator);
        var options = configurator.Build(busType);
        var publishStore = ServiceCollectionExtensions.GetOrCreatePublishStore(services);
        foreach (var p in configurator.Publishes)
            publishStore.Add(busType, p.MessageType, p.Stream, p.IsSuperStream, p.StreamSpec);
        foreach (var filter in configurator.PublishFilters)
        {
            publishStore.AddFilter(busType, filter);
            // Publish filters are resolved once when the bus is built and
            // cached for the bus lifetime (PublishPipelineCache). Registering
            // them as singletons makes that contract explicit; the cache
            // rejects scoped/transient registrations at bus construction.
            if (filter.FilterType is { } filterType && filter.Instance is null)
                services.TryAddSingleton(filterType, filterType);
        }
        publishStore.Seal(busType);
        var consumeFilterStore = ServiceCollectionExtensions.GetOrCreateConsumeFilterStore(services);
        foreach (var filter in configurator.ConsumeFilters)
        {
            consumeFilterStore.AddFilter(busType, filter);
            // Consume filters are resolved once when the consumer attaches and
            // cached for the bus lifetime (StreamConsumerRegistry); see the
            // matching publish-side comment above. Registering them as
            // singletons makes that contract explicit; the registry rejects
            // scoped/transient registrations when the pipeline is built.
            if (filter.FilterType is { } filterType && filter.Instance is null)
                services.TryAddSingleton(filterType, filterType);
        }
        consumeFilterStore.Seal(busType);
        var endpointStore = ServiceCollectionExtensions.GetOrCreateEndpointStore(services);
        var consumerStore = ServiceCollectionExtensions.GetOrCreateConsumerStore(services);
        foreach (var descriptor in configurator.Endpoints)
        {
            endpointStore.Add(busType, descriptor);
            services.TryAddTransient(descriptor.ConsumerType, descriptor.ConsumerType);
            foreach (var filter in descriptor.ConsumeFilters)
            {
                if (filter.FilterType is { } filterType && filter.Instance is null)
                    services.TryAddSingleton(filterType, filterType);
            }
            var busName = busType.FullName ?? busType.Name;
            var derived = RetryDerivation.Derive(
                configurator.GlobalRetry,
                descriptor.RetryOverride,
                descriptor.DeadLetter,
                descriptor.OnExhausted is not null,
                descriptor.Stream,
                busName);
            consumerStore.Add(busType, descriptor.MessageType, descriptor.ConsumerType, descriptor.Stream, new ConsumeOptions
            {
                IsSuperStream = descriptor.IsSuperStream,
                ConsumerGroup = descriptor.ConsumerGroup,
                SingleActiveConsumer = descriptor.SingleActiveConsumer,
                StartOffset = descriptor.StartOffset,
                OffsetStore = descriptor.OffsetStore,
                Retry = derived.Policy,
                ErrorStream = derived.Kind == ExhaustionKind.StreamHandoff ? derived.Stream : null,
                Readiness = descriptor.Readiness,
                ErrorPublishTimeout = descriptor.ErrorPublishTimeout,
                OnExhausted = derived.Kind == ExhaustionKind.Callback ? descriptor.OnExhausted : null,
                DeadLetter = derived.Kind == ExhaustionKind.QueueHandoff ? derived.Queue : null,
                ShadowedDeadLetter = derived.DestinationShadowed ? descriptor.DeadLetter : null,
                ConsumeFilters = descriptor.ConsumeFilters,
                MessageType = descriptor.MessageType,
            });
        }
        ServiceCollectionExtensions.AddShared(services, busType, options);
        if (busType == typeof(DefaultBus))
            services.TryAddSingleton<IStreamBus>(sp => sp.GetRequiredService<IStreamBusRegistry>().GetOrCreateBus(options.Vhost));
        else
            services.TryAddSingleton(typeof(IStreamBus<>).MakeGenericType(busType), sp => ((IStreamBusRegistry)sp.GetRequiredService<IStreamBusRegistry>()).GetOrCreateEntry(busType, options.Vhost).Bus);
        return services;
    }
}
