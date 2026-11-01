using System.Diagnostics.CodeAnalysis;
using System.Text;
using System.Text.Json;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;

namespace StreamBus;

public interface IStreamRegistrationContext
{
    IServiceCollection Services { get; }
}

public interface IHostConfigurator
{
    void Credentials(string user, string password);
    void StreamPort(int port = 5552);
    void AmqpPort(int port = 5672);
    void Heartbeat(TimeSpan heartbeat);
    void MonitorInterval(TimeSpan interval);
    void ProducersPerConnection(int count = 1);
    void ConsumersPerConnection(int count = 1);
}

public interface IStreamBusConfigurator
{
    void Host(string host, string vhost, Action<IHostConfigurator> configure);
    void Serializer(IStreamSerializer serializer);
    void Serializer(Action<JsonSerializerOptions> configure);
    void Publish<TMessage>(string stream, Action<IStreamOptionsBuilder>? configure = null);
    void Publish<TMessage>(string stream, StreamOptions spec);
    void PublishSuper<TMessage>(string stream, Action<ISuperStreamOptionsBuilder> configure);
    void PublishSuper<TMessage>(string stream, StreamOptions spec);
    void ReceiveEndpoint(string stream, Action<IReceiveEndpointConfigurator> configure);
    void UseMessageRetry(Action<IRetryConfigurator> configure);
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
    [RequiresUnreferencedCode(RuntimeGeneric.TrimMessage)]
    [RequiresDynamicCode(RuntimeGeneric.AotMessage)]
    void UseConsumeFilter<[DynamicallyAccessedMembers(RuntimeGeneric.FilterMembers)] TFilter>() where TFilter : class;
    /// <summary>
    /// Registers a typed consume filter whose endpoint message type is
    /// <typeparamref name="TMessage"/>. Unlike
    /// <see cref="UseConsumeFilter{TFilter}()"/>, this overload closes the typed
    /// adapter at the call site and needs no runtime generic closing.
    /// </summary>
    void UseConsumeFilter<[DynamicallyAccessedMembers(RuntimeGeneric.FilterMembers)] TFilter, TMessage>() where TFilter : class, IStreamConsumeFilter<TMessage>;
    [RequiresUnreferencedCode(RuntimeGeneric.TrimMessage)]
    [RequiresDynamicCode(RuntimeGeneric.AotMessage)]
    void UseConsumeFilter([DynamicallyAccessedMembers(RuntimeGeneric.FilterMembers)] Type filterType);
    void UseConsumeFilter(Func<IServiceProvider, IStreamConsumeFilter> factory);
    void UseConsumeFilter(IStreamConsumeFilter instance);
}

public interface IReceiveEndpointConfigurator
{
    void Consumer<TConsumer>(string consumerGroup) where TConsumer : class;
    void SuperStream(bool enabled = true);
    void SingleActiveConsumer(bool enabled = true);
    void Offset(ConsumeOffset offset);
    void OffsetStore(OffsetStoreOptions options);
    void Readiness(ReadinessPolicy policy);
    void UseMessageRetry(Action<IRetryConfigurator> configure);
    void UseRetry(RetryPolicy policy);
    /// <summary>
    /// Registers a consume filter on this endpoint and discovers its typed
    /// message interfaces at runtime. Use
    /// <see cref="UseConsumeFilter{TFilter,TMessage}()"/> to avoid closing the
    /// adapter at runtime.
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
    /// serializer are still reflection-based).
    /// </summary>
    void UseConsumeFilter<[DynamicallyAccessedMembers(RuntimeGeneric.FilterMembers)] TFilter, TMessage>() where TFilter : class, IStreamConsumeFilter<TMessage>;
    [RequiresUnreferencedCode(RuntimeGeneric.TrimMessage)]
    [RequiresDynamicCode(RuntimeGeneric.AotMessage)]
    void UseConsumeFilter([DynamicallyAccessedMembers(RuntimeGeneric.FilterMembers)] Type filterType);
    void UseConsumeFilter(Func<IServiceProvider, IStreamConsumeFilter> factory);
    void UseConsumeFilter(IStreamConsumeFilter instance);
    void DeadLetterToStream(string name, Action<IStreamOptionsBuilder>? configure = null);
    void DeadLetterToStream(string name, StreamOptions spec);
    void DeadLetterToQueue(string name, DeadLetterQueueType type);
    void DeadLetterToQueue(string name, DeadLetterQueueType type, Action<IErrorQueueOptionsBuilder> configure);
    void ErrorHandoffTimeout(TimeSpan timeout);
    void OnExhausted(Func<ExhaustedContext, CancellationToken, Task> handler);
    void Partitions(int partitions);
    void MaxAge(TimeSpan maxAge);
    void MaxLengthBytes(ulong maxLengthBytes);
    void MaxSegmentSizeBytes(int maxSegmentSizeBytes);
    void LeaderLocator(StreamLeaderLocator locator);
}

internal sealed record ReceiveEndpointDescriptor
{
    public required string Stream { get; init; }
    public required Type MessageType { get; init; }
    public required Type ConsumerType { get; init; }
    public required string Reference { get; init; }
    public bool IsSuperStream { get; init; }
    public int? SuperPartitions { get; init; }
    public TimeSpan? MaxAge { get; init; }
    public ulong? MaxLengthBytes { get; init; }
    public int? MaxSegmentSizeBytes { get; init; }
    public StreamLeaderLocator? LeaderLocator { get; init; }
    public bool SingleActiveConsumer { get; init; } = true;
    public ConsumeOffset Offset { get; init; } = ConsumeOffset.ResumeOrFirst;
    public OffsetStoreOptions OffsetStore { get; init; } = new();
    public RetryPolicy? RetryOverride { get; init; }
    public DeadLetterOptions? DeadLetter { get; init; }
    public TimeSpan HandoffConfirmTimeout { get; init; } = TimeSpan.FromSeconds(10);
    public Func<ExhaustedContext, CancellationToken, Task>? OnExhausted { get; init; }
    public ReadinessPolicy Readiness { get; init; } = ReadinessPolicy.FailFastPolicy;
    internal IReadOnlyList<ConsumeFilterRegistration> ConsumeFilters { get; init; } = Array.Empty<ConsumeFilterRegistration>();

    internal void Validate()
        => StreamOptionsValidation.Validate(
            new StreamOptions
            {
                Partitions = IsSuperStream ? SuperPartitions : null,
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
    private readonly List<(Type BusMarker, ReceiveEndpointDescriptor Descriptor)> _endpoints = new();

    internal void Add(Type busMarker, ReceiveEndpointDescriptor descriptor)
    {
        ArgumentNullException.ThrowIfNull(busMarker);
        ArgumentNullException.ThrowIfNull(descriptor);
        if (_endpoints.Any(e => e.BusMarker == busMarker && e.Descriptor.Stream == descriptor.Stream && e.Descriptor.ConsumerType == descriptor.ConsumerType && e.Descriptor.Reference == descriptor.Reference))
            throw new StreamBusConfigurationException($"Duplicate receive endpoint for {descriptor.ConsumerType.FullName} on stream '{descriptor.Stream}' with group '{descriptor.Reference}' (marker {busMarker.FullName}).");
        _endpoints.Add((busMarker, descriptor));
    }

    public IReadOnlyList<(Type BusMarker, ReceiveEndpointDescriptor Descriptor)> Endpoints => _endpoints;
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
    private TimeSpan _heartbeat = TimeSpan.FromSeconds(60);
    private TimeSpan _monitorInterval = TimeSpan.FromSeconds(5);
    private int _producers = 1;
    private int _consumers = 1;
    private IStreamSerializer _serializer = SystemTextJsonStreamSerializer.Default;
    private readonly List<(Type MessageType, string Stream, bool IsSuper, StreamOptions Spec)> _publishes = new();
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

    internal IReadOnlyList<(Type MessageType, string Stream, bool IsSuper, StreamOptions Spec)> Publishes => _publishes;

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

    public void UseMessageRetry(Action<IRetryConfigurator> configure)
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
            StreamPort = _streamPort,
            AmqpPort = _amqpPort,
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

        public void StreamPort(int port = 5552) => _owner._streamPort = port;

        public void AmqpPort(int port = 5672) => _owner._amqpPort = port;

        public void Heartbeat(TimeSpan heartbeat) => _owner._heartbeat = heartbeat;

        public void MonitorInterval(TimeSpan interval) => _owner._monitorInterval = interval;

        public void ProducersPerConnection(int count = 1) => _owner._producers = count;

        public void ConsumersPerConnection(int count = 1) => _owner._consumers = count;
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
        private string? _reference;
        private bool _super;
        private int? _superPartitions;
        private TimeSpan? _maxAge;
        private ulong? _maxLengthBytes;
        private int? _maxSegmentSizeBytes;
        private StreamLeaderLocator? _locator;
        private bool _sac = true;
        private ConsumeOffset _offset = ConsumeOffset.ResumeOrFirst;
        private OffsetStoreOptions _store = new();
        private RetryPolicy? _retryOverride;
        private DeadLetterOptions? _deadLetter;
        private TimeSpan _handoffConfirmTimeout = TimeSpan.FromSeconds(10);
        private Func<ExhaustedContext, CancellationToken, Task>? _onExhausted;
        private ReadinessPolicy _readiness = ReadinessPolicy.FailFastPolicy;
        private readonly List<ConsumeFilterRegistration> _consumeFilters = new();

        public ReceiveEndpointConfigurator(string stream) => _stream = stream;

        public void Consumer<TConsumer>(string consumerGroup) where TConsumer : class
        {
            if (_consumerType is not null)
                throw new StreamBusConfigurationException($"ReceiveEndpoint '{_stream}': Consumer<T> may be called exactly once per endpoint.");
            if (string.IsNullOrWhiteSpace(consumerGroup))
                throw new StreamBusConfigurationException($"ReceiveEndpoint '{_stream}': consumer group must be supplied (no default exists to collide on).");
            if (Encoding.UTF8.GetByteCount(consumerGroup) > StreamBusLimits.MaxReferenceBytes)
                throw new StreamBusConfigurationException($"ReceiveEndpoint '{_stream}': consumer group exceeds the broker's 255-byte limit.");
            var consumerType = typeof(TConsumer);
            var messageType = ResolveConsumerMessageType(consumerType);
            _consumerType = consumerType;
            _messageType = messageType;
            _reference = consumerGroup;
        }

        public void SuperStream(bool enabled = true) => _super = enabled;
        public void SingleActiveConsumer(bool enabled = true) => _sac = enabled;
        public void Offset(ConsumeOffset offset) => _offset = offset;
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
        public void UseMessageRetry(Action<IRetryConfigurator> configure)
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
            _deadLetter = new DeadLetterOptions { Name = name.Trim(), Spec = spec };
        }
        public void DeadLetterToStream(string name, StreamOptions spec)
        {
            EnsureDeadLetterSlot("stream");
            ArgumentException.ThrowIfNullOrWhiteSpace(name);
            ArgumentNullException.ThrowIfNull(spec);
            StreamOptionsValidation.Validate(spec, $"ReceiveEndpoint '{_stream}' error-stream destination '{name.Trim()}'", allowPartitions: false);
            _deadLetter = new DeadLetterOptions { Name = name.Trim(), Spec = spec };
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
        public void ErrorHandoffTimeout(TimeSpan timeout)
        {
            if (timeout <= TimeSpan.Zero)
                throw new StreamBusConfigurationException($"ReceiveEndpoint '{_stream}': ErrorHandoffTimeout must be positive.");
            _handoffConfirmTimeout = timeout < TimeSpan.FromSeconds(1) ? TimeSpan.FromSeconds(1) : timeout;
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
            if (_consumerType is null || _messageType is null || _reference is null)
                throw new StreamBusConfigurationException($"ReceiveEndpoint '{_stream}': Consumer<T>(\"group\") must be called exactly once per endpoint.");
            ValidateEndpointTypedFilters(_messageType);
            var descriptor = new ReceiveEndpointDescriptor
            {
                Stream = _stream,
                MessageType = _messageType,
                ConsumerType = _consumerType,
                Reference = _reference,
                IsSuperStream = _super,
                SuperPartitions = _superPartitions,
                MaxAge = _maxAge,
                MaxLengthBytes = _maxLengthBytes,
                MaxSegmentSizeBytes = _maxSegmentSizeBytes,
                LeaderLocator = _locator,
                SingleActiveConsumer = _sac,
                Offset = _offset,
                OffsetStore = _store,
                RetryOverride = _retryOverride,
                DeadLetter = _deadLetter,
                HandoffConfirmTimeout = _handoffConfirmTimeout,
                OnExhausted = _onExhausted,
                Readiness = _readiness,
                ConsumeFilters = _consumeFilters.ToArray(),
            };
            descriptor.Validate();
            return descriptor;
        }
    }
}

public static class StreamBusConfiguratorExtensions
{
    public static IServiceCollection AddStreamBus(
        this IServiceCollection services,
        Action<IStreamRegistrationContext, IStreamBusConfigurator> configure)
    {
        ArgumentNullException.ThrowIfNull(services);
        ArgumentNullException.ThrowIfNull(configure);
        return AddCore(services, typeof(DefaultBus), configure);
    }

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
            publishStore.Add(busType, p.MessageType, p.Stream, p.IsSuper, p.Spec);
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
            if (filter.FilterType is { } filterType && filter.Instance is null)
                services.TryAddTransient(filterType, filterType);
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
                    services.TryAddTransient(filterType, filterType);
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
                Reference = descriptor.Reference,
                SingleActiveConsumer = descriptor.SingleActiveConsumer,
                Offset = descriptor.Offset,
                OffsetStore = descriptor.OffsetStore,
                Retry = derived.Policy,
                ErrorStream = derived.Kind == ExhaustionKind.StreamHandoff ? derived.Stream : null,
                ReadinessPolicy = descriptor.Readiness,
                HandoffConfirmTimeout = descriptor.HandoffConfirmTimeout,
                ExhaustionCallback = derived.Kind == ExhaustionKind.Callback ? descriptor.OnExhausted : null,
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
