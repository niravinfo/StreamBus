using System.Text;
using System.Text.Json;
using Microsoft.Extensions.Configuration;
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
    void ProducersPerConnection(int count = 1);
    void ConsumersPerConnection(int count = 1);
}

public interface IStreamBusConfigurator
{
    void Host(string host, string vhost, Action<IHostConfigurator> configure);
    void Serializer(IStreamSerializer serializer);
    void Serializer(Action<JsonSerializerOptions> configure);
    void DeclareStream(string name, Action<StreamDeclarationKnobsBuilder>? configure = null);
    void DeclareSuperStream(string name, int partitions, Action<StreamDeclarationKnobsBuilder>? configure = null);
    void Publish<TMessage>(string stream);
    void PublishSuper<TMessage>(string stream);
    void ReceiveEndpoint(string stream, Action<IReceiveEndpointConfigurator> configure);
    void UseMessageRetry(Action<IRetryConfigurator> configure);
    void UseRetry(RetryPolicy policy);
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
    void DeadLetterTo(string name, DeadLetterType type);
    void OnExhausted(Func<ExhaustedContext, CancellationToken, Task> handler);
    void SetSuperStream(int partitions);
    void SetMaxAge(TimeSpan maxAge);
    void SetMaxLengthBytes(ulong maxLengthBytes);
    void SetMaxSegmentSizeBytes(int maxSegmentSizeBytes);
    void SetLeaderLocator(StreamLeaderLocator locator);
}

public sealed record ReceiveEndpointDescriptor
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
    public Func<ExhaustedContext, CancellationToken, Task>? OnExhausted { get; init; }
    public ReadinessPolicy Readiness { get; init; } = ReadinessPolicy.FailFastPolicy;
}

public sealed class StreamBusEndpointStore
{
    private readonly List<(Type BusMarker, ReceiveEndpointDescriptor Descriptor)> _endpoints = new();

    public void Add(Type busMarker, ReceiveEndpointDescriptor descriptor)
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
    private int _producers = 1;
    private int _consumers = 1;
    private string? _sectionPath;
    private IStreamSerializer _serializer = SystemTextJsonStreamSerializer.Default;
    private readonly List<StreamDeclaration> _streams = new();
    private readonly List<SuperStreamDeclaration> _supers = new();
    private readonly List<(Type MessageType, string Stream, bool IsSuper)> _publishes = new();
    private readonly List<ReceiveEndpointDescriptor> _endpoints = new();
    private RetryPolicy? _globalRetry;
    private bool _hostCalled;

    internal StreamBusConfigurator(Type busType)
    {
        ArgumentNullException.ThrowIfNull(busType);
        _busType = busType;
    }

    internal IReadOnlyList<(Type MessageType, string Stream, bool IsSuper)> Publishes => _publishes;

    internal IReadOnlyList<ReceiveEndpointDescriptor> Endpoints => _endpoints;

    internal RetryPolicy? GlobalRetry => _globalRetry;

    internal void ApplySectionPrimitives(SectionPrimitives primitives, string sectionPath)
    {
        ArgumentNullException.ThrowIfNull(primitives);
        ArgumentException.ThrowIfNullOrWhiteSpace(sectionPath);
        _host = primitives.Host;
        _vhost = primitives.Vhost;
        if (primitives.UserName is not null) _user = primitives.UserName;
        if (primitives.Password is not null) _password = primitives.Password;
        _streamPort = primitives.StreamPort;
        _amqpPort = primitives.AmqpPort;
        _heartbeat = primitives.Heartbeat;
        _producers = primitives.ProducersPerConnection;
        _consumers = primitives.ConsumersPerConnection;
        _sectionPath = sectionPath;
    }

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

    public void DeclareStream(string name, Action<StreamDeclarationKnobsBuilder>? configure = null)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(name);
        var knobs = new StreamDeclarationKnobsBuilder();
        configure?.Invoke(knobs);
        var declaration = knobs.BuildStream(name.Trim());
        TopologyValidation.ValidateStream(declaration);
        _streams.Add(declaration);
    }

    public void DeclareSuperStream(string name, int partitions, Action<StreamDeclarationKnobsBuilder>? configure = null)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(name);
        var knobs = new StreamDeclarationKnobsBuilder();
        configure?.Invoke(knobs);
        var declaration = knobs.BuildSuper(name.Trim(), partitions);
        TopologyValidation.ValidateSuper(declaration);
        _supers.Add(declaration);
    }

    public void Publish<TMessage>(string stream)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(stream);
        _publishes.Add((typeof(TMessage), stream.Trim(), false));
    }

    public void PublishSuper<TMessage>(string stream)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(stream);
        _publishes.Add((typeof(TMessage), stream.Trim(), true));
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

    internal StreamBusOptions Build(Type busType)
    {
        ArgumentNullException.ThrowIfNull(busType);
        if (busType != _busType)
            throw new StreamBusConfigurationException($"Internal error: configurator for {_busType.FullName} built as {busType.FullName}.");
        if (!_hostCalled && _sectionPath is null)
            throw new StreamBusConfigurationException($"Bus {busType.FullName}: Host must be configured exactly once.");
        if (string.IsNullOrWhiteSpace(_host))
            throw new StreamBusConfigurationException($"Bus {busType.FullName}: Host host must be supplied.");
        if (string.IsNullOrWhiteSpace(_vhost))
            throw new StreamBusConfigurationException($"Bus {busType.FullName}: Host vhost must be supplied.");
        if ((string.IsNullOrWhiteSpace(_user) || string.IsNullOrWhiteSpace(_password)) && _sectionPath is not null && !_hostCalled)
            throw new StreamBusConfigurationException($"Bus {busType.FullName}: StreamBus UserName and Password must be supplied (section '{_sectionPath}': UserName and Password, or cfg.Host(host, vhost, h => h.Credentials(user, password))).");
        if (_streamPort is < 1 or > 65535)
            throw new StreamBusConfigurationException($"Bus {busType.FullName}: StreamPort must be in 1..65535.");
        if (_amqpPort is < 1 or > 65535)
            throw new StreamBusConfigurationException($"Bus {busType.FullName}: AmqpPort must be in 1..65535.");
        if (_heartbeat <= TimeSpan.Zero)
            throw new StreamBusConfigurationException($"Bus {busType.FullName}: Heartbeat must be positive.");
        if (_producers < 1 || _producers > byte.MaxValue)
            throw new StreamBusConfigurationException($"Bus {busType.FullName}: ProducersPerConnection must be in 1..{byte.MaxValue}.");
        if (_consumers < 1 || _consumers > byte.MaxValue)
            throw new StreamBusConfigurationException($"Bus {busType.FullName}: ConsumersPerConnection must be in 1..{byte.MaxValue}.");

        var plainNames = new HashSet<string>(_streams.Select(s => s.Name), StringComparer.Ordinal);
        foreach (var s in _supers)
        {
            if (plainNames.Contains(s.Name))
                throw new StreamConfigurationConflictException(s.Name, $"super stream '{s.Name}'", $"plain stream '{s.Name}'");
        }

        var options = new StreamBusOptions
        {
            Vhost = _vhost.Trim(),
            UserName = _user,
            Password = _password,
            Host = _host.Trim(),
            StreamPort = _streamPort,
            AmqpPort = _amqpPort,
            Heartbeat = _heartbeat,
            Connection = new StreamBusConnectionOptions
            {
                ProducersPerConnection = _producers,
                ConsumersPerConnection = _consumers,
            },
            Topology = new StreamTopologyDeclarations
            {
                Streams = _streams.ToList(),
                SuperStreams = _supers.ToList(),
            },
            Serializer = _serializer,
        };
        StreamBusCredentialValidator.Validate(options);
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
            if (port is < 1 or > 65535)
                throw new StreamBusConfigurationException("StreamPort must be in 1..65535.");
            _owner._streamPort = port;
        }

        public void AmqpPort(int port = 5672)
        {
            if (port is < 1 or > 65535)
                throw new StreamBusConfigurationException("AmqpPort must be in 1..65535.");
            _owner._amqpPort = port;
        }

        public void Heartbeat(TimeSpan heartbeat)
        {
            if (heartbeat <= TimeSpan.Zero)
                throw new StreamBusConfigurationException("Heartbeat must be positive.");
            _owner._heartbeat = heartbeat;
        }

        public void ProducersPerConnection(int count = 1)
        {
            if (count < 1 || count > byte.MaxValue)
                throw new StreamBusConfigurationException($"ProducersPerConnection must be in 1..{byte.MaxValue}.");
            _owner._producers = count;
        }

        public void ConsumersPerConnection(int count = 1)
        {
            if (count < 1 || count > byte.MaxValue)
                throw new StreamBusConfigurationException($"ConsumersPerConnection must be in 1..{byte.MaxValue}.");
            _owner._consumers = count;
        }
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
        private Func<ExhaustedContext, CancellationToken, Task>? _onExhausted;
        private ReadinessPolicy _readiness = ReadinessPolicy.FailFastPolicy;

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
        public void DeadLetterTo(string name, DeadLetterType type)
        {
            ArgumentException.ThrowIfNullOrWhiteSpace(name);
            if (!Enum.IsDefined(type))
                throw new StreamBusConfigurationException($"ReceiveEndpoint '{_stream}': DeadLetterType has an undefined value.");
            _deadLetter = new DeadLetterOptions { Name = name.Trim(), Type = type };
        }
        public void SetSuperStream(int partitions)
        {
            if (partitions < 1)
                throw new StreamBusConfigurationException($"ReceiveEndpoint '{_stream}': super-stream partitions must be >= 1.");
            _super = true;
            _superPartitions = partitions;
        }
        public void SetMaxAge(TimeSpan maxAge)
        {
            if (maxAge <= TimeSpan.Zero)
                throw new StreamBusConfigurationException($"ReceiveEndpoint '{_stream}': MaxAge must be positive when set.");
            _maxAge = maxAge;
        }
        public void SetMaxLengthBytes(ulong maxLengthBytes)
        {
            if (maxLengthBytes < 1)
                throw new StreamBusConfigurationException($"ReceiveEndpoint '{_stream}': MaxLengthBytes must be >= 1 when set.");
            _maxLengthBytes = maxLengthBytes;
        }
        public void SetMaxSegmentSizeBytes(int maxSegmentSizeBytes)
        {
            if (maxSegmentSizeBytes < 1)
                throw new StreamBusConfigurationException($"ReceiveEndpoint '{_stream}': MaxSegmentSizeBytes must be >= 1 when set.");
            _maxSegmentSizeBytes = maxSegmentSizeBytes;
        }
        public void SetLeaderLocator(StreamLeaderLocator locator)
        {
            if (!Enum.IsDefined(locator))
                throw new StreamBusConfigurationException($"ReceiveEndpoint '{_stream}': LeaderLocator has an undefined value.");
            _locator = locator;
        }

        public ReceiveEndpointDescriptor Build()
        {
            if (_consumerType is null || _messageType is null || _reference is null)
                throw new StreamBusConfigurationException($"ReceiveEndpoint '{_stream}': Consumer<T>(\"group\") must be called exactly once per endpoint.");
            return new ReceiveEndpointDescriptor
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
                OnExhausted = _onExhausted,
                Readiness = _readiness,
            };
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
        return AddCore(services, typeof(DefaultBus), section: null, configure);
    }

    public static IServiceCollection AddStreamBus<TBus>(
        this IServiceCollection services,
        Action<IStreamRegistrationContext, IStreamBusConfigurator> configure)
    {
        ArgumentNullException.ThrowIfNull(services);
        ArgumentNullException.ThrowIfNull(configure);
        return AddCore(services, typeof(TBus), section: null, configure);
    }

    public static IServiceCollection AddStreamBus(
        this IServiceCollection services,
        IConfigurationSection section,
        Action<IStreamRegistrationContext, IStreamBusConfigurator>? configure = null)
    {
        ArgumentNullException.ThrowIfNull(services);
        ArgumentNullException.ThrowIfNull(section);
        return AddCore(services, typeof(DefaultBus), section, configure);
    }

    public static IServiceCollection AddStreamBus<TBus>(
        this IServiceCollection services,
        IConfigurationSection section,
        Action<IStreamRegistrationContext, IStreamBusConfigurator>? configure = null)
    {
        ArgumentNullException.ThrowIfNull(services);
        ArgumentNullException.ThrowIfNull(section);
        return AddCore(services, typeof(TBus), section, configure);
    }

    private static IServiceCollection AddCore(
        IServiceCollection services,
        Type busType,
        IConfigurationSection? section,
        Action<IStreamRegistrationContext, IStreamBusConfigurator>? configure)
    {
        var configurator = new StreamBusConfigurator(busType);
        if (section is not null)
            StreamBusConfigurationBinding.Apply(configurator, section);
        var context = new StreamRegistrationContext(services);
        configure?.Invoke(context, configurator);
        var options = configurator.Build(busType);
        var publishStore = ServiceCollectionExtensions.GetOrCreatePublishStore(services);
        foreach (var p in configurator.Publishes)
            publishStore.Add(busType, p.MessageType, p.Stream, p.IsSuper);
        var endpointStore = ServiceCollectionExtensions.GetOrCreateEndpointStore(services);
        var consumerStore = ServiceCollectionExtensions.GetOrCreateConsumerStore(services);
        foreach (var descriptor in configurator.Endpoints)
        {
            endpointStore.Add(busType, descriptor);
            services.TryAddTransient(descriptor.ConsumerType, descriptor.ConsumerType);
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
                ExhaustionCallback = derived.Kind == ExhaustionKind.Callback ? descriptor.OnExhausted : null,
                DeadLetter = derived.Kind == ExhaustionKind.QueueHandoff ? derived.Queue : null,
                ShadowedDeadLetter = derived.DestinationShadowed ? descriptor.DeadLetter : null,
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
