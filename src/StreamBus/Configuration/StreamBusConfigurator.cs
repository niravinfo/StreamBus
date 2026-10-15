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

    /// <summary>
    /// Temporary per-bus hosting carry-over. Task 7 replaces this with global AddStreamBusHosting.
    /// </summary>
    void Hosting(Action<StreamBusHostOptionsBuilder> configure);
}

public interface IReceiveEndpointConfigurator
{
    void Consumer<TConsumer>(string consumerGroup) where TConsumer : class;
    void SuperStream(bool enabled = true);
    void SingleActiveConsumer(bool enabled = true);
    void Offset(ConsumeOffset offset);
    void OffsetStore(OffsetStoreOptions options);
    void Readiness(ReadinessPolicy policy);
    void OnRetryExhausted(Func<ErrorHandoff, Task>? hook);
    void SetSuperStream(int partitions);
    void SetMaxAge(TimeSpan maxAge);
    void SetMaxLengthBytes(ulong maxLengthBytes);
    void SetMaxSegmentSizeBytes(int maxSegmentSizeBytes);
    void SetLeaderLocator(StreamLeaderLocator locator);

    /// <summary>
    /// Temporary v1 retry carry-over. Task 4 replaces this with layered UseMessageRetry.
    /// </summary>
    void Retry(RetryPolicy policy);
    /// <summary>
    /// Temporary v1 error-stream carry-over. Task 4 replaces this with DeadLetterTo/OnExhausted.
    /// </summary>
    void ErrorStream(ErrorStreamOptions? options);
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
    public RetryPolicy Retry { get; init; } = new();
    public ErrorStreamOptions? ErrorStream { get; init; }
    public Func<ErrorHandoff, Task>? OnRetryExhausted { get; init; }
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
    private IStreamSerializer _serializer = SystemTextJsonStreamSerializer.Default;
    private readonly List<StreamDeclaration> _streams = new();
    private readonly List<SuperStreamDeclaration> _supers = new();
    private readonly List<(Type MessageType, string Stream, bool IsSuper)> _publishes = new();
    private readonly List<ReceiveEndpointDescriptor> _endpoints = new();
    private StreamBusHostOptions _hosting = new();
    private bool _hostCalled;

    internal StreamBusConfigurator(Type busType)
    {
        ArgumentNullException.ThrowIfNull(busType);
        _busType = busType;
    }

    internal IReadOnlyList<(Type MessageType, string Stream, bool IsSuper)> Publishes => _publishes;

    internal IReadOnlyList<ReceiveEndpointDescriptor> Endpoints => _endpoints;

    internal void ApplySectionVhost(IConfigurationSection section)
    {
        ArgumentNullException.ThrowIfNull(section);
        var v = section.GetSection("Vhost").Value;
        if (!string.IsNullOrWhiteSpace(v))
            _vhost = v.Trim();
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

    public void Hosting(Action<StreamBusHostOptionsBuilder> configure)
    {
        ArgumentNullException.ThrowIfNull(configure);
        var builder = new StreamBusHostOptionsBuilder();
        configure(builder);
        _hosting = builder.Build();
    }

    public void ReceiveEndpoint(string stream, Action<IReceiveEndpointConfigurator> configure)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(stream);
        ArgumentNullException.ThrowIfNull(configure);
        var endpoint = new ReceiveEndpointConfigurator(stream.Trim());
        configure(endpoint);
        _endpoints.Add(endpoint.Build());
    }

    internal StreamBusOptions Build(Type busType)
    {
        ArgumentNullException.ThrowIfNull(busType);
        if (busType != _busType)
            throw new StreamBusConfigurationException($"Internal error: configurator for {_busType.FullName} built as {busType.FullName}.");
        if (!_hostCalled)
            throw new StreamBusConfigurationException($"Bus {busType.FullName}: Host must be configured exactly once.");
        if (string.IsNullOrWhiteSpace(_host))
            throw new StreamBusConfigurationException($"Bus {busType.FullName}: Host host must be supplied.");
        if (string.IsNullOrWhiteSpace(_vhost))
            throw new StreamBusConfigurationException($"Bus {busType.FullName}: Host vhost must be supplied.");
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
            Hosting = _hosting,
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
        private RetryPolicy _retry = new();
        private ErrorStreamOptions? _error;
        private Func<ErrorHandoff, Task>? _hook;
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
        public void OnRetryExhausted(Func<ErrorHandoff, Task>? hook) => _hook = hook;
        public void Retry(RetryPolicy policy)
        {
            ArgumentNullException.ThrowIfNull(policy);
            _retry = policy;
        }
        public void ErrorStream(ErrorStreamOptions? options) => _error = options;
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
                Retry = _retry,
                ErrorStream = _error,
                OnRetryExhausted = _hook,
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
            configurator.ApplySectionVhost(section);
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
            consumerStore.Add(busType, descriptor.MessageType, descriptor.ConsumerType, descriptor.Stream, new ConsumeOptions
            {
                IsSuperStream = descriptor.IsSuperStream,
                Reference = descriptor.Reference,
                SingleActiveConsumer = descriptor.SingleActiveConsumer,
                Offset = descriptor.Offset,
                OffsetStore = descriptor.OffsetStore,
                Retry = descriptor.Retry,
                ErrorStream = descriptor.ErrorStream,
                OnRetryExhausted = descriptor.OnRetryExhausted,
                ReadinessPolicy = descriptor.Readiness,
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
