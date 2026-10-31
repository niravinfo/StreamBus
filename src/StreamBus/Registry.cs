using System.Collections.Concurrent;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

namespace StreamBus;

public interface IStreamBusRegistry
{
    IStreamBus GetOrCreateBus(string vhost);
    IStreamBus<TBus> GetOrCreateBus<TBus>(string vhost);
    IEnumerable<(string Vhost, string BusType, BusConnectionState State)> DescribeStates();
    internal BusEntry GetOrCreateEntry(Type busType, string vhost);
    internal StreamBusConnectionSupervisor SupervisorFor(string vhost);
    internal void RemoveSupervisor(string vhost);
    internal StreamTopologyRegistry TopologyFor(string vhost);
    internal StreamBusHealthAggregator HealthFor(string vhost);
    internal ErrorQueueTransport ErrorQueuesFor(string vhost);
    internal IReadOnlyCollection<StreamBusHealthAggregator> HealthAggregators { get; }
    internal IReadOnlyCollection<IStreamConsumer> ActiveConsumers { get; }
}

internal sealed class BusEntry
{
    public required StreamBusConnectionSupervisor Supervisor { get; init; }
    public required IStreamBus Bus { get; init; }
    public required IReadOnlyDictionary<string, DeclaredStream> Declarations { get; init; }
}

internal sealed class StreamBusRegistry : IStreamBusRegistry
{
    private readonly ConcurrentDictionary<(Type BusType, string Vhost), BusEntry> _entries = new();
    private readonly Func<string, StreamBusOptions> _resolveOptions;
    private readonly Func<StreamBusOptions, StreamBusConnectionSupervisor> _createSupervisor;
    private readonly ILoggerFactory _loggerFactory;
    private readonly StreamPublishRouter _publishRouter;
    private readonly Func<Type, IStreamSerializer> _serializerResolver;
    private readonly IServiceScopeFactory? _scopes;
    private readonly Func<Type, IReadOnlyList<DeclaredStream>> _declarationResolver;
    private readonly StreamBusPublishStore? _publishStore;
    private readonly IServiceProvider? _rootProvider;
    private readonly StreamBusConsumeFilterStore? _consumeFilterStore;
    private readonly object _entryGate = new();

    public StreamBusRegistry(
        Func<string, StreamBusOptions> resolveOptions,
        Func<StreamBusOptions, StreamBusConnectionSupervisor> createSupervisor,
        ILoggerFactory? loggerFactory = null,
        StreamPublishRouter? publishRouter = null,
        Func<Type, IStreamSerializer>? serializerResolver = null,
        IServiceScopeFactory? scopeFactory = null,
        Func<Type, IReadOnlyList<DeclaredStream>>? declarationResolver = null,
        StreamBusPublishStore? publishStore = null,
        IServiceProvider? rootProvider = null,
        StreamBusConsumeFilterStore? consumeFilterStore = null)
    {
        _resolveOptions = resolveOptions;
        _createSupervisor = createSupervisor;
        _loggerFactory = loggerFactory ?? NullLoggerFactory.Instance;
        _publishRouter = publishRouter ?? new StreamPublishRouter(new StreamBusPublishStore());
        _serializerResolver = serializerResolver ?? (_ => SystemTextJsonStreamSerializer.Default);
        _scopes = scopeFactory;
        _declarationResolver = declarationResolver ?? (_ => Array.Empty<DeclaredStream>());
        _publishStore = publishStore;
        _rootProvider = rootProvider;
        _consumeFilterStore = consumeFilterStore;
    }

    public IStreamBus GetOrCreateBus(string vhost)
        => (IStreamBus)GetOrCreateCore(typeof(DefaultBus), vhost).Bus;

    public IStreamBus<TBus> GetOrCreateBus<TBus>(string vhost)
        => (IStreamBus<TBus>)GetOrCreateCore(typeof(TBus), vhost).Bus;

    public IEnumerable<(string Vhost, string BusType, BusConnectionState State)> DescribeStates()
        => _entries.Select(kv => (kv.Key.Vhost, kv.Key.BusType.FullName ?? kv.Key.BusType.Name, kv.Value.Bus.State));

    BusEntry IStreamBusRegistry.GetOrCreateEntry(Type busType, string vhost)
        => GetOrCreateCore(busType, vhost);

    private readonly ConcurrentDictionary<string, StreamBusConnectionSupervisor> _supervisors = new();
    private readonly ConcurrentDictionary<string, StreamProducerRegistry> _producers = new();
    private readonly ConcurrentDictionary<string, StreamConsumerRegistry> _consumers = new();
    private readonly ConcurrentDictionary<string, StreamTopologyRegistry> _topologies = new();
    private readonly ConcurrentDictionary<string, ErrorQueueTransport> _errorQueues = new();
    private readonly ConcurrentDictionary<string, StreamBusHealthAggregator> _health = new();
    private readonly ConcurrentDictionary<string, bool> _healthWired = new();

    internal ErrorQueueTransport ErrorQueuesFor(string vhost)
        => _errorQueues.GetOrAdd(vhost, _ => new ErrorQueueTransport(
            _resolveOptions(vhost),
            null,
            _loggerFactory.CreateLogger<ErrorQueueTransport>()));

    ErrorQueueTransport IStreamBusRegistry.ErrorQueuesFor(string vhost) => ErrorQueuesFor(vhost);

    internal StreamProducerRegistry ProducersFor(string vhost)
        => _producers.GetOrAdd(vhost, _ => new StreamProducerRegistry(SupervisorForCore(vhost), vhost));

    private readonly ConcurrentDictionary<string, ResultProducerRegistry> _results = new();

    internal ResultProducerRegistry ResultProducersFor(string vhost)
        => _results.GetOrAdd(vhost, _ => new ResultProducerRegistry(SupervisorForCore(vhost), vhost));

    internal StreamConsumerRegistry ConsumersFor(string vhost)
        => _consumers.GetOrAdd(vhost, _ => new StreamConsumerRegistry(SupervisorForCore(vhost), vhost, ProducersFor(vhost), _loggerFactory, ResultProducersFor(vhost), _scopes, ErrorQueuesFor(vhost), HealthFor(vhost), consumeFilters: _consumeFilterStore, rootProvider: _rootProvider));

    internal StreamBusHealthAggregator HealthFor(string vhost)
        => _health.GetOrAdd(vhost, static _ => new StreamBusHealthAggregator());

    internal IReadOnlyCollection<StreamBusHealthAggregator> HealthAggregators => _health.Values.ToList();

    StreamBusHealthAggregator IStreamBusRegistry.HealthFor(string vhost) => HealthFor(vhost);

    IReadOnlyCollection<StreamBusHealthAggregator> IStreamBusRegistry.HealthAggregators => HealthAggregators;

    internal IReadOnlyCollection<IStreamConsumer> ActiveConsumers
        => _consumers.Values.SelectMany(r => r.ActiveConsumers).ToList();

    IReadOnlyCollection<IStreamConsumer> IStreamBusRegistry.ActiveConsumers => ActiveConsumers;

    StreamBusConnectionSupervisor IStreamBusRegistry.SupervisorFor(string vhost)
        => SupervisorForCore(vhost);

    internal void RemoveSupervisor(string vhost)
    {
        _supervisors.TryRemove(vhost, out _);
        // A replacement supervisor must re-subscribe its connection state, or
        // health would keep reporting the disposed supervisor's last state.
        _healthWired.TryRemove(vhost, out _);
    }

    void IStreamBusRegistry.RemoveSupervisor(string vhost) => RemoveSupervisor(vhost);

    internal StreamTopologyRegistry TopologyFor(string vhost)
        => _topologies.GetOrAdd(vhost, _ => new StreamTopologyRegistry(SupervisorForCore(vhost), vhost, _loggerFactory.CreateLogger<StreamTopologyRegistry>()));

    StreamTopologyRegistry IStreamBusRegistry.TopologyFor(string vhost) => TopologyFor(vhost);

    internal StreamBusConnectionSupervisor SupervisorForCore(string vhost)
    {
        var supervisor = _supervisors.GetOrAdd(vhost, _ => _createSupervisor(_resolveOptions(vhost)));
        if (_healthWired.TryAdd(vhost, true))
        {
            var health = HealthFor(vhost);
            health.SetConnection(supervisor.State);
            supervisor.StateChanged += health.SetConnection;
        }
        return supervisor;
    }

    internal BusEntry GetOrCreateCore(Type busType, string vhost)
    {
        var key = (busType, vhost);
        if (_entries.TryGetValue(key, out var existing))
            return existing;
        lock (_entryGate)
        {
            if (_entries.TryGetValue(key, out existing))
                return existing;
            var created = CreateEntry(busType, vhost);
            _entries[key] = created;
            return created;
        }
    }

    private BusEntry CreateEntry(Type busType, string vhost)
    {
        var supervisor = SupervisorForCore(vhost);
        var serializer = _serializerResolver(busType) ?? SystemTextJsonStreamSerializer.Default;
        // Configuration is immutable after AddStreamBus, so build the
        // declaration map once instead of rebuilding the merge set on every
        // explicit-stream publish. First declaration wins, matching the old
        // FirstOrDefault lookup for duplicate names.
        var declarations = new Dictionary<string, DeclaredStream>(StringComparer.Ordinal);
        foreach (var declaration in _declarationResolver(busType))
            declarations.TryAdd(declaration.Name, declaration);
        Func<string, DeclaredStream?> findDeclaration = stream
            => declarations.TryGetValue(stream, out var declaration) ? declaration : null;

        var registrations = _publishStore?.FiltersFor(busType) ?? Array.Empty<PublishFilterRegistration>();
        var pipelines = new PublishPipelineCache(busType, registrations, _rootProvider);
        if (_publishStore is not null)
        {
            foreach (var registration in _publishStore.Registrations)
            {
                if (registration.BusMarker == busType)
                    pipelines.GetOrCreate(registration.MessageType);
            }
        }
        IStreamBus bus = busType == typeof(DefaultBus)
            ? new StreamBus(vhost, supervisor, ProducersFor(vhost), ConsumersFor(vhost), TopologyFor(vhost), serializer, _publishRouter, ErrorQueuesFor(vhost), findDeclaration, pipelines, _scopes)
            : (IStreamBus)Activator.CreateInstance(
                typeof(StreamBus<>).MakeGenericType(busType),
                vhost, supervisor, ProducersFor(vhost), ConsumersFor(vhost), TopologyFor(vhost), serializer,
                _publishRouter, ErrorQueuesFor(vhost), findDeclaration, pipelines, _scopes)!;
        var created = new BusEntry { Supervisor = supervisor, Bus = bus, Declarations = declarations };
        StreamBusMetrics.Shared.TrackConnection(vhost, busType.FullName ?? busType.Name, () => created.Supervisor.State);
        return created;
    }
}
