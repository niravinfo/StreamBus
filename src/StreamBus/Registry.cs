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
    internal StreamTopologyRegistry TopologyFor(string vhost);
    internal IReadOnlyCollection<IStreamConsumer> ActiveConsumers { get; }
}

internal sealed class BusEntry
{
    public required StreamBusConnectionSupervisor Supervisor { get; init; }
    public required IStreamBus Bus { get; init; }
}

public sealed class StreamBusRegistry : IStreamBusRegistry
{
    private readonly ConcurrentDictionary<string, BusEntry> _entries = new();
    private readonly Func<string, StreamBusOptions> _resolveOptions;
    private readonly Func<StreamBusOptions, StreamBusConnectionSupervisor> _createSupervisor;
    private readonly ILoggerFactory _loggerFactory;
    private readonly StreamPublishRouter _publishRouter;
    private readonly Func<Type, IStreamSerializer> _serializerResolver;
    private readonly IServiceScopeFactory? _scopes;

    public StreamBusRegistry(
        Func<string, StreamBusOptions> resolveOptions,
        Func<StreamBusOptions, StreamBusConnectionSupervisor> createSupervisor,
        ILoggerFactory? loggerFactory = null,
        StreamPublishRouter? publishRouter = null,
        Func<Type, IStreamSerializer>? serializerResolver = null,
        IServiceScopeFactory? scopeFactory = null)
    {
        _resolveOptions = resolveOptions;
        _createSupervisor = createSupervisor;
        _loggerFactory = loggerFactory ?? NullLoggerFactory.Instance;
        _publishRouter = publishRouter ?? new StreamPublishRouter(new StreamBusPublishStore());
        _serializerResolver = serializerResolver ?? (_ => SystemTextJsonStreamSerializer.Default);
        _scopes = scopeFactory;
    }

    public IStreamBus GetOrCreateBus(string vhost)
        => (IStreamBus)GetOrCreateCore(typeof(DefaultBus), vhost).Bus;

    public IStreamBus<TBus> GetOrCreateBus<TBus>(string vhost)
        => (IStreamBus<TBus>)GetOrCreateCore(typeof(TBus), vhost).Bus;

    public IEnumerable<(string Vhost, string BusType, BusConnectionState State)> DescribeStates()
        => _entries.Select(kv => ParseKey(kv.Key, kv.Value.Bus.State));

    BusEntry IStreamBusRegistry.GetOrCreateEntry(Type busType, string vhost)
        => GetOrCreateCore(busType, vhost);

    private readonly ConcurrentDictionary<string, StreamBusConnectionSupervisor> _supervisors = new();
    private readonly ConcurrentDictionary<string, StreamProducerRegistry> _producers = new();
    private readonly ConcurrentDictionary<string, StreamConsumerRegistry> _consumers = new();
    private readonly ConcurrentDictionary<string, StreamTopologyRegistry> _topologies = new();
    private readonly ConcurrentDictionary<string, ErrorQueueTransport> _errorQueues = new();

    internal ErrorQueueTransport ErrorQueuesFor(string vhost)
        => _errorQueues.GetOrAdd(vhost, _ => new ErrorQueueTransport(
            _resolveOptions(vhost),
            null,
            _loggerFactory.CreateLogger<ErrorQueueTransport>()));

    internal StreamProducerRegistry ProducersFor(string vhost)
        => _producers.GetOrAdd(vhost, _ => new StreamProducerRegistry(SupervisorForCore(vhost), vhost));

    private readonly ConcurrentDictionary<string, ResultProducerRegistry> _results = new();

    internal ResultProducerRegistry ResultProducersFor(string vhost)
        => _results.GetOrAdd(vhost, _ => new ResultProducerRegistry(SupervisorForCore(vhost), vhost));

    internal StreamConsumerRegistry ConsumersFor(string vhost)
        => _consumers.GetOrAdd(vhost, _ => new StreamConsumerRegistry(SupervisorForCore(vhost), vhost, ProducersFor(vhost), _loggerFactory, ResultProducersFor(vhost), _scopes, ErrorQueuesFor(vhost)));

    internal IReadOnlyCollection<IStreamConsumer> ActiveConsumers
        => _consumers.Values.SelectMany(r => r.ActiveConsumers).ToList();

    IReadOnlyCollection<IStreamConsumer> IStreamBusRegistry.ActiveConsumers => ActiveConsumers;

    StreamBusConnectionSupervisor IStreamBusRegistry.SupervisorFor(string vhost)
        => SupervisorForCore(vhost);

    internal StreamTopologyRegistry TopologyFor(string vhost)
        => _topologies.GetOrAdd(vhost, _ => new StreamTopologyRegistry(SupervisorForCore(vhost), vhost, _loggerFactory.CreateLogger<StreamTopologyRegistry>()));

    StreamTopologyRegistry IStreamBusRegistry.TopologyFor(string vhost) => TopologyFor(vhost);

    internal StreamBusConnectionSupervisor SupervisorForCore(string vhost)
        => _supervisors.GetOrAdd(vhost, _ => _createSupervisor(_resolveOptions(vhost)));

    internal BusEntry GetOrCreateCore(Type busType, string vhost)
    {
        var key = $"{busType.FullName}@{vhost}";
        var entry = _entries.GetOrAdd(key, _ =>
        {
            var supervisor = SupervisorForCore(vhost);
            var serializer = _serializerResolver(busType) ?? SystemTextJsonStreamSerializer.Default;
            IStreamBus bus = busType == typeof(DefaultBus)
                ? new StreamBus(vhost, supervisor, ProducersFor(vhost), ConsumersFor(vhost), TopologyFor(vhost), serializer, _publishRouter, ErrorQueuesFor(vhost))
                : (IStreamBus)Activator.CreateInstance(typeof(StreamBus<>).MakeGenericType(busType), vhost, supervisor, ProducersFor(vhost), ConsumersFor(vhost), TopologyFor(vhost), serializer, _publishRouter, ErrorQueuesFor(vhost))!;
            var created = new BusEntry { Supervisor = supervisor, Bus = bus };
            StreamBusMetrics.Shared.TrackConnection(vhost, busType.FullName ?? busType.Name, () => created.Supervisor.State);
            return created;
        });
        return entry;
    }

    private static (string, string, BusConnectionState) ParseKey(string key, BusConnectionState state)
    {
        var at = key.LastIndexOf('@');
        return (key[(at + 1)..], key[..at], state);
    }
}
