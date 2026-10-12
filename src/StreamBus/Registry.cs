using System.Collections.Concurrent;
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

    public StreamBusRegistry(
        Func<string, StreamBusOptions> resolveOptions,
        Func<StreamBusOptions, StreamBusConnectionSupervisor> createSupervisor,
        ILoggerFactory? loggerFactory = null)
    {
        _resolveOptions = resolveOptions;
        _createSupervisor = createSupervisor;
        _loggerFactory = loggerFactory ?? NullLoggerFactory.Instance;
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

    internal StreamProducerRegistry ProducersFor(string vhost)
        => _producers.GetOrAdd(vhost, _ => new StreamProducerRegistry(SupervisorForCore(vhost), vhost));

    internal StreamConsumerRegistry ConsumersFor(string vhost)
        => _consumers.GetOrAdd(vhost, _ => new StreamConsumerRegistry(SupervisorForCore(vhost), vhost, _loggerFactory));

    internal IReadOnlyCollection<IStreamConsumer> ActiveConsumers
        => _consumers.Values.SelectMany(r => r.ActiveConsumers).ToList();

    IReadOnlyCollection<IStreamConsumer> IStreamBusRegistry.ActiveConsumers => ActiveConsumers;

    StreamBusConnectionSupervisor IStreamBusRegistry.SupervisorFor(string vhost)
        => SupervisorForCore(vhost);

    internal StreamBusConnectionSupervisor SupervisorForCore(string vhost)
        => _supervisors.GetOrAdd(vhost, _ => _createSupervisor(_resolveOptions(vhost)));

    internal BusEntry GetOrCreateCore(Type busType, string vhost)
    {
        var key = $"{busType.FullName}@{vhost}";
        var entry = _entries.GetOrAdd(key, _ =>
        {
            var supervisor = SupervisorForCore(vhost);
            IStreamBus bus = busType == typeof(DefaultBus)
                ? new StreamBus(vhost, supervisor, ProducersFor(vhost), ConsumersFor(vhost))
                : (IStreamBus)Activator.CreateInstance(typeof(StreamBus<>).MakeGenericType(busType), vhost, supervisor, ProducersFor(vhost), ConsumersFor(vhost))!;
            return new BusEntry { Supervisor = supervisor, Bus = bus };
        });
        StreamBusMetrics.Shared.TrackConnection(vhost, busType.FullName ?? busType.Name, () => entry.Supervisor.State);
        return entry;
    }

    private static (string, string, BusConnectionState) ParseKey(string key, BusConnectionState state)
    {
        var at = key.LastIndexOf('@');
        return (key[(at + 1)..], key[..at], state);
    }
}
