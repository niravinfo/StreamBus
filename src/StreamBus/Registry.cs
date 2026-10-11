using System.Collections.Concurrent;

namespace StreamBus;

public interface IStreamBusRegistry
{
    IStreamBus GetOrCreateBus(string vhost);
    IStreamBus<TBus> GetOrCreateBus<TBus>(string vhost);
    IEnumerable<(string Vhost, string BusType, BusConnectionState State)> DescribeStates();
    internal BusEntry GetOrCreateEntry(Type busType, string vhost);
    internal StreamBusConnectionSupervisor SupervisorFor(string vhost);
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

    public StreamBusRegistry(
        Func<string, StreamBusOptions> resolveOptions,
        Func<StreamBusOptions, StreamBusConnectionSupervisor> createSupervisor)
    {
        _resolveOptions = resolveOptions;
        _createSupervisor = createSupervisor;
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

    StreamBusConnectionSupervisor IStreamBusRegistry.SupervisorFor(string vhost)
        => SupervisorForCore(vhost);

    internal StreamBusConnectionSupervisor SupervisorForCore(string vhost)
        => _supervisors.GetOrAdd(vhost, _ => _createSupervisor(_resolveOptions(vhost)));

    internal BusEntry GetOrCreateCore(Type busType, string vhost)
    {
        var key = $"{busType.FullName}@{vhost}";
        return _entries.GetOrAdd(key, _ =>
        {
            var supervisor = SupervisorForCore(vhost);
            IStreamBus bus = busType == typeof(DefaultBus)
                ? new StreamBus(vhost, supervisor)
                : (IStreamBus)Activator.CreateInstance(typeof(StreamBus<>).MakeGenericType(busType), vhost, supervisor)!;
            return new BusEntry { Supervisor = supervisor, Bus = bus };
        });
    }

    private static (string, string, BusConnectionState) ParseKey(string key, BusConnectionState state)
    {
        var at = key.LastIndexOf('@');
        return (key[(at + 1)..], key[..at], state);
    }
}
