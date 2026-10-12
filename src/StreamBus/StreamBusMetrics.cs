using System.Collections.Concurrent;
using System.Diagnostics;
using System.Diagnostics.Metrics;

namespace StreamBus;

internal sealed class PartitionState
{
    public long Inflight;
    public long LastDelivered;
    public long LastStored;
    public long RetryBlocked;
    public long Lag;
}

internal sealed class StreamBusMetrics
{
    public static readonly StreamBusMetrics Shared = new();

    private readonly Meter _meter = new("StreamBus");
    private readonly ConcurrentDictionary<string, PartitionState> _partitions = new();
    private readonly ConcurrentDictionary<string, Func<BusConnectionState>> _connections = new();

    private StreamBusMetrics()
    {
        _ = _meter.CreateObservableUpDownCounter("streambus_partition_lag",
            () => ObservePartitions(s => s.Lag));
        _ = _meter.CreateObservableUpDownCounter("streambus_partition_inflight",
            () => ObservePartitions(s => s.Inflight));
        _ = _meter.CreateObservableUpDownCounter("streambus_partition_retry_blocked",
            () => ObservePartitions(s => s.RetryBlocked, includeFailureMode: true),
            unit: null, description: null);
        _ = _meter.CreateObservableUpDownCounter<int>("streambus_connection_state",
            () => _connections.Select(kv => new Measurement<int>((int)kv.Value(),
                new TagList { { "vhost", VhostOf(kv.Key) }, { "bus", BusOf(kv.Key) } })));
        StoreRejected = _meter.CreateCounter<long>("streambus_offset_store_rejected_total");
        Regressed = _meter.CreateCounter<long>("streambus_offset_regressed_total");
        Fallback = _meter.CreateCounter<long>("streambus_offset_fallback_total");
    }

    public Counter<long> StoreRejected { get; }
    public Counter<long> Regressed { get; }
    public Counter<long> Fallback { get; }

    public IDisposable TrackPartition(string vhost, string bus, string stream, int partition, string reference, PartitionState state)
    {
        var key = $"{vhost}|{bus}|{stream}|{partition}|{reference}";
        _partitions[key] = state;
        return new Unregister(() => _partitions.TryRemove(key, out _));
    }

    public void TrackConnection(string vhost, string bus, Func<BusConnectionState> readState)
    {
        _connections[$"{vhost}|{bus}"] = readState;
    }

    public void RecordStoreRejected(string vhost, string bus, string stream, int partition, string reference)
        => StoreRejected.Add(1, BuildTags(vhost, bus, stream, partition, reference));

    public void RecordRegressed(string vhost, string bus, string stream, int partition, string reference)
        => Regressed.Add(1, BuildTags(vhost, bus, stream, partition, reference));

    public void RecordFallback(string vhost, string bus, string stream, int partition, string reference, string branch)
        => Fallback.Add(1, BuildTags(vhost, bus, stream, partition, reference, ("branch", branch)));

    private IEnumerable<Measurement<long>> ObservePartitions(
        Func<PartitionState, long> select, bool includeFailureMode = false)
    {
        foreach (var kv in _partitions)
        {
            var parts = kv.Key.Split('|');
            if (parts.Length != 5 || !int.TryParse(parts[3], out var partition))
            {
                continue;
            }
            var tags = new TagList
            {
                { "vhost", parts[0] },
                { "bus", parts[1] },
                { "stream", parts[2] },
                { "partition", partition },
                { "reference", parts[4] },
            };
            if (includeFailureMode)
            {
                tags.Add("failure_mode", "strict");
            }
            yield return new Measurement<long>(select(kv.Value), tags);
        }
    }

    private static string VhostOf(string key)
    {
        var parts = key.Split('|');
        return parts.Length > 0 ? parts[0] : string.Empty;
    }

    private static string BusOf(string key)
    {
        var parts = key.Split('|');
        return parts.Length > 1 ? parts[1] : string.Empty;
    }

    private static TagList BuildTags(
        string vhost, string bus, string stream, int partition, string reference,
        (string Name, string Value)? extra = null)
    {
        var tags = new TagList
        {
            { "vhost", vhost },
            { "bus", bus },
            { "stream", stream },
            { "partition", partition },
            { "reference", reference },
        };
        if (extra is { } e)
        {
            tags.Add(e.Name, e.Value);
        }
        return tags;
    }

    private sealed class Unregister : IDisposable
    {
        private Action? _remove;

        public Unregister(Action remove) => _remove = remove;

        public void Dispose() => Interlocked.Exchange(ref _remove, null)?.Invoke();
    }
}
