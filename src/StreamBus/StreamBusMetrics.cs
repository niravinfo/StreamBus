using System.Collections.Concurrent;
using System.Diagnostics;
using System.Diagnostics.Metrics;

namespace StreamBus;

internal sealed class PartitionState
{
    public long Inflight;
    // -1 = nothing delivered/stored yet, so an untouched consumer observes lag 0
    // (Max(0, -1 - -1)), offset 0 delivered observes 1, offset 0 stored observes 0.
    public long LastDelivered = -1;
    public long LastStored = -1;
    public long RetryBlocked;
}

internal sealed class StreamBusMetrics
{
    public static readonly StreamBusMetrics Shared = new();

    private readonly Meter _meter = new("StreamBus");
    private readonly ConcurrentDictionary<(string Vhost, string Bus, string Stream, int Partition, string Reference), PartitionState> _partitions = new();
    private readonly ConcurrentDictionary<(string Vhost, string Bus), Func<BusConnectionState>> _connections = new();

    private StreamBusMetrics()
    {
        // Consumer-held unprocessed backlog (delivered but not yet stored), computed
        // live — not the broker-total backlog (no usable end-offset source on this broker).
        _ = _meter.CreateObservableUpDownCounter("streambus_partition_lag",
            () => ObservePartitions(s => Math.Max(0, s.LastDelivered - s.LastStored)),
            description: "Consumer-held unprocessed backlog max(0, delivered - stored), not broker-total backlog.");
        _ = _meter.CreateObservableUpDownCounter("streambus_partition_inflight",
            () => ObservePartitions(s => s.Inflight));
        _ = _meter.CreateObservableUpDownCounter("streambus_partition_retry_blocked",
            () => ObservePartitions(s => s.RetryBlocked),
            unit: null, description: null);
        _ = _meter.CreateObservableUpDownCounter<int>("streambus_connection_state",
            () => _connections.Select(kv => new Measurement<int>((int)kv.Value(),
                new TagList { { "vhost", kv.Key.Vhost }, { "bus", kv.Key.Bus } })));
        StoreRejected = _meter.CreateCounter<long>("streambus_offset_store_rejected_total");
        Regressed = _meter.CreateCounter<long>("streambus_offset_regressed_total");
        Fallback = _meter.CreateCounter<long>("streambus_offset_fallback_total");
        _pipelineDuration = _meter.CreateHistogram<double>("streambus_pipeline_duration", unit: "s",
            description: "End-to-end duration of a publish or consume filter pipeline, including the terminal handler.");
    }

    public Counter<long> StoreRejected { get; }
    public Counter<long> Regressed { get; }
    public Counter<long> Fallback { get; }

    private readonly Histogram<double> _pipelineDuration;

    public void RecordPipelineDuration(string kind, double seconds)
        => _pipelineDuration.Record(seconds, new KeyValuePair<string, object?>("kind", kind));

    public IDisposable TrackPartition(string vhost, string bus, string stream, int partition, string reference, PartitionState state)
    {
        var key = (vhost, bus, stream, partition, reference);
        _partitions[key] = state;
        return new Unregister(() => _partitions.TryRemove(key, out _));
    }

    public void TrackConnection(string vhost, string bus, Func<BusConnectionState> readState)
    {
        _connections[(vhost, bus)] = readState;
    }

    public void RecordStoreRejected(string vhost, string bus, string stream, int partition, string reference)
        => StoreRejected.Add(1, BuildTags(vhost, bus, stream, partition, reference));

    public void RecordRegressed(string vhost, string bus, string stream, int partition, string reference)
        => Regressed.Add(1, BuildTags(vhost, bus, stream, partition, reference));

    public void RecordFallback(string vhost, string bus, string stream, int partition, string reference, string branch)
        => Fallback.Add(1, BuildTags(vhost, bus, stream, partition, reference, ("branch", branch)));

    private IEnumerable<Measurement<long>> ObservePartitions(Func<PartitionState, long> select)
    {
        foreach (var kv in _partitions)
        {
            var tags = new TagList
            {
                { "vhost", kv.Key.Vhost },
                { "bus", kv.Key.Bus },
                { "stream", kv.Key.Stream },
                { "partition", kv.Key.Partition },
                { "reference", kv.Key.Reference },
            };
            yield return new Measurement<long>(select(kv.Value), tags);
        }
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
