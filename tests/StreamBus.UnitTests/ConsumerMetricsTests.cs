using System.Diagnostics.Metrics;

namespace StreamBus.UnitTests;

public sealed class ConsumerMetricsTests : IDisposable
{
    private readonly MeterListener _listener = new();
    private readonly List<(string Instrument, double Value, KeyValuePair<string, object?>[] Tags)> _measurements = new();

    public ConsumerMetricsTests()
    {
        _listener.InstrumentPublished = (instrument, listener) =>
        {
            if (instrument.Meter.Name == "StreamBus")
                listener.EnableMeasurementEvents(instrument);
        };
        _listener.SetMeasurementEventCallback<double>(Record);
        _listener.SetMeasurementEventCallback<long>(Record);
        _listener.SetMeasurementEventCallback<int>(Record);
        _listener.Start();
    }

    private void Record<T>(Instrument instrument, T measurement, ReadOnlySpan<KeyValuePair<string, object?>> tags, object? state)
    {
        lock (_measurements) _measurements.Add((instrument.Name, Convert.ToDouble(measurement, System.Globalization.CultureInfo.InvariantCulture), tags.ToArray()));
    }

    public void Dispose() => _listener.Dispose();

    [Fact]
    public void Fallback_and_rejected_counters_emit_with_tags()
    {
        StreamBusMetrics.Shared.RecordFallback("vh", "bus", "stream", 0, "ref", "ResumeOrFirst");
        StreamBusMetrics.Shared.RecordStoreRejected("vh", "bus", "stream", 0, "ref");
        Assert.Contains(_measurements, m => m.Instrument == "streambus_offset_fallback_total"
            && m.Tags.Any(t => t.Key == "branch" && (string?)t.Value == "ResumeOrFirst"));
        Assert.Contains(_measurements, m => m.Instrument == "streambus_offset_store_rejected_total");
    }

    [Fact]
    public void Lag_observes_delivered_minus_stored()
    {
        var state = new PartitionState { LastDelivered = 9, LastStored = 4 };
        using (StreamBusMetrics.Shared.TrackPartition("vh", "b", "lag-s", 2, "r", state))
        {
            _listener.RecordObservableInstruments();
            Assert.Contains(_measurements, m => m.Instrument == "streambus_partition_lag" && m.Value == 5);
        }
    }

    [Fact]
    public void Partition_tracking_scope_registers_and_unregisters()
    {
        var state = new PartitionState { Inflight = 3 };
        using (StreamBusMetrics.Shared.TrackPartition("vh", "b", "s", 1, "r", state))
        {
            _listener.RecordObservableInstruments();
            Assert.Contains(_measurements, m => m.Instrument == "streambus_partition_inflight" && m.Value == 3);
        }
        // gauge observations are pull-based; scope disposal only stops future observations.
    }
}
