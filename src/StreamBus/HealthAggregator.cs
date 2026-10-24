using System.Collections.Concurrent;

namespace StreamBus;

internal enum BusRegistrationState { Attaching, Attached, FailedRetrying, Stopped }

internal sealed record BusRegistrationHealth(
    string Vhost,
    string Registration,
    string Kind,
    BusRegistrationState State,
    string? Reason,
    DateTimeOffset Since);

internal sealed class StreamBusHealthAggregator
{
    internal enum HealthStatus { Healthy, Degraded, Unhealthy }

    private readonly ConcurrentDictionary<string, BusRegistrationHealth> _rows = new();
    private readonly object _lock = new();
    private BusConnectionState _connection = BusConnectionState.Starting;

    public void SetConnection(BusConnectionState state)
    {
        lock (_lock) _connection = state;
    }

    public void UpsertRegistration(string vhost, string registration, string kind,
        BusRegistrationState state, string? reason = null)
    {
        var key = $"{vhost}|{registration}";
        _rows.AddOrUpdate(key,
            _ => new BusRegistrationHealth(vhost, registration, kind, state, reason, DateTimeOffset.UtcNow),
            (_, existing) =>
                existing.State == state && existing.Reason == reason
                    ? existing // no churn: keep the original Since
                    : existing with { State = state, Reason = reason, Since = DateTimeOffset.UtcNow });
    }

    public void RemoveRegistration(string vhost, string registration)
        => _rows.TryRemove($"{vhost}|{registration}", out _);

    public (BusConnectionState Connection, IReadOnlyList<BusRegistrationHealth> Registrations) Snapshot()
    {
        lock (_lock) return (_connection, _rows.Values.OrderBy(r => r.Vhost, StringComparer.Ordinal)
            .ThenBy(r => r.Registration, StringComparer.Ordinal).ToArray());
    }

    public HealthStatus Compute()
    {
        var (connection, rows) = Snapshot();
        var consumers = rows.Where(r => r.Kind == "consumer").ToList();
        var declares = rows.Where(r => r.Kind == "declare").ToList();

        bool declareExhausted = declares.Any(d => d.State == BusRegistrationState.FailedRetrying);
        bool allConsumersFailed = consumers.Count > 0
            && consumers.All(c => c.State == BusRegistrationState.FailedRetrying);
        bool anyConsumerRetrying = consumers.Any(c => c.State == BusRegistrationState.FailedRetrying);

        if (connection == BusConnectionState.Fatal || declareExhausted || allConsumersFailed)
            return HealthStatus.Unhealthy;
        if (anyConsumerRetrying || connection != BusConnectionState.Ready)
            return HealthStatus.Degraded;
        return HealthStatus.Healthy;
    }
}
