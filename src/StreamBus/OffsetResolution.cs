using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using RabbitMQ.Stream.Client;

namespace StreamBus;

internal static class OffsetResolution
{
    public static IOffsetType ResolveOffset(
        ConsumeOffset mode,
        ulong? stored,
        ulong localWater,
        string reference,
        string partitionStream,
        ILogger logger,
        Action<string> onFallback)
    {
        ArgumentNullException.ThrowIfNull(logger);
        ArgumentNullException.ThrowIfNull(onFallback);
        if (mode is ConsumeOffset.AlwaysFirst) return new OffsetTypeFirst();
        if (mode is ConsumeOffset.AlwaysNext) return new OffsetTypeNext();
        if (stored is null)
        {
            var branch = mode switch
            {
                ConsumeOffset.ResumeOrFirst => "ResumeOrFirst",
                ConsumeOffset.ResumeOrNext => "ResumeOrNext",
                ConsumeOffset.ResumeOrFail => "ResumeOrFail",
                _ => throw new StreamBusConfigurationException($"Unknown consume offset mode {mode}."),
            };
            if (mode == ConsumeOffset.ResumeOrFail)
                throw new StreamBusConfigurationException(
                    $"No stored offset for reference '{reference}' on partition '{partitionStream}' (mode ResumeOrFail).");
            var resolved = mode == ConsumeOffset.ResumeOrFirst ? (IOffsetType)new OffsetTypeFirst() : new OffsetTypeNext();
            logger.LogWarning(
                "No stored offset for reference '{Reference}' on partition '{Partition}' (branch {Branch}); resolved {Resolved}.",
                reference, partitionStream, branch, resolved.GetType().Name);
            onFallback(branch);
            return resolved;
        }
        if (localWater > stored.Value)
            logger.LogWarning(
                "Local high-water {Local} exceeds broker-stored {Stored} for reference '{Reference}' on partition '{Partition}'; broker wins, replaying from {Resume}.",
                localWater, stored.Value, reference, partitionStream, stored.Value + 1);
        return new OffsetTypeOffset(stored.Value + 1);
    }

    public static async Task<IOffsetType> ResolveWithQueryAsync(
        ConsumeOffset mode,
        Func<Task<ulong?>> queryStored,
        ulong localWater,
        string reference,
        string partitionStream,
        ILogger logger,
        Action<string> onFallback)
    {
        ulong? stored;
        try
        {
            stored = await queryStored().ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            logger.LogWarning(ex,
                "Offset query failed for reference '{Reference}' on partition '{Partition}'; degrading to the no-stored-offset branch.",
                reference, partitionStream);
            stored = null;
        }
        return ResolveOffset(mode, stored, localWater, reference, partitionStream, logger, onFallback);
    }
}

internal sealed class MonotonicOffsetGuard
{
    private readonly Dictionary<string, ulong> _water = new();
    private readonly object _gate = new();

    public ulong Water(string key)
    {
        lock (_gate) return _water.TryGetValue(key, out var w) ? w : 0;
    }

    public bool TryAdmit(string key, ulong candidate)
    {
        lock (_gate)
        {
            if (!_water.TryGetValue(key, out var current))
            {
                // First contact admits anything: offset 0 IS progress from nothing.
                _water[key] = candidate;
                return true;
            }
            if (candidate <= current) return false;
            _water[key] = candidate;
            return true;
        }
    }

    public void Overwrite(string key, ulong value)
    {
        lock (_gate) _water[key] = value;
    }
}
