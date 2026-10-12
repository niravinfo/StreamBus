using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

namespace StreamBus;

public interface IStreamConsumer : IAsyncDisposable
{
    string Stream { get; }
    string Reference { get; }
}

internal sealed class StreamConsumerRegistration : IStreamConsumer
{
    private readonly List<(StreamPartitionConsumer Loop, Task RunTask)> _loops;
    private readonly List<(PartitionLagPoller Poller, Task PollTask)> _pollers;
    private readonly List<IDisposable> _scopes;
    private readonly CancellationTokenSource _family;
    private readonly Action<StreamConsumerRegistration> _deregister;
    private int _disposed;

    public StreamConsumerRegistration(
        string stream,
        string reference,
        List<(StreamPartitionConsumer Loop, Task RunTask)> loops,
        List<(PartitionLagPoller Poller, Task PollTask)> pollers,
        List<IDisposable> scopes,
        CancellationTokenSource family,
        Action<StreamConsumerRegistration> deregister)
    {
        Stream = stream;
        Reference = reference;
        _loops = loops;
        _pollers = pollers;
        _scopes = scopes;
        _family = family;
        _deregister = deregister;
    }

    public string Stream { get; }
    public string Reference { get; }

    public async ValueTask DisposeAsync()
    {
        if (Interlocked.Exchange(ref _disposed, 1) == 1)
        {
            return;
        }
        _family.Cancel();
        try
        {
            foreach (var (_, run) in _loops)
            {
                await run.ConfigureAwait(false);
            }
            foreach (var (_, poll) in _pollers)
            {
                await poll.ConfigureAwait(false);
            }
        }
        finally
        {
            foreach (var (loop, _) in _loops)
            {
                await loop.DisposeAsync().ConfigureAwait(false);
            }
            foreach (var (poller, _) in _pollers)
            {
                await poller.DisposeAsync().ConfigureAwait(false);
            }
            foreach (var scope in _scopes)
            {
                scope.Dispose();
            }
            _family.Dispose();
            _deregister(this);
        }
    }
}

internal sealed class StreamConsumerRegistry
{
    private static readonly TimeSpan LagPollInterval = TimeSpan.FromSeconds(15);

    private readonly StreamBusConnectionSupervisor _supervisor;
    private readonly string _vhost;
    private readonly ILoggerFactory _loggerFactory;
    private readonly ILogger<StreamConsumerRegistry> _logger;
    private readonly MonotonicOffsetGuard _guard = new();
    private readonly HashSet<StreamConsumerRegistration> _live = new();
    private readonly object _gate = new();

    public StreamConsumerRegistry(StreamBusConnectionSupervisor supervisor, string vhost, ILoggerFactory? loggerFactory = null)
    {
        _supervisor = supervisor;
        _vhost = vhost;
        _loggerFactory = loggerFactory ?? NullLoggerFactory.Instance;
        _logger = _loggerFactory.CreateLogger<StreamConsumerRegistry>();
    }

    internal IReadOnlyCollection<IStreamConsumer> ActiveConsumers
    {
        get
        {
            lock (_gate)
            {
                return _live.ToList();
            }
        }
    }

    public async Task<IStreamConsumer> ConsumeAsync(
        Type busType, string stream, ConsumeOptions options,
        Func<ConsumeContext, Task> handler, CancellationToken ct = default)
    {
        ArgumentNullException.ThrowIfNull(busType);
        ArgumentException.ThrowIfNullOrWhiteSpace(stream);
        ArgumentNullException.ThrowIfNull(options);
        ArgumentNullException.ThrowIfNull(handler);
        ArgumentNullException.ThrowIfNull(options.OffsetStore);
        ArgumentNullException.ThrowIfNull(options.ReadinessPolicy);
        if (options.OffsetStore.EveryMessages < 1)
        {
            throw new StreamBusConfigurationException("ConsumeOptions.OffsetStore.EveryMessages must be >= 1.");
        }
        if (options.OffsetStore.MaxInterval is { } maxInterval && maxInterval <= TimeSpan.Zero)
        {
            throw new StreamBusConfigurationException("ConsumeOptions.OffsetStore.MaxInterval must be positive when set.");
        }
        var reference = options.Reference ?? ReferenceDerivation.Derive(busType, stream);
        ReferenceDerivation.Validate(reference);

        var readiness = await _supervisor.WaitUntilReadyAsync(options.ReadinessPolicy, ct).ConfigureAwait(false);
        if (readiness == BusReadiness.Fatal)
        {
            throw new StreamBusFatalException($"StreamBus for vhost '{_vhost}' is in Fatal state; consume of '{stream}' refused.");
        }
        if (readiness != BusReadiness.Ready)
        {
            throw new StreamBusNotReadyException($"StreamBus for vhost '{_vhost}' is not ready ({readiness}); consume of '{stream}' refused.");
        }
        var handle = _supervisor.CurrentHandle
            ?? throw new StreamBusNotReadyException($"StreamBus for vhost '{_vhost}' is not ready; consume of '{stream}' refused.");

        var exists = options.IsSuperStream
            ? await handle.SuperStreamExistsAsync(stream, ct).ConfigureAwait(false)
            : await handle.StreamExistsAsync(stream, ct).ConfigureAwait(false);
        if (!exists)
        {
            throw new StreamBusStreamNotFoundException(stream);
        }
        var partitionStreams = options.IsSuperStream
            ? await handle.QueryPartitionAsync(stream, ct).ConfigureAwait(false)
            : new[] { stream };

        if (options.Offset == ConsumeOffset.ResumeOrFail)
        {
            foreach (var partitionStream in partitionStreams)
            {
                ulong? stored;
                try
                {
                    stored = await handle.TryQueryOffsetAsync(reference, partitionStream, ct).ConfigureAwait(false);
                }
                catch (OperationCanceledException)
                {
                    throw;
                }
                catch (Exception)
                {
                    stored = null;
                }
                if (stored is null)
                {
                    throw new StreamBusConfigurationException(
                        $"No stored offset for reference '{reference}' on partition '{partitionStream}' (mode ResumeOrFail).");
                }
            }
        }

        var busName = busType.FullName ?? busType.Name;
        var family = new CancellationTokenSource();
        var loops = new List<(StreamPartitionConsumer Loop, Task RunTask)>(partitionStreams.Length);
        var pollers = new List<(PartitionLagPoller Poller, Task PollTask)>(partitionStreams.Length);
        var scopes = new List<IDisposable>(partitionStreams.Length);
        try
        {
            for (var index = 0; index < partitionStreams.Length; index++)
            {
                var partitionStream = partitionStreams[index];
                var state = new PartitionState();
                scopes.Add(StreamBusMetrics.Shared.TrackPartition(_vhost, busName, stream, index, reference, state));
                var loop = new StreamPartitionConsumer(
                    handle, stream, partitionStream, index, reference, options, handler,
                    _guard, state, StreamBusMetrics.Shared, _vhost, busName,
                    _loggerFactory.CreateLogger<StreamPartitionConsumer>());
                loops.Add((loop, loop.RunAsync(family.Token)));
                var endStream = partitionStream;
                var poller = new PartitionLagPoller(
                    state,
                    () => handle.GetEndOffsetAsync(endStream, CancellationToken.None),
                    () => loop.LastSuccess,
                    LagPollInterval);
                pollers.Add((poller, poller.RunAsync(family.Token)));
            }
        }
        catch
        {
            family.Cancel();
            foreach (var (loop, _) in loops)
            {
                await loop.DisposeAsync().ConfigureAwait(false);
            }
            foreach (var (poller, _) in pollers)
            {
                await poller.DisposeAsync().ConfigureAwait(false);
            }
            foreach (var scope in scopes)
            {
                scope.Dispose();
            }
            family.Dispose();
            throw;
        }

        var registration = new StreamConsumerRegistration(stream, reference, loops, pollers, scopes, family, Remove);
        lock (_gate)
        {
            _live.Add(registration);
        }
        return registration;
    }

    public async Task<ulong?> GetOffsetAsync(string stream, string reference, int partition, CancellationToken ct = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(stream);
        ReferenceDerivation.Validate(reference);
        await EnsureReadyAsync(stream, ct).ConfigureAwait(false);
        var handle = _supervisor.CurrentHandle
            ?? throw new StreamBusNotReadyException($"StreamBus for vhost '{_vhost}' is not ready; offset query for '{stream}' refused.");
        var partitionStream = await ResolvePartitionStreamAsync(handle, stream, partition, ct).ConfigureAwait(false);
        return await handle.TryQueryOffsetAsync(reference, partitionStream, ct).ConfigureAwait(false);
    }

    public async Task SetOffsetAsync(string stream, string reference, int partition, ulong offset, CancellationToken ct = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(stream);
        ReferenceDerivation.Validate(reference);
        await EnsureReadyAsync(stream, ct).ConfigureAwait(false);
        var handle = _supervisor.CurrentHandle
            ?? throw new StreamBusNotReadyException($"StreamBus for vhost '{_vhost}' is not ready; offset store for '{stream}' refused.");
        var partitionStream = await ResolvePartitionStreamAsync(handle, stream, partition, ct).ConfigureAwait(false);
        await handle.StoreSystemOffsetAsync(reference, partitionStream, offset, ct).ConfigureAwait(false);
        _guard.Overwrite($"{reference}@{partitionStream}", offset);
        _logger.LogInformation(
            "Consumer offset for reference '{Reference}' on stream '{Stream}' partition {Partition} set to offset {Offset}.",
            reference, stream, partition, offset);
    }

    private async Task EnsureReadyAsync(string stream, CancellationToken ct)
    {
        var readiness = await _supervisor.WaitUntilReadyAsync(ReadinessPolicy.FailFastPolicy, ct).ConfigureAwait(false);
        if (readiness == BusReadiness.Fatal)
        {
            throw new StreamBusFatalException($"StreamBus for vhost '{_vhost}' is in Fatal state; offset operation for '{stream}' refused.");
        }
        if (readiness != BusReadiness.Ready)
        {
            throw new StreamBusNotReadyException($"StreamBus for vhost '{_vhost}' is not ready ({readiness}); offset operation for '{stream}' refused.");
        }
    }

    private static async Task<string> ResolvePartitionStreamAsync(IStreamSystemHandle handle, string stream, int partition, CancellationToken ct)
    {
        if (await handle.StreamExistsAsync(stream, ct).ConfigureAwait(false))
        {
            if (partition != 0)
            {
                throw new StreamBusConfigurationException(
                    $"Partition {partition} is out of range for plain stream '{stream}' (only partition 0 exists).");
            }
            return stream;
        }
        if (await handle.SuperStreamExistsAsync(stream, ct).ConfigureAwait(false))
        {
            var partitions = await handle.QueryPartitionAsync(stream, ct).ConfigureAwait(false);
            if (partition < 0 || partition >= partitions.Length)
            {
                throw new StreamBusConfigurationException(
                    $"Partition {partition} is out of range for super stream '{stream}' ({partitions.Length} partitions).");
            }
            return partitions[partition];
        }
        throw new StreamBusStreamNotFoundException(stream);
    }

    private void Remove(StreamConsumerRegistration registration)
    {
        lock (_gate)
        {
            _live.Remove(registration);
        }
    }
}
