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
    private readonly List<IDisposable> _scopes;
    private readonly CancellationTokenSource _family;
    private readonly Action<StreamConsumerRegistration> _deregister;
    private int _disposed;

    public StreamConsumerRegistration(
        string stream,
        string reference,
        List<(StreamPartitionConsumer Loop, Task RunTask)> loops,
        List<IDisposable> scopes,
        CancellationTokenSource family,
        Action<StreamConsumerRegistration> deregister)
    {
        Stream = stream;
        Reference = reference;
        _loops = loops;
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
            await Task.WhenAll(_loops.Select(l => l.RunTask)).ConfigureAwait(false);
        }
        finally
        {
            foreach (var (loop, _) in _loops)
            {
                await loop.DisposeAsync().ConfigureAwait(false);
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
    private readonly StreamBusConnectionSupervisor _supervisor;
    private readonly string _vhost;
    private readonly ILoggerFactory _loggerFactory;
    private readonly ILogger<StreamConsumerRegistry> _logger;
    private readonly StreamProducerRegistry _producers;
    private readonly MonotonicOffsetGuard _guard = new();
    private readonly HashSet<StreamConsumerRegistration> _live = new();
    private readonly object _gate = new();

    public StreamConsumerRegistry(StreamBusConnectionSupervisor supervisor, string vhost, StreamProducerRegistry producers, ILoggerFactory? loggerFactory = null)
    {
        _supervisor = supervisor;
        _vhost = vhost;
        _producers = producers;
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
        ArgumentNullException.ThrowIfNull(options.Retry);
        if (options.OffsetStore.EveryMessages < 1)
        {
            throw new StreamBusConfigurationException("ConsumeOptions.OffsetStore.EveryMessages must be >= 1.");
        }
        if (options.OffsetStore.MaxInterval is { } maxInterval && maxInterval <= TimeSpan.Zero)
        {
            throw new StreamBusConfigurationException("ConsumeOptions.OffsetStore.MaxInterval must be positive when set.");
        }
        var retry = options.Retry;
        if (retry.MaxAttempts is { } maxAttempts && maxAttempts < 1)
        {
            throw new StreamBusConfigurationException("ConsumeOptions.Retry.MaxAttempts must be >= 1 when set.");
        }
        if (retry.FailureMode == FailureMode.Strict && retry.MaxAttempts is not null)
        {
            throw new StreamBusConfigurationException("ConsumeOptions.Retry.MaxAttempts must be null with FailureMode.Strict (unbounded retry).");
        }
        if (retry.FailureMode == FailureMode.SkipAfterDeadline && retry.MaxAttempts is null)
        {
            throw new StreamBusConfigurationException("ConsumeOptions.Retry.MaxAttempts is required with FailureMode.SkipAfterDeadline.");
        }
        if (retry.BaseDelay <= TimeSpan.Zero)
        {
            throw new StreamBusConfigurationException("ConsumeOptions.Retry.BaseDelay must be positive.");
        }
        if (retry.MaxDelay <= TimeSpan.Zero)
        {
            throw new StreamBusConfigurationException("ConsumeOptions.Retry.MaxDelay must be positive.");
        }
        if (retry.Multiplier < 1.0)
        {
            throw new StreamBusConfigurationException("ConsumeOptions.Retry.Multiplier must be >= 1.");
        }
        if (retry.MaxJitter < TimeSpan.Zero)
        {
            throw new StreamBusConfigurationException("ConsumeOptions.Retry.MaxJitter must be non-negative.");
        }
        if (retry.FailureMode == FailureMode.Strict && options.ErrorStream is not null)
        {
            throw new StreamBusConfigurationException("ConsumeOptions.ErrorStream must not be configured with FailureMode.Strict.");
        }
        if (retry.FailureMode == FailureMode.SkipAfterDeadline && options.ErrorStream is null)
        {
            throw new StreamBusConfigurationException("ConsumeOptions.ErrorStream is required with FailureMode.SkipAfterDeadline.");
        }
        if (options.ErrorStream is { } errorOptions)
        {
            ArgumentException.ThrowIfNullOrWhiteSpace(errorOptions.Stream);
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
        if (options.ErrorStream is { } error)
        {
            if (await handle.SuperStreamExistsAsync(error.Stream, ct).ConfigureAwait(false))
            {
                throw new StreamBusConfigurationException($"Error stream '{error.Stream}' must be a plain stream in v1 (super-stream error destinations are not supported).");
            }
            if (!await handle.StreamExistsAsync(error.Stream, ct).ConfigureAwait(false))
            {
                throw new StreamBusStreamNotFoundException(error.Stream);
            }
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
        var errors = new ErrorPublisher(_producers, _loggerFactory.CreateLogger<ErrorPublisher>());
        var loops = new List<(StreamPartitionConsumer Loop, Task RunTask)>(partitionStreams.Length);
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
                    _guard, state, StreamBusMetrics.Shared, _vhost, busName, errors,
                    _loggerFactory.CreateLogger<StreamPartitionConsumer>());
                loops.Add((loop, loop.RunAsync(family.Token)));
            }
            // The attach point is fixed before ConsumeAsync returns: wait for every
            // partition's first broker subscription (caller-cancellable; the shared
            // teardown below runs on cancellation or failure).
            await Task.WhenAll(loops.Select(l => l.Loop.FirstAttachAsync)).WaitAsync(ct).ConfigureAwait(false);
        }
        catch
        {
            family.Cancel();
            try
            {
                await Task.WhenAll(loops.Select(l => l.RunTask)).ConfigureAwait(false);
            }
            catch (Exception)
            {
                // Teardown observes the already-started tasks; the original failure is rethrown below.
            }
            foreach (var (loop, _) in loops)
            {
                await loop.DisposeAsync().ConfigureAwait(false);
            }
            foreach (var scope in scopes)
            {
                scope.Dispose();
            }
            family.Dispose();
            throw;
        }

        var registration = new StreamConsumerRegistration(stream, reference, loops, scopes, family, Remove);
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
            string[] partitions;
            try
            {
                partitions = await handle.QueryPartitionAsync(stream, ct).ConfigureAwait(false);
            }
            catch (OperationCanceledException)
            {
                throw;
            }
            catch (Exception)
            {
                // Check-then-act race: the super stream may have vanished between the
                // existence probe above and the partition query. Re-probe existence:
                // absent -> NotFound naming the stream, else rethrow the original.
                var recheckOk = false;
                var stillExists = true;
                try
                {
                    stillExists = await handle.SuperStreamExistsAsync(stream, ct).ConfigureAwait(false);
                    recheckOk = true;
                }
                catch (Exception)
                {
                    // Re-probe inconclusive: fall through and rethrow the original.
                }
                if (recheckOk && !stillExists)
                {
                    throw new StreamBusStreamNotFoundException(stream);
                }
                throw;
            }
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
