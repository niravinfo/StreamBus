using System.Collections.Concurrent;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using RabbitMQ.Stream.Client;

namespace StreamBus;

/// <summary>
/// A live consumer subscription returned by a consume call. Dispose it to
/// stop the partitions; consumers resubscribe on connection loss, so this
/// is the only way to stop.
/// </summary>
public interface IStreamConsumer : IAsyncDisposable
{
    /// <summary>Logical stream name the subscription was started for (the
    /// super-stream name for a partitioned source).</summary>
    string Stream { get; }

    /// <summary>Consumer-group reference that owns the stored offsets.</summary>
    string Reference { get; }
}

internal sealed class StreamConsumerRegistration : IStreamConsumer
{
    private readonly List<(StreamPartitionConsumer Loop, Task RunTask)> _loops;
    private readonly List<IDisposable> _scopes;
    private readonly CancellationTokenSource _family;
    private readonly Action<StreamConsumerRegistration> _deregister;
    private readonly Action? _onStopped;
    private int _disposed;

    /// <summary>
    /// One live consume registration: the partition loops for a stream,
    /// their scopes, and the shared family cancellation source.
    /// </summary>
    /// <param name="stream">Logical stream name being consumed.</param>
    /// <param name="reference">Consumer-group reference for the stored offsets.</param>
    /// <param name="loops">Partition loops with their run tasks.</param>
    /// <param name="scopes">Meter disposables tracked alongside the loops.</param>
    /// <param name="family">Cancellation source shared by every partition loop.</param>
    /// <param name="deregister">Callback that removes this registration from the registry.</param>
    /// <param name="onStopped">Optional hook run after teardown completes.</param>
    public StreamConsumerRegistration(
        string stream,
        string reference,
        List<(StreamPartitionConsumer Loop, Task RunTask)> loops,
        List<IDisposable> scopes,
        CancellationTokenSource family,
        Action<StreamConsumerRegistration> deregister,
        Action? onStopped = null)
    {
        Stream = stream;
        Reference = reference;
        _loops = loops;
        _scopes = scopes;
        _family = family;
        _deregister = deregister;
        _onStopped = onStopped;
    }

    /// <summary>Logical stream name being consumed.</summary>
    public string Stream { get; }
    /// <summary>Consumer-group reference that owns the stored offsets.</summary>
    public string Reference { get; }

    /// <summary>
    /// Stops the registration: cancels every partition loop, drains the
    /// run tasks, disposes the loops, scopes, and family source, then runs
    /// the stopped hook and deregisters. Idempotent.
    /// </summary>
    public async ValueTask DisposeAsync()
    {
        if (Interlocked.Exchange(ref _disposed, 1) == 1)
        {
            return;
        }

        await ConsumerTeardown.RunAsync(
            _family,
            _loops,
            _scopes,
            onStopped: _onStopped,
            onDisposed: () => _deregister(this)).ConfigureAwait(false);
    }
}

/// <summary>
/// Single owner of consumer teardown: cancel the family, drain the run tasks, dispose the
/// partition loops, scopes, and cancellation source, then run the stopped/deregistered hooks.
/// Shared by the registration's <see cref="StreamConsumerRegistration.DisposeAsync"/> and the
/// failed-start cleanup in <see cref="StreamConsumerRegistry.ConsumeAsync"/>.
/// </summary>
internal static class ConsumerTeardown
{
    public static async Task RunAsync(
        CancellationTokenSource family,
        List<(StreamPartitionConsumer Loop, Task RunTask)> loops,
        List<IDisposable> scopes,
        Action? onStopped = null,
        Action? onDisposed = null,
        bool swallowRunTaskFailures = false)
    {
        family.Cancel();
        try
        {
            await Task.WhenAll(loops.Select(l => l.RunTask)).ConfigureAwait(false);
        }
        catch (Exception) when (swallowRunTaskFailures)
        {
            // The caller rethrows its original failure; the run tasks are observed only.
        }
        finally
        {
            foreach (var (loop, _) in loops)
            {
                await loop.DisposeAsync().ConfigureAwait(false);
            }
            foreach (var scope in scopes)
            {
                scope.Dispose();
            }
            family.Dispose();
            onStopped?.Invoke();
            onDisposed?.Invoke();
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
    private readonly IServiceScopeFactory? _scopes;
    private readonly ErrorQueueTransport? _queues;
    private readonly StreamBusHealthAggregator? _health;
    private readonly Func<IStreamSystemHandle, RawConsumerConfig, CancellationToken, Task<IStreamConsumerHandle>>? _consumerFactory;
    private readonly ConcurrentDictionary<(string BusName, string Stream), int> _rawSequence = new();
    private readonly StreamBusConsumeFilterStore? _consumeFilterStore;
    private readonly IServiceProvider? _rootProvider;
    private readonly ConcurrentDictionary<(Type BusType, ConsumeFilterRegistration Registration), IStreamConsumeFilter> _filterInstances = new();
    private readonly ConcurrentDictionary<(Type BusType, Type ClosedFilterType), object> _typedFilterInstances = new();
    private readonly ConcurrentDictionary<(Type BusType, Type ClosedFilterType, Type MessageType), IStreamConsumeFilter> _typedFilterAdapters = new();

    /// <summary>Creates a consumer registry bound to one vhost's supervisor
    /// and producer registry.</summary>
    public StreamConsumerRegistry(StreamBusConnectionSupervisor supervisor, string vhost, StreamProducerRegistry producers, ILoggerFactory? loggerFactory, IServiceScopeFactory? scopeFactory = null, ErrorQueueTransport? queues = null, StreamBusHealthAggregator? health = null, Func<IStreamSystemHandle, RawConsumerConfig, CancellationToken, Task<IStreamConsumerHandle>>? consumerFactory = null, StreamBusConsumeFilterStore? consumeFilters = null, IServiceProvider? rootProvider = null)
    {
        _supervisor = supervisor;
        _vhost = vhost;
        _producers = producers;
        _scopes = scopeFactory;
        _queues = queues;
        _health = health;
        _consumerFactory = consumerFactory;
        _consumeFilterStore = consumeFilters;
        _rootProvider = rootProvider;
        _loggerFactory = loggerFactory ?? NullLoggerFactory.Instance;
        _logger = _loggerFactory.CreateLogger<StreamConsumerRegistry>();
    }

    /// <summary>Snapshot of the currently live consumer registrations.</summary>
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

    private ConsumePipeline BuildPipeline(
        Type busType,
        string stream,
        IReadOnlyList<ConsumeFilterRegistration> endpointFilters,
        Type? messageType)
    {
        var busFilters = _consumeFilterStore?.FiltersFor(busType) ?? Array.Empty<ConsumeFilterRegistration>();
        if (busFilters.Count == 0 && endpointFilters.Count == 0)
            return new ConsumePipeline(Array.Empty<IStreamConsumeFilter>());
        if (_rootProvider is null)
        {
            throw new StreamBusConfigurationException(
                $"Consume filters are configured for {busType.FullName}, but no root service provider is available.");
        }

        var filters = new List<IStreamConsumeFilter>(busFilters.Count + endpointFilters.Count);
        AddFilters(busFilters, filters, "bus", endpointScope: false);
        AddFilters(endpointFilters, filters, "endpoint", endpointScope: true);

        return new ConsumePipeline(filters);

        void AddFilters(IReadOnlyList<ConsumeFilterRegistration> registrations, List<IStreamConsumeFilter> target, string scope, bool endpointScope)
        {
            var seen = new HashSet<Type>();
            foreach (var registration in registrations)
            {
                IStreamConsumeFilter filter;
                if (registration.IsTyped)
                {
                    if (messageType is null)
                        continue;
                    var closedType = registration.ClosedFilterType(messageType);
                    if (closedType is null)
                    {
                        if (endpointScope)
                            throw new StreamBusConfigurationException(
                                $"ReceiveEndpoint '{stream}': consume filter '{registration.FilterType}' does not implement IStreamConsumeFilter<{messageType.Name}> for this endpoint's message type.");
                        continue;
                    }
                    var instance = _typedFilterInstances.GetOrAdd(
                        (busType, closedType),
                        _ =>
                        {
                            FilterRegistrationSupport.EnsureSingletonLifetime(
                                _rootProvider!, closedType, "Consume");
                            return registration.ResolveTyped(closedType, _rootProvider!);
                        });
                    filter = _typedFilterAdapters.GetOrAdd(
                        (busType, closedType, messageType),
                        _ => registration.WrapTyped(messageType, instance));
                }
                else
                {
                    if (registration.Instance is null)
                    {
                        FilterRegistrationSupport.EnsureSingletonLifetime(
                            _rootProvider, registration.FilterType, "Consume");
                    }
                    filter = _filterInstances.GetOrAdd(
                        (busType, registration),
                        _ => registration.CreateUniversal(_rootProvider!));
                }
                var filterType = FilterIdentity.TypeOf(filter);
                if (!seen.Add(filterType))
                {
                    throw new StreamBusConfigurationException(
                        $"Consume filter '{filterType}' is already registered on this {scope}; duplicate registration would run it twice.");
                }
                target.Add(filter);
            }
        }
    }

    /// <summary>
    /// Starts a raw consume: verifies the options, waits for readiness,
    /// resolves the partition streams, and attaches one loop per partition.
    /// Returns only after every partition's first broker subscription;
    /// on failure the partial registration is torn down.
    /// </summary>
    /// <param name="busType">Marker type that owns the consume filters and health naming.</param>
    /// <param name="stream">Stream or super stream to consume.</param>
    /// <param name="options">Consume options; the reference is required.</param>
    /// <param name="handler">Handler invoked for each delivery.</param>
    /// <param name="cancellationToken">Cancellation for the attach; the subscription itself runs until disposed.</param>
    /// <param name="serializer">Optional serializer override for the pipeline.</param>
    /// <returns>A registration whose disposal stops the consumer.</returns>
    /// <exception cref="StreamBusConfigurationException">The options are invalid or contradictory.</exception>
    /// <exception cref="StreamBusStreamNotFoundException">The stream (or error destination) does not exist.</exception>
    /// <exception cref="StreamBusNotReadyException">The connection is not ready.</exception>
    /// <exception cref="StreamBusFatalException">The connection is in a fatal state.</exception>
    public Task<IStreamConsumer> ConsumeAsync(
        Type busType, string stream, ConsumeOptions options,
        Func<ConsumeContext, Task> handler, CancellationToken cancellationToken = default,
        IStreamSerializer? serializer = null)
        => ConsumeCoreAsync(busType, stream, options, handler, serializer, registrationName: null, cancellationToken);

    internal Task<IStreamConsumer> ConsumeDeclaredEndpointAsync(
        Type busType, string stream, ConsumeOptions options,
        Func<ConsumeContext, Task> handler, CancellationToken cancellationToken = default,
        IStreamSerializer? serializer = null)
        => ConsumeCoreAsync(busType, stream, options, handler, serializer,
            ConsumerHealthNames.Endpoint(busType.FullName ?? busType.Name, stream), cancellationToken);

    private async Task<IStreamConsumer> ConsumeCoreAsync(
        Type busType, string stream, ConsumeOptions options,
        Func<ConsumeContext, Task> handler, IStreamSerializer? serializer,
        string? registrationName, CancellationToken cancellationToken)
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
        if (options.ErrorStream is not null && options.OnExhausted is not null)
        {
            throw new StreamBusConfigurationException(
                "ConsumeOptions configures both ErrorStream and OnExhausted; the callback owns exhaustion, so configure exactly one (ErrorStream for a hand-off destination, OnExhausted for a terminal callback).");
        }
        var hasHandoff = options.ErrorStream is not null
            || options.DeadLetter is not null
            || options.OnExhausted is not null;
        if (retry.MaxAttempts is null)
        {
            if (hasHandoff)
            {
                throw new StreamBusConfigurationException(
                    "ConsumeOptions.Retry has no attempt limit (MaxAttempts is null); a hand-off destination (ErrorStream/DeadLetter/OnExhausted) never fires. Set MaxAttempts or remove the destination.");
            }
        }
        else if (!hasHandoff)
        {
            throw new StreamBusConfigurationException(
                "ConsumeOptions.Retry sets an attempt limit (MaxAttempts), but no hand-off destination is configured; configure ErrorStream, a DeadLetter destination, or an OnExhausted callback.");
        }
        if (retry.BaseDelay <= TimeSpan.Zero)
        {
            throw new StreamBusConfigurationException("ConsumeOptions.Retry.BaseDelay must be positive.");
        }
        if (retry.MaxDelay <= TimeSpan.Zero)
        {
            throw new StreamBusConfigurationException("ConsumeOptions.Retry.MaxDelay must be positive.");
        }
        if (retry.Multiplier < 1.0 || !double.IsFinite(retry.Multiplier))
        {
            throw new StreamBusConfigurationException("ConsumeOptions.Retry.Multiplier must be a finite number >= 1.");
        }
        if (retry.MaxJitter < TimeSpan.Zero)
        {
            throw new StreamBusConfigurationException("ConsumeOptions.Retry.MaxJitter must be non-negative.");
        }
        if (options.ErrorStream is { } errorOptions)
        {
            ArgumentException.ThrowIfNullOrWhiteSpace(errorOptions.Stream);
        }
        var reference = ConsumerReferenceValidation.Require(options.Reference, stream);

        var handle = await RequireReadyAsync(
            options.ReadinessPolicy, $"consume of '{stream}'", cancellationToken).ConfigureAwait(false);

        var exists = options.IsSuperStream
            ? await handle.SuperStreamExistsAsync(stream, cancellationToken).ConfigureAwait(false)
            : await handle.StreamExistsAsync(stream, cancellationToken).ConfigureAwait(false);
        if (!exists)
        {
            throw new StreamBusStreamNotFoundException(stream);
        }
        if (options.ErrorStream is { } error)
        {
            if (await handle.SuperStreamExistsAsync(error.Stream, cancellationToken).ConfigureAwait(false))
            {
                throw new StreamBusConfigurationException($"Error stream '{error.Stream}' must be a plain stream in v1 (super-stream error destinations are not supported).");
            }
            if (!await handle.StreamExistsAsync(error.Stream, cancellationToken).ConfigureAwait(false))
            {
                throw new StreamBusStreamNotFoundException(error.Stream);
            }
        }
        var partitionStreams = options.IsSuperStream
            ? await handle.QueryPartitionAsync(stream, cancellationToken).ConfigureAwait(false)
            : new[] { stream };

        if (options.Offset == ConsumeOffset.ResumeOrFail)
        {
            foreach (var partitionStream in partitionStreams)
            {
                ulong? stored;
                try
                {
                    stored = await handle.TryQueryOffsetAsync(reference, partitionStream, cancellationToken).ConfigureAwait(false);
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
        registrationName ??= NextRawRegistration(busName, stream);
        var tracker = _health is null ? null : new RegistrationTracker(_health, _vhost, registrationName, partitionStreams.Length);
        Action<int, BusRegistrationState, string?>? healthCallback = null;
        if (tracker is not null) healthCallback = tracker.Update;
        var family = new CancellationTokenSource();
        var errors = new ErrorPublisher(_producers, _loggerFactory.CreateLogger<ErrorPublisher>(), _queues);
        var pipeline = BuildPipeline(busType, stream, options.ConsumeFilters, options.MessageType);
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
                    _loggerFactory.CreateLogger<StreamPartitionConsumer>(), _scopes,
                    _consumerFactory, healthCallback, serializer, pipeline,
                    () => _supervisor.CurrentHandle);
                loops.Add((loop, loop.RunAsync(family.Token)));
            }
            // The attach point is fixed before ConsumeAsync returns: wait for every
            // partition's first broker subscription (caller-cancellable; the shared
            // teardown below runs on cancellation or failure).
            await Task.WhenAll(loops.Select(l => l.Loop.FirstAttachAsync)).WaitAsync(cancellationToken).ConfigureAwait(false);
        }
        catch
        {
            await ConsumerTeardown.RunAsync(
                family, loops, scopes, onStopped: tracker is null ? null : tracker.Stop, swallowRunTaskFailures: true).ConfigureAwait(false);
            throw;
        }

        Action? onStopped = null;
        if (tracker is not null) onStopped = tracker.Stop;
        var registration = new StreamConsumerRegistration(stream, reference, loops, scopes, family, Remove, onStopped);
        lock (_gate)
        {
            _live.Add(registration);
        }
        return registration;
    }

    /// <summary>Queries the stored offset of a consumer reference on one
    /// partition of a stream or super stream.</summary>
    /// <param name="stream">Stream name, or the super-stream name with a partition index.</param>
    /// <param name="reference">Consumer-group reference.</param>
    /// <param name="partition">Partition index; 0 for a plain stream.</param>
    /// <param name="cancellationToken">Cancellation for the query.</param>
    /// <returns>The stored offset, or <see langword="null"/> when the reference has none.</returns>
    /// <exception cref="StreamBusConfigurationException"><paramref name="partition"/> is out of range.</exception>
    /// <exception cref="StreamBusNotReadyException">The connection is not ready (fail-fast readiness).</exception>
    /// <exception cref="StreamBusFatalException">The connection is in a fatal state.</exception>
    /// <exception cref="StreamBusStreamNotFoundException">The stream does not exist.</exception>
    public async Task<ulong?> GetOffsetAsync(string stream, string reference, int partition, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(stream);
        ConsumerReferenceValidation.Validate(reference);
        var handle = await RequireReadyAsync(
            ReadinessPolicy.FailFastPolicy, $"offset query for '{stream}'", cancellationToken).ConfigureAwait(false);
        var partitionStream = await ResolvePartitionStreamAsync(handle, stream, partition, cancellationToken).ConfigureAwait(false);
        return await handle.TryQueryOffsetAsync(reference, partitionStream, cancellationToken).ConfigureAwait(false);
    }

    /// <summary>Overwrites the stored offset of a consumer reference on one
    /// partition, so the next consume with the same reference resumes from
    /// the new offset.</summary>
    /// <param name="stream">Stream name, or the super-stream name with a partition index.</param>
    /// <param name="reference">Consumer-group reference.</param>
    /// <param name="partition">Partition index; 0 for a plain stream.</param>
    /// <param name="offset">Offset to store.</param>
    /// <param name="cancellationToken">Cancellation for the store.</param>
    /// <exception cref="StreamBusConfigurationException"><paramref name="partition"/> is out of range.</exception>
    /// <exception cref="StreamBusNotReadyException">The connection is not ready (fail-fast readiness).</exception>
    /// <exception cref="StreamBusFatalException">The connection is in a fatal state.</exception>
    /// <exception cref="StreamBusStreamNotFoundException">The stream does not exist.</exception>
    public async Task SetOffsetAsync(string stream, string reference, int partition, ulong offset, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(stream);
        ConsumerReferenceValidation.Validate(reference);
        var handle = await RequireReadyAsync(
            ReadinessPolicy.FailFastPolicy, $"offset store for '{stream}'", cancellationToken).ConfigureAwait(false);
        var partitionStream = await ResolvePartitionStreamAsync(handle, stream, partition, cancellationToken).ConfigureAwait(false);
        await handle.StoreSystemOffsetAsync(reference, partitionStream, offset, cancellationToken).ConfigureAwait(false);
        _guard.Overwrite($"{reference}@{partitionStream}", offset);
        _logger.LogInformation(
            "Consumer offset for reference '{Reference}' on stream '{Stream}' partition {Partition} set to offset {Offset}.",
            reference, stream, partition, offset);
    }

    /// <summary>
    /// Replays an error stream: parses each error envelope and republishes
    /// the body to its original partition stream (or a single explicit
    /// target stream), preserving message ids and deduplicating by source
    /// offset. Ends when the stream is idle; malformed entries and vanished
    /// target streams are skipped with a warning.
    /// </summary>
    /// <param name="errorStream">Error stream to drain; must be a plain stream.</param>
    /// <param name="targetStream">Optional plain-stream override that receives every replayed body.</param>
    /// <param name="cancellationToken">Cancellation for the replay.</param>
    /// <returns>The number of messages replayed (skipped entries not counted).</returns>
    /// <exception cref="StreamBusConfigurationException"><paramref name="errorStream"/> or <paramref name="targetStream"/> is a super stream.</exception>
    /// <exception cref="StreamBusStreamNotFoundException">The error stream or target stream does not exist.</exception>
    /// <exception cref="StreamBusNotReadyException">The connection is not ready (fail-fast readiness).</exception>
    /// <exception cref="StreamBusFatalException">The connection is in a fatal state.</exception>
    public async Task<int> ReplayAsync(string errorStream, string? targetStream, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(errorStream);
        if (targetStream is not null) ArgumentException.ThrowIfNullOrWhiteSpace(targetStream);
        var handle = await RequireReadyAsync(
            ReadinessPolicy.FailFastPolicy, $"replay of '{errorStream}'", cancellationToken).ConfigureAwait(false);
        if (await handle.SuperStreamExistsAsync(errorStream, cancellationToken).ConfigureAwait(false))
        {
            throw new StreamBusConfigurationException($"Error stream '{errorStream}' must be a plain stream in v1.");
        }
        if (!await handle.StreamExistsAsync(errorStream, cancellationToken).ConfigureAwait(false))
        {
            throw new StreamBusStreamNotFoundException(errorStream);
        }
        if (targetStream is not null)
        {
            if (await handle.SuperStreamExistsAsync(targetStream, cancellationToken).ConfigureAwait(false))
            {
                throw new StreamBusConfigurationException($"Replay target '{targetStream}' must be a plain stream in v1.");
            }
            if (!await handle.StreamExistsAsync(targetStream, cancellationToken).ConfigureAwait(false))
            {
                throw new StreamBusStreamNotFoundException(targetStream);
            }
        }

        var reference = ReplayNaming.Reference(errorStream);
        var replayOptions = new ConsumeOptions { Offset = ConsumeOffset.AlwaysFirst };
        var guard = new MonotonicOffsetGuard(); // replay-scoped: never touches consumer watermarks.
        var state = new PartitionState(); // untracked: replay progress surfaces via the return count + logs, not the meter.
        var errors = new ErrorPublisher(_producers, _loggerFactory.CreateLogger<ErrorPublisher>(), _queues);
        var replayed = 0;
        var skipped = 0;
        long lastTicks = DateTimeOffset.UtcNow.UtcTicks; // cross-thread via Interlocked below.
        var producers = new Dictionary<(string Source, string Partition), IStreamPlainProducerHandle>();
        async Task DisposeProducersAsync()
        {
            foreach (var (_, producer) in producers)
            {
                try
                {
                    await producer.DisposeAsync().ConfigureAwait(false);
                }
                catch (Exception ex)
                {
                    _logger.LogDebug(ex, "Error disposing replay producer for error stream '{Error}'.", errorStream);
                }
            }
            producers.Clear();
        }
        using var family = new CancellationTokenSource();
        try
        {
            var loop = new StreamPartitionConsumer(
                handle, errorStream, errorStream, 0, reference, replayOptions,
                async ctx =>
                {
                    if (!ErrorEnvelope.TryParse(errorStream, ctx.Body.ToArray(), ctx.Headers.All, out var envelope) || envelope is null)
                    {
                        Interlocked.Increment(ref skipped);
                        _logger.LogWarning("Replay skipping malformed message on error stream '{Error}' at offset {Offset}.",
                            errorStream, ctx.Offset);
                        return;
                    }
                    // A supervisor swap can dispose the handle captured when replay
                    // started. Resolve per message so retries and producer creation use
                    // the live handle; a null handle is transient and retried by the
                    // dispatch loop.
                    var current = _supervisor.CurrentHandle
                        ?? throw new StreamBusTransientException(
                            $"StreamBus for vhost '{_vhost}' is not ready; replay of '{errorStream}' is retrying.");
                    var partitionStream = targetStream ?? envelope.SourcePartitionStream;
                    if (targetStream is null
                        && !await current.StreamExistsAsync(partitionStream, cancellationToken).ConfigureAwait(false))
                    {
                        Interlocked.Increment(ref skipped);
                        _logger.LogWarning("Replay skipping message for vanished partition stream '{Partition}' (error offset {Offset}).",
                            partitionStream, ctx.Offset);
                        return;
                    }
                    // Dedup is scoped to (stream, producer name, publishing id). A single
                    // replay target can receive equal source offsets from different source
                    // partitions, so the producer identity must include the source
                    // partition, not just the destination.
                    var producerKey = (envelope.SourceStream, envelope.SourcePartitionStream);
                    if (!producers.TryGetValue(producerKey, out var producer))
                    {
                        producer = await current.CreatePlainProducerWithReferenceAsync(
                            targetStream ?? envelope.SourcePartitionStream,
                            ReplayNaming.ProducerName(envelope.SourceStream, envelope.SourcePartitionStream),
                            cancellationToken).ConfigureAwait(false);
                        producers[producerKey] = producer;
                    }
                    await producer.PublishReplayAsync(
                        envelope.Body,
                        ReplayNaming.PublishingId(envelope.SourceOffset),
                        envelope.ToReplayOptions(),
                        cancellationToken).ConfigureAwait(false);
                    Interlocked.Increment(ref replayed);
                    Interlocked.Exchange(ref lastTicks, DateTimeOffset.UtcNow.UtcTicks);
                },
                guard, state, StreamBusMetrics.Shared, _vhost, "replay", errors,
                _loggerFactory.CreateLogger<StreamPartitionConsumer>(),
                consumerFactory: _consumerFactory,
                systemProvider: () => _supervisor.CurrentHandle);
            var run = loop.RunAsync(family.Token);
            try
            {
                await loop.FirstAttachAsync.WaitAsync(cancellationToken).ConfigureAwait(false);
                // No usable broker end offset exists (Phase 5 Round 2), so replay
                // terminates on idle: 5 s with no delivery and nothing in flight.
                var idle = TimeSpan.FromSeconds(5);
                while (true)
                {
                    cancellationToken.ThrowIfCancellationRequested();
                    var since = DateTimeOffset.UtcNow - new DateTimeOffset(Interlocked.Read(ref lastTicks), TimeSpan.Zero);
                    if (since >= idle && Volatile.Read(ref state.Inflight) == 0) break;
                    await Task.Delay(TimeSpan.FromMilliseconds(200), cancellationToken).ConfigureAwait(false);
                }
            }
            finally
            {
                family.Cancel();
                try
                {
                    await run.ConfigureAwait(false);
                }
                catch (OperationCanceledException)
                {
                }
                await loop.DisposeAsync().ConfigureAwait(false);
            }
            _logger.LogInformation("Replayed {Replayed} messages from error stream '{Error}' ({Skipped} skipped).",
                replayed, errorStream, skipped);
            return replayed;
        }
        finally
        {
            await DisposeProducersAsync().ConfigureAwait(false);
        }
    }

    private async Task<IStreamSystemHandle> RequireReadyAsync(
        ReadinessPolicy policy, string operation, CancellationToken cancellationToken)
    {
        var readiness = await _supervisor.WaitUntilReadyAsync(policy, cancellationToken).ConfigureAwait(false);
        if (readiness == BusReadiness.Fatal)
        {
            throw new StreamBusFatalException($"StreamBus for vhost '{_vhost}' is in Fatal state; {operation} refused.");
        }
        if (readiness != BusReadiness.Ready)
        {
            throw new StreamBusNotReadyException($"StreamBus for vhost '{_vhost}' is not ready ({readiness}); {operation} refused.");
        }
        return _supervisor.CurrentHandle
            ?? throw new StreamBusNotReadyException($"StreamBus for vhost '{_vhost}' is not ready; {operation} refused.");
    }

    private static async Task<string> ResolvePartitionStreamAsync(IStreamSystemHandle handle, string stream, int partition, CancellationToken cancellationToken)
    {
        if (await handle.StreamExistsAsync(stream, cancellationToken).ConfigureAwait(false))
        {
            if (partition != 0)
            {
                throw new StreamBusConfigurationException(
                    $"Partition {partition} is out of range for plain stream '{stream}' (only partition 0 exists).");
            }
            return stream;
        }
        if (await handle.SuperStreamExistsAsync(stream, cancellationToken).ConfigureAwait(false))
        {
            string[] partitions;
            try
            {
                partitions = await handle.QueryPartitionAsync(stream, cancellationToken).ConfigureAwait(false);
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
                    stillExists = await handle.SuperStreamExistsAsync(stream, cancellationToken).ConfigureAwait(false);
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

    private string NextRawRegistration(string busName, string stream)
    {
        var sequence = _rawSequence.AddOrUpdate((busName, stream), 1, static (_, current) => current + 1);
        return ConsumerHealthNames.Raw(busName, stream, sequence);
    }

    private sealed class RegistrationTracker
    {
        private readonly StreamBusHealthAggregator _health;
        private readonly string _vhost;
        private readonly string _registration;
        private readonly object _gate = new();
        private readonly bool[] _attached;
        private readonly string?[] _failed;
        private bool _stopped;

        public RegistrationTracker(StreamBusHealthAggregator health, string vhost, string registration, int partitions)
        {
            _health = health;
            _vhost = vhost;
            _registration = registration;
            _attached = new bool[partitions];
            _failed = new string?[partitions];
            lock (_gate) PublishLocked();
        }

        public void Update(int partition, BusRegistrationState state, string? reason)
        {
            if (partition < 0 || partition >= _attached.Length) return;
            lock (_gate)
            {
                if (_stopped) return;
                switch (state)
                {
                    case BusRegistrationState.Attached:
                        _attached[partition] = true;
                        _failed[partition] = null;
                        break;
                    case BusRegistrationState.FailedRetrying:
                        _attached[partition] = false;
                        _failed[partition] = reason ?? "unknown";
                        break;
                }
                PublishLocked();
            }
        }

        public void Stop()
        {
            lock (_gate)
            {
                if (_stopped) return;
                _stopped = true;
                PublishLocked();
            }
        }

        private void PublishLocked()
        {
            string? reason = _failed.FirstOrDefault(r => r is not null);
            var state = _stopped ? BusRegistrationState.Stopped
                : reason is not null ? BusRegistrationState.FailedRetrying
                : _attached.All(a => a) ? BusRegistrationState.Attached
                : BusRegistrationState.Attaching;
            _health.UpsertRegistration(_vhost, _registration, "consumer", state, reason);
        }
    }
}
