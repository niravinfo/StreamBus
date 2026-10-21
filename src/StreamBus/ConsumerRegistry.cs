using System.Collections.Concurrent;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using RabbitMQ.Stream.Client;

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
    private readonly Action? _onStopped;
    private int _disposed;

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
            _onStopped?.Invoke();
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
    private readonly ResultProducerRegistry _results;
    private readonly MonotonicOffsetGuard _guard = new();
    private readonly HashSet<StreamConsumerRegistration> _live = new();
    private readonly object _gate = new();
    private readonly IServiceScopeFactory? _scopes;
    private readonly ErrorQueueTransport? _queues;
    private readonly StreamBusHealthAggregator? _health;
    private readonly Func<RawConsumerConfig, CancellationToken, Task<IStreamConsumerHandle>>? _consumerFactory;
    private readonly ConcurrentDictionary<string, int> _rawSequence = new();
    private readonly StreamBusConsumeFilterStore? _consumeFilterStore;
    private readonly IServiceProvider? _rootProvider;
    private readonly ConcurrentDictionary<(Type BusType, ConsumeFilterRegistration Registration), IStreamConsumeFilter> _filterInstances = new();

    public StreamConsumerRegistry(StreamBusConnectionSupervisor supervisor, string vhost, StreamProducerRegistry producers, ILoggerFactory? loggerFactory, ResultProducerRegistry results, IServiceScopeFactory? scopeFactory = null, ErrorQueueTransport? queues = null, StreamBusHealthAggregator? health = null, Func<RawConsumerConfig, CancellationToken, Task<IStreamConsumerHandle>>? consumerFactory = null, StreamBusConsumeFilterStore? consumeFilters = null, IServiceProvider? rootProvider = null)
    {
        _supervisor = supervisor;
        _vhost = vhost;
        _producers = producers;
        ArgumentNullException.ThrowIfNull(results);
        _results = results;
        _scopes = scopeFactory;
        _queues = queues;
        _health = health;
        _consumerFactory = consumerFactory;
        _consumeFilterStore = consumeFilters;
        _rootProvider = rootProvider;
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

    private ConsumePipeline BuildPipeline(
        Type busType,
        string stream,
        IReadOnlyList<ConsumeFilterRegistration> endpointFilters,
        bool effectivelyOnce,
        bool typedEndpoint,
        bool allowSkip)
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
        AddFilters(busFilters, filters, "bus");
        AddFilters(endpointFilters, filters, "endpoint");

        foreach (var filter in filters)
        {
            if (effectivelyOnce && filter.CanSkip)
            {
                throw new StreamBusConfigurationException(
                    $"Consume filter '{filter.GetType().Name}' can skip messages (CanSkip), but effectively-once pipelines must never skip: a skipped source message would publish no result and replay forever.");
            }
            if (typedEndpoint && !allowSkip && filter.CanSkip)
            {
                throw new StreamBusConfigurationException(
                    $"Consume filter '{filter.GetType().Name}' can skip messages (CanSkip), but typed endpoint '{stream}' has not called AllowPipelineSkip(); a skipped message would advance the offset without invoking the consumer.");
            }
        }
        return new ConsumePipeline(filters);

        void AddFilters(IReadOnlyList<ConsumeFilterRegistration> registrations, List<IStreamConsumeFilter> target, string scope)
        {
            var seen = new HashSet<Type>();
            foreach (var registration in registrations)
            {
                var filter = _filterInstances.GetOrAdd(
                    (busType, registration),
                    _ => registration.Create(_rootProvider));
                if (!seen.Add(filter.GetType()))
                {
                    throw new StreamBusConfigurationException(
                        $"Consume filter '{filter.GetType()}' is already registered on this {scope}; duplicate registration would run it twice.");
                }
                target.Add(filter);
            }
        }
    }

    public async Task<IStreamConsumer> ConsumeAsync(
        Type busType, string stream, ConsumeOptions options,
        Func<ConsumeContext, Task> handler, CancellationToken ct = default, string? registrationName = null,
        IStreamSerializer? serializer = null)
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
        if (retry.Multiplier < 1.0 || !double.IsFinite(retry.Multiplier))
        {
            throw new StreamBusConfigurationException("ConsumeOptions.Retry.Multiplier must be a finite number >= 1.");
        }
        if (retry.MaxJitter < TimeSpan.Zero)
        {
            throw new StreamBusConfigurationException("ConsumeOptions.Retry.MaxJitter must be non-negative.");
        }
        if (retry.FailureMode == FailureMode.Strict && (options.ErrorStream is not null || options.DeadLetter is not null))
        {
            throw new StreamBusConfigurationException("Retry policy Strict never exhausts; a hand-off destination (ErrorStream/DeadLetter) cannot fire. Remove the destination or use a bounded retry schedule.");
        }
        if (retry.FailureMode == FailureMode.SkipAfterDeadline && options.ErrorStream is null && options.DeadLetter is null && options.ExhaustionCallback is null)
        {
            throw new StreamBusConfigurationException("ConsumeOptions.ErrorStream is required with FailureMode.SkipAfterDeadline (or a queue DeadLetter destination / OnExhausted callback).");
        }
        if (options.ErrorStream is { } errorOptions)
        {
            ArgumentException.ThrowIfNullOrWhiteSpace(errorOptions.Stream);
        }
        var reference = ConsumerReferenceValidation.Require(options.Reference, stream);

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
        registrationName ??= NextRawRegistration(stream);
        var tracker = _health is null ? null : new RegistrationTracker(_health, _vhost, registrationName, partitionStreams.Length);
        Action<int, BusRegistrationState, string?>? healthCallback = null;
        if (tracker is not null) healthCallback = tracker.Update;
        var family = new CancellationTokenSource();
        var errors = new ErrorPublisher(_producers, _loggerFactory.CreateLogger<ErrorPublisher>(), _queues);
        var pipeline = BuildPipeline(busType, stream, options.ConsumeFilters, false, options.IsTypedEndpoint, options.AllowPipelineSkip);
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
                    _loggerFactory.CreateLogger<StreamPartitionConsumer>(), null, null, _scopes,
                    _consumerFactory, healthCallback, serializer, pipeline);
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
            tracker?.Stop();
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

    public async Task<IStreamConsumer> ConsumeEffectivelyOnceAsync(
        Type busType, string sourceStream, string targetStream, EffectivelyOnceOptions options,
        Func<ConsumeContext, Task<ReadOnlyMemory<byte>>> handler, CancellationToken ct = default,
        IStreamSerializer? serializer = null)
    {
        ArgumentNullException.ThrowIfNull(busType);
        ArgumentException.ThrowIfNullOrWhiteSpace(sourceStream);
        ArgumentException.ThrowIfNullOrWhiteSpace(targetStream);
        ArgumentNullException.ThrowIfNull(options);
        ArgumentNullException.ThrowIfNull(handler);
        ArgumentNullException.ThrowIfNull(options.Retry);
        ArgumentNullException.ThrowIfNull(options.ReadinessPolicy);
        EffectivelyOnceValidation.ValidateConsume(options.Retry);
        var reference = options.Reference ?? ResultNaming.ConsumerReference(busType, sourceStream, targetStream);
        ConsumerReferenceValidation.Validate(reference);
        if (options.Reference is not null) ConsumerReferenceValidation.Validate(options.Reference);

        var readiness = await _supervisor.WaitUntilReadyAsync(options.ReadinessPolicy, ct).ConfigureAwait(false);
        if (readiness == BusReadiness.Fatal)
        {
            throw new StreamBusFatalException($"StreamBus for vhost '{_vhost}' is in Fatal state; effectively-once consume of '{sourceStream}' refused.");
        }
        if (readiness != BusReadiness.Ready)
        {
            throw new StreamBusNotReadyException($"StreamBus for vhost '{_vhost}' is not ready ({readiness}); effectively-once consume of '{sourceStream}' refused.");
        }
        var handle = _supervisor.CurrentHandle
            ?? throw new StreamBusNotReadyException($"StreamBus for vhost '{_vhost}' is not ready; effectively-once consume of '{sourceStream}' refused.");

        string[] sourcePartitions;
        if (options.SourceIsSuperStream)
        {
            if (!await handle.SuperStreamExistsAsync(sourceStream, ct).ConfigureAwait(false))
                throw new StreamBusStreamNotFoundException(sourceStream);
            sourcePartitions = await handle.QueryPartitionAsync(sourceStream, ct).ConfigureAwait(false);
        }
        else
        {
            if (await handle.SuperStreamExistsAsync(sourceStream, ct).ConfigureAwait(false))
                throw new StreamBusConfigurationException($"Effectively-once source '{sourceStream}' is a super stream but SourceIsSuperStream is false.");
            if (!await handle.StreamExistsAsync(sourceStream, ct).ConfigureAwait(false))
                throw new StreamBusStreamNotFoundException(sourceStream);
            sourcePartitions = new[] { sourceStream };
        }
        if (options.TargetIsSuperStream)
        {
            if (!await handle.SuperStreamExistsAsync(targetStream, ct).ConfigureAwait(false))
                throw new StreamBusStreamNotFoundException(targetStream);
        }
        else
        {
            if (await handle.SuperStreamExistsAsync(targetStream, ct).ConfigureAwait(false))
                throw new StreamBusConfigurationException($"Effectively-once target '{targetStream}' is a super stream but TargetIsSuperStream is false.");
            if (!await handle.StreamExistsAsync(targetStream, ct).ConfigureAwait(false))
                throw new StreamBusStreamNotFoundException(targetStream);
        }

        var busName = busType.FullName ?? busType.Name;
        var tracker = _health is null ? null : new RegistrationTracker(_health, _vhost, $"eo:{sourceStream}", sourcePartitions.Length);
        Action<int, BusRegistrationState, string?>? healthCallback = null;
        if (tracker is not null) healthCallback = tracker.Update;
        var family = new CancellationTokenSource();
        var errors = new ErrorPublisher(_producers, _loggerFactory.CreateLogger<ErrorPublisher>(), _queues);
        var pipeline = BuildPipeline(busType, sourceStream, Array.Empty<ConsumeFilterRegistration>(), true, false, false);
        var targetRouting = options.TargetRouting ?? (static ctx => ctx.RoutingValue);
        var loops = new List<(StreamPartitionConsumer Loop, Task RunTask)>(sourcePartitions.Length);
        var scopes = new List<IDisposable>(sourcePartitions.Length);
        try
        {
            for (var index = 0; index < sourcePartitions.Length; index++)
            {
                var partitionStream = sourcePartitions[index];
                var state = new PartitionState();
                scopes.Add(StreamBusMetrics.Shared.TrackPartition(_vhost, busName, sourceStream, index, reference, state));
                var consumeOptions = new ConsumeOptions
                {
                    IsSuperStream = options.SourceIsSuperStream,
                    Reference = reference,
                    SingleActiveConsumer = true,
                    Offset = ConsumeOffset.ResumeOrFirst,
                    Retry = options.Retry,
                    ReadinessPolicy = options.ReadinessPolicy,
                    EffectivelyOnce = new EffectivelyOnceConfig
                    {
                        TargetStream = targetStream,
                        TargetIsSuperStream = options.TargetIsSuperStream,
                        TargetRouting = targetRouting,
                        ResultHandler = handler,
                        ResumeQuery = _results.GetResumeOffsetAsync,
                    },
                };
                var loop = new StreamPartitionConsumer(
                    handle, sourceStream, partitionStream, index, reference, consumeOptions,
                    static _ => Task.CompletedTask, // unreached in EO mode (the branch above calls ResultHandler)
                    _guard, state, StreamBusMetrics.Shared, _vhost, busName, errors,
                    _loggerFactory.CreateLogger<StreamPartitionConsumer>(), consumeOptions.EffectivelyOnce, _results, _scopes,
                    _consumerFactory, healthCallback, serializer, pipeline);
                loops.Add((loop, loop.RunAsync(family.Token)));
            }
            // Same attach contract as ConsumeAsync: every partition's first broker
            // subscription is confirmed before the call returns.
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
            tracker?.Stop();
            throw;
        }

        Action? onStopped = null;
        if (tracker is not null) onStopped = tracker.Stop;
        var registration = new StreamConsumerRegistration(sourceStream, reference, loops, scopes, family, Remove, onStopped);
        lock (_gate)
        {
            _live.Add(registration);
        }
        return registration;
    }

    public async Task<ulong?> GetOffsetAsync(string stream, string reference, int partition, CancellationToken ct = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(stream);
        ConsumerReferenceValidation.Validate(reference);
        await EnsureReadyAsync(stream, ct).ConfigureAwait(false);
        var handle = _supervisor.CurrentHandle
            ?? throw new StreamBusNotReadyException($"StreamBus for vhost '{_vhost}' is not ready; offset query for '{stream}' refused.");
        var partitionStream = await ResolvePartitionStreamAsync(handle, stream, partition, ct).ConfigureAwait(false);
        return await handle.TryQueryOffsetAsync(reference, partitionStream, ct).ConfigureAwait(false);
    }

    public async Task SetOffsetAsync(string stream, string reference, int partition, ulong offset, CancellationToken ct = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(stream);
        ConsumerReferenceValidation.Validate(reference);
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

    public async Task<int> ReplayAsync(string errorStream, string? targetStream, CancellationToken ct = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(errorStream);
        if (targetStream is not null) ArgumentException.ThrowIfNullOrWhiteSpace(targetStream);
        await EnsureReadyAsync(errorStream, ct).ConfigureAwait(false);
        var handle = _supervisor.CurrentHandle
            ?? throw new StreamBusNotReadyException($"StreamBus for vhost '{_vhost}' is not ready; replay of '{errorStream}' refused.");
        if (await handle.SuperStreamExistsAsync(errorStream, ct).ConfigureAwait(false))
        {
            throw new StreamBusConfigurationException($"Error stream '{errorStream}' must be a plain stream in v1.");
        }
        if (!await handle.StreamExistsAsync(errorStream, ct).ConfigureAwait(false))
        {
            throw new StreamBusStreamNotFoundException(errorStream);
        }
        if (targetStream is not null)
        {
            if (await handle.SuperStreamExistsAsync(targetStream, ct).ConfigureAwait(false))
            {
                throw new StreamBusConfigurationException($"Replay target '{targetStream}' must be a plain stream in v1.");
            }
            if (!await handle.StreamExistsAsync(targetStream, ct).ConfigureAwait(false))
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
        var producers = new Dictionary<string, IStreamPlainProducerHandle>();
        void DisposeProducers()
        {
            foreach (var (_, producer) in producers)
            {
                try
                {
                    producer.DisposeAsync().AsTask().GetAwaiter().GetResult();
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
                    if (!ErrorEnvelope.TryParse(errorStream, ctx.Body.ToArray(), ctx.Metadata.ApplicationProperties, out var envelope) || envelope is null)
                    {
                        Interlocked.Increment(ref skipped);
                        _logger.LogWarning("Replay skipping malformed message on error stream '{Error}' at offset {Offset}.",
                            errorStream, ctx.Offset);
                        return;
                    }
                    var partitionStream = targetStream ?? envelope.SourcePartitionStream;
                    if (targetStream is null
                        && !await handle.StreamExistsAsync(partitionStream, ct).ConfigureAwait(false))
                    {
                        Interlocked.Increment(ref skipped);
                        _logger.LogWarning("Replay skipping message for vanished partition stream '{Partition}' (error offset {Offset}).",
                            partitionStream, ctx.Offset);
                        return;
                    }
                    if (!producers.TryGetValue(partitionStream, out var producer))
                    {
                        producer = await handle.CreatePlainProducerWithReferenceAsync(
                            partitionStream, ReplayNaming.ProducerName(envelope.SourceStream, partitionStream), ct).ConfigureAwait(false);
                        producers[partitionStream] = producer;
                    }
                    await producer.PublishReplayAsync(
                        envelope.Body,
                        ReplayNaming.PublishingId(envelope.SourceOffset),
                        new PublishOptions(),
                        ct).ConfigureAwait(false);
                    Interlocked.Increment(ref replayed);
                    Interlocked.Exchange(ref lastTicks, DateTimeOffset.UtcNow.UtcTicks);
                },
                guard, state, StreamBusMetrics.Shared, _vhost, "replay", errors,
                _loggerFactory.CreateLogger<StreamPartitionConsumer>());
            var run = loop.RunAsync(family.Token);
            try
            {
                await loop.FirstAttachAsync.WaitAsync(ct).ConfigureAwait(false);
                // No usable broker end offset exists (Phase 5 Round 2), so replay
                // terminates on idle: 5 s with no delivery and nothing in flight.
                var idle = TimeSpan.FromSeconds(5);
                while (true)
                {
                    ct.ThrowIfCancellationRequested();
                    var since = DateTimeOffset.UtcNow - new DateTimeOffset(Interlocked.Read(ref lastTicks), TimeSpan.Zero);
                    if (since >= idle && Volatile.Read(ref state.Inflight) == 0) break;
                    await Task.Delay(TimeSpan.FromMilliseconds(200), ct).ConfigureAwait(false);
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
            DisposeProducers();
        }
    }

    public async Task PublishResultAsync(string targetStream, ReadOnlyMemory<byte> result, ulong sourceOffset, PublishResultOptions? options, CancellationToken ct = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(targetStream);
        ArgumentNullException.ThrowIfNull(options);
        EffectivelyOnceValidation.ValidatePublishResult(options);
        await EnsureReadyAsync(targetStream, ct).ConfigureAwait(false);
        var handle = _supervisor.CurrentHandle
            ?? throw new StreamBusNotReadyException($"StreamBus for vhost '{_vhost}' is not ready; result publish to '{targetStream}' refused.");
        if (options.IsSuperStream)
        {
            if (!await handle.SuperStreamExistsAsync(targetStream, ct).ConfigureAwait(false))
                throw new StreamBusStreamNotFoundException(targetStream);
        }
        else
        {
            if (await handle.SuperStreamExistsAsync(targetStream, ct).ConfigureAwait(false))
                throw new StreamBusConfigurationException($"Result target '{targetStream}' is a super stream but PublishResultOptions.IsSuperStream is false.");
            if (!await handle.StreamExistsAsync(targetStream, ct).ConfigureAwait(false))
                throw new StreamBusStreamNotFoundException(targetStream);
        }
        var publisher = new ResultPublisher(_results, _loggerFactory.CreateLogger<ResultPublisher>());
        await publisher.PublishForeverAsync(
            new ResultWork
            {
                TargetStream = targetStream,
                TargetSuper = options.IsSuperStream,
                Body = result,
                SourceOffset = sourceOffset,
                RoutingKey = options.RoutingKey,
                ProducerName = options.ProducerName,
            },
            attempt => StreamBusDelays.ExponentialCapped(attempt, options.BaseDelay, options.MaxDelay, options.Multiplier, options.MaxJitter),
            ct).ConfigureAwait(false);
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

    private string NextRawRegistration(string stream)
    {
        var sequence = _rawSequence.AddOrUpdate(stream, 1, static (_, current) => current + 1);
        return sequence == 1 ? $"raw:{stream}" : $"raw:{stream}:{sequence}";
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
