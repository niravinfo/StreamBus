using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using RabbitMQ.Stream.Client;
using System.Buffers;

namespace StreamBus;

// Internal seam around the client's concrete RawConsumer so the attach loop,
// death callbacks and watchdog can be exercised without a broker. The client
// exposes IsOpen() (no IsClosed); a closed entity is !IsOpen.
internal interface IStreamConsumerHandle
{
    bool IsOpen { get; }
    Task<ResponseCode> Close();
    Task StoreOffset(ulong offset);
}

internal sealed class RawConsumerHandle : IStreamConsumerHandle
{
    private readonly RawConsumer _consumer;

    public RawConsumerHandle(RawConsumer consumer)
    {
        ArgumentNullException.ThrowIfNull(consumer);
        _consumer = consumer;
    }

    public bool IsOpen => _consumer.IsOpen();
    public Task<ResponseCode> Close() => _consumer.Close();
    public Task StoreOffset(ulong offset) => _consumer.StoreOffset(offset);
}

internal sealed class OffsetStorer
{
    private readonly int _every;
    private readonly MonotonicOffsetGuard _guard;
    private readonly string _waterKey;
    private readonly Action<ulong> _onRejected;
    private int _sinceLastStore;
    private ulong _lastSuccess;
    private bool _hasSuccess;
    private ulong _lastStored;
    private bool _hasStored;

    public OffsetStorer(int every, MonotonicOffsetGuard guard, string waterKey, Action<ulong> onRejected)
    {
        _every = every;
        _guard = guard;
        _waterKey = waterKey;
        _onRejected = onRejected;
    }

    public ulong LastSuccess => _lastSuccess;
    public bool HasSuccess => _hasSuccess;

    public bool ShouldStoreOnSuccess(ulong offset)
    {
        _lastSuccess = offset;
        _hasSuccess = true;
        _sinceLastStore++;
        return _sinceLastStore >= _every;
    }

    public void Stored(ulong offset)
    {
        _sinceLastStore = 0;
        _lastSuccess = offset;
        _lastStored = offset;
        _hasStored = true;
    }

    public bool IsStored(ulong offset) => _hasStored && _lastStored == offset;

    public bool TryAdmit(ulong candidate)
    {
        if (_guard.TryAdmit(_waterKey, candidate)) return true;
        _onRejected(candidate);
        return false;
    }
}

internal sealed class StreamPartitionConsumer : IAsyncDisposable
{
    // owns: attach loop (create → resolve → consume → failure/backoff/recreate),
    // promotion listener (resolve on every promote, never throws),
    // dispatch wrapper (context build, strictly-awaited handler, cadence stores),
    // regression read-back after the first store per attach, metrics state.

    // Sequential dispatch invariant: every delivery for this partition flows through
    // _dispatchGate, which is held across the awaited handler call, so at most one
    // handler runs at a time and offsets complete in delivery order. Handler failures
    // never escape into the client callback: the wrapper records them on the attach
    // state and returns, and the attach loop observes the signal outside the callback,
    // closes the consumer, backs off, and recreates from the stored offset.

    private const string RoutingKeyProperty = "streambus.routing-key";
    private const string PromotionFallbackBranch = "PromotionFailed";

    // Parity with the ConnectionSupervisor defaults (200 ms base, 30 s cap).
    private static readonly TimeSpan AttachBaseDelay = TimeSpan.FromMilliseconds(200);
    private static readonly TimeSpan AttachMaxDelay = TimeSpan.FromSeconds(30);

    private readonly IStreamSystemHandle _system;
    private readonly string _stream;
    private readonly string _partitionStream;
    private readonly int _partitionIndex;
    private readonly string _reference;
    private readonly ConsumeOptions _options;
    private readonly Func<ConsumeContext, Task> _handler;
    private readonly MonotonicOffsetGuard _guard;
    private readonly PartitionState _state;
    private readonly StreamBusMetrics _metrics;
    private readonly string _vhost;
    private readonly string _bus;
    private readonly ErrorPublisher _errors;
    private readonly ILogger<StreamPartitionConsumer> _logger;
    private readonly EffectivelyOnceConfig? _eo;
    private readonly ResultProducerRegistry? _results;
    private readonly string _waterKey;
    private readonly OffsetStorer _storer;
    private readonly IServiceScopeFactory? _scopes;
    // Configuration callbacks are built before the consumer exists; the holder
    // lets them see the instance without resurrecting races on the current field.
    private readonly ConsumerHolder _consumerHandle = new();
    private readonly Func<RawConsumerConfig, CancellationToken, Task<IStreamConsumerHandle>> _createConsumer;
    private readonly Action<int, BusRegistrationState, string?>? _health;
    private readonly IStreamSerializer _serializer;

    private readonly SemaphoreSlim _dispatchGate = new(1, 1);
    private readonly SemaphoreSlim _storeGate = new(1, 1);
    private readonly object _life = new();
    private readonly CancellationTokenSource _loopCts = new();
    private readonly TaskCompletionSource _firstAttach = new(TaskCreationOptions.RunContinuationsAsynchronously);
    private PeriodicTimer? _timer;
    private Task? _inflight;
    private int _disposed;

    public StreamPartitionConsumer(
        IStreamSystemHandle system,
        string stream,
        string partitionStream,
        int partitionIndex,
        string reference,
        ConsumeOptions options,
        Func<ConsumeContext, Task> handler,
        MonotonicOffsetGuard guard,
        PartitionState state,
        StreamBusMetrics metrics,
        string vhost,
        string bus,
        ErrorPublisher errorPublisher,
        ILogger<StreamPartitionConsumer>? logger = null,
        EffectivelyOnceConfig? effectivelyOnce = null,
        ResultProducerRegistry? results = null,
        IServiceScopeFactory? scopeFactory = null,
        Func<RawConsumerConfig, CancellationToken, Task<IStreamConsumerHandle>>? consumerFactory = null,
        Action<int, BusRegistrationState, string?>? health = null,
        IStreamSerializer? serializer = null)
    {
        ArgumentNullException.ThrowIfNull(system);
        ArgumentNullException.ThrowIfNull(stream);
        ArgumentNullException.ThrowIfNull(partitionStream);
        ArgumentNullException.ThrowIfNull(reference);
        ArgumentNullException.ThrowIfNull(options);
        ArgumentNullException.ThrowIfNull(handler);
        ArgumentNullException.ThrowIfNull(guard);
        ArgumentNullException.ThrowIfNull(state);
        ArgumentNullException.ThrowIfNull(metrics);
        ArgumentNullException.ThrowIfNull(vhost);
        ArgumentNullException.ThrowIfNull(bus);
        ArgumentNullException.ThrowIfNull(errorPublisher);
        if (effectivelyOnce is not null && results is null)
            throw new ArgumentException("Effectively-once requires a result producer registry.", nameof(results));
        if (effectivelyOnce is null && results is not null)
            throw new ArgumentException("A result producer registry without effectively-once config is inconsistent.", nameof(effectivelyOnce));
        _eo = effectivelyOnce;
        _results = results;
        _scopes = scopeFactory;
        _system = system;
        _stream = stream;
        _partitionStream = partitionStream;
        _partitionIndex = partitionIndex;
        _reference = reference;
        _options = options;
        _handler = handler;
        _guard = guard;
        _state = state;
        _metrics = metrics;
        _vhost = vhost;
        _bus = bus;
        _errors = errorPublisher;
        _logger = logger ?? NullLogger<StreamPartitionConsumer>.Instance;
        _createConsumer = consumerFactory
            ?? (async (config, ct) => new RawConsumerHandle(
                await _system.CreateRawConsumerAsync(config, ct).ConfigureAwait(false)));
        _health = health;
        _serializer = serializer ?? SystemTextJsonStreamSerializer.Default;
        // Same key format Task 4's SetOffsetAsync overwrites on the shared guard.
        _waterKey = $"{reference}@{partitionStream}";
        _storer = new OffsetStorer(
            options.OffsetStore.EveryMessages,
            guard,
            _waterKey,
            rejected =>
            {
                _metrics.RecordStoreRejected(_vhost, _bus, _stream, _partitionIndex, _reference);
                _logger.LogDebug(
                    "Offset store rejected for stream {Stream} partition {Partition} offset {Offset} (not forward of local water).",
                    _stream, _partitionIndex, rejected);
            });
    }

    // Completes when the broker confirms the first subscription. ConsumeAsync
    // awaits this so the attach point is fixed before the call returns: publishes
    // issued after ConsumeAsync returns are deterministically post-attach (an
    // AlwaysNext tail can never miss them to a subscribe race).
    internal Task FirstAttachAsync => _firstAttach.Task;

    public async Task RunAsync(CancellationToken ct)
    {
        using var linked = CancellationTokenSource.CreateLinkedTokenSource(ct, _loopCts.Token);
        var loopCt = linked.Token;
        var failures = 0;
        while (!loopCt.IsCancellationRequested)
        {
            IStreamConsumerHandle? consumer = null;
            Exception? attachError = null;
            try
            {
                var resolved = await OffsetResolution.ResolveWithQueryAsync(
                    _options.Offset,
                    () => QueryStoredAsync(loopCt),
                    _guard.Water(_waterKey),
                    _reference,
                    _partitionStream,
                    _logger,
                    branch => _metrics.RecordFallback(_vhost, _bus, _stream, _partitionIndex, _reference, branch)).ConfigureAwait(false);

                var attach = new AttachState { LoopToken = loopCt };
                // SAC+Next subscriptions receive nothing on this broker (tail attach
                // cannot join an SAC group), so Next-resolved consumers attach
                // non-exclusively. Promotion-time Next on an SAC-created consumer
                // remains a documented residual corner.
                var singleActive = _options.SingleActiveConsumer && resolved is not OffsetTypeNext;
                if (_options.SingleActiveConsumer && !singleActive)
                {
                    _logger.LogInformation(
                        "SAC disabled for stream {Stream} partition {Partition}: Next-resolved consumers cannot join an SAC group (tail attach receives nothing under SAC on this broker).",
                        _stream, _partitionIndex);
                }
                var config = new RawConsumerConfig(_partitionStream)
                {
                    Reference = _reference,
                    OffsetSpec = resolved,
                    IsSingleActiveConsumer = singleActive,
                    ConsumerUpdateListener = OnPromotedAsync,
                    MessageHandler = (c, context, message) =>
                    {
                        var tracked = _consumerHandle.Current ?? new RawConsumerHandle(c);
                        var dispatch = DispatchAsync(tracked, context, message, attach);
                        lock (_life)
                        {
                            _inflight = dispatch;
                        }
                        return AwaitTrackedAsync(dispatch);
                    },
                    ConnectionClosedHandler = reason =>
                    {
                        // The client marks the consumer closed and delivers nothing further;
                        // the only recovery is our attach loop closing and recreating.
                        _consumerHandle.Clear();
                        FailAttach(
                            attach,
                            new StreamBusTransientException($"Consumer subscription closed by the stream client ({reason})."),
                            "consumer_connection_closed");
                        return Task.CompletedTask;
                    },
                    MetadataHandler = metadata =>
                    {
                        // The client closes the consumer on a metadata change (e.g.
                        // leader move). Probe the current handle so a closed one is
                        // reported once; MUST NOT THROW (the client's fallback for a
                        // throwing listener is OffsetNext = a skipped backlog).
                        if (_consumerHandle.Current is { IsOpen: false })
                        {
                            _consumerHandle.Clear();
                            FailAttach(
                                attach,
                                new StreamBusTransientException($"Stream metadata change closed the subscription ({metadata.Stream}/{metadata.Code})."),
                                "consumer_metadata_change");
                        }
                        return Task.CompletedTask;
                    },
                };

                consumer = await _createConsumer(config, loopCt).ConfigureAwait(false);
                _firstAttach.TrySetResult();
                _consumerHandle.Set(consumer);
                _health?.Invoke(_partitionIndex, BusRegistrationState.Attached, null);
                failures = 0; // A successful attach resets the backoff streak.
                Interlocked.Exchange(ref _state.RetryBlocked, 0); // A recreated attach never inherits a stuck 1 from a failed attach that died mid-retry.
                _logger.LogInformation(
                    "Consume attach ready for stream {Stream} partition {Partition} reference {Reference}.",
                    _stream, _partitionIndex, _reference);

                PeriodicTimer? timer = null;
                Task? timerTask = null;
                CancellationTokenSource? tickCts = null;
                if (_eo is null && _options.OffsetStore.MaxInterval is { } interval && interval > TimeSpan.Zero)
                {
                    timer = new PeriodicTimer(interval);
                    lock (_life)
                    {
                        _timer = timer;
                    }
                    tickCts = CancellationTokenSource.CreateLinkedTokenSource(loopCt);
                    timerTask = TickAsync(consumer, timer, attach, tickCts.Token);
                }

                try
                {
                    // Suspend until handler failure or shutdown. The failure task never
                    // completes successfully, so this await only ends by rethrowing the
                    // handler exception (attach failure below) or by cancellation (exit).
                    // The 5s liveness watchdog is defensive: callbacks are the primary
                    // signal, but a closed handle without a callback must not park forever.
                    while (true)
                    {
                        try
                        {
                            await attach.Failure.Task.WaitAsync(TimeSpan.FromSeconds(5), loopCt).ConfigureAwait(false);
                            break; // failure task completed successfully: impossible
                        }
                        catch (TimeoutException)
                        {
                            if (_consumerHandle.Current is { IsOpen: false })
                            {
                                _consumerHandle.Clear();
                                var watchdog = new StreamBusTransientException(
                                    "Consumer handle reports closed while parked (watchdog).");
                                FailAttach(attach, watchdog, "consumer_watchdog");
                                throw watchdog;
                            }
                            // Handle still open: keep waiting.
                        }
                    }
                }
                finally
                {
                    if (tickCts is not null)
                    {
                        tickCts.Cancel();
                        tickCts.Dispose();
                    }
                    if (timerTask is not null)
                    {
                        try
                        {
                            await timerTask.ConfigureAwait(false);
                        }
                        catch (OperationCanceledException)
                        {
                            // Expected on attach teardown (tick scope cancelled).
                        }
                        catch (ObjectDisposedException)
                        {
                            // Timer disposed during teardown; the tick loop is over.
                        }
                        catch (Exception ex)
                        {
                            _logger.LogWarning(
                                ex,
                                "Offset timer for stream {Stream} partition {Partition} reference {Reference} faulted.",
                                _stream, _partitionIndex, _reference);
                        }
                    }
                    timer?.Dispose();
                    lock (_life)
                    {
                        if (ReferenceEquals(_timer, timer))
                        {
                            _timer = null;
                        }
                    }
                }
            }
            catch (OperationCanceledException) when (loopCt.IsCancellationRequested)
            {
                await CloseQuietlyAsync(consumer).ConfigureAwait(false);
                _firstAttach.TrySetCanceled(loopCt);
                return;
            }
            catch (Exception ex)
            {
                attachError = ex;
                failures++;
            }

            // Brief order: close the consumer first, then back off, then recreate
            // (redelivery comes from the stored offset).
            await CloseQuietlyAsync(consumer).ConfigureAwait(false);

            if (attachError is not null)
            {
                _logger.LogWarning(
                    attachError,
                    "Consume attach for stream {Stream} partition {Partition} reference {Reference} failed (streak {Attempt}); backing off before recreate.",
                    _stream, _partitionIndex, _reference, failures);
                try
                {
                    await Task.Delay(
                        StreamBusDelays.ExponentialCapped(failures, AttachBaseDelay, AttachMaxDelay),
                        loopCt).ConfigureAwait(false);
                }
                catch (OperationCanceledException) when (loopCt.IsCancellationRequested)
                {
                    return;
                }
            }
        }
    }

    public async ValueTask DisposeAsync()
    {
        if (Interlocked.Exchange(ref _disposed, 1) == 1)
        {
            return;
        }
        _loopCts.Cancel();
        IStreamConsumerHandle? consumer;
        PeriodicTimer? timer;
        lock (_life)
        {
            consumer = _consumerHandle.Current;
            timer = _timer;
        }
        await CloseQuietlyAsync(consumer).ConfigureAwait(false);
        timer?.Dispose();
        Task? inflight;
        lock (_life)
        {
            inflight = _inflight;
        }
        if (inflight is not null)
        {
            // A hung handler blocks disposal by design (same ceiling as hot keys).
            try
            {
                await inflight.ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                _logger.LogDebug(
                    ex,
                    "In-flight dispatch faulted during disposal for stream {Stream} partition {Partition}.",
                    _stream, _partitionIndex);
            }
        }
        // The gates are intentionally not disposed: client callbacks may still hold
        // references during teardown, and disposing a SemaphoreSlim under a queued
        // waiter would fault that waiter into the client callback.
        _loopCts.Dispose();
        lock (_life)
        {
            _consumerHandle.Clear();
            _timer = null;
            _inflight = null;
        }
    }

    private async Task DispatchAsync(IStreamConsumerHandle consumer, MessageContext context, Message message, AttachState attach)
    {
        await _dispatchGate.WaitAsync().ConfigureAwait(false);
        try
        {
            // Post-failure deliveries racing the loop's close are skipped; they are
            // redelivered from the stored offset after recreate.
            if (Interlocked.CompareExchange(ref attach.Drained, 0, 0) == 1)
            {
                return;
            }
            var offset = context.Offset;
            Interlocked.Exchange(ref _state.LastDelivered, (long)offset);

            ConsumeContext consumeContext;
            try
            {
                consumeContext = BuildContext(offset, message, attach.LoopToken);
            }
            catch (Exception ex)
            {
                FailAttach(attach, offset, ex, "Failed to build consume context");
                return;
            }

            Interlocked.Increment(ref _state.Inflight);
            try
            {
                if (_eo is not null)
                {
                    await DispatchEffectivelyOnceAsync(offset, consumeContext, attach).ConfigureAwait(false);
                    return;
                }
                // In-handler retry (FR-4.1): the attempt loop runs under the already-held
                // dispatch gate, so a retry sleep stalls exactly this partition and order
                // is preserved by construction. `attempts` counts completed handler
                // attempts (initial = 1), matching MaxAttempts semantics.
                var attempts = 0;
                while (true)
                {
                    attempts++;
                    try
                    {
                        await _handler(consumeContext).ConfigureAwait(false);
                        break;
                    }
                    catch (OperationCanceledException) when (attach.LoopToken.IsCancellationRequested)
                    {
                        Interlocked.Decrement(ref _state.Inflight);
                        Interlocked.Exchange(ref _state.RetryBlocked, 0);
                        return;
                    }
                    catch (Exception ex)
                    {
                        var strict = _options.Retry.FailureMode == FailureMode.Strict;
                        var exhausted = !strict && attempts >= (_options.Retry.MaxAttempts ?? int.MaxValue);
                        if (RetryDelays.ShouldLogRetryWarning(attempts))
                            _logger.LogWarning(ex,
                                "Consume handler failed for stream {Stream} partition {Partition} offset {Offset} (attempt {Attempt}, mode {Mode}); retrying in-handler.",
                                _stream, _partitionIndex, offset, attempts, _options.Retry.FailureMode);
                        else
                            _logger.LogDebug(ex,
                                "Consume handler failed for stream {Stream} partition {Partition} offset {Offset} (attempt {Attempt}, mode {Mode}); retrying in-handler.",
                                _stream, _partitionIndex, offset, attempts, _options.Retry.FailureMode);
                        if (exhausted)
                        {
                            Interlocked.Decrement(ref _state.Inflight);
                            Interlocked.Exchange(ref _state.RetryBlocked, 0);
                            await ExhaustAsync(consumer, offset, consumeContext, attempts, ex, attach).ConfigureAwait(false);
                            return;
                        }
                        Interlocked.Exchange(ref _state.RetryBlocked, 1);
                        var delay = RetryDelays.ResolveDelay(_options.Retry, attempts, ex, _logger);
                        try
                        {
                            await Task.Delay(delay, attach.LoopToken).ConfigureAwait(false);
                        }
                        catch (OperationCanceledException) when (attach.LoopToken.IsCancellationRequested)
                        {
                            Interlocked.Decrement(ref _state.Inflight);
                            Interlocked.Exchange(ref _state.RetryBlocked, 0);
                            return;
                        }
                    }
                }
            }
            catch (OperationCanceledException) when (attach.LoopToken.IsCancellationRequested)
            {
                // Shutdown raced dispatch; exit quietly without signalling failure.
                return;
            }
            Interlocked.Decrement(ref _state.Inflight);
            Interlocked.Exchange(ref _state.RetryBlocked, 0);

            if (_eo is null && _storer.ShouldStoreOnSuccess(offset))
            {
                await StoreAndTrackAsync(consumer, offset, attach, attach.LoopToken).ConfigureAwait(false);
            }
        }
        catch (OperationCanceledException) when (attach.LoopToken.IsCancellationRequested)
        {
            // Shutdown raced dispatch; exit quietly without signalling failure.
        }
        catch (Exception ex)
        {
            // Safety net: dispatch must never throw into the client callback.
            // Unwind via the attach loop instead (close + backoff + recreate).
            FailAttach(attach, context.Offset, ex, "Consume dispatch faulted");
        }
        finally
        {
            _dispatchGate.Release();
        }
    }

    // SkipAfterDeadline exhaustion: confirmed error-stream handoff first, single
    // advance-store second, observe-only hook third. Never throws into dispatch:
    // HandOffAsync's commit failure propagates to the dispatch safety net (attach
    // recycle + redeliver), never to a silent skip.
    private async Task ExhaustAsync(IStreamConsumerHandle consumer, ulong offset, ConsumeContext consumeContext, int attempts, Exception lastError, AttachState attach)
    {
        if (_options.ExhaustionCallback is { } owned)
        {
            _logger.LogWarning(lastError,
                "Retry budget exhausted for stream {Stream} partition {Partition} offset {Offset} after {Attempts} attempts; handing off to the OnExhausted callback.",
                _stream, _partitionIndex, offset, attempts);
            var scopes = _scopes ?? throw new StreamBusConfigurationException(
                $"OnExhausted callback for stream '{_stream}' has no IServiceScopeFactory available (internal error).");
            using var scope = scopes.CreateScope();
            var exhausted = new ExhaustedContext
            {
                Stream = _stream,
                Partition = _partitionIndex,
                Offset = offset,
                Body = consumeContext.Body,
                Metadata = consumeContext.Metadata,
                Attempts = attempts,
                Exception = lastError,
                FailedAt = DateTimeOffset.UtcNow,
                Services = scope.ServiceProvider,
            };
            await ExhaustionCallbackRunner.RunAsync(
                owned,
                exhausted,
                () => AdmitAndCommitStoreAsync(consumer, offset, attach, attach.LoopToken),
                ex =>
                {
                    _logger.LogWarning(ex,
                        "OnExhausted callback faulted for stream {Stream} partition {Partition}; recycling the attach (redeliver, never skip).",
                        _stream, _partitionIndex);
                    FailAttach(attach, offset, ex, "OnExhausted callback faulted");
                },
                attach.LoopToken).ConfigureAwait(false);
            return;
        }
        if (_options.DeadLetter is { } queue)
        {
            var queueEnvelope = new ErrorEnvelope
            {
                ErrorStream = queue.Name,
                SourceStream = _stream,
                SourcePartition = _partitionIndex,
                SourcePartitionStream = _partitionStream,
                SourceOffset = offset,
                Body = consumeContext.Body.ToArray(),
                ExceptionType = lastError.GetType().FullName ?? lastError.GetType().Name,
                ExceptionMessage = lastError.Message,
                ExceptionStack = lastError.ToString(),
                Attempts = attempts,
                FailedAt = DateTimeOffset.UtcNow,
                MessageId = consumeContext.Metadata.MessageId,
                CorrelationId = consumeContext.Metadata.CorrelationId,
                RoutingValue = consumeContext.RoutingValue,
                ErrorRoutingValue = _stream,
            };
            _logger.LogWarning(lastError,
                "Retry budget exhausted for stream {Stream} partition {Partition} offset {Offset} after {Attempts} attempts; handing off to error queue {ErrorQueue}.",
                _stream, _partitionIndex, offset, attempts, queue.Name);
            await _errors.HandOffAsync(
                queueEnvelope,
                (env, ct) => _errors.PublishQueueHandoffAsync(queue, env, ct),
                () => AdmitAndCommitStoreAsync(consumer, offset, attach, attach.LoopToken),
                (n, ex) => RetryDelays.ResolveDelay(_options.Retry, n, ex, _logger),
                attach.LoopToken).ConfigureAwait(false);
            return;
        }
        var errorStream = _options.ErrorStream!.Stream;
        var failedAt = DateTimeOffset.UtcNow;
        var envelope = new ErrorEnvelope
        {
            ErrorStream = errorStream,
            SourceStream = _stream,
            SourcePartition = _partitionIndex,
            SourcePartitionStream = _partitionStream,
            SourceOffset = offset,
            Body = consumeContext.Body.ToArray(),
            ExceptionType = lastError.GetType().FullName ?? lastError.GetType().Name,
            ExceptionMessage = lastError.Message,
            ExceptionStack = lastError.ToString(),
            Attempts = attempts,
            FailedAt = failedAt,
            MessageId = consumeContext.Metadata.MessageId,
            CorrelationId = consumeContext.Metadata.CorrelationId,
            RoutingValue = consumeContext.RoutingValue,
            ErrorRoutingValue = _options.ErrorStream.RoutingValue ?? _stream,
        };
        _logger.LogWarning(lastError,
            "Retry budget exhausted for stream {Stream} partition {Partition} offset {Offset} after {Attempts} attempts; handing off to error stream {ErrorStream}.",
            _stream, _partitionIndex, offset, attempts, errorStream);
        // The handoff commit resets the cadence counter via Stored(): the handoff
        // offset is always stored exactly once regardless of EveryMessages, and the
        // MaxInterval timer then skips it via IsStored.
        await _errors.HandOffAsync(
            envelope,
            (env, ct) => _errors.PublishHandoffAsync(errorStream, env, _options.HandoffConfirmTimeout, ct),
            () => AdmitAndCommitStoreAsync(consumer, offset, attach, attach.LoopToken),
            (n, ex) => RetryDelays.ResolveDelay(_options.Retry, n, ex, _logger),
            attach.LoopToken,
            _options.HandoffConfirmTimeout).ConfigureAwait(false);
        if (_options.OnRetryExhausted is { } hook)
        {
            var handoff = new ErrorHandoff
            {
                ErrorStream = errorStream,
                SourceStream = _stream,
                SourcePartition = _partitionIndex,
                SourceOffset = offset,
                Attempts = attempts,
                FailedAt = failedAt,
                Exception = lastError,
            };
            var hookTask = hook(handoff);
            try
            {
                await hookTask.WaitAsync(attach.LoopToken).ConfigureAwait(false);
            }
            catch (OperationCanceledException) when (attach.LoopToken.IsCancellationRequested)
            {
                _ = hookTask.ContinueWith(
                    t =>
                    {
                        if (t.Exception is { } hex)
                            _logger.LogWarning(hex,
                                "OnRetryExhausted hook faulted after abandon for stream {Stream} partition {Partition} reference {Reference}.",
                                _stream, _partitionIndex, _reference);
                    },
                    CancellationToken.None,
                    TaskContinuationOptions.OnlyOnFaulted | TaskContinuationOptions.ExecuteSynchronously,
                    TaskScheduler.Default);
                _logger.LogDebug(
                    "OnRetryExhausted hook abandoned on shutdown for stream {Stream} partition {Partition}.",
                    _stream, _partitionIndex);
            }
            catch (Exception hex)
            {
                _logger.LogWarning(hex,
                    "OnRetryExhausted hook failed for stream {Stream} partition {Partition}; handoff outcome unchanged.",
                    _stream, _partitionIndex);
            }
        }
    }

    // Effectively-once dispatch (FR-6): Strict-unbounded compute retry (EO
    // validation forces FailureMode.Strict + null MaxAttempts, so this loop never
    // exhausts), then publish-forever with id = source offset (FR-6.1/FR-6.5),
    // then return WITHOUT storing (FR-10.14 — the publishing id is the resume
    // point). Runs under the dispatch gate, so one partition publishes under one
    // stable name sequentially (FR-6.3). A single finally owns the inflight
    // decrement + retry_blocked reset for every exit path below.
    private async Task DispatchEffectivelyOnceAsync(ulong offset, ConsumeContext consumeContext, AttachState attach)
    {
        try
        {
            ReadOnlyMemory<byte> result;
            var attempts = 0;
            while (true)
            {
                attempts++;
                try
                {
                    result = await _eo!.ResultHandler(consumeContext).ConfigureAwait(false);
                    break;
                }
                catch (OperationCanceledException) when (attach.LoopToken.IsCancellationRequested)
                {
                    return;
                }
                catch (Exception ex)
                {
                    if (RetryDelays.ShouldLogRetryWarning(attempts))
                        _logger.LogWarning(ex,
                            "Effectively-once handler failed for stream {Stream} partition {Partition} offset {Offset} (attempt {Attempt}); retrying in-handler.",
                            _stream, _partitionIndex, offset, attempts);
                    else
                        _logger.LogDebug(ex,
                            "Effectively-once handler failed for stream {Stream} partition {Partition} offset {Offset} (attempt {Attempt}); retrying in-handler.",
                            _stream, _partitionIndex, offset, attempts);
                    Interlocked.Exchange(ref _state.RetryBlocked, 1);
                    var delay = RetryDelays.ResolveDelay(_options.Retry, attempts, ex, _logger);
                    try
                    {
                        await Task.Delay(delay, attach.LoopToken).ConfigureAwait(false);
                    }
                    catch (OperationCanceledException) when (attach.LoopToken.IsCancellationRequested)
                    {
                        return;
                    }
                }
            }
            Interlocked.Exchange(ref _state.RetryBlocked, 0);
            // A throwing TargetRouting propagates to the dispatch safety net (attach
            // recycle + redeliver, never a skip).
            var routing = _eo!.TargetRouting(consumeContext);
            if (_eo.TargetIsSuperStream && routing is null)
            {
                FailAttach(attach, offset,
                    new StreamBusConfigurationException(
                        $"Effectively-once target routing resolved null for super target '{_eo.TargetStream}' (source offset {offset}); supply EffectivelyOnceOptions.TargetRouting or a routed source."),
                    "Effectively-once target routing missing");
                return;
            }
            var producerName = ResultNaming.ProducerName(_partitionStream, _eo.TargetStream);
            var publishAttempts = 0;
            while (true)
            {
                attach.LoopToken.ThrowIfCancellationRequested();
                publishAttempts++;
                try
                {
                    await _results!.PublishResultAsync(_eo.TargetStream, _eo.TargetIsSuperStream, result, offset, routing, producerName, attach.LoopToken).ConfigureAwait(false);
                    return;
                }
                catch (OperationCanceledException) when (attach.LoopToken.IsCancellationRequested)
                {
                    return;
                }
                catch (Exception ex)
                {
                    if (RetryDelays.ShouldLogRetryWarning(publishAttempts))
                        _logger.LogWarning(ex,
                            "Effectively-once publish failed for target {Target} source offset {Offset} (attempt {Attempt}); retrying without advancing.",
                            _eo.TargetStream, offset, publishAttempts);
                    else
                        _logger.LogDebug(ex,
                            "Effectively-once publish failed for target {Target} source offset {Offset} (attempt {Attempt}); retrying without advancing.",
                            _eo.TargetStream, offset, publishAttempts);
                    var delay = RetryDelays.ResolveDelay(_options.Retry, publishAttempts, ex, _logger);
                    try
                    {
                        await Task.Delay(delay, attach.LoopToken).ConfigureAwait(false);
                    }
                    catch (OperationCanceledException) when (attach.LoopToken.IsCancellationRequested)
                    {
                        return;
                    }
                }
            }
        }
        finally
        {
            Interlocked.Decrement(ref _state.Inflight);
            Interlocked.Exchange(ref _state.RetryBlocked, 0);
        }
    }

    private ConsumeContext BuildContext(ulong offset, Message message, CancellationToken ct)
    {
        var body = message.Data.Contents.ToArray();
        IDictionary<string, object>? appProps = message.ApplicationProperties;
        var metadata = MapMetadata(message.Properties, appProps, _serializer);
        metadata.ApplicationProperties.TryGetValue(RoutingKeyProperty, out var routingObj);
        return new ConsumeContext
        {
            Stream = _stream,
            Partition = _partitionIndex,
            Offset = offset,
            RoutingValue = routingObj?.ToString(),
            Body = body,
            Metadata = metadata,
            CancellationToken = ct,
        };
    }

    private static MessageMetadata MapMetadata(
        RabbitMQ.Stream.Client.AMQP.Properties? properties,
        IDictionary<string, object>? applicationProperties,
        IStreamSerializer serializer)
    {
        var app = applicationProperties is null
            ? new Dictionary<string, object?>()
            : applicationProperties.ToDictionary(kv => kv.Key, kv => (object?)kv.Value);
        app.TryGetValue("streambus.routing-key", out var routingObj);
        return new MessageMetadata
        {
            MessageId = properties?.MessageId?.ToString(),
            CorrelationId = properties?.CorrelationId?.ToString(),
            ApplicationProperties = app,
            Timestamp = properties?.CreationTime is { } ct
                ? new DateTimeOffset(DateTime.SpecifyKind(ct, DateTimeKind.Utc))
                : null,
            Serializer = serializer,
        };
    }

    private async Task StoreAndTrackAsync(IStreamConsumerHandle consumer, ulong offset, AttachState attach, CancellationToken ct)
    {
        // Admission precedes the gated commit: a rejected (non-forward) candidate only
        // counts the metric and never reaches the wire. A rejection racing a newer
        // admission is still correct — the guard is the single ordering authority.
        // (The handoff path admits inside its own gated commit instead, so teardown
        // can never strand an admitted-but-unstored offset.)
        if (!_storer.TryAdmit(offset))
        {
            return;
        }
        try
        {
            await CommitStoreAsync(consumer, offset, attach, ct).ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            // Catch-log-continue: the cadence counter is deliberately not reset so
            // the next success retries the store.
            _logger.LogWarning(
                ex,
                "Offset store failed for stream {Stream} partition {Partition} offset {Offset}; continuing without advancing.",
                _stream, _partitionIndex, offset);
        }
    }

    // Gated wire store + bookkeeping shared by the cadence path and the handoff
    // commit. A store failure propagates (the cadence caller catches; the handoff
    // caller lets it recycle the attach so the confirmed handoff is never silently
    // unadvanced). Gate-wait cancellation returns quietly, and a store is never
    // issued on a cancelled attach at all: teardown is in progress and the next
    // attach redelivers, so storing would risk advancing past unprocessed messages
    // (the G-5b teardown window). The next attach redelivers instead.
    private async Task CommitStoreAsync(IStreamConsumerHandle consumer, ulong offset, AttachState attach, CancellationToken ct)
    {
        try
        {
            await _storeGate.WaitAsync(ct).ConfigureAwait(false);
        }
        catch (OperationCanceledException)
        {
            return;
        }
        try
        {
            if (ct.IsCancellationRequested)
            {
                return;
            }
            await SendAndBookkeepAsync(consumer, offset, attach, ct).ConfigureAwait(false);
        }
        finally
        {
            _storeGate.Release();
        }
    }

    // Handoff commit: admission and the wire store happen in one gated step so a
    // teardown between them cannot poison the guard water (admitted but unstored
    // would make the redelivery reject its own handoff and skip without handing
    // off). A rejected candidate only counts the metric: another instance already
    // owns this offset's handoff. A duplicate error envelope from a stale instance
    // is benign (replay dedups on stable name + id). Residual micro-race (cancel
    // landing between the check and the send, or a local send throw after admission)
    // is accepted: the broker store path has no acknowledgement, so no local
    // arrangement can be fully atomic; at-least-once is preserved regardless.
    private async Task AdmitAndCommitStoreAsync(IStreamConsumerHandle consumer, ulong offset, AttachState attach, CancellationToken ct)
    {
        try
        {
            await _storeGate.WaitAsync(ct).ConfigureAwait(false);
        }
        catch (OperationCanceledException)
        {
            return;
        }
        try
        {
            if (ct.IsCancellationRequested)
            {
                return;
            }
            if (!_storer.TryAdmit(offset))
            {
                return;
            }
            await SendAndBookkeepAsync(consumer, offset, attach, ct).ConfigureAwait(false);
        }
        finally
        {
            _storeGate.Release();
        }
    }

    private async Task SendAndBookkeepAsync(IStreamConsumerHandle consumer, ulong offset, AttachState attach, CancellationToken ct)
    {
        _logger.LogDebug(
            "Storing offset {Offset} for stream {Stream} partition {Partition} reference {Reference}.",
            offset, _stream, _partitionIndex, _reference);
        await consumer.StoreOffset(offset).ConfigureAwait(false);
        _logger.LogDebug(
            "Stored offset {Offset} for stream {Stream} partition {Partition} reference {Reference}.",
            offset, _stream, _partitionIndex, _reference);
        _storer.Stored(offset);
        Interlocked.Exchange(ref _state.LastStored, (long)offset);
        if (Interlocked.Exchange(ref attach.RegressionChecked, 1) == 0)
        {
            await CheckRegressionAsync(ct).ConfigureAwait(false);
        }
    }

    private async Task CheckRegressionAsync(CancellationToken ct)
    {
        ulong? brokerHeld;
        try
        {
            brokerHeld = await _system.TryQueryOffsetAsync(_reference, _partitionStream, ct).ConfigureAwait(false);
        }
        catch (OperationCanceledException)
        {
            return;
        }
        catch (Exception ex)
        {
            _logger.LogDebug(
                ex,
                "Regression read-back query failed for stream {Stream} partition {Partition} reference {Reference}.",
                _stream, _partitionIndex, _reference);
            return;
        }
        var water = _guard.Water(_waterKey);
        if (brokerHeld is { } held && held < water)
        {
            _metrics.RecordRegressed(_vhost, _bus, _stream, _partitionIndex, _reference);
            _logger.LogWarning(
                "Stored offset regressed for stream {Stream} partition {Partition} reference {Reference}: broker holds {Broker} below local water {Water}.",
                _stream, _partitionIndex, _reference, held, water);
        }
    }

    private async Task TickAsync(IStreamConsumerHandle consumer, PeriodicTimer timer, AttachState attach, CancellationToken tickCt)
    {
        try
        {
            while (await timer.WaitForNextTickAsync(tickCt).ConfigureAwait(false))
            {
                if (!_storer.HasSuccess)
                {
                    continue;
                }
                var offset = _storer.LastSuccess;
                if (_storer.IsStored(offset))
                {
                    continue;
                }
                // Guard-checked, fire-and-forget; the fault-logging continuation observes
                // the child (logs, never rethrows) so no exception goes unobserved.
                _ = StoreAndTrackAsync(consumer, offset, attach, tickCt).ContinueWith(
                    t =>
                    {
                        if (t.Exception is { } ex)
                        {
                            _logger.LogWarning(
                                ex,
                                "Offset timer store faulted for stream {Stream} partition {Partition} reference {Reference}.",
                                _stream, _partitionIndex, _reference);
                        }
                    },
                    CancellationToken.None,
                    TaskContinuationOptions.OnlyOnFaulted | TaskContinuationOptions.ExecuteSynchronously,
                    TaskScheduler.Default);
            }
        }
        catch (OperationCanceledException)
        {
            // Expected on attach teardown (tick scope cancelled).
        }
        catch (ObjectDisposedException)
        {
            // Timer disposed during teardown; the tick loop is over.
        }
    }

    private Task<IOffsetType> OnPromotedAsync(string reference, string stream, bool promotedAsActive)
    {
        // The client's fallback on listener failure is OffsetTypeNext, which would skip
        // the backlog — so this listener never throws and never returns null. It ignores
        // its arguments except for logging and queries the broker fresh on every
        // promotion. No ambient cancellation token exists here; CancellationToken.None
        // avoids coupling promotion resolution to loop shutdown.
        return OnPromotedCoreAsync(reference, stream, promotedAsActive);
    }

    // Effectively-once resume (FR-6.7/FR-10.14): the source stores nothing, so the
    // resume point is the target's last publishing id (the shared stored+1 rule
    // turns it into lastId+1) on every attach — create, promote, reconnect alike.
    private Task<ulong?> QueryStoredAsync(CancellationToken ct)
        => _eo is null
            ? _system.TryQueryOffsetAsync(_reference, _partitionStream, ct)
            : _eo.ResumeQuery(_eo.TargetStream, _eo.TargetIsSuperStream, ResultNaming.ProducerName(_partitionStream, _eo.TargetStream), ct);

    private async Task<IOffsetType> OnPromotedCoreAsync(string reference, string stream, bool promotedAsActive)
    {
        try
        {
            _logger.LogInformation(
                "Consumer promoted for reference {Reference} on {PartitionStream} (active {Active}); re-resolving offset.",
                reference, stream, promotedAsActive);
            return await OffsetResolution.ResolveWithQueryAsync(
                _options.Offset,
                () => QueryStoredAsync(CancellationToken.None),
                _guard.Water(_waterKey),
                _reference,
                _partitionStream,
                _logger,
                branch => _metrics.RecordFallback(_vhost, _bus, _stream, _partitionIndex, _reference, branch)).ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            // Deviation from creation-time fail-fast: promotion has no caller to throw
            // to (this listener must never throw), so even ResumeOrFail degrades here
            // to a replay from the first offset instead of failing.
            _logger.LogWarning(
                ex,
                "Offset re-resolution on promotion failed for reference {Reference} on {PartitionStream}; falling back to first offset.",
                reference, stream);
            _metrics.RecordFallback(_vhost, _bus, _stream, _partitionIndex, _reference, PromotionFallbackBranch);
            return new OffsetTypeFirst();
        }
    }

    private void FailAttach(AttachState attach, ulong offset, Exception ex, string message)
    {
        _logger.LogError(
            ex,
            "{Message} for stream {Stream} partition {Partition} offset {Offset}; disposing consumer for redelivery.",
            message, _stream, _partitionIndex, offset);
        Interlocked.Exchange(ref attach.Drained, 1);
        attach.Failure.TrySetException(ex);
        _health?.Invoke(_partitionIndex, BusRegistrationState.FailedRetrying, message);
    }

    // Death callbacks have no offset to report; the attach loop redelivers from
    // the stored offset, so the exact dispatch position is not needed here.
    private void FailAttach(AttachState attach, Exception ex, string message)
    {
        _logger.LogError(
            ex,
            "{Message} for stream {Stream} partition {Partition}; disposing consumer for redelivery.",
            message, _stream, _partitionIndex);
        Interlocked.Exchange(ref attach.Drained, 1);
        attach.Failure.TrySetException(ex);
        _health?.Invoke(_partitionIndex, BusRegistrationState.FailedRetrying, message);
    }

    private async Task AwaitTrackedAsync(Task dispatch)
    {
        try
        {
            await dispatch.ConfigureAwait(false);
        }
        finally
        {
            lock (_life)
            {
                if (ReferenceEquals(_inflight, dispatch))
                {
                    _inflight = null;
                }
            }
        }
    }

    private async Task CloseQuietlyAsync(IStreamConsumerHandle? consumer)
    {
        if (consumer is null)
        {
            return;
        }
        if (ReferenceEquals(_consumerHandle.Current, consumer))
        {
            _consumerHandle.Clear();
        }
        try
        {
            await consumer.Close().ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            _logger.LogDebug(
                ex,
                "Error closing consumer for stream {Stream} partition {Partition} reference {Reference}.",
                _stream, _partitionIndex, _reference);
        }
    }

    private sealed class ConsumerHolder
    {
        private volatile IStreamConsumerHandle? _current;
        public IStreamConsumerHandle? Current => _current;
        public void Set(IStreamConsumerHandle consumer) => _current = consumer;
        public void Clear() => _current = null;
    }

    private sealed class AttachState
    {
        public readonly TaskCompletionSource<Exception> Failure = new(TaskCreationOptions.RunContinuationsAsynchronously);
        public int Drained;
        public int RegressionChecked;
        public CancellationToken LoopToken;
    }
}
