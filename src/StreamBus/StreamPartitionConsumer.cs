using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using RabbitMQ.Stream.Client;
using System.Buffers;

namespace StreamBus;

internal sealed class OffsetStorer
{
    private readonly int _every;
    private readonly MonotonicOffsetGuard _guard;
    private readonly string _waterKey;
    private readonly Action<ulong> _onRejected;
    private int _sinceLastStore;
    private ulong _lastSuccess;
    private bool _hasSuccess;

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

    public void Stored(ulong offset, int keepRemainder)
    {
        _sinceLastStore = keepRemainder;
        _lastSuccess = offset;
    }

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
    private readonly ILogger<StreamPartitionConsumer> _logger;
    private readonly string _waterKey;
    private readonly OffsetStorer _storer;

    private readonly SemaphoreSlim _dispatchGate = new(1, 1);
    private readonly SemaphoreSlim _storeGate = new(1, 1);
    private readonly object _life = new();
    private readonly CancellationTokenSource _loopCts = new();
    private readonly TaskCompletionSource _firstAttach = new(TaskCreationOptions.RunContinuationsAsynchronously);
    private RawConsumer? _current;
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
        ILogger<StreamPartitionConsumer>? logger = null)
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
        _logger = logger ?? NullLogger<StreamPartitionConsumer>.Instance;
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

    // The newest successfully handled offset (cadence may not have stored it yet;
    // PartitionState.LastStored tracks the stored one).
    internal ulong LastSuccess => _storer.LastSuccess;

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
            RawConsumer? consumer = null;
            Exception? attachError = null;
            try
            {
                var resolved = await OffsetResolution.ResolveWithQueryAsync(
                    _options.Offset,
                    () => _system.TryQueryOffsetAsync(_reference, _partitionStream, loopCt),
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
                        var dispatch = DispatchAsync(c, context, message, attach);
                        lock (_life)
                        {
                            _inflight = dispatch;
                        }
                        return AwaitTrackedAsync(dispatch);
                    },
                };

                consumer = await _system.CreateRawConsumerAsync(config, loopCt).ConfigureAwait(false);
                _firstAttach.TrySetResult();
                lock (_life)
                {
                    _current = consumer;
                }
                failures = 0; // A successful attach resets the backoff streak.
                _logger.LogInformation(
                    "Consume attach ready for stream {Stream} partition {Partition} reference {Reference}.",
                    _stream, _partitionIndex, _reference);

                PeriodicTimer? timer = null;
                Task? timerTask = null;
                CancellationTokenSource? tickCts = null;
                if (_options.OffsetStore.MaxInterval is { } interval && interval > TimeSpan.Zero)
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
                    await attach.Failure.Task.WaitAsync(loopCt).ConfigureAwait(false);
                    throw new InvalidOperationException("Attach failure task completed successfully, which is impossible.");
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
        RawConsumer? consumer;
        PeriodicTimer? timer;
        lock (_life)
        {
            consumer = _current;
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
            _current = null;
            _timer = null;
            _inflight = null;
        }
    }

    private async Task DispatchAsync(RawConsumer consumer, MessageContext context, Message message, AttachState attach)
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
                await _handler(consumeContext).ConfigureAwait(false);
            }
            catch (OperationCanceledException) when (attach.LoopToken.IsCancellationRequested)
            {
                Interlocked.Decrement(ref _state.Inflight);
                return;
            }
            catch (Exception ex)
            {
                Interlocked.Decrement(ref _state.Inflight);
                FailAttach(attach, offset, ex, "Consume handler failed");
                return;
            }
            Interlocked.Decrement(ref _state.Inflight);

            if (_storer.ShouldStoreOnSuccess(offset))
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

    private ConsumeContext BuildContext(ulong offset, Message message, CancellationToken ct)
    {
        var body = message.Data.Contents.ToArray();
        IDictionary<string, object>? appProps = message.ApplicationProperties;
        var metadata = MapMetadata(message.Properties, appProps);
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

    private static MessageMetadata MapMetadata(RabbitMQ.Stream.Client.AMQP.Properties? properties, IDictionary<string, object>? applicationProperties)
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
        };
    }

    private async Task StoreAndTrackAsync(RawConsumer consumer, ulong offset, AttachState attach, CancellationToken ct)
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
            // Admission happens under the store gate so wire order matches admission
            // order; a rejected (non-forward) candidate only counts the metric.
            if (!_storer.TryAdmit(offset))
            {
                return;
            }
            try
            {
                _logger.LogDebug(
                    "Storing offset {Offset} for stream {Stream} partition {Partition} reference {Reference}.",
                    offset, _stream, _partitionIndex, _reference);
                await consumer.StoreOffset(offset).ConfigureAwait(false);
                _logger.LogDebug(
                    "Stored offset {Offset} for stream {Stream} partition {Partition} reference {Reference}.",
                    offset, _stream, _partitionIndex, _reference);
            }
            catch (Exception ex)
            {
                // Catch-log-continue: the cadence counter is deliberately not reset so
                // the next success retries the store.
                _logger.LogWarning(
                    ex,
                    "Offset store failed for stream {Stream} partition {Partition} offset {Offset}; continuing without advancing.",
                    _stream, _partitionIndex, offset);
                return;
            }
            _storer.Stored(offset, 0);
            Interlocked.Exchange(ref _state.LastStored, (long)offset);
            if (Interlocked.Exchange(ref attach.RegressionChecked, 1) == 0)
            {
                await CheckRegressionAsync(ct).ConfigureAwait(false);
            }
        }
        finally
        {
            _storeGate.Release();
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

    private async Task TickAsync(RawConsumer consumer, PeriodicTimer timer, AttachState attach, CancellationToken tickCt)
    {
        try
        {
            while (await timer.WaitForNextTickAsync(tickCt).ConfigureAwait(false))
            {
                if (!_storer.HasSuccess)
                {
                    continue;
                }
                // Guard-checked, fire-and-forget with catch-log inside StoreAndTrackAsync.
                _ = StoreAndTrackAsync(consumer, _storer.LastSuccess, attach, tickCt);
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

    private async Task<IOffsetType> OnPromotedCoreAsync(string reference, string stream, bool promotedAsActive)
    {
        try
        {
            _logger.LogInformation(
                "Consumer promoted for reference {Reference} on {PartitionStream} (active {Active}); re-resolving offset.",
                reference, stream, promotedAsActive);
            return await OffsetResolution.ResolveWithQueryAsync(
                _options.Offset,
                () => _system.TryQueryOffsetAsync(_reference, _partitionStream, CancellationToken.None),
                _guard.Water(_waterKey),
                _reference,
                _partitionStream,
                _logger,
                branch => _metrics.RecordFallback(_vhost, _bus, _stream, _partitionIndex, _reference, branch)).ConfigureAwait(false);
        }
        catch (Exception ex)
        {
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

    private async Task CloseQuietlyAsync(RawConsumer? consumer)
    {
        if (consumer is null)
        {
            return;
        }
        lock (_life)
        {
            if (ReferenceEquals(_current, consumer))
            {
                _current = null;
            }
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

    private sealed class AttachState
    {
        public readonly TaskCompletionSource<Exception> Failure = new(TaskCreationOptions.RunContinuationsAsynchronously);
        public int Drained;
        public int RegressionChecked;
        public CancellationToken LoopToken;
    }
}
