using System.Collections.Concurrent;
using RabbitMQ.Stream.Client;

namespace StreamBus;

internal sealed class StreamProducerRegistry
{
    private readonly StreamBusConnectionSupervisor _supervisor;
    private readonly string _vhost;
    private readonly ConcurrentDictionary<string, ProducerSlot> _slots = new();

    private sealed class ProducerSlot
    {
        public readonly object Gate = new();
        public TaskCompletionSource<IStreamProducerHandle>? Pending;
        public TaskCompletionSource<IStreamProducerHandle>? Success;
        public bool Failed;
    }

    public StreamProducerRegistry(StreamBusConnectionSupervisor supervisor, string vhost)
    {
        _supervisor = supervisor;
        _vhost = vhost;
    }

    public async Task PublishAsync(string stream, ReadOnlyMemory<byte> body, PublishOptions options, CancellationToken ct = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(stream);
        ArgumentNullException.ThrowIfNull(options);
        if (options.Timeout <= TimeSpan.Zero)
            throw new StreamBusConfigurationException("PublishOptions.Timeout must be positive.");
        if (options.IsSuperStream && string.IsNullOrWhiteSpace(options.RoutingKey))
            throw new StreamBusConfigurationException($"PublishOptions.RoutingKey is required for super stream '{stream}'.");
        var readiness = await _supervisor.WaitUntilReadyAsync(options.ReadinessPolicy, ct).ConfigureAwait(false);
        if (readiness == BusReadiness.Fatal)
            throw new StreamBusFatalException($"StreamBus for vhost '{_vhost}' is in Fatal state; publish to '{stream}' refused.");
        if (readiness != BusReadiness.Ready)
            throw new StreamBusNotReadyException($"StreamBus for vhost '{_vhost}' is not ready ({readiness}); publish to '{stream}' refused.");
        var (entry, handle) = await GetOrCreateAsync(stream, options.IsSuperStream, ct).ConfigureAwait(false);
        using var timeoutCts = new CancellationTokenSource(options.Timeout);
        using var linked = CancellationTokenSource.CreateLinkedTokenSource(ct, timeoutCts.Token);
        try
        {
            await handle.SendAsync(body, options, linked.Token).ConfigureAwait(false);
        }
        catch (OperationCanceledException) when (timeoutCts.IsCancellationRequested && !ct.IsCancellationRequested)
        {
            await InvalidateAsync(entry, handle).ConfigureAwait(false);
            throw new StreamBusTimeoutException($"Publish to stream '{stream}' was not confirmed within {options.Timeout} (outcome unknown).");
        }
        catch (StreamBusPublishException)
        {
            throw;
        }
        catch (Exception ex) when (!IsCallerCancel(ex, ct) && StreamBusErrorClassification.IsFatal(ex))
        {
            throw;
        }
        catch (Exception ex) when (!IsCallerCancel(ex, ct))
        {
            await InvalidateAsync(entry, handle).ConfigureAwait(false);
            throw new StreamBusPublishTransientException($"Publish to stream '{stream}' failed transiently; the cached producer was discarded.", ex);
        }
    }

    public async Task PublishEnvelopeAsync(string stream, ReadOnlyMemory<byte> body, string? routingKey, IReadOnlyDictionary<string, object?> applicationProperties, string? messageId, string? correlationId, TimeSpan confirmTimeout, CancellationToken ct = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(stream);
        ArgumentNullException.ThrowIfNull(applicationProperties);
        if (confirmTimeout <= TimeSpan.Zero)
            throw new StreamBusConfigurationException("Error hand-off confirm timeout must be positive.");
        var readiness = await _supervisor.WaitUntilReadyAsync(ReadinessPolicy.FailFastPolicy, ct).ConfigureAwait(false);
        if (readiness == BusReadiness.Fatal)
            throw new StreamBusFatalException($"StreamBus for vhost '{_vhost}' is in Fatal state; publish to '{stream}' refused.");
        if (readiness != BusReadiness.Ready)
            throw new StreamBusNotReadyException($"StreamBus for vhost '{_vhost}' is not ready ({readiness}); publish to '{stream}' refused.");
        var (entry, handle) = await GetOrCreateAsync(stream, super: false, ct).ConfigureAwait(false);
        var plain = handle as IStreamPlainProducerHandle
            ?? throw new StreamBusConfigurationException($"Error hand-off target '{stream}' is not a plain stream (internal error).");
        using var attemptCts = CancellationTokenSource.CreateLinkedTokenSource(ct);
        attemptCts.CancelAfter(confirmTimeout);
        try
        {
            await plain.SendEnvelopeAsync(
                body,
                new PublishOptions { IsSuperStream = false, RoutingKey = routingKey },
                applicationProperties,
                messageId,
                correlationId,
                attemptCts.Token).ConfigureAwait(false);
        }
        catch (OperationCanceledException) when (!ct.IsCancellationRequested && attemptCts.IsCancellationRequested)
        {
            // The hand-off must never wedge the partition dispatch gate. The
            // handle's outcome is unknown, so discard it; the caller retries and
            // re-publishes the same envelope (at-least-once, never a skip).
            await InvalidateAsync(entry, handle).ConfigureAwait(false);
            throw new StreamBusTransientException(
                $"Error hand-off publish was not confirmed within {confirmTimeout}; outcome unknown. A retry will re-publish the envelope.");
        }
        catch (OperationCanceledException)
        {
            throw;
        }
        catch (StreamBusPublishException)
        {
            throw;
        }
        catch (Exception ex) when (!IsCallerCancel(ex, ct))
        {
            await InvalidateAsync(entry, handle).ConfigureAwait(false);
            throw new StreamBusPublishTransientException($"Publish to stream '{stream}' failed transiently; the cached producer was discarded.", ex);
        }
    }

    // A non-cancellation failure racing caller cancellation is wrapped as transient
    // (indistinguishable coincidence case; accepted behavior).
    private static bool IsCallerCancel(Exception ex, CancellationToken ct)
        => ex is OperationCanceledException && ct.IsCancellationRequested;

    private static string Key(string stream, bool super) => super ? $"super:{stream}" : $"plain:{stream}";

    private async Task InvalidateAsync(ProducerSlot expected, IStreamProducerHandle handleToDispose)
    {
        bool shouldDispose;
        lock (expected.Gate)
        {
            shouldDispose = expected.Success is { } success
                && success.Task.IsCompletedSuccessfully
                && ReferenceEquals(success.Task.Result, handleToDispose);
            if (shouldDispose)
                expected.Success = null;
        }
        if (!shouldDispose)
            return;
        try
        {
            await handleToDispose.DisposeAsync().ConfigureAwait(false);
        }
        catch
        {
        }
    }

    private async Task<(ProducerSlot Entry, IStreamProducerHandle Handle)> GetOrCreateAsync(string stream, bool super, CancellationToken ct)
    {
        var key = Key(stream, super);
        // Slot creation has no side effects, so the harmless duplicate a
        // ConcurrentDictionary.GetOrAdd factory race may produce is discarded;
        // exactly one slot survives and owns the key's single producer.
        var slot = _slots.GetOrAdd(key, static _ => new ProducerSlot());
        // The shared creation is not tied to this caller's ct: a caller cancelling
        // must not abort producer creation for waiters that follow.
        var handle = await CreateOrWaitAsync(slot, stream, super).WaitAsync(ct).ConfigureAwait(false);
        return (slot, handle);
    }

    private async Task<IStreamProducerHandle> CreateOrWaitAsync(ProducerSlot slot, string stream, bool super)
    {
        TaskCompletionSource<IStreamProducerHandle>? completed = null;
        TaskCompletionSource<IStreamProducerHandle>? pending = null;
        TaskCompletionSource<IStreamProducerHandle>? owner = null;
        lock (slot.Gate)
        {
            if (slot.Success is { } success)
                completed = success;
            else if (slot.Pending is null)
            {
                owner = new TaskCompletionSource<IStreamProducerHandle>(TaskCreationOptions.RunContinuationsAsynchronously);
                slot.Pending = owner;
                slot.Failed = false;
            }
            else
                pending = slot.Pending;
        }
        if (completed is not null)
            return await completed.Task.ConfigureAwait(false);
        if (pending is not null)
            return await pending.Task.ConfigureAwait(false);

        try
        {
            var handle = await CreateAsync(stream, super, CancellationToken.None).ConfigureAwait(false);
            lock (slot.Gate)
            {
                slot.Success = owner!;
                slot.Pending = null;
                slot.Failed = false;
                owner!.TrySetResult(handle);
            }
            return handle;
        }
        catch (Exception ex)
        {
            lock (slot.Gate)
            {
                slot.Pending = null;
                slot.Failed = true;
                owner!.TrySetException(ex);
            }
            throw;
        }
    }

    private async Task<IStreamProducerHandle> CreateAsync(string stream, bool super, CancellationToken ct)
    {
        var handle = _supervisor.CurrentHandle
            ?? throw new StreamBusNotReadyException($"StreamBus for vhost '{_vhost}' is not ready; publish to '{stream}' refused.");
        try
        {
            IStreamProducerHandle created = super
                ? await handle.CreateSuperStreamProducerAsync(stream, ct).ConfigureAwait(false)
                : await handle.CreatePlainProducerAsync(stream, ct).ConfigureAwait(false);
            return created;
        }
        catch (CreateProducerException ex) when (ex.ResponseCode == ResponseCode.StreamDoesNotExist)
        {
            throw new StreamBusStreamNotFoundException(stream, ex);
        }
        catch (CreateProducerException ex)
        {
            throw new StreamBusPublishException($"Could not create a producer for stream '{stream}' (status {ex.ResponseCode}).", ex);
        }
    }
}
