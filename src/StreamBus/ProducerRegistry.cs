using System.Collections.Concurrent;
using RabbitMQ.Stream.Client;

namespace StreamBus;

internal sealed class StreamProducerRegistry
{
    private readonly StreamBusConnectionSupervisor _supervisor;
    private readonly string _vhost;
    private readonly ConcurrentDictionary<string, Task<IStreamProducerHandle>> _handles = new();

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
        var key = Key(stream, options.IsSuperStream);
        using var timeoutCts = new CancellationTokenSource(options.Timeout);
        using var linked = CancellationTokenSource.CreateLinkedTokenSource(ct, timeoutCts.Token);
        try
        {
            await handle.SendAsync(body, options.RoutingKey, linked.Token).ConfigureAwait(false);
        }
        catch (OperationCanceledException) when (timeoutCts.IsCancellationRequested && !ct.IsCancellationRequested)
        {
            await InvalidateAsync(key, entry, handle).ConfigureAwait(false);
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
            await InvalidateAsync(key, entry, handle).ConfigureAwait(false);
            throw new StreamBusPublishTransientException($"Publish to stream '{stream}' failed transiently; the cached producer was discarded.", ex);
        }
    }

    public async Task PublishEnvelopeAsync(string stream, ReadOnlyMemory<byte> body, string? routingKey, IReadOnlyDictionary<string, object?> applicationProperties, string? messageId, string? correlationId, CancellationToken ct = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(stream);
        ArgumentNullException.ThrowIfNull(applicationProperties);
        var readiness = await _supervisor.WaitUntilReadyAsync(ReadinessPolicy.FailFastPolicy, ct).ConfigureAwait(false);
        if (readiness == BusReadiness.Fatal)
            throw new StreamBusFatalException($"StreamBus for vhost '{_vhost}' is in Fatal state; publish to '{stream}' refused.");
        if (readiness != BusReadiness.Ready)
            throw new StreamBusNotReadyException($"StreamBus for vhost '{_vhost}' is not ready ({readiness}); publish to '{stream}' refused.");
        var (entry, handle) = await GetOrCreateAsync(stream, super: false, ct).ConfigureAwait(false);
        var key = Key(stream, super: false);
        try
        {
            await handle.SendEnvelopeAsync(body, routingKey, applicationProperties, messageId, correlationId, ct).ConfigureAwait(false);
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
            await InvalidateAsync(key, entry, handle).ConfigureAwait(false);
            throw new StreamBusPublishTransientException($"Publish to stream '{stream}' failed transiently; the cached producer was discarded.", ex);
        }
    }

    // A non-cancellation failure racing caller cancellation is wrapped as transient
    // (indistinguishable coincidence case; accepted behavior).
    private static bool IsCallerCancel(Exception ex, CancellationToken ct)
        => ex is OperationCanceledException && ct.IsCancellationRequested;

    private static string Key(string stream, bool super) => super ? $"super:{stream}" : $"plain:{stream}";

    private async Task InvalidateAsync(string key, Task<IStreamProducerHandle> expected, IStreamProducerHandle handleToDispose)
    {
        var removed = ((ICollection<KeyValuePair<string, Task<IStreamProducerHandle>>>)_handles)
            .Remove(new KeyValuePair<string, Task<IStreamProducerHandle>>(key, expected));
        if (!removed)
            return;
        try
        {
            await handleToDispose.DisposeAsync().ConfigureAwait(false);
        }
        catch
        {
        }
    }

    private async Task<(Task<IStreamProducerHandle> Entry, IStreamProducerHandle Handle)> GetOrCreateAsync(string stream, bool super, CancellationToken ct)
    {
        var key = Key(stream, super);
        // The factory must not capture the first caller's ct: a caller cancelling must not
        // abort producer creation for concurrent or future waiters. Client creates are
        // RpcTimeOut-bounded so nothing hangs; each waiter observes its own cancellation below.
        var task = _handles.GetOrAdd(key, _ => CreateAsync(stream, super, CancellationToken.None));
        try
        {
            var handle = await task.WaitAsync(ct).ConfigureAwait(false);
            return (task, handle);
        }
        catch
        {
            if (task.IsFaulted || task.IsCanceled)
                ((ICollection<KeyValuePair<string, Task<IStreamProducerHandle>>>)_handles).Remove(new KeyValuePair<string, Task<IStreamProducerHandle>>(key, task));
            throw;
        }
    }

    private async Task<IStreamProducerHandle> CreateAsync(string stream, bool super, CancellationToken ct)
    {
        var handle = _supervisor.CurrentHandle
            ?? throw new StreamBusNotReadyException($"StreamBus for vhost '{_vhost}' is not ready; publish to '{stream}' refused.");
        try
        {
            return super
                ? await handle.CreateSuperStreamProducerAsync(stream, ct).ConfigureAwait(false)
                : await handle.CreatePlainProducerAsync(stream, ct).ConfigureAwait(false);
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
