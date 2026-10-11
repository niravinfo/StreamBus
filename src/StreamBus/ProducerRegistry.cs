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
        var handle = await GetOrCreateAsync(stream, options.IsSuperStream, ct).ConfigureAwait(false);
        using var timeoutCts = new CancellationTokenSource(options.Timeout);
        using var linked = CancellationTokenSource.CreateLinkedTokenSource(ct, timeoutCts.Token);
        try
        {
            await handle.SendAsync(body, options.RoutingKey, linked.Token).ConfigureAwait(false);
        }
        catch (OperationCanceledException) when (timeoutCts.IsCancellationRequested && !ct.IsCancellationRequested)
        {
            Invalidate(stream, options.IsSuperStream);
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
            Invalidate(stream, options.IsSuperStream);
            throw new StreamBusPublishTransientException($"Publish to stream '{stream}' failed transiently; the cached producer was discarded.", ex);
        }
    }

    private static bool IsCallerCancel(Exception ex, CancellationToken ct)
        => ex is OperationCanceledException && ct.IsCancellationRequested;

    private static string Key(string stream, bool super) => super ? $"super:{stream}" : $"plain:{stream}";

    private void Invalidate(string stream, bool super)
        => _handles.TryRemove(Key(stream, super), out _);

    private async Task<IStreamProducerHandle> GetOrCreateAsync(string stream, bool super, CancellationToken ct)
    {
        var key = Key(stream, super);
        var task = _handles.GetOrAdd(key, _ => CreateAsync(stream, super, ct));
        try
        {
            return await task.ConfigureAwait(false);
        }
        catch
        {
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
