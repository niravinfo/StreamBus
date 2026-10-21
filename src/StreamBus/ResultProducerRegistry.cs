using System.Collections.Concurrent;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using RabbitMQ.Stream.Client;

namespace StreamBus;

internal sealed class ResultProducerRegistry
{
    internal const int MaxProducerNamesPerStream = 255;

    private readonly StreamBusConnectionSupervisor _supervisor;
    private readonly string _vhost;
    private readonly ILogger<ResultProducerRegistry> _logger;
    private readonly ConcurrentDictionary<string, Task<IStreamProducerHandle>> _handles = new();

    public ResultProducerRegistry(StreamBusConnectionSupervisor supervisor, string vhost, ILogger<ResultProducerRegistry>? logger = null)
    {
        ArgumentNullException.ThrowIfNull(supervisor);
        ArgumentNullException.ThrowIfNull(vhost);
        _supervisor = supervisor;
        _vhost = vhost;
        _logger = logger ?? NullLogger<ResultProducerRegistry>.Instance;
    }

    public async Task PublishResultAsync(string targetStream, bool targetSuper, ReadOnlyMemory<byte> body, ulong sourceOffset, string? routingKey, string producerName, CancellationToken ct = default)
        => await PublishResultAsync(targetStream, targetSuper, body, sourceOffset, routingKey, producerName, ct, outputHeaders: null).ConfigureAwait(false);

    internal async Task PublishResultAsync(string targetStream, bool targetSuper, ReadOnlyMemory<byte> body, ulong sourceOffset, string? routingKey, string producerName, CancellationToken ct, IReadOnlyDictionary<string, object?>? outputHeaders)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(targetStream);
        ArgumentException.ThrowIfNullOrWhiteSpace(producerName);
        var readiness = await _supervisor.WaitUntilReadyAsync(ReadinessPolicy.FailFastPolicy, ct).ConfigureAwait(false);
        if (readiness == BusReadiness.Fatal)
            throw new StreamBusFatalException($"StreamBus for vhost '{_vhost}' is in Fatal state; result publish to '{targetStream}' refused.");
        if (readiness != BusReadiness.Ready)
            throw new StreamBusNotReadyException($"StreamBus for vhost '{_vhost}' is not ready ({readiness}); result publish to '{targetStream}' refused.");
        var (entry, handle) = await GetOrCreateCoreAsync(targetStream, targetSuper, producerName, ct).ConfigureAwait(false);
        var key = Key(targetStream, targetSuper, producerName);
        var options = ResultOptions(targetSuper, routingKey, outputHeaders);
        try
        {
            var publishingId = ResultNaming.PublishingId(sourceOffset);
            if (targetSuper)
            {
                var super = handle as IStreamSuperProducerHandle
                    ?? throw new StreamBusConfigurationException($"Result target '{targetStream}' is not a super stream (internal error).");
                await super.PublishWithIdAsync(publishingId, body, options with { IsSuperStream = true, RoutingKey = routingKey }, ct).ConfigureAwait(false);
            }
            else
            {
                var plain = handle as IStreamPlainProducerHandle
                    ?? throw new StreamBusConfigurationException($"Result target '{targetStream}' is not a plain stream (internal error).");
                await plain.PublishReplayAsync(body, publishingId, options, ct).ConfigureAwait(false);
            }
        }
        catch (OperationCanceledException)
        {
            throw;
        }
        catch (StreamBusPublishException)
        {
            // Negative confirm: the handle is healthy, the message was refused.
            // Keep the cached producer (no invalidate); the caller retries.
            throw;
        }
        catch (StreamBusConfigurationException)
        {
            throw;
        }
        catch (Exception ex) when (!IsCallerCancel(ex, ct))
        {
            await InvalidateAsync(key, entry, handle).ConfigureAwait(false);
            throw new StreamBusPublishTransientException($"Result publish to stream '{targetStream}' failed transiently; the cached stable producer was discarded (the same name is recreated and publishing id {sourceOffset} is safe to retry).", ex);
        }
    }

    private static PublishOptions ResultOptions(
        bool targetSuper,
        string? routingKey,
        IReadOnlyDictionary<string, object?>? outputHeaders)
    {
        var options = new PublishOptions
        {
            IsSuperStream = targetSuper,
            RoutingKey = targetSuper ? routingKey : null,
        };
        if (outputHeaders is { Count: > 0 })
            options = options with { Properties = new StreamMessageProperties { Headers = outputHeaders } };
        return options;
    }

    public async Task<IStreamProducerHandle> GetOrCreateAsync(string targetStream, bool targetSuper, string producerName, CancellationToken ct)
    {
        var (_, handle) = await GetOrCreateCoreAsync(targetStream, targetSuper, producerName, ct).ConfigureAwait(false);
        return handle;
    }

    private async Task<(Task<IStreamProducerHandle> Entry, IStreamProducerHandle Handle)> GetOrCreateCoreAsync(string targetStream, bool targetSuper, string producerName, CancellationToken ct)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(targetStream);
        ArgumentException.ThrowIfNullOrWhiteSpace(producerName);
        var key = Key(targetStream, targetSuper, producerName);
        // The factory must not capture the caller's ct (same rationale as the
        // unique-name registry). The 255-name cap is exact single-threaded and
        // best-effort under racing first-use; the broker enforces the real limit.
        var task = _handles.GetOrAdd(key, _ =>
        {
            if (NamesFor(targetStream, targetSuper) >= MaxProducerNamesPerStream)
                throw new StreamBusConfigurationException($"Effectively-once target '{targetStream}' already has {MaxProducerNamesPerStream} stable producer names (broker dedup limit); add a target stream instead of more source partitions.");
            return CreateAsync(targetStream, targetSuper, producerName, CancellationToken.None);
        });
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

    public async Task<ulong?> GetResumeOffsetAsync(string targetStream, bool targetSuper, string producerName, CancellationToken ct = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(targetStream);
        ArgumentException.ThrowIfNullOrWhiteSpace(producerName);
        // Returns the last publishing id, or null when nothing was ever published.
        // Every doubt resolves to null (replay from first): reprocessing under a
        // stable (name, id) is absorbed by broker dedup, while an overstated resume
        // would skip (FR-6.5). A missing handle (startup race) also replays; the
        // attach loop backs off and re-resolves.
        var handle = _supervisor.CurrentHandle;
        if (handle is null) return null;
        try
        {
            if (!targetSuper)
                return await QueryOneAsync(handle, producerName, targetStream, ct).ConfigureAwait(false);
            string[] partitions;
            try
            {
                partitions = await handle.QueryPartitionAsync(targetStream, ct).ConfigureAwait(false);
            }
            catch (OperationCanceledException) when (ct.IsCancellationRequested)
            {
                throw;
            }
            catch (Exception ex)
            {
                _logger.LogDebug(ex, "Effectively-once resume partition query failed for target '{Target}'; resolving to first offset.", targetStream);
                return null;
            }
            var known = new List<ulong?>(partitions.Length);
            foreach (var partition in partitions)
            {
                var last = await QueryOneAsync(handle, producerName, partition, ct).ConfigureAwait(false);
                if (last is not null) known.Add(last);
            }
            return ResultResume.ResumeOffset(known);
        }
        catch (OperationCanceledException) when (ct.IsCancellationRequested)
        {
            throw;
        }
        catch (Exception ex)
        {
            _logger.LogDebug(ex, "Effectively-once resume query failed for producer '{Producer}' on target '{Target}'; resolving to first offset.", producerName, targetStream);
            return null;
        }
    }

    private async Task<ulong?> QueryOneAsync(IStreamSystemHandle handle, string producerName, string stream, CancellationToken ct)
    {
        try
        {
            var last = await handle.QuerySequenceAsync(producerName, stream, ct).ConfigureAwait(false);
            // Ruling R1: the broker returns 0 both for never-published and for
            // "only id 0 published" (Task 1 probe). Map 0 to unknown: the resume
            // then resolves to First (replay, dedup-absorbed) instead of
            // skipping offset 0. Never moves the resume later — safe direction.
            if (last == 0) return null;
            return last;
        }
        catch (OperationCanceledException) when (ct.IsCancellationRequested)
        {
            throw;
        }
        catch (Exception ex)
        {
            _logger.LogDebug(ex, "Effectively-once resume query found nothing for producer '{Producer}' on '{Stream}'; treating as never-published.", producerName, stream);
            return null;
        }
    }

    private static bool IsCallerCancel(Exception ex, CancellationToken ct)
        => ex is OperationCanceledException && ct.IsCancellationRequested;

    private static string Key(string target, bool super, string name) => super ? $"result:super:{target}:{name}" : $"result:plain:{target}:{name}";

    private int NamesFor(string target, bool super)
    {
        var prefix = super ? $"result:super:{target}:" : $"result:plain:{target}:";
        var count = 0;
        foreach (var key in _handles.Keys)
            if (key.StartsWith(prefix, StringComparison.Ordinal)) count++;
        return count;
    }

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

    private async Task<IStreamProducerHandle> CreateAsync(string targetStream, bool targetSuper, string producerName, CancellationToken ct)
    {
        var handle = _supervisor.CurrentHandle
            ?? throw new StreamBusNotReadyException($"StreamBus for vhost '{_vhost}' is not ready; result publish to '{targetStream}' refused.");
        try
        {
            IStreamProducerHandle created = targetSuper
                ? await handle.CreateSuperStreamProducerWithReferenceAsync(targetStream, producerName, ct).ConfigureAwait(false)
                : await handle.CreatePlainProducerWithReferenceAsync(targetStream, producerName, ct).ConfigureAwait(false);
            return created;
        }
        catch (CreateProducerException ex) when (ex.ResponseCode == ResponseCode.StreamDoesNotExist)
        {
            throw new StreamBusStreamNotFoundException(targetStream, ex);
        }
        catch (CreateProducerException ex)
        {
            throw new StreamBusPublishException($"Could not create a result producer for stream '{targetStream}' (status {ex.ResponseCode}).", ex);
        }
    }
}
