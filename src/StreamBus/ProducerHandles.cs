using System.Collections.Concurrent;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using RabbitMQ.Stream.Client;

namespace StreamBus;

/// <summary>
/// Common producer surface: every transport can send a message and await its
/// broker confirmation. The plain/super split below keeps transport-specific
/// members off the wrong surface instead of silently ignoring or throwing.
/// </summary>
public interface IStreamProducerHandle : IAsyncDisposable
{
    Task SendAsync(ReadOnlyMemory<byte> body, PublishOptions options, CancellationToken ct);
}

public interface IStreamPlainProducerHandle : IStreamProducerHandle
{
    Task SendEnvelopeAsync(
        ReadOnlyMemory<byte> body,
        PublishOptions options,
        IReadOnlyDictionary<string, object?> applicationProperties,
        string? messageId,
        string? correlationId,
        CancellationToken ct);

    Task PublishReplayAsync(ReadOnlyMemory<byte> body, ulong publishingId, PublishOptions options, CancellationToken ct);
}

public interface IStreamSuperProducerHandle : IStreamPlainProducerHandle
{
    Task PublishWithIdAsync(ulong publishingId, ReadOnlyMemory<byte> body, PublishOptions options, CancellationToken ct);
    Task PublishResultAsync(ReadOnlyMemory<byte> body, ulong publishingId, string? routingKey, CancellationToken ct);
}

/// <summary>
/// Per-handle routing table for the client's static <c>Routing</c> callback.
/// The callback receives only the outgoing <see cref="Message"/>, so the handle
/// stamps the routing value immediately before send and removes it afterwards.
/// </summary>
internal sealed class SuperStreamRouting
{
    private readonly ConcurrentDictionary<Message, string> _table = new();

    public string Route(Message message) => _table[message];

    public void Stamp(Message message, string routing) => _table[message] = routing;

    public void Remove(Message message) => _table.TryRemove(message, out _);
}

// Bridges the client's callback-based confirms (RawProducerConfig.ConfirmHandler)
// to per-message tasks. The 1.12.1 raw producers expose no Task-returning Send:
// every Send returns ValueTask and the confirm arrives later via ConfirmHandler,
// keyed by publishing id. Publishing ids are unique per producer instance
// (Interlocked counter; no dedup in this phase).
internal sealed class ConfirmationTracker
{
    private long _nextPublishingId;
    private readonly ConcurrentDictionary<ulong, TaskCompletionSource<ResponseCode>> _pending = new();

    public ulong NextPublishingId() => (ulong)Interlocked.Increment(ref _nextPublishingId);

    public void Complete(Confirmation confirmation)
    {
        if (_pending.TryRemove(confirmation.PublishingId, out var tcs))
            tcs.TrySetResult(confirmation.Code);
    }

    public async Task PublishAsync(IProducer producer, string stream, Message message, CancellationToken ct)
    {
        ct.ThrowIfCancellationRequested();
        var publishingId = NextPublishingId();
        var confirmation = new TaskCompletionSource<ResponseCode>(TaskCreationOptions.RunContinuationsAsynchronously);
        _pending[publishingId] = confirmation;
        try
        {
            await producer.Send(publishingId, message).ConfigureAwait(false);
        }
        catch
        {
            _pending.TryRemove(publishingId, out _);
            throw;
        }
        using (ct.Register(static state => ((TaskCompletionSource<ResponseCode>)state!).TrySetCanceled(), confirmation))
        {
            ResponseCode code;
            try
            {
                code = await confirmation.Task.ConfigureAwait(false);
            }
            catch (OperationCanceledException) when (ct.IsCancellationRequested)
            {
                _pending.TryRemove(publishingId, out _);
                throw;
            }
            if (code != ResponseCode.Ok)
                throw new StreamBusPublishException($"Publish to stream '{stream}' was not confirmed (status {code}).");
        }
    }

    public void CancelAll()
    {
        CancelAll(error: null);
    }

    // A connection drop never invokes ConfirmHandler for the lost publishing ids.
    // Resolve every pending waiter with the supplied error so awaiters stop
    // waiting and the registry invalidates the closed handle. Idempotent and
    // safe from any thread; must not throw.
    public void OnConnectionLost()
        => CancelAll(new StreamBusTransientException(
            "Producer connection closed before confirmation; publish outcome unknown."));

    private void CancelAll(Exception? error)
    {
        foreach (var publishingId in _pending.Keys)
        {
            if (!_pending.TryRemove(publishingId, out var tcs))
                continue;
            if (error is null)
                tcs.TrySetCanceled();
            else
                tcs.TrySetException(error);
        }
    }

    public async Task PublishWithIdAsync(IProducer producer, string stream, ulong publishingId, Message message, CancellationToken ct)
    {
        ct.ThrowIfCancellationRequested();
        var confirmation = new TaskCompletionSource<ResponseCode>(TaskCreationOptions.RunContinuationsAsynchronously);
        if (!_pending.TryAdd(publishingId, confirmation))
            throw new StreamBusPublishException($"Replay publishing id {publishingId} is already in flight for stream '{stream}' (internal error).");
        try
        {
            await producer.Send(publishingId, message).ConfigureAwait(false);
        }
        catch
        {
            _pending.TryRemove(publishingId, out _);
            throw;
        }
        using (ct.Register(static state => ((TaskCompletionSource<ResponseCode>)state!).TrySetCanceled(), confirmation))
        {
            ResponseCode code;
            try
            {
                code = await confirmation.Task.ConfigureAwait(false);
            }
            catch (OperationCanceledException) when (ct.IsCancellationRequested)
            {
                _pending.TryRemove(publishingId, out _);
                throw;
            }
            if (code != ResponseCode.Ok)
                throw new StreamBusPublishException($"Publish to stream '{stream}' was not confirmed (status {code}).");
        }
    }
}

internal static class ProducerClose
{
    private static readonly TimeSpan Cap = TimeSpan.FromSeconds(3);

    // RawProducer.Dispose() closes the client's mid-wait synchronously. Keep that
    // block off the caller's thread and cap it: an unresponsive broker must not
    // wedge dispatch or shutdown (the broker reaps the idle publisher anyway).
    public static async ValueTask DisposeOffThreadAsync(IProducer producer, string stream, ILogger logger)
    {
        try
        {
            await Task.Run(producer.Dispose).WaitAsync(Cap).ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            logger.LogWarning(ex, "Raw producer close for stream '{Stream}' did not complete within 3s; abandoning (the broker will reap the idle publisher).", stream);
        }
    }
}

internal sealed class StreamProducerHandle : IStreamPlainProducerHandle
{
    private readonly IProducer _producer;
    private readonly string _stream;
    private readonly ConfirmationTracker _confirmations;
    private readonly ILogger _logger;

    public StreamProducerHandle(IProducer producer, string stream, ConfirmationTracker confirmations, ILogger? logger = null)
    {
        ArgumentNullException.ThrowIfNull(producer);
        ArgumentNullException.ThrowIfNull(stream);
        ArgumentNullException.ThrowIfNull(confirmations);
        _producer = producer;
        _stream = stream;
        _confirmations = confirmations;
        _logger = logger ?? NullLogger.Instance;
    }

    public async Task SendAsync(ReadOnlyMemory<byte> body, PublishOptions options, CancellationToken ct)
    {
        ArgumentNullException.ThrowIfNull(options);
        ct.ThrowIfCancellationRequested();
        var message = new Message(body.ToArray());
        StreamMessageProperties.ApplyTo(message, options.Properties);
        await _confirmations.PublishAsync(_producer, _stream, message, ct).ConfigureAwait(false);
    }

    public async Task SendEnvelopeAsync(ReadOnlyMemory<byte> body, PublishOptions options, IReadOnlyDictionary<string, object?> applicationProperties, string? messageId, string? correlationId, CancellationToken ct)
    {
        ArgumentNullException.ThrowIfNull(options);
        ct.ThrowIfCancellationRequested();
        ArgumentNullException.ThrowIfNull(applicationProperties);
        var message = new Message(body.ToArray());
        if (messageId is not null || correlationId is not null)
        {
            message.Properties = new RabbitMQ.Stream.Client.AMQP.Properties { MessageId = messageId, CorrelationId = correlationId };
        }
        var app = new RabbitMQ.Stream.Client.AMQP.ApplicationProperties();
        foreach (var kv in applicationProperties)
        {
            app[kv.Key] = kv.Value;
        }
        message.ApplicationProperties = app;
        await _confirmations.PublishAsync(_producer, _stream, message, ct).ConfigureAwait(false);
    }

    public async Task PublishReplayAsync(ReadOnlyMemory<byte> body, ulong publishingId, PublishOptions options, CancellationToken ct)
    {
        ArgumentNullException.ThrowIfNull(options);
        ct.ThrowIfCancellationRequested();
        var message = new Message(body.ToArray());
        StreamMessageProperties.ApplyTo(message, options.Properties);
        await _confirmations.PublishWithIdAsync(_producer, _stream, publishingId, message, ct).ConfigureAwait(false);
    }

    public async ValueTask DisposeAsync()
    {
        _confirmations.CancelAll();
        await ProducerClose.DisposeOffThreadAsync(_producer, _stream, _logger).ConfigureAwait(false);
    }
}

internal sealed class SuperStreamProducerHandle : IStreamSuperProducerHandle
{
    private readonly ISuperStreamProducer _producer;
    private readonly string _stream;
    private readonly ConfirmationTracker _confirmations;
    private readonly SuperStreamRouting _routing;
    private readonly ILogger _logger;

    public SuperStreamProducerHandle(ISuperStreamProducer producer, string stream, ConfirmationTracker confirmations, SuperStreamRouting routing, ILogger? logger = null)
    {
        ArgumentNullException.ThrowIfNull(producer);
        ArgumentNullException.ThrowIfNull(stream);
        ArgumentNullException.ThrowIfNull(confirmations);
        ArgumentNullException.ThrowIfNull(routing);
        _producer = producer;
        _stream = stream;
        _confirmations = confirmations;
        _routing = routing;
        _logger = logger ?? NullLogger.Instance;
    }

    public Task SendAsync(ReadOnlyMemory<byte> body, PublishOptions options, CancellationToken ct)
        => SendMessageAsync(
            new Message(body.ToArray()),
            options,
            (message, token) => _confirmations.PublishAsync(_producer, _stream, message, token),
            ct);

    public Task SendEnvelopeAsync(
        ReadOnlyMemory<byte> body,
        PublishOptions options,
        IReadOnlyDictionary<string, object?> applicationProperties,
        string? messageId,
        string? correlationId,
        CancellationToken ct)
    {
        ArgumentNullException.ThrowIfNull(applicationProperties);
        var message = new Message(body.ToArray());
        if (messageId is not null || correlationId is not null)
        {
            message.Properties = new RabbitMQ.Stream.Client.AMQP.Properties { MessageId = messageId, CorrelationId = correlationId };
        }
        var app = new RabbitMQ.Stream.Client.AMQP.ApplicationProperties();
        foreach (var kv in applicationProperties)
            app[kv.Key] = kv.Value;
        message.ApplicationProperties = app;
        return SendMessageAsync(
            message,
            options,
            (outgoing, token) => _confirmations.PublishAsync(_producer, _stream, outgoing, token),
            ct);
    }

    public Task PublishReplayAsync(ReadOnlyMemory<byte> body, ulong publishingId, PublishOptions options, CancellationToken ct)
        => SendMessageAsync(
            new Message(body.ToArray()),
            options,
            (message, token) => _confirmations.PublishWithIdAsync(_producer, _stream, publishingId, message, token),
            ct);

    public Task PublishWithIdAsync(ulong publishingId, ReadOnlyMemory<byte> body, PublishOptions options, CancellationToken ct)
        => SendMessageAsync(
            new Message(body.ToArray()),
            options,
            (message, token) => _confirmations.PublishWithIdAsync(_producer, _stream, publishingId, message, token),
            ct);

    public Task PublishResultAsync(ReadOnlyMemory<byte> body, ulong publishingId, string? routingKey, CancellationToken ct)
        => PublishWithIdAsync(
            publishingId,
            body,
            new PublishOptions { IsSuperStream = true, RoutingKey = routingKey },
            ct);

    private async Task SendMessageAsync(
        Message message,
        PublishOptions options,
        Func<Message, CancellationToken, Task> send,
        CancellationToken ct)
    {
        ArgumentNullException.ThrowIfNull(options);
        if (string.IsNullOrWhiteSpace(options.RoutingKey))
            throw new StreamBusConfigurationException($"Super-stream publish to '{_stream}' requires a routing key.");
        ct.ThrowIfCancellationRequested();
        StreamMessageProperties.ApplyTo(message, options.Properties);
        _routing.Stamp(message, options.RoutingKey);
        try
        {
            await send(message, ct).ConfigureAwait(false);
        }
        finally
        {
            _routing.Remove(message);
        }
    }

    public async ValueTask DisposeAsync()
    {
        _confirmations.CancelAll();
        await ProducerClose.DisposeOffThreadAsync(_producer, _stream, _logger).ConfigureAwait(false);
    }
}
