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
    /// <summary>Sends the body and awaits the broker confirmation; a non-Ok code throws <see cref="StreamBusPublishException"/>.</summary>
    Task SendAsync(ReadOnlyMemory<byte> body, PublishOptions options, CancellationToken cancellationToken);
}

/// <summary>Plain-stream producer surface, adding the routing-key-free envelope send to the base surface.</summary>
public interface IStreamPlainProducerHandle : IStreamProducerHandle
{
    /// <summary>Sends an envelope using only the supplied properties (no routing key); used for confirmed error-envelope-style sends.</summary>
    Task SendEnvelopeAsync(
        ReadOnlyMemory<byte> body,
        StreamMessageProperties properties,
        CancellationToken cancellationToken);
}

/// <summary>
/// Super-stream producer surface. Every send on a super stream must carry a
/// routing key (the broker routes each message to one partition), so the
/// envelope send — which has no routing key — is deliberately absent here
/// and lives only on <see cref="IStreamPlainProducerHandle"/>. Calling code
/// that tries it simply does not compile.
/// </summary>
public interface IStreamSuperProducerHandle : IStreamProducerHandle
{
}

/// <summary>
/// Per-handle routing table for the client's static <c>Routing</c> callback.
/// The callback receives only the outgoing <see cref="Message"/>, so the handle
/// stamps the routing value immediately before send and removes it afterwards.
/// </summary>
internal sealed class SuperStreamRouting
{
    private readonly ConcurrentDictionary<Message, string> _table = new();

    /// <summary>Routing callback for the client: returns the routing value stamped for <paramref name="message"/>.</summary>
    public string Route(Message message) => _table[message];

    /// <summary>Stamps the routing value to use for <paramref name="message"/> just before send.</summary>
    public void Stamp(Message message, string routing) => _table[message] = routing;

    /// <summary>Removes the stamp for <paramref name="message"/> after send.</summary>
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

    /// <summary>Allocates the next sequential publishing id for this handle.</summary>
    public ulong NextPublishingId() => (ulong)Interlocked.Increment(ref _nextPublishingId);

    /// <summary>Resolves the pending waiter for a broker confirmation.</summary>
    public void Complete(Confirmation confirmation)
    {
        if (_pending.TryRemove(confirmation.PublishingId, out var tcs))
            tcs.TrySetResult(confirmation.Code);
    }

    /// <summary>Sends with a fresh sequential publishing id and awaits its confirmation.</summary>
    public async Task PublishAsync(IProducer producer, string stream, Message message, CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
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
        using (cancellationToken.Register(static state => ((TaskCompletionSource<ResponseCode>)state!).TrySetCanceled(), confirmation))
        {
            ResponseCode code;
            try
            {
                code = await confirmation.Task.ConfigureAwait(false);
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
            {
                _pending.TryRemove(publishingId, out _);
                throw;
            }
            if (code != ResponseCode.Ok)
                throw new StreamBusPublishException($"Publish to stream '{stream}' was not confirmed (status {code}).");
        }
    }

    /// <summary>Cancels every pending waiter (cancellation for the awaits).</summary>
    public void CancelAll()
    {
        CancelAll(error: null);
    }

    // A connection drop never invokes ConfirmHandler for the lost publishing ids.
    // Resolve every pending waiter with the supplied error so awaiters stop
    // waiting and the registry invalidates the closed handle. Idempotent and
    // safe from any thread; must not throw.
    /// <summary>Fails every still-uncleared publishing id with a transient error; a dropped connection never delivers confirms.</summary>
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

    /// <summary>Wraps a client plain producer; a null <paramref name="logger"/> falls back to a null logger.</summary>
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

    /// <summary>Sends the body with the options' properties/routing key and awaits confirmation.</summary>
    public async Task SendAsync(ReadOnlyMemory<byte> body, PublishOptions options, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(options);
        cancellationToken.ThrowIfCancellationRequested();
        var message = new Message(body.ToArray());
        StreamMessageProperties.ApplyTo(message, options.Properties, options.RoutingKey);
        await _confirmations.PublishAsync(_producer, _stream, message, cancellationToken).ConfigureAwait(false);
    }

    /// <summary>Sends the body with the supplied properties only (no routing key) and awaits confirmation.</summary>
    public async Task SendEnvelopeAsync(ReadOnlyMemory<byte> body, StreamMessageProperties properties, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(properties);
        cancellationToken.ThrowIfCancellationRequested();
        var message = new Message(body.ToArray());
        StreamMessageProperties.ApplyTo(message, properties);
        await _confirmations.PublishAsync(_producer, _stream, message, cancellationToken).ConfigureAwait(false);
    }

    /// <summary>Cancels pending confirms and closes the producer off-thread, capped at 3s.</summary>
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

    /// <summary>Wraps a client super-stream producer; every send requires a routing key in options.</summary>
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

    /// <summary>Sends the body with the (required) options routing key and awaits confirmation.</summary>
    public Task SendAsync(ReadOnlyMemory<byte> body, PublishOptions options, CancellationToken cancellationToken)
        => SendMessageAsync(
            new Message(body.ToArray()),
            options,
            (message, token) => _confirmations.PublishAsync(_producer, _stream, message, token),
            cancellationToken);

    // SendEnvelopeAsync is intentionally absent: it carries no routing key,
    // so a super-stream envelope send cannot be routed. The interface split
    // (IStreamSuperProducerHandle no longer derives from
    // IStreamPlainProducerHandle) makes that a compile-time guarantee.

    /// <summary>Stamps the routing key on the message for the client callback, then removes it after send.</summary>
    private async Task SendMessageAsync(
        Message message,
        PublishOptions options,
        Func<Message, CancellationToken, Task> send,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(options);
        if (string.IsNullOrWhiteSpace(options.RoutingKey))
            throw new StreamBusConfigurationException($"Super-stream publish to '{_stream}' requires a routing key.");
        cancellationToken.ThrowIfCancellationRequested();
        StreamMessageProperties.ApplyTo(message, options.Properties, options.RoutingKey);
        _routing.Stamp(message, options.RoutingKey);
        try
        {
            await send(message, cancellationToken).ConfigureAwait(false);
        }
        finally
        {
            _routing.Remove(message);
        }
    }

    /// <summary>Cancels pending confirms and closes the producer off-thread, capped at 3s.</summary>
    public async ValueTask DisposeAsync()
    {
        _confirmations.CancelAll();
        await ProducerClose.DisposeOffThreadAsync(_producer, _stream, _logger).ConfigureAwait(false);
    }
}
