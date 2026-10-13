using System.Collections.Concurrent;
using System.Runtime.CompilerServices;
using RabbitMQ.Stream.Client;

namespace StreamBus;

public interface IStreamProducerHandle : IAsyncDisposable
{
    Task SendAsync(ReadOnlyMemory<byte> body, string? routingKey, CancellationToken ct);
    Task SendEnvelopeAsync(ReadOnlyMemory<byte> body, string? routingKey, IReadOnlyDictionary<string, object?> applicationProperties, string? messageId, string? correlationId, CancellationToken ct);
    Task PublishReplayAsync(ReadOnlyMemory<byte> body, ulong publishingId, CancellationToken ct);
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
        foreach (var publishingId in _pending.Keys)
        {
            if (_pending.TryRemove(publishingId, out var tcs))
                tcs.TrySetCanceled();
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

internal sealed class StreamProducerHandle : IStreamProducerHandle
{
    private readonly IProducer _producer;
    private readonly string _stream;
    private readonly ConfirmationTracker _confirmations;

    public StreamProducerHandle(IProducer producer, string stream, ConfirmationTracker confirmations)
    {
        ArgumentNullException.ThrowIfNull(producer);
        ArgumentNullException.ThrowIfNull(stream);
        ArgumentNullException.ThrowIfNull(confirmations);
        _producer = producer;
        _stream = stream;
        _confirmations = confirmations;
    }

    public async Task SendAsync(ReadOnlyMemory<byte> body, string? routingKey, CancellationToken ct)
    {
        ct.ThrowIfCancellationRequested();
        await _confirmations.PublishAsync(_producer, _stream, new Message(body.ToArray()), ct).ConfigureAwait(false);
    }

    public async Task SendEnvelopeAsync(ReadOnlyMemory<byte> body, string? routingKey, IReadOnlyDictionary<string, object?> applicationProperties, string? messageId, string? correlationId, CancellationToken ct)
    {
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

    public async Task PublishReplayAsync(ReadOnlyMemory<byte> body, ulong publishingId, CancellationToken ct)
    {
        ct.ThrowIfCancellationRequested();
        await _confirmations.PublishWithIdAsync(_producer, _stream, publishingId, new Message(body.ToArray()), ct).ConfigureAwait(false);
    }

    public ValueTask DisposeAsync()
    {
        _confirmations.CancelAll();
        _producer.Dispose();
        return ValueTask.CompletedTask;
    }
}

internal sealed class SuperStreamProducerHandle : IStreamProducerHandle
{
    private static readonly ConditionalWeakTable<Message, string> RoutingTable = new();
    private readonly ISuperStreamProducer _producer;
    private readonly string _stream;
    private readonly ConfirmationTracker _confirmations;

    public SuperStreamProducerHandle(ISuperStreamProducer producer, string stream, ConfirmationTracker confirmations)
    {
        ArgumentNullException.ThrowIfNull(producer);
        ArgumentNullException.ThrowIfNull(stream);
        ArgumentNullException.ThrowIfNull(confirmations);
        _producer = producer;
        _stream = stream;
        _confirmations = confirmations;
    }

    public static string RoutingFor(Message message)
        => RoutingTable.TryGetValue(message, out var routing)
            ? routing
            : throw new InvalidOperationException($"No routing value was stamped for the outgoing message to '{typeof(SuperStreamProducerHandle)}' (internal error).");

    public async Task SendAsync(ReadOnlyMemory<byte> body, string? routingKey, CancellationToken ct)
    {
        ct.ThrowIfCancellationRequested();
        var message = new Message(body.ToArray());
        RoutingTable.Add(message, routingKey!);
        try
        {
            await _confirmations.PublishAsync(_producer, _stream, message, ct).ConfigureAwait(false);
        }
        finally
        {
            RoutingTable.Remove(message);
        }
    }

    public Task SendEnvelopeAsync(ReadOnlyMemory<byte> body, string? routingKey, IReadOnlyDictionary<string, object?> applicationProperties, string? messageId, string? correlationId, CancellationToken ct)
        => throw new StreamBusConfigurationException("Error streams and replay targets are plain streams; this path is not supported.");

    public Task PublishReplayAsync(ReadOnlyMemory<byte> body, ulong publishingId, CancellationToken ct)
        => throw new StreamBusConfigurationException("Error streams and replay targets are plain streams; this path is not supported.");

    public ValueTask DisposeAsync()
    {
        _confirmations.CancelAll();
        _producer.Dispose();
        return ValueTask.CompletedTask;
    }
}
