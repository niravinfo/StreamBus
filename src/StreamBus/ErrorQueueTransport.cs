using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using RabbitMQ.Client.Exceptions;

namespace StreamBus;

public interface IErrorQueueConnectionFactory
{
    Task<IConnection> ConnectAsync(StreamBusOptions options, CancellationToken ct);
}

internal sealed class AmqpConnectionFactory : IErrorQueueConnectionFactory
{
    public async Task<IConnection> ConnectAsync(StreamBusOptions options, CancellationToken ct)
    {
        ArgumentNullException.ThrowIfNull(options);
        var factory = new ConnectionFactory
        {
            HostName = options.Host,
            Port = options.AmqpPort,
            UserName = options.UserName,
            Password = options.Password,
            VirtualHost = options.Vhost,
            ClientProvidedName = "streambus-error-queues",
            RequestedHeartbeat = options.Heartbeat,
        };
        try
        {
            return await factory.CreateConnectionAsync(ct).ConfigureAwait(false);
        }
        catch (AuthenticationFailureException ex)
        {
            throw new StreamBusFatalException(
                $"AMQP authentication failed for vhost '{options.Vhost}' (user '{options.UserName}'). Create a dedicated non-guest user and reconfigure UserName/Password.", ex);
        }
    }
}

public sealed class ErrorQueueTransport : IAsyncDisposable
{
    private static readonly TimeSpan ConfirmTimeout = TimeSpan.FromSeconds(10);
    private static readonly TimeSpan DrainIdle = TimeSpan.FromSeconds(5);
    private static readonly TimeSpan DrainPoll = TimeSpan.FromMilliseconds(200);

    private readonly StreamBusOptions _options;
    private readonly IErrorQueueConnectionFactory _connector;
    private readonly ILogger<ErrorQueueTransport> _logger;
    private readonly SemaphoreSlim _gate = new(1, 1);
    private IConnection? _connection;
    private bool _fatal;
    private Exception? _fatalError;

    public ErrorQueueTransport(
        StreamBusOptions options,
        IErrorQueueConnectionFactory? connector = null,
        ILogger<ErrorQueueTransport>? logger = null)
    {
        ArgumentNullException.ThrowIfNull(options);
        _options = options;
        _connector = connector ?? new AmqpConnectionFactory();
        _logger = logger ?? NullLogger<ErrorQueueTransport>.Instance;
    }

    internal static Dictionary<string, object?> QueueArguments(DeadLetterType type) => new()
    {
        ["x-queue-type"] = type == DeadLetterType.QuorumQueue ? "quorum" : "classic",
    };

    internal static IReadOnlyList<DeadLetterOptions> CollectQueueDeclarations(IEnumerable<TypedConsumerRegistration> registrations)
    {
        ArgumentNullException.ThrowIfNull(registrations);
        return registrations
            .Where(r => r.Options.DeadLetter is not null && r.Options.ExhaustionCallback is null)
            .Select(r => r.Options.DeadLetter!)
            .Distinct()
            .ToList();
    }

    public async Task DeclareAsync(IEnumerable<DeadLetterOptions> queues, CancellationToken ct = default)
    {
        ArgumentNullException.ThrowIfNull(queues);
        var distinct = queues.Distinct().ToList();
        if (distinct.Count == 0) return;
        var connection = await GetConnectionAsync(ct).ConfigureAwait(false);
        foreach (var queue in distinct)
        {
            ArgumentException.ThrowIfNullOrWhiteSpace(queue.Name);
            await using var channel = await connection.CreateChannelAsync(new CreateChannelOptions(false, false, null, null), ct).ConfigureAwait(false);
            try
            {
                await channel.QueueDeclareAsync(
                    queue: queue.Name,
                    durable: true,
                    exclusive: false,
                    autoDelete: false,
                    arguments: QueueArguments(queue.Type),
                    passive: false,
                    noWait: false,
                    cancellationToken: ct).ConfigureAwait(false);
            }
            catch (OperationInterruptedException ex) when (ex.ShutdownReason is { ReplyCode: 406 })
            {
                throw new StreamConfigurationConflictException(queue.Name,
                    $"queue '{queue.Name}' ({queue.Type}, durable)",
                    "an existing queue with conflicting type or arguments");
            }
        }
    }

    public async Task PublishAsync(DeadLetterOptions queue, ErrorEnvelope envelope, CancellationToken ct = default)
    {
        ArgumentNullException.ThrowIfNull(queue);
        ArgumentNullException.ThrowIfNull(envelope);
        ArgumentException.ThrowIfNullOrWhiteSpace(queue.Name);
        var connection = await GetConnectionAsync(ct).ConfigureAwait(false);
        try
        {
            var confirmOptions = new CreateChannelOptions(true, true, null, null);
            await using var channel = await connection.CreateChannelAsync(confirmOptions, ct).ConfigureAwait(false);
            await PublishConfirmedAsync(channel, queue.Name, envelope, ct).ConfigureAwait(false);
        }
        catch (BrokerUnreachableException)
        {
            InvalidateConnection();
            throw;
        }
        catch (AlreadyClosedException)
        {
            InvalidateConnection();
            throw;
        }
    }

    private static async Task PublishConfirmedAsync(IChannel channel, string queue, ErrorEnvelope envelope, CancellationToken ct)
    {
        var confirmed = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
        void Complete(Exception? error)
        {
            if (error is null) confirmed.TrySetResult(true);
            else confirmed.TrySetException(error);
        }
        AsyncEventHandler<BasicAckEventArgs> onAck = (_, _) => { Complete(null); return Task.CompletedTask; };
        AsyncEventHandler<BasicNackEventArgs> onNack = (_, e) => { Complete(new StreamBusPublishException($"Error queue '{queue}' negatively confirmed the handoff (delivery tag {e.DeliveryTag}).")); return Task.CompletedTask; };
        AsyncEventHandler<BasicReturnEventArgs> onReturn = (_, e) => { Complete(new StreamBusPublishException($"Error queue '{queue}' returned the handoff as unroutable (reply {e.ReplyCode} {e.ReplyText}).")); return Task.CompletedTask; };
        AsyncEventHandler<ShutdownEventArgs> onShutdown = (_, e) => { Complete(new StreamBusTimeoutException($"Publish to error queue '{queue}' ended with an unknown confirm outcome (channel closed: {e.ReplyText}).")); return Task.CompletedTask; };
        channel.BasicAcksAsync += onAck;
        channel.BasicNacksAsync += onNack;
        channel.BasicReturnAsync += onReturn;
        channel.ChannelShutdownAsync += onShutdown;
        try
        {
            var properties = new BasicProperties
            {
                Persistent = true,
                DeliveryMode = DeliveryModes.Persistent,
                MessageId = envelope.MessageId,
                CorrelationId = envelope.CorrelationId,
                Headers = envelope.ToHeaders(),
            };
            await channel.BasicPublishAsync(
                exchange: string.Empty,
                routingKey: queue,
                mandatory: true,
                basicProperties: properties,
                body: envelope.Body,
                cancellationToken: ct).ConfigureAwait(false);
            using var timeout = new CancellationTokenSource(ConfirmTimeout);
            using var linked = CancellationTokenSource.CreateLinkedTokenSource(ct, timeout.Token);
            try
            {
                await confirmed.Task.WaitAsync(linked.Token).ConfigureAwait(false);
            }
            catch (OperationCanceledException) when (!ct.IsCancellationRequested)
            {
                throw new StreamBusTimeoutException($"Publish to error queue '{queue}' was not confirmed within {ConfirmTimeout} (outcome unknown).");
            }
        }
        finally
        {
            channel.BasicAcksAsync -= onAck;
            channel.BasicNacksAsync -= onNack;
            channel.BasicReturnAsync -= onReturn;
            channel.ChannelShutdownAsync -= onShutdown;
        }
    }

    public async Task<int> DrainAsync(string errorQueue, Func<ErrorEnvelope, CancellationToken, Task> republish, CancellationToken ct = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(errorQueue);
        ArgumentNullException.ThrowIfNull(republish);
        var connection = await GetConnectionAsync(ct).ConfigureAwait(false);
        await using var channel = await connection.CreateChannelAsync(new CreateChannelOptions(false, false, null, null), ct).ConfigureAwait(false);
        try
        {
            await channel.QueueDeclarePassiveAsync(errorQueue, ct).ConfigureAwait(false);
        }
        catch (OperationInterruptedException ex) when (ex.ShutdownReason is { ReplyCode: 404 })
        {
            throw new StreamBusStreamNotFoundException(errorQueue, ex);
        }
        var requeued = 0;
        var skipped = 0;
        var lastTicks = DateTimeOffset.UtcNow.UtcTicks;
        while (true)
        {
            ct.ThrowIfCancellationRequested();
            var delivery = await channel.BasicGetAsync(errorQueue, autoAck: false, cancellationToken: ct).ConfigureAwait(false);
            if (delivery is null)
            {
                var since = DateTimeOffset.UtcNow - new DateTimeOffset(Interlocked.Read(ref lastTicks), TimeSpan.Zero);
                if (since >= DrainIdle) break;
                await Task.Delay(DrainPoll, ct).ConfigureAwait(false);
                continue;
            }
            Interlocked.Exchange(ref lastTicks, DateTimeOffset.UtcNow.UtcTicks);
            var body = delivery.Body.ToArray();
            var headers = delivery.BasicProperties?.Headers is { } raw
                ? new Dictionary<string, object?>(raw, StringComparer.Ordinal)
                : new Dictionary<string, object?>(StringComparer.Ordinal);
            if (!ErrorEnvelope.TryParseHeaders(errorQueue, body, headers, out var envelope) || envelope is null)
            {
                _logger.LogWarning("Requeue skipping malformed message on error queue '{Queue}' (delivery tag {Tag}).",
                    errorQueue, delivery.DeliveryTag);
                await channel.BasicAckAsync(delivery.DeliveryTag, multiple: false, cancellationToken: ct).ConfigureAwait(false);
                skipped++;
                continue;
            }
            await republish(envelope, ct).ConfigureAwait(false);
            await channel.BasicAckAsync(delivery.DeliveryTag, multiple: false, cancellationToken: ct).ConfigureAwait(false);
            requeued++;
        }
        if (skipped > 0)
            _logger.LogWarning("Requeue from error queue '{Queue}' skipped {Skipped} malformed entries.", errorQueue, skipped);
        return requeued;
    }

    private async Task<IConnection> GetConnectionAsync(CancellationToken ct)
    {
        var current = Volatile.Read(ref _connection);
        if (current is not null && current.IsOpen) return current;
        await _gate.WaitAsync(ct).ConfigureAwait(false);
        try
        {
            current = Volatile.Read(ref _connection);
            if (current is not null && current.IsOpen) return current;
            if (_fatal)
                throw new StreamBusFatalException("AMQP error-queue connection is in Fatal state; see the earlier authentication failure.", _fatalError);
            IConnection created;
            try
            {
                created = await _connector.ConnectAsync(_options, ct).ConfigureAwait(false);
            }
            catch (StreamBusFatalException ex)
            {
                _fatal = true;
                _fatalError = ex;
                throw;
            }
            Volatile.Write(ref _connection, created);
            return created;
        }
        finally
        {
            _gate.Release();
        }
    }

    private void InvalidateConnection()
    {
        var current = Interlocked.Exchange(ref _connection, null);
        if (current is null) return;
        try { current.Dispose(); }
        catch { }
    }

    public async ValueTask DisposeAsync()
    {
        var current = Interlocked.Exchange(ref _connection, null);
        if (current is null) return;
        try { await current.CloseAsync(200, "disposing", TimeSpan.FromSeconds(5), abort: false, cancellationToken: CancellationToken.None).ConfigureAwait(false); }
        catch { }
        try { current.Dispose(); }
        catch { }
        _gate.Dispose();
    }
}
