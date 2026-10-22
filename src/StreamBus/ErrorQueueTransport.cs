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
                $"AMQP authentication failed for vhost '{options.Vhost}' (user '{options.UserName}'). Check UserName/Password, or create a dedicated non-guest user.",
                ex);
        }
    }
}

// Minimal AMQP seam for the error-queue transport. The transport only needs
// three shapes: a connection that can hand out channels, a control channel
// for declare/drain, and a publisher-confirm channel. Keeping those shapes
// narrow makes the channel-reuse and shutdown behaviour unit-testable without
// a broker.
internal interface IErrorQueueConnection : IAsyncDisposable
{
    bool IsOpen { get; }
    Task<IErrorQueuePublishChannel> CreatePublishChannelAsync(CancellationToken ct);
    Task<IErrorQueueControlChannel> CreateControlChannelAsync(CancellationToken ct);
    Task CloseAsync(CancellationToken ct);
}

internal interface IErrorQueuePublishChannel : IAsyncDisposable
{
    bool IsOpen { get; }
    Task PublishAsync(string queue, ErrorEnvelope envelope, TimeSpan confirmTimeout, CancellationToken ct);
}

internal interface IErrorQueueControlChannel : IAsyncDisposable
{
    bool IsOpen { get; }
    Task DeclareAsync(string queue, IReadOnlyDictionary<string, object?> arguments, CancellationToken ct);
    Task DeclarePassiveAsync(string queue, CancellationToken ct);
    Task<ErrorQueueDelivery?> GetAsync(string queue, CancellationToken ct);
    Task AckAsync(ulong deliveryTag, CancellationToken ct);
}

internal sealed record ErrorQueueDelivery(
    ulong DeliveryTag,
    ReadOnlyMemory<byte> Body,
    IReadOnlyDictionary<string, object?> Headers);

internal sealed class AmqpErrorQueueConnection : IErrorQueueConnection
{
    private readonly IConnection _connection;

    public AmqpErrorQueueConnection(IConnection connection) => _connection = connection;

    public bool IsOpen => _connection.IsOpen;

    public async Task<IErrorQueuePublishChannel> CreatePublishChannelAsync(CancellationToken ct)
    {
        var channel = await _connection
            .CreateChannelAsync(new CreateChannelOptions(true, true, null, null), ct)
            .ConfigureAwait(false);
        return new AmqpPublishChannel(channel);
    }

    public async Task<IErrorQueueControlChannel> CreateControlChannelAsync(CancellationToken ct)
    {
        var channel = await _connection
            .CreateChannelAsync(new CreateChannelOptions(false, false, null, null), ct)
            .ConfigureAwait(false);
        return new AmqpControlChannel(channel);
    }

    public async Task CloseAsync(CancellationToken ct)
        => await _connection.CloseAsync(200, "disposing", TimeSpan.FromSeconds(5), abort: false, cancellationToken: ct)
            .ConfigureAwait(false);

    public ValueTask DisposeAsync()
    {
        _connection.Dispose();
        return ValueTask.CompletedTask;
    }
}

internal sealed class AmqpPublishChannel : IErrorQueuePublishChannel
{
    private readonly IChannel _channel;

    public AmqpPublishChannel(IChannel channel) => _channel = channel;

    public bool IsOpen => _channel.IsOpen;

    public async Task PublishAsync(string queue, ErrorEnvelope envelope, TimeSpan confirmTimeout, CancellationToken ct)
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
        _channel.BasicAcksAsync += onAck;
        _channel.BasicNacksAsync += onNack;
        _channel.BasicReturnAsync += onReturn;
        _channel.ChannelShutdownAsync += onShutdown;
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
            // The confirm budget covers the send as well as the ack/nack: a blocked
            // AMQP publish must not hold the partition dispatch gate past the budget.
            using var timeout = new CancellationTokenSource(confirmTimeout);
            using var linked = CancellationTokenSource.CreateLinkedTokenSource(ct, timeout.Token);
            try
            {
                await _channel.BasicPublishAsync(
                    exchange: string.Empty,
                    routingKey: queue,
                    mandatory: true,
                    basicProperties: properties,
                    body: envelope.Body,
                    cancellationToken: linked.Token).ConfigureAwait(false);
                await confirmed.Task.WaitAsync(linked.Token).ConfigureAwait(false);
            }
            catch (OperationCanceledException) when (!ct.IsCancellationRequested)
            {
                throw new StreamBusTimeoutException($"Publish to error queue '{queue}' was not confirmed within {confirmTimeout} (outcome unknown).");
            }
        }
        finally
        {
            _channel.BasicAcksAsync -= onAck;
            _channel.BasicNacksAsync -= onNack;
            _channel.BasicReturnAsync -= onReturn;
            _channel.ChannelShutdownAsync -= onShutdown;
        }
    }

    public async ValueTask DisposeAsync()
        => await _channel.DisposeAsync().ConfigureAwait(false);
}

internal sealed class AmqpControlChannel : IErrorQueueControlChannel
{
    private readonly IChannel _channel;

    public AmqpControlChannel(IChannel channel) => _channel = channel;

    public bool IsOpen => _channel.IsOpen;

    public async Task DeclareAsync(string queue, IReadOnlyDictionary<string, object?> arguments, CancellationToken ct)
        => await _channel.QueueDeclareAsync(
            queue: queue,
            durable: true,
            exclusive: false,
            autoDelete: false,
            arguments: new Dictionary<string, object?>(arguments),
            passive: false,
            noWait: false,
            cancellationToken: ct).ConfigureAwait(false);

    public async Task DeclarePassiveAsync(string queue, CancellationToken ct)
        => await _channel.QueueDeclarePassiveAsync(queue, ct).ConfigureAwait(false);

    public async Task<ErrorQueueDelivery?> GetAsync(string queue, CancellationToken ct)
    {
        var delivery = await _channel.BasicGetAsync(queue, autoAck: false, cancellationToken: ct).ConfigureAwait(false);
        if (delivery is null) return null;
        var headers = delivery.BasicProperties?.Headers is { } raw
            ? new Dictionary<string, object?>(raw, StringComparer.Ordinal)
            : new Dictionary<string, object?>(StringComparer.Ordinal);
        return new ErrorQueueDelivery(delivery.DeliveryTag, delivery.Body, headers);
    }

    public async Task AckAsync(ulong deliveryTag, CancellationToken ct)
        => await _channel.BasicAckAsync(deliveryTag, multiple: false, cancellationToken: ct).ConfigureAwait(false);

    public async ValueTask DisposeAsync()
        => await _channel.DisposeAsync().ConfigureAwait(false);
}

public sealed class ErrorQueueTransport : IAsyncDisposable
{
    private static readonly TimeSpan DrainIdle = TimeSpan.FromSeconds(5);
    private static readonly TimeSpan DrainPoll = TimeSpan.FromMilliseconds(200);

    private readonly StreamBusOptions _options;
    private readonly Func<StreamBusOptions, CancellationToken, Task<IErrorQueueConnection>> _connect;
    private readonly ILogger<ErrorQueueTransport> _logger;
    private readonly SemaphoreSlim _gate = new(1, 1);
    private readonly SemaphoreSlim _publishGate = new(1, 1);
    private IErrorQueueConnection? _connection;
    private IErrorQueuePublishChannel? _publishChannel;
    private IErrorQueueConnection? _publishChannelConnection;
    private bool _fatal;
    private Exception? _fatalError;
    private int _disposed;

    public ErrorQueueTransport(
        StreamBusOptions options,
        IErrorQueueConnectionFactory? connector = null,
        ILogger<ErrorQueueTransport>? logger = null)
        : this(options, Wrap(connector ?? new AmqpConnectionFactory()), logger, marker: true)
    {
    }

    private ErrorQueueTransport(
        StreamBusOptions options,
        Func<StreamBusOptions, CancellationToken, Task<IErrorQueueConnection>> connect,
        ILogger<ErrorQueueTransport>? logger,
        bool marker)
    {
        ArgumentNullException.ThrowIfNull(options);
        ArgumentNullException.ThrowIfNull(connect);
        _options = options;
        _connect = connect;
        _logger = logger ?? NullLogger<ErrorQueueTransport>.Instance;
    }

    internal static ErrorQueueTransport ForTest(
        StreamBusOptions options,
        Func<StreamBusOptions, CancellationToken, Task<IErrorQueueConnection>> connect,
        ILogger<ErrorQueueTransport>? logger = null)
        => new(options, connect, logger, marker: true);

    private static Func<StreamBusOptions, CancellationToken, Task<IErrorQueueConnection>> Wrap(
        IErrorQueueConnectionFactory connector)
        => async (options, ct) =>
            new AmqpErrorQueueConnection(await connector.ConnectAsync(options, ct).ConfigureAwait(false));

    internal static Dictionary<string, object?> QueueArguments(DeadLetterQueueType type) => new()
    {
        ["x-queue-type"] = type == DeadLetterQueueType.QuorumQueue ? "quorum" : "classic",
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
            if (queue.QueueType is not { } queueType)
                throw new StreamBusConfigurationException($"Internal error: stream dead-letter destination '{queue.Name}' reached the queue transport; it must never declare an AMQP queue.");
            await using var channel = await connection.CreateControlChannelAsync(ct).ConfigureAwait(false);
            try
            {
                await channel.DeclareAsync(queue.Name, QueueArguments(queueType), ct).ConfigureAwait(false);
            }
            catch (OperationInterruptedException ex) when (ex.ShutdownReason is { ReplyCode: 406 })
            {
                throw new StreamConfigurationConflictException(queue.Name,
                    $"queue '{queue.Name}' ({queueType}, durable)",
                    "an existing queue with conflicting type or arguments");
            }
        }
    }

    public async Task PublishAsync(DeadLetterOptions queue, ErrorEnvelope envelope, CancellationToken ct = default)
    {
        ArgumentNullException.ThrowIfNull(queue);
        ArgumentNullException.ThrowIfNull(envelope);
        ArgumentException.ThrowIfNullOrWhiteSpace(queue.Name);
        var confirmTimeout = queue.QueueOptions.ConfirmTimeout;
        if (confirmTimeout <= TimeSpan.Zero)
            throw new StreamBusConfigurationException(
                $"Error queue '{queue.Name}' has a non-positive ConfirmTimeout; configure a positive ErrorQueueOptions.ConfirmTimeout.");

        var connection = await GetConnectionAsync(ct).ConfigureAwait(false);
        try
        {
            try
            {
                await _publishGate.WaitAsync(ct).ConfigureAwait(false);
            }
            catch (ObjectDisposedException ex)
            {
                throw new StreamBusTransientException("Error queue transport is stopping.", ex);
            }

            try
            {
                var channel = await GetPublishChannelAsync(connection, ct).ConfigureAwait(false);
                try
                {
                    await channel.PublishAsync(queue.Name, envelope, confirmTimeout, ct).ConfigureAwait(false);
                }
                catch (StreamBusTimeoutException)
                {
                    // The confirm outcome is unknown; a late ack must not be
                    // mistaken for the next publish on this channel.
                    await ResetPublishChannelAsync().ConfigureAwait(false);
                    throw;
                }
                catch (ObjectDisposedException)
                {
                    await ResetPublishChannelAsync().ConfigureAwait(false);
                    throw;
                }
                catch (AlreadyClosedException)
                {
                    await ResetPublishChannelAsync().ConfigureAwait(false);
                    throw;
                }
            }
            finally
            {
                try { _publishGate.Release(); }
                catch (ObjectDisposedException) { }
            }
        }
        catch (BrokerUnreachableException)
        {
            await InvalidateConnectionAsync().ConfigureAwait(false);
            throw;
        }
        catch (AlreadyClosedException)
        {
            await InvalidateConnectionAsync().ConfigureAwait(false);
            throw;
        }
        catch (ObjectDisposedException ex)
        {
            await InvalidateConnectionAsync().ConfigureAwait(false);
            throw new StreamBusTransientException("Error queue transport is stopping.", ex);
        }
    }

    private async Task<IErrorQueuePublishChannel> GetPublishChannelAsync(
        IErrorQueueConnection connection,
        CancellationToken ct)
    {
        if (_publishChannel is { IsOpen: true } channel && ReferenceEquals(_publishChannelConnection, connection))
            return channel;
        await ResetPublishChannelAsync().ConfigureAwait(false);
        var created = await connection.CreatePublishChannelAsync(ct).ConfigureAwait(false);
        _publishChannel = created;
        _publishChannelConnection = connection;
        return created;
    }

    private async Task ResetPublishChannelAsync()
    {
        var channel = Interlocked.Exchange(ref _publishChannel, null);
        Interlocked.Exchange(ref _publishChannelConnection, null);
        if (channel is null) return;
        try { await channel.DisposeAsync().ConfigureAwait(false); }
        catch { }
    }

    public async Task<int> DrainAsync(string errorQueue, Func<ErrorEnvelope, CancellationToken, Task> republish, CancellationToken ct = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(errorQueue);
        ArgumentNullException.ThrowIfNull(republish);
        var connection = await GetConnectionAsync(ct).ConfigureAwait(false);
        await using var channel = await connection.CreateControlChannelAsync(ct).ConfigureAwait(false);
        try
        {
            await channel.DeclarePassiveAsync(errorQueue, ct).ConfigureAwait(false);
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
            var delivery = await channel.GetAsync(errorQueue, ct).ConfigureAwait(false);
            if (delivery is null)
            {
                var since = DateTimeOffset.UtcNow - new DateTimeOffset(Interlocked.Read(ref lastTicks), TimeSpan.Zero);
                if (since >= DrainIdle) break;
                await Task.Delay(DrainPoll, ct).ConfigureAwait(false);
                continue;
            }
            Interlocked.Exchange(ref lastTicks, DateTimeOffset.UtcNow.UtcTicks);
            if (!ErrorEnvelope.TryParseHeaders(errorQueue, delivery.Body.ToArray(), delivery.Headers, out var envelope) || envelope is null)
            {
                _logger.LogWarning("Requeue skipping malformed message on error queue '{Queue}' (delivery tag {Tag}).",
                    errorQueue, delivery.DeliveryTag);
                await channel.AckAsync(delivery.DeliveryTag, ct).ConfigureAwait(false);
                skipped++;
                continue;
            }
            await republish(envelope, ct).ConfigureAwait(false);
            await channel.AckAsync(delivery.DeliveryTag, ct).ConfigureAwait(false);
            requeued++;
        }
        if (skipped > 0)
            _logger.LogWarning("Requeue from error queue '{Queue}' skipped {Skipped} malformed entries.", errorQueue, skipped);
        return requeued;
    }

    private async Task<IErrorQueueConnection> GetConnectionAsync(CancellationToken ct)
    {
        if (Volatile.Read(ref _disposed) == 1)
            throw new StreamBusTransientException("Error queue transport is stopping.");

        var current = Volatile.Read(ref _connection);
        if (current is not null && current.IsOpen) return current;
        try
        {
            await _gate.WaitAsync(ct).ConfigureAwait(false);
        }
        catch (ObjectDisposedException ex)
        {
            throw new StreamBusTransientException("Error queue transport is stopping.", ex);
        }

        try
        {
            current = Volatile.Read(ref _connection);
            if (current is not null && current.IsOpen) return current;
            if (_fatal)
                throw new StreamBusFatalException("AMQP error-queue connection is in Fatal state; see the earlier authentication failure.", _fatalError);
            IErrorQueueConnection created;
            try
            {
                created = await _connect(_options, ct).ConfigureAwait(false);
            }
            catch (ObjectDisposedException ex)
            {
                throw new StreamBusTransientException("Error queue transport is stopping.", ex);
            }
            catch (StreamBusFatalException ex)
            {
                _fatal = true;
                _fatalError = ex;
                throw;
            }
            if (Volatile.Read(ref _disposed) == 1)
            {
                try { await created.DisposeAsync().ConfigureAwait(false); } catch { }
                throw new StreamBusTransientException("Error queue transport is stopping.");
            }
            Volatile.Write(ref _connection, created);
            return created;
        }
        finally
        {
            try { _gate.Release(); }
            catch (ObjectDisposedException) { }
        }
    }

    private async Task InvalidateConnectionAsync()
    {
        await ResetPublishChannelAsync().ConfigureAwait(false);
        var current = Interlocked.Exchange(ref _connection, null);
        if (current is null) return;
        try { await current.DisposeAsync().ConfigureAwait(false); }
        catch { }
    }

    public async ValueTask DisposeAsync()
    {
        if (Interlocked.Exchange(ref _disposed, 1) == 1) return;
        await ResetPublishChannelAsync().ConfigureAwait(false);
        var current = Interlocked.Exchange(ref _connection, null);
        if (current is not null)
        {
            try { await current.CloseAsync(CancellationToken.None).ConfigureAwait(false); }
            catch { }
            try { await current.DisposeAsync().ConfigureAwait(false); }
            catch { }
        }
        _gate.Dispose();
        _publishGate.Dispose();
    }
}
