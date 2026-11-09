using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using RabbitMQ.Client.Exceptions;
using System.Security.Authentication;

namespace StreamBus;

internal interface IErrorQueueConnectionFactory
{
    Task<IConnection> ConnectAsync(StreamBusOptions options, CancellationToken cancellationToken);
}

internal sealed class AmqpConnectionFactory : IErrorQueueConnectionFactory
{
    internal static ConnectionFactory BuildFactory(StreamBusOptions options)
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
        if (options.Tls is { Enabled: true } tls)
        {
            if (tls.ClientCertPath is { } certPath && !File.Exists(certPath))
                throw new StreamBusConfigurationException($"TLS client certificate not found at '{certPath}'.");
            factory.Ssl = new SslOption
            {
                Enabled = true,
                ServerName = tls.ServerName ?? options.Host,
                CertPath = tls.ClientCertPath ?? string.Empty,
                CertPassphrase = tls.ClientCertPassword ?? string.Empty,
                AcceptablePolicyErrors = tls.AcceptableCertificateErrors,
                Version = tls.ProtocolVersion,
            };
        }
        return factory;
    }

    public async Task<IConnection> ConnectAsync(StreamBusOptions options, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(options);
        var factory = BuildFactory(options);
        try
        {
            return await factory.CreateConnectionAsync(cancellationToken).ConfigureAwait(false);
        }
        catch (AuthenticationFailureException ex)
        {
            throw new StreamBusFatalException(
                $"AMQP authentication failed for vhost '{options.Vhost}' (user '{options.UserName}'). Check UserName/Password, or create a dedicated non-guest user.",
                ex);
        }
        // A TLS handshake can only fail this way when the client speaks TLS to a
        // listener that is not (observed live: RabbitMQ.Client 7.2.2 throws
        // BrokerUnreachableException wrapping an AuthenticationException "Cannot
        // determine the frame size or a corrupted frame was received"). That is a
        // permanent misconfiguration, so classify it fatal: the queue transport's
        // fatal latch, the startup declare loop, and the handoff retry path all
        // stop instead of retrying forever with an unhelpful "endpoint
        // unreachable" message. Guarded by TLS-enabled so plain-connect
        // unreachability stays retriable (e.g. a restarting broker).
        catch (BrokerUnreachableException ex) when (options.Tls is { Enabled: true } && HasTlsHandshakeFailure(ex))
        {
            throw new StreamBusFatalException(
                $"AMQP TLS handshake failed for vhost '{options.Vhost}' on {options.Host}:{options.AmqpPort}. " +
                "The listener is probably a plain (non-TLS) port, or the TLS negotiation is otherwise unrecoverable. " +
                "Check UseTls and the AmqpPort (the TLS default is 5671).",
                ex);
        }
    }

    /// <summary>True when the exception's inner chain contains a TLS handshake failure.</summary>
    private static bool HasTlsHandshakeFailure(Exception ex)
    {
        for (var current = ex; current is not null; current = current.InnerException)
        {
            if (current is AuthenticationException)
                return true;
        }
        return false;
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
    Task<IErrorQueuePublishChannel> CreatePublishChannelAsync(CancellationToken cancellationToken);
    Task<IErrorQueueControlChannel> CreateControlChannelAsync(CancellationToken cancellationToken);
    Task CloseAsync(CancellationToken cancellationToken);
}

internal interface IErrorQueuePublishChannel : IAsyncDisposable
{
    bool IsOpen { get; }
    Task PublishAsync(string queue, ErrorEnvelope envelope, TimeSpan confirmTimeout, CancellationToken cancellationToken);
}

internal interface IErrorQueueControlChannel : IAsyncDisposable
{
    bool IsOpen { get; }
    Task DeclareAsync(string queue, IReadOnlyDictionary<string, object?> arguments, CancellationToken cancellationToken);
    Task DeclarePassiveAsync(string queue, CancellationToken cancellationToken);
}

internal sealed class AmqpErrorQueueConnection : IErrorQueueConnection
{
    private readonly IConnection _connection;

    public AmqpErrorQueueConnection(IConnection connection) => _connection = connection;

    public bool IsOpen => _connection.IsOpen;

    public async Task<IErrorQueuePublishChannel> CreatePublishChannelAsync(CancellationToken cancellationToken)
    {
        var channel = await _connection
            .CreateChannelAsync(new CreateChannelOptions(true, true, null, null), cancellationToken)
            .ConfigureAwait(false);
        return new AmqpPublishChannel(channel);
    }

    public async Task<IErrorQueueControlChannel> CreateControlChannelAsync(CancellationToken cancellationToken)
    {
        var channel = await _connection
            .CreateChannelAsync(new CreateChannelOptions(false, false, null, null), cancellationToken)
            .ConfigureAwait(false);
        return new AmqpControlChannel(channel);
    }

    public async Task CloseAsync(CancellationToken cancellationToken)
        => await _connection.CloseAsync(200, "disposing", TimeSpan.FromSeconds(5), abort: false, cancellationToken: cancellationToken)
            .ConfigureAwait(false);

    public async ValueTask DisposeAsync()
        => await _connection.DisposeAsync().ConfigureAwait(false);
}

internal sealed class AmqpPublishChannel : IErrorQueuePublishChannel
{
    private readonly IChannel _channel;

    public AmqpPublishChannel(IChannel channel) => _channel = channel;

    public bool IsOpen => _channel.IsOpen;

    public async Task PublishAsync(string queue, ErrorEnvelope envelope, TimeSpan confirmTimeout, CancellationToken cancellationToken)
    {
        var confirmed = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
        void Complete(Exception? error)
        {
            if (error is null) confirmed.TrySetResult(true);
            else confirmed.TrySetException(error);
        }

        // Confirms are pending per delivery tag on this channel; a late ack or
        // nack from an earlier unknown-outcome publish must not resolve this
        // publish's confirm. Publishes are serialized by the transport gate,
        // so the next sequence number read here is this publish's own tag.
        var deliveryTag = await _channel.GetNextPublishSequenceNumberAsync(cancellationToken).ConfigureAwait(false);
        void CompleteIfOwn(ulong tag, bool multiple, Exception? error)
        {
            // An ack/nack with multiple=true covers every tag up to DeliveryTag.
            if (tag != deliveryTag && !(multiple && tag > deliveryTag))
                return;
            Complete(error);
        }

        AsyncEventHandler<BasicAckEventArgs> onAck = (_, e) => { CompleteIfOwn(e.DeliveryTag, e.Multiple, null); return Task.CompletedTask; };
        AsyncEventHandler<BasicNackEventArgs> onNack = (_, e) => { CompleteIfOwn(e.DeliveryTag, e.Multiple, new StreamBusPublishException($"Error queue '{queue}' negatively confirmed the handoff (delivery tag {e.DeliveryTag}).")); return Task.CompletedTask; };
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
                MessageId = envelope.MessageId?.ToString("D"),
                CorrelationId = envelope.CorrelationId?.ToString("D"),
                ContentType = envelope.ContentType,
                Headers = envelope.ToHeaders(),
            };
            // The confirm budget covers the send as well as the ack/nack: a blocked
            // AMQP publish must not hold the partition dispatch gate past the budget.
            using var timeout = new CancellationTokenSource(confirmTimeout);
            using var linked = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken, timeout.Token);
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
            catch (OperationCanceledException) when (!cancellationToken.IsCancellationRequested)
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

    public async Task DeclareAsync(string queue, IReadOnlyDictionary<string, object?> arguments, CancellationToken cancellationToken)
        => await _channel.QueueDeclareAsync(
            queue: queue,
            durable: true,
            exclusive: false,
            autoDelete: false,
            arguments: new Dictionary<string, object?>(arguments),
            passive: false,
            noWait: false,
            cancellationToken: cancellationToken).ConfigureAwait(false);

    public async Task DeclarePassiveAsync(string queue, CancellationToken cancellationToken)
        => await _channel.QueueDeclarePassiveAsync(queue, cancellationToken).ConfigureAwait(false);

    public async ValueTask DisposeAsync()
        => await _channel.DisposeAsync().ConfigureAwait(false);
}

internal sealed class ErrorQueueTransport : IAsyncDisposable
{
    private sealed record PublishChannelState(IErrorQueuePublishChannel Channel, IErrorQueueConnection Connection);

    private readonly StreamBusOptions _options;
    private readonly Func<StreamBusOptions, CancellationToken, Task<IErrorQueueConnection>> _connect;
    private readonly ILogger<ErrorQueueTransport> _logger;
    private readonly SemaphoreSlim _gate = new(1, 1);
    private readonly SemaphoreSlim _publishGate = new(1, 1);
    private IErrorQueueConnection? _connection;

    // The cached publish channel and the connection it was created on are one
    // atomic pair: a torn observation (channel from one connection, connection
    // field from another) can never happen because the pair is swapped with a
    // single reference write. All writes happen under _publishGate.
    private PublishChannelState? _publishState;
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
        => async (options, cancellationToken) =>
            new AmqpErrorQueueConnection(await connector.ConnectAsync(options, cancellationToken).ConfigureAwait(false));

    internal static Dictionary<string, object?> QueueArguments(DeadLetterQueueType type) => new()
    {
        ["x-queue-type"] = type == DeadLetterQueueType.QuorumQueue ? "quorum" : "classic",
    };

    internal static IReadOnlyList<DeadLetterOptions> CollectQueueDeclarations(IEnumerable<TypedConsumerRegistration> registrations)
    {
        ArgumentNullException.ThrowIfNull(registrations);
        return registrations
            .Where(r => r.Options.DeadLetter is not null && r.Options.OnExhausted is null)
            .Select(r => r.Options.DeadLetter!)
            .Distinct()
            .ToList();
    }

    public async Task DeclareAsync(IEnumerable<DeadLetterOptions> queues, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(queues);
        var distinct = queues.Distinct().ToList();
        if (distinct.Count == 0) return;
        var connection = await GetConnectionAsync(cancellationToken).ConfigureAwait(false);
        foreach (var queue in distinct)
        {
            ArgumentException.ThrowIfNullOrWhiteSpace(queue.Name);
            if (queue.QueueType is not { } queueType)
                throw new StreamBusConfigurationException($"Internal error: stream dead-letter destination '{queue.Name}' reached the queue transport; it must never declare an AMQP queue.");
            await using var channel = await connection.CreateControlChannelAsync(cancellationToken).ConfigureAwait(false);
            try
            {
                await channel.DeclareAsync(queue.Name, QueueArguments(queueType), cancellationToken).ConfigureAwait(false);
            }
            catch (OperationInterruptedException ex) when (ex.ShutdownReason is { ReplyCode: 406 })
            {
                throw new StreamBusStreamConfigurationConflictException(queue.Name,
                    $"queue '{queue.Name}' ({queueType}, durable)",
                    "an existing queue with conflicting type or arguments");
            }
        }
    }

    public async Task PublishAsync(DeadLetterOptions queue, ErrorEnvelope envelope, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(queue);
        ArgumentNullException.ThrowIfNull(envelope);
        ArgumentException.ThrowIfNullOrWhiteSpace(queue.Name);
        var confirmTimeout = queue.QueueOptions.ConfirmTimeout;
        if (confirmTimeout <= TimeSpan.Zero)
            throw new StreamBusConfigurationException(
                $"Error queue '{queue.Name}' has a non-positive ConfirmTimeout; configure a positive ErrorQueueOptions.ConfirmTimeout.");

        var connection = await GetConnectionAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            try
            {
                await _publishGate.WaitAsync(cancellationToken).ConfigureAwait(false);
            }
            catch (ObjectDisposedException ex)
            {
                throw new StreamBusTransientException("Error queue transport is stopping.", ex);
            }

            try
            {
                var channel = await GetPublishChannelAsync(connection, cancellationToken).ConfigureAwait(false);
                try
                {
                    await channel.PublishAsync(queue.Name, envelope, confirmTimeout, cancellationToken).ConfigureAwait(false);
                }
                catch (StreamBusTimeoutException)
                {
                    // The confirm outcome is unknown; a late ack must not be
                    // mistaken for the next publish on this channel.
                    await ResetPublishChannelAsync().ConfigureAwait(false);
                    throw;
                }
                catch (OperationCanceledException)
                {
                    // Caller cancelled mid-send: the confirmation outcome is
                    // unknown, so this channel must not be reused either.
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

    private Task<IErrorQueuePublishChannel> GetPublishChannelAsync(
        IErrorQueueConnection connection,
        CancellationToken cancellationToken)
    {
        var state = Volatile.Read(ref _publishState);
        if (state is { Channel: { IsOpen: true } } matched && ReferenceEquals(matched.Connection, connection))
            return Task.FromResult(matched.Channel);
        return CreatePublishChannelAsync(connection, cancellationToken);
    }

    private async Task<IErrorQueuePublishChannel> CreatePublishChannelAsync(
        IErrorQueueConnection connection,
        CancellationToken cancellationToken)
    {
        await ResetPublishChannelAsync().ConfigureAwait(false);
        var created = await connection.CreatePublishChannelAsync(cancellationToken).ConfigureAwait(false);
        Volatile.Write(ref _publishState, new PublishChannelState(created, connection));
        return created;
    }

    private async Task ResetPublishChannelAsync()
    {
        var state = Interlocked.Exchange(ref _publishState, null);
        if (state is null) return;
        try { await state.Channel.DisposeAsync().ConfigureAwait(false); }
        catch { }
    }

    private async Task<IErrorQueueConnection> GetConnectionAsync(CancellationToken cancellationToken)
    {
        if (Volatile.Read(ref _disposed) == 1)
            throw new StreamBusTransientException("Error queue transport is stopping.");

        var current = Volatile.Read(ref _connection);
        if (current is not null && current.IsOpen) return current;
        try
        {
            await _gate.WaitAsync(cancellationToken).ConfigureAwait(false);
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
                created = await _connect(_options, cancellationToken).ConfigureAwait(false);
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
        // Take the publish gate so the reset cannot interleave with an
        // in-flight publish's channel swap. If the gate is already disposed,
        // DisposeAsync has already cleared the cached pair — nothing to reset.
        if (await TryEnterPublishGateAsync().ConfigureAwait(false))
        {
            try { await ResetPublishChannelAsync().ConfigureAwait(false); }
            finally { ExitPublishGate(); }
        }
        var current = Interlocked.Exchange(ref _connection, null);
        if (current is null) return;
        try { await current.DisposeAsync().ConfigureAwait(false); }
        catch { }
    }

    private async Task<bool> TryEnterPublishGateAsync()
    {
        try
        {
            await _publishGate.WaitAsync(CancellationToken.None).ConfigureAwait(false);
            return true;
        }
        catch (ObjectDisposedException)
        {
            return false;
        }
    }

    private void ExitPublishGate()
    {
        try { _publishGate.Release(); }
        catch (ObjectDisposedException) { }
    }

    public async ValueTask DisposeAsync()
    {
        if (Interlocked.Exchange(ref _disposed, 1) == 1) return;
        // Serialize with any in-flight publish so the cached channel/connection
        // pair can never be swapped or disposed mid-publish. Publishes are
        // bounded (confirm budget and caller token), so this wait terminates.
        if (await TryEnterPublishGateAsync().ConfigureAwait(false))
        {
            try
            {
                await ResetPublishChannelAsync().ConfigureAwait(false);
            }
            finally
            {
                ExitPublishGate();
            }
        }
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
