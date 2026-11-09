using System.Net;
using System.Text;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using RabbitMQ.Stream.Client;

namespace StreamBus;

/// <summary>
/// Live connection to one broker: the surface the rest of StreamBus uses for
/// producers, consumers, offsets, and topology queries/implementations over a
/// single connected <c>StreamSystem</c>. Dispose closes the system.
/// </summary>
public interface IStreamSystemHandle : IAsyncDisposable
{
    /// <summary><see langword="true"/> once the underlying system has been closed or lost.</summary>
    bool IsClosed { get; }

    /// <summary>Creates a plain-stream producer with a generated unique reference suitable for fresh publishing.</summary>
    /// <param name="stream">Target stream name.</param>
    /// <param name="cancellationToken">Cancels the create.</param>
    Task<IStreamPlainProducerHandle> CreatePlainProducerAsync(string stream, CancellationToken cancellationToken);

    /// <summary>Creates a plain-stream producer under an explicit stable reference (dedup identity); the reference must fit the broker's 255-byte name limit.</summary>
    /// <param name="stream">Target stream name.</param>
    /// <param name="reference">Stable producer reference; recreating under the same name keeps broker-side dedup deterministic.</param>
    /// <param name="cancellationToken">Cancels the create.</param>
    Task<IStreamPlainProducerHandle> CreatePlainProducerWithReferenceAsync(string stream, string reference, CancellationToken cancellationToken);

    /// <summary>Creates a super-stream producer with a generated unique reference; every send must carry a routing key.</summary>
    /// <param name="stream">Target super-stream name.</param>
    /// <param name="cancellationToken">Cancels the create.</param>
    Task<IStreamSuperProducerHandle> CreateSuperStreamProducerAsync(string stream, CancellationToken cancellationToken);

    /// <summary>Creates a super-stream producer under an explicit stable reference; the reference must fit the broker's 255-byte name limit.</summary>
    /// <param name="stream">Target super-stream name.</param>
    /// <param name="reference">Stable producer reference.</param>
    /// <param name="cancellationToken">Cancels the create.</param>
    Task<IStreamSuperProducerHandle> CreateSuperStreamProducerWithReferenceAsync(string stream, string reference, CancellationToken cancellationToken);

    /// <summary>Queries the broker's last publishing id for a producer reference on a stream (used to resume id sequences).</summary>
    /// <param name="reference">Producer reference.</param>
    /// <param name="stream">Stream name.</param>
    /// <param name="cancellationToken">Cancels the query.</param>
    Task<ulong> QuerySequenceAsync(string reference, string stream, CancellationToken cancellationToken);

    /// <summary>Creates a raw consumer for one stream partition from the given client config.</summary>
    /// <param name="config">Client raw-consumer config (reference, offset handlers, stream).</param>
    /// <param name="cancellationToken">Cancels the create.</param>
    Task<RawConsumer> CreateRawConsumerAsync(RawConsumerConfig config, CancellationToken cancellationToken);

    /// <summary>Queries the stored offset for a reference on a stream without failing when absent.</summary>
    /// <param name="reference">Consumer/producer reference.</param>
    /// <param name="stream">Stream name.</param>
    /// <param name="cancellationToken">Cancels the query.</param>
    /// <returns>The stored offset, or null when none is stored.</returns>
    Task<ulong?> TryQueryOffsetAsync(string reference, string stream, CancellationToken cancellationToken);

    /// <summary>Stores an offset for a reference on a stream (broker-side persistence).</summary>
    /// <param name="reference">Consumer/producer reference.</param>
    /// <param name="stream">Stream name.</param>
    /// <param name="offset">Offset to store.</param>
    /// <param name="cancellationToken">Cancels the store.</param>
    Task StoreSystemOffsetAsync(string reference, string stream, ulong offset, CancellationToken cancellationToken);

    /// <summary><see langword="true"/> when a plain stream with this name exists.</summary>
    /// <param name="stream">Stream name.</param>
    /// <param name="cancellationToken">Cancels the probe.</param>
    Task<bool> StreamExistsAsync(string stream, CancellationToken cancellationToken);

    /// <summary><see langword="true"/> when a super stream with this name exists.</summary>
    /// <param name="superStream">Super-stream name.</param>
    /// <param name="cancellationToken">Cancels the probe.</param>
    Task<bool> SuperStreamExistsAsync(string superStream, CancellationToken cancellationToken);

    /// <summary>Lists the partition stream names of a super stream.</summary>
    /// <param name="superStream">Super-stream name.</param>
    /// <param name="cancellationToken">Cancels the query.</param>
    Task<string[]> QueryPartitionAsync(string superStream, CancellationToken cancellationToken);

    /// <summary>Creates a plain stream from a client stream spec (idempotency is the caller's).</summary>
    Task CreateStreamAsync(RabbitMQ.Stream.Client.StreamSpec spec, CancellationToken cancellationToken);

    /// <summary>Creates a super stream from a client super-stream spec.</summary>
    Task CreateSuperStreamAsync(RabbitMQ.Stream.Client.PartitionsSuperStreamSpec spec, CancellationToken cancellationToken);
}

/// <summary>
/// Opens a client <c>StreamSystem</c> for one <see cref="StreamBusOptions"/>
/// (host/port, credentials, heartbeat, connection pool) and returns the
/// resulting <see cref="IStreamSystemHandle"/>.
/// </summary>
public interface IStreamSystemConnector
{
    /// <summary>Validates <paramref name="options"/> and connects to the broker.</summary>
    /// <param name="options">Connection options; invalid options throw <see cref="StreamBusConfigurationException"/>.</param>
    /// <param name="cancellationToken">Cancels the connect.</param>
    /// <returns>A handle over the connected system.</returns>
    Task<IStreamSystemHandle> ConnectAsync(StreamBusOptions options, CancellationToken cancellationToken);
}

/// <summary>
/// Fail-fast guard for the <see cref="StreamSystemHandle.CreateRawConsumerAsync"/> result: the
/// client's <see cref="StreamSystem.CreateRawConsumer"/> contract returns <see cref="IConsumer"/>,
/// but the partition loop requires the concrete <see cref="RawConsumer"/> subclass (StoreOffset/Close).
/// A client version returning a different concrete type must surface an actionable error here
/// instead of an opaque <see cref="InvalidCastException"/> from inside the attach loop.
/// </summary>
internal static class RawConsumerCast
{
    /// <summary>Returns the consumer when it is the required <see cref="RawConsumer"/> subclass.</summary>
    public static RawConsumer Require(IConsumer consumer) => consumer as RawConsumer
        ?? throw new StreamBusConfigurationException(
            $"The stream client returned '{consumer?.GetType().FullName ?? "null"}' where a RawConsumer was required " +
            "(client library version mismatch; update RabbitMQ.Stream.Client or StreamBus).");
}

internal sealed class StreamSystemHandle : IStreamSystemHandle
{
    private readonly StreamSystem _system;
    private readonly ILoggerFactory _factory;
    // Process-wide: sequential same-stream producers across systems must never
    // collide (the broker deduplicates by producer name + publishing id).
    private static long _producerSeq;

    private static string ProducerReference(string stream)
        => StreamBusNaming.PublishReference(stream, Interlocked.Increment(ref _producerSeq));
    /// <summary>Underlying client system (for disposal); null <paramref name="loggerFactory"/> falls back to a null logger.</summary>
    public StreamSystemHandle(StreamSystem system, ILoggerFactory? loggerFactory = null)
    {
        _system = system;
        _factory = loggerFactory ?? NullLoggerFactory.Instance;
    }
    /// <summary><see langword="true"/> once the underlying system has been closed or lost.</summary>
    public bool IsClosed => _system.IsClosed;

    /// <summary>Creates a plain-stream producer with a generated unique reference.</summary>
    public async Task<IStreamPlainProducerHandle> CreatePlainProducerAsync(string stream, CancellationToken cancellationToken)
        => await CreatePlainProducerCoreAsync(stream, ProducerReference(stream), cancellationToken).ConfigureAwait(false);

    /// <summary>Creates a plain-stream producer under an explicit stable reference; exceeding the 255-byte name limit throws.</summary>
    public async Task<IStreamPlainProducerHandle> CreatePlainProducerWithReferenceAsync(string stream, string reference, CancellationToken cancellationToken)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(reference);
        if (Encoding.UTF8.GetByteCount(reference) > StreamBusNaming.MaxName)
            throw new StreamBusConfigurationException($"Producer reference exceeds the broker's 255-byte limit.");
        return await CreatePlainProducerCoreAsync(stream, reference, cancellationToken).ConfigureAwait(false);
    }

    private async Task<IStreamPlainProducerHandle> CreatePlainProducerCoreAsync(string stream, string reference, CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        var confirmations = new ConfirmationTracker();
        var producer = await _system.CreateRawProducer(
            ProducerConfigs.Plain(stream, reference, confirmations),
            _factory.CreateLogger<RawProducer>()).ConfigureAwait(false);
        return new StreamProducerHandle(producer, stream, confirmations, _factory.CreateLogger<StreamProducerHandle>());
    }

    /// <summary>Creates a super-stream producer with a generated unique reference.</summary>
    public async Task<IStreamSuperProducerHandle> CreateSuperStreamProducerAsync(string stream, CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        var confirmations = new ConfirmationTracker();
        var routing = new SuperStreamRouting();
        var producer = await _system.CreateRawSuperStreamProducer(
            ProducerConfigs.Super(stream, ProducerReference(stream), confirmations, routing),
            _factory.CreateLogger<RawProducer>()).ConfigureAwait(false);
        return new SuperStreamProducerHandle(producer, stream, confirmations, routing, _factory.CreateLogger<SuperStreamProducerHandle>());
    }

    /// <summary>Creates a super-stream producer under an explicit stable reference; exceeding the 255-byte name limit throws.</summary>
    public async Task<IStreamSuperProducerHandle> CreateSuperStreamProducerWithReferenceAsync(string stream, string reference, CancellationToken cancellationToken)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(reference);
        if (Encoding.UTF8.GetByteCount(reference) > StreamBusNaming.MaxName)
            throw new StreamBusConfigurationException($"Producer reference exceeds the broker's 255-byte limit.");
        cancellationToken.ThrowIfCancellationRequested();
        var confirmations = new ConfirmationTracker();
        var routing = new SuperStreamRouting();
        var producer = await _system.CreateRawSuperStreamProducer(
            ProducerConfigs.Super(stream, reference, confirmations, routing),
            _factory.CreateLogger<RawProducer>()).ConfigureAwait(false);
        return new SuperStreamProducerHandle(producer, stream, confirmations, routing, _factory.CreateLogger<SuperStreamProducerHandle>());
    }

    /// <summary>Creates a plain stream from a client spec; a <see langword="null"/> spec throws.</summary>
    public async Task CreateStreamAsync(RabbitMQ.Stream.Client.StreamSpec spec, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(spec);
        cancellationToken.ThrowIfCancellationRequested();
        await _system.CreateStream(spec).ConfigureAwait(false);
    }

    /// <summary>Creates a super stream from a client spec; a <see langword="null"/> spec throws.</summary>
    public async Task CreateSuperStreamAsync(RabbitMQ.Stream.Client.PartitionsSuperStreamSpec spec, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(spec);
        cancellationToken.ThrowIfCancellationRequested();
        await _system.CreateSuperStream(spec).ConfigureAwait(false);
    }

    /// <summary>Queries the broker's last publishing id for a reference on a stream.</summary>
    public async Task<ulong> QuerySequenceAsync(string reference, string stream, CancellationToken cancellationToken)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(reference);
        ArgumentException.ThrowIfNullOrWhiteSpace(stream);
        cancellationToken.ThrowIfCancellationRequested();
        return await _system.QuerySequence(reference, stream).ConfigureAwait(false);
    }

    /// <summary>Creates a raw consumer; the client's <see cref="RawConsumer"/> subclass is required for StoreOffset/Close.</summary>
    public async Task<RawConsumer> CreateRawConsumerAsync(RawConsumerConfig config, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(config);
        cancellationToken.ThrowIfCancellationRequested();
        // StreamSystem.CreateRawConsumer surfaces IConsumer; the client constructs a
        // RawConsumer for plain-consumer configs, which the partition loop needs for
        // StoreOffset/Close. The cast lives here (single place), not in the loop.
        var consumer = await _system.CreateRawConsumer(config, _factory.CreateLogger<RawConsumer>()).ConfigureAwait(false);
        cancellationToken.ThrowIfCancellationRequested();
        return RawConsumerCast.Require(consumer);
    }

    /// <summary>Disposes the underlying system.</summary>
    public ValueTask DisposeAsync() => _system.DisposeAsync();

    /// <summary>Queries the stored offset for a reference; null when none is stored.</summary>
    public async Task<ulong?> TryQueryOffsetAsync(string reference, string stream, CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        return await _system.TryQueryOffset(reference, stream).ConfigureAwait(false);
    }

    /// <summary>Stores an offset broker-side for a reference on a stream.</summary>
    public async Task StoreSystemOffsetAsync(string reference, string stream, ulong offset, CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        await _system.StoreOffset(reference, stream, offset).ConfigureAwait(false);
    }

    /// <summary><see langword="true"/> when a plain stream with this name exists.</summary>
    public async Task<bool> StreamExistsAsync(string stream, CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        return await _system.StreamExists(stream).ConfigureAwait(false);
    }

    /// <summary><see langword="true"/> when a super stream with this name exists.</summary>
    public async Task<bool> SuperStreamExistsAsync(string superStream, CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        return await _system.SuperStreamExists(superStream).ConfigureAwait(false);
    }

    /// <summary>Lists the partition stream names of a super stream.</summary>
    public async Task<string[]> QueryPartitionAsync(string superStream, CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        return await _system.QueryPartition(superStream).ConfigureAwait(false);
    }
}

internal sealed class StreamSystemConnector : IStreamSystemConnector
{
    private readonly ILoggerFactory _factory;
    /// <summary>Creates a connector; a null <paramref name="loggerFactory"/> falls back to a null logger.</summary>
    public StreamSystemConnector(ILoggerFactory? loggerFactory = null) =>
        _factory = loggerFactory ?? NullLoggerFactory.Instance;

    /// <summary>Validates <paramref name="options"/>, builds the endpoint and pool config, connects, and returns the handle.</summary>
    public async Task<IStreamSystemHandle> ConnectAsync(StreamBusOptions options, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(options);
        foreach (var error in options.Validate())
            throw new StreamBusConfigurationException(error);
        EndPoint endpoint = IPAddress.TryParse(options.Host, out var address)
            ? new IPEndPoint(address, options.StreamPort)
            : new DnsEndPoint(options.Host, options.StreamPort);
        var config = new StreamSystemConfig
        {
            UserName = options.UserName,
            Password = options.Password,
            VirtualHost = options.Vhost,
            Heartbeat = options.Heartbeat,
            Endpoints = new List<EndPoint> { endpoint },
            ClientProvidedName = "streambus-locator",
            ConnectionPoolConfig = new ConnectionPoolConfig
            {
                ProducersPerConnection = (byte)options.Connection.ProducersPerConnection,
                ConsumersPerConnection = (byte)options.Connection.ConsumersPerConnection,
            },
        };
        var system = await StreamSystem.Create(config, _factory.CreateLogger<StreamSystem>()).ConfigureAwait(false);
        cancellationToken.ThrowIfCancellationRequested();
        return new StreamSystemHandle(system, _factory);
    }
}
