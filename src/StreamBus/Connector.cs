using System.Net;
using System.Security.Cryptography;
using System.Text;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using RabbitMQ.Stream.Client;

namespace StreamBus;

public interface IStreamSystemHandle : IAsyncDisposable
{
    bool IsClosed { get; }
    Task<IStreamProducerHandle> CreatePlainProducerAsync(string stream, CancellationToken ct);
    Task<IStreamProducerHandle> CreatePlainProducerWithReferenceAsync(string stream, string reference, CancellationToken ct);
    Task<IStreamProducerHandle> CreateSuperStreamProducerAsync(string stream, CancellationToken ct);
    Task<IStreamProducerHandle> CreateSuperStreamProducerWithReferenceAsync(string stream, string reference, CancellationToken ct);
    Task<ulong> QuerySequenceAsync(string reference, string stream, CancellationToken ct);
    Task<RawConsumer> CreateRawConsumerAsync(RawConsumerConfig config, CancellationToken ct);
    Task<ulong?> TryQueryOffsetAsync(string reference, string stream, CancellationToken ct);
    Task StoreSystemOffsetAsync(string reference, string stream, ulong offset, CancellationToken ct);
    Task<bool> StreamExistsAsync(string stream, CancellationToken ct);
    Task<bool> SuperStreamExistsAsync(string superStream, CancellationToken ct);
    Task<string[]> QueryPartitionAsync(string superStream, CancellationToken ct);
    Task CreateStreamAsync(RabbitMQ.Stream.Client.StreamSpec spec, CancellationToken ct);
    Task CreateSuperStreamAsync(RabbitMQ.Stream.Client.PartitionsSuperStreamSpec spec, CancellationToken ct);
}

public interface IStreamSystemConnector
{
    Task<IStreamSystemHandle> ConnectAsync(StreamBusOptions options, CancellationToken ct);
}

public sealed class StreamSystemHandle : IStreamSystemHandle
{
    private readonly StreamSystem _system;
    private readonly ILoggerFactory _factory;
    // Process-wide: sequential same-stream producers across systems must never
    // collide (the broker deduplicates by producer name + publishing id).
    private static long _producerSeq;

    private static string ProducerReference(string stream)
    {
        var full = $"streambus-pub-{stream}-{Interlocked.Increment(ref _producerSeq)}";
        if (Encoding.UTF8.GetByteCount(full) <= StreamBusLimits.MaxReferenceBytes)
            return full;
        // Mirror the consumer-reference fallback: hash-suffix stays under the cap.
        // (Phase 7 replaces naming with stable per-(partition,target) names.)
        var hash = Convert.ToHexString(SHA256.HashData(Encoding.UTF8.GetBytes(full)))[..32];
        return $"streambus-pub-{hash}";
    }
    public StreamSystemHandle(StreamSystem system, ILoggerFactory? loggerFactory = null)
    {
        _system = system;
        _factory = loggerFactory ?? NullLoggerFactory.Instance;
    }
    public bool IsClosed => _system.IsClosed;

    public async Task<IStreamProducerHandle> CreatePlainProducerAsync(string stream, CancellationToken ct)
        => await CreatePlainProducerCoreAsync(stream, ProducerReference(stream), ct).ConfigureAwait(false);

    public async Task<IStreamProducerHandle> CreatePlainProducerWithReferenceAsync(string stream, string reference, CancellationToken ct)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(reference);
        if (Encoding.UTF8.GetByteCount(reference) > StreamBusLimits.MaxReferenceBytes)
            throw new StreamBusConfigurationException($"Producer reference exceeds the broker's 255-byte limit.");
        return await CreatePlainProducerCoreAsync(stream, reference, ct).ConfigureAwait(false);
    }

    private async Task<IStreamProducerHandle> CreatePlainProducerCoreAsync(string stream, string reference, CancellationToken ct)
    {
        ct.ThrowIfCancellationRequested();
        var confirmations = new ConfirmationTracker();
        // Unique producer name per handle on the ProducerReference path: the broker
        // deduplicates publishes by (producer name, publishing id) and ids restart
        // at 1 per handle, so a reused name would silently drop a later handle's
        // first messages as duplicates (confirmed-Ok but never appended). Distinct
        // names keep every handle's id sequence collision-free, including after
        // recreates. (Explicit references are replay-scoped stable names — Phase 7
        // owns stable FR-6 naming; replay idempotency relies on this determinism.)
        var producer = await _system.CreateRawProducer(new RawProducerConfig(stream)
        {
            MessagesBufferSize = 100,
            Reference = reference,
            ConfirmHandler = confirmations.Complete,
            ConnectionClosedHandler = _ =>
            {
                confirmations.OnConnectionLost();
                return Task.CompletedTask;
            },
        }, _factory.CreateLogger<RawProducer>()).ConfigureAwait(false);
        return new StreamProducerHandle(producer, stream, confirmations, _factory.CreateLogger<StreamProducerHandle>());
    }

    public async Task<IStreamProducerHandle> CreateSuperStreamProducerAsync(string stream, CancellationToken ct)
    {
        ct.ThrowIfCancellationRequested();
        var confirmations = new ConfirmationTracker();
        // Same dedup rationale as the plain-producer call above.
        var producer = await _system.CreateRawSuperStreamProducer(new RawSuperStreamProducerConfig(stream)
        {
            MessagesBufferSize = 100,
            Reference = ProducerReference(stream),
            Routing = SuperStreamProducerHandle.RoutingFor,
            ConfirmHandler = confirmation => confirmations.Complete(confirmation.Item2),
            ConnectionClosedHandler = (_, _) =>
            {
                confirmations.OnConnectionLost();
                return Task.CompletedTask;
            },
        }, _factory.CreateLogger<RawProducer>()).ConfigureAwait(false);
        return new SuperStreamProducerHandle(producer, stream, confirmations, _factory.CreateLogger<SuperStreamProducerHandle>());
    }

    public async Task<IStreamProducerHandle> CreateSuperStreamProducerWithReferenceAsync(string stream, string reference, CancellationToken ct)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(reference);
        if (Encoding.UTF8.GetByteCount(reference) > StreamBusLimits.MaxReferenceBytes)
            throw new StreamBusConfigurationException($"Producer reference exceeds the broker's 255-byte limit.");
        ct.ThrowIfCancellationRequested();
        var confirmations = new ConfirmationTracker();
        // Stable name (not the unique-name path): recreating under the same name is
        // safe — dedup is broker-side per (name, id) and the fresh tracker holds no
        // stale in-flight ids. The same routing table as the unique-name path applies.
        var producer = await _system.CreateRawSuperStreamProducer(new RawSuperStreamProducerConfig(stream)
        {
            MessagesBufferSize = 100,
            Reference = reference,
            Routing = SuperStreamProducerHandle.RoutingFor,
            ConfirmHandler = confirmation => confirmations.Complete(confirmation.Item2),
            ConnectionClosedHandler = (_, _) =>
            {
                confirmations.OnConnectionLost();
                return Task.CompletedTask;
            },
        }, _factory.CreateLogger<RawProducer>()).ConfigureAwait(false);
        return new SuperStreamProducerHandle(producer, stream, confirmations, _factory.CreateLogger<SuperStreamProducerHandle>());
    }

    public async Task CreateStreamAsync(RabbitMQ.Stream.Client.StreamSpec spec, CancellationToken ct)
    {
        ArgumentNullException.ThrowIfNull(spec);
        ct.ThrowIfCancellationRequested();
        await _system.CreateStream(spec).ConfigureAwait(false);
    }

    public async Task CreateSuperStreamAsync(RabbitMQ.Stream.Client.PartitionsSuperStreamSpec spec, CancellationToken ct)
    {
        ArgumentNullException.ThrowIfNull(spec);
        ct.ThrowIfCancellationRequested();
        await _system.CreateSuperStream(spec).ConfigureAwait(false);
    }

    public async Task<ulong> QuerySequenceAsync(string reference, string stream, CancellationToken ct)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(reference);
        ArgumentException.ThrowIfNullOrWhiteSpace(stream);
        ct.ThrowIfCancellationRequested();
        return await _system.QuerySequence(reference, stream).ConfigureAwait(false);
    }

    public async Task<RawConsumer> CreateRawConsumerAsync(RawConsumerConfig config, CancellationToken ct)
    {
        ArgumentNullException.ThrowIfNull(config);
        ct.ThrowIfCancellationRequested();
        // StreamSystem.CreateRawConsumer surfaces IConsumer; the client constructs a
        // RawConsumer for plain-consumer configs, which the partition loop needs for
        // StoreOffset/Close. The cast lives here (single place), not in the loop.
        var consumer = await _system.CreateRawConsumer(config, _factory.CreateLogger<RawConsumer>()).ConfigureAwait(false);
        ct.ThrowIfCancellationRequested();
        return (RawConsumer)consumer;
    }

    public ValueTask DisposeAsync() => _system.DisposeAsync();

    public async Task<ulong?> TryQueryOffsetAsync(string reference, string stream, CancellationToken ct)
    {
        ct.ThrowIfCancellationRequested();
        return await _system.TryQueryOffset(reference, stream).ConfigureAwait(false);
    }

    public async Task StoreSystemOffsetAsync(string reference, string stream, ulong offset, CancellationToken ct)
    {
        ct.ThrowIfCancellationRequested();
        await _system.StoreOffset(reference, stream, offset).ConfigureAwait(false);
    }

    public async Task<bool> StreamExistsAsync(string stream, CancellationToken ct)
    {
        ct.ThrowIfCancellationRequested();
        return await _system.StreamExists(stream).ConfigureAwait(false);
    }

    public async Task<bool> SuperStreamExistsAsync(string superStream, CancellationToken ct)
    {
        ct.ThrowIfCancellationRequested();
        return await _system.SuperStreamExists(superStream).ConfigureAwait(false);
    }

    public async Task<string[]> QueryPartitionAsync(string superStream, CancellationToken ct)
    {
        ct.ThrowIfCancellationRequested();
        return await _system.QueryPartition(superStream).ConfigureAwait(false);
    }
}

public sealed class StreamSystemConnector : IStreamSystemConnector
{
    private readonly ILoggerFactory _factory;
    public StreamSystemConnector(ILoggerFactory? loggerFactory = null) =>
        _factory = loggerFactory ?? NullLoggerFactory.Instance;

    public async Task<IStreamSystemHandle> ConnectAsync(StreamBusOptions options, CancellationToken ct)
    {
        ArgumentNullException.ThrowIfNull(options);
        if (options.Connection.ProducersPerConnection > byte.MaxValue)
            throw new StreamBusConfigurationException(
                $"ProducersPerConnection must be <= {byte.MaxValue} (client pool limit).");
        if (options.Connection.ConsumersPerConnection > byte.MaxValue)
            throw new StreamBusConfigurationException(
                $"ConsumersPerConnection must be <= {byte.MaxValue} (client pool limit).");
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
        ct.ThrowIfCancellationRequested();
        return new StreamSystemHandle(system, _factory);
    }
}
