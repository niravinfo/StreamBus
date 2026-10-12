using System.Net;
using RabbitMQ.Stream.Client;

namespace StreamBus;

public interface IStreamSystemHandle : IAsyncDisposable
{
    bool IsClosed { get; }
    Task<IStreamProducerHandle> CreatePlainProducerAsync(string stream, CancellationToken ct);
    Task<IStreamProducerHandle> CreateSuperStreamProducerAsync(string stream, CancellationToken ct);
    Task<ulong?> TryQueryOffsetAsync(string reference, string stream, CancellationToken ct);
    Task StoreSystemOffsetAsync(string reference, string stream, ulong offset, CancellationToken ct);
    Task<ulong> GetEndOffsetAsync(string stream, CancellationToken ct);
    Task<bool> StreamExistsAsync(string stream, CancellationToken ct);
    Task<bool> SuperStreamExistsAsync(string superStream, CancellationToken ct);
    Task<string[]> QueryPartitionAsync(string superStream, CancellationToken ct);
}

public interface IStreamSystemConnector
{
    Task<IStreamSystemHandle> ConnectAsync(StreamBusOptions options, CancellationToken ct);
}

public sealed class StreamSystemHandle : IStreamSystemHandle
{
    private readonly StreamSystem _system;
    public StreamSystemHandle(StreamSystem system) => _system = system;
    public bool IsClosed => _system.IsClosed;

    public async Task<IStreamProducerHandle> CreatePlainProducerAsync(string stream, CancellationToken ct)
    {
        ct.ThrowIfCancellationRequested();
        var confirmations = new ConfirmationTracker();
        var producer = await _system.CreateRawProducer(new RawProducerConfig(stream)
        {
            MessagesBufferSize = 100,
            Reference = $"streambus-pub-{stream}",
            ConfirmHandler = confirmations.Complete,
        }).ConfigureAwait(false);
        return new StreamProducerHandle(producer, stream, confirmations);
    }

    public async Task<IStreamProducerHandle> CreateSuperStreamProducerAsync(string stream, CancellationToken ct)
    {
        ct.ThrowIfCancellationRequested();
        var confirmations = new ConfirmationTracker();
        var producer = await _system.CreateRawSuperStreamProducer(new RawSuperStreamProducerConfig(stream)
        {
            MessagesBufferSize = 100,
            Reference = $"streambus-pub-{stream}",
            Routing = SuperStreamProducerHandle.RoutingFor,
            ConfirmHandler = confirmation => confirmations.Complete(confirmation.Item2),
        }).ConfigureAwait(false);
        return new SuperStreamProducerHandle(producer, stream, confirmations);
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

    public async Task<ulong> GetEndOffsetAsync(string stream, CancellationToken ct)
    {
        ct.ThrowIfCancellationRequested();
        var stats = await _system.StreamStats(stream).ConfigureAwait(false);
#pragma warning disable CS0618 // 1.12.1 marks LastOffset obsolete in favor of CommittedChunkId (different semantic); probe-pinned as the only end-offset source.
        return stats.LastOffset();
#pragma warning restore CS0618
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
            ? new IPEndPoint(address, options.Port)
            : new DnsEndPoint(options.Host, options.Port);
        var config = new StreamSystemConfig
        {
            UserName = options.UserName,
            Password = options.Password,
            VirtualHost = options.Vhost,
            Endpoints = new List<EndPoint> { endpoint },
            ClientProvidedName = "streambus-locator",
            ConnectionPoolConfig = new ConnectionPoolConfig
            {
                ProducersPerConnection = (byte)options.Connection.ProducersPerConnection,
                ConsumersPerConnection = (byte)options.Connection.ConsumersPerConnection,
            },
        };
        var system = await StreamSystem.Create(config).ConfigureAwait(false);
        ct.ThrowIfCancellationRequested();
        return new StreamSystemHandle(system);
    }
}
