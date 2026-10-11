using System.Net;
using RabbitMQ.Stream.Client;

namespace StreamBus;

public interface IStreamSystemHandle : IAsyncDisposable
{
    bool IsClosed { get; }
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
    public ValueTask DisposeAsync() => _system.DisposeAsync();
}

public sealed class StreamSystemConnector : IStreamSystemConnector
{
    public async Task<IStreamSystemHandle> ConnectAsync(StreamBusOptions options, CancellationToken ct)
    {
        ArgumentNullException.ThrowIfNull(options);
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
