using System.Net;
using DotNet.Testcontainers.Builders;
using RabbitMQ.Stream.Client;
using Testcontainers.RabbitMq;

namespace StreamBus.IntegrationTests;

/// <summary>
/// Lazily starts one RabbitMQ Testcontainers broker per test process and hands
/// out its connection details.
/// </summary>
/// <remarks>
/// The stream plugins cannot be enabled via <c>WithCommand</c>: that replaces the
/// image's <c>rabbitmq-server</c> CMD and the container exits. Instead the server
/// starts, then <c>rabbitmq-plugins enable rabbitmq_stream rabbitmq_stream_management</c>
/// runs via container exec and a readiness poll gates the returned handle. The
/// management plugin exposes the <c>api/stream/*</c> endpoints the chaos tests use.
/// Cleanup is left to <c>WithCleanUp(true)</c> (Ryuk reaps the container when the
/// test process exits).
/// </remarks>
internal static class TestBroker
{
    private static Task<BrokerHandle>? _start;

    public static Task<BrokerHandle> StartAsync(CancellationToken ct = default)
    {
        var task = _start;
        if (task is not null)
        {
            return task;
        }

        lock (typeof(TestBroker))
        {
            _start ??= StartCoreAsync();
            return _start;
        }
    }

    /// <summary>
    /// Picks a free TCP port on the loopback interface for the stream listener's
    /// host-side binding (and matching advertised port).
    /// </summary>
    private static int GetFreeHostPort()
    {
        var listener = new System.Net.Sockets.TcpListener(System.Net.IPAddress.Loopback, 0);
        listener.Start();
        try
        {
            return ((System.Net.IPEndPoint)listener.LocalEndpoint).Port;
        }
        finally
        {
            listener.Stop();
        }
    }

    private static async Task<BrokerHandle> StartCoreAsync()
    {
        // The stream listener must be advertised on its mapped host port: the
        // .NET stream client connects to producers/consumers via the endpoint
        // the broker advertises in metadata, so with a random Testcontainers
        // port mapping the advertised port has to equal the host-side port.
        // It is pre-allocated here (still random, so it cannot collide with a
        // broker already holding the default ports) and handed to both the
        // port binding and the stream plugin's advertised_port.
        var streamHostPort = GetFreeHostPort();
        var container = new RabbitMqBuilder("rabbitmq:4.2-management")
            .WithUsername("app")
            .WithPassword("password")
            .WithPortBinding(streamHostPort, 5552)
            .WithPortBinding(15672, true)
            .WithEnvironment("RABBITMQ_SERVER_ADDITIONAL_ERL_ARGS",
                $"-rabbitmq_stream advertised_host localhost advertised_port {streamHostPort}")
            .WithCleanUp(true)
            .Build();

        await container.StartAsync();

        // rabbitmq_stream_management is a separate plugin from rabbitmq_stream:
        // without it the api/stream/* management endpoints answer HTTP 400/404,
        // which the chaos tests use to find and kill stream connections.
        await container.ExecAsync(new[]
        {
            "rabbitmq-plugins",
            "enable",
            "rabbitmq_stream",
            "rabbitmq_stream_management",
        });

        var handle = new BrokerHandle(
            container.Hostname,
            streamHostPort,
            container.GetMappedPublicPort(5672),
            container.GetMappedPublicPort(15672),
            "app",
            "password");

        await WaitForStreamListenerAsync(handle);
        return handle;
    }

    /// <summary>
    /// Polls the stream listener (up to 60 s) by opening a real client connection;
    /// plugin enablement is asynchronous, so the port may not accept connections
    /// as soon as the exec call returns.
    /// </summary>
    private static async Task WaitForStreamListenerAsync(BrokerHandle handle)
    {
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(60));
        while (true)
        {
            try
            {
                await using var system = await StreamSystem.Create(new StreamSystemConfig
                {
                    Endpoints = new List<EndPoint>
                    {
                        new IPEndPoint(IPAddress.Parse(handle.Host), handle.StreamPort),
                    },
                    UserName = handle.Username,
                    Password = handle.Password,
                    VirtualHost = "/",
                });
                BrokerProbe.StreamExists(system, "readiness-probe");
                return;
            }
            catch (OperationCanceledException)
            {
                throw new TimeoutException(
                    $"RabbitMQ stream listener did not accept connections within 60s on {handle.Host}:{handle.StreamPort}.");
            }
            catch (Exception)
            {
                await Task.Delay(500, timeout.Token);
            }
        }
    }

    public sealed record BrokerHandle(
        string Host,
        int StreamPort,
        int AmqpPort,
        int ManagementPort,
        string Username,
        string Password);
}
