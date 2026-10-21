using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using RabbitMQ.Stream.Client;

namespace StreamBus.IntegrationTests;

public sealed record PipelineOrder(string Id);

public sealed class PipelineTenantFilter : IStreamPublishFilter
{
    public Task OnPublish(StreamPublishFilterContext context, PublishDelegate next, CancellationToken ct)
    {
        context.Headers.Set("tenant", "acme");
        return next(context);
    }
}

public sealed class PublishPipelineIntegrationTests : IAsyncDisposable
{
    public PublishPipelineIntegrationTests() => IntegrationLane.RequireBroker();

    private static string User() => Environment.GetEnvironmentVariable("STREAMBUS_USER") ?? "app";
    private static string Password() => Environment.GetEnvironmentVariable("STREAMBUS_PASSWORD") ?? "password";
    private readonly List<string> _streams = new();

    private static async Task<StreamSystem> ConnectAdminAsync()
        => await StreamSystem.Create(new StreamSystemConfig
        {
            UserName = User(),
            Password = Password(),
            VirtualHost = "/",
            Endpoints = new List<System.Net.EndPoint> { new System.Net.IPEndPoint(System.Net.IPAddress.Loopback, 5552) },
        });

    private static string Unique(string prefix) => $"{prefix}-{Guid.NewGuid():N}";

    public async ValueTask DisposeAsync()
    {
        var admin = await ConnectAdminAsync();
        try
        {
            foreach (var stream in _streams)
            {
                try { await admin.DeleteStream(stream); } catch { }
            }
        }
        finally { await admin.DisposeAsync(); }
    }

    [Fact]
    public async Task Publish_filter_headers_are_visible_to_the_consumer()
    {
        var ct = TestContext.Current.CancellationToken;
        var stream = Unique("pipe");
        await using var admin = await ConnectAdminAsync();
        await admin.CreateStream(new StreamSpec(stream));
        _streams.Add(stream);

        var services = new ServiceCollection();
        services.AddLogging();
        services.AddStreamBus((_, cfg) =>
        {
            cfg.Host("localhost", "/", h => h.Credentials(User(), Password()));
            cfg.Publish<PipelineOrder>(stream);
            cfg.UsePublishFilter<PipelineTenantFilter>();
        });
        await using var sp = services.BuildServiceProvider();
        var hosted = sp.GetServices<IHostedService>().OfType<StreamBusHostedService>().Single();
        await hosted.StartAsync(ct);
        try
        {
            var bus = sp.GetRequiredService<IStreamBus>();
            Assert.Equal(BusReadiness.Ready, await bus.WaitUntilReadyAsync(
                ReadinessPolicy.UpTo(TimeSpan.FromSeconds(30)), ct));
            await bus.PublishAsync(new PipelineOrder("1"), ct);

            var seen = new TaskCompletionSource<string?>(TaskCreationOptions.RunContinuationsAsynchronously);
            var consumer = await admin.CreateRawConsumer(new RawConsumerConfig(stream)
            {
                Reference = $"pipe-probe-{Guid.NewGuid():N}",
                OffsetSpec = new OffsetTypeFirst(),
                MessageHandler = (_, _, message) =>
                {
                    seen.TrySetResult(message.ApplicationProperties?["tenant"]?.ToString());
                    return Task.CompletedTask;
                },
            });
            try
            {
                var tenant = await seen.Task.WaitAsync(TimeSpan.FromSeconds(30), ct);
                Assert.Equal("acme", tenant);
            }
            finally { consumer.Dispose(); }
        }
        finally { await hosted.StopAsync(CancellationToken.None); }
    }
}
