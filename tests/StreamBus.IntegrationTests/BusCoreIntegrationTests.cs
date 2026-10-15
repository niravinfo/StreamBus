using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Diagnostics.HealthChecks;
using Microsoft.Extensions.Hosting;

namespace StreamBus.IntegrationTests;

public sealed class BusCoreIntegrationTests
{
    private static string User() => Environment.GetEnvironmentVariable("STREAMBUS_USER") ?? "app";
    private static string Password() => Environment.GetEnvironmentVariable("STREAMBUS_PASSWORD") ?? "password";

    [Fact]
    public async Task Single_vhost_bus_becomes_ready()
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddStreamBus((_, cfg) => cfg.Host("127.0.0.1", "/", h => h.Credentials(User(), Password())));
        await using var sp = services.BuildServiceProvider();
        var bus = sp.GetRequiredService<IStreamBus>();
        var hosted = sp.GetServices<IHostedService>().OfType<StreamBusHostedService>().Single();
        await hosted.StartAsync(CancellationToken.None);
        try
        {
            var readiness = await bus.WaitUntilReadyAsync(ReadinessPolicy.UpTo(TimeSpan.FromSeconds(30)), TestContext.Current.CancellationToken);
            Assert.Equal(BusReadiness.Ready, readiness);
        }
        finally
        {
            await hosted.StopAsync(CancellationToken.None);
        }
    }

    [Fact]
    public async Task Health_reports_per_vhost_bus_type_state()
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddStreamBus((_, cfg) => cfg.Host("127.0.0.1", "/", h => h.Credentials(User(), Password())));
        await using var sp = services.BuildServiceProvider();
        var hosted = sp.GetServices<IHostedService>().OfType<StreamBusHostedService>().Single();
        await hosted.StartAsync(CancellationToken.None);
        try
        {
            var bus = sp.GetRequiredService<IStreamBus>();
            Assert.Equal(BusReadiness.Ready, await bus.WaitUntilReadyAsync(ReadinessPolicy.UpTo(TimeSpan.FromSeconds(30)), TestContext.Current.CancellationToken));
            var check = sp.GetServices<IHealthCheck>().OfType<StreamBusHealthCheck>().Single();
            var result = await check.CheckHealthAsync(new HealthCheckContext(), CancellationToken.None);
            Assert.Equal(HealthStatus.Healthy, result.Status);
            Assert.Contains(result.Data.Keys, k => k.StartsWith("/|"));
        }
        finally
        {
            await hosted.StopAsync(CancellationToken.None);
        }
    }

    [Fact]
    public async Task Unreachable_broker_never_surfaces_terminal_error_and_recovers()
    {
        var options = new StreamBusOptions
        {
            Vhost = "/",
            UserName = User(),
            Password = Password(),
            Host = "127.0.0.1",
            StreamPort = 59999,
        };
        await using var supervisor = new StreamBusConnectionSupervisor(options, new StreamSystemConnector());
        await supervisor.StartAsync(CancellationToken.None);
        var early = await supervisor.WaitUntilReadyAsync(ReadinessPolicy.UpTo(TimeSpan.FromSeconds(2)), TestContext.Current.CancellationToken);
        Assert.Equal(BusReadiness.TimedOut, early);
        Assert.True(supervisor.State is BusConnectionState.Starting or BusConnectionState.TransientFailure);
    }
}
