using Microsoft.Extensions.Diagnostics.HealthChecks;
using RabbitMQ.Stream.Client;

namespace StreamBus.UnitTests;

file sealed class HealthSystemHandle : NeverReadyConsumerSystem
{
    public override Task<RawConsumer> CreateRawConsumerAsync(RawConsumerConfig config, CancellationToken ct)
        => throw new NotSupportedException("HealthAggregatorTests inject their own consumer factory.");
}

file sealed class HealthConnector : IStreamSystemConnector
{
    private readonly HealthSystemHandle _handle;
    public HealthConnector(HealthSystemHandle handle) => _handle = handle;
    public Task<IStreamSystemHandle> ConnectAsync(StreamBusOptions options, CancellationToken ct)
        => Task.FromResult<IStreamSystemHandle>(_handle);
}

public sealed class HealthAggregatorTests
{
    private sealed class OrdersBus;

    private sealed class BillingBus;

    private static StreamBusHealthAggregator Ready()
    {
        var aggregator = new StreamBusHealthAggregator();
        aggregator.SetConnection(BusConnectionState.Ready);
        return aggregator;
    }

    [Fact]
    public void Consumer_failed_registration_maps_to_degraded()
    {
        var aggregator = Ready();
        aggregator.UpsertRegistration("/", "orders", "consumer", BusRegistrationState.Attached);
        aggregator.UpsertRegistration("/", "payments", "consumer", BusRegistrationState.FailedRetrying, "consumer_connection_closed");

        Assert.Equal(StreamBusHealthAggregator.HealthStatus.Degraded, aggregator.Compute());
    }

    [Fact]
    public void All_failed_after_startup_maps_to_unhealthy()
    {
        var aggregator = Ready();
        aggregator.UpsertRegistration("/", "orders", "consumer", BusRegistrationState.FailedRetrying, "consumer_watchdog");
        aggregator.UpsertRegistration("/", "payments", "consumer", BusRegistrationState.FailedRetrying, "consumer_connection_closed");

        Assert.Equal(StreamBusHealthAggregator.HealthStatus.Unhealthy, aggregator.Compute());
    }

    [Fact]
    public void Declare_failure_row_marks_degraded_until_exhausted_then_unhealthy()
    {
        var aggregator = Ready();
        aggregator.UpsertRegistration("/", "orders", "consumer", BusRegistrationState.Attached);
        aggregator.UpsertRegistration("/", "declare:events", "declare", BusRegistrationState.Attaching);

        // An in-flight declaration is startup noise, not degradation.
        Assert.Equal(StreamBusHealthAggregator.HealthStatus.Healthy, aggregator.Compute());

        // Exhaustion is reported as FailedRetrying by the hosting declare loop.
        aggregator.UpsertRegistration("/", "declare:events", "declare", BusRegistrationState.FailedRetrying, "conflict");

        Assert.Equal(StreamBusHealthAggregator.HealthStatus.Unhealthy, aggregator.Compute());
    }

    [Fact]
    public async Task Raw_consumers_are_first_class_registrations()
    {
        var system = new HealthSystemHandle();
        var options = new StreamBusOptions { Vhost = "/", UserName = "app", Password = "secret" };
        await using var supervisor = new StreamBusConnectionSupervisor(options, new HealthConnector(system));
        await supervisor.StartAsync(CancellationToken.None);
        Assert.Equal(BusReadiness.Ready,
            await supervisor.WaitUntilReadyAsync(ReadinessPolicy.Forever, TestContext.Current.CancellationToken));

        var health = new StreamBusHealthAggregator();
        health.SetConnection(supervisor.State);
        var consumers = new StreamConsumerRegistry(
            supervisor, "/",
            new StreamProducerRegistry(supervisor, "/"),
            null,
                        health: health,
            consumerFactory: new ScriptedConsumerFactory().CreateAsync);

        var consumeOptions = new ConsumeOptions
        {
            ConsumerGroup = "grp",
            SingleActiveConsumer = false,
            StartOffset = StartOffset.AlwaysFirst,
        };
        await using var first = await consumers.ConsumeAsync(
            typeof(DefaultBus), "orders", consumeOptions, _ => Task.CompletedTask, TestContext.Current.CancellationToken);
        await using var second = await consumers.ConsumeAsync(
            typeof(DefaultBus), "orders", consumeOptions, _ => Task.CompletedTask, TestContext.Current.CancellationToken);

        var rows = health.Snapshot().Registrations.Where(r => r.Kind == "consumer").ToList();
        Assert.Contains(rows, r => r.Registration == $"raw:{typeof(DefaultBus).FullName}:orders" && r.State == BusRegistrationState.Attached);
        Assert.Contains(rows, r => r.Registration == $"raw:{typeof(DefaultBus).FullName}:orders:2" && r.State == BusRegistrationState.Attached);
        Assert.Equal(StreamBusHealthAggregator.HealthStatus.Healthy, health.Compute());
    }

    [Fact]
    public async Task Raw_consumer_rows_on_a_shared_vhost_name_the_bus_that_owns_them()
    {
        var ct = TestContext.Current.CancellationToken;
        var system = new HealthSystemHandle();
        var options = new StreamBusOptions { Vhost = "/", UserName = "app", Password = "secret" };
        await using var supervisor = new StreamBusConnectionSupervisor(options, new HealthConnector(system));
        await supervisor.StartAsync(ct);
        Assert.Equal(BusReadiness.Ready, await supervisor.WaitUntilReadyAsync(ReadinessPolicy.Forever, ct));

        var health = new StreamBusHealthAggregator();
        health.SetConnection(supervisor.State);
        var consumers = new StreamConsumerRegistry(
            supervisor, "/",
            new StreamProducerRegistry(supervisor, "/"),
            null,
                        health: health,
            consumerFactory: new ScriptedConsumerFactory().CreateAsync);
        var consumeOptions = new ConsumeOptions
        {
            ConsumerGroup = "grp",
            SingleActiveConsumer = false,
            StartOffset = StartOffset.AlwaysFirst,
        };

        await using var orders = await consumers.ConsumeAsync(
            typeof(OrdersBus), "orders", consumeOptions, _ => Task.CompletedTask, ct);
        await using var billing = await consumers.ConsumeAsync(
            typeof(BillingBus), "orders", consumeOptions, _ => Task.CompletedTask, ct);

        var rows = health.Snapshot().Registrations.Where(r => r.Kind == "consumer").ToList();
        Assert.Contains(rows, r => r.Registration == $"raw:{typeof(OrdersBus).FullName}:orders" && r.State == BusRegistrationState.Attached);
        Assert.Contains(rows, r => r.Registration == $"raw:{typeof(BillingBus).FullName}:orders" && r.State == BusRegistrationState.Attached);
    }

    [Fact]
    public async Task Declared_endpoint_rows_name_the_bus_that_owns_them()
    {
        var ct = TestContext.Current.CancellationToken;
        var system = new HealthSystemHandle();
        var options = new StreamBusOptions { Vhost = "/", UserName = "app", Password = "secret" };
        await using var supervisor = new StreamBusConnectionSupervisor(options, new HealthConnector(system));
        await supervisor.StartAsync(ct);
        Assert.Equal(BusReadiness.Ready, await supervisor.WaitUntilReadyAsync(ReadinessPolicy.Forever, ct));

        var health = new StreamBusHealthAggregator();
        health.SetConnection(supervisor.State);
        var consumers = new StreamConsumerRegistry(
            supervisor, "/",
            new StreamProducerRegistry(supervisor, "/"),
            null,
                        health: health,
            consumerFactory: new ScriptedConsumerFactory().CreateAsync);
        var consumeOptions = new ConsumeOptions
        {
            ConsumerGroup = "grp",
            SingleActiveConsumer = false,
            StartOffset = StartOffset.AlwaysFirst,
        };

        await using var endpoint = await consumers.ConsumeDeclaredEndpointAsync(
            typeof(OrdersBus), "orders", consumeOptions, _ => Task.CompletedTask, ct);

        var rows = health.Snapshot().Registrations.Where(r => r.Kind == "consumer").ToList();
        Assert.Contains(rows, r => r.Registration == $"endpoint:{typeof(OrdersBus).FullName}:orders" && r.State == BusRegistrationState.Attached);
    }

    [Fact]
    public async Task Health_check_description_lists_failed_rows_with_reason()
    {
        var registry = new StreamBusRegistry(
            static _ => throw new InvalidOperationException("no options expected"),
            static _ => throw new InvalidOperationException("no supervisor expected"));
        var health = registry.HealthFor("/");
        health.SetConnection(BusConnectionState.Ready);
        health.UpsertRegistration("/", "orders-attached", "consumer", BusRegistrationState.Attached);
        health.UpsertRegistration("/", "orders", "consumer", BusRegistrationState.FailedRetrying, "consumer_watchdog");

        var check = new StreamBusHealthCheck(registry);
        var result = await check.CheckHealthAsync(new HealthCheckContext(), TestContext.Current.CancellationToken);

        Assert.Equal(HealthStatus.Degraded, result.Status);
        Assert.Contains("orders", result.Description, StringComparison.Ordinal);
        Assert.Contains("consumer_watchdog", result.Description, StringComparison.Ordinal);
    }

    [Fact]
    public async Task Health_check_survives_registration_name_colliding_with_a_bus_marker()
    {
        var options = new StreamBusOptions { Vhost = "/", UserName = "app", Password = "secret" };
        await using var supervisor = new StreamBusConnectionSupervisor(options, new HealthConnector(new HealthSystemHandle()));
        var registry = new StreamBusRegistry(
            _ => options,
            _ => supervisor);

        // A bus state row (bus marker) plus a registration row whose free-form
        // name happens to equal the bus type's full name on the same vhost.
        _ = registry.GetOrCreateBus("/");
        var health = registry.HealthFor("/");
        health.SetConnection(BusConnectionState.Ready);
        health.UpsertRegistration("/", typeof(DefaultBus).FullName!, "consumer", BusRegistrationState.Attached);

        var check = new StreamBusHealthCheck(registry);
        var result = await check.CheckHealthAsync(new HealthCheckContext(), TestContext.Current.CancellationToken);

        // Both rows survive under their own namespaces; the check does not throw.
        Assert.Equal(HealthStatus.Healthy, result.Status);
        Assert.True(result.Data.ContainsKey($"bus:/|{typeof(DefaultBus).FullName}"), "bus marker row missing");
        Assert.True(result.Data.ContainsKey($"reg:/|{typeof(DefaultBus).FullName}"), "registration row missing");
        Assert.Equal("consumer:Attached", result.Data[$"reg:/|{typeof(DefaultBus).FullName}"]);
    }

    [Fact]
    public void Suppression_after_stopped_is_healthy()
    {
        var aggregator = Ready();
        aggregator.UpsertRegistration("/", "orders", "consumer", BusRegistrationState.Stopped);
        aggregator.UpsertRegistration("/", "payments", "consumer", BusRegistrationState.Stopped);

        Assert.Equal(StreamBusHealthAggregator.HealthStatus.Healthy, aggregator.Compute());
    }
}
