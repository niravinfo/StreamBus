using Microsoft.Extensions.Diagnostics.HealthChecks;
using RabbitMQ.Stream.Client;

namespace StreamBus.UnitTests;

file sealed class HealthSystemHandle : IStreamSystemHandle
{
    public bool IsClosed => false;
    public Task<bool> StreamExistsAsync(string stream, CancellationToken ct) => Task.FromResult(true);
    public Task<bool> SuperStreamExistsAsync(string superStream, CancellationToken ct) => Task.FromResult(false);
    public Task<string[]> QueryPartitionAsync(string superStream, CancellationToken ct) => Task.FromResult(new[] { superStream });
    public Task<RawConsumer> CreateRawConsumerAsync(RawConsumerConfig config, CancellationToken ct)
        => throw new NotSupportedException("HealthAggregatorTests inject their own consumer factory.");
    public Task<ulong?> TryQueryOffsetAsync(string reference, string stream, CancellationToken ct) => Task.FromResult<ulong?>(null);
    public Task StoreSystemOffsetAsync(string reference, string stream, ulong offset, CancellationToken ct) => Task.CompletedTask;
    public Task<IStreamPlainProducerHandle> CreatePlainProducerAsync(string stream, CancellationToken ct) => throw new NotSupportedException();
    public Task<IStreamPlainProducerHandle> CreatePlainProducerWithReferenceAsync(string stream, string reference, CancellationToken ct) => throw new NotSupportedException();
    public Task<IStreamSuperProducerHandle> CreateSuperStreamProducerAsync(string stream, CancellationToken ct) => throw new NotSupportedException();
    public Task<IStreamSuperProducerHandle> CreateSuperStreamProducerWithReferenceAsync(string stream, string reference, CancellationToken ct) => throw new NotSupportedException();
    public Task<ulong> QuerySequenceAsync(string reference, string stream, CancellationToken ct) => throw new NotSupportedException();
    public Task CreateStreamAsync(StreamSpec spec, CancellationToken ct) => Task.CompletedTask;
    public Task CreateSuperStreamAsync(PartitionsSuperStreamSpec spec, CancellationToken ct) => Task.CompletedTask;
    public ValueTask DisposeAsync() => ValueTask.CompletedTask;
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
            await supervisor.WaitUntilReadyAsync(ReadinessPolicy.Indefinitely, TestContext.Current.CancellationToken));

        var health = new StreamBusHealthAggregator();
        health.SetConnection(supervisor.State);
        var consumers = new StreamConsumerRegistry(
            supervisor, "/",
            new StreamProducerRegistry(supervisor, "/"),
            null,
            new ResultProducerRegistry(supervisor, "/"),
            health: health,
            consumerFactory: new ScriptedConsumerFactory().CreateAsync);

        var consumeOptions = new ConsumeOptions
        {
            Reference = "grp",
            SingleActiveConsumer = false,
            Offset = ConsumeOffset.AlwaysFirst,
        };
        await using var first = await consumers.ConsumeAsync(
            typeof(DefaultBus), "orders", consumeOptions, _ => Task.CompletedTask, TestContext.Current.CancellationToken);
        await using var second = await consumers.ConsumeAsync(
            typeof(DefaultBus), "orders", consumeOptions, _ => Task.CompletedTask, TestContext.Current.CancellationToken);

        var rows = health.Snapshot().Registrations.Where(r => r.Kind == "consumer").ToList();
        Assert.Contains(rows, r => r.Registration == "raw:orders" && r.State == BusRegistrationState.Attached);
        Assert.Contains(rows, r => r.Registration == "raw:orders:2" && r.State == BusRegistrationState.Attached);
        Assert.Equal(StreamBusHealthAggregator.HealthStatus.Healthy, health.Compute());
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
    public void Suppression_after_stopped_is_healthy()
    {
        var aggregator = Ready();
        aggregator.UpsertRegistration("/", "orders", "consumer", BusRegistrationState.Stopped);
        aggregator.UpsertRegistration("/", "payments", "consumer", BusRegistrationState.Stopped);

        Assert.Equal(StreamBusHealthAggregator.HealthStatus.Healthy, aggregator.Compute());
    }
}
