using System.Diagnostics;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Diagnostics.HealthChecks;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using RabbitMQ.Stream.Client;

namespace StreamBus.UnitTests;

public sealed class HostingTests
{
    private sealed class OrdersBus;
    private sealed class BillingBus;

    private sealed class StubHandle : ScriptedSystemHandle
    {
        public StubHandle()
        {
            StreamExistsFunc = (_, _) => Task.FromResult(false);
            SuperStreamExistsFunc = (_, _) => Task.FromResult(false);
        }

        public Func<string, CancellationToken, Task<bool>>? SuperExistsFunc
        {
            get => SuperStreamExistsFunc;
            set => SuperStreamExistsFunc = value;
        }

        public List<string> CreatedStreams => CreatedStreamNames;
        public List<string> CreatedSupers => CreatedSuperNames;
        public override Task<IStreamPlainProducerHandle> CreatePlainProducerAsync(string stream, CancellationToken ct)
            => throw new NotImplementedException("HostingTests never publishes.");
        public override Task<IStreamPlainProducerHandle> CreatePlainProducerWithReferenceAsync(string stream, string reference, CancellationToken ct)
            => throw new NotImplementedException("HostingTests never publishes.");
        public override Task<IStreamSuperProducerHandle> CreateSuperStreamProducerAsync(string stream, CancellationToken ct)
            => throw new NotImplementedException("HostingTests never publishes.");
        public override Task<IStreamSuperProducerHandle> CreateSuperStreamProducerWithReferenceAsync(string stream, string reference, CancellationToken ct)
            => throw new NotImplementedException("StubHandle never creates stable super producers.");
        public override Task<ulong> QuerySequenceAsync(string reference, string stream, CancellationToken ct)
            => throw new NotImplementedException("StubHandle never queries publishing sequences.");
        public override Task<ulong?> TryQueryOffsetAsync(string reference, string stream, CancellationToken ct)
            => throw new NotImplementedException("HostingTests never queries offsets.");
        public override Task StoreSystemOffsetAsync(string reference, string stream, ulong offset, CancellationToken ct)
            => throw new NotImplementedException("HostingTests never queries offsets.");
        public override Task<string[]> QueryPartitionAsync(string superStream, CancellationToken ct)
            => Task.FromResult(Array.Empty<string>());
        public override Task<RawConsumer> CreateRawConsumerAsync(RawConsumerConfig config, CancellationToken ct)
            => throw new NotImplementedException("HostingTests never consumes.");
    }

    private sealed class CountingHandle : StrictScriptedSystemHandle
    {
        public int DisposedCount;
        public bool ThrowOnDispose;
        public override bool IsClosed => DisposedCount > 0;
        public override ValueTask DisposeAsync()
        {
            var dispose = base.DisposeAsync();
            Interlocked.Increment(ref DisposedCount);
            if (ThrowOnDispose) throw new InvalidOperationException("scripted supervisor dispose failure");
            return dispose;
        }
    }

    private sealed class SuccessConnector : IStreamSystemConnector
    {
        private readonly IStreamSystemHandle _handle;
        public SuccessConnector(IStreamSystemHandle? handle = null) => _handle = handle ?? new StubHandle();
        public Task<IStreamSystemHandle> ConnectAsync(StreamBusOptions options, CancellationToken ct) => Task.FromResult(_handle);
    }

    private sealed class NeverReadyConnector : IStreamSystemConnector
    {
        public async Task<IStreamSystemHandle> ConnectAsync(StreamBusOptions options, CancellationToken ct)
        {
            await Task.Delay(Timeout.InfiniteTimeSpan, ct).ConfigureAwait(false);
            throw new OperationCanceledException(ct);
        }
    }

    private sealed class CapturingLoggerProvider : ILoggerProvider
    {
        public readonly ListLogger Logger = new();
        public ILogger CreateLogger(string categoryName) => Logger;
        public void Dispose() { }
    }

    private sealed class FatalConnector : IStreamSystemConnector
    {
        public Task<IStreamSystemHandle> ConnectAsync(StreamBusOptions options, CancellationToken ct)
            => Task.FromException<IStreamSystemHandle>(new GenericProtocolException(ResponseCode.InternalError, "fatal"));
    }

    private static StreamBusOptions ValidOptions() => new()
    {
        Vhost = "/",
        UserName = "app",
        Password = "secret",
    };

    [Fact]
    public void Same_vhost_different_password_throws_naming_vhost()
    {
        var services = new ServiceCollection();
        services.AddStreamBus((_, cfg) => cfg.Host("localhost", "/", h => h.Credentials("app", "secret")));
        var ex = Assert.Throws<StreamBusConfigurationException>(() =>
            services.AddStreamBus<OrdersBus>((_, cfg) => cfg.Host("localhost", "/", h => h.Credentials("app", "other-secret"))));
        Assert.Contains("/", ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void Same_vhost_registrations_coexist_with_global_hosting()
    {
        var services = new ServiceCollection();
        services.AddStreamBus((_, cfg) => cfg.Host("localhost", "/", h => h.Credentials("app", "secret")));
        services.AddStreamBus<OrdersBus>((_, cfg) => cfg.Host("localhost", "/", h => h.Credentials("app", "secret")));
        services.AddStreamBusHosting(h => h.BlockStartupUntilReady(TimeSpan.FromSeconds(5)));
        var sp = services.BuildServiceProvider();
        Assert.Equal("secret", sp.GetRequiredService<StreamBusConfigStore>().ForVhost("/").Password);
        Assert.Equal(TimeSpan.FromSeconds(5), sp.GetRequiredService<StreamBusHostingStore>().Options.StartupTimeout);
    }

    [Fact]
    public void AddStreamBusHosting_second_call_throws()
    {
        var services = new ServiceCollection();
        services.AddStreamBusHosting(h => h.BlockStartupUntilReady(TimeSpan.FromSeconds(5)));
        Assert.Throws<StreamBusConfigurationException>(() =>
            services.AddStreamBusHosting(h => h.BlockStartupUntilReady(TimeSpan.FromSeconds(5))));
    }

    [Fact]
    public void AddStreamBusHosting_null_arguments_throw()
    {
        var services = new ServiceCollection();
        Assert.Throws<ArgumentNullException>(() => ((IServiceCollection)null!).AddStreamBusHosting(h => h.BlockStartupUntilReady()));
        Assert.Throws<ArgumentNullException>(() => services.AddStreamBusHosting(null!));
    }

    [Fact]
    public void Duplicate_default_registration_fails()
    {
        var services = new ServiceCollection();
        services.AddStreamBus((_, cfg) => cfg.Host("localhost", "/", h => h.Credentials("app", "secret")));
        Assert.Throws<StreamBusConfigurationException>(() =>
            services.AddStreamBus((_, cfg) => cfg.Host("localhost", "/", h => h.Credentials("app", "secret"))));
    }

    [Fact]
    public void Duplicate_marker_registration_fails_but_distinct_markers_coexist()
    {
        var services = new ServiceCollection();
        services.AddStreamBus<OrdersBus>((_, cfg) => cfg.Host("localhost", "/", h => h.Credentials("app", "secret")));
        Assert.Throws<StreamBusConfigurationException>(() =>
            services.AddStreamBus<OrdersBus>((_, cfg) => cfg.Host("localhost", "/", h => h.Credentials("app", "secret"))));
        services.AddStreamBus<BillingBus>((_, cfg) => cfg.Host("localhost", "/", h => h.Credentials("app", "secret")));
        var sp = services.BuildServiceProvider();
        Assert.NotSame(sp.GetRequiredService<IStreamBus<OrdersBus>>(), sp.GetRequiredService<IStreamBus<BillingBus>>());
    }

    [Fact]
    public void Default_and_marker_resolve_distinct_buses()
    {
        var services = new ServiceCollection();
        services.AddStreamBus((_, cfg) => cfg.Host("localhost", "/", h => h.Credentials("app", "secret")));
        services.AddStreamBus<OrdersBus>((_, cfg) => cfg.Host("localhost", "/", h => h.Credentials("app", "secret")));
        var sp = services.BuildServiceProvider();
        Assert.NotSame(sp.GetRequiredService<IStreamBus>(), (IStreamBus)sp.GetRequiredService<IStreamBus<OrdersBus>>());
    }

    [Fact]
    public void Pooling_defaults_are_one_and_overridable()
    {
        var services = new ServiceCollection();
        services.AddStreamBus((_, cfg) =>
        {
            cfg.Host("localhost", "/", h =>
            {
                h.Credentials("app", "secret");
                h.ProducersPerConnection(3);
                h.ConsumersPerConnection(5);
            });
        });
        var sp = services.BuildServiceProvider();
        var opts = sp.GetRequiredService<StreamBusConfigStore>().ForVhost("/");
        Assert.Equal(3, opts.Connection.ProducersPerConnection);
        Assert.Equal(5, opts.Connection.ConsumersPerConnection);
        var defaults = new StreamBusConnectionOptions();
        Assert.Equal(1, defaults.ProducersPerConnection);
        Assert.Equal(1, defaults.ConsumersPerConnection);
    }

    [Fact]
    public void Duplicate_marker_across_vhosts_fails()
    {
        var services = new ServiceCollection();
        services.AddStreamBus<OrdersBus>((_, cfg) => cfg.Host("localhost", "/", h => h.Credentials("app", "secret")));
        Assert.Throws<StreamBusConfigurationException>(() =>
            services.AddStreamBus<OrdersBus>((_, cfg) => cfg.Host("localhost", "other", h => h.Credentials("app", "secret"))));
    }

    [Fact]
    public void Duplicate_default_across_vhosts_fails()
    {
        var services = new ServiceCollection();
        services.AddStreamBus((_, cfg) => cfg.Host("localhost", "/", h => h.Credentials("app", "secret")));
        Assert.Throws<StreamBusConfigurationException>(() =>
            services.AddStreamBus((_, cfg) => cfg.Host("localhost", "/other", h => h.Credentials("app", "secret"))));
    }

    [Fact]
    public void Unknown_vhost_throws_configuration_naming_vhost()
    {
        var services = new ServiceCollection();
        services.AddStreamBus((_, cfg) => cfg.Host("localhost", "/", h => h.Credentials("app", "secret")));
        var sp = services.BuildServiceProvider();
        var store = sp.GetRequiredService<StreamBusConfigStore>();
        var ex = Assert.Throws<StreamBusConfigurationException>(() => store.ForVhost("missing-vh"));
        Assert.Contains("missing-vh", ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void AddStreamBus_null_arguments_throw()
    {
        var services = new ServiceCollection();
        Action<IStreamRegistrationContext, IStreamBusConfigurator> configure = (_, cfg) => cfg.Host("localhost", "/", h => h.Credentials("a", "b"));
        Assert.Throws<ArgumentNullException>(() => services.AddStreamBus(null!));
        Assert.Throws<ArgumentNullException>(() => services.AddStreamBus<OrdersBus>(null!));
        Assert.Throws<ArgumentNullException>(() => ((IServiceCollection)null!).AddStreamBus(configure));
        Assert.Throws<ArgumentNullException>(() => ((IServiceCollection)null!).AddStreamBus<OrdersBus>(configure));
    }

    [Fact]
    public void Pool_upper_bound_rejected_with_named_limit()
    {
        var producersOver = new StreamBusConnectionOptions { ProducersPerConnection = 256, ConsumersPerConnection = 1 };
        Assert.Contains(producersOver.Validate(), e => e.Contains("ProducersPerConnection", StringComparison.Ordinal) && e.Contains("255", StringComparison.Ordinal));
        var consumersOver = new StreamBusConnectionOptions { ProducersPerConnection = 1, ConsumersPerConnection = 1000 };
        Assert.Contains(consumersOver.Validate(), e => e.Contains("ConsumersPerConnection", StringComparison.Ordinal) && e.Contains("255", StringComparison.Ordinal));
        Assert.Throws<StreamBusConfigurationException>(() =>
            new StreamBusConnectionSupervisor(
                new StreamBusOptions { Vhost = "/", UserName = "app", Password = "secret", Connection = producersOver },
                new NeverReadyConnector()));
    }

    [Fact]
    public async Task FailFast_reports_fatal_after_terminal_error()
    {
        await using var sup = new StreamBusConnectionSupervisor(ValidOptions(), new FatalConnector());
        await sup.StartAsync(CancellationToken.None);
        var readiness = await sup.WaitUntilReadyAsync(ReadinessPolicy.Indefinitely, new CancellationTokenSource(TimeSpan.FromSeconds(5)).Token);
        Assert.Equal(BusReadiness.Fatal, readiness);
        Assert.Equal(BusReadiness.Fatal, await sup.WaitUntilReadyAsync(ReadinessPolicy.FailFastPolicy, TestContext.Current.CancellationToken));
    }

    [Theory]
    [InlineData(0)]
    [InlineData(-1)]
    public async Task WaitUpTo_non_positive_returns_timed_out_immediately(int milliseconds)
    {
        await using var sup = new StreamBusConnectionSupervisor(ValidOptions(), new NeverReadyConnector());
        await sup.StartAsync(CancellationToken.None);
        var readiness = await sup.WaitUntilReadyAsync(ReadinessPolicy.UpTo(TimeSpan.FromMilliseconds(milliseconds)), TestContext.Current.CancellationToken);
        Assert.Equal(BusReadiness.TimedOut, readiness);
    }

    [Fact]
    public void BlockUntilConsumersAttached_defaults_true_and_can_be_disabled()
    {
        var defaults = new StreamBusHostingOptionsBuilder().Build();
        Assert.True(defaults.BlockUntilConsumersAttached);
        Assert.False(defaults.BlockStartupUntilReady);

        var enabled = new StreamBusHostingOptionsBuilder().BlockUntilConsumersAttached().Build();
        Assert.True(enabled.BlockUntilConsumersAttached);
        Assert.True(enabled.BlockStartupUntilReady);

        var disabled = new StreamBusHostingOptionsBuilder()
            .BlockStartupUntilReady()
            .BlockUntilConsumersAttached(false)
            .Build();
        Assert.False(disabled.BlockUntilConsumersAttached);
        Assert.True(disabled.BlockStartupUntilReady);
    }

    [Fact]
    public async Task Empty_registry_reports_degraded_with_hint()
    {
        var registry = new StreamBusRegistry(
            static (string vhost) => throw new InvalidOperationException("no options expected for " + vhost),
            static (StreamBusOptions _) => throw new InvalidOperationException("no supervisor expected"));
        var check = new StreamBusHealthCheck(registry);
        var result = await check.CheckHealthAsync(new HealthCheckContext(), TestContext.Current.CancellationToken);
        Assert.Equal(HealthStatus.Degraded, result.Status);
        Assert.Contains("no buses registered", result.Description, StringComparison.Ordinal);
    }

    private sealed class FailingConnector : IStreamSystemConnector
    {
        private readonly TaskCompletionSource _shutdownObserved = new(TaskCreationOptions.RunContinuationsAsynchronously);
        public Task ShutdownObserved => _shutdownObserved.Task;
        public Task<IStreamSystemHandle> ConnectAsync(StreamBusOptions options, CancellationToken ct)
        {
            ct.Register(() => _shutdownObserved.TrySetResult());
            return Task.FromException<IStreamSystemHandle>(new IOException("broker down"));
        }
    }

    [Fact]
    public async Task StopAsync_disposes_and_deregisters_supervisors_that_never_connected()
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddStreamBus((_, cfg) => cfg.Host("localhost", "/", h => h.Credentials("app", "secret")));
        var connector = new FailingConnector();
        services.AddSingleton<IStreamSystemConnector>(connector);
        await using var sp = services.BuildServiceProvider();
        var hosted = sp.GetServices<IHostedService>().OfType<StreamBusHostedService>().Single();
        var deadline = DateTime.UtcNow + TimeSpan.FromSeconds(5);

        await hosted.StartAsync(TestContext.Current.CancellationToken);
        var registry = sp.GetRequiredService<IStreamBusRegistry>();
        var before = registry.SupervisorFor("/");
        // StartAsync is fire-and-forget; wait for the first failed attempt so
        // the test pins the "supervisor alive but never ready" condition.
        while (before.State != BusConnectionState.TransientFailure)
        {
            Assert.True(DateTime.UtcNow < deadline, $"Supervisor state was {before.State}, expected TransientFailure.");
            await Task.Delay(10, TestContext.Current.CancellationToken);
        }

        await hosted.StopAsync(CancellationToken.None);

        // Dispose ran: the retry loop observed cancellation.
        await connector.ShutdownObserved.WaitAsync(TimeSpan.FromSeconds(5), TestContext.Current.CancellationToken);
        // Deregister ran: the registry no longer hands back the same supervisor.
        Assert.NotSame(before, registry.SupervisorFor("/"));
    }

    [Fact]
    public async Task Blocking_startup_succeeds_when_connector_answers()
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddStreamBus((_, cfg) => cfg.Host("localhost", "/", h => h.Credentials("app", "secret")));
        services.AddStreamBusHosting(h => h.BlockStartupUntilReady(TimeSpan.FromSeconds(5)));
        services.AddSingleton<IStreamSystemConnector>(new SuccessConnector());
        await using var sp = services.BuildServiceProvider();
        var hosted = sp.GetServices<IHostedService>().OfType<StreamBusHostedService>().Single();
        await hosted.StartAsync(TestContext.Current.CancellationToken);
        try
        {
            var bus = sp.GetRequiredService<IStreamBus>();
            Assert.Equal(BusConnectionState.Ready, bus.State);
        }
        finally
        {
            await hosted.StopAsync(CancellationToken.None);
        }
    }

    [Fact]
    public async Task Blocking_startup_timeout_surfaces_timed_out()
    {
        var timeout = TimeSpan.FromMilliseconds(200);
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddStreamBus((_, cfg) => cfg.Host("localhost", "/", h => h.Credentials("app", "secret")));
        services.AddStreamBusHosting(h => h.BlockStartupUntilReady(timeout));
        services.AddSingleton<IStreamSystemConnector>(new NeverReadyConnector());
        await using var sp = services.BuildServiceProvider();
        var hosted = sp.GetServices<IHostedService>().OfType<StreamBusHostedService>().Single();
        try
        {
            var ex = await Assert.ThrowsAsync<StreamBusTimeoutException>(() => hosted.StartAsync(TestContext.Current.CancellationToken));
            Assert.Contains("/", ex.Message, StringComparison.Ordinal);
            Assert.Contains(timeout.ToString(), ex.Message, StringComparison.Ordinal);
        }
        finally
        {
            await hosted.StopAsync(CancellationToken.None);
        }
    }

    // Refutation pin: "BlockUntilConsumersAttached treats rows.Count == 0 as
    // success — startup can pass with zero consumers" is unreachable. The attach-wait
    // loop only runs when BlockStartupUntilReady is true (StartAsync returns early
    // otherwise), and the same StartAsync first awaits Task.WhenAll over the very
    // consumer start tasks whose ConsumeCoreAsync upserts the health row synchronously
    // and only returns after every partition's first attach. So a configured-but-
    // unattached consumer can never let StartAsync finish successfully; rows.Count == 0
    // at the loop can only mean "no typed consumers were registered" (correct success).
    private sealed record PinEvent;

    private sealed class PinConsumer : IConsumer<PinEvent>
    {
        public Task Consume(ConsumeContext<PinEvent> context) => Task.CompletedTask;
    }

    private sealed class NeverAttachingHandle : NeverReadyConsumerSystem
    {
        public override Task<RawConsumer> CreateRawConsumerAsync(RawConsumerConfig config, CancellationToken ct)
            => new TaskCompletionSource<RawConsumer>(TaskCreationOptions.RunContinuationsAsynchronously).Task;
        public override Task<string[]> QueryPartitionAsync(string superStream, CancellationToken ct)
            => Task.FromResult(Array.Empty<string>());
    }

    [Fact]
    public async Task Blocking_startup_times_out_while_a_typed_consumer_is_unattached()
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddStreamBus<OrdersBus>((_, cfg) => cfg.Host("localhost", "/", h => h.Credentials("app", "secret")));
        services.AddStreamBusHosting(h => h.BlockStartupUntilReady(TimeSpan.FromSeconds(1)));
        services.AddSingleton<IStreamSystemConnector>(new SuccessConnector(new NeverAttachingHandle()));
        await using var sp = services.BuildServiceProvider();
        sp.GetRequiredService<StreamBusConsumerStore>().Add(
            typeof(OrdersBus), typeof(PinEvent), typeof(PinConsumer), "orders",
            new ConsumeOptions { Reference = "pin-group" });
        var hosted = sp.GetServices<IHostedService>().OfType<StreamBusHostedService>().Single();
        try
        {
            var startTask = hosted.StartAsync(TestContext.Current.CancellationToken);

            // The consumer's health row is upserted synchronously inside ConsumeCoreAsync
            // (RegistrationTracker constructor), before the attach wait — so the pending
            // start means rows for unattached consumers never read as "registered == 0".
            var registry = sp.GetRequiredService<IStreamBusRegistry>();
            BusRegistrationHealth? row = null;
            var rowDeadline = DateTime.UtcNow + TimeSpan.FromSeconds(5);
            while (row is null)
            {
                Assert.True(DateTime.UtcNow < rowDeadline, "The typed consumer's health row never appeared.");
                row = registry.HealthFor("/").Snapshot().Registrations.FirstOrDefault(r => r.Kind == "consumer");
                if (row is null) await Task.Delay(10, TestContext.Current.CancellationToken);
            }
            Assert.Equal(BusRegistrationState.Attaching, row.State);

            // Startup must NOT pass with the consumer unattached: the pending start
            // task trips the startup budget, it can never return successfully.
            var ex = await Assert.ThrowsAsync<StreamBusTimeoutException>(() => startTask);
            Assert.NotEqual(BusRegistrationState.Attached, row.State);
        }
        finally
        {
            await hosted.StopAsync(CancellationToken.None);
        }
    }

    [Fact]
    public async Task Blocking_startup_with_no_typed_consumers_completes_with_zero_rows()
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddStreamBus((_, cfg) => cfg.Host("localhost", "/", h => h.Credentials("app", "secret")));
        services.AddStreamBusHosting(h => h.BlockStartupUntilReady(TimeSpan.FromSeconds(5)));
        services.AddSingleton<IStreamSystemConnector>(new SuccessConnector());
        await using var sp = services.BuildServiceProvider();
        var hosted = sp.GetServices<IHostedService>().OfType<StreamBusHostedService>().Single();
        try
        {
            await hosted.StartAsync(TestContext.Current.CancellationToken);

            // rows.Count == 0 in the attach-wait loop means "no typed consumers were
            // configured", which is a correct success — there is nothing to wait for.
            var registry = sp.GetRequiredService<IStreamBusRegistry>();
            Assert.DoesNotContain(
                registry.HealthFor("/").Snapshot().Registrations,
                r => r.Kind == "consumer");
            var bus = sp.GetRequiredService<IStreamBus>();
            Assert.Equal(BusConnectionState.Ready, bus.State);
        }
        finally
        {
            await hosted.StopAsync(CancellationToken.None);
        }
    }

    [Fact]
    public async Task Non_blocking_startup_returns_without_waiting()
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddStreamBus((_, cfg) => cfg.Host("localhost", "/", h => h.Credentials("app", "secret")));
        services.AddSingleton<IStreamSystemConnector>(new NeverReadyConnector());
        await using var sp = services.BuildServiceProvider();
        var hosted = sp.GetServices<IHostedService>().OfType<StreamBusHostedService>().Single();
        var sw = Stopwatch.StartNew();
        await hosted.StartAsync(TestContext.Current.CancellationToken);
        try
        {
            sw.Stop();
            Assert.True(sw.Elapsed < TimeSpan.FromSeconds(2), $"StartAsync took {sw.Elapsed}.");
            var bus = sp.GetRequiredService<IStreamBus>();
            Assert.NotEqual(BusConnectionState.Ready, bus.State);
        }
        finally
        {
            await hosted.StopAsync(CancellationToken.None);
        }
    }

    [Fact]
    public async Task Caller_cancellation_propagates_as_canceled_not_timeout()
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddStreamBus((_, cfg) => cfg.Host("localhost", "/", h => h.Credentials("app", "secret")));
        services.AddStreamBusHosting(h => h.BlockStartupUntilReady(TimeSpan.FromSeconds(30)));
        services.AddSingleton<IStreamSystemConnector>(new NeverReadyConnector());
        await using var sp = services.BuildServiceProvider();
        var hosted = sp.GetServices<IHostedService>().OfType<StreamBusHostedService>().Single();
        try
        {
            using var cts = new CancellationTokenSource(TimeSpan.FromMilliseconds(200));
            await Assert.ThrowsAnyAsync<OperationCanceledException>(() => hosted.StartAsync(cts.Token));
        }
        finally
        {
            await hosted.StopAsync(CancellationToken.None);
        }
    }

    [Fact]
    public async Task Stop_during_blocking_startup_rethrows_OCE_not_timeout()
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddStreamBus((_, cfg) => cfg.Host("localhost", "/", h => h.Credentials("app", "secret")));
        services.AddStreamBusHosting(h => h.BlockStartupUntilReady(TimeSpan.FromSeconds(30)));
        services.AddSingleton<IStreamSystemConnector>(new NeverReadyConnector());
        await using var sp = services.BuildServiceProvider();
        var hosted = sp.GetServices<IHostedService>().OfType<StreamBusHostedService>().Single();
        try
        {
            using var stop = new CancellationTokenSource(TimeSpan.FromMilliseconds(200));
            var ex = await Record.ExceptionAsync(() => hosted.StartAsync(stop.Token));
            Assert.IsAssignableFrom<OperationCanceledException>(ex);
            Assert.IsNotType<StreamBusTimeoutException>(ex);
        }
        finally
        {
            await hosted.StopAsync(CancellationToken.None);
        }
    }

    [Fact]
    public async Task Stop_logs_dispose_failures_instead_of_swallowing()
    {
        var provider = new CapturingLoggerProvider();
        var services = new ServiceCollection();
        services.AddLogging(b => b.AddProvider(provider));
        services.AddStreamBus((_, cfg) => cfg.Host("localhost", "/", h => h.Credentials("app", "secret")));
        services.AddSingleton<IStreamSystemConnector>(new SuccessConnector(new CountingHandle { ThrowOnDispose = true }));
        await using var sp = services.BuildServiceProvider();
        var hosted = sp.GetServices<IHostedService>().OfType<StreamBusHostedService>().Single();
        await hosted.StartAsync(TestContext.Current.CancellationToken);

        var registry = sp.GetRequiredService<IStreamBusRegistry>();
        var before = registry.SupervisorFor("/");
        await before.WaitUntilReadyAsync(ReadinessPolicy.Indefinitely, TestContext.Current.CancellationToken);

        await hosted.StopAsync(CancellationToken.None);

        Assert.Contains(provider.Logger.Lines, line =>
            line.Contains("Supervisor dispose did not complete cleanly", StringComparison.Ordinal));

        var after = registry.SupervisorFor("/");
        Assert.NotSame(before, after);
        await after.DisposeAsync();
    }

    [Fact]
    public async Task Healthcheck_pipeline_reports_per_vhost_bus_type_data()
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddStreamBus((_, cfg) => cfg.Host("localhost", "/", h => h.Credentials("app", "secret")));
        services.AddStreamBus<OrdersBus>((_, cfg) => cfg.Host("localhost", "orders-vh", h => h.Credentials("app", "secret")));
        services.AddSingleton<IStreamSystemConnector>(new NeverReadyConnector());
        await using var sp = services.BuildServiceProvider();
        Assert.NotNull(sp.GetRequiredService<StreamBusHealthCheck>());
        Assert.Single(sp.GetServices<IHealthCheck>().OfType<StreamBusHealthCheck>());
        var hosted = sp.GetServices<IHostedService>().OfType<StreamBusHostedService>().Single();
        await hosted.StartAsync(TestContext.Current.CancellationToken);
        try
        {
            var health = sp.GetRequiredService<HealthCheckService>();
            var report = await health.CheckHealthAsync(TestContext.Current.CancellationToken);
            Assert.True(report.Entries.ContainsKey("streambus"));
            var entry = report.Entries["streambus"];
            Assert.Contains(entry.Data.Keys, k => k.StartsWith("bus:/|", StringComparison.Ordinal));
            Assert.Contains(entry.Data.Keys, k => k.StartsWith("bus:orders-vh|", StringComparison.Ordinal));
            Assert.Contains(entry.Data.Keys, k => k.Contains(nameof(DefaultBus), StringComparison.Ordinal));
            Assert.Contains(entry.Data.Keys, k => k.Contains("OrdersBus", StringComparison.Ordinal));
        }
        finally
        {
            await hosted.StopAsync(CancellationToken.None);
        }
    }

    [Fact]
    public async Task StopAsync_disposes_started_systems()
    {
        var handle = new CountingHandle();
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddStreamBus((_, cfg) => cfg.Host("localhost", "/", h => h.Credentials("app", "secret")));
        services.AddSingleton<IStreamSystemConnector>(new SuccessConnector(handle));
        await using var sp = services.BuildServiceProvider();
        var hosted = sp.GetServices<IHostedService>().OfType<StreamBusHostedService>().Single();
        await hosted.StartAsync(TestContext.Current.CancellationToken);
        Assert.Equal(0, handle.DisposedCount);
        await hosted.StopAsync(CancellationToken.None);
        Assert.Equal(1, handle.DisposedCount);
    }

    [Fact]
    public async Task Startup_declares_union_of_registrations()
    {
        var ct = TestContext.Current.CancellationToken;
        var handle = new StubHandle();
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddStreamBus((_, cfg) =>
        {
            cfg.Host("localhost", "/", h => h.Credentials("app", "secret"));
            cfg.Publish<DerivedEvent>("stream-a");
        });
        services.AddStreamBus<OrdersBus>((_, cfg) =>
        {
            cfg.Host("localhost", "/", h => h.Credentials("app", "secret"));
            cfg.Publish<DerivedEvent>("stream-b");
        });
        services.AddSingleton<IStreamSystemConnector>(new SuccessConnector(handle));
        await using var sp = services.BuildServiceProvider();
        var hosted = sp.GetServices<IHostedService>().OfType<StreamBusHostedService>().Single();
        await hosted.StartAsync(ct);
        try
        {
            var deadline = DateTime.UtcNow + TimeSpan.FromSeconds(5);
            while (true)
            {
                List<string> snapshot;
                lock (handle.CreatedStreams) snapshot = handle.CreatedStreams.ToList();
                if (snapshot.Contains("stream-a") && snapshot.Contains("stream-b")) break;
                if (DateTime.UtcNow >= deadline) break;
                await Task.Delay(TimeSpan.FromMilliseconds(50), ct);
            }
            List<string> created;
            lock (handle.CreatedStreams) created = handle.CreatedStreams.ToList();
            Assert.Contains("stream-a", created);
            Assert.Contains("stream-b", created);
        }
        finally
        {
            await hosted.StopAsync(CancellationToken.None);
        }
    }

    [Fact]
    public void Same_vhost_union_registrations_allowed()
    {
        var store = new StreamBusConfigStore();
        var a = new StreamBusOptions { Vhost = "/", UserName = "u", Password = "p" };
        var b = new StreamBusOptions { Vhost = "/", UserName = "u", Password = "p" };
        store.Add(typeof(DefaultBus), a);
        store.Add(typeof(OrdersBus), b);
        Assert.Equal(2, store.Registrations.Count);
    }

    [Fact]
    public void Registration_view_exposes_connection_metadata_without_credentials()
    {
        var store = new StreamBusConfigStore();
        store.Add(typeof(DefaultBus), new StreamBusOptions
        {
            Vhost = "/",
            Host = "broker.internal",
            UserName = "app",
            Password = "secret",
            StreamPort = 5553,
            AmqpPort = 5673,
            Heartbeat = TimeSpan.FromSeconds(30),
        });

        var view = Assert.Single(store.Registrations);
        Assert.Equal("/", view.Vhost);
        Assert.Equal("broker.internal", view.Host);
        Assert.Equal(5553, view.StreamPort);
        Assert.Equal(5673, view.AmqpPort);
        Assert.Equal(30, view.HeartbeatSeconds);
        Assert.DoesNotContain(
            typeof(StreamBusRegistrationView).GetProperties(),
            p => p.Name.Contains("Password", StringComparison.OrdinalIgnoreCase));
    }

    [Fact]
    public async Task Blocking_startup_timeout_names_topology()
    {
        var ct = TestContext.Current.CancellationToken;
        var handle = new StubHandle();
        handle.StreamExistsFunc = async (_, ct2) => { await Task.Delay(Timeout.Infinite, ct2); return false; };
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddStreamBus((_, cfg) =>
        {
            cfg.Host("localhost", "/", h => h.Credentials("app", "secret"));
            cfg.Publish<DerivedEvent>("hung-stream");
        });
        services.AddStreamBusHosting(h => h.BlockStartupUntilReady(TimeSpan.FromMilliseconds(200)));
        services.AddSingleton<IStreamSystemConnector>(new SuccessConnector(handle));
        await using var sp = services.BuildServiceProvider();
        var hosted = sp.GetServices<IHostedService>().OfType<StreamBusHostedService>().Single();
        try
        {
            var ex = await Assert.ThrowsAsync<StreamBusTimeoutException>(() => hosted.StartAsync(ct));
            Assert.Contains("topology", ex.Message, StringComparison.Ordinal);
        }
        finally
        {
            await hosted.StopAsync(CancellationToken.None);
        }
    }

    private sealed record DerivedEvent(string Id);

    private sealed class DerivedConsumer : IConsumer<DerivedEvent>
    {
        public Task Consume(ConsumeContext<DerivedEvent> context) => Task.CompletedTask;
    }

    private sealed class OtherDerivedConsumer : IConsumer<DerivedEvent>
    {
        public Task Consume(ConsumeContext<DerivedEvent> context) => Task.CompletedTask;
    }

    private static IReadOnlyList<DeclaredStream> DeclareSet(
        IEnumerable<ReceiveEndpointDescriptor>? endpoints = null,
        IEnumerable<PublishRegistration>? publishes = null)
        => DeclareSetBuilder.Build(
            endpoints ?? Array.Empty<ReceiveEndpointDescriptor>(),
            publishes ?? Array.Empty<PublishRegistration>(),
            "TestBus");

    private static ReceiveEndpointDescriptor EndpointDescriptor(
        string stream, bool super = false, int? partitions = null, Type? consumer = null, string reference = "grp")
    {
        var consumerType = consumer ?? typeof(DerivedConsumer);
        return new ReceiveEndpointDescriptor
        {
            Stream = stream,
            MessageType = typeof(DerivedEvent),
            ConsumerType = consumerType,
            Reference = reference,
            IsSuperStream = super,
            SuperPartitions = partitions,
        };
    }

    [Fact]
    public void Declare_set_is_consumer_publish_deadletter_union()
    {
        var set = DeclareSet(
            endpoints: new[] { EndpointDescriptor("endpoint-plain"), EndpointDescriptor("endpoint-super", super: true, partitions: 3) },
            publishes: new[] { new PublishRegistration { BusMarker = typeof(DefaultBus), MessageType = typeof(DerivedEvent), Stream = "publish-plain", IsSuper = false } });
        Assert.Equal(3, set.Count);
        Assert.Contains(set, d => d.Name == "endpoint-plain" && !d.IsSuper && d.Source == DeclaredStreamSource.Endpoint);
        Assert.Contains(set, d => d is { Name: "endpoint-super", IsSuper: true, Partitions: 3 } && d.Source == DeclaredStreamSource.Endpoint);
        Assert.Contains(set, d => d.Name == "publish-plain" && !d.IsSuper && d.Source == DeclaredStreamSource.Publish);
        Assert.Contains(set, d => d.Name == "endpoint-plain" && d.Description == "endpoint 'endpoint-plain'");
        Assert.Contains(set, d => d.Name == "publish-plain" && d.Description.StartsWith("publish ", StringComparison.Ordinal));
    }

    [Fact]
    public void Stream_deadletter_destinations_join_the_declare_set()
    {
        var errorEndpoint = new ReceiveEndpointDescriptor
        {
            Stream = "orders",
            MessageType = typeof(DerivedEvent),
            ConsumerType = typeof(DerivedConsumer),
            Reference = "grp",
            DeadLetter = new DeadLetterOptions { Name = "orders-errors", Spec = new StreamOptions() },
        };
        var set = DeclareSet(endpoints: new[] { errorEndpoint });
        Assert.Equal(2, set.Count);
        Assert.Contains(set, d => d.Name == "orders-errors" && !d.IsSuper && d.Source == DeclaredStreamSource.DeadLetter);
        Assert.Contains(set, d => d.Name == "orders" && !d.IsSuper && d.Source == DeclaredStreamSource.Endpoint);
    }

    [Fact]
    public void Endpoint_kind_conflict_throws_naming_stream()
    {
        var ex = Assert.Throws<StreamBusStreamConfigurationConflictException>(() => DeclareSet(
            endpoints: new[]
            {
                EndpointDescriptor("orders"),
                EndpointDescriptor("orders", super: true, partitions: 3),
            }));
        Assert.Contains("orders", ex.Message, StringComparison.Ordinal);
        Assert.Contains("endpoint", ex.Expected, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void Publish_kind_mismatch_throws_naming_type_and_stream()
    {
        var plainSuper = Assert.Throws<StreamBusConfigurationException>(() => DeclareSet(
            endpoints: new[] { EndpointDescriptor("orders-super", super: true, partitions: 3) },
            publishes: new[] { new PublishRegistration { BusMarker = typeof(DefaultBus), MessageType = typeof(DerivedEvent), Stream = "orders-super", IsSuper = false } }));
        Assert.Contains(typeof(DerivedEvent).FullName!, plainSuper.Message, StringComparison.Ordinal);
        Assert.Contains("orders-super", plainSuper.Message, StringComparison.Ordinal);
        Assert.Contains("PublishSuper", plainSuper.Message, StringComparison.Ordinal);

        var superPlain = Assert.Throws<StreamBusConfigurationException>(() => DeclareSet(
            endpoints: new[] { EndpointDescriptor("orders") },
            publishes: new[] { new PublishRegistration { BusMarker = typeof(DefaultBus), MessageType = typeof(DerivedEvent), Stream = "orders", IsSuper = true } }));
        Assert.Contains(typeof(DerivedEvent).FullName!, superPlain.Message, StringComparison.Ordinal);
        Assert.Contains("PublishAsync", superPlain.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void Super_publish_without_partitions_throws_naming_stream()
    {
        var ex = Assert.Throws<StreamBusConfigurationException>(() => DeclareSet(
            publishes: new[] { new PublishRegistration { BusMarker = typeof(DefaultBus), MessageType = typeof(DerivedEvent), Stream = "orders-super", IsSuper = true } }));
        Assert.Contains("orders-super", ex.Message, StringComparison.Ordinal);
        Assert.Contains("Partitions", ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void Same_type_on_two_streams_throws_listing_both_at_startup()
    {
        var ex = Assert.Throws<StreamBusConfigurationException>(() => DeclareSet(
            publishes: new[]
            {
                new PublishRegistration { BusMarker = typeof(DefaultBus), MessageType = typeof(DerivedEvent), Stream = "orders-a", IsSuper = false },
                new PublishRegistration { BusMarker = typeof(DefaultBus), MessageType = typeof(DerivedEvent), Stream = "orders-b", IsSuper = false },
            }));
        Assert.Contains("orders-a", ex.Message, StringComparison.Ordinal);
        Assert.Contains("orders-b", ex.Message, StringComparison.Ordinal);
        Assert.Contains(typeof(DerivedEvent).FullName!, ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void Super_publish_with_endpoint_partitions_is_satisfied()
    {
        var set = DeclareSet(
            endpoints: new[] { EndpointDescriptor("orders-super", super: true, partitions: 3) },
            publishes: new[] { new PublishRegistration { BusMarker = typeof(DefaultBus), MessageType = typeof(DerivedEvent), Stream = "orders-super", IsSuper = true } });
        Assert.DoesNotContain(set, d => d.Source == DeclaredStreamSource.Publish);
        Assert.Contains(set, d => d is { Name: "orders-super", IsSuper: true, Partitions: 3 });
    }

    [Fact]
    public async Task Missing_plain_endpoint_is_declared_at_startup()
    {
        var ct = TestContext.Current.CancellationToken;
        var handle = new StubHandle();
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddStreamBus((_, cfg) =>
        {
            cfg.Host("localhost", "/", h => h.Credentials("app", "secret"));
            cfg.ReceiveEndpoint("derived-plain", e => e.Consumer<DerivedConsumer>("grp"));
        });
        services.AddSingleton<IStreamSystemConnector>(new SuccessConnector(handle));
        await using var sp = services.BuildServiceProvider();
        var hosted = sp.GetServices<IHostedService>().OfType<StreamBusHostedService>().Single();
        await hosted.StartAsync(ct);
        try
        {
            var deadline = DateTime.UtcNow + TimeSpan.FromSeconds(5);
            while (true)
            {
                List<string> snapshot;
                lock (handle.CreatedStreams) snapshot = handle.CreatedStreams.ToList();
                if (snapshot.Contains("derived-plain")) break;
                if (DateTime.UtcNow >= deadline) break;
                await Task.Delay(TimeSpan.FromMilliseconds(50), ct);
            }
            List<string> created;
            lock (handle.CreatedStreams) created = handle.CreatedStreams.ToList();
            Assert.Contains("derived-plain", created);
        }
        finally
        {
            await hosted.StopAsync(CancellationToken.None);
        }
    }

    [Fact]
    public async Task Adopt_existing_super_skips_declare()
    {
        var ct = TestContext.Current.CancellationToken;
        var handle = new StubHandle();
        handle.SuperExistsFunc = (_, _) => Task.FromResult(true);
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddStreamBus((_, cfg) =>
        {
            cfg.Host("localhost", "/", h => h.Credentials("app", "secret"));
            cfg.ReceiveEndpoint("adopted-super", e =>
            {
                e.Consumer<DerivedConsumer>("grp");
                e.SuperStream();
            });
        });
        services.AddStreamBusHosting(h => h.BlockStartupUntilReady(TimeSpan.FromSeconds(30)));
        services.AddSingleton<IStreamSystemConnector>(new SuccessConnector(handle));
        await using var sp = services.BuildServiceProvider();
        var hosted = sp.GetServices<IHostedService>().OfType<StreamBusHostedService>().Single();
        await hosted.StartAsync(ct);
        try
        {
            List<string> supers;
            lock (handle.CreatedSupers) supers = handle.CreatedSupers.ToList();
            Assert.DoesNotContain("adopted-super", supers);
        }
        finally
        {
            await hosted.StopAsync(CancellationToken.None);
        }
    }

    [Fact]
    public async Task Adopt_plain_against_super_expectation_throws_conflict()
    {
        var ct = TestContext.Current.CancellationToken;
        var handle = new StubHandle();
        handle.StreamExistsFunc = (_, _) => Task.FromResult(true);
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddStreamBus((_, cfg) =>
        {
            cfg.Host("localhost", "/", h => h.Credentials("app", "secret"));
            cfg.ReceiveEndpoint("clashing", e =>
            {
                e.Consumer<DerivedConsumer>("grp");
                e.Partitions(3);
            });
        });
        services.AddStreamBusHosting(h => h.BlockStartupUntilReady(TimeSpan.FromSeconds(30)));
        services.AddSingleton<IStreamSystemConnector>(new SuccessConnector(handle));
        await using var sp = services.BuildServiceProvider();
        var hosted = sp.GetServices<IHostedService>().OfType<StreamBusHostedService>().Single();
        try
        {
            var ex = await Assert.ThrowsAsync<StreamBusStreamConfigurationConflictException>(() => hosted.StartAsync(ct));
            Assert.Contains("clashing", ex.Message, StringComparison.Ordinal);
        }
        finally
        {
            await hosted.StopAsync(CancellationToken.None);
        }
    }

    [Fact]
    public async Task Missing_super_without_partitions_throws_naming_stream()
    {
        var ct = TestContext.Current.CancellationToken;
        var handle = new StubHandle();
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddStreamBus((_, cfg) =>
        {
            cfg.Host("localhost", "/", h => h.Credentials("app", "secret"));
            cfg.ReceiveEndpoint("partitionless-super", e =>
            {
                e.Consumer<DerivedConsumer>("grp");
                e.SuperStream();
            });
        });
        services.AddStreamBusHosting(h => h.BlockStartupUntilReady(TimeSpan.FromSeconds(5)));
        services.AddSingleton<IStreamSystemConnector>(new SuccessConnector(handle));
        await using var sp = services.BuildServiceProvider();
        var hosted = sp.GetServices<IHostedService>().OfType<StreamBusHostedService>().Single();
        try
        {
            var ex = await Assert.ThrowsAsync<StreamBusConfigurationException>(() => hosted.StartAsync(ct));
            Assert.Contains("partitionless-super", ex.Message, StringComparison.Ordinal);
        }
        finally
        {
            await hosted.StopAsync(CancellationToken.None);
        }
    }
}
