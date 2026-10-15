using System.Diagnostics;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Diagnostics.HealthChecks;
using Microsoft.Extensions.Hosting;
using RabbitMQ.Stream.Client;

namespace StreamBus.UnitTests;

public sealed class HostingTests
{
    private sealed class OrdersBus;
    private sealed class BillingBus;

    private sealed class StubHandle : IStreamSystemHandle
    {
        public readonly List<string> CreatedStreams = new();
        public readonly List<string> CreatedSupers = new();
        public Func<string, CancellationToken, Task<bool>> StreamExistsFunc = (_, _) => Task.FromResult(false);
        public Func<string, CancellationToken, Task<bool>> SuperExistsFunc = (_, _) => Task.FromResult(false);
        public bool IsClosed => false;
        public ValueTask DisposeAsync() => ValueTask.CompletedTask;
        public Task<IStreamProducerHandle> CreatePlainProducerAsync(string stream, CancellationToken ct)
            => throw new NotImplementedException("HostingTests never publishes.");
        public Task<IStreamProducerHandle> CreatePlainProducerWithReferenceAsync(string stream, string reference, CancellationToken ct)
            => throw new NotImplementedException("HostingTests never publishes.");
        public Task<IStreamProducerHandle> CreateSuperStreamProducerAsync(string stream, CancellationToken ct)
            => throw new NotImplementedException("HostingTests never publishes.");
        public Task<IStreamProducerHandle> CreateSuperStreamProducerWithReferenceAsync(string stream, string reference, CancellationToken ct)
            => throw new NotImplementedException("StubHandle never creates stable super producers.");
        public Task<ulong> QuerySequenceAsync(string reference, string stream, CancellationToken ct)
            => throw new NotImplementedException("StubHandle never queries publishing sequences.");
        public Task<ulong?> TryQueryOffsetAsync(string reference, string stream, CancellationToken ct)
            => throw new NotImplementedException("HostingTests never queries offsets.");
        public Task StoreSystemOffsetAsync(string reference, string stream, ulong offset, CancellationToken ct)
            => throw new NotImplementedException("HostingTests never queries offsets.");
        public Task<bool> StreamExistsAsync(string stream, CancellationToken ct)
            => StreamExistsFunc(stream, ct);
        public Task<bool> SuperStreamExistsAsync(string superStream, CancellationToken ct)
            => SuperExistsFunc(superStream, ct);
        public Task<string[]> QueryPartitionAsync(string superStream, CancellationToken ct)
            => Task.FromResult(Array.Empty<string>());
        public Task<RawConsumer> CreateRawConsumerAsync(RawConsumerConfig config, CancellationToken ct)
            => throw new NotImplementedException("HostingTests never consumes.");
        public Task CreateStreamAsync(StreamSpec spec, CancellationToken ct)
        {
            lock (CreatedStreams) CreatedStreams.Add(spec.Name);
            return Task.CompletedTask;
        }
        public Task CreateSuperStreamAsync(PartitionsSuperStreamSpec spec, CancellationToken ct)
        {
            lock (CreatedSupers) CreatedSupers.Add(spec.Name);
            return Task.CompletedTask;
        }
    }

    private sealed class CountingHandle : IStreamSystemHandle
    {
        public int DisposedCount;
        public bool IsClosed => DisposedCount > 0;
        public ValueTask DisposeAsync()
        {
            Interlocked.Increment(ref DisposedCount);
            return ValueTask.CompletedTask;
        }
        public Task<IStreamProducerHandle> CreatePlainProducerAsync(string stream, CancellationToken ct)
            => throw new NotImplementedException("HostingTests never publishes.");
        public Task<IStreamProducerHandle> CreatePlainProducerWithReferenceAsync(string stream, string reference, CancellationToken ct)
            => throw new NotImplementedException("HostingTests never publishes.");
        public Task<IStreamProducerHandle> CreateSuperStreamProducerAsync(string stream, CancellationToken ct)
            => throw new NotImplementedException("HostingTests never publishes.");
        public Task<IStreamProducerHandle> CreateSuperStreamProducerWithReferenceAsync(string stream, string reference, CancellationToken ct)
            => throw new NotImplementedException("CountingHandle never creates stable super producers.");
        public Task<ulong> QuerySequenceAsync(string reference, string stream, CancellationToken ct)
            => throw new NotImplementedException("CountingHandle never queries publishing sequences.");
        public Task<ulong?> TryQueryOffsetAsync(string reference, string stream, CancellationToken ct)
            => throw new NotImplementedException("HostingTests never queries offsets.");
        public Task StoreSystemOffsetAsync(string reference, string stream, ulong offset, CancellationToken ct)
            => throw new NotImplementedException("HostingTests never queries offsets.");
        public Task<bool> StreamExistsAsync(string stream, CancellationToken ct)
            => throw new NotImplementedException("HostingTests never queries offsets.");
        public Task<bool> SuperStreamExistsAsync(string superStream, CancellationToken ct)
            => throw new NotImplementedException("HostingTests never queries offsets.");
        public Task<string[]> QueryPartitionAsync(string superStream, CancellationToken ct)
            => throw new NotImplementedException("HostingTests never queries offsets.");
        public Task<RawConsumer> CreateRawConsumerAsync(RawConsumerConfig config, CancellationToken ct)
            => throw new NotImplementedException("HostingTests never consumes.");
        public Task CreateStreamAsync(StreamSpec spec, CancellationToken ct)
            => throw new NotImplementedException("HostingTests never declares topology.");
        public Task CreateSuperStreamAsync(PartitionsSuperStreamSpec spec, CancellationToken ct)
            => throw new NotImplementedException("HostingTests never declares topology.");
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
        services.AddStreamBus((_, cfg) => cfg.Host("127.0.0.1", "/", h => h.Credentials("app", "secret")));
        var ex = Assert.Throws<StreamBusConfigurationException>(() =>
            services.AddStreamBus<OrdersBus>((_, cfg) => cfg.Host("127.0.0.1", "/", h => h.Credentials("app", "other-secret"))));
        Assert.Contains("/", ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void Same_vhost_registrations_coexist_with_global_hosting()
    {
        var services = new ServiceCollection();
        services.AddStreamBus((_, cfg) => cfg.Host("127.0.0.1", "/", h => h.Credentials("app", "secret")));
        services.AddStreamBus<OrdersBus>((_, cfg) => cfg.Host("127.0.0.1", "/", h => h.Credentials("app", "secret")));
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
        services.AddStreamBus((_, cfg) => cfg.Host("127.0.0.1", "/", h => h.Credentials("app", "secret")));
        Assert.Throws<StreamBusConfigurationException>(() =>
            services.AddStreamBus((_, cfg) => cfg.Host("127.0.0.1", "/", h => h.Credentials("app", "secret"))));
    }

    [Fact]
    public void Duplicate_marker_registration_fails_but_distinct_markers_coexist()
    {
        var services = new ServiceCollection();
        services.AddStreamBus<OrdersBus>((_, cfg) => cfg.Host("127.0.0.1", "/", h => h.Credentials("app", "secret")));
        Assert.Throws<StreamBusConfigurationException>(() =>
            services.AddStreamBus<OrdersBus>((_, cfg) => cfg.Host("127.0.0.1", "/", h => h.Credentials("app", "secret"))));
        services.AddStreamBus<BillingBus>((_, cfg) => cfg.Host("127.0.0.1", "/", h => h.Credentials("app", "secret")));
        var sp = services.BuildServiceProvider();
        Assert.NotSame(sp.GetRequiredService<IStreamBus<OrdersBus>>(), sp.GetRequiredService<IStreamBus<BillingBus>>());
    }

    [Fact]
    public void Default_and_marker_resolve_distinct_buses()
    {
        var services = new ServiceCollection();
        services.AddStreamBus((_, cfg) => cfg.Host("127.0.0.1", "/", h => h.Credentials("app", "secret")));
        services.AddStreamBus<OrdersBus>((_, cfg) => cfg.Host("127.0.0.1", "/", h => h.Credentials("app", "secret")));
        var sp = services.BuildServiceProvider();
        Assert.NotSame(sp.GetRequiredService<IStreamBus>(), (IStreamBus)sp.GetRequiredService<IStreamBus<OrdersBus>>());
    }

    [Fact]
    public void Pooling_defaults_are_one_and_overridable()
    {
        var services = new ServiceCollection();
        services.AddStreamBus((_, cfg) =>
        {
            cfg.Host("127.0.0.1", "/", h =>
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
        services.AddStreamBus<OrdersBus>((_, cfg) => cfg.Host("127.0.0.1", "/", h => h.Credentials("app", "secret")));
        Assert.Throws<StreamBusConfigurationException>(() =>
            services.AddStreamBus<OrdersBus>((_, cfg) => cfg.Host("127.0.0.1", "other", h => h.Credentials("app", "secret"))));
    }

    [Fact]
    public void Duplicate_default_across_vhosts_fails()
    {
        var services = new ServiceCollection();
        services.AddStreamBus((_, cfg) => cfg.Host("127.0.0.1", "/", h => h.Credentials("app", "secret")));
        Assert.Throws<StreamBusConfigurationException>(() =>
            services.AddStreamBus((_, cfg) => cfg.Host("127.0.0.1", "/other", h => h.Credentials("app", "secret"))));
    }

    [Fact]
    public void Unknown_vhost_throws_configuration_naming_vhost()
    {
        var services = new ServiceCollection();
        services.AddStreamBus((_, cfg) => cfg.Host("127.0.0.1", "/", h => h.Credentials("app", "secret")));
        var sp = services.BuildServiceProvider();
        var store = sp.GetRequiredService<StreamBusConfigStore>();
        var ex = Assert.Throws<StreamBusConfigurationException>(() => store.ForVhost("missing-vh"));
        Assert.Contains("missing-vh", ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void AddStreamBus_null_arguments_throw()
    {
        var services = new ServiceCollection();
        Action<IStreamRegistrationContext, IStreamBusConfigurator> configure = (_, cfg) => cfg.Host("127.0.0.1", "/", h => h.Credentials("a", "b"));
        Assert.Throws<ArgumentNullException>(() => services.AddStreamBus(null!));
        Assert.Throws<ArgumentNullException>(() => services.AddStreamBus<OrdersBus>(null!));
        Assert.Throws<ArgumentNullException>(() => ((IServiceCollection)null!).AddStreamBus(configure));
        Assert.Throws<ArgumentNullException>(() => ((IServiceCollection)null!).AddStreamBus<OrdersBus>(configure));
        Assert.Throws<ArgumentNullException>(() => services.AddStreamBus((Microsoft.Extensions.Configuration.IConfigurationSection)null!));
        Assert.Throws<ArgumentNullException>(() => services.AddStreamBus<OrdersBus>((Microsoft.Extensions.Configuration.IConfigurationSection)null!));
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

    [Fact]
    public async Task Blocking_startup_succeeds_when_connector_answers()
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddStreamBus((_, cfg) => cfg.Host("127.0.0.1", "/", h => h.Credentials("app", "secret")));
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
        services.AddStreamBus((_, cfg) => cfg.Host("127.0.0.1", "/", h => h.Credentials("app", "secret")));
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

    [Fact]
    public async Task Non_blocking_startup_returns_without_waiting()
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddStreamBus((_, cfg) => cfg.Host("127.0.0.1", "/", h => h.Credentials("app", "secret")));
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
        services.AddStreamBus((_, cfg) => cfg.Host("127.0.0.1", "/", h => h.Credentials("app", "secret")));
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
    public async Task Healthcheck_pipeline_reports_per_vhost_bus_type_data()
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddStreamBus((_, cfg) => cfg.Host("127.0.0.1", "/", h => h.Credentials("app", "secret")));
        services.AddStreamBus<OrdersBus>((_, cfg) => cfg.Host("127.0.0.1", "orders-vh", h => h.Credentials("app", "secret")));
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
            Assert.Contains(entry.Data.Keys, k => k.StartsWith("/|", StringComparison.Ordinal));
            Assert.Contains(entry.Data.Keys, k => k.StartsWith("orders-vh|", StringComparison.Ordinal));
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
        services.AddStreamBus((_, cfg) => cfg.Host("127.0.0.1", "/", h => h.Credentials("app", "secret")));
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
            cfg.Host("127.0.0.1", "/", h => h.Credentials("app", "secret"));
            cfg.DeclareStream("stream-a");
        });
        services.AddStreamBus<OrdersBus>((_, cfg) =>
        {
            cfg.Host("127.0.0.1", "/", h => h.Credentials("app", "secret"));
            cfg.DeclareStream("stream-b");
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
    public void Guest_at_registration_fails_with_actionable_message()
    {
        var services = new ServiceCollection();
        services.AddLogging();
        var ex = Assert.Throws<StreamBusConfigurationException>(() =>
            services.AddStreamBus((_, cfg) => cfg.Host("127.0.0.1", "/", h => h.Credentials("guest", "guest"))));
        Assert.Contains("guest", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void Same_vhost_union_declarations_allowed()
    {
        var store = new StreamBusConfigStore();
        var a = new StreamBusOptions { Vhost = "/", UserName = "u", Password = "p", Topology = new StreamTopologyDeclarations { Streams = new[] { new StreamDeclaration { Name = "a" } } } };
        var b = new StreamBusOptions { Vhost = "/", UserName = "u", Password = "p", Topology = new StreamTopologyDeclarations { SuperStreams = new[] { new SuperStreamDeclaration { Name = "b", Partitions = 2 } } } };
        store.Add(typeof(DefaultBus), a);
        store.Add(typeof(OrdersBus), b);
        Assert.Equal(2, store.Registrations.Count);
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
            cfg.Host("127.0.0.1", "/", h => h.Credentials("app", "secret"));
            cfg.DeclareStream("hung-stream");
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
        public Task Consume(ConsumeContext<DerivedEvent> context, CancellationToken ct) => Task.CompletedTask;
    }

    private sealed class OtherDerivedConsumer : IConsumer<DerivedEvent>
    {
        public Task Consume(ConsumeContext<DerivedEvent> context, CancellationToken ct) => Task.CompletedTask;
    }

    private static IReadOnlyList<DeclaredStream> DeclareSet(
        IEnumerable<StreamDeclaration>? streams = null,
        IEnumerable<SuperStreamDeclaration>? supers = null,
        IEnumerable<ReceiveEndpointDescriptor>? endpoints = null,
        IEnumerable<PublishRegistration>? publishes = null)
        => DeclareSetBuilder.Build(
            streams ?? Array.Empty<StreamDeclaration>(),
            supers ?? Array.Empty<SuperStreamDeclaration>(),
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
    public void Declare_set_is_consumer_publish_explicit_union()
    {
        var set = DeclareSet(
            streams: new[] { new StreamDeclaration { Name = "explicit-plain" } },
            supers: new[] { new SuperStreamDeclaration { Name = "explicit-super", Partitions = 2 } },
            endpoints: new[] { EndpointDescriptor("endpoint-plain"), EndpointDescriptor("endpoint-super", super: true, partitions: 3) },
            publishes: new[] { new PublishRegistration { BusMarker = typeof(DefaultBus), MessageType = typeof(DerivedEvent), Stream = "publish-plain", IsSuper = false } });
        Assert.Equal(5, set.Count);
        Assert.Contains(set, d => d.Name == "explicit-plain" && !d.IsSuper && d.Source == "explicit");
        Assert.Contains(set, d => d is { Name: "explicit-super", IsSuper: true, Partitions: 2 } && d.Source == "explicit");
        Assert.Contains(set, d => d.Name == "endpoint-plain" && !d.IsSuper && d.Source == "endpoint");
        Assert.Contains(set, d => d is { Name: "endpoint-super", IsSuper: true, Partitions: 3 } && d.Source == "endpoint");
        Assert.Contains(set, d => d.Name == "publish-plain" && !d.IsSuper && d.Source == "publish");
    }

    [Fact]
    public void Explicit_declaration_wins_over_derived()
    {
        var set = DeclareSet(
            streams: new[] { new StreamDeclaration { Name = "orders" } },
            endpoints: new[] { EndpointDescriptor("orders") });
        var match = Assert.Single(set);
        Assert.Equal("explicit", match.Source);
    }

    [Fact]
    public void Derived_super_plain_mismatch_against_explicit_throws_naming_sources()
    {
        var ex = Assert.Throws<StreamConfigurationConflictException>(() => DeclareSet(
            streams: new[] { new StreamDeclaration { Name = "orders" } },
            endpoints: new[] { EndpointDescriptor("orders", super: true, partitions: 3) }));
        Assert.Contains("orders", ex.Message, StringComparison.Ordinal);
        Assert.Contains("endpoint", ex.Expected, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void Publish_kind_mismatch_throws_naming_type_and_stream()
    {
        var plainSuper = Assert.Throws<StreamBusConfigurationException>(() => DeclareSet(
            supers: new[] { new SuperStreamDeclaration { Name = "orders-super", Partitions = 3 } },
            publishes: new[] { new PublishRegistration { BusMarker = typeof(DefaultBus), MessageType = typeof(DerivedEvent), Stream = "orders-super", IsSuper = false } }));
        Assert.Contains(typeof(DerivedEvent).FullName!, plainSuper.Message, StringComparison.Ordinal);
        Assert.Contains("orders-super", plainSuper.Message, StringComparison.Ordinal);
        Assert.Contains("PublishSuper", plainSuper.Message, StringComparison.Ordinal);

        var superPlain = Assert.Throws<StreamBusConfigurationException>(() => DeclareSet(
            streams: new[] { new StreamDeclaration { Name = "orders" } },
            publishes: new[] { new PublishRegistration { BusMarker = typeof(DefaultBus), MessageType = typeof(DerivedEvent), Stream = "orders", IsSuper = true } }));
        Assert.Contains(typeof(DerivedEvent).FullName!, superPlain.Message, StringComparison.Ordinal);
        Assert.Contains("PublishAsync", superPlain.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void Super_publish_without_declaration_throws_naming_stream()
    {
        var ex = Assert.Throws<StreamBusConfigurationException>(() => DeclareSet(
            publishes: new[] { new PublishRegistration { BusMarker = typeof(DefaultBus), MessageType = typeof(DerivedEvent), Stream = "orders-super", IsSuper = true } }));
        Assert.Contains("orders-super", ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void Super_publish_with_endpoint_partitions_is_satisfied()
    {
        var set = DeclareSet(
            endpoints: new[] { EndpointDescriptor("orders-super", super: true, partitions: 3) },
            publishes: new[] { new PublishRegistration { BusMarker = typeof(DefaultBus), MessageType = typeof(DerivedEvent), Stream = "orders-super", IsSuper = true } });
        Assert.DoesNotContain(set, d => d.Source == "publish");
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
            cfg.Host("127.0.0.1", "/", h => h.Credentials("app", "secret"));
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
            cfg.Host("127.0.0.1", "/", h => h.Credentials("app", "secret"));
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
            cfg.Host("127.0.0.1", "/", h => h.Credentials("app", "secret"));
            cfg.ReceiveEndpoint("clashing", e =>
            {
                e.Consumer<DerivedConsumer>("grp");
                e.SetSuperStream(3);
            });
        });
        services.AddStreamBusHosting(h => h.BlockStartupUntilReady(TimeSpan.FromSeconds(30)));
        services.AddSingleton<IStreamSystemConnector>(new SuccessConnector(handle));
        await using var sp = services.BuildServiceProvider();
        var hosted = sp.GetServices<IHostedService>().OfType<StreamBusHostedService>().Single();
        try
        {
            var ex = await Assert.ThrowsAsync<StreamConfigurationConflictException>(() => hosted.StartAsync(ct));
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
            cfg.Host("127.0.0.1", "/", h => h.Credentials("app", "secret"));
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
