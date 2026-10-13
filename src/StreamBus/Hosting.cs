using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Diagnostics.HealthChecks;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

namespace StreamBus;

public sealed class StreamBusConfigStore
{
    private readonly HashSet<Type> _busTypes = new();
    private readonly Dictionary<string, StreamBusOptions> _byVhost = new();
    private readonly List<(Type BusType, StreamBusOptions Options)> _registrations = new();

    public void Add(Type busType, StreamBusOptions options)
    {
        ArgumentNullException.ThrowIfNull(busType);
        ArgumentNullException.ThrowIfNull(options);
        if (!_busTypes.Add(busType))
            throw new StreamBusConfigurationException($"Duplicate StreamBus registration for {busType.FullName}. The non-generic overload may be used at most once per service and each marker TBus at most once.");
        if (_byVhost.TryGetValue(options.Vhost, out var existing) && !SameConnection(existing, options))
            throw new StreamBusConfigurationException($"Conflicting StreamBus connection options for vhost '{options.Vhost}'. Registrations sharing a vhost must agree on connection identity (user, password, host, port, connection pool); first registration wins.");
        _byVhost.TryAdd(options.Vhost, options);
        _registrations.Add((busType, options));
    }

    private static bool SameConnection(StreamBusOptions a, StreamBusOptions b)
        => a.Vhost == b.Vhost
            && a.UserName == b.UserName
            && a.Password == b.Password
            && a.Host == b.Host
            && a.Port == b.Port
            && a.Connection == b.Connection;

    public StreamBusOptions ForVhost(string vhost)
    {
        if (_byVhost.TryGetValue(vhost, out var options))
            return options;
        throw new StreamBusConfigurationException($"No StreamBus registration for vhost '{vhost}'.");
    }

    public IReadOnlyList<(Type BusType, StreamBusOptions Options)> Registrations => _registrations;
}

public static class ServiceCollectionExtensions
{
    public static IServiceCollection AddStreamBus(this IServiceCollection services, string vhost, Action<StreamBusOptionsBuilder> configure)
    {
        ArgumentNullException.ThrowIfNull(services);
        ArgumentNullException.ThrowIfNull(vhost);
        ArgumentNullException.ThrowIfNull(configure);
        var builder = new StreamBusOptionsBuilder(vhost);
        configure(builder);
        var options = builder.Build();
        AddShared(services, typeof(DefaultBus), options);
        services.TryAddSingleton<IStreamBus>(sp => sp.GetRequiredService<IStreamBusRegistry>().GetOrCreateBus(options.Vhost));
        return services;
    }

    public static IServiceCollection AddStreamBus<TBus>(this IServiceCollection services, string vhost, Action<StreamBusOptionsBuilder> configure)
    {
        ArgumentNullException.ThrowIfNull(services);
        ArgumentNullException.ThrowIfNull(vhost);
        ArgumentNullException.ThrowIfNull(configure);
        var builder = new StreamBusOptionsBuilder(vhost);
        configure(builder);
        var options = builder.Build();
        AddShared(services, typeof(TBus), options);
        services.TryAddSingleton(typeof(IStreamBus<TBus>), sp => sp.GetRequiredService<IStreamBusRegistry>().GetOrCreateBus<TBus>(options.Vhost));
        return services;
    }

    private static IServiceCollection AddShared(IServiceCollection services, Type busType, StreamBusOptions options)
    {
        var store = GetOrCreateStore(services);
        store.Add(busType, options);
        services.TryAddSingleton<IStreamSystemConnector>(sp => new StreamSystemConnector(sp.GetService<ILoggerFactory>()));
        services.TryAddSingleton<IStreamBusRegistry>(sp =>
            new StreamBusRegistry(
                store.ForVhost,
                opts => new StreamBusConnectionSupervisor(opts, sp.GetRequiredService<IStreamSystemConnector>()),
                sp.GetService<ILoggerFactory>()));
        services.TryAddSingleton<IHostedService, StreamBusHostedService>();
        services.TryAddSingleton<StreamBusHealthCheck>();
        services.TryAddSingleton<IHealthCheck>(sp => sp.GetRequiredService<StreamBusHealthCheck>());
        if (!services.Any(d => d.ServiceType == typeof(HealthCheckRegistrationMarker)))
        {
            services.AddSingleton(new HealthCheckRegistrationMarker());
            services.AddHealthChecks().AddCheck<StreamBusHealthCheck>("streambus");
        }
        return services;
    }

    private sealed class HealthCheckRegistrationMarker
    {
    }

    private static StreamBusConfigStore GetOrCreateStore(IServiceCollection services)
    {
        var existing = services.FirstOrDefault(d =>
            d.ServiceType == typeof(StreamBusConfigStore) && d.ImplementationInstance is StreamBusConfigStore);
        if (existing?.ImplementationInstance is StreamBusConfigStore store) return store;
        var created = new StreamBusConfigStore();
        services.AddSingleton<StreamBusConfigStore>(created);
        return created;
    }
}

public sealed class StreamBusOptionsBuilder
{
    private string _user = "";
    private string _password = "";
    private string _host = "127.0.0.1";
    private int _port = 5552;
    private StreamBusConnectionOptions _connection = new();
    private StreamBusHostOptions _hosting = new();
    private StreamTopologyDeclarations _topology = new();
    private readonly string _vhost;

    public StreamBusOptionsBuilder(string vhost) => _vhost = vhost;
    public StreamBusOptionsBuilder Credentials(string user, string password) { _user = user; _password = password; return this; }
    public StreamBusOptionsBuilder Endpoint(string host, int port) { _host = host; _port = port; return this; }
    public StreamBusOptionsBuilder Connection(Action<StreamBusConnectionOptionsBuilder> configure)
    {
        var b = new StreamBusConnectionOptionsBuilder();
        configure(b);
        _connection = b.Build();
        return this;
    }
    public StreamBusOptionsBuilder Hosting(Action<StreamBusHostOptionsBuilder> configure)
    {
        var b = new StreamBusHostOptionsBuilder();
        configure(b);
        _hosting = b.Build();
        return this;
    }
    public StreamBusOptionsBuilder Topology(Action<StreamTopologyDeclarationsBuilder> configure)
    {
        ArgumentNullException.ThrowIfNull(configure);
        var b = new StreamTopologyDeclarationsBuilder();
        configure(b);
        _topology = b.Build();
        return this;
    }
    public StreamBusOptions Build() => new()
    {
        Vhost = _vhost,
        UserName = _user,
        Password = _password,
        Host = _host,
        Port = _port,
        Connection = _connection,
        Hosting = _hosting,
        Topology = _topology,
    };
}

public sealed class StreamBusConnectionOptionsBuilder
{
    private int _producers = 1;
    private int _consumers = 1;
    public StreamBusConnectionOptionsBuilder ProducersPerConnection(int n) { _producers = n; return this; }
    public StreamBusConnectionOptionsBuilder ConsumersPerConnection(int n) { _consumers = n; return this; }
    public StreamBusConnectionOptions Build() => new() { ProducersPerConnection = _producers, ConsumersPerConnection = _consumers };
}

public sealed class StreamBusHostOptionsBuilder
{
    private bool _block;
    private TimeSpan? _timeout;
    public StreamBusHostOptionsBuilder BlockStartupUntilReady(TimeSpan? timeout = null) { _block = true; _timeout = timeout; return this; }
    public StreamBusHostOptions Build() => new() { BlockStartupUntilReady = _block, StartupTimeout = _timeout };
}

public sealed class StreamTopologyDeclarationsBuilder
{
    private readonly List<StreamDeclaration> _streams = new();
    private readonly List<SuperStreamDeclaration> _supers = new();

    public StreamTopologyDeclarationsBuilder DeclareStream(string name, Action<StreamDeclarationKnobsBuilder>? configure = null)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(name);
        var knobs = new StreamDeclarationKnobsBuilder();
        configure?.Invoke(knobs);
        _streams.Add(knobs.BuildStream(name));
        return this;
    }

    public StreamTopologyDeclarationsBuilder DeclareSuperStream(string name, int partitions, Action<StreamDeclarationKnobsBuilder>? configure = null)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(name);
        var knobs = new StreamDeclarationKnobsBuilder();
        configure?.Invoke(knobs);
        _supers.Add(knobs.BuildSuper(name, partitions));
        return this;
    }

    public StreamTopologyDeclarations Build()
    {
        var built = new StreamTopologyDeclarations { Streams = _streams.ToList(), SuperStreams = _supers.ToList() };
        foreach (var s in built.Streams) TopologyValidation.ValidateStream(s);
        foreach (var s in built.SuperStreams) TopologyValidation.ValidateSuper(s);
        return built;
    }
}

public sealed class StreamDeclarationKnobsBuilder
{
    private TimeSpan? _maxAge;
    private ulong? _maxLengthBytes;
    private int? _maxSegmentSizeBytes;
    private StreamLeaderLocator _locator = StreamLeaderLocator.LeastLeaders;

    public StreamDeclarationKnobsBuilder MaxAge(TimeSpan maxAge) { _maxAge = maxAge; return this; }
    public StreamDeclarationKnobsBuilder MaxLengthBytes(ulong maxLengthBytes) { _maxLengthBytes = maxLengthBytes; return this; }
    public StreamDeclarationKnobsBuilder MaxSegmentSizeBytes(int maxSegmentSizeBytes) { _maxSegmentSizeBytes = maxSegmentSizeBytes; return this; }
    public StreamDeclarationKnobsBuilder LeaderLocator(StreamLeaderLocator locator) { _locator = locator; return this; }

    internal StreamDeclaration BuildStream(string name) => new()
    {
        Name = name, MaxAge = _maxAge, MaxLengthBytes = _maxLengthBytes,
        MaxSegmentSizeBytes = _maxSegmentSizeBytes, LeaderLocator = _locator,
    };

    internal SuperStreamDeclaration BuildSuper(string name, int partitions) => new()
    {
        Name = name, Partitions = partitions, MaxAge = _maxAge, MaxLengthBytes = _maxLengthBytes,
        MaxSegmentSizeBytes = _maxSegmentSizeBytes, LeaderLocator = _locator,
    };
}

public sealed class StreamBusHostedService : IHostedService
{
    private readonly StreamBusConfigStore _store;
    private readonly IStreamBusRegistry _registry;
    private readonly List<StreamBusConnectionSupervisor> _started = new();
    private readonly object _gate = new();
    private readonly ILogger<StreamBusHostedService> _logger;
    private CancellationTokenSource? _declareCts;

    public StreamBusHostedService(StreamBusConfigStore store, IStreamBusRegistry registry, ILogger<StreamBusHostedService>? logger = null)
    {
        _store = store;
        _registry = registry;
        _logger = logger ?? NullLogger<StreamBusHostedService>.Instance;
    }

    public async Task StartAsync(CancellationToken ct)
    {
        var supervisors = new List<(string Vhost, TimeSpan? Timeout, StreamBusConnectionSupervisor Supervisor)>();
        foreach (var (busType, options) in _store.Registrations)
            _registry.GetOrCreateEntry(busType, options.Vhost);
        foreach (var vhost in _store.Registrations.Select(r => r.Options.Vhost).Distinct())
        {
            var supervisor = _registry.SupervisorFor(vhost);
            await supervisor.StartAsync(ct).ConfigureAwait(false);
            lock (_gate) _started.Add(supervisor);
            var timeout = _store.Registrations.First(r => r.Options.Vhost == vhost).Options.Hosting.StartupTimeout;
            supervisors.Add((vhost, timeout, supervisor));
        }
        CancellationTokenSource declareCts;
        lock (_gate)
        {
            _declareCts?.Dispose();
            _declareCts = CancellationTokenSource.CreateLinkedTokenSource(ct);
            declareCts = _declareCts;
        }
        var blocking = _store.Registrations.Any(r => r.Options.Hosting.BlockStartupUntilReady);
        var declareTasks = new List<Task>();
        foreach (var vhost in _store.Registrations.Select(r => r.Options.Vhost).Distinct())
        {
            var streams = _store.Registrations.Where(r => r.Options.Vhost == vhost).SelectMany(r => r.Options.Topology.Streams).ToList();
            var supers = _store.Registrations.Where(r => r.Options.Vhost == vhost).SelectMany(r => r.Options.Topology.SuperStreams).ToList();
            if (streams.Count == 0 && supers.Count == 0) continue;
            var task = DeclareUntilReadyAsync(vhost, streams, supers, declareCts.Token);
            if (!blocking)
            {
                _ = task.ContinueWith(
                    t =>
                    {
                        if (t.Exception is { } ex)
                            _logger.LogCritical(ex, "Topology declaration for vhost '{Vhost}' failed permanently; streams were NOT declared.", vhost);
                    },
                    CancellationToken.None,
                    TaskContinuationOptions.OnlyOnFaulted | TaskContinuationOptions.ExecuteSynchronously,
                    TaskScheduler.Default);
            }
            else
            {
                declareTasks.Add(task);
            }
        }
        if (!_store.Registrations.Any(r => r.Options.Hosting.BlockStartupUntilReady)) return;
        var timeouts = _store.Registrations
            .Where(r => r.Options.Hosting is { BlockStartupUntilReady: true, StartupTimeout: not null })
            .Select(r => r.Options.Hosting.StartupTimeout!.Value)
            .ToList();
        using var cts = CancellationTokenSource.CreateLinkedTokenSource(ct);
        if (timeouts.Count > 0) cts.CancelAfter(timeouts.Max());
        foreach (var (vhost, timeout, supervisor) in supervisors)
        {
            try
            {
                var readiness = await supervisor.WaitUntilReadyAsync(ReadinessPolicy.Indefinitely, cts.Token).ConfigureAwait(false);
                if (readiness == BusReadiness.Fatal)
                    throw new StreamBusFatalException("A StreamBus connection reported Fatal during startup.");
            }
            catch (OperationCanceledException) when (!ct.IsCancellationRequested)
            {
                var effective = timeout ?? (timeouts.Count > 0 ? timeouts.Max() : (TimeSpan?)null);
                throw new StreamBusTimeoutException($"Timed out waiting for StreamBus vhost '{vhost}' to become ready within {effective}.");
            }
        }
        if (declareTasks.Count > 0)
        {
            try
            {
                await Task.WhenAll(declareTasks).WaitAsync(cts.Token).ConfigureAwait(false);
            }
            catch (OperationCanceledException) when (!ct.IsCancellationRequested)
            {
                throw new StreamBusTimeoutException($"Timed out declaring topology within the startup timeout.");
            }
        }
    }

    private async Task DeclareUntilReadyAsync(string vhost, List<StreamDeclaration> streams, List<SuperStreamDeclaration> supers, CancellationToken ct)
    {
        var topology = _registry.TopologyFor(vhost);
        var attempt = 0;
        while (true)
        {
            ct.ThrowIfCancellationRequested();
            try
            {
                foreach (var s in streams)
                    await topology.EnsureStreamAsync(s, ct).ConfigureAwait(false);
                foreach (var s in supers)
                    await topology.EnsureSuperStreamAsync(s, ct).ConfigureAwait(false);
                return;
            }
            catch (OperationCanceledException) when (ct.IsCancellationRequested)
            {
                throw;
            }
            catch (StreamConfigurationConflictException)
            {
                throw;
            }
            catch (StreamBusFatalException)
            {
                throw;
            }
            catch (Exception ex)
            {
                attempt++;
                _logger.LogWarning(ex, "Topology declaration for vhost '{Vhost}' failed (attempt {Attempt}); retrying.", vhost, attempt);
                await Task.Delay(StreamBusDelays.ExponentialCapped(attempt, TimeSpan.FromMilliseconds(200), TimeSpan.FromSeconds(30), 2.0, TimeSpan.FromSeconds(1)), ct).ConfigureAwait(false);
            }
        }
    }

    public async Task StopAsync(CancellationToken ct)
    {
        foreach (var consumer in _registry.ActiveConsumers)
        {
            try { await consumer.DisposeAsync().ConfigureAwait(false); }
            catch (Exception) { /* shutdown best-effort; supervisor disposal below still runs */ }
        }
        CancellationTokenSource? declareCts;
        lock (_gate)
        {
            declareCts = _declareCts;
            _declareCts = null;
        }
        if (declareCts is not null)
        {
            try { declareCts.Cancel(); }
            finally { declareCts.Dispose(); }
        }
        List<StreamBusConnectionSupervisor> copy;
        lock (_gate)
        {
            copy = new List<StreamBusConnectionSupervisor>(_started);
            _started.Clear();
        }
        foreach (var supervisor in copy)
            await supervisor.DisposeAsync().ConfigureAwait(false);
    }
}

public sealed class StreamBusHealthCheck : IHealthCheck
{
    private readonly IStreamBusRegistry _registry;
    public StreamBusHealthCheck(IStreamBusRegistry registry) => _registry = registry;

    public Task<HealthCheckResult> CheckHealthAsync(HealthCheckContext context, CancellationToken ct = default)
    {
        var rows = _registry.DescribeStates().ToList();
        var data = rows.ToDictionary(
            r => $"{r.Vhost}|{r.BusType}",
            r => (object)r.State.ToString());
        if (rows.Count == 0)
            return Task.FromResult(HealthCheckResult.Degraded("no buses registered.", data: data));
        var worst = rows.Any(r => r.State == BusConnectionState.Fatal) ? BusConnectionState.Fatal
            : rows.Any(r => r.State != BusConnectionState.Ready) ? BusConnectionState.TransientFailure
            : BusConnectionState.Ready;
        return Task.FromResult(worst switch
        {
            BusConnectionState.Ready => HealthCheckResult.Healthy("All StreamBus connections ready.", data),
            BusConnectionState.Fatal => HealthCheckResult.Unhealthy("At least one StreamBus connection is fatal.", data: data),
            _ => HealthCheckResult.Degraded("StreamBus is starting or retrying.", data: data),
        });
    }
}
