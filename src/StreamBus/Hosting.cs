using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Diagnostics.HealthChecks;
using Microsoft.Extensions.Hosting;

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
        _byVhost.TryAdd(options.Vhost, options);
        _registrations.Add((busType, options));
    }

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
        services.TryAddSingleton<IStreamSystemConnector, StreamSystemConnector>();
        services.TryAddSingleton<IStreamBusRegistry>(sp =>
            new StreamBusRegistry(
                store.ForVhost,
                opts => new StreamBusConnectionSupervisor(opts, sp.GetRequiredService<IStreamSystemConnector>())));
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
    public StreamBusOptions Build() => new()
    {
        Vhost = _vhost,
        UserName = _user,
        Password = _password,
        Host = _host,
        Port = _port,
        Connection = _connection,
        Hosting = _hosting,
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

public sealed class StreamBusHostedService : IHostedService
{
    private readonly StreamBusConfigStore _store;
    private readonly IStreamBusRegistry _registry;
    private readonly List<StreamBusConnectionSupervisor> _started = new();
    private readonly object _gate = new();

    public StreamBusHostedService(StreamBusConfigStore store, IStreamBusRegistry registry)
    {
        _store = store;
        _registry = registry;
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
    }

    public async Task StopAsync(CancellationToken ct)
    {
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
