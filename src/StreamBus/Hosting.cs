using System.Reflection;
using System.Runtime.ExceptionServices;
using System.Text.Json;
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
            && a.StreamPort == b.StreamPort
            && a.AmqpPort == b.AmqpPort
            && a.Heartbeat == b.Heartbeat
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
    internal static IServiceCollection AddShared(IServiceCollection services, Type busType, StreamBusOptions options)
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

    public static IServiceCollection AddStreamBusConsumer<TMessage, TConsumer>(this IServiceCollection services, string stream, Action<ConsumeOptionsBuilder>? configure = null)
        where TConsumer : class, IConsumer<TMessage>
        => AddStreamBusConsumerCore<TMessage, TConsumer>(services, typeof(DefaultBus), stream, configure);

    public static IServiceCollection AddStreamBusConsumer<TBus, TMessage, TConsumer>(this IServiceCollection services, string stream, Action<ConsumeOptionsBuilder>? configure = null)
        where TConsumer : class, IConsumer<TMessage>
        => AddStreamBusConsumerCore<TMessage, TConsumer>(services, typeof(TBus), stream, configure);

    private static IServiceCollection AddStreamBusConsumerCore<TMessage, TConsumer>(IServiceCollection services, Type busMarker, string stream, Action<ConsumeOptionsBuilder>? configure)
        where TConsumer : class, IConsumer<TMessage>
    {
        ArgumentNullException.ThrowIfNull(services);
        ArgumentException.ThrowIfNullOrWhiteSpace(stream);
        services.TryAddTransient<TConsumer>();
        var builder = new ConsumeOptionsBuilder();
        configure?.Invoke(builder);
        GetOrCreateConsumerStore(services).Add(busMarker, typeof(TMessage), typeof(TConsumer), stream, builder.Build());
        return services;
    }

    private static StreamBusConsumerStore GetOrCreateConsumerStore(IServiceCollection services)
    {
        var existing = services.FirstOrDefault(d =>
            d.ServiceType == typeof(StreamBusConsumerStore) && d.ImplementationInstance is StreamBusConsumerStore);
        if (existing?.ImplementationInstance is StreamBusConsumerStore store) return store;
        var created = new StreamBusConsumerStore();
        services.AddSingleton<StreamBusConsumerStore>(created);
        return created;
    }
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
    public StreamBusHostOptionsBuilder BlockStartupUntilReady(TimeSpan? timeout = null)
    {
        if (timeout is { } t && t <= TimeSpan.Zero)
            throw new StreamBusConfigurationException("StartupTimeout must be positive when set.");
        _block = true; _timeout = timeout; return this;
    }
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

public sealed class ConsumeOptionsBuilder
{
    private bool _super;
    private string? _reference;
    private bool _sac = true;
    private ConsumeOrdering _ordering = ConsumeOrdering.Strict;
    private ConsumeOffset _offset = ConsumeOffset.ResumeOrFirst;
    private OffsetStoreOptions _store = new();
    private RetryPolicy _retry = new();
    private ErrorStreamOptions? _error;
    private Func<ErrorHandoff, Task>? _hook;
    private ReadinessPolicy _readiness = ReadinessPolicy.FailFastPolicy;

    public ConsumeOptionsBuilder SuperStream(bool enabled = true) { _super = enabled; return this; }
    public ConsumeOptionsBuilder Reference(string? reference) { _reference = reference; return this; }
    public ConsumeOptionsBuilder SingleActiveConsumer(bool enabled = true) { _sac = enabled; return this; }
    public ConsumeOptionsBuilder Ordering(ConsumeOrdering ordering) { _ordering = ordering; return this; }
    public ConsumeOptionsBuilder Offset(ConsumeOffset offset) { _offset = offset; return this; }
    public ConsumeOptionsBuilder OffsetStore(OffsetStoreOptions options) { _store = options ?? throw new ArgumentNullException(nameof(options)); return this; }
    public ConsumeOptionsBuilder Retry(RetryPolicy policy) { _retry = policy ?? throw new ArgumentNullException(nameof(policy)); return this; }
    public ConsumeOptionsBuilder ErrorStream(ErrorStreamOptions? options) { _error = options; return this; }
    public ConsumeOptionsBuilder OnRetryExhausted(Func<ErrorHandoff, Task>? hook) { _hook = hook; return this; }
    public ConsumeOptionsBuilder Readiness(ReadinessPolicy policy) { _readiness = policy ?? throw new ArgumentNullException(nameof(policy)); return this; }

    public ConsumeOptions Build() => new()
    {
        IsSuperStream = _super,
        Reference = _reference,
        SingleActiveConsumer = _sac,
        Ordering = _ordering,
        Offset = _offset,
        OffsetStore = _store,
        Retry = _retry,
        ErrorStream = _error,
        OnRetryExhausted = _hook,
        ReadinessPolicy = _readiness,
    };
}

public sealed record TypedConsumerRegistration
{
    public required Type BusMarker { get; init; }
    public required Type MessageType { get; init; }
    public required Type ConsumerType { get; init; }
    public required string Stream { get; init; }
    public required ConsumeOptions Options { get; init; }
}

public sealed class StreamBusConsumerStore
{
    private readonly List<TypedConsumerRegistration> _registrations = new();

    public void Add(Type busMarker, Type messageType, Type consumerType, string stream, ConsumeOptions options)
    {
        ArgumentNullException.ThrowIfNull(busMarker);
        ArgumentNullException.ThrowIfNull(messageType);
        ArgumentNullException.ThrowIfNull(consumerType);
        ArgumentException.ThrowIfNullOrWhiteSpace(stream);
        ArgumentNullException.ThrowIfNull(options);
        if (_registrations.Any(r => r.BusMarker == busMarker && r.MessageType == messageType && r.ConsumerType == consumerType && r.Stream == stream))
            throw new StreamBusConfigurationException($"Duplicate typed consumer registration for {consumerType.FullName} on stream '{stream}' (marker {busMarker.FullName}).");
        _registrations.Add(new TypedConsumerRegistration { BusMarker = busMarker, MessageType = messageType, ConsumerType = consumerType, Stream = stream, Options = options });
    }

    public IReadOnlyList<TypedConsumerRegistration> Registrations => _registrations;
}

internal static class TypedConsumerHandler
{
    public static Func<ConsumeContext, Task> Create(Type messageType, Type consumerType, IServiceScopeFactory scopes, IStreamSerializer serializer)
    {
        ArgumentNullException.ThrowIfNull(messageType);
        ArgumentNullException.ThrowIfNull(consumerType);
        ArgumentNullException.ThrowIfNull(scopes);
        ArgumentNullException.ThrowIfNull(serializer);
        var open = typeof(TypedConsumerHandler).GetMethod(nameof(CreateTyped), BindingFlags.NonPublic | BindingFlags.Static)
            ?? throw new StreamBusConfigurationException("Typed consumer invoker is missing (internal error).");
        MethodInfo closed;
        try
        {
            closed = open.MakeGenericMethod(messageType, consumerType);
        }
        catch (ArgumentException)
        {
            throw new StreamBusConfigurationException($"Consumer {consumerType.FullName} cannot consume {messageType.FullName} (must implement IConsumer<{messageType.Name}>).");
        }
        return (Func<ConsumeContext, Task>)closed.Invoke(null, new object[] { scopes, serializer })!;
    }

    private static Func<ConsumeContext, Task> CreateTyped<T, TConsumer>(IServiceScopeFactory scopes, IStreamSerializer serializer)
        where TConsumer : class, IConsumer<T>
        => async raw =>
        {
            T message;
            try
            {
                message = serializer.Deserialize<T>(raw.Body);
            }
            catch (Exception ex) when (ex is not OperationCanceledException)
            {
                throw new StreamBusDeserializationException(raw.Stream, raw.Partition, raw.Offset, typeof(T), ex);
            }
            if (ReferenceEquals(message, null))
                throw new StreamBusDeserializationException(raw.Stream, raw.Partition, raw.Offset, typeof(T), null);
            using var scope = scopes.CreateScope();
            var consumer = scope.ServiceProvider.GetRequiredService<TConsumer>();
            await consumer.Consume(new ConsumeContext<T>
            {
                Message = message,
                Stream = raw.Stream,
                Partition = raw.Partition,
                Offset = raw.Offset,
                RoutingValue = raw.RoutingValue,
                Body = raw.Body,
                Metadata = raw.Metadata,
                CancellationToken = raw.CancellationToken,
            }, raw.CancellationToken).ConfigureAwait(false);
        };
}

public sealed class StreamBusHostedService : IHostedService
{
    private readonly StreamBusConfigStore _store;
    private readonly IStreamBusRegistry _registry;
    private readonly List<StreamBusConnectionSupervisor> _started = new();
    private readonly object _gate = new();
    private readonly ILogger<StreamBusHostedService> _logger;
    private readonly IServiceScopeFactory? _scopes;
    private readonly StreamBusConsumerStore? _consumers;
    private CancellationTokenSource? _startupCts;

    public StreamBusHostedService(StreamBusConfigStore store, IStreamBusRegistry registry, ILogger<StreamBusHostedService>? logger = null, IServiceScopeFactory? scopeFactory = null, StreamBusConsumerStore? consumerStore = null)
    {
        _store = store;
        _registry = registry;
        _logger = logger ?? NullLogger<StreamBusHostedService>.Instance;
        _scopes = scopeFactory;
        _consumers = consumerStore;
    }

    public async Task StartAsync(CancellationToken ct)
    {
        var starters = new List<(TypedConsumerRegistration Reg, string Vhost, IStreamSerializer Serializer)>();
        if (_consumers is not null)
        {
            if (_scopes is null && _consumers.Registrations.Count > 0)
                throw new StreamBusConfigurationException("Typed consumers are registered but no IServiceScopeFactory is available (internal error).");
            foreach (var reg in _consumers.Registrations)
            {
                var match = _store.Registrations.FirstOrDefault(r => r.BusType == reg.BusMarker);
                if (match.BusType is null)
                    throw new StreamBusConfigurationException($"Typed consumer {reg.ConsumerType.FullName} on stream '{reg.Stream}' references marker {reg.BusMarker.FullName} with no AddStreamBus registration. Register the bus (Add-call order does not matter).");
                starters.Add((reg, match.Options.Vhost, match.Options.Serializer ?? SystemTextJsonStreamSerializer.Default));
            }
        }
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
        CancellationTokenSource startupCts;
        lock (_gate)
        {
            _startupCts?.Dispose();
            _startupCts = CancellationTokenSource.CreateLinkedTokenSource(ct);
            startupCts = _startupCts;
        }
        var blocking = _store.Registrations.Any(r => r.Options.Hosting.BlockStartupUntilReady);
        var declareTasks = new List<Task>();
        foreach (var vhost in _store.Registrations.Select(r => r.Options.Vhost).Distinct())
        {
            var streams = _store.Registrations.Where(r => r.Options.Vhost == vhost).SelectMany(r => r.Options.Topology.Streams).ToList();
            var supers = _store.Registrations.Where(r => r.Options.Vhost == vhost).SelectMany(r => r.Options.Topology.SuperStreams).ToList();
            if (streams.Count == 0 && supers.Count == 0) continue;
            var task = DeclareUntilReadyAsync(vhost, streams, supers, startupCts.Token);
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
        var consumerTasks = new List<Task>();
        foreach (var (reg, vhost, serializer) in starters)
        {
            var task = StartConsumerUntilReadyAsync(reg, vhost, serializer, startupCts.Token);
            if (!blocking)
            {
                _ = task.ContinueWith(
                    t =>
                    {
                        if (t.Exception is { } ex)
                            _logger.LogCritical(ex, "Typed consumer {Consumer} on stream '{Stream}' failed permanently; it is NOT consuming.", reg.ConsumerType.FullName, reg.Stream);
                    },
                    CancellationToken.None,
                    TaskContinuationOptions.OnlyOnFaulted | TaskContinuationOptions.ExecuteSynchronously,
                    TaskScheduler.Default);
            }
            else
            {
                consumerTasks.Add(task);
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
        var startupTasks = declareTasks.Concat(consumerTasks).ToList();
        if (startupTasks.Count > 0)
        {
            foreach (var task in declareTasks)
            {
                _ = task.ContinueWith(
                    t =>
                    {
                        if (t.Exception?.InnerExceptions.Any(e => e is StreamConfigurationConflictException) == true)
                        {
                            try { startupCts.Cancel(); }
                            catch (ObjectDisposedException) { }
                        }
                    },
                    CancellationToken.None,
                    TaskContinuationOptions.OnlyOnFaulted | TaskContinuationOptions.ExecuteSynchronously,
                    TaskScheduler.Default);
            }
            try
            {
                await Task.WhenAll(startupTasks).WaitAsync(cts.Token).ConfigureAwait(false);
            }
            catch (OperationCanceledException) when (!ct.IsCancellationRequested)
            {
                try { startupCts.Cancel(); }
                catch (ObjectDisposedException) { }
                var conflict = startupTasks
                    .SelectMany(t => t.Exception?.InnerExceptions.AsEnumerable() ?? Enumerable.Empty<Exception>())
                    .OfType<StreamConfigurationConflictException>()
                    .FirstOrDefault();
                if (conflict is not null) ExceptionDispatchInfo.Capture(conflict).Throw();
                var config = startupTasks
                    .SelectMany(t => t.Exception?.InnerExceptions.AsEnumerable() ?? Enumerable.Empty<Exception>())
                    .OfType<StreamBusConfigurationException>()
                    .FirstOrDefault();
                if (config is not null) ExceptionDispatchInfo.Capture(config).Throw();
                throw new StreamBusTimeoutException($"Timed out declaring topology and starting typed consumers within the startup timeout.");
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

    internal static bool IsStartupTransient(Exception ex)
        => ex is StreamBusNotReadyException or StreamBusStreamNotFoundException;

    private async Task StartConsumerUntilReadyAsync(TypedConsumerRegistration reg, string vhost, IStreamSerializer serializer, CancellationToken ct)
    {
        var entry = _registry.GetOrCreateEntry(reg.BusMarker, vhost);
        var reference = reg.Options.Reference ?? ReferenceDerivation.Derive(reg.BusMarker, reg.ConsumerType, reg.Stream);
        ReferenceDerivation.Validate(reference);
        var options = reg.Options with { Reference = reference };
        var handler = TypedConsumerHandler.Create(reg.MessageType, reg.ConsumerType, _scopes!, serializer);
        var attempt = 0;
        while (true)
        {
            ct.ThrowIfCancellationRequested();
            try
            {
                await entry.Bus.ConsumeAsync(reg.Stream, options, handler, ct).ConfigureAwait(false);
                return;
            }
            catch (OperationCanceledException) when (ct.IsCancellationRequested)
            {
                throw;
            }
            catch (Exception ex) when (IsStartupTransient(ex))
            {
                attempt++;
                _logger.LogWarning(ex, "Typed consumer {Consumer} on stream '{Stream}' not started (attempt {Attempt}); retrying.", reg.ConsumerType.FullName, reg.Stream, attempt);
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
        CancellationTokenSource? startupCts;
        lock (_gate)
        {
            startupCts = _startupCts;
            _startupCts = null;
        }
        if (startupCts is not null)
        {
            try { startupCts.Cancel(); }
            finally { startupCts.Dispose(); }
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
