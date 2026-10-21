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

/// <summary>
/// Credential-free projection of one bus registration for health/status and
/// diagnostics. The live <see cref="StreamBusOptions"/> stays internal to the
/// store so a password can never leak through a public read accessor.
/// </summary>
public sealed record StreamBusRegistrationView
{
    public required string Vhost { get; init; }
    public required string Host { get; init; }
    public required int StreamPort { get; init; }
    public required int AmqpPort { get; init; }
    public required double HeartbeatSeconds { get; init; }
}

public sealed class StreamBusConfigStore
{
    private readonly HashSet<Type> _busTypes = new();
    private readonly Dictionary<string, StreamBusOptions> _byVhost = new();
    private readonly List<(Type BusType, StreamBusOptions Options)> _registrations = new();

    internal void Add(Type busType, StreamBusOptions options)
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

    public IReadOnlyList<StreamBusRegistrationView> Registrations
        => _registrations
            .Select(r => new StreamBusRegistrationView
            {
                Vhost = r.Options.Vhost,
                Host = r.Options.Host,
                StreamPort = r.Options.StreamPort,
                AmqpPort = r.Options.AmqpPort,
                HeartbeatSeconds = r.Options.Heartbeat.TotalSeconds,
            })
            .ToList();

    internal IReadOnlyList<(Type BusType, StreamBusOptions Options)> Entries => _registrations;
}

public static class ServiceCollectionExtensions
{
    internal static IServiceCollection AddShared(IServiceCollection services, Type busType, StreamBusOptions options)
    {
        var store = GetOrCreateStore(services);
        store.Add(busType, options);
        var publishStore = GetOrCreatePublishStore(services);
        var router = new StreamPublishRouter(publishStore);
        var endpointStore = GetOrCreateEndpointStore(services);
        IReadOnlyList<DeclaredStream> DeclarationsFor(Type marker)
        {
            var markerVhost = store.Entries.Where(r => r.BusType == marker).Select(r => r.Options.Vhost).FirstOrDefault();
            if (markerVhost is null) return Array.Empty<DeclaredStream>();
            return DeclareSetBuilder.Build(
                endpointStore.Endpoints.Where(e => e.BusMarker == marker).Select(e => e.Descriptor),
                publishStore.Registrations.Where(r => r.BusMarker == marker),
                marker.FullName ?? marker.Name);
        }
        services.TryAddSingleton<IStreamSystemConnector>(sp => new StreamSystemConnector(sp.GetService<ILoggerFactory>()));
        services.TryAddSingleton<IStreamBusRegistry>(sp =>
            new StreamBusRegistry(
                store.ForVhost,
                opts => new StreamBusConnectionSupervisor(opts, sp.GetRequiredService<IStreamSystemConnector>()),
                sp.GetService<ILoggerFactory>(),
                router,
                marker => store.Entries.Where(r => r.BusType == marker).Select(r => r.Options.Serializer).FirstOrDefault() ?? SystemTextJsonStreamSerializer.Default,
                sp.GetService<IServiceScopeFactory>(),
                DeclarationsFor));
        services.TryAddSingleton<IHostedService>(sp => StreamBusHostedService.Create(sp));
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

    internal static StreamBusConsumerStore GetOrCreateConsumerStore(IServiceCollection services)
    {
        var existing = services.FirstOrDefault(d =>
            d.ServiceType == typeof(StreamBusConsumerStore) && d.ImplementationInstance is StreamBusConsumerStore);
        if (existing?.ImplementationInstance is StreamBusConsumerStore store) return store;
        var created = new StreamBusConsumerStore();
        services.AddSingleton<StreamBusConsumerStore>(created);
        return created;
    }

    internal static StreamBusEndpointStore GetOrCreateEndpointStore(IServiceCollection services)
    {
        var existing = services.FirstOrDefault(d =>
            d.ServiceType == typeof(StreamBusEndpointStore) && d.ImplementationInstance is StreamBusEndpointStore);
        if (existing?.ImplementationInstance is StreamBusEndpointStore store) return store;
        var created = new StreamBusEndpointStore();
        services.AddSingleton<StreamBusEndpointStore>(created);
        return created;
    }

    internal static StreamBusPublishStore GetOrCreatePublishStore(IServiceCollection services)
    {
        var existing = services.FirstOrDefault(d =>
            d.ServiceType == typeof(StreamBusPublishStore) && d.ImplementationInstance is StreamBusPublishStore);
        if (existing?.ImplementationInstance is StreamBusPublishStore store) return store;
        var created = new StreamBusPublishStore();
        services.AddSingleton<StreamBusPublishStore>(created);
        return created;
    }

    public static IServiceCollection AddStreamBusHosting(this IServiceCollection services, Action<StreamBusHostingOptionsBuilder> configure)
    {
        ArgumentNullException.ThrowIfNull(services);
        ArgumentNullException.ThrowIfNull(configure);
        if (services.Any(d => d.ServiceType == typeof(StreamBusHostingStore) && d.ImplementationInstance is StreamBusHostingStore))
            throw new StreamBusConfigurationException("AddStreamBusHosting may be called at most once per service.");
        var builder = new StreamBusHostingOptionsBuilder();
        configure(builder);
        services.AddSingleton(new StreamBusHostingStore(builder.Build()));
        return services;
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

public sealed record StreamBusHostingOptions
{
    public bool BlockStartupUntilReady { get; init; } = false;
    public bool BlockUntilConsumersAttached { get; init; } = false;
    public TimeSpan? StartupTimeout { get; init; }
}

public sealed class StreamBusHostingStore
{
    internal StreamBusHostingStore(StreamBusHostingOptions options)
    {
        ArgumentNullException.ThrowIfNull(options);
        Options = options;
    }

    public StreamBusHostingOptions Options { get; }
}

public sealed class StreamBusHostingOptionsBuilder
{
    private bool _block;
    private bool _blockConsumers;
    private TimeSpan? _timeout;
    public StreamBusHostingOptionsBuilder BlockStartupUntilReady(TimeSpan? timeout = null)
    {
        if (timeout is { } t && t <= TimeSpan.Zero)
            throw new StreamBusConfigurationException("StartupTimeout must be positive when set.");
        _block = true; _timeout = timeout; return this;
    }
    public StreamBusHostingOptionsBuilder BlockUntilConsumersAttached(bool enabled = true)
    {
        _blockConsumers = enabled;
        if (enabled) _block = true;
        return this;
    }
    public StreamBusHostingOptions Build() => new()
    {
        BlockStartupUntilReady = _block,
        BlockUntilConsumersAttached = _blockConsumers,
        StartupTimeout = _timeout,
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

    internal void Add(Type busMarker, Type messageType, Type consumerType, string stream, ConsumeOptions options)
    {
        ArgumentNullException.ThrowIfNull(busMarker);
        ArgumentNullException.ThrowIfNull(messageType);
        ArgumentNullException.ThrowIfNull(consumerType);
        ArgumentException.ThrowIfNullOrWhiteSpace(stream);
        ArgumentNullException.ThrowIfNull(options);
        ConsumerReferenceValidation.Require(options.Reference, stream);
        if (_registrations.Any(r => r.BusMarker == busMarker && r.MessageType == messageType && r.ConsumerType == consumerType && r.Stream == stream && r.Options.Reference == options.Reference))
            throw new StreamBusConfigurationException($"Duplicate typed consumer registration for {consumerType.FullName} on stream '{stream}' with group '{options.Reference}' (marker {busMarker.FullName}).");
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

internal sealed class StreamBusHostedService : IHostedService
{
    private readonly StreamBusConfigStore _store;
    private readonly IStreamBusRegistry _registry;
    private readonly List<(string Vhost, StreamBusConnectionSupervisor Supervisor)> _started = new();
    private readonly object _gate = new();
    private readonly ILogger<StreamBusHostedService> _logger;
    private readonly IServiceScopeFactory _scopes;
    private readonly StreamBusConsumerStore _consumers;
    private readonly StreamBusEndpointStore _endpoints;
    private readonly StreamBusPublishStore _publishes;
    private readonly StreamBusHostingOptions _hosting;
    private CancellationTokenSource? _startupCts;

    internal StreamBusHostedService(
        StreamBusConfigStore store,
        IStreamBusRegistry registry,
        ILogger<StreamBusHostedService> logger,
        IServiceScopeFactory scopeFactory,
        StreamBusConsumerStore consumerStore,
        StreamBusEndpointStore endpointStore,
        StreamBusPublishStore publishStore,
        StreamBusHostingStore hostingStore)
    {
        ArgumentNullException.ThrowIfNull(store);
        ArgumentNullException.ThrowIfNull(registry);
        ArgumentNullException.ThrowIfNull(logger);
        ArgumentNullException.ThrowIfNull(scopeFactory);
        ArgumentNullException.ThrowIfNull(consumerStore);
        ArgumentNullException.ThrowIfNull(endpointStore);
        ArgumentNullException.ThrowIfNull(publishStore);
        ArgumentNullException.ThrowIfNull(hostingStore);
        _store = store;
        _registry = registry;
        _logger = logger;
        _scopes = scopeFactory;
        _consumers = consumerStore;
        _endpoints = endpointStore;
        _publishes = publishStore;
        _hosting = hostingStore.Options;
    }

    internal static StreamBusHostedService Create(IServiceProvider services)
    {
        ArgumentNullException.ThrowIfNull(services);
        return new StreamBusHostedService(
            services.GetRequiredService<StreamBusConfigStore>(),
            services.GetRequiredService<IStreamBusRegistry>(),
            services.GetService<ILogger<StreamBusHostedService>>() ?? NullLogger<StreamBusHostedService>.Instance,
            services.GetRequiredService<IServiceScopeFactory>(),
            services.GetRequiredService<StreamBusConsumerStore>(),
            services.GetRequiredService<StreamBusEndpointStore>(),
            services.GetRequiredService<StreamBusPublishStore>(),
            services.GetService<StreamBusHostingStore>() ?? new StreamBusHostingStore(new StreamBusHostingOptions()));
    }

    public async Task StartAsync(CancellationToken ct)
    {
        var starters = new List<(TypedConsumerRegistration Reg, string Vhost, IStreamSerializer Serializer)>();
        foreach (var reg in _consumers.Registrations)
        {
            var match = _store.Entries.FirstOrDefault(r => r.BusType == reg.BusMarker);
            if (match.BusType is null)
                throw new StreamBusConfigurationException($"Typed consumer {reg.ConsumerType.FullName} on stream '{reg.Stream}' references marker {reg.BusMarker.FullName} with no AddStreamBus registration. Register the bus (Add-call order does not matter).");
            starters.Add((reg, match.Options.Vhost, match.Options.Serializer ?? SystemTextJsonStreamSerializer.Default));
        }
        var supervisors = new List<(string Vhost, TimeSpan? Timeout, StreamBusConnectionSupervisor Supervisor)>();
        foreach (var (busType, options) in _store.Entries)
            _registry.GetOrCreateEntry(busType, options.Vhost);
        foreach (var vhost in _store.Entries.Select(r => r.Options.Vhost).Distinct())
        {
            var supervisor = _registry.SupervisorFor(vhost);
            await supervisor.StartAsync(ct).ConfigureAwait(false);
            lock (_gate) _started.Add((vhost, supervisor));
            supervisors.Add((vhost, _hosting.StartupTimeout, supervisor));
        }
        CancellationTokenSource startupCts;
        lock (_gate)
        {
            _startupCts?.Dispose();
            _startupCts = CancellationTokenSource.CreateLinkedTokenSource(ct);
            startupCts = _startupCts;
        }
        var blocking = _hosting.BlockStartupUntilReady;
        var declareTasks = new List<Task>();
        var consumerTasks = new List<Task>();
        foreach (var (busType, busOptions) in _store.Entries)
        {
            var endpoints = _endpoints.Endpoints
                .Where(e => e.BusMarker == busType)
                .Select(e => e.Descriptor)
                .ToList();
            var publishes = _publishes.Registrations
                .Where(r => r.BusMarker == busType)
                .ToList();
            if (endpoints.Count == 0 && publishes.Count == 0) continue;
            var task = DeclareDerivedUntilReadyAsync(busType, busOptions.Vhost, endpoints, publishes, startupCts.Token);
            if (!blocking)
            {
                _ = task.ContinueWith(
                    t =>
                    {
                        if (t.Exception is { } ex)
                            _logger.LogCritical(ex, "Derived topology for vhost '{Vhost}' failed permanently; streams were NOT derived.", busOptions.Vhost);
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
        var queuesByVhost = starters
            .Where(s => s.Reg.Options.DeadLetter is not null && s.Reg.Options.ExhaustionCallback is null)
            .GroupBy(s => s.Vhost, StringComparer.Ordinal)
            .ToDictionary(
                g => g.Key,
                g => g.Select(s => s.Reg.Options.DeadLetter!).Distinct().ToList(),
                StringComparer.Ordinal);
        foreach (var (vhost, queues) in queuesByVhost)
        {
            var task = DeclareQueuesUntilReadyAsync(vhost, queues, startupCts.Token);
            if (!blocking)
            {
                _ = task.ContinueWith(
                    t =>
                    {
                        if (t.Exception is { } ex)
                            _logger.LogCritical(ex, "Error-queue declaration for vhost '{Vhost}' failed permanently; queues were NOT declared.", vhost);
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
        if (!_hosting.BlockStartupUntilReady) return;
        var timeouts = new List<TimeSpan>();
        if (_hosting is { BlockStartupUntilReady: true, StartupTimeout: not null })
            timeouts.Add(_hosting.StartupTimeout!.Value);
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
            catch (OperationCanceledException) when (ct.IsCancellationRequested)
            {
                // Host is stopping: cancelling the STARTUP wait is NOT a "Timed out" event.
                throw;
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
                        if (t.Exception?.InnerExceptions.Any(e => e is StreamConfigurationConflictException or StreamBusConfigurationException) == true)
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
            catch (OperationCanceledException) when (ct.IsCancellationRequested)
            {
                // Host is stopping: this is a shutdown, not a startup budget expiry.
                throw;
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
        if (_hosting.BlockUntilConsumersAttached)
        {
            var consumerVhosts = starters.Select(s => s.Vhost).Distinct().ToList();
            while (true)
            {
                var rows = consumerVhosts
                    .SelectMany(v => _registry.HealthFor(v).Snapshot().Registrations)
                    .Where(r => r.Kind == "consumer")
                    .ToList();
                var failed = rows.FirstOrDefault(r => r.State is BusRegistrationState.FailedRetrying or BusRegistrationState.Stopped);
                if (failed is not null)
                    throw new StreamBusStartupBlockedException(
                        $"Typed consumer '{failed.Registration}' on vhost '{failed.Vhost}' is {failed.State}: {failed.Reason ?? "no reason recorded"}.");
                if (rows.Count == 0 || rows.All(r => r.State == BusRegistrationState.Attached))
                    break;
                try
                {
                    await Task.Delay(TimeSpan.FromMilliseconds(25), cts.Token).ConfigureAwait(false);
                }
                catch (OperationCanceledException) when (!ct.IsCancellationRequested)
                {
                    throw new StreamBusTimeoutException("Timed out waiting for typed consumers to attach during startup.");
                }
            }
        }
    }

    internal static bool IsStartupTransient(Exception ex)
        => ex is StreamBusNotReadyException or StreamBusStreamNotFoundException;

    private async Task DeclareDerivedUntilReadyAsync(
        Type busType,
        string vhost,
        List<ReceiveEndpointDescriptor> endpoints,
        List<PublishRegistration> publishes,
        CancellationToken ct)
    {
        var busName = busType.FullName ?? busType.Name;
        var declareSet = DeclareSetBuilder.Build(endpoints, publishes, busName);
        var topology = _registry.TopologyFor(vhost);
        var health = _registry.HealthFor(vhost);
        foreach (var entry in declareSet)
            health.UpsertRegistration(vhost, $"declare:{entry.Name}", "declare", BusRegistrationState.Attaching);
        void MarkDeclareFailed(Exception ex)
        {
            foreach (var entry in declareSet)
                health.UpsertRegistration(vhost, $"declare:{entry.Name}", "declare", BusRegistrationState.FailedRetrying, ex.Message);
        }
        await StreamRuntimeLoop.RetryUntilAsync(
            async attemptCt =>
            {
                attemptCt.ThrowIfCancellationRequested();
                foreach (var entry in declareSet)
                {
                    if (!entry.IsSuper && entry.Source is "publish" or "dead-letter")
                    {
                        await topology.EnsureStreamAsync(new StreamDeclaration
                        {
                            Name = entry.Name,
                            MaxAge = entry.MaxAge,
                            MaxLengthBytes = entry.MaxLengthBytes,
                            MaxSegmentSizeBytes = entry.MaxSegmentSizeBytes,
                            LeaderLocator = entry.LeaderLocator ?? StreamLeaderLocator.LeastLeaders,
                        }, attemptCt).ConfigureAwait(false);
                        continue;
                    }
                    var probe = await topology.ProbeStreamAsync(entry.Name, attemptCt).ConfigureAwait(false);
                    var who = entry.Source == "endpoint" ? $"endpoint '{entry.Name}'" : $"publish registration for '{entry.Name}'";
                    if (entry.IsSuper)
                    {
                        if (probe.PlainExists)
                            throw new StreamConfigurationConflictException(entry.Name,
                                $"{who} expects a super stream",
                                "a plain stream");
                        if (entry.Partitions is null)
                        {
                            if (probe.SuperExists) continue; // adopt: broker rules the partition count
                            throw new StreamBusConfigurationException(
                                $"Endpoint stream '{entry.Name}' is expected to be a super stream but does not exist and no registration resolves its partition count. Set Partitions on the PublishSuper registration (s.Partitions(n)) or Partitions on a consuming endpoint.");
                        }
                        await topology.EnsureSuperStreamAsync(new SuperStreamDeclaration
                        {
                            Name = entry.Name,
                            Partitions = entry.Partitions.Value,
                            MaxAge = entry.MaxAge,
                            MaxLengthBytes = entry.MaxLengthBytes,
                            MaxSegmentSizeBytes = entry.MaxSegmentSizeBytes,
                            LeaderLocator = entry.LeaderLocator ?? StreamLeaderLocator.LeastLeaders,
                        }, attemptCt).ConfigureAwait(false);
                    }
                    else
                    {
                        if (probe.PlainExists) continue;
                        if (probe.SuperExists)
                            throw new StreamConfigurationConflictException(entry.Name,
                                $"{who} expects a plain stream",
                                "a super stream");
                        await topology.EnsureStreamAsync(new StreamDeclaration
                        {
                            Name = entry.Name,
                            MaxAge = entry.MaxAge,
                            MaxLengthBytes = entry.MaxLengthBytes,
                            MaxSegmentSizeBytes = entry.MaxSegmentSizeBytes,
                            LeaderLocator = entry.LeaderLocator ?? StreamLeaderLocator.LeastLeaders,
                        }, attemptCt).ConfigureAwait(false);
                    }
                }
                foreach (var entry in declareSet)
                    health.RemoveRegistration(vhost, $"declare:{entry.Name}");
                return true;
            },
            ex =>
            {
                if (ex is StreamConfigurationConflictException or StreamBusConfigurationException or StreamBusFatalException)
                {
                    MarkDeclareFailed(ex);
                    return StreamBusRetryKind.Fatal;
                }
                return StreamBusRetryKind.Transient;
            },
            TimeSpan.FromMilliseconds(200),
            TimeSpan.FromSeconds(30),
            int.MaxValue,
            _logger,
            $"Derived topology for vhost '{vhost}'",
            ct).ConfigureAwait(false);
    }

    private async Task DeclareQueuesUntilReadyAsync(string vhost, List<DeadLetterOptions> queues, CancellationToken ct)
    {
        var health = _registry.HealthFor(vhost);
        foreach (var queue in queues)
            health.UpsertRegistration(vhost, $"declare:{queue.Name}", "declare", BusRegistrationState.Attaching);
        void MarkDeclareFailed(Exception ex)
        {
            foreach (var queue in queues)
                health.UpsertRegistration(vhost, $"declare:{queue.Name}", "declare", BusRegistrationState.FailedRetrying, ex.Message);
        }
        await StreamRuntimeLoop.RetryUntilAsync(
            async attemptCt =>
            {
                attemptCt.ThrowIfCancellationRequested();
                await _registry.ErrorQueuesFor(vhost).DeclareAsync(queues, attemptCt).ConfigureAwait(false);
                foreach (var queue in queues)
                    health.RemoveRegistration(vhost, $"declare:{queue.Name}");
                return true;
            },
            ex =>
            {
                if (ex is StreamConfigurationConflictException or StreamBusFatalException)
                {
                    MarkDeclareFailed(ex);
                    return StreamBusRetryKind.Fatal;
                }
                return StreamBusRetryKind.Transient;
            },
            TimeSpan.FromMilliseconds(200),
            TimeSpan.FromSeconds(30),
            int.MaxValue,
            _logger,
            $"Error-queue declaration for vhost '{vhost}'",
            ct).ConfigureAwait(false);
    }

    private async Task StartConsumerUntilReadyAsync(TypedConsumerRegistration reg, string vhost, IStreamSerializer serializer, CancellationToken ct)
    {
        var entry = _registry.GetOrCreateEntry(reg.BusMarker, vhost);
        var reference = ConsumerReferenceValidation.Require(reg.Options.Reference, reg.Stream);
        var options = reg.Options with { Reference = reference };
        if (options.ExhaustionCallback is not null && options.ShadowedDeadLetter is { } shadowed)
            _logger.LogWarning(
                "ReceiveEndpoint '{Stream}' sets both OnExhausted and a dead-letter destination '{Destination}'; the callback owns exhaustion and the destination is shadowed.",
                reg.Stream, shadowed.Name);
        var handler = TypedConsumerHandler.Create(reg.MessageType, reg.ConsumerType, _scopes, serializer);
        var consumers = _registry is StreamBusRegistry concrete ? concrete.ConsumersFor(vhost) : null;
        await StreamRuntimeLoop.RetryUntilAsync(
            async attemptCt =>
            {
                attemptCt.ThrowIfCancellationRequested();
                if (consumers is not null)
                    await consumers.ConsumeAsync(reg.BusMarker, reg.Stream, options, handler, attemptCt, registrationName: reg.Stream).ConfigureAwait(false);
                else
                    await entry.Bus.ConsumeAsync(reg.Stream, options, handler, attemptCt).ConfigureAwait(false);
                return true;
            },
            ex => IsStartupTransient(ex) ? StreamBusRetryKind.Transient : StreamBusRetryKind.Fatal,
            TimeSpan.FromMilliseconds(200),
            TimeSpan.FromSeconds(30),
            int.MaxValue,
            _logger,
            $"Typed consumer {reg.ConsumerType.FullName} on stream '{reg.Stream}'",
            ct).ConfigureAwait(false);
    }

    public async Task StopAsync(CancellationToken ct)
    {
        foreach (var consumer in _registry.ActiveConsumers)
        {
            try { await consumer.DisposeAsync().ConfigureAwait(false); }
            catch (Exception ex) { _logger.LogWarning(ex, "Consumer dispose did not complete cleanly during StopAsync."); }
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
        List<(string Vhost, StreamBusConnectionSupervisor Supervisor)> copy;
        lock (_gate)
        {
            copy = new List<(string, StreamBusConnectionSupervisor)>(_started);
            _started.Clear();
        }
        foreach (var (vhost, supervisor) in copy)
        {
            try { await supervisor.DisposeAsync().ConfigureAwait(false); }
            catch (Exception ex) { _logger.LogWarning(ex, "Supervisor dispose did not complete cleanly during StopAsync for vhost '{Vhost}'.", vhost); }
            finally { _registry.RemoveSupervisor(vhost); }
        }
    }
}

public sealed class StreamBusHealthCheck : IHealthCheck
{
    private readonly IStreamBusRegistry _registry;
    public StreamBusHealthCheck(IStreamBusRegistry registry) => _registry = registry;

    public Task<HealthCheckResult> CheckHealthAsync(HealthCheckContext context, CancellationToken ct = default)
    {
        var states = _registry.DescribeStates().ToList();
        var aggregators = _registry.HealthAggregators.ToList();
        if (aggregators.Count == 0)
        {
            var emptyData = states.ToDictionary(
                r => $"{r.Vhost}|{r.BusType}",
                r => (object)r.State.ToString());
            return Task.FromResult(HealthCheckResult.Degraded("no buses registered.", data: emptyData));
        }

        var snapshots = aggregators
            .Select(a => (Snapshot: a.Snapshot(), Status: a.Compute()))
            .ToList();
        var rows = snapshots.SelectMany(s => s.Snapshot.Registrations).ToList();
        var data = states.ToDictionary(
            r => $"{r.Vhost}|{r.BusType}",
            r => (object)r.State.ToString());
        foreach (var row in rows)
            data[$"{row.Vhost}|{row.Registration}"] = $"{row.Kind}:{row.State}{(row.Reason is null ? "" : $" ({row.Reason})")}";

        var details = rows
            .Where(r => r.State != BusRegistrationState.Attached)
            .Select(r => $"{r.Vhost}|{r.Registration}={r.State}{(r.Reason is null ? "" : $": {r.Reason}")} since {r.Since:O}")
            .ToList();
        var suffix = details.Count == 0 ? "" : " " + string.Join("; ", details);

        var worst = snapshots.Any(s => s.Status == StreamBusHealthAggregator.HealthStatus.Unhealthy)
            ? StreamBusHealthAggregator.HealthStatus.Unhealthy
            : snapshots.Any(s => s.Status == StreamBusHealthAggregator.HealthStatus.Degraded)
                ? StreamBusHealthAggregator.HealthStatus.Degraded
                : StreamBusHealthAggregator.HealthStatus.Healthy;
        return Task.FromResult(worst switch
        {
            StreamBusHealthAggregator.HealthStatus.Healthy =>
                HealthCheckResult.Healthy("All StreamBus registrations attached and connections ready.", data),
            StreamBusHealthAggregator.HealthStatus.Unhealthy =>
                HealthCheckResult.Unhealthy("StreamBus is unhealthy (fatal connection, exhausted declaration, or all typed consumers failed)." + suffix, data: data),
            _ => HealthCheckResult.Degraded("StreamBus is starting or retrying." + suffix, data: data),
        });
    }
}
