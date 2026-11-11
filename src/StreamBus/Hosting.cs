using System.Diagnostics.CodeAnalysis;
using System.Runtime.ExceptionServices;
using System.Text.Json;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Diagnostics.HealthChecks;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;

namespace StreamBus;

/// <summary>
/// Credential-free projection of one bus registration for health/status and
/// diagnostics. The live <see cref="StreamBusOptions"/> stays internal to the
/// store so a password can never leak through a public read accessor.
/// </summary>
public sealed record StreamBusRegistrationView
{
    /// <summary>Virtual host for the registration.</summary>
    public required string Vhost { get; init; }

    /// <summary>RabbitMQ host name.</summary>
    public required string Host { get; init; }

    /// <summary>Stream protocol port.</summary>
    public required int StreamPort { get; init; }

    /// <summary>AMQP protocol port.</summary>
    public required int AmqpPort { get; init; }

    /// <summary>Configured connection heartbeat interval.</summary>
    public required TimeSpan Heartbeat { get; init; }

    /// <summary>Whether TLS is enabled for this registration's connections.</summary>
    public required bool TlsEnabled { get; init; }
}

internal sealed class StreamBusConfigStore
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
            && a.Connection == b.Connection
            && SameTls(a.Tls, b.Tls);

    // TLS identity: a disabled Tls record means TLS-off, exactly like null,
    // so both compare as "off". Two enabled TLS settings must match fully
    // (record equality: only field values matter).
    private static bool SameTls(StreamBusTlsOptions? a, StreamBusTlsOptions? b)
    {
        var aOn = a is { Enabled: true };
        var bOn = b is { Enabled: true };
        return aOn == bOn && (!aOn || a == b);
    }

    internal StreamBusOptions ForVhost(string vhost)
    {
        if (_byVhost.TryGetValue(vhost, out var options))
            return options;
        throw new StreamBusConfigurationException($"No StreamBus registration for vhost '{vhost}'.");
    }

    /// <summary>Credential-free views of every bus registration, for health/status and diagnostics.</summary>
    public IReadOnlyList<StreamBusRegistrationView> Registrations
        => _registrations
            .Select(r => new StreamBusRegistrationView
            {
                Vhost = r.Options.Vhost,
                Host = r.Options.Host,
                StreamPort = r.Options.StreamPort,
                AmqpPort = r.Options.AmqpPort,
                Heartbeat = r.Options.Heartbeat,
                TlsEnabled = r.Options.Tls?.Enabled ?? false,
            })
            .ToList();

    internal IReadOnlyList<(Type BusType, StreamBusOptions Options)> Entries => _registrations;
}

/// <summary>Extension methods that register StreamBus services (buses, hosted service, and health checks) on an <see cref="IServiceCollection"/>.</summary>
public static class ServiceCollectionExtensions
{
    internal static IServiceCollection AddShared(IServiceCollection services, Type busType, StreamBusOptions options)
    {
        var store = GetOrCreateStore(services);
        store.Add(busType, options);
        var publishStore = GetOrCreatePublishStore(services);
        var consumeFilterStore = GetOrCreateConsumeFilterStore(services);
        var router = new StreamPublishRouter(publishStore);
        var endpointStore = GetOrCreateEndpointStore(services);
        IReadOnlyList<DeclaredStream> DeclarationsFor(Type marker)
        {
            var markerVhost = store.Entries.Where(r => r.BusType == marker).Select(r => r.Options.Vhost).FirstOrDefault();
            if (markerVhost is null) return Array.Empty<DeclaredStream>();
            return DeclareSetBuilder.Build(
                endpointStore.Endpoints.Where(e => e.BusType == marker).Select(e => e.Descriptor),
                publishStore.Registrations.Where(r => r.BusType == marker),
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
                DeclarationsFor,
                publishStore,
                sp,
                consumeFilterStore));
        services.AddOptions<StreamBusHostOptions>();
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

    internal static StreamBusConsumeFilterStore GetOrCreateConsumeFilterStore(IServiceCollection services)
    {
        var existing = services.FirstOrDefault(d =>
            d.ServiceType == typeof(StreamBusConsumeFilterStore) && d.ImplementationInstance is StreamBusConsumeFilterStore);
        if (existing?.ImplementationInstance is StreamBusConsumeFilterStore store) return store;
        var created = new StreamBusConsumeFilterStore();
        services.AddSingleton<StreamBusConsumeFilterStore>(created);
        return created;
    }

}

/// <summary>One typed (endpoint-configured) consumer to start when the host starts.</summary>
internal sealed record TypedConsumerRegistration
{
    /// <summary>Bus marker the endpoint belongs to.</summary>
    public required Type BusType { get; init; }
    /// <summary>Message type the consumer handles.</summary>
    public required Type MessageType { get; init; }
    /// <summary>Consumer type implementing <c>IConsumer&lt;T&gt;</c>.</summary>
    public required Type ConsumerType { get; init; }
    /// <summary>Stream (or super stream) the endpoint consumes.</summary>
    public required string Stream { get; init; }
    /// <summary>Consume options for the endpoint (consumerGroup, retry, error destination...).</summary>
    public required ConsumeOptions Options { get; init; }
}

internal sealed class StreamBusConsumerStore
{
    private readonly List<TypedConsumerRegistration> _registrations = new();

    internal void Add(Type busType, Type messageType, Type consumerType, string stream, ConsumeOptions options)
    {
        ArgumentNullException.ThrowIfNull(busType);
        ArgumentNullException.ThrowIfNull(messageType);
        ArgumentNullException.ThrowIfNull(consumerType);
        ArgumentException.ThrowIfNullOrWhiteSpace(stream);
        ArgumentNullException.ThrowIfNull(options);
        ConsumerGroupValidation.Require(options.ConsumerGroup, stream);
        if (_registrations.Any(r => r.BusType == busType && r.MessageType == messageType && r.ConsumerType == consumerType && r.Stream == stream && r.Options.ConsumerGroup == options.ConsumerGroup))
            throw new StreamBusConfigurationException($"Duplicate typed consumer registration for {consumerType.FullName} on stream '{stream}' with group '{options.ConsumerGroup}' (marker {busType.FullName}).");
        _registrations.Add(new TypedConsumerRegistration { BusType = busType, MessageType = messageType, ConsumerType = consumerType, Stream = stream, Options = options });
    }

    /// <summary>All typed consumer registrations, in registration order.</summary>
    public IReadOnlyList<TypedConsumerRegistration> Registrations => _registrations;
}

internal interface ITypedConsumerInvoker
{
    Func<ConsumeContext, Task> Create();
}

internal static class TypedConsumerHandler
{
    [RequiresUnreferencedCode(RuntimeGeneric.TrimMessage)]
    [RequiresDynamicCode(RuntimeGeneric.AotMessage)]
    public static Func<ConsumeContext, Task> Create(Type messageType, Type consumerType)
    {
        ArgumentNullException.ThrowIfNull(messageType);
        ArgumentNullException.ThrowIfNull(consumerType);

        Type consumerInterface;
        try
        {
            consumerInterface = typeof(IConsumer<>).MakeGenericType(messageType);
        }
        catch (ArgumentException)
        {
            throw new StreamBusConfigurationException(
                $"Consumer {consumerType.FullName} cannot consume {messageType.FullName} (must implement IConsumer<{messageType.Name}>).");
        }

        if (!consumerInterface.IsAssignableFrom(consumerType))
        {
            throw new StreamBusConfigurationException(
                $"Consumer {consumerType.FullName} cannot consume {messageType.FullName} (must implement IConsumer<{messageType.Name}>).");
        }

        var invoker = RuntimeGeneric.Create<ITypedConsumerInvoker>(
            typeof(TypedConsumerInvoker<,>),
            messageType,
            consumerType);
        return invoker.Create();
    }
}

internal sealed class TypedConsumerInvoker<T, TConsumer> : ITypedConsumerInvoker
    where TConsumer : class, IConsumer<T>
{
    public Func<ConsumeContext, Task> Create()
        => async context =>
        {
            if (context is not ConsumeContext<T> typed)
            {
                throw new StreamBusConfigurationException(
                    $"Typed consumer {typeof(TConsumer).FullName} for {typeof(T).FullName} received a {context.GetType().FullName} context without a deserialized message (internal error).");
            }

            var consumer = context.Services.GetRequiredService<TConsumer>();
            await consumer.Consume(typed).ConfigureAwait(false);
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
    private readonly StreamBusHostOptions _hosting;
    private CancellationTokenSource? _startupCts;

    internal StreamBusHostedService(
        StreamBusConfigStore store,
        IStreamBusRegistry registry,
        ILogger<StreamBusHostedService> logger,
        IServiceScopeFactory scopeFactory,
        StreamBusConsumerStore consumerStore,
        StreamBusEndpointStore endpointStore,
        StreamBusPublishStore publishStore,
        StreamBusHostOptions options)
    {
        ArgumentNullException.ThrowIfNull(store);
        ArgumentNullException.ThrowIfNull(registry);
        ArgumentNullException.ThrowIfNull(logger);
        ArgumentNullException.ThrowIfNull(scopeFactory);
        ArgumentNullException.ThrowIfNull(consumerStore);
        ArgumentNullException.ThrowIfNull(endpointStore);
        ArgumentNullException.ThrowIfNull(publishStore);
        ArgumentNullException.ThrowIfNull(options);
        _store = store;
        _registry = registry;
        _logger = logger;
        _scopes = scopeFactory;
        _consumers = consumerStore;
        _endpoints = endpointStore;
        _publishes = publishStore;
        _hosting = options;
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
            services.GetRequiredService<IOptions<StreamBusHostOptions>>().Value);
    }

    /// <summary>Starts every registered supervisor, declares derived topology and error queues, and starts typed consumers; blocking per hosting options.</summary>
    public async Task StartAsync(CancellationToken cancellationToken)
    {
        StreamBusHostOptions.ValidateStart(_hosting);
        var starters = new List<(TypedConsumerRegistration Reg, string Vhost, IStreamSerializer Serializer)>();
        foreach (var reg in _consumers.Registrations)
        {
            var match = _store.Entries.FirstOrDefault(r => r.BusType == reg.BusType);
            if (match.BusType is null)
                throw new StreamBusConfigurationException($"Typed consumer {reg.ConsumerType.FullName} on stream '{reg.Stream}' references marker {reg.BusType.FullName} with no AddStreamBus registration. Register the bus (Add-call order does not matter).");
            starters.Add((reg, match.Options.Vhost, match.Options.Serializer ?? SystemTextJsonStreamSerializer.Default));
        }
        var supervisors = new List<(string Vhost, TimeSpan? Timeout, StreamBusConnectionSupervisor Supervisor)>();
        foreach (var (busType, options) in _store.Entries)
            _registry.GetOrCreateEntry(busType, options.Vhost);
        foreach (var vhost in _store.Entries.Select(r => r.Options.Vhost).Distinct())
        {
            var supervisor = _registry.SupervisorFor(vhost);
            supervisors.Add((vhost, _hosting.StartTimeout, supervisor));
            StartSupervisor(vhost, supervisor, cancellationToken);
        }
        CancellationTokenSource startupCts;
        lock (_gate)
        {
            _startupCts?.Dispose();
            _startupCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
            startupCts = _startupCts;
        }
        var blocking = _hosting.WaitUntilStarted;
        var declareTasks = new List<Task>();
        var consumerTasks = new List<Task>();
        foreach (var (busType, busOptions) in _store.Entries)
        {
            var endpoints = _endpoints.Endpoints
                .Where(e => e.BusType == busType)
                .Select(e => e.Descriptor)
                .ToList();
            var publishes = _publishes.Registrations
                .Where(r => r.BusType == busType)
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

        foreach (var group in starters
            .Where(s => s.Reg.Options.DeadLetter is not null && s.Reg.Options.OnExhausted is null)
            .GroupBy(s => s.Vhost, StringComparer.Ordinal))
        {
            // Single owner for the cohort filter: this is the same helper the
            // drain/declare path pins in ErrorQueueTests, so the two can't drift.
            var queues = ErrorQueueTransport.CollectQueueDeclarations(group.Select(s => s.Reg));
            if (queues.Count == 0) continue;
            var vhost = group.Key;
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
        if (!_hosting.WaitUntilStarted) return;
        var startupTimeout = _hosting is { WaitUntilStarted: true, StartTimeout: { } value }
            ? value
            : (TimeSpan?)null;
        using var cts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        if (startupTimeout is { } budget)
            cts.CancelAfter(budget);
        foreach (var (vhost, timeout, supervisor) in supervisors)
        {
            try
            {
                var readiness = await supervisor.WaitUntilReadyAsync(ReadinessPolicy.Forever, cts.Token).ConfigureAwait(false);
                if (readiness == BusReadiness.Fatal)
                    throw new StreamBusFatalException("A StreamBus connection reported Fatal during startup.");
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
            {
                // Host is stopping: cancelling the STARTUP wait is NOT a "Timed out" event.
                throw;
            }
            catch (OperationCanceledException) when (!cancellationToken.IsCancellationRequested)
            {
                var effective = timeout ?? startupTimeout;
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
                        if (t.Exception?.InnerExceptions.Any(e => e is StreamBusStreamConfigurationConflictException or StreamBusConfigurationException) == true)
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
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
            {
                // Host is stopping: this is a shutdown, not a startup budget expiry.
                throw;
            }
            catch (OperationCanceledException) when (!cancellationToken.IsCancellationRequested)
            {
                try { startupCts.Cancel(); }
                catch (ObjectDisposedException) { }
                var conflict = startupTasks
                    .SelectMany(t => t.Exception?.InnerExceptions.AsEnumerable() ?? Enumerable.Empty<Exception>())
                    .OfType<StreamBusStreamConfigurationConflictException>()
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
        if (_hosting.WaitUntilConsumersAttached)
        {
            var consumerVhosts = starters.Select(s => s.Vhost).Distinct().ToList();
            while (true)
            {
                var rows = consumerVhosts
                    .SelectMany(v => _registry.HealthFor(v).Snapshot().Registrations)
                    .Where(r => r.Kind == "consumer")
                    .ToList();
                if (rows.Count == 0 || rows.All(r => r.State == BusRegistrationState.Attached))
                    break;
                try
                {
                    await Task.Delay(TimeSpan.FromMilliseconds(25), cts.Token).ConfigureAwait(false);
                }
                catch (OperationCanceledException) when (!cancellationToken.IsCancellationRequested)
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
        CancellationToken cancellationToken)
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
                    if (!entry.IsSuperStream && entry.Source is DeclaredStreamSource.Publish or DeclaredStreamSource.DeadLetter)
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
                    var who = entry.Source == DeclaredStreamSource.Endpoint ? $"endpoint '{entry.Name}'" : $"publish registration for '{entry.Name}'";
                    if (entry.IsSuperStream)
                    {
                        if (probe.PlainExists)
                            throw new StreamBusStreamConfigurationConflictException(entry.Name,
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
                            throw new StreamBusStreamConfigurationConflictException(entry.Name,
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
                if (ex is StreamBusStreamConfigurationConflictException or StreamBusConfigurationException or StreamBusFatalException)
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
            cancellationToken).ConfigureAwait(false);
    }

    private async Task DeclareQueuesUntilReadyAsync(string vhost, IReadOnlyList<DeadLetterOptions> queues, CancellationToken cancellationToken)
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
                if (ex is StreamBusStreamConfigurationConflictException or StreamBusFatalException)
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
            cancellationToken).ConfigureAwait(false);
    }

    private async Task StartConsumerUntilReadyAsync(TypedConsumerRegistration reg, string vhost, IStreamSerializer serializer, CancellationToken cancellationToken)
    {
        var entry = _registry.GetOrCreateEntry(reg.BusType, vhost);
        var consumerGroup = ConsumerGroupValidation.Require(reg.Options.ConsumerGroup, reg.Stream);
        var options = reg.Options with { ConsumerGroup = consumerGroup };
        if (options.OnExhausted is not null && options.ShadowedDeadLetter is { } shadowed)
            _logger.LogWarning(
                "ReceiveEndpoint '{Stream}' sets both OnExhausted and a dead-letter destination '{Destination}'; the callback owns exhaustion and the destination is shadowed.",
                reg.Stream, shadowed.Name);
        var handler = TypedConsumerHandler.Create(reg.MessageType, reg.ConsumerType);
        var consumers = _registry is StreamBusRegistry concrete ? concrete.ConsumersFor(vhost) : null;
        await StreamRuntimeLoop.RetryUntilAsync(
            async attemptCt =>
            {
                attemptCt.ThrowIfCancellationRequested();
                if (consumers is not null)
                    await consumers.ConsumeDeclaredEndpointAsync(reg.BusType, reg.Stream, options, handler, attemptCt, serializer).ConfigureAwait(false);
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
            cancellationToken).ConfigureAwait(false);
    }

    // Starting a supervisor never fails: StartAsync is fire-and-forget and
    // RunLoopAsync owns every failure by design (transient -> backoff and
    // retry, fatal -> State = Fatal), so readiness is surfaced through
    // State / WaitUntilReadyAsync instead of exceptions. Record the
    // supervisor immediately so StopAsync disposes every supervisor it
    // started, whatever connection state they end up in.
    private void StartSupervisor(string vhost, StreamBusConnectionSupervisor supervisor, CancellationToken cancellationToken)
    {
        supervisor.StartAsync(cancellationToken);
        lock (_gate) _started.Add((vhost, supervisor));
    }

    /// <summary>Disposes active consumers, cancels startup, then disposes every supervisor and the error-queue transports.</summary>
    public async Task StopAsync(CancellationToken cancellationToken)
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
        // Error-queue transports own live AMQP connections; dispose them last,
        // after supervisors, so in-flight dead-letter hand-offs finish first.
        await _registry.DisposeAsync().ConfigureAwait(false);
    }
}

/// <summary>
/// Registers as the <c>streambus</c> health check: healthy only when every
/// registration is attached and every connection ready; degraded while
/// starting/retrying (also when no buses are registered); unhealthy on fatal
/// connections, exhausted declarations, or all typed consumers failed. Data
/// rows are <c>{vhost}|{busType}</c> (connection state) and
/// <c>{vhost}|{registration}</c> (one per consumer/declaration).
/// </summary>
public sealed class StreamBusHealthCheck : IHealthCheck
{
    private readonly IStreamBusRegistry _registry;

    /// <summary>Creates the check over the shared bus registry.</summary>
    public StreamBusHealthCheck(IStreamBusRegistry registry) => _registry = registry;

    /// <summary>Aggregates every bus connection state and health snapshot into one result.</summary>
    /// <param name="context">The health-check context.</param>
    /// <param name="cancellationToken">Ignored; the result is computed from live snapshots.</param>
    public Task<HealthCheckResult> CheckHealthAsync(HealthCheckContext context, CancellationToken cancellationToken = default)
    {
        var states = _registry.DescribeStates().ToList();
        var aggregators = _registry.HealthAggregators.ToList();
        if (aggregators.Count == 0)
        {
            var emptyData = new Dictionary<string, object>();
            foreach (var state in states)
                emptyData[$"bus:{state.Vhost}|{state.BusType}"] = state.State.ToString();
            return Task.FromResult(HealthCheckResult.Degraded("no buses registered.", data: emptyData));
        }

        var snapshots = aggregators
            .Select(a => (Snapshot: a.Snapshot(), Status: a.Compute()))
            .ToList();
        var rows = snapshots.SelectMany(s => s.Snapshot.Registrations).ToList();
        // Bus markers and registration names are separate key namespaces; the
        // prefixes keep a free-form registration name that collides with a bus
        // type name from silently overwriting (or being confused with) a bus
        // state row.
        var data = new Dictionary<string, object>();
        foreach (var state in states)
            data[$"bus:{state.Vhost}|{state.BusType}"] = state.State.ToString();
        foreach (var row in rows)
            data[$"reg:{row.Vhost}|{row.Registration}"] = $"{row.Kind}:{row.State}{(row.Reason is null ? "" : $" ({row.Reason})")}";

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
