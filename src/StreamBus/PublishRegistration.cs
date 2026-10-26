namespace StreamBus;

public sealed record PublishRegistration
{
    public required Type BusMarker { get; init; }
    public required Type MessageType { get; init; }
    public required string Stream { get; init; }
    public required bool IsSuper { get; init; }
    public int? Partitions { get; init; }
    public TimeSpan? MaxAge { get; init; }
    public ulong? MaxLengthBytes { get; init; }
    public int? MaxSegmentSizeBytes { get; init; }
    public StreamLeaderLocator? LeaderLocator { get; init; }
}

internal sealed class PublishFilterRegistration
{
    public Type? FilterType { get; init; }
    public Func<IServiceProvider, IStreamPublishFilter>? Factory { get; init; }
    public IStreamPublishFilter? Instance { get; init; }
    public FilterRegistrationKind Kind { get; init; } = FilterRegistrationKind.Universal;
    public IReadOnlyList<Type> TypedMessageTypes { get; init; } = Array.Empty<Type>();
    public bool IsTyped => Kind != FilterRegistrationKind.Universal;

    public static PublishFilterRegistration ForType(Type filterType)
    {
        var classification = FilterTypeInspector.Classify(
            filterType,
            typeof(IStreamPublishFilter),
            typeof(IStreamPublishFilter<>),
            "Publish");
        return new()
        {
            FilterType = filterType,
            Kind = classification.Kind,
            TypedMessageTypes = classification.MessageTypes,
        };
    }

    public static PublishFilterRegistration ForFactory(Func<IServiceProvider, IStreamPublishFilter> factory)
        => new() { Factory = factory };

    public static PublishFilterRegistration ForInstance(IStreamPublishFilter instance)
        => new() { FilterType = instance.GetType(), Instance = instance };

    public IStreamPublishFilter Create(IServiceProvider provider) => CreateUniversal(provider);

    public IStreamPublishFilter CreateUniversal(IServiceProvider provider)
    {
        ArgumentNullException.ThrowIfNull(provider);
        if (IsTyped)
        {
            throw new StreamBusConfigurationException(
                $"Publish filter '{FilterType}' is typed and cannot be created without a message type.");
        }
        if (Instance is { } instance)
            return instance;
        if (Factory is { } factory)
        {
            return factory(provider)
                ?? throw new StreamBusConfigurationException("Publish filter factory returned null.");
        }
        return provider.GetService(FilterType!) as IStreamPublishFilter
            ?? throw new StreamBusConfigurationException(
                $"Publish filter '{FilterType!.FullName}' could not be resolved from the root service provider.");
    }

    public Type? ClosedFilterType(Type messageType)
    {
        ArgumentNullException.ThrowIfNull(messageType);
        return Kind switch
        {
            FilterRegistrationKind.Universal => null,
            FilterRegistrationKind.ConcreteTyped => TypedMessageTypes.Contains(messageType) ? FilterType : null,
            FilterRegistrationKind.OpenGenericTyped => CloseOpenGeneric(messageType),
            _ => throw new StreamBusConfigurationException(
                $"Publish filter '{FilterType}' has unknown registration kind '{Kind}'."),
        };
    }

    public IStreamPublishFilter CreateTyped(Type closedFilterType, Type messageType, IServiceProvider provider)
    {
        ArgumentNullException.ThrowIfNull(closedFilterType);
        ArgumentNullException.ThrowIfNull(messageType);
        ArgumentNullException.ThrowIfNull(provider);
        var filter = provider.GetService(closedFilterType)
            ?? throw new StreamBusConfigurationException(
                $"Publish filter '{FilterType?.FullName ?? closedFilterType.FullName}' could not be resolved from the root service provider.");
        return TypedFilterAdapter.Publish(messageType, filter);
    }

    private Type CloseOpenGeneric(Type messageType)
    {
        Type closedFilterType;
        try
        {
            closedFilterType = FilterType!.MakeGenericType(messageType);
        }
        catch (ArgumentException ex)
        {
            throw new StreamBusConfigurationException(
                $"Publish filter '{FilterType}' cannot be closed for message type '{messageType.FullName}': {ex.Message}");
        }

        var typedInterface = typeof(IStreamPublishFilter<>).MakeGenericType(messageType);
        if (!typedInterface.IsAssignableFrom(closedFilterType))
        {
            throw new StreamBusConfigurationException(
                $"Publish filter '{FilterType}' closed for message type '{messageType.FullName}' does not implement IStreamPublishFilter<{messageType.Name}>.");
        }
        return closedFilterType;
    }
}

public sealed class StreamBusPublishStore
{
    private readonly List<PublishRegistration> _registrations = new();
    private readonly Dictionary<Type, List<PublishFilterRegistration>> _filters = new();
    private readonly HashSet<Type> _sealedBusTypes = new();

    internal void AddFilter(Type busMarker, PublishFilterRegistration filter)
    {
        ArgumentNullException.ThrowIfNull(busMarker);
        ArgumentNullException.ThrowIfNull(filter);
        if (_sealedBusTypes.Contains(busMarker))
        {
            throw new StreamBusConfigurationException(
                $"Publish filters cannot be registered after bus {busMarker.FullName} is built.");
        }
        if (!_filters.TryGetValue(busMarker, out var filters))
        {
            filters = new List<PublishFilterRegistration>();
            _filters[busMarker] = filters;
        }
        if (filter.FilterType is { } type && filters.Any(existing => existing.FilterType == type))
        {
            throw new StreamBusConfigurationException(
                $"Publish filter '{type}' is already registered on this bus; duplicate registration would run it twice.");
        }
        filters.Add(filter);
    }

    internal IReadOnlyList<PublishFilterRegistration> FiltersFor(Type busMarker)
        => _filters.TryGetValue(busMarker, out var filters)
            ? filters
            : Array.Empty<PublishFilterRegistration>();

    internal void Seal(Type busMarker) => _sealedBusTypes.Add(busMarker);

    internal void Add(Type busMarker, Type messageType, string stream, bool isSuper, StreamOptions? spec = null)
    {
        ArgumentNullException.ThrowIfNull(busMarker);
        ArgumentNullException.ThrowIfNull(messageType);
        ArgumentException.ThrowIfNullOrWhiteSpace(stream);
        StreamOptionsValidation.Validate(spec ?? new StreamOptions(), $"Publish '{stream}'", allowPartitions: isSuper);
        if (_registrations.Any(r => r.BusMarker == busMarker && r.MessageType == messageType && r.Stream == stream))
            throw new StreamBusConfigurationException($"Duplicate publish registration for {messageType.FullName} on stream '{stream}' (marker {busMarker.FullName}).");
        _registrations.Add(new PublishRegistration
        {
            BusMarker = busMarker,
            MessageType = messageType,
            Stream = stream,
            IsSuper = isSuper,
            Partitions = spec?.Partitions,
            MaxAge = spec?.MaxAge,
            MaxLengthBytes = spec?.MaxLengthBytes,
            MaxSegmentSizeBytes = spec?.MaxSegmentSizeBytes,
            LeaderLocator = spec?.LeaderLocator,
        });
    }

    public IReadOnlyList<PublishRegistration> Registrations => _registrations;
}

public sealed class StreamPublishRouter
{
    private readonly StreamBusPublishStore _store;

    public StreamPublishRouter(StreamBusPublishStore store)
    {
        ArgumentNullException.ThrowIfNull(store);
        _store = store;
    }

    public PublishRegistration Resolve(Type busMarker, Type messageType)
    {
        ArgumentNullException.ThrowIfNull(busMarker);
        ArgumentNullException.ThrowIfNull(messageType);
        var matches = _store.Registrations
            .Where(r => r.BusMarker == busMarker && r.MessageType == messageType)
            .ToList();
        if (matches.Count == 0)
            throw new StreamBusConfigurationException($"No publish registration for message {messageType.FullName} on bus {busMarker.FullName}. Register it with cfg.Publish<{messageType.Name}>(stream) or cfg.PublishSuper<{messageType.Name}>(stream).");
        if (matches.Count > 1)
        {
            var streams = string.Join("', '", matches.Select(m => m.Stream));
            throw new StreamBusConfigurationException($"Ambiguous publish registration for message {messageType.FullName} on bus {busMarker.FullName}: registered for streams '{streams}'. Publish with an explicit stream: PublishAsync<T>(stream, message, ...) or PublishSuperAsync<T>(stream, message, key, ...).");
        }
        return matches[0];
    }
}
