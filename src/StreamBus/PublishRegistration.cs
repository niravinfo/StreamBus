using System.Diagnostics.CodeAnalysis;

namespace StreamBus;

internal sealed record PublishRegistration
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

    /// <summary>
    /// Compile-time adapter factory supplied by
    /// <see cref="ForTyped{TFilter,TMessage}"/>. When present, the typed filter
    /// is wrapped without closing a generic type at runtime.
    /// </summary>
    public Func<object, IStreamPublishFilter>? TypedAdapterFactory { get; init; }

    public bool IsTyped => Kind != FilterRegistrationKind.Universal;

    /// <summary>
    /// Returns <see langword="true"/> when this registration and
    /// <paramref name="other"/> would both run for at least one message type.
    /// The same typed filter may be registered twice when each registration is
    /// pinned to a disjoint message type; universal and typed registrations
    /// always overlap because the universal one runs for every message type.
    /// </summary>
    public bool ConflictsWith(PublishFilterRegistration other)
    {
        ArgumentNullException.ThrowIfNull(other);
        if (FilterType is null || FilterType != other.FilterType)
            return false;
        if (!IsTyped || !other.IsTyped)
            return true;
        return TypedMessageTypes.Intersect(other.TypedMessageTypes).Any();
    }

    public static PublishFilterRegistration ForType(
        [DynamicallyAccessedMembers(RuntimeGeneric.FilterMembers)] Type filterType)
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

    /// <summary>
    /// Registers a typed publish filter whose message type is pinned at the call
    /// site. The adapter is constructed by the generic method itself, so the
    /// common typed-filter path needs no runtime generic closing.
    /// </summary>
    public static PublishFilterRegistration ForTyped<[DynamicallyAccessedMembers(RuntimeGeneric.FilterMembers)] TFilter, TMessage>()
        where TFilter : class, IStreamPublishFilter<TMessage>
    {
        // Preserve the universal/typed classification rules (including the
        // rejection of filters that also implement the universal interface).
        FilterTypeInspector.Classify(
            typeof(TFilter),
            typeof(IStreamPublishFilter),
            typeof(IStreamPublishFilter<>),
            "Publish");
        return new()
        {
            FilterType = typeof(TFilter),
            Kind = FilterRegistrationKind.ConcreteTyped,
            TypedMessageTypes = new[] { typeof(TMessage) },
            TypedAdapterFactory = filter => new TypedPublishFilterAdapter<TMessage>(
                (IStreamPublishFilter<TMessage>)filter),
        };
    }

    public static PublishFilterRegistration ForInstance(IStreamPublishFilter instance)
        => new() { FilterType = instance.GetType(), Instance = instance };

    public IStreamPublishFilter CreateUniversal(IServiceProvider provider)
        => FilterRegistrationSupport.CreateUniversal(provider, FilterType, Factory, Instance, IsTyped, "Publish");

    public Type? ClosedFilterType(Type messageType)
        => FilterRegistrationSupport.ClosedFilterType(
            Kind,
            FilterType,
            TypedMessageTypes,
            messageType,
            typeof(IStreamPublishFilter<>),
            "Publish");

    /// <summary>
    /// Wraps a resolved typed filter in the adapter for
    /// <paramref name="messageType"/>. Registrations created by
    /// <see cref="ForTyped{TFilter,TMessage}"/> use their compile-time factory;
    /// the remaining (open-generic or interface-derived) registrations close the
    /// adapter at runtime.
    /// </summary>
    public IStreamPublishFilter WrapTyped(Type messageType, object filter)
    {
        ArgumentNullException.ThrowIfNull(messageType);
        ArgumentNullException.ThrowIfNull(filter);
        if (TypedAdapterFactory is { } factory)
        {
            if (!TypedMessageTypes.Contains(messageType))
            {
                throw new StreamBusConfigurationException(
                    $"Publish filter '{FilterType}' is pinned to '{string.Join("', '", TypedMessageTypes.Select(t => t.Name))}' and cannot be wrapped for message type '{messageType.Name}'.");
            }
            return factory(filter);
        }
        return CreateTypedAdapter(messageType, filter);
    }

    public object ResolveTyped(Type closedFilterType, IServiceProvider provider)
    {
        ArgumentNullException.ThrowIfNull(closedFilterType);
        return FilterRegistrationSupport.ResolveTyped(FilterType, closedFilterType, provider, "Publish");
    }

    public static IStreamPublishFilter CreateTypedAdapter(Type messageType, object filter)
    {
        ArgumentNullException.ThrowIfNull(messageType);
        ArgumentNullException.ThrowIfNull(filter);
        return TypedFilterAdapter.Publish(messageType, filter);
    }
}

internal sealed class StreamBusPublishStore
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
        if (filter.FilterType is { } type && filters.Any(filter.ConflictsWith))
        {
            throw new StreamBusConfigurationException(
                $"Publish filter '{type}' is already registered on this bus for one of its message types; duplicate registration would run it twice.");
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

internal sealed class StreamPublishRouter
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
