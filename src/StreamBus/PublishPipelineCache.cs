using System.Collections.Concurrent;

namespace StreamBus;

internal sealed class PublishPipelineCache
{
    private readonly IReadOnlyList<PublishFilterRegistration> _registrations;
    private readonly IServiceProvider? _rootProvider;
    private readonly Dictionary<PublishFilterRegistration, IStreamPublishFilter> _universal = new();
    private readonly ConcurrentDictionary<(Type ClosedFilterType, Type MessageType), IStreamPublishFilter> _typed = new();
    private readonly ConcurrentDictionary<Type, PublishPipeline> _pipelines = new();

    public PublishPipelineCache(
        Type busType,
        IReadOnlyList<PublishFilterRegistration> registrations,
        IServiceProvider? rootProvider)
    {
        ArgumentNullException.ThrowIfNull(busType);
        ArgumentNullException.ThrowIfNull(registrations);

        _registrations = registrations;
        _rootProvider = rootProvider;

        if (registrations.Count == 0)
            return;

        if (rootProvider is null)
        {
            throw new StreamBusConfigurationException(
                $"Publish filters are configured for {busType.FullName}, but no root service provider is available.");
        }

        var seen = new HashSet<Type>();
        foreach (var registration in registrations)
        {
            if (registration.IsTyped)
                continue;

            var filter = registration.CreateUniversal(rootProvider);
            var filterType = FilterIdentity.TypeOf(filter);
            if (!seen.Add(filterType))
            {
                throw new StreamBusConfigurationException(
                    $"Publish filter '{filterType}' is already registered on this bus; duplicate registration would run it twice.");
            }

            _universal.Add(registration, filter);
        }
    }

    public PublishPipeline GetOrCreate(Type messageType)
    {
        ArgumentNullException.ThrowIfNull(messageType);
        return _pipelines.GetOrAdd(messageType, Build);
    }

    private PublishPipeline Build(Type messageType)
    {
        var filters = new List<IStreamPublishFilter>(_registrations.Count);
        var seen = new HashSet<Type>();

        foreach (var registration in _registrations)
        {
            IStreamPublishFilter filter;
            if (!registration.IsTyped)
            {
                filter = _universal[registration];
            }
            else
            {
                var closedFilterType = registration.ClosedFilterType(messageType);
                if (closedFilterType is null)
                    continue;

                filter = _typed.GetOrAdd(
                    (closedFilterType, messageType),
                    _ => registration.CreateTyped(closedFilterType, messageType, _rootProvider!));
            }

            var filterType = FilterIdentity.TypeOf(filter);
            if (!seen.Add(filterType))
            {
                throw new StreamBusConfigurationException(
                    $"Publish filter '{filterType}' is already registered on this bus; duplicate registration would run it twice.");
            }

            filters.Add(filter);
        }

        return new PublishPipeline(filters);
    }
}
