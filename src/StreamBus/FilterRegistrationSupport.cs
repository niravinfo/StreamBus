using Microsoft.Extensions.DependencyInjection;

namespace StreamBus;

internal static class FilterRegistrationSupport
{
    /// <summary>
    /// Rejects scoped and transient filter registrations. A filter is resolved
    /// once when the bus is built and cached for the bus lifetime, so a
    /// non-singleton registration would silently become a captive dependency
    /// (and a scoped registration resolved from the root provider is invalid
    /// when scope validation is enabled). Scoped services belong on the
    /// per-publish/per-delivery <c>Services</c> member of the context.
    /// </summary>
    public static void EnsureSingletonLifetime(
        IServiceProvider rootProvider,
        Type? filterType,
        string family)
    {
        ArgumentNullException.ThrowIfNull(rootProvider);
        ArgumentException.ThrowIfNullOrWhiteSpace(family);
        if (filterType is null)
            return;
        if (rootProvider.GetService(typeof(IServiceScopeFactory)) is not IServiceScopeFactory scopeFactory)
            return;

        object? rootInstance;
        try
        {
            rootInstance = rootProvider.GetService(filterType);
        }
        catch (InvalidOperationException ex)
        {
            throw new StreamBusConfigurationException(
                $"{family} filter '{filterType.FullName}' could not be resolved from the root service provider. " +
                $"{family} filters are resolved once per bus and must be registered as singletons; resolve scoped services from the context's Services member. " +
                ex.Message);
        }

        // A missing registration is reported by the caller's own resolution
        // path, which names the filter and the root provider.
        if (rootInstance is null)
            return;

        using var scope = scopeFactory.CreateScope();
        var scopedInstance = scope.ServiceProvider.GetService(filterType);
        if (!ReferenceEquals(rootInstance, scopedInstance))
        {
            throw new StreamBusConfigurationException(
                $"{family} filter '{filterType.FullName}' is registered with a scoped or transient lifetime, " +
                $"but {family.ToLowerInvariant()} filters are resolved once per bus and cached, so a non-singleton registration becomes a captive dependency. " +
                "Register the filter as a singleton and resolve scoped services from the context's Services member.");
        }
    }

    public static TFilter CreateUniversal<TFilter>(
        IServiceProvider provider,
        Type? filterType,
        Func<IServiceProvider, TFilter>? factory,
        TFilter? instance,
        bool isTyped,
        string family)
        where TFilter : class
    {
        ArgumentNullException.ThrowIfNull(provider);
        ArgumentException.ThrowIfNullOrWhiteSpace(family);

        if (isTyped)
        {
            throw new StreamBusConfigurationException(
                $"{family} filter '{filterType}' is typed and cannot be created without a message type.");
        }
        if (instance is not null)
            return instance;
        if (factory is not null)
        {
            return factory(provider)
                ?? throw new StreamBusConfigurationException($"{family} filter factory returned null.");
        }
        return provider.GetService(filterType!) as TFilter
            ?? throw new StreamBusConfigurationException(
                $"{family} filter '{filterType!.FullName}' could not be resolved from the root service provider.");
    }

    public static Type? ClosedFilterType(
        FilterRegistrationKind kind,
        Type? filterType,
        IReadOnlyList<Type> typedMessageTypes,
        Type messageType,
        Type typedInterfaceDefinition,
        string family)
    {
        ArgumentNullException.ThrowIfNull(typedMessageTypes);
        ArgumentNullException.ThrowIfNull(messageType);
        ArgumentNullException.ThrowIfNull(typedInterfaceDefinition);
        ArgumentException.ThrowIfNullOrWhiteSpace(family);

        return kind switch
        {
            FilterRegistrationKind.Universal => null,
            FilterRegistrationKind.ConcreteTyped => typedMessageTypes.Contains(messageType) ? filterType : null,
            FilterRegistrationKind.OpenGenericTyped => CloseOpenGeneric(
                filterType!,
                messageType,
                typedInterfaceDefinition,
                family),
            _ => throw new StreamBusConfigurationException(
                $"{family} filter '{filterType}' has unknown registration kind '{kind}'."),
        };
    }

    public static object ResolveTyped(
        Type? registeredFilterType,
        Type closedFilterType,
        IServiceProvider provider,
        string family)
    {
        ArgumentNullException.ThrowIfNull(closedFilterType);
        ArgumentNullException.ThrowIfNull(provider);
        ArgumentException.ThrowIfNullOrWhiteSpace(family);

        var namedFilterType = registeredFilterType ?? closedFilterType;
        return provider.GetService(closedFilterType)
            ?? throw new StreamBusConfigurationException(
                $"{family} filter '{namedFilterType.FullName}' could not be resolved from the root service provider.");
    }

    private static Type CloseOpenGeneric(
        Type filterType,
        Type messageType,
        Type typedInterfaceDefinition,
        string family)
    {
        Type closedFilterType;
        try
        {
            closedFilterType = filterType.MakeGenericType(messageType);
        }
        catch (ArgumentException ex)
        {
            throw new StreamBusConfigurationException(
                $"{family} filter '{filterType}' cannot be closed for message type '{messageType.FullName}': {ex.Message}");
        }

        var typedInterface = typedInterfaceDefinition.MakeGenericType(messageType);
        if (!typedInterface.IsAssignableFrom(closedFilterType))
        {
            throw new StreamBusConfigurationException(
                $"{family} filter '{filterType}' closed for message type '{messageType.FullName}' does not implement {DisplayName(typedInterfaceDefinition)}<{messageType.Name}>.");
        }
        return closedFilterType;
    }

    private static string DisplayName(Type type)
    {
        var name = type.Name;
        var tick = name.IndexOf('`');
        return tick < 0 ? name : name[..tick];
    }
}
