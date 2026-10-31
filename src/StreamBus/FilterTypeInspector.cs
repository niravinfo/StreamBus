namespace StreamBus;

internal enum FilterRegistrationKind
{
    Universal,
    ConcreteTyped,
    OpenGenericTyped,
}

internal static class FilterTypeInspector
{
    public static (FilterRegistrationKind Kind, IReadOnlyList<Type> MessageTypes) Classify(
        Type filterType,
        Type universalInterface,
        Type typedInterfaceDefinition,
        string family)
    {
        ArgumentNullException.ThrowIfNull(filterType);
        ArgumentNullException.ThrowIfNull(universalInterface);
        ArgumentNullException.ThrowIfNull(typedInterfaceDefinition);
        ArgumentException.ThrowIfNullOrWhiteSpace(family);

        if (!filterType.IsClass || filterType.IsAbstract)
        {
            throw new StreamBusConfigurationException(
                $"{family} filter type '{filterType}' must implement {Describe(universalInterface)} or {Describe(typedInterfaceDefinition)} as a concrete, non-abstract class.");
        }

        if (filterType.IsGenericTypeDefinition)
        {
            var parameters = filterType.GetGenericArguments();
            if (parameters.Length != 1)
            {
                throw new StreamBusConfigurationException(
                    $"{family} filter type '{filterType}' must declare exactly one generic parameter; found {parameters.Length}.");
            }

            if (universalInterface.IsAssignableFrom(filterType))
            {
                var typedMessageTypesForUniversal = TypedMessageTypes(filterType, typedInterfaceDefinition);
                if (typedMessageTypesForUniversal.Count > 0)
                {
                    throw new StreamBusConfigurationException(
                        $"{family} filter type '{filterType}' implements both {Describe(universalInterface)} and {Describe(typedInterfaceDefinition)}; register universal and typed filters separately.");
                }

                throw new StreamBusConfigurationException(
                    $"{family} filter type '{filterType}' is an open generic universal filter implementing only {Describe(universalInterface)}; open generic registration supports only {Describe(typedInterfaceDefinition)}, so a closed universal filter must be registered.");
            }

            var parameter = parameters[0];
            var typedMessageTypes = TypedMessageTypes(filterType, typedInterfaceDefinition);
            if (!typedMessageTypes.Contains(parameter))
            {
                throw new StreamBusConfigurationException(
                    $"{family} filter type '{filterType}' must implement {Describe(typedInterfaceDefinition)} using the same generic parameter as the filter type.");
            }

            return (FilterRegistrationKind.OpenGenericTyped, typedMessageTypes);
        }

        if (filterType.ContainsGenericParameters)
        {
            throw new StreamBusConfigurationException(
                $"{family} filter type '{filterType}' must be a closed type that implements {Describe(universalInterface)} or {Describe(typedInterfaceDefinition)}; partially open constructed types cannot be registered.");
        }

        var universal = universalInterface.IsAssignableFrom(filterType);
        var typedTypes = TypedMessageTypes(filterType, typedInterfaceDefinition);
        if (universal && typedTypes.Count > 0)
        {
            throw new StreamBusConfigurationException(
                $"{family} filter type '{filterType}' implements both {Describe(universalInterface)} and {Describe(typedInterfaceDefinition)}; register universal and typed filters separately.");
        }
        if (!universal && typedTypes.Count == 0)
        {
            throw new StreamBusConfigurationException(
                $"{family} filter type '{filterType}' must implement either {Describe(universalInterface)} or {Describe(typedInterfaceDefinition)}.");
        }

        return universal
            ? (FilterRegistrationKind.Universal, Array.Empty<Type>())
            : (FilterRegistrationKind.ConcreteTyped, typedTypes);
    }

    private static List<Type> TypedMessageTypes(Type filterType, Type typedInterfaceDefinition)
        => filterType.GetInterfaces()
            .Where(i => i.IsGenericType && i.GetGenericTypeDefinition() == typedInterfaceDefinition)
            .Select(i => i.GetGenericArguments()[0])
            .Distinct()
            .ToList();

    private static string Describe(Type type)
    {
        if (!type.IsGenericTypeDefinition)
            return type.Name;

        var name = type.Name;
        var tick = name.IndexOf('`');
        return tick < 0 ? name : $"{name[..tick]}<T>";
    }
}
