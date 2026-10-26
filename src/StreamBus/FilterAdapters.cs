namespace StreamBus;

internal interface ITypedFilterAdapter
{
    Type FilterType { get; }
}

internal static class FilterIdentity
{
    public static Type TypeOf(object filter)
    {
        ArgumentNullException.ThrowIfNull(filter);
        return filter is ITypedFilterAdapter typed ? typed.FilterType : filter.GetType();
    }
}

internal static class TypedFilterAdapter
{
    public static IStreamPublishFilter Publish(Type messageType, object filter)
    {
        ArgumentNullException.ThrowIfNull(messageType);
        ArgumentNullException.ThrowIfNull(filter);
        return (IStreamPublishFilter)Activator.CreateInstance(
            typeof(TypedPublishFilterAdapter<>).MakeGenericType(messageType),
            filter)!;
    }

    public static IStreamConsumeFilter Consume(Type messageType, object filter)
    {
        ArgumentNullException.ThrowIfNull(messageType);
        ArgumentNullException.ThrowIfNull(filter);
        return (IStreamConsumeFilter)Activator.CreateInstance(
            typeof(TypedConsumeFilterAdapter<>).MakeGenericType(messageType),
            filter)!;
    }
}

internal sealed class TypedPublishFilterAdapter<T> : IStreamPublishFilter, ITypedFilterAdapter
{
    private readonly IStreamPublishFilter<T> _filter;

    public TypedPublishFilterAdapter(IStreamPublishFilter<T> filter)
    {
        _filter = filter;
        FilterType = filter.GetType();
    }

    public Type FilterType { get; }

    public Task OnPublish(StreamPublishFilterContext context, PublishDelegate next, CancellationToken ct)
        => _filter.OnPublish(new StreamPublishFilterContext<T> { Inner = context }, next, ct);
}

internal sealed class TypedConsumeFilterAdapter<T> : IStreamConsumeFilter, ITypedFilterAdapter
{
    private readonly IStreamConsumeFilter<T> _filter;

    public TypedConsumeFilterAdapter(IStreamConsumeFilter<T> filter)
    {
        _filter = filter;
        FilterType = filter.GetType();
    }

    public Type FilterType { get; }

    public bool CanSkip => _filter.CanSkip;

    public Task OnConsume(StreamConsumeFilterContext context, ConsumeDelegate next, CancellationToken ct)
        => _filter.OnConsume(new StreamConsumeFilterContext<T>
        {
            Inner = context,
            Message = context.TypedMessage is T message
                ? message
                : throw new StreamBusConfigurationException(
                    $"Typed consume filter for {typeof(T).FullName} ran without a deserialized message (internal error)."),
        }, next, ct);
}
