using System.Diagnostics.CodeAnalysis;

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
    [RequiresUnreferencedCode(RuntimeGeneric.TrimMessage)]
    [RequiresDynamicCode(RuntimeGeneric.AotMessage)]
    public static IStreamPublishFilter Publish(Type messageType, object filter)
    {
        ArgumentNullException.ThrowIfNull(messageType);
        ArgumentNullException.ThrowIfNull(filter);
        return RuntimeGeneric.Create<IStreamPublishFilter>(
            typeof(TypedPublishFilterAdapter<>),
            new[] { messageType },
            filter);
    }

    [RequiresUnreferencedCode(RuntimeGeneric.TrimMessage)]
    [RequiresDynamicCode(RuntimeGeneric.AotMessage)]
    public static IStreamConsumeFilter Consume(Type messageType, object filter)
    {
        ArgumentNullException.ThrowIfNull(messageType);
        ArgumentNullException.ThrowIfNull(filter);
        return RuntimeGeneric.Create<IStreamConsumeFilter>(
            typeof(TypedConsumeFilterAdapter<>),
            new[] { messageType },
            filter);
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

    public Task OnPublish(PublishContext context, PublishDelegate next)
        => _filter.OnPublish(
            context as PublishContext<T>
                ?? throw new StreamBusConfigurationException(
                    $"Typed publish filter for {typeof(T).FullName} ran without a typed publish context (internal error)."),
            next);
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

    public Task OnConsume(ConsumeContext context, ConsumeDelegate next)
        => _filter.OnConsume(
            context as ConsumeContext<T>
                ?? throw new StreamBusConfigurationException(
                    $"Typed consume filter for {typeof(T).FullName} ran without a typed consume context (internal error)."),
            next);
}
