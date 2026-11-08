using System.Collections.Concurrent;
using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;

namespace StreamBus;

/// <summary>
/// Universal consume filter that runs for every delivery on the bus.
/// </summary>
/// <remarks>
/// Filter instances are resolved once when the consumer attaches and cached
/// for the bus lifetime, so a filter must be thread-safe. Register filter
/// types with a singleton lifetime: a scoped or transient registration is
/// rejected when the consumer attaches. Scoped services are available
/// through <see cref="ConsumeContext.Services"/>.
/// </remarks>
public interface IStreamConsumeFilter
{
    /// <summary>
    /// Runs before the consumer handler. Call <paramref name="next"/> to handle
    /// the message, or return without calling it to filter the delivery.
    /// </summary>
    /// <param name="context">The shared consume context for this delivery.</param>
    /// <param name="next">
    /// The continuation. Call it at most once and await the returned task
    /// before returning: a filter that returns while the continuation is still
    /// running is rejected. Zero calls mark the delivery as handled and advance
    /// the offset without invoking the handler.
    /// </param>
    /// <returns>A task that completes when the filter and any continuation finish.</returns>
    Task OnConsume(ConsumeContext context, ConsumeNext next);
}

/// <summary>
/// Consume filter that runs only on endpoints whose message type is
/// <typeparamref name="T"/>.
/// </summary>
/// <typeparam name="T">The endpoint message type.</typeparam>
/// <remarks>
/// Filter instances are resolved once when the consumer attaches and cached
/// for the bus lifetime, so a filter must be thread-safe. Register filter
/// types with a singleton lifetime: a scoped or transient registration is
/// rejected when the consumer attaches. Scoped services are available
/// through <see cref="ConsumeContext.Services"/>.
/// </remarks>
public interface IStreamConsumeFilter<T>
{
    /// <summary>
    /// Runs before the consumer handler. Call <paramref name="next"/> to handle
    /// the message, or return without calling it to filter the delivery.
    /// </summary>
    /// <param name="context">The typed consume context for this delivery.</param>
    /// <param name="next">
    /// The continuation. Call it at most once and await the returned task
    /// before returning: a filter that returns while the continuation is still
    /// running is rejected. Zero calls mark the delivery as handled and advance
    /// the offset without invoking the handler.
    /// </param>
    /// <returns>A task that completes when the filter and any continuation finish.</returns>
    Task OnConsume(ConsumeContext<T> context, ConsumeNext next);
}

/// <summary>
/// Continues a consume pipeline. A filter may call it zero or one times: zero
/// skips the delivery (the handler is not invoked and the offset advances);
/// more than one call is rejected. A filter that calls it must await the
/// returned task before returning.
/// </summary>
public delegate Task ConsumeNext();

internal sealed class ConsumeFilterRegistration
{
    public Type? FilterType { get; init; }
    public Func<IServiceProvider, IStreamConsumeFilter>? Factory { get; init; }
    public IStreamConsumeFilter? Instance { get; init; }
    public FilterRegistrationKind Kind { get; init; } = FilterRegistrationKind.Universal;
    public IReadOnlyList<Type> TypedMessageTypes { get; init; } = Array.Empty<Type>();

    /// <summary>
    /// Compile-time adapter factory supplied by
    /// <see cref="ForTyped{TFilter,TMessage}"/>. When present, the typed filter
    /// is wrapped without closing a generic type at runtime.
    /// </summary>
    public Func<object, IStreamConsumeFilter>? TypedAdapterFactory { get; init; }

    public bool IsTyped => Kind != FilterRegistrationKind.Universal;

    /// <summary>
    /// Returns <see langword="true"/> when this registration and
    /// <paramref name="other"/> would both run for at least one message type.
    /// The same typed filter may be registered twice when each registration is
    /// pinned to a disjoint message type; universal and typed registrations
    /// always overlap because the universal one runs for every message type.
    /// </summary>
    public bool ConflictsWith(ConsumeFilterRegistration other)
    {
        ArgumentNullException.ThrowIfNull(other);
        if (FilterType is null || FilterType != other.FilterType)
            return false;
        if (!IsTyped || !other.IsTyped)
            return true;
        return TypedMessageTypes.Intersect(other.TypedMessageTypes).Any();
    }

    public static ConsumeFilterRegistration ForType(
        [DynamicallyAccessedMembers(RuntimeGeneric.FilterMembers)] Type filterType)
    {
        var classification = FilterTypeInspector.Classify(
            filterType,
            typeof(IStreamConsumeFilter),
            typeof(IStreamConsumeFilter<>),
            "Consume");
        return new()
        {
            FilterType = filterType,
            Kind = classification.Kind,
            TypedMessageTypes = classification.MessageTypes,
        };
    }

    public static ConsumeFilterRegistration ForFactory(Func<IServiceProvider, IStreamConsumeFilter> factory)
        => new() { Factory = factory };

    /// <summary>
    /// Registers a typed consume filter whose message type is pinned at the call
    /// site. The adapter is constructed by the generic method itself, so the
    /// common typed-filter path needs no runtime generic closing.
    /// </summary>
    public static ConsumeFilterRegistration ForTyped<[DynamicallyAccessedMembers(RuntimeGeneric.FilterMembers)] TFilter, TMessage>()
        where TFilter : class, IStreamConsumeFilter<TMessage>
    {
        // Preserve the universal/typed classification rules (including the
        // rejection of filters that also implement the universal interface).
        FilterTypeInspector.Classify(
            typeof(TFilter),
            typeof(IStreamConsumeFilter),
            typeof(IStreamConsumeFilter<>),
            "Consume");
        return new()
        {
            FilterType = typeof(TFilter),
            Kind = FilterRegistrationKind.ConcreteTyped,
            TypedMessageTypes = new[] { typeof(TMessage) },
            TypedAdapterFactory = filter => new TypedConsumeFilterAdapter<TMessage>(
                (IStreamConsumeFilter<TMessage>)filter),
        };
    }

    public static ConsumeFilterRegistration ForInstance(IStreamConsumeFilter instance)
        => new() { FilterType = instance.GetType(), Instance = instance };

    public IStreamConsumeFilter CreateUniversal(IServiceProvider provider)
        => FilterRegistrationSupport.CreateUniversal(provider, FilterType, Factory, Instance, IsTyped, "Consume");

    public Type? ClosedFilterType(Type messageType)
        => FilterRegistrationSupport.ClosedFilterType(
            Kind,
            FilterType,
            TypedMessageTypes,
            messageType,
            typeof(IStreamConsumeFilter<>),
            "Consume");

    /// <summary>
    /// Wraps a resolved typed filter in the adapter for
    /// <paramref name="messageType"/>. Registrations created by
    /// <see cref="ForTyped{TFilter,TMessage}"/> use their compile-time factory;
    /// the remaining (open-generic or interface-derived) registrations close the
    /// adapter at runtime.
    /// </summary>
    public IStreamConsumeFilter WrapTyped(Type messageType, object filter)
    {
        ArgumentNullException.ThrowIfNull(messageType);
        ArgumentNullException.ThrowIfNull(filter);
        if (TypedAdapterFactory is { } factory)
        {
            if (!TypedMessageTypes.Contains(messageType))
            {
                throw new StreamBusConfigurationException(
                    $"Consume filter '{FilterType}' is pinned to '{string.Join("', '", TypedMessageTypes.Select(t => t.Name))}' and cannot be wrapped for message type '{messageType.Name}'.");
            }
            return factory(filter);
        }
        return CreateTypedAdapter(messageType, filter);
    }

    public object ResolveTyped(Type closedFilterType, IServiceProvider provider)
    {
        ArgumentNullException.ThrowIfNull(closedFilterType);
        return FilterRegistrationSupport.ResolveTyped(FilterType, closedFilterType, provider, "Consume");
    }

    public static IStreamConsumeFilter CreateTypedAdapter(Type messageType, object filter)
    {
        ArgumentNullException.ThrowIfNull(messageType);
        ArgumentNullException.ThrowIfNull(filter);
        return TypedFilterAdapter.Consume(messageType, filter);
    }
}

internal sealed class StreamBusConsumeFilterStore
{
    private readonly Dictionary<Type, List<ConsumeFilterRegistration>> _filters = new();
    private readonly HashSet<Type> _sealedBusTypes = new();

    internal void AddFilter(Type busMarker, ConsumeFilterRegistration filter)
    {
        ArgumentNullException.ThrowIfNull(busMarker);
        ArgumentNullException.ThrowIfNull(filter);
        if (_sealedBusTypes.Contains(busMarker))
        {
            throw new StreamBusConfigurationException(
                $"Consume filters cannot be registered after bus {busMarker.FullName} is built.");
        }
        if (!_filters.TryGetValue(busMarker, out var filters))
        {
            filters = new List<ConsumeFilterRegistration>();
            _filters[busMarker] = filters;
        }
        if (filter.FilterType is { } type && filters.Any(filter.ConflictsWith))
        {
            throw new StreamBusConfigurationException(
                $"Consume filter '{type}' is already registered on this bus for one of its message types; duplicate registration would run it twice.");
        }
        filters.Add(filter);
    }

    internal IReadOnlyList<ConsumeFilterRegistration> FiltersFor(Type busMarker)
        => _filters.TryGetValue(busMarker, out var filters)
            ? filters
            : Array.Empty<ConsumeFilterRegistration>();

    internal void Seal(Type busMarker) => _sealedBusTypes.Add(busMarker);
}

internal sealed class ConsumePipeline
{
    private readonly IStreamConsumeFilter[] _filters;

    public ConsumePipeline(IReadOnlyList<IStreamConsumeFilter> filters)
    {
        ArgumentNullException.ThrowIfNull(filters);
        _filters = filters.ToArray();
    }

    public int Count => _filters.Length;

    public Task RunAsync(ConsumeContext context, ConsumeNext terminal)
    {
        ArgumentNullException.ThrowIfNull(context);
        ArgumentNullException.ThrowIfNull(terminal);
        return RunMeasuredAsync(context, terminal);
    }

    private async Task RunMeasuredAsync(ConsumeContext context, ConsumeNext terminal)
    {
        var started = Stopwatch.GetTimestamp();
        try
        {
            await InvokeAsync(0, context, terminal).ConfigureAwait(false);
        }
        finally
        {
            StreamBusMetrics.Shared.RecordPipelineDuration("consume", Stopwatch.GetElapsedTime(started).TotalSeconds);
        }
    }

    private async Task InvokeAsync(int index, ConsumeContext context, ConsumeNext terminal)
    {
        if (index == _filters.Length)
        {
            await terminal().ConfigureAwait(false);
            return;
        }

        var filter = _filters[index];
        var filterName = FilterIdentity.TypeOf(filter).Name;

        // next must be awaited exactly once within the filter's returned task
        // (documented on ConsumeNext); the pipeline enforces that below, so the
        // call count never needs cross-thread visibility.
        var nextCalls = 0;
        Task? nextTask = null;
        Task Next()
        {
            nextCalls++;
            if (nextCalls > 1)
            {
                throw new StreamBusTransientException(
                    $"Consume filter '{filterName}' invoked next {nextCalls} times; a message must be handled at most once.");
            }

            nextTask = InvokeAsync(index + 1, context, terminal);
            return nextTask;
        }

        await filter.OnConsume(context, Next).ConfigureAwait(false);

        var calls = nextCalls;
        if (calls == 0)
        {
            // A consume filter may skip a delivery by returning without calling
            // next: the handler is not invoked and the offset advances.
            return;
        }
        if (nextTask is not { IsCompleted: true })
        {
            // The filter called next but returned before awaiting it: the
            // continuation (including the handler) is still running while the
            // pipeline would report completion, advancing the offset under a
            // live handler and hiding faults. Reject loudly instead.
            throw new StreamBusFilterContractException(
                $"Consume filter '{filterName}' returned before its continuation completed; await next().");
        }
    }
}

internal interface ITypedConsumeContextFactory
{
    ConsumeContext Create(ConsumeContext delivery, object message);
}

internal static class TypedConsumeContext
{
    private static readonly ConcurrentDictionary<Type, ITypedConsumeContextFactory> Factories = new();

    public static ConsumeContext Create(Type messageType, ConsumeContext delivery, object message)
    {
        ArgumentNullException.ThrowIfNull(messageType);
        ArgumentNullException.ThrowIfNull(delivery);
        ArgumentNullException.ThrowIfNull(message);
        return Factories.GetOrAdd(messageType, CreateFactory).Create(delivery, message);
    }

    [RequiresUnreferencedCode(RuntimeGeneric.TrimMessage)]
    [RequiresDynamicCode(RuntimeGeneric.AotMessage)]
    private static ITypedConsumeContextFactory CreateFactory(Type messageType)
        => RuntimeGeneric.Create<ITypedConsumeContextFactory>(
            typeof(TypedConsumeContextFactory<>),
            messageType);
}

internal sealed class TypedConsumeContextFactory<T> : ITypedConsumeContextFactory
{
    public ConsumeContext Create(ConsumeContext delivery, object message)
        => new ConsumeContext<T>
        {
            Stream = delivery.Stream,
            Partition = delivery.Partition,
            Offset = delivery.Offset,
            RoutingValue = delivery.RoutingValue,
            Body = delivery.Body,
            MessageId = delivery.MessageId,
            CorrelationId = delivery.CorrelationId,
            SentTime = delivery.SentTime,
            ContentType = delivery.ContentType,
            Headers = delivery.Headers,
            SendHeaders = delivery.SendHeaders,
            Services = delivery.Services,
            CancellationToken = delivery.CancellationToken,
            Message = (T)message,
        };
}
