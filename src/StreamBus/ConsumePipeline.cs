using System.Collections.Concurrent;
using System.Diagnostics;
using System.Reflection;

namespace StreamBus;

/// <summary>
/// Universal consume filter that runs for every delivery on the bus.
/// </summary>
public interface IStreamConsumeFilter
{
    /// <summary>
    /// Runs before the consumer handler. Call <paramref name="next"/> to handle
    /// the message, or return without calling it to filter the delivery.
    /// </summary>
    /// <param name="context">The shared consume context for this delivery.</param>
    /// <param name="next">
    /// The continuation. Call it at most once; zero calls mark the delivery as
    /// handled and advance the offset without invoking the handler.
    /// </param>
    /// <returns>A task that completes when the filter and any continuation finish.</returns>
    Task OnConsume(ConsumeContext context, ConsumeDelegate next);
}

/// <summary>
/// Consume filter that runs only on endpoints whose message type is
/// <typeparamref name="T"/>.
/// </summary>
/// <typeparam name="T">The endpoint message type.</typeparam>
public interface IStreamConsumeFilter<T>
{
    /// <summary>
    /// Runs before the consumer handler. Call <paramref name="next"/> to handle
    /// the message, or return without calling it to filter the delivery.
    /// </summary>
    /// <param name="context">The typed consume context for this delivery.</param>
    /// <param name="next">
    /// The continuation. Call it at most once; zero calls mark the delivery as
    /// handled and advance the offset without invoking the handler.
    /// </param>
    /// <returns>A task that completes when the filter and any continuation finish.</returns>
    Task OnConsume(ConsumeContext<T> context, ConsumeDelegate next);
}

/// <summary>
/// Continues a consume pipeline. Normal filters may call it zero or one time;
/// effectively-once pipelines require exactly one call.
/// </summary>
public delegate Task ConsumeDelegate();

internal sealed class ConsumeFilterRegistration
{
    public Type? FilterType { get; init; }
    public Func<IServiceProvider, IStreamConsumeFilter>? Factory { get; init; }
    public IStreamConsumeFilter? Instance { get; init; }
    public FilterRegistrationKind Kind { get; init; } = FilterRegistrationKind.Universal;
    public IReadOnlyList<Type> TypedMessageTypes { get; init; } = Array.Empty<Type>();
    public bool IsTyped => Kind != FilterRegistrationKind.Universal;

    public static ConsumeFilterRegistration ForType(Type filterType)
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

    public static ConsumeFilterRegistration ForInstance(IStreamConsumeFilter instance)
        => new() { FilterType = instance.GetType(), Instance = instance };

    public IStreamConsumeFilter Create(IServiceProvider provider) => CreateUniversal(provider);

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

    public IStreamConsumeFilter CreateTyped(Type closedFilterType, Type messageType, IServiceProvider provider)
    {
        ArgumentNullException.ThrowIfNull(closedFilterType);
        ArgumentNullException.ThrowIfNull(messageType);
        return CreateTypedAdapter(messageType, ResolveTyped(closedFilterType, provider));
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
        if (filter.FilterType is { } type && filters.Any(existing => existing.FilterType == type))
        {
            throw new StreamBusConfigurationException(
                $"Consume filter '{type}' is already registered on this bus; duplicate registration would run it twice.");
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
    private readonly bool _requiresContinuation;

    public ConsumePipeline(
        IReadOnlyList<IStreamConsumeFilter> filters,
        bool requiresContinuation = false)
    {
        ArgumentNullException.ThrowIfNull(filters);
        _filters = filters.ToArray();
        _requiresContinuation = requiresContinuation;
    }

    public int Count => _filters.Length;

    public Task RunAsync(ConsumeContext context, ConsumeDelegate terminal)
    {
        ArgumentNullException.ThrowIfNull(context);
        ArgumentNullException.ThrowIfNull(terminal);
        return RunMeasuredAsync(context, terminal);
    }

    private async Task RunMeasuredAsync(ConsumeContext context, ConsumeDelegate terminal)
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

    private async Task InvokeAsync(int index, ConsumeContext context, ConsumeDelegate terminal)
    {
        if (index == _filters.Length)
        {
            await terminal().ConfigureAwait(false);
            return;
        }

        var filter = _filters[index];
        var filterName = FilterIdentity.TypeOf(filter).Name;
        var nextCalls = 0;
        Task Next()
        {
            var call = Interlocked.Increment(ref nextCalls);
            if (call > 1)
            {
                var message =
                    $"Consume filter '{filterName}' invoked next {call} times; a message must be handled exactly once.";
                throw _requiresContinuation
                    ? new StreamBusFilterContractException(message)
                    : new StreamBusTransientException(message);
            }

            return InvokeAsync(index + 1, context, terminal);
        }

        await filter.OnConsume(context, Next).ConfigureAwait(false);

        var calls = Volatile.Read(ref nextCalls);
        if (calls == 0)
        {
            if (_requiresContinuation)
            {
                throw new StreamBusFilterContractException(
                    $"Effectively-once consume filter '{filterName}' returned without calling next; an effectively-once pipeline requires exactly one continuation.");
            }

            return;
        }
        if (calls > 1)
        {
            var message =
                $"Consume filter '{filterName}' invoked next {calls} times; a message must be handled exactly once.";
            throw _requiresContinuation
                ? new StreamBusFilterContractException(message)
                : new StreamBusTransientException(message);
        }
    }
}

internal static class TypedConsumeContext
{
    private static readonly ConcurrentDictionary<
        Type,
        Func<ConsumeContext, object, ConsumeContext>> Factories = new();

    public static ConsumeContext Create(Type messageType, ConsumeContext delivery, object message)
    {
        ArgumentNullException.ThrowIfNull(messageType);
        ArgumentNullException.ThrowIfNull(delivery);
        ArgumentNullException.ThrowIfNull(message);
        return Factories.GetOrAdd(messageType, CreateFactory)(delivery, message);
    }

    private static Func<ConsumeContext, object, ConsumeContext> CreateFactory(Type messageType)
    {
        var method = typeof(TypedConsumeContext).GetMethod(
            nameof(CreateTyped),
            BindingFlags.NonPublic | BindingFlags.Static)!;
        return (Func<ConsumeContext, object, ConsumeContext>)method
            .MakeGenericMethod(messageType)
            .CreateDelegate(typeof(Func<ConsumeContext, object, ConsumeContext>));
    }

    private static ConsumeContext<T> CreateTyped<T>(ConsumeContext delivery, object message)
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
            Headers = delivery.Headers,
            SendHeaders = delivery.SendHeaders,
            Services = delivery.Services,
            CancellationToken = delivery.CancellationToken,
            Message = (T)message,
        };
}

internal sealed class EmptyServiceProvider : IServiceProvider
{
    public static readonly EmptyServiceProvider Instance = new();

    public object? GetService(Type serviceType) => null;
}
