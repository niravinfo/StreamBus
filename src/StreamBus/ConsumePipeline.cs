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
    /// <param name="ct">Cancellation token for the delivery.</param>
    /// <returns>A task that completes when the filter and any continuation finish.</returns>
    Task OnConsume(StreamConsumeFilterContext context, ConsumeDelegate next, CancellationToken ct);
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
    /// <param name="ct">Cancellation token for the delivery.</param>
    /// <returns>A task that completes when the filter and any continuation finish.</returns>
    Task OnConsume(StreamConsumeFilterContext<T> context, ConsumeDelegate next, CancellationToken ct);
}

/// <summary>
/// Continues a consume pipeline. Normal filters may call it zero or one time;
/// effectively-once pipelines require exactly one call.
/// </summary>
public delegate Task ConsumeDelegate();

/// <summary>
/// Shared context for one delivery. Universal and typed consume filters receive
/// the same context instance.
/// </summary>
public class StreamConsumeFilterContext
{
    /// <summary>The raw delivery, including body and broker metadata.</summary>
    public required ConsumeContext Raw { get; init; }

    /// <summary>
    /// Endpoint message type; null for raw and effectively-once pipelines.
    /// </summary>
    public virtual Type? MessageType { get; init; }

    /// <summary>
    /// Headers carried on the hand-off envelope or effectively-once result.
    /// </summary>
    public required IStreamHeaderWriter OutputHeaders { get; init; }

    /// <summary>
    /// Per-delivery service scope shared with the typed consumer.
    /// </summary>
    public required IServiceProvider Services { get; init; }

    private readonly Dictionary<Type, object?> _payloads = new();

    /// <summary>Gets a value stored by an earlier filter.</summary>
    /// <typeparam name="T">Type used as the storage key.</typeparam>
    /// <returns>The stored value, or the default value when absent.</returns>
    public T? GetPayload<T>()
        => _payloads.TryGetValue(typeof(T), out var value) && value is T typed
            ? typed
            : default;

    /// <summary>Stores a value for later filters in this pipeline.</summary>
    /// <typeparam name="T">Type used as the storage key.</typeparam>
    /// <param name="value">The value to store.</param>
    public void SetPayload<T>(T value) => _payloads[typeof(T)] = value;
}

/// <summary>
/// Typed consume context for an endpoint message type. It derives from and is
/// the same object as the universal <see cref="StreamConsumeFilterContext"/>.
/// </summary>
/// <typeparam name="T">The endpoint message type.</typeparam>
public sealed class StreamConsumeFilterContext<T> : StreamConsumeFilterContext
{
    /// <summary>The deserialized endpoint message.</summary>
    public required T Message { get; init; }

    /// <inheritdoc />
    public override Type? MessageType => typeof(T);
}

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
    private static readonly ConcurrentDictionary<
        Type,
        Func<ConsumeContext, IStreamHeaderWriter, IServiceProvider, StreamConsumeFilterContext>> ContextFactories = new();

    private readonly IStreamConsumeFilter[] _filters;
    private readonly Type? _endpointMessageType;
    private readonly bool _requiresContinuation;

    public ConsumePipeline(
        IReadOnlyList<IStreamConsumeFilter> filters,
        Type? endpointMessageType = null,
        bool requiresTypedMessage = false,
        bool requiresContinuation = false)
    {
        ArgumentNullException.ThrowIfNull(filters);
        if (requiresTypedMessage && endpointMessageType is null)
        {
            throw new StreamBusConfigurationException(
                "A consume pipeline with typed filters requires an endpoint message type.");
        }

        _filters = filters.ToArray();
        _endpointMessageType = endpointMessageType;
        _requiresContinuation = requiresContinuation;
        RequiresTypedMessage = requiresTypedMessage;
    }

    public int Count => _filters.Length;

    public bool RequiresTypedMessage { get; }

    public StreamConsumeFilterContext CreateContext(
        ConsumeContext raw,
        IStreamHeaderWriter outputHeaders,
        IServiceProvider services)
    {
        ArgumentNullException.ThrowIfNull(raw);
        ArgumentNullException.ThrowIfNull(outputHeaders);
        ArgumentNullException.ThrowIfNull(services);

        if (!RequiresTypedMessage)
        {
            return new StreamConsumeFilterContext
            {
                Raw = raw,
                MessageType = _endpointMessageType,
                OutputHeaders = outputHeaders,
                Services = services,
            };
        }

        var factory = ContextFactories.GetOrAdd(_endpointMessageType!, CreateContextFactory);
        return factory(raw, outputHeaders, services);
    }

    public Task RunAsync(
        StreamConsumeFilterContext context,
        ConsumeDelegate terminal,
        CancellationToken ct)
    {
        ArgumentNullException.ThrowIfNull(context);
        ArgumentNullException.ThrowIfNull(terminal);
        return RunMeasuredAsync(context, terminal, ct);
    }

    private async Task RunMeasuredAsync(
        StreamConsumeFilterContext context,
        ConsumeDelegate terminal,
        CancellationToken ct)
    {
        var started = Stopwatch.GetTimestamp();
        try
        {
            await InvokeAsync(0, context, terminal, ct).ConfigureAwait(false);
        }
        finally
        {
            StreamBusMetrics.Shared.RecordPipelineDuration("consume", Stopwatch.GetElapsedTime(started).TotalSeconds);
        }
    }

    private async Task InvokeAsync(
        int index,
        StreamConsumeFilterContext context,
        ConsumeDelegate terminal,
        CancellationToken ct)
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

            return InvokeAsync(index + 1, context, terminal, ct);
        }

        await filter.OnConsume(context, Next, ct).ConfigureAwait(false);

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

    private static Func<ConsumeContext, IStreamHeaderWriter, IServiceProvider, StreamConsumeFilterContext>
        CreateContextFactory(Type messageType)
    {
        var method = typeof(ConsumePipeline).GetMethod(
            nameof(CreateTypedContext),
            BindingFlags.NonPublic | BindingFlags.Static)!;
        return (Func<ConsumeContext, IStreamHeaderWriter, IServiceProvider, StreamConsumeFilterContext>)
            method.MakeGenericMethod(messageType).CreateDelegate(
                typeof(Func<ConsumeContext, IStreamHeaderWriter, IServiceProvider, StreamConsumeFilterContext>));
    }

    private static StreamConsumeFilterContext<T> CreateTypedContext<T>(
        ConsumeContext raw,
        IStreamHeaderWriter outputHeaders,
        IServiceProvider services)
    {
        if (raw.TypedMessage is not T message)
        {
            throw new StreamBusConfigurationException(
                $"The consume pipeline for {typeof(T).FullName} received a message without a deserialized {typeof(T).FullName} payload (internal error).");
        }

        return new StreamConsumeFilterContext<T>
        {
            Raw = raw,
            Message = message,
            OutputHeaders = outputHeaders,
            Services = services,
        };
    }
}

internal sealed class EmptyServiceProvider : IServiceProvider
{
    public static readonly EmptyServiceProvider Instance = new();

    public object? GetService(Type serviceType) => null;
}
