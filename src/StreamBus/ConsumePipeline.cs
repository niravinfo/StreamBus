using System.Diagnostics;

namespace StreamBus;

public interface IStreamConsumeFilter
{
    bool CanSkip => false;

    Task OnConsume(StreamConsumeFilterContext context, ConsumeDelegate next, CancellationToken ct);
}

public interface IStreamConsumeFilter<T>
{
    bool CanSkip => false;

    Task OnConsume(StreamConsumeFilterContext<T> context, ConsumeDelegate next, CancellationToken ct);
}

public delegate Task ConsumeDelegate(StreamConsumeFilterContext context);

public sealed class StreamConsumeFilterContext
{
    public required ConsumeContext Message { get; init; }

    /// <summary>
    /// Endpoint message type; null for raw and effectively-once pipelines.
    /// </summary>
    public Type? MessageType { get; init; }

    internal object? TypedMessage { get; init; }

    public required IStreamHeaderWriter OutputHeaders { get; init; }
    public required IServiceProvider Services { get; init; }
    public bool SkipHandler { get; set; }

    private readonly Dictionary<Type, object?> _payloads = new();

    public T? GetPayload<T>()
        => _payloads.TryGetValue(typeof(T), out var value) && value is T typed
            ? typed
            : default;

    public void SetPayload<T>(T value) => _payloads[typeof(T)] = value;
}

public sealed class StreamConsumeFilterContext<T>
{
    public required StreamConsumeFilterContext Inner { get; init; }

    public required T Message { get; init; }

    public Type MessageType => typeof(T);

    public ConsumeContext Raw => Inner.Message;

    public IStreamHeaderWriter OutputHeaders => Inner.OutputHeaders;

    public IServiceProvider Services => Inner.Services;

    public bool SkipHandler
    {
        get => Inner.SkipHandler;
        set => Inner.SkipHandler = value;
    }

    public TPayload? GetPayload<TPayload>() => Inner.GetPayload<TPayload>();

    public void SetPayload<TPayload>(TPayload value) => Inner.SetPayload(value);
}

internal sealed class ConsumeFilterRegistration
{
    public Type? FilterType { get; init; }
    public Func<IServiceProvider, IStreamConsumeFilter>? Factory { get; init; }
    public IStreamConsumeFilter? Instance { get; init; }

    public static ConsumeFilterRegistration ForType(Type filterType) => new()
    {
        FilterType = filterType,
        Factory = provider => provider.GetService(filterType) as IStreamConsumeFilter
            ?? throw new StreamBusConfigurationException(
                $"Consume filter '{filterType.FullName}' could not be resolved from the root service provider."),
    };

    public static ConsumeFilterRegistration ForFactory(Func<IServiceProvider, IStreamConsumeFilter> factory)
        => new() { Factory = factory };

    public static ConsumeFilterRegistration ForInstance(IStreamConsumeFilter instance)
        => new() { FilterType = instance.GetType(), Instance = instance };

    public IStreamConsumeFilter Create(IServiceProvider provider)
    {
        var filter = Instance ?? Factory!(provider);
        return filter ?? throw new StreamBusConfigurationException("Consume filter factory returned null.");
    }
}

public sealed class StreamBusConsumeFilterStore
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

    public ConsumePipeline(IReadOnlyList<IStreamConsumeFilter> filters)
    {
        ArgumentNullException.ThrowIfNull(filters);
        _filters = filters.ToArray();
    }

    public int Count => _filters.Length;

    public Task RunAsync(
        StreamConsumeFilterContext context,
        ConsumeDelegate terminal,
        CancellationToken ct,
        bool allowSkip)
    {
        ArgumentNullException.ThrowIfNull(context);
        ArgumentNullException.ThrowIfNull(terminal);
        return RunMeasuredAsync(context, terminal, ct, allowSkip);
    }

    private async Task RunMeasuredAsync(
        StreamConsumeFilterContext context,
        ConsumeDelegate terminal,
        CancellationToken ct,
        bool allowSkip)
    {
        var started = Stopwatch.GetTimestamp();
        try
        {
            await InvokeAsync(0, context, terminal, ct, allowSkip).ConfigureAwait(false);
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
        CancellationToken ct,
        bool allowSkip)
    {
        if (index == _filters.Length)
        {
            await terminal(context).ConfigureAwait(false);
            return;
        }

        var filter = _filters[index];
        var nextCalls = 0;
        Task Next(StreamConsumeFilterContext _)
        {
            var call = Interlocked.Increment(ref nextCalls);
            if (call > 1)
            {
                throw new StreamBusTransientException(
                    $"Consume filter '{filter.GetType().Name}' invoked next {call} times; a message must be handled exactly once.");
            }

            return InvokeAsync(index + 1, context, terminal, ct, allowSkip);
        }

        await filter.OnConsume(context, Next, ct).ConfigureAwait(false);

        var calls = Volatile.Read(ref nextCalls);
        if (calls == 0)
        {
            if (allowSkip && context.SkipHandler)
                return;
            throw new StreamBusTransientException(
                $"Consume filter '{filter.GetType().Name}' returned without calling next and without setting SkipHandler; the message would be dropped while the offset advances.");
        }
        if (calls > 1)
        {
            throw new StreamBusTransientException(
                $"Consume filter '{filter.GetType().Name}' invoked next {calls} times; a message must be handled exactly once.");
        }
    }
}

internal sealed class EmptyServiceProvider : IServiceProvider
{
    public static readonly EmptyServiceProvider Instance = new();

    public object? GetService(Type serviceType) => null;
}
