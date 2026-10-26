using System.Diagnostics;

namespace StreamBus;

public interface IStreamPublishFilter
{
    Task OnPublish(StreamPublishFilterContext context, PublishDelegate next, CancellationToken ct);
}

public interface IStreamPublishFilter<T>
{
    Task OnPublish(StreamPublishFilterContext<T> context, PublishDelegate next, CancellationToken ct);
}

public delegate Task PublishDelegate(StreamPublishFilterContext context);

public sealed class StreamPublishFilterContext
{
    public required string Stream { get; init; }
    public required string? RoutingKey { get; init; }
    public required object Message { get; init; }

    /// <summary>
    /// Declared pipe type (<c>typeof(T)</c> at the publish call site); null only
    /// for a context constructed directly outside the bus.
    /// </summary>
    public Type? MessageType { get; init; }

    public Type RuntimeMessageType => Message.GetType();

    public required IStreamHeaderWriter Headers { get; init; }
    public required StreamBusRegistrationView Bus { get; init; }
    public required IServiceProvider Services { get; init; }
}

public sealed class StreamPublishFilterContext<T>
{
    public required StreamPublishFilterContext Inner { get; init; }

    public T Message => (T)Inner.Message;

    public Type MessageType => typeof(T);

    public Type RuntimeMessageType => Inner.RuntimeMessageType;

    public string Stream => Inner.Stream;

    public string? RoutingKey => Inner.RoutingKey;

    public IStreamHeaderWriter Headers => Inner.Headers;

    public StreamBusRegistrationView Bus => Inner.Bus;

    public IServiceProvider Services => Inner.Services;
}

public interface IStreamHeaderWriter
{
    void Set(string key, object? value);
    bool TryGet(string key, out object? value);
    bool Remove(string key);
}

internal sealed class HeaderWriter : IStreamHeaderWriter
{
    private readonly Dictionary<string, object?> _headers;

    public HeaderWriter(IReadOnlyDictionary<string, object?>? initial = null)
        => _headers = initial is null
            ? new Dictionary<string, object?>(StringComparer.Ordinal)
            : new Dictionary<string, object?>(initial, StringComparer.Ordinal);

    public void Set(string key, object? value)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(key);
        StreamMessageProperties.ValidateValue(key, value);
        _headers[key] = value;
    }

    public bool TryGet(string key, out object? value)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(key);
        return _headers.TryGetValue(key, out value);
    }

    public bool Remove(string key)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(key);
        return _headers.Remove(key);
    }

    internal StreamMessageProperties? Materialize(StreamMessageProperties? callLevel)
    {
        if (_headers.Count == 0)
            return callLevel;

        var headers = new Dictionary<string, object?>(_headers, StringComparer.Ordinal);
        if (callLevel is not null)
        {
            foreach (var (key, value) in callLevel.Headers)
                headers[key] = value;
        }

        return (callLevel ?? new StreamMessageProperties()) with { Headers = headers };
    }

    internal IReadOnlyDictionary<string, object?> Snapshot()
        => new Dictionary<string, object?>(_headers, StringComparer.Ordinal);
}

internal sealed class PublishPipeline
{
    private readonly IStreamPublishFilter[] _filters;

    public PublishPipeline(IReadOnlyList<IStreamPublishFilter> filters)
    {
        ArgumentNullException.ThrowIfNull(filters);
        _filters = filters.ToArray();
    }

    public int Count => _filters.Length;

    public Task RunAsync(
        StreamPublishFilterContext context,
        PublishDelegate terminal,
        CancellationToken ct)
    {
        ArgumentNullException.ThrowIfNull(context);
        ArgumentNullException.ThrowIfNull(terminal);
        return RunMeasuredAsync(context, terminal, ct);
    }

    private async Task RunMeasuredAsync(
        StreamPublishFilterContext context,
        PublishDelegate terminal,
        CancellationToken ct)
    {
        var started = Stopwatch.GetTimestamp();
        try
        {
            await InvokeAsync(0, context, terminal, ct).ConfigureAwait(false);
        }
        finally
        {
            StreamBusMetrics.Shared.RecordPipelineDuration("publish", Stopwatch.GetElapsedTime(started).TotalSeconds);
        }
    }

    private async Task InvokeAsync(
        int index,
        StreamPublishFilterContext context,
        PublishDelegate terminal,
        CancellationToken ct)
    {
        if (index == _filters.Length)
        {
            await terminal(context).ConfigureAwait(false);
            return;
        }

        var filter = _filters[index];
        var nextCalls = 0;
        Task Next(StreamPublishFilterContext _)
        {
            var call = Interlocked.Increment(ref nextCalls);
            if (call > 1)
            {
                throw new StreamBusConfigurationException(
                    $"Publish filter {filter.GetType().FullName} invoked next {call} times; exactly one continuation is allowed.");
            }

            // Stream, routing key, message, bus view and services are resolved
            // before the pipeline; a filter can only mutate Headers in place.
            return InvokeAsync(index + 1, context, terminal, ct);
        }

        await filter.OnPublish(context, Next, ct).ConfigureAwait(false);

        var calls = Volatile.Read(ref nextCalls);
        if (calls == 0)
        {
            throw new StreamBusConfigurationException(
                $"Publish filter '{filter.GetType().FullName}' returned without calling next; the message would have been silently dropped.");
        }
        if (calls > 1)
        {
            throw new StreamBusConfigurationException(
                $"Publish filter {filter.GetType().FullName} invoked next {calls} times; exactly one continuation is allowed.");
        }
    }
}
