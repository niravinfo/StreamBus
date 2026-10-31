using System.Diagnostics;

namespace StreamBus;

/// <summary>
/// Universal publish filter that runs for every typed publish on the bus.
/// </summary>
public interface IStreamPublishFilter
{
    /// <summary>
    /// Runs before the publish is serialized and sent.
    /// </summary>
    /// <param name="context">The shared publish context for this message.</param>
    /// <param name="next">The continuation; call it exactly once.</param>
    /// <param name="ct">Cancellation token for the publish.</param>
    /// <returns>A task that completes when the filter and its continuation finish.</returns>
    Task OnPublish(StreamPublishFilterContext context, PublishDelegate next, CancellationToken ct);
}

/// <summary>
/// Publish filter that runs only for publishes whose declared pipe type is
/// <typeparamref name="T"/>.
/// </summary>
/// <typeparam name="T">The declared message type.</typeparam>
public interface IStreamPublishFilter<T>
{
    /// <summary>
    /// Runs before the publish is serialized and sent.
    /// </summary>
    /// <param name="context">The typed publish context for this message.</param>
    /// <param name="next">The continuation; call it exactly once.</param>
    /// <param name="ct">Cancellation token for the publish.</param>
    /// <returns>A task that completes when the filter and its continuation finish.</returns>
    Task OnPublish(StreamPublishFilterContext<T> context, PublishDelegate next, CancellationToken ct);
}

/// <summary>
/// Continues a publish pipeline. Publish filters must invoke it exactly once.
/// </summary>
public delegate Task PublishDelegate();

/// <summary>
/// Shared context for one typed publish. Universal and typed publish filters
/// receive the same context instance.
/// </summary>
public class StreamPublishFilterContext
{
    /// <summary>Target stream resolved by the publish call.</summary>
    public required string Stream { get; init; }

    /// <summary>Routing key for a super-stream publish; null for a plain stream.</summary>
    public required string? RoutingKey { get; init; }

    /// <summary>
    /// Published payload. Universal filters can observe it; publish filters
    /// cannot replace the body.
    /// </summary>
    public required object Payload { get; init; }

    /// <summary>
    /// Declared pipe type (<c>typeof(T)</c> at the publish call site); null only
    /// for a context constructed directly outside the bus.
    /// </summary>
    public virtual Type? MessageType { get; init; }

    /// <summary>Runtime type of <see cref="Payload"/>.</summary>
    public Type RuntimeMessageType => Payload.GetType();

    /// <summary>Headers that are sent with the publish.</summary>
    public required IStreamHeaderWriter Headers { get; init; }

    /// <summary>Credential-free registration view for the current bus.</summary>
    public required StreamBusRegistrationView Bus { get; init; }

    /// <summary>Per-publish service scope.</summary>
    public required IServiceProvider Services { get; init; }
}

/// <summary>
/// Typed publish context for a declared message type. It derives from and is
/// the same object as the universal <see cref="StreamPublishFilterContext"/>.
/// </summary>
/// <typeparam name="T">The declared message type.</typeparam>
public sealed class StreamPublishFilterContext<T> : StreamPublishFilterContext
{
    /// <summary>The published payload as <typeparamref name="T"/>.</summary>
    public T Message => (T)Payload;

    /// <inheritdoc />
    public override Type MessageType => typeof(T);
}

/// <summary>
/// Mutable header collection exposed to publish and consume filters.
/// </summary>
public interface IStreamHeaderWriter
{
    /// <summary>Sets a header, replacing any existing value.</summary>
    /// <param name="key">Non-empty header key.</param>
    /// <param name="value">Header value; must be a supported primitive or null.</param>
    void Set(string key, object? value);

    /// <summary>Reads a header without removing it.</summary>
    /// <param name="key">Header key.</param>
    /// <param name="value">The stored value when found.</param>
    /// <returns><see langword="true"/> when the header exists.</returns>
    bool TryGet(string key, out object? value);

    /// <summary>Removes a header.</summary>
    /// <param name="key">Header key.</param>
    /// <returns><see langword="true"/> when a header was removed.</returns>
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
            await terminal().ConfigureAwait(false);
            return;
        }

        var filter = _filters[index];
        var filterType = FilterIdentity.TypeOf(filter);
        var filterName = filterType.FullName ?? filterType.Name;
        var nextCalls = 0;
        Task Next()
        {
            var call = Interlocked.Increment(ref nextCalls);
            if (call > 1)
            {
                throw new StreamBusConfigurationException(
                    $"Publish filter {filterName} invoked next {call} times; exactly one continuation is allowed.");
            }

            // Stream, routing key, payload, bus view and services are resolved
            // before the pipeline; a filter can only mutate Headers in place.
            return InvokeAsync(index + 1, context, terminal, ct);
        }

        await filter.OnPublish(context, Next, ct).ConfigureAwait(false);

        var calls = Volatile.Read(ref nextCalls);
        if (calls == 0)
        {
            throw new StreamBusConfigurationException(
                $"Publish filter '{filterName}' returned without calling next; the message would have been silently dropped.");
        }
        if (calls > 1)
        {
            throw new StreamBusConfigurationException(
                $"Publish filter {filterName} invoked next {calls} times; exactly one continuation is allowed.");
        }
    }
}
