using Microsoft.Extensions.DependencyInjection;

namespace StreamBus;

/// <summary>
/// An optional per-delivery DI scope, disposable asynchronously. Wraps a
/// nullable <see cref="AsyncServiceScope"/> so consume paths that only
/// sometimes need a scope can use <c>await using</c> uniformly: scoped services
/// implementing <see cref="IAsyncDisposable"/> (e.g. a DbContext) are disposed
/// asynchronously instead of synchronously on the dispatch thread.
/// </summary>
internal readonly struct OptionalAsyncScope(AsyncServiceScope? scope) : IAsyncDisposable
{
    public static readonly OptionalAsyncScope Empty = new(null);

    /// <summary>The scope's service provider, or null when no scope was created.</summary>
    public IServiceProvider? Services => scope?.ServiceProvider;

    public ValueTask DisposeAsync()
        => scope is { } created ? created.DisposeAsync() : ValueTask.CompletedTask;
}
