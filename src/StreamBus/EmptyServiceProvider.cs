namespace StreamBus;

/// <summary>
/// Null-object <see cref="IServiceProvider"/> used as the
/// <see cref="ConsumeContext.Services"/> value for deliveries that create no
/// DI scope: untyped endpoints with no filters and no message type, or buses
/// constructed without dependency injection. Resolving any service returns
/// <see langword="null"/>, the provider contract's "no such service" answer —
/// the <c>GetRequiredService</c> extension on it therefore throws the standard
/// missing-service exception. Filter pipelines never observe it: a consumer
/// with filters but no scope factory fails at construction instead.
/// </summary>
internal sealed class EmptyServiceProvider : IServiceProvider
{
    public static readonly EmptyServiceProvider Instance = new();

    public object? GetService(Type serviceType) => null;
}
