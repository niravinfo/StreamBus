using Microsoft.Extensions.DependencyInjection;
using StreamBus;

// Sample consume filter: read the tenant header and stash it in the per-message
// scope, where the typed consumer (or a downstream repository) can resolve the
// same instance. Register TenantScope as Scoped.
public sealed class TenantScope
{
    public string? Tenant { get; set; }
}

public sealed class TenantFilter : IStreamConsumeFilter
{
    public Task OnConsume(ConsumeContext context, ConsumeDelegate next)
    {
        TenantScope scope = context.Services.GetRequiredService<TenantScope>();
        scope.Tenant = context.Headers.GetString("x-tenant");

        return next();
    }
}
