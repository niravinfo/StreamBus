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
    public Task OnConsume(StreamConsumeFilterContext context, ConsumeDelegate next, CancellationToken ct)
    {
        var scope = (TenantScope)context.Services.GetRequiredService(typeof(TenantScope));
        scope.Tenant = context.Message.Metadata.GetString("x-tenant");
        context.SetPayload(scope.Tenant);
        return next(context);
    }
}
