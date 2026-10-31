using StreamBus;

// Sample publish filter: the ambient tenant is read from a singleton owned by
// the application and projected onto every outbound message. Product code can
// replace TenantScope with IHttpContextAccessor, AsyncLocal, etc.
public sealed class TenantScope
{
    public string Tenant { get; set; } = "acme";
}

public sealed class TenantFilter(TenantScope ambient) : IStreamPublishFilter
{
    public Task OnPublish(PublishContext context, PublishDelegate next)
    {
        context.Headers.Set("x-tenant", ambient.Tenant);
        return next();
    }
}
