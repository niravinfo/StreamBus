using System.Diagnostics;
using StreamBus;

// Sample publish filter: propagate a W3C-compatible correlation id so the
// consume side can link its span to this publish. A real app would normally
// take Activity.Current.TraceId and let OpenTelemetry wire the rest.
public sealed class CorrelationIdFilter : IStreamPublishFilter
{
    public Task OnPublish(StreamPublishFilterContext context, PublishDelegate next, CancellationToken ct)
    {
        var correlationId = Activity.Current?.TraceId.ToString() ?? Guid.NewGuid().ToString("N");
        context.Headers.Set("x-correlation-id", correlationId);
        return next(context);
    }
}
