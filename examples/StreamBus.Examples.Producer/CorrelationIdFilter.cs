using System.Diagnostics;
using StreamBus;

// Sample publish filter: propagate a W3C-compatible correlation id so the
// consume side can link its span to this publish. A real app would normally
// take Activity.Current.TraceId and let OpenTelemetry wire the rest.
// This is a user header, not the AMQP correlation id; supply that through
// PublishOptions.Properties.CorrelationId (a Guid) when you need it.
public sealed class CorrelationIdFilter : IStreamPublishFilter
{
    public Task OnPublish(PublishContext context, PublishDelegate next)
    {
        var correlationId = Activity.Current?.TraceId.ToString() ?? Guid.NewGuid().ToString("N");
        context.Headers.Set("x-correlation-id", correlationId);
        return next();
    }
}
