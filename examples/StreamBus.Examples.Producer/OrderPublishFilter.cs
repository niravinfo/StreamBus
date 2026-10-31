using StreamBus;

// Typed publish filter: attaches only to publishes whose declared pipe
// type is OrderEvent. The universal filters (TenantFilter,
// CorrelationIdFilter) still run for every publish; this one runs only
// alongside them on OrderEvent sends — the PaymentEvent pipeline is
// untouched.
public sealed class OrderPublishFilter : IStreamPublishFilter<OrderEvent>
{
    public Task OnPublish(PublishContext<OrderEvent> context, PublishDelegate next)
    {
        // Typed access: context.Message is OrderEvent, no casting.
        context.Headers.Set("x-order-id", context.Message.OrderId);

        // Universal and typed filters share this one context; the pipeline
        // continues through it.
        return next();
    }
}
