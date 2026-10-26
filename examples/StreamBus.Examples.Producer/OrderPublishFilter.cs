using StreamBus;

// Typed publish filter: attaches only to publishes whose declared pipe
// type is OrderEvent. The universal filters (TenantFilter,
// CorrelationIdFilter) still run for every publish; this one runs only
// alongside them on OrderEvent sends — the PaymentEvent pipeline is
// untouched.
public sealed class OrderPublishFilter : IStreamPublishFilter<OrderEvent>
{
    public Task OnPublish(StreamPublishFilterContext<OrderEvent> context, PublishDelegate next, CancellationToken ct)
    {
        // Typed access: context.Message is OrderEvent, no casting.
        context.Headers.Set("x-order-id", context.Message.OrderId);

        // The typed context wraps the shared universal context; the
        // pipeline continues through it.
        return next(context.Inner);
    }
}
