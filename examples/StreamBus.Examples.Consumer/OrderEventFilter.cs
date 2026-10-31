using StreamBus;

// Typed consume filter: validates the message before the consumer runs.
// Registered bus-wide, it attaches only to endpoints whose message type
// is OrderEvent (the "orders" endpoint here) — the payment endpoint never
// sees it. A filter exception follows the normal retry/exhaustion path.
public sealed class OrderEventFilter : IStreamConsumeFilter<OrderEvent>
{
    public Task OnConsume(StreamConsumeFilterContext<OrderEvent> context, ConsumeDelegate next, CancellationToken ct)
    {
        // Typed access: context.Message is OrderEvent. Blank ids are a
        // poison-message shape, so reject loudly instead of handling.
        if (string.IsNullOrWhiteSpace(context.Message.OrderId))
        {
            throw new InvalidOperationException(
                $"Order event rejected: OrderId is blank (stream '{context.Raw.Stream}', offset {context.Raw.Offset}).");
        }

        // Universal and typed filters share this one context; the pipeline
        // (and then the consumer) continues through it.
        return next();
    }
}
