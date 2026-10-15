# Retry and errors

## Layering

Every endpoint inherits the global schedule unless it overrides it.
Without any configuration the endpoint retries forever (`Strict`).

```csharp
services.AddStreamBus((context, cfg) =>
{
    cfg.Host("localhost", "/", h => h.Credentials("app", "s3cret"));

    cfg.UseMessageRetry(r => r.Intervals(
        TimeSpan.FromMilliseconds(200),
        TimeSpan.FromSeconds(2),
        TimeSpan.FromSeconds(10)));

    cfg.Publish<OrderEvent>("orders");

    cfg.ReceiveEndpoint("orders", e =>
    {
        e.Consumer<OrderConsumer>("order-processors");
    });

    cfg.ReceiveEndpoint("audit", e =>
    {
        e.Consumer<AuditConsumer>("audit-log");
        e.UseMessageRetry(r => r.Immediate(5));
        e.DeadLetterTo("audit-errors", DeadLetterType.Stream);
    });

    cfg.ReceiveEndpoint("payments", e =>
    {
        e.Consumer<PaymentsConsumer>("payments-ops");
        e.UseMessageRetry(r => r.Intervals(TimeSpan.FromMilliseconds(200)));
        e.OnExhausted(async (ctx, ct) =>
        {
            var bus = ctx.GetService<IStreamBus>();
            await bus.PublishRawAsync("payments-dlq", ctx.Body, new PublishOptions(), ct);
        });
    });

    cfg.ReceiveEndpoint("metrics", e =>
    {
        e.Consumer<MetricsConsumer>("metrics-tail");
        e.UseMessageRetry(r => r.None());
    });
});
```

Schedules: `Immediate(n)`, `Interval(n, delay)`, `Intervals(params)`,
`Exponential(count, base, max, multiplier)`, `None()`. `UseRetry` takes
a raw `RetryPolicy` as an escape hatch on both levels.

## Exhaustion rules

| Retry | Exhaustion | Effective mode |
|---|---|---|
| bounded schedule | `DeadLetterTo` | handoff to the declared destination, then advance |
| bounded schedule | `OnExhausted` | the callback owns the outcome (a declared destination is shadowed with a startup warning) |
| bounded schedule | neither | handoff to the derived default: classic queue `{stream}_error` (durable, full headers) |
| unbounded (default) | neither | `Strict`: retry forever |
| unbounded | destination or callback | configuration error at startup (a handoff needs a bound) |
| `None()` | explicit/callback/nothing | immediate handoff or callback on first failure |

`DeadLetterType`: `Stream` (v1 envelope semantics, advance-once,
replayable with `ReplayErrorStreamAsync`), `ClassicQueue`,
`QuorumQueue` (durable, persistent, broker-confirmed, full
`streambus.error.*` headers, requeue with `RequeueErrorsAsync`).
Explicit `DeadLetterTo` requires both name and type. Stream
destinations must be explicitly declared; queue destinations are
declared by the transport, never by topology.

`OnExhausted` runs with the full `ExhaustedContext` (stream, partition,
offset, body, metadata, attempts, exception, services). Returning
advances past the message; throwing recycles the attach (redeliver,
never skip). Use `ctx.GetService<T>()` for buses and other services.

## Requeue

```csharp
int requeued = await bus.RequeueErrorsAsync("orders", errorQueue: "orders-errors", ct);
```

`BasicGet`-loops the queue and republishes bodies to the source stream as
fresh messages (tail append, at-least-once), acknowledging only after the
stream confirm. Malformed entries are acknowledged with a warning and
skipped. Stream destinations replay with `ReplayErrorStreamAsync`
instead.
