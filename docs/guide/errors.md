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
    cfg.Publish<PaymentFailed>("payments-dlq"); // target used by the payments callback

    cfg.ReceiveEndpoint("orders", e =>
    {
        e.Consumer<OrderConsumer>("order-processors");
    });

    cfg.ReceiveEndpoint("audit", e =>
    {
        e.Consumer<AuditConsumer>("audit-log");
        e.UseMessageRetry(r => r.Immediate(5));
        e.DeadLetterToStream("audit-errors");
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
| bounded schedule | `DeadLetterToStream` / `DeadLetterToQueue` | handoff to the declared destination, then advance |
| bounded schedule | `OnExhausted` | the callback owns the outcome (a declared destination is shadowed with a startup warning) |
| bounded schedule | neither | handoff to the derived destination: classic queue `{stream}_error` (durable, full headers) |
| unbounded (default) | neither | `Strict`: retry forever |
| unbounded | destination or callback | configuration error at startup (a handoff needs a bound) |
| `None()` | explicit/callback/nothing | immediate handoff or callback on first failure |

Dead-letter destinations are kind-split, and a second call on the same
endpoint throws at registration (the destination choice is a one-time,
exactly-once decision — `OnExhausted` knobs stay last-call-wins):

- `DeadLetterToStream(name, spec?)`: an error **stream** with
  error-envelope semantics, advance-once, replayable with
  `ReplayErrorStreamAsync`. The destination stream is declared with the
  destination's `StreamOptions` at startup.
- `DeadLetterToQueue(name, type)`: a durable AMQP queue
  (`DeadLetterQueueType.ClassicQueue` or `DeadLetterQueueType.QuorumQueue`),
  broker-confirmed, full `streambus.error.*` headers, requeue with
  `RequeueErrorsAsync`. The transport declares it, never topology.

## Hand-off confirmation timeouts

Both error destinations bound the hand-off publish instead of letting a
lost confirmation wedge the partition:

- Stream hand-off: `e.ErrorHandoffTimeout(TimeSpan.FromSeconds(10))`.
  The default is 10 seconds; values below 1 second are raised to 1
  second. A lost confirmation surfaces as a timeout with an unknown
  outcome and the envelope stays replayable.
- Queue hand-off:
  `e.DeadLetterToQueue("orders-errors", DeadLetterQueueType.ClassicQueue,
  q => q.ConfirmTimeout(TimeSpan.FromSeconds(10)))`. The default is 10
  seconds. A timeout means the broker may or may not have accepted the
  message; requeue/replay only when the destination can tolerate a
  duplicate.

`OnExhausted` runs with the full `ExhaustedContext` (stream, partition,
offset, body, metadata, attempts, exception, services). Returning
advances past the message; throwing recycles the attach (redeliver,
never skip). Use `ctx.GetService<T>()` for buses and other services.

## Requeue

```csharp
int requeued = await bus.RequeueErrorsAsync("orders", "orders-errors", ct);
```

`BasicGet`-loops the queue and republishes bodies to the source stream as
fresh messages (tail append, at-least-once), acknowledging only after the
stream confirm. Malformed entries are acknowledged with a warning and
skipped. Stream destinations replay with `ReplayErrorStreamAsync`
instead.

## Full example

Bounded retry hands the poison message to a classic error queue; the
operator drains it back after deploying the fix:

```csharp
services.AddStreamBus((context, cfg) =>
{
    cfg.Host("localhost", "/", h => h.Credentials("app", "s3cret"));
    cfg.Publish<OrderEvent>("orders");
    cfg.ReceiveEndpoint("orders", e =>
    {
        e.Consumer<OrderConsumer>("order-processors");
        e.UseMessageRetry(r => r.None());
        e.DeadLetterToQueue("orders-errors", DeadLetterQueueType.ClassicQueue);
    });
});
services.AddStreamBusHosting(h => h.BlockStartupUntilReady(TimeSpan.FromSeconds(30)));

// ... poison message exhausts on first failure, lands in "orders-errors"
// with full streambus.error.* headers ...

// After the fix is deployed, redeliver everything:
int requeued = await bus.RequeueErrorsAsync("orders", "orders-errors", ct);
```

## Exception table

Every StreamBus exception derives from `StreamBusException`, so a host can
catch the whole library surface with one type. Retryability remains
type-specific:

| Exception | Meaning | Retry? |
|---|---|---|
| `StreamBusException` | Abstract base for every StreamBus exception. | Catch-all only. |
| `StreamBusConfigurationException` | Invalid or contradictory configuration. | No - fix the configuration. |
| `StreamConfigurationConflictException` | Existing broker topology disagrees with the declaration. | No - align the declaration or recreate the stream. |
| `StreamBusStartupBlockedException` | A typed consumer could not attach during blocking startup. | No - fix the consumer or dependency first. |
| `StreamBusFatalException` | The bus connection is fatal. | No - reconnect/operator action. |
| `StreamBusNotReadyException` | The bus is not ready for the operation yet. | Yes - transient. |
| `StreamBusStreamNotFoundException` | A referenced publish, consume, replay, or effectively-once stream is missing. | No at runtime; a blocking startup attach may retry while declarations converge. |
| `StreamBusTimeoutException` | A budget expired; the outcome may be unknown. | Only when the operation is safe/idempotent. |
| `StreamBusPublishTransientException` | A publish failed transiently and the producer was discarded. | Yes - retry. |
| `StreamBusPublishException` | A publish was rejected, returned, or otherwise failed without a transient classification. | Do not blindly retry. |
| `StreamBusDeserializationException` | The payload could not be deserialized as the consumer's message type. | No - treat as poison and route to the error destination. |
