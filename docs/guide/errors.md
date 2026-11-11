# Retry and errors

## Layering

Every endpoint inherits the global schedule unless it overrides it.
Without any configuration the endpoint retries forever (`Strict`).

```csharp
services.AddStreamBus((context, cfg) =>
{
    cfg.Host("localhost", "/", h => h.Credentials("app", "s3cret"));

    cfg.UseRetry(r => r.Intervals(
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
        e.UseRetry(r => r.Immediate(5));
        e.DeadLetterToStream("audit-errors");
    });

    cfg.ReceiveEndpoint("payments", e =>
    {
        e.Consumer<PaymentsConsumer>("payments-ops");
        e.UseRetry(r => r.Intervals(TimeSpan.FromMilliseconds(200)));
        e.OnExhausted(async (ctx, ct) =>
        {
            var bus = ctx.GetService<IStreamBus>();
            await bus.PublishRawAsync("payments-dlq", ctx.Body, new PublishOptions(), ct);
        });
    });

    cfg.ReceiveEndpoint("metrics", e =>
    {
        e.Consumer<MetricsConsumer>("metrics-tail");
        e.UseRetry(r => r.None());
    });
});
```

Schedules: `Immediate(n)`, `Interval(n, delay)`, `Intervals(params)`,
`Exponential(count, min, max)`, `None()`. `UseRetry` accepts
either the fluent schedule callback or a raw `RetryPolicy` (the escape
hatch for schedules the fluent configurator cannot express) on both levels.

`MaxAttempts` counts total attempts including the first (`None()` is one
attempt, `Immediate(5)` is six). Leaving it unset sets no attempt limit:
the endpoint retries until the handler succeeds and its partition waits.

## Exhaustion rules

| Retry | Exhaustion | Effective mode |
|---|---|---|
| attempt limit set (`MaxAttempts`) | `DeadLetterToStream` / `DeadLetterToQueue` | handoff to the declared destination, then advance |
| attempt limit set (`MaxAttempts`) | `OnExhausted` | the callback owns the outcome (a declared destination is shadowed with a startup warning) |
| attempt limit set (`MaxAttempts`) | neither | handoff to the derived destination: classic queue `{stream}_error` (durable, full headers) |
| no attempt limit (`MaxAttempts` unset, default) | neither | `Strict`: retry forever; the partition waits |
| no attempt limit | destination or callback | configuration error at startup (a hand-off needs an attempt limit) |
| `None()` | explicit/callback/nothing | immediate handoff or callback on first failure |

Dead-letter destinations are kind-split, and a second call on the same
endpoint throws at registration (the destination choice is a one-time
decision — `OnExhausted` knobs stay last-call-wins):

- `DeadLetterToStream(name, spec?)`: an error **stream** with
  error-envelope semantics, advance-once, and full envelope
  app-properties. The destination stream is declared with the
  destination's `StreamOptions` at startup.
- `DeadLetterToQueue(name, type)`: a durable AMQP queue
  (`DeadLetterQueueType.ClassicQueue` or `DeadLetterQueueType.QuorumQueue`),
  broker-confirmed, full `streambus.error.*` headers. The transport
  declares it, never topology.

In both cases StreamBus's responsibility ends at the hand-off: consuming
from the error stream, draining the error queue, or moving messages back
to the source is the application's job, using any compatible client.

## Hand-off confirmation timeouts

Both error destinations bound the hand-off publish instead of letting a
lost confirmation wedge the partition:

- Stream hand-off: `e.ErrorPublishTimeout(TimeSpan.FromSeconds(10))`.
  The default is null — the wait is bounded only by cancellation; values
  below 1 second are raised to 1 second. A lost confirmation surfaces as
  a timeout with an unknown outcome and the hand-off is retried without
  advancing.
- Queue hand-off:
  `e.DeadLetterToQueue("orders-errors", DeadLetterQueueType.ClassicQueue,
  q => q.ConfirmTimeout(TimeSpan.FromSeconds(10)))`. The default is 10
  seconds. A timeout means the broker may or may not have accepted the
  message; a duplicate in the error destination is possible and the
  destination should tolerate one when drained.

A hand-off that fails transiently (not-ready, timeout, broker unreachable,
or a destination stream that does not exist yet) is retried without
advancing: the message is never skipped. A *fatal* failure — a latched
connection/transport state (`StreamBusFatalException`), a fatal
protocol/SSL/auth error, or a configuration error — cannot recover by
retrying, so it surfaces immediately: the partition is marked failed and
recycled (redelivery, never a skip) instead of retrying forever at the
backoff delay.

`OnExhausted` runs with the full `ExhaustedContext` (stream, partition,
offset, body, message id, correlation id, sent time, headers, attempts,
exception, services). Returning advances past the message; throwing
recycles the attach (redeliver, never skip). Use `ctx.GetService<T>()` for
buses and other services. A missing registration throws the standard
`InvalidOperationException` from `GetRequiredService<T>()` — the bus
already started, so this is a DI wiring problem, not invalid bus
configuration.

The raw `ConsumeOptions` path exposes the same callback as
`OnExhausted(Func<ExhaustedContext, CancellationToken, Task>)`, so there is
one exhaustion shape across both APIs. On that path `OnExhausted` and
`ErrorStream` are mutually exclusive: the callback owns the outcome, so
configuring both fails with `StreamBusConfigurationException` instead of
silently shadowing the destination. Endpoints resolve the same conflict by
shadowing the declared destination with a startup warning.

## Recovery

StreamBus's responsibility ends at the hand-off. After a fix is deployed,
move messages out of the error destination yourself: consume the error
stream (its envelopes carry the source stream/partition/offset and original
headers in app-properties) or drain the queue with any AMQP client, and
republish to the source stream as new messages.

## Full example

A retry schedule with an attempt limit hands the poison message to a
classic error queue for the operator to inspect:

```csharp
services.AddStreamBus((context, cfg) =>
{
    cfg.Host("localhost", "/", h => h.Credentials("app", "s3cret"));
    cfg.Publish<OrderEvent>("orders");
    cfg.ReceiveEndpoint("orders", e =>
    {
        e.Consumer<OrderConsumer>("order-processors");
        e.UseRetry(r => r.None());
        e.DeadLetterToQueue("orders-errors", DeadLetterQueueType.ClassicQueue);
    });
});
services.AddOptions<StreamBusHostOptions>().Configure(o =>
{
    o.WaitUntilStarted = true;
    o.StartTimeout = TimeSpan.FromSeconds(30);
});

// ... poison message exhausts on first failure, lands in "orders-errors"
// with full streambus.error.* headers, ready for manual recovery ...
```

## Exception table

Every StreamBus exception derives from `StreamBusException`, so a host can
catch the whole library surface with one type. Retryability remains
type-specific:

| Exception | Meaning | Retry? |
|---|---|---|
| `StreamBusException` | Abstract base for every StreamBus exception. | Catch-all only. |
| `StreamBusConfigurationException` | Invalid or contradictory configuration. | No - fix the configuration. |
| `StreamBusStreamConfigurationConflictException` | Existing broker topology disagrees with the declaration. | No - align the declaration or recreate the stream. |
| `StreamBusFatalException` | The bus connection is fatal. | No - reconnect/operator action. |
| `StreamBusNotReadyException` | The bus is not ready for the operation yet. | Yes - transient. |
| `StreamBusStreamNotFoundException` | A referenced publish, consume, or error stream is missing. | No at runtime; a blocking startup attach may retry while declarations converge. |
| `StreamBusTimeoutException` | A budget expired; the outcome may be unknown (a blocking-start `StartTimeout` expiry raises this; a consumer that cannot attach does not throw - it keeps the wait going). | Only when the operation is safe/idempotent. |
| `StreamBusPublishTransientException` | A publish failed transiently and the producer was discarded. | Yes - retry. |
| `StreamBusPublishException` | A publish was rejected, returned, or otherwise failed without a transient classification. | Do not blindly retry. |
| `StreamBusDeserializationException` | The payload could not be deserialized as the consumer's message type. | No - treat as poison and route to the error destination. |
