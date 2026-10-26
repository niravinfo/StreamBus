# Hosting, readiness, health

## Startup modes

Default is non-blocking: `StartAsync` returns once connections start,
while declarations and consumer attaches converge in the background
(permanent failures surface as critical logs).

```csharp
services.AddStreamBusHosting(h => h.BlockStartupUntilReady(TimeSpan.FromSeconds(30)));
```

Blocking waits for connections, topology declarations (streams, super
streams, error queues), and typed consumer attaches - then either returns
or throws. A timeout raises `StreamBusTimeoutException`, a configuration
or topology conflict raises immediately, and a consumer that cannot attach
raises `StreamBusStartupBlockedException`. Omit the timeout to wait
indefinitely.

Blocking startup waits for consumers by default. Disable that part when
the host must come up before consumers can attach:

```csharp
services.AddStreamBusHosting(h =>
{
    h.BlockStartupUntilReady(TimeSpan.FromSeconds(30));
    h.BlockUntilConsumersAttached(false);
});
```

## Readiness waits

```csharp
var readiness = await bus.WaitUntilReadyAsync(
    ReadinessPolicy.UpTo(TimeSpan.FromSeconds(30)), ct);

if (readiness == BusReadiness.Fatal)
    throw new InvalidOperationException("Broker connection is fatal.");
```

Policies: `FailFastPolicy` (current state, no wait), `UpTo(timeout)`,
`Indefinitely`. States: `Ready`, `TimedOut`, `Fatal`. `bus.State`
reports the live `BusConnectionState` (`Starting`, `Ready`,
`TransientFailure`, `Fatal`).

A worker that must not publish before the bus is ready:

```csharp
public sealed class CheckoutWorker(IStreamBus bus) : BackgroundService
{
    protected override async Task ExecuteAsync(CancellationToken ct)
    {
        var readiness = await bus.WaitUntilReadyAsync(
            ReadinessPolicy.UpTo(TimeSpan.FromSeconds(30)), ct);
        if (readiness != BusReadiness.Ready)
            throw new StreamBusTimeoutException("StreamBus did not become ready.");
        await bus.PublishAsync(new OrderEvent("o-1", 1), ct);
    }
}
```

## Health checks

A `streambus` health check is registered automatically. Data entries use
two shapes:

- `{vhost}|{busType}` - the live connection state for that bus.
- `{vhost}|{registration}` - one row per typed consumer or declaration,
  formatted as `consumer:<state>` or `declare:<state>`, with the failure
  reason and `since` timestamp appended when present.

The check is healthy only when every registration is attached and every
connection is ready. It is degraded while a bus is starting or retrying,
or while some consumers are retrying. It is unhealthy when a connection
is fatal, a declaration has exhausted its retry budget, or every typed
consumer has failed. No registered buses reports degraded with a hint.

```csharp
var report = await health.CheckHealthAsync(ct);
var entry = report.Entries["streambus"];
var connection = entry.Data["/|StreamBus.DefaultBus"];
```
