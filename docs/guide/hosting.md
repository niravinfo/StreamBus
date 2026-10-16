# Hosting, readiness, health

## Startup modes

Default is non-blocking: `StartAsync` returns once connections start,
while declarations and consumer attaches converge in the background
(permanent failures surface as critical logs).

```csharp
services.AddStreamBusHosting(h => h.BlockStartupUntilReady(TimeSpan.FromSeconds(30)));
```

Blocking waits for connections, topology declarations (streams, super
streams, error queues), and consumer attaches — then either returns or
throws (`StreamBusTimeoutException` on timeout, configuration or
conflict errors immediately). Omit the timeout to wait indefinitely.

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

A `streambus` health check is registered automatically. Per-bus data
entries are keyed `{vhost}|{busType}` with the connection state as the
value; the check is healthy only when every bus is ready:

```csharp
var report = await health.CheckHealthAsync(ct);
var entry = report.Entries["streambus"];
```
