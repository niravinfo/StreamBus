# Health, Readiness & Observability

> Startup blocking, connection states, the `"streambus"` health check, metrics,
> and logging. Source: `Readiness.cs`, `ConnectionSupervisor.cs`,
> `Hosting.cs:402-683`, `StreamBusMetrics.cs`, `ErrorClassification.cs`.

## 1. Readiness model

```csharp
public enum BusConnectionState { Starting, Ready, TransientFailure, Fatal }
public enum BusReadiness { Ready, TimedOut, Fatal }

public abstract record ReadinessPolicy
{
    public static ReadinessPolicy FailFastPolicy { get; }  // throw now if not ready
    public static ReadinessPolicy Indefinitely { get; }    // wait forever
    public static ReadinessPolicy UpTo(TimeSpan timeout)   // wait up to t
}
```

- `IStreamBus.State` = current state (cheap, sync).
- `await bus.WaitUntilReadyAsync(policy, ct)` = gate (`ConnectionSupervisor.cs:41-81`):
  `Ready`/`Fatal` return immediately; `FailFast` returns `TimedOut` when not ready;
  `WaitUpTo`/`Indefinitely` subscribe to state changes; caller-cancel is yours,
  policy-timeout maps to `TimedOut`.
- Every publish/consume/topology/offset call takes a `ReadinessPolicy`
  (default `FailFast`): not-ready → `StreamBusNotReadyException`; `Fatal` →
  `StreamBusFatalException` — except `WaitUntilReadyAsync` itself, which returns
  the `BusReadiness` value instead of throwing.
- Supervisor: single `ConnectAsync`, then `Ready` forever (no post-ready
  reconnect loop); transient failures retry with 200ms→30s backoff as
  `TransientFailure`; `ProtocolException`/`SslException`/auth-mechanism/auth-failure
  (inner-chain name prefix, `ErrorClassification.cs`) → `Fatal`.

## 2. Startup: block or background

Default non-blocking (`BlockStartupUntilReady = false`): `StartAsync` returns
while connect + declare + consumer-attach proceed in the background; background
faults = `LogCritical` only. **Workers should block**:

```csharp
o.Hosting(h => h.BlockStartupUntilReady(TimeSpan.FromSeconds(30)));
```

Blocking awaits connection ready, then all startup declarations + all typed
attaches under the timeout: config/conflict errors rethrown (fail fast),
else `StreamBusTimeoutException`. `null` timeout = wait indefinitely.

`StopAsync` disposes all active consumers (waits in-flight — hung handlers block
shutdown by design), cancels startup work, and disposes supervisors best-effort.

## 3. Health check

`AddStreamBus` registers `StreamBusHealthCheck` + `AddHealthChecks().AddCheck("streambus")`
once (`Hosting.cs:87-94`). Map it in a web/worker hybrid:

```csharp
app.MapHealthChecks("/healthz");
```

| Buses | Result |
|---|---|
| all `Ready` | `Healthy` |
| any `Fatal` | `Unhealthy` |
| otherwise (starting/transient, or zero buses) | `Degraded` |

`data` = `{ "vhost|busType": "State" }`. `IStreamBusRegistry.DescribeStates()`
exposes the same map programmatically.

## 4. Metrics (`Meter("StreamBus")`, `StreamBusMetrics.cs`)

| Instrument | Meaning |
|---|---|
| `streambus_partition_lag` (gauge) | `Max(0, LastDelivered − LastStored)` — **consumer-held backlog**, not broker-total. `0` when untouched; meaningless for effectively-once refs (no stored offset). Tags: vhost, bus, stream, partition, reference. |
| `streambus_partition_inflight` (gauge) | Messages currently in handler per partition. |
| `streambus_partition_retry_blocked` (gauge, `failure_mode=strict`) | `1` while a partition is parked in retry backoff. |
| `streambus_connection_state` (gauge, int) | Per (vhost, bus) connection state. |
| `streambus_offset_store_rejected_total` (counter) | Non-forward store admits rejected by the monotonic guard. |
| `streambus_offset_regressed_total` (counter) | Stored offset moved backwards (re-query detected). |
| `streambus_offset_fallback_total{branch}` (counter) | No-stored-offset fallbacks: `ResumeOrFirst` / `ResumeOrNext` / `PromotionFailed`. |

Replay consumers are untracked. Standard `System.Diagnostics.Metrics` — plug
OpenTelemetry / Prometheus exporter as usual.

## 5. Logging

All components take `ILoggerFactory` optionally (absent = `NullLogger`). Notable
events: fallback resolutions + broker-wins replays (warn), retry sleeps
(warn ≤ attempt 5 then every 100th, else debug), store failures (warn, retry
next success), SAC+Next downgrade (info), background startup faults (critical).
`DelayCalculator` throw/negative → warn + fallback/clamp.

## 6. Troubleshooting

| Symptom | Likely cause | Action |
|---|---|---|
| `StreamBusConfigurationException` at `Add*` | Duplicate bus/marker/consumer; same-vhost connection mismatch; bad knobs (pool, timeout, age, partitions, ref > 255 B) | Message names the conflict — align registrations/knobs |
| `StreamBusFatalException` / `Fatal` state | Protocol/SSL/auth (incl. `guest` on non-loopback) | Fix credentials/vhost/TLS, restart |
| `StreamBusNotReadyException` (FailFast) | Call before connect | `WaitUntilReadyAsync(UpTo(...))` first or pass a waiting policy |
| `StreamBusTimeoutException` (publish) | No confirm in `Timeout` — **outcome unknown** | Idempotent retry / dedup, don't blindly resend non-idempotent writes |
| `StreamBusStreamNotFoundException` | No such stream (no auto-create) | Declare via topology / `Ensure*Async` |
| `StreamConfigurationConflictException` | Immutable spec mismatch | Delete/recreate stream or align declaration |
| `StreamBusDeserializationException` | Serializer mismatch / null JSON | Align `o.Serializer(...)` with publishers |
| Consumer stalls (one partition, `retry_blocked=1`) | `Strict` handler throwing | Fix handler; Strict never skips by design |
| `SAC+Next` receives nothing | SAC auto-disabled on `Next` (logged) | Use explicit `Reference` + `AlwaysNext` for tails, or `ResumeOr*` for real groups |
| Lag gauge looks wrong on EO pipeline | By design (no stored offset) | Monitor target stream depth instead |
| Shutdown hangs | In-flight handler ignoring token | Honor the passed `CancellationToken` in handlers/publishers |
