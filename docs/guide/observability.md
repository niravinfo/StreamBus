# Observability

## Health

The `streambus` health check reports per-bus connection state under data
keys `{vhost}|{busType}` (for example `/|StreamBus.DefaultBus`). No
buses registered reports degraded; any non-ready bus degrades the check
and a fatal connection marks it unhealthy.

## Metrics

Meter name: `StreamBus`. Consume with any `System.Diagnostics.Metrics`
listener (for example OpenTelemetry):

| Instrument | Kind | Tags |
|---|---|---|
| `streambus_partition_lag` | observable up-down counter | vhost, bus, stream, partition, reference |
| `streambus_partition_inflight` | observable up-down counter | vhost, bus, stream, partition, reference |
| `streambus_partition_retry_blocked` | observable up-down counter | vhost, bus, stream, partition, reference, failure_mode |
| `streambus_connection_state` | observable up-down counter (int) | vhost, bus |
| `streambus_offset_store_rejected_total` | counter | vhost, bus, stream, partition, reference |
| `streambus_offset_regressed_total` | counter | vhost, bus, stream, partition, reference |
| `streambus_offset_fallback_total` | counter | vhost, bus, stream, partition, reference |

The lag gauge compares the last delivered offset against the last stored
offset per partition. It is not meaningful on effectively-once
references (the source never stores offsets).

## Logs

Expect warnings (not errors) for routine redelivery: handler failures
during retry (debug after the first attempts), exhausted budgets naming
stream, partition, offset, and attempts, error-stream handoff publish
retries, `OnExhausted` faults noting the attach recycle, and requeue
skips for malformed error-queue entries. Shutdown races log at debug.
