# Observability

## Health

The `streambus` health check reports both connection and registration
state:

- `{vhost}|{busType}` - connection state for that bus (for example
  `/|StreamBus.DefaultBus`).
- `{vhost}|{registration}` - one row per typed consumer or declaration,
  formatted as `consumer:<state>` or `declare:<state>`, with the reason
  and `since` timestamp when present.

No buses registered reports degraded with a hint. The check is healthy
only when every registration is attached and every connection is ready.
It is degraded while a bus is starting or retrying, or while some
consumers are retrying. It is unhealthy when a connection is fatal, a
declaration has exhausted its retry budget, or every typed consumer has
failed.

The connection monitor runs at `h.MonitorInterval(...)` (default 5
seconds), so a broker that disappears is detected on that cadence.

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
| `streambus_pipeline_duration` | histogram (seconds) | kind = publish \| consume |

The lag gauge compares the last delivered offset against the last stored
offset per partition. It is not meaningful on effectively-once
references (the source never stores offsets).

The pipeline duration covers the whole chain, including filters and the
terminal handler. It is recorded only when a pipeline actually runs
(i.e. at least one filter is configured).

## Activities

`ActivitySource` name: `StreamBus` (version `2.0.0`). It is always
available and costs nothing when no listener samples it.

| Span | Kind | Tags |
|---|---|---|
| `streambus.publish` | Producer | `streambus.stream`, `streambus.size`, `streambus.routing_key` (super only) |
| `streambus.consume` | Consumer | `streambus.stream`, `streambus.partition`, `streambus.offset` |

Consume spans add an `exception` event with `exception.type` and
`exception.message` when a delivery fails. Consume spans carry an
`ActivityLink` when the AMQP `correlation-id` holds a valid W3C
`traceparent`; ids produced by StreamBus are Guids, so the link applies to
foreign producers only. Malformed values are ignored rather than thrown.

## Logs

Expect warnings (not errors) for routine redelivery: handler failures
during retry (debug after the first attempts), exhausted budgets naming
stream, partition, offset, and attempts, error-stream handoff publish
retries, `OnExhausted` faults noting the attach recycle, and requeue
skips for malformed error-queue entries. Shutdown races log at debug.
