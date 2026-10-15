# Config

## Code-first

```csharp
services.AddStreamBus((context, cfg) =>
{
    cfg.Host("rabbitmq.internal", "/", h => h.Credentials("app", "s3cret"));

    cfg.Serializer(json => json.PropertyNamingPolicy = JsonNamingPolicy.CamelCase);
    cfg.DeclareStream("audit", k => k.MaxAge(TimeSpan.FromDays(30)));
    cfg.DeclareSuperStream("orders-super", partitions: 3);

    cfg.Publish<OrderEvent>("orders");
    cfg.ReceiveEndpoint("orders", e => e.Consumer<OrderConsumer>("order-processors"));
});
```

`Host` is required exactly once per bus (a second call throws). Pool
sizes default to 1/1, the stream port to 5552, the AMQP port to 5672
(only used with queue dead-letter destinations — otherwise no AMQP
connection is ever opened), and the heartbeat to 60 seconds, always
mapped so broker upgrades cannot drift it:

```csharp
cfg.Host("rabbitmq.internal", "/", h =>
{
    h.Credentials("app", "s3cret");
    h.StreamPort(5552);
    h.AmqpPort(5672);
    h.Heartbeat(TimeSpan.FromSeconds(60));
    h.ProducersPerConnection(1);
    h.ConsumersPerConnection(1);
});
```

## JSON binding

```json
{
  "StreamBus": {
    "Vhost": "/",
    "UserName": "app",
    "Password": "s3cret",
    "Host": "127.0.0.1",
    "StreamPort": 5552,
    "AmqpPort": 5672,
    "HeartbeatSeconds": 60,
    "Connection": { "ProducersPerConnection": 1, "ConsumersPerConnection": 1 },
    "Topology": {
      "Streams": [{ "Name": "audit", "MaxAgeDays": 30 }],
      "SuperStreams": [{ "Name": "orders-super", "Partitions": 3 }]
    }
  }
}
```

```csharp
services.AddStreamBus(
    builder.Configuration.GetSection("StreamBus"),
    (context, cfg) =>
    {
        cfg.Publish<OrderEvent>("orders");
        cfg.ReceiveEndpoint("orders", e => e.Consumer<OrderConsumer>("order-processors"));
    });
```

Binding keys and defaults: `Vhost` (`/`), `UserName`/`Password`
(required), `Host` (`127.0.0.1`), `StreamPort` (5552), `AmqpPort` (5672),
`HeartbeatSeconds` (60), `Connection:ProducersPerConnection` /
`Connection:ConsumersPerConnection` (1), `Topology:Streams[]:{Name,
MaxAgeDays, MaxLengthBytes, MaxSegmentSizeBytes, LeaderLocator}`,
`Topology:SuperStreams[]:{... + Partitions}`. Malformed values fail
together in one error naming each section path and key. The code lambda
runs after binding and adds types, routes, endpoints, and retry.

## Startup hosting

Blocking is service-level, not per-bus:

```csharp
services.AddStreamBusHosting(h => h.BlockStartupUntilReady(TimeSpan.FromSeconds(30)));
```

A second `AddStreamBusHosting` call throws. Without it, startup is
non-blocking (today's default): declarations and consumer attaches run
in the background.
