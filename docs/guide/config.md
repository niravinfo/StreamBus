# Config

## Code-first, user-owned

The library never reads configuration automatically — the user owns the
values and passes them in code. Everything is validated together at
registration (`Build`), so bad inputs fail once at startup, each error
naming the knob.

```csharp
// Read the settings yourself (appsettings, environment, vault — anywhere).
var section = builder.Configuration.GetSection("StreamBus");
services.AddStreamBus((context, cfg) =>
{
    cfg.Host(
        section["Host"] ?? "127.0.0.1",
        section["Vhost"] ?? "/",
        h =>
        {
            h.Credentials(section["UserName"] ?? "app", section["Password"] ?? "");
            h.StreamPort(int.TryParse(section["StreamPort"], out var sp) ? sp : 5552);
            h.Heartbeat(TimeSpan.FromSeconds(
                double.TryParse(section["HeartbeatSeconds"], out var hb) ? hb : 60));
        });

    // Stream specs live on the registrations that reference the stream.
    cfg.Publish<InvoiceEvent>("invoices", s =>
    {
        s.MaxAge(TimeSpan.FromDays(30));
        s.MaxLengthBytes(1_000_000_000);
    });
    cfg.PublishSuper<OrdersEvent>("orders-super", s => s.Partitions(3));

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

Credentials are validated centrally at startup: `UserName`/`Password`
are required and `guest` is rejected (the stream client follows broker
metadata to non-loopback addresses, where the broker refuses `guest`).

## Startup hosting

Blocking is service-level, not per-bus:

```csharp
services.AddStreamBusHosting(h => h.BlockStartupUntilReady(TimeSpan.FromSeconds(30)));
```

A second `AddStreamBusHosting` call throws. Without it, startup is
non-blocking (today's default): stream declarations and consumer
attaches run in the background.
