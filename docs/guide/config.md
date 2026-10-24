# Config

## Code-first, user-owned

The library never reads configuration automatically. There is no
host-configuration overload and no JSON settings binding: the user owns the
values and passes them in code. Everything is validated together at
registration (`Build`), so bad inputs fail once at startup, each error
naming the knob.

If you want values from environment variables, a vault, or another store,
read them in your own code and pass the resolved values to the
configurator:

```csharp
services.AddStreamBus((_, cfg) =>
{
    cfg.Host("localhost", "/", h =>
    {
        h.Credentials("guest", "guest");
        h.StreamPort(5552);
        h.AmqpPort(5672);
        h.Heartbeat(TimeSpan.FromSeconds(60));
        h.MonitorInterval(TimeSpan.FromSeconds(5));
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
(used for queue dead-letter destinations and error-queue requeues;
otherwise no AMQP connection is opened), the heartbeat to 60 seconds,
and the connection monitor interval to 5 seconds. The heartbeat is always
mapped so a broker upgrade cannot drift it; the monitor interval is applied
explicitly by the supervisor:

```csharp
cfg.Host("rabbitmq.internal", "/", h =>
{
    h.Credentials("app", "s3cret");
    h.StreamPort(5552);
    h.AmqpPort(5672);
    h.Heartbeat(TimeSpan.FromSeconds(60));
    h.MonitorInterval(TimeSpan.FromSeconds(5));
    h.ProducersPerConnection(1);
    h.ConsumersPerConnection(1);
});
```

Credentials are validated centrally at startup: `UserName`/`Password`
must be non-empty. Empty values are rejected. `guest`/`guest` is accepted
against a machine-local broker; for remote or multi-service deployments
pass a dedicated non-`guest` user.

`MonitorInterval` controls how often the connection supervisor checks the
broker handle for closure. It must be positive and at most 60 seconds.

## Startup hosting

Blocking is service-level, not per-bus:

```csharp
services.AddStreamBusHosting(h => h.BlockStartupUntilReady(TimeSpan.FromSeconds(30)));
```

A second `AddStreamBusHosting` call throws. Without it, startup is
non-blocking: stream declarations and consumer attaches run in the
background.

Blocking startup waits for connections, topology declarations, and typed
consumer attaches by default. Opt out of the consumer wait explicitly:

```csharp
services.AddStreamBusHosting(h =>
{
    h.BlockStartupUntilReady(TimeSpan.FromSeconds(30));
    h.BlockUntilConsumersAttached(false);
});
```
