# Config

## Code-first, user-owned

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
(used for queue dead-letter destinations;
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

## TLS

TLS is optional and off by default: with no `UseTls` call, both
transports (stream protocol and the AMQP error-queue connection) speak
plain TCP. Enabling it switches both at once — there is no per-transport
toggle — and, unless you set ports explicitly, moves the defaults to the
conventional RabbitMQ TLS ports (stream 5551, AMQP 5671):

```csharp
cfg.Host("rabbitmq.internal", "/", h =>
{
    h.Credentials("app", "s3cret");
    h.StreamPort(15552);      // optional: explicit ports always win,
    h.AmqpPort(15671);        // regardless of call order
    h.UseTls(tls =>
    {
        tls.ServerName("rabbitmq.internal");            // default: Host
        tls.ClientCertificate("./certs/client.pfx", "pw"); // mutual TLS
        tls.AcceptableCertificateErrors(SslPolicyErrors.None);
        tls.ProtocolVersion(SslProtocols.Tls13);        // optional
    });
});
```

Details:

- `ServerName` defaults to the host; it drives TLS certificate validation
  (SNI and name matching).
- `AcceptableCertificateErrors` lists certificate policy errors that are
  tolerated despite failing validation (private-CA deployments commonly
  pass `SslPolicyErrors.RemoteCertificateChainErrors`); the default is
  `None`, i.e. every error fails the connection. **Caution:** accepting
  `RemoteCertificateNameMismatch` or `RemoteCertificateNotAvailable`
  disables hostname verification — the TLS protection against
  man-in-the-middle attacks — and should not be used in production.
- `ProtocolVersion` defaults to the operating system default
  (`SslProtocols.None`). Anything below TLS 1.2 is rejected.
- The client certificate (optional; for mutual TLS) is a PFX file path plus
  optional password. The file's existence is checked at connect time, not
  at configuration time.
- Calling `UseTls` again replaces the previous TLS settings (last call
  wins).

## Startup hosting

Blocking is service-level, not per-bus:

```csharp
services.AddStreamBusHosting(h => h.WaitUntilStarted(TimeSpan.FromSeconds(30)));
```

A second `AddStreamBusHosting` call throws. Without it, startup is
non-blocking: stream declarations and consumer attaches run in the
background.

Blocking startup waits for connections, topology declarations, and typed
consumer attaches by default. Opt out of the consumer wait explicitly:

```csharp
services.AddStreamBusHosting(h =>
{
    h.WaitUntilStarted(TimeSpan.FromSeconds(30));
    h.WaitUntilConsumersAttached(false);
});
```
