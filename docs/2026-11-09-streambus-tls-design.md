# StreamBus TLS Support Design

Date: 2026-11-09
Status: Approved (short design)

## Intent

Add optional TLS to both StreamBus transports — the RabbitMQ stream protocol
connection (stream producer/consumer, default port 5552) and the AMQP 0-9-1
connection used by the error-queue transport (default port 5672), which
publishes error messages when a consume fails. Both transports share host and
credentials, so TLS is controlled by a single switch. TLS is optional and OFF
by default: with no configuration, behavior is byte-identical to today.

## Key user-facing decisions

- One method: `IHostConfigurator.UseTls(Action<ITlsConfigurator>? configure = null)`.
  Applies to both transports. No per-transport toggle.
- Naming: `UseTls`, not `UseSsl` — SSL refers to deprecated protocols; TLS is
  what is actually negotiated (.NET, RabbitMQ docs, and both client libraries
  all use TLS terminology).
- Not restrictive: ports remain user-supplied via existing
  `StreamPort(int)` / `AmqpPort(int)`. Calling `UseTls()` without explicit
  ports switches the defaults to the conventional RabbitMQ TLS ports
  (stream 5551, AMQP 5671). Explicit `StreamPort`/`AmqpPort` calls always win,
  regardless of call order. The configurator tracks "user-set" vs
  "default-applied" separately.
- Mutual TLS supported: server name override, client certificate
  (path + password), acceptable certificate error policy, optional protocol
  version restriction.

## Configuration surface (pseudocode)

```
bus.Host("rabbitmq.internal", "/", h =>
{
    h.Credentials("user", "pass");
    h.StreamPort(15552);      // optional; explicit ports always win
    h.AmqpPort(15671);
    h.UseTls(tls =>
    {
        tls.ServerName("rabbitmq.internal");            // default: Host
        tls.ClientCertificate("./certs/client.pfx", "pw");
        tls.AcceptableCertificateErrors(SslPolicyErrors.None);
        tls.ProtocolVersion(SslProtocols.Tls13);        // optional
    });
});
```

Options model:

```csharp
public sealed record StreamBusTlsOptions
{
    public bool Enabled { get; init; }                    // false = off
    public string? ServerName { get; init; }              // defaults to Host
    public string? ClientCertPath { get; init; }
    public string? ClientCertPassword { get; init; }
    public SslPolicyErrors AcceptableErrors { get; init; } // default None
    public SslProtocols Protocols { get; init; }           // default: OS/Tls12+
}
```

`StreamBusOptions` gains `StreamBusTlsOptions? Tls { get; init; }` (null = off).
`Hosting`/health records surface TLS state so connection identity comparison
accounts for it.

## Internal wiring

- Stream side (`StreamSystemConnector`): set `SslOption` on
  `RabbitMQ.Stream.Client.StreamSystemConfig`. Note the client takes an
  `X509CertificateCollection`/cert path + optional password; map from
  `StreamBusTlsOptions`.
- AMQP side (`AmqpConnectionFactory`): set `ConnectionFactory.Ssl`
  (`SslOption` from `RabbitMQ.Client`), including `AcceptablePolicyErrors` and
  client certificate collection.
- `ErrorClassification` already maps `SslException`; review for any TLS
  handshake failure shapes worth classifying as fatal/retryable.

## Validation (single boundary: `StreamBusOptions.Validate()`)

- If `Tls?.Enabled`: `ClientCertPath`, when non-null, must reference an
  existing file (checked at connect time to avoid build-machine surprises —
  pick one and note it in the plan).
- `ServerName` non-empty when supplied.
- Ports validated as today (1..65535); no coupling to TLS state.

## Non-goals

- Per-transport TLS toggles (mixed TLS stream + plaintext AMQP).
- Certificate thumbprint/store-based client cert selection (path-based only
  for now; can be added later without breaking changes).
- TLS for the management API (not used by the library).

## Approaches considered

1. Per-transport toggles (`UseStreamTls`/`UseAmqpTls`) — rejected: shared
   credentials argue for one switch; splits the API for a case (mixed TLS
   plaintext) we explicitly do not want to encourage.
2. Global `UseTls` with TLS-port defaults and explicit-port override —
   **chosen**: simple, matches the shared host/credential model, ports fully
   user-controlled.
3. Toggling via options record only (no fluent method) — rejected: the
   fluent host configurator is the public surface for connection settings.

## Out of scope for this doc

Test plan, implementation steps (covered by the implementation plan that
follows this doc).
