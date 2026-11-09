# StreamBus TLS Support Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add optional, off-by-default TLS to both StreamBus transports (stream protocol + AMQP error queue) behind a single `UseTls` host-configuration switch, with TLS default ports 5551/5671 and explicit user ports always winning.

**Architecture:** One `StreamBusTlsOptions` record on `StreamBusOptions`, built by one `UseTls` method on `IHostConfigurator`. The stream connector maps it to `RabbitMQ.Stream.Client`'s `SslOption` on `StreamSystemConfig.Ssl`; the AMQP error-queue factory maps it to `RabbitMQ.Client`'s `SslOption` on `ConnectionFactory.Ssl`. TLS state joins the connection-identity comparison in the config store.

**Tech Stack:** .NET 10, `RabbitMQ.Stream.Client` 1.12.1 (stream client `SslOption`: `Enabled`, `ServerName`, `Certs`, `AcceptablePolicyErrors`, `CertPath`, `CertPassphrase`, `Version`), `RabbitMQ.Client` 7.2.2 (AMQP client `SslOption`: `Enabled`, `ServerName`, `Certs`, `AcceptablePolicyErrors`, `CertPath`, `CertPasswd`, `Version`), xUnit v3, Testcontainers.RabbitMq.

**Spec:** `docs/2026-11-09-streambus-tls-design.md`

Note: both client libraries expose a class named `SslOption` in different namespaces (`RabbitMQ.Stream.Client` vs `RabbitMQ.Client`). Inside files that use both, alias them (`using StreamSslOption = RabbitMQ.Stream.Client.SslOption;`).

## Global Constraints

- TLS is OFF by default: with no `UseTls` call, all behavior is byte-identical to today (plain TCP, 5552/5672).
- One switch: `UseTls` applies to both transports. No per-transport toggle.
- Explicit `StreamPort`/`AmqpPort` calls always win over TLS default ports (5551 stream, 5671 AMQP), regardless of call order. Track "user-set" vs "default" with nullable backing fields; resolve at `Build()`.
- Naming is `UseTls` / `Tls` throughout (never "Ssl" in public API names).
- `ServerName`: stored null in options; both connectors default it to `options.Host` at connect time. Null default keeps `StreamBusOptions` record equality simple.
- Validation surface: `StreamBusOptions.Validate()` stays the single validation boundary. Client cert file existence is checked at connect time (both connectors throw `StreamBusConfigurationException` with a clear message before dialing), per spec.
- `ErrorClassification` already maps `SslException` — do not regress it.
- TDD: every task writes the failing test first. Commands use `dotnet test` from repo root.
- `UseTls` may be called at most... no: `UseTls` is last-wins idempotent-overwritable — a second call with a new callback replaces the TLS settings (documented behavior).

## Review Focus

Input classes the design implies but task tests below must still pin:

1. **TLS enabled against a plaintext listener** — connection fails as a classified TLS error, not a hang or a confusing timeout. Test: TlsErrorClassification unit test in Task 5 (ErrorClassification mapping present and covered for `SslException`).
2. **Client cert path points at a missing file** — clear `StreamBusConfigurationException` naming the path, thrown before connecting. Test: both connector BuildX unit tests in Tasks 3 and 4.
3. **`Enabled = false` with junk fields set** — ignored entirely; factory/system get no SslOption (plain TCP). Test: Tasks 3/4 BuildX asserts `Ssl.Enabled == false`/absent.
4. **Same vhost, two registrations differing only in TLS** — the config store rejects the second (connection identity must include TLS). Test: Task 5 `SameConnection`/`Add` unit test.
5. **ServerName fallback to Host** — both transports validate the server certificate against `Host` when no `ServerName` was configured. Test: Tasks 3/4 BuildX assert `Ssl.ServerName == options.Host` when null.
6. **`UseTls()` then `StreamPort(x)`** — x wins; also `StreamPort(x)` then `UseTls()` — x still wins. Test: Task 2 configurator tests, both orders.

---

### Task 1: `StreamBusTlsOptions` record + `StreamBusOptions.Tls` + validation

**Files:**
- Create: `src/StreamBus/TlsOptions.cs`
- Modify: `src/StreamBus/Options.cs` (add `Tls` property + validation branch in `Validate()`)
- Test: `tests/StreamBus.UnitTests/TlsOptionsTests.cs`

**Interfaces:**
- Produces: `public sealed record StreamBusTlsOptions { bool Enabled; string? ServerName; string? ClientCertPath; string? ClientCertPassword; SslPolicyErrors AcceptableCertificateErrors; SslProtocols ProtocolVersion }` (all `init`, defaults: `Enabled = false`, `AcceptableCertificateErrors = SslPolicyErrors.None`, `ProtocolVersion = SslProtocols.None` meaning "OS default" — document in XML doc), plus `StreamBusOptions.Tls { get; init; }` of type `StreamBusTlsOptions?` defaulting to `null`.
- Validation rules added to `Validate()`: when `Tls?.Enabled == true` and `Tls.ServerName` is non-null it must be non-whitespace. `Tls == null` or `Enabled == false` produce no errors regardless of other fields.

- [ ] **Step 1: Write failing tests** in `TlsOptionsTests.cs` covering: (a) default `StreamBusOptions` has `Tls == null` and validates clean; (b) `Tls` with `Enabled = true` and no server name validates clean (fallback case); (c) `Enabled = true` with `ServerName = "  "` fails with a message containing "ServerName"; (d) `Enabled = false` with whitespace `ServerName` validates clean (fields ignored).
- [ ] **Step 2: Run** `dotnet test tests/StreamBus.UnitTests --filter TlsOptions` — expect compile failure (`StreamBusTlsOptions` undefined).
- [ ] **Step 3: Implement** `StreamBusTlsOptions` in `src/StreamBus/TlsOptions.cs` (XML docs, `sealed record` matching the style of `Options.cs`), add `Tls` to `StreamBusOptions` after `AmqpPort`, extend `Validate()` per the rules above.
- [ ] **Step 4: Run** `dotnet test tests/StreamBus.UnitTests` — all pass.
- [ ] **Step 5: Commit** `git add -A && git commit -m "feat: add StreamBusTlsOptions with validation boundary"` style message matching the repo's commit convention (inspect `git log --oneline -5` and mirror it).

### Task 2: `UseTls` host configuration + port-default resolution

**Files:**
- Modify: `src/StreamBus/Configuration/StreamBusConfigurator.cs` (interface `IHostConfigurator`, new `ITlsConfigurator`, `HostConfigurator`, configurator state, `Build()`)
- Test: extend the existing configurator unit test file(s) — find them with `grep -l "IHostConfigurator" tests/StreamBus.UnitTests` (likely `HostingTests.cs` or a dedicated configurator test); if none test the host callback directly, create `tests/StreamBus.UnitTests/TlsConfiguratorTests.cs`.

**Interfaces:**
- Consumes: `StreamBusTlsOptions` (Task 1).
- Produces: `void UseTls(Action<ITlsConfigurator>? configure = null)` on `IHostConfigurator`; `public interface ITlsConfigurator { void ServerName(string name); void ClientCertificate(string path, string? password = null); void AcceptableCertificateErrors(SslPolicyErrors errors); void ProtocolVersion(SslProtocols version); }`. `Build()` emits `StreamBusOptions.Tls` (enabled: settings record; disabled/never-called: `null`).
- Port resolution: keep private `int? _streamPortUser; int? _amqpPortUser;` alongside the existing ints (existing `StreamPort`/`AmqpPort` setters record the user value); private `bool _tlsEnabled; StreamBusTlsOptions? _tls;`. In `Build()`: if `_tlsEnabled` and `_streamPortUser` is null → `StreamPort = 5551`; same for AMQP → `5671`. Explicit values always win, order-independent.

- [ ] **Step 1: Write failing tests** covering: (a) no `UseTls` → `options.Tls` null and ports 5552/5672; (b) `UseTls()` alone → ports 5551/5671, `Tls.Enabled == true`; (c) `UseTls()` before/after `StreamPort(1234)`/`AmqpPort(15672)` → explicit values win in both orders; (d) `UseTls(tls => { tls.ServerName("broker.example"); tls.ClientCertificate("cert.pfx", "pw"); tls.AcceptableCertificateErrors(SslPolicyErrors.RemoteCertificateChainErrors); })` → all fields land on options; (e) second `UseTls` overwrites the first (last-wins).
- [ ] **Step 2: Run filtered** — expect compile failure (`UseTls` undefined).
- [ ] **Step 3: Implement** per Interfaces block. `UseTls` sets `_tlsEnabled = true`, builds `StreamBusTlsOptions` from the callback (defaults: `ServerName = null`, `AcceptableCertificateErrors = None`, `ProtocolVersion = SslProtocols.None`), stores it. Apply port defaults in `Build()`, not in the callback, for order-independence.
- [ ] **Step 4: Run full unit suite** — pass.
- [ ] **Step 5: Commit.**

### Task 3: Stream connector TLS wiring

**Files:**
- Modify: `src/StreamBus/Connector.cs` (`StreamSystemConnector.ConnectAsync`, lines ~310-337)
- Test: create `tests/StreamBus.UnitTests/StreamConnectorTlsTests.cs`

**Interfaces:**
- Consumes: `StreamBusOptions.Tls` (Tasks 1-2).
- Produces (internal, for testability): `internal static StreamSystemConfig BuildLocatorConfig(StreamBusOptions options)` in `StreamSystemConnector` — extracted from `ConnectAsync`; `ConnectAsync` then calls it and `StreamSystem.Create(config, ...)`.
- Mapping when `options.Tls?.Enabled == true`: `config.Ssl = new RabbitMQ.Stream.Client.SslOption { Enabled = true, ServerName = options.Tls.ServerName ?? options.Host, CertPath = options.Tls.ClientCertPath, CertPassphrase = options.Tls.ClientCertPassword, AcceptablePolicyErrors = options.Tls.AcceptableCertificateErrors, Version = options.Tls.ProtocolVersion == SslProtocols.None ? SslProtocols.None /*client default*/ : options.Tls.ProtocolVersion }`. When TLS off/absent: do not set `Ssl` at all. Client cert file existence: if `ClientCertPath` is non-null and `File.Exists` is false, throw `StreamBusConfigurationException($"TLS client certificate not found at '{path}'.")` before connecting.
- NOTE: verify against the actual `RabbitMQ.Stream.Client` 1.12.1 property names by inspecting the package (F12 / `obj` decompile); DeepWiki reports `Ssl`, `CertPath`, `CertPassphrase`, `AcceptablePolicyErrors`, `Version`, `Certs`. Trust the package over this plan if names differ; keep the test assertions on the mapped values.

- [ ] **Step 1: Write failing tests** against `BuildLocatorConfig`: (a) TLS null → `Ssl` null-or-disabled, ports/credentials unchanged from today's shape; (b) TLS enabled → `Ssl.Enabled == true`, `ServerName` from option; (c) `ServerName` null → `Ssl.ServerName == options.Host`; (d) missing cert file → `StreamBusConfigurationException` with the path in the message; (e) existing cert file (create a temp file in the test) → `CertPath` mapped, no throw.
- [ ] **Step 2: Run filtered** — expect compile failure (`BuildLocatorConfig` undefined).
- [ ] **Step 3: Implement** the extraction + mapping per Interfaces.
- [ ] **Step 4: Run full unit suite** — pass.
- [ ] **Step 5: Commit.**

### Task 4: AMQP error-queue factory TLS wiring

**Files:**
- Modify: `src/StreamBus/ErrorQueueTransport.cs` (`AmqpConnectionFactory.ConnectAsync`, lines ~14-40)
- Test: create `tests/StreamBus.UnitTests/AmqpConnectionFactoryTlsTests.cs`

**Interfaces:**
- Produces (internal, for testability): `internal static ConnectionFactory BuildFactory(StreamBusOptions options)` in `AmqpConnectionFactory`; `ConnectAsync` uses it.
- Mapping when TLS enabled: `factory.Ssl = new RabbitMQ.Client.SslOption { Enabled = true, ServerName = options.Tls.ServerName ?? options.Host, CertPath = options.Tls.ClientCertPath, CertPasswd = options.Tls.ClientCertPassword, AcceptablePolicyErrors = options.Tls.AcceptableCertificateErrors, Version = options.Tls.ProtocolVersion }`. TLS off: leave `factory.Ssl` default (disabled). Same missing-cert-file check as the stream connector (`StreamBusConfigurationException` naming the path, thrown before `CreateConnectionAsync`).
- `HostName`/`Port` continue to come from `options.Host`/`options.AmqpPort` — port 5671 by Task 2's default when TLS is on and user set nothing.

- [ ] **Step 1: Write failing tests** mirroring Task 3's five cases against `BuildFactory` (asserting `Port`, `HostName`, and `Ssl` members).
- [ ] **Step 2: Run filtered** — compile failure.
- [ ] **Step 3: Implement** extraction + mapping.
- [ ] **Step 4: Run full unit suite** — pass.
- [ ] **Step 5: Commit.**

### Task 5: Connection identity, registration view, error classification

**Files:**
- Modify: `src/StreamBus/Hosting.cs` (`StreamBusRegistrationView` + `SameConnection`)
- Test: extend `tests/StreamBus.UnitTests/HostingTests.cs` and `tests/StreamBus.UnitTests/ErrorClassificationTests.cs`

**Interfaces:**
- Consumes: options from Tasks 1-2.
- Produces: `StreamBusRegistrationView` gains `public required bool TlsEnabled { get; init; }` (populated from `r.Options.Tls?.Enabled ?? false`); `SameConnection(a, b)` additionally requires `a.Tls == b.Tls` (record equality covers it — add `Tls` to the `&&` chain).

- [ ] **Step 1: Write failing tests:** (a) two registrations same vhost, identical except TLS → second `store.Add` throws `StreamBusConfigurationException` mentioning conflicting connection options; (b) registrations differing only in disabled-TLS fields (same `Enabled`) do NOT conflict; (c) registration view exposes `TlsEnabled == true/false` correctly for both cases; (d) ErrorClassification: `SslException` maps to the same classification it has today (pin it — TLS-on-plaintext must surface as that classified error).
- [ ] **Step 2: Run filtered** — expect failures (missing `TlsEnabled`, `SameConnection` conflict test fails).
- [ ] **Step 3: Implement** the three changes. Update any other construct sites of `StreamBusRegistrationView` (grep for it) to populate `TlsEnabled`.
- [ ] **Step 4: Run full unit suite + build** `dotnet build` — pass.
- [ ] **Step 5: Commit.**

### Task 6: TLS integration test (real broker over TLS) + docs

**Files:**
- Create: `tests/StreamBus.IntegrationTests/TlsBroker.cs` (fixture, mirrors `TestBroker.cs`) and `tests/StreamBus.IntegrationTests/TlsIntegrationTests.cs`; certs under `tests/StreamBus.IntegrationTests/Tls/`
- Modify: `docs/guide/config.md` (document `UseTls` with the pseudocode from the design doc; note default ports 5551/5671 and that explicit ports win)

**Approach:** second shared fixture alongside `TestBroker`: a `rabbitmq:4.2-management` container with TLS listeners on both `stream.listeners.ssl.default` (in-container 5551) and AMQP `listeners.ssl.default` (in-container 5671), pre-allocated random host ports (reuse `TestBroker`'s pattern of matching host port and advertised stream port). Generate a throwaway test CA + localhost server cert once with `openssl` (script committed at `tests/StreamBus.IntegrationTests/Tls/generate-certs.ps1`, outputs committed as fixtures; throwaway CA, never used elsewhere) and mount via `WithResourceMapping` into `/etc/rabbitmq/certs`. Broker config: `ssl_options.certfile`/`keyfile`/`cacertfile`, `ssl_options.verify = verify_none`, `ssl_options.fail_if_no_peer_cert = false`, plus the `rabbitmq_stream advertised_host localhost advertised_port <hostPort>` env arg as in `TestBroker.StartCoreAsync`.

- [ ] **Step 1: Generate certs** per the script; verify the container starts with TLS listeners by probing the mapped ports before writing tests.
- [ ] **Step 2: Write the failing integration test:** configure a bus with `UseTls(tls => tls.AcceptableCertificateErrors(SslPolicyErrors.RemoteCertificateChainErrors | SslPolicyErrors.RemoteCertificateNameMismatch))` (self-signed CA is not OS-trusted; name may mismatch container hostname) against the TLS broker; round-trip: publish to a stream, consume, and force an error message onto the error queue (reuse the pattern from `ErrorQueueIntegrationTests.cs`) so BOTH transports' TLS paths are exercised. Assert message round-trips.
- [ ] **Step 3: Run** `dotnet test tests/StreamBus.IntegrationTests --filter Tls` — expect FAIL (fixture/feature gap) then iterate on fixture + feature until it PASSES against the real TLS broker.
- [ ] **Step 4: Run the FULL integration suite** `dotnet test` (all projects) — no regressions, non-TLS tests unchanged.
- [ ] **Step 5: Update `docs/guide/config.md`** with the `UseTls` section (pseudocode from the design doc; document default ports, explicit-port precedence, last-wins double call, client cert path checked at connect time).
- [ ] **Step 6: Commit.**
