# StreamBus Host Options — Design

**Date:** Nov 11, 2026
**Status:** Approved in conversation, Nov 11 2026
**Supersedes:** `docs/superpowers/plans/2026-11-10-blocking-hosting-default.md` (blocking-default flip — abandoned in favor of MassTransit parity)

## Intent and why

Users asked why StreamBus's startup blocking differs from MassTransit and directed parity:
host configuration must follow MassTransit's Options-pattern posture (`MassTransitHostOptions`),
not a bespoke fluent extension. Defaults must match MassTransit exactly, and an infinite
timeout must behave truly infinitely. Prior art checked: MassTransit v8
`MassTransitHostOptions` docs and the history in MassTransit issue #1644 (blocking default
was deliberately abandoned there in favor of background connect + health checks).

## Goals

- One options class, `StreamBusHostOptions`, configured via the standard
  `services.AddOptions<StreamBusHostOptions>().Configure(...)`.
- Defaults identical to MassTransit: startup does **not** block; blocking is opt-in
  (`WaitUntilStarted = true`).
- `StartTimeout = null` (the default) blocks indefinitely when waiting is enabled; a set
  timeout throws `StreamBusTimeoutException` on expiry.
- Stop budgets ported too: `StopTimeout`, `ConsumerStopTimeout` (MassTransit parity set).
- Hosted service stays internal and auto-registered (already true via `AddShared`).

## Non-goals / anti-goals

- No flip of the default to blocking (the previous plan's goal — rejected).
- No at-most-once guard on configuration; Options pattern is naturally last-wins.
- Health-check behavior unchanged (`StreamBusHealthCheck` already reports
  degraded/unhealthy while connecting — the MassTransit posture complement).
- Getting Started doc rewrite stays out of scope (still under evaluation).

## The API

```csharp
public sealed class StreamBusHostOptions
{
    public bool WaitUntilStarted { get; set; }              // default false
    public TimeSpan? StartTimeout { get; set; }            // default null = infinite
    public bool WaitUntilConsumersAttached { get; set; } = true; // StreamBus extension
    public TimeSpan? StopTimeout { get; set; }             // default null = infinite
    public TimeSpan? ConsumerStopTimeout { get; set; }     // default null = unbounded
}
```

## Semantics

**Start (`StreamBusHostedService.StartAsync`)**
- `WaitUntilStarted = false`: return immediately after starting supervisors and
  background declare/attach tasks; permanent failures logged critical (today's
  non-blocking path, unchanged).
- `WaitUntilStarted = true`: wait for supervisor readiness, topology declaration, and —
  when `WaitUntilConsumersAttached` — every typed consumer row `Attached`.
- `StartTimeout = null`: no `CancelAfter` is armed; the wait is bounded only by the
  host's startup `CancellationToken` (shutdown still unblocks, never a deadlock).
- `StartTimeout` set: expiry throws `StreamBusTimeoutException` (violations of the
  "host stopping vs budget expired" disambiguation preserved from current code).
- **Attach failures wait:** a consumer in `FailedRetrying` does not fail startup; the
  loop keeps waiting (consumers retry indefinitely) until `Attached` or
  `StartTimeout`/shutdown. Accepted trade-off: a broken consumer config with an
  infinite timeout hangs startup — identical to MassTransit's posture.

**Stop (`StreamBusHostedService.StopAsync`)**
- `StopTimeout = null`: unbounded, today's behavior.
- `StopTimeout` set: the whole stop sequence (consumer disposes → startup CTS cancel →
  supervisor disposes → registry dispose) runs under a budget; on expiry remaining
  steps are abandoned with log warnings (dispose can't be force-killed; we stop
  awaiting it and proceed), matching the host's own shutdown-timeout contract.
- `ConsumerStopTimeout` set: each `consumer.DisposeAsync()` is awaited with
  `WaitAsync` under that budget, and the consumer's cooperative cancellation is
  triggered; expiry logs a warning and moves on.

**Validation (in `StartAsync`/`StopAsync`, throwing `StreamBusConfigurationException`)**
- `StartTimeout` and `StopTimeout` positive when set.
- `ConsumerStopTimeout <= StopTimeout` (MassTransit's rule).

## Deletions (breaking, listed loudly in the 0.2.0 changelog)

- `AddStreamBusHosting` (extension method), `StreamBusHostingOptionsBuilder`,
  `StreamBusHostingOptions`, `StreamBusHostingStore`.
- `StreamBusStartupBlockedException` — dead under the keep-waiting decision (the
  attach loop no longer throws on failed consumers).

## Wiring

- `AddShared` registers `services.AddOptions<StreamBusHostOptions>()`.
- `StreamBusHostedService.Create` resolves `IOptions<StreamBusHostOptions>`
  (replacing the `GetService<StreamBusHostingStore>() ?? new(...)` fallback at
  `Hosting.cs:431`).

## Blast radius (mechanical rewrites)

- `tests/StreamBus.UnitTests/HostingTests.cs` (~15 call sites),
  `TypedConsumerTests.cs` (builder-ctor + store-ctor sites),
  `ErrorQueueShutdownTests.cs` (store ctor).
- `tests/StreamBus.IntegrationTests`: `ErrorQueueIntegrationTests.cs`,
  `TlsIntegrationTests.cs`, `TopologyIntegrationTests.cs`,
  `TypedStartupIntegrationTests.cs` — `AddStreamBusHosting(h => h.WaitUntilStarted(t))`
  becomes `AddOptions<StreamBusHostOptions>().Configure(o => { o.WaitUntilStarted = true;
  o.StartTimeout = t; })`.
- `examples/StreamBus.Examples.Producer/Program.cs:61`,
  `examples/StreamBus.Examples.Consumer/Program.cs:66` — same rewrite, or dropped
  where the default is what's wanted.
- Docs: `hosting.mdx`, `configuration.mdx`, changelog entry.

## Approaches considered

1. **MassTransit-parity options class, delete the fluent API** — chosen. Exact parity,
   no redundant surface; package pre-0.2.0 so the break is cheap.
2. **Options class + keep `AddStreamBusHosting` as a translating shim** — rejected:
   two ways to configure one thing, double documentation and test surface.
3. **Keep fluent API as source of truth, options wrap it** — rejected: contradicts the
   requested posture.
4. **Previous plan (blocking default via flipped record defaults)** — rejected by the
   user after the MassTransit comparison; deliberately more blocking than MassTransit
   with no offsetting health-check story.

## Decisions record

- Delete the old API outright, no obsoletion shim — user, Nov 11.
- Full parity including `StopTimeout`/`ConsumerStopTimeout` — user, Nov 11.
- Attach failure keeps waiting (no fast-fail) — user, Nov 11.
- Validation placement in `StartAsync`/`StopAsync`, per-dispose `WaitAsync` for
  `ConsumerStopTimeout` — assistant's call.
