# Default Blocking Hosting — Implementation Plan

> **SUPERSEDED (Nov 11, 2026):** Replaced by `docs/superpowers/plans/2026-11-11-host-options.md`
> (MassTransit-parity `StreamBusHostOptions`; default stays NON-blocking). Do not execute this plan.

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** `AddStreamBus` alone makes startup block until the broker connection, topology, and consumers are ready (no timeout, indefinite wait); `AddStreamBusHosting` becomes an optional override, with `RunNonBlocking()` restoring the old behavior.

**Architecture:** The hosted service is already registered by `AddShared` (`src/StreamBus/Hosting.cs:135`); blocking is driven by the resolved `StreamBusHostingOptions` — today a blocking default exists only when `AddStreamBusHosting` ran. The flip: make `StreamBusHostingOptions`' defaults blocking (`WaitUntilStarted = true`, `WaitUntilConsumersAttached = true`, `StartTimeout = null`), so the `GetService<StreamBusHostingStore>() ?? new(new StreamBusHostingOptions())` fallback in `StreamBusHostedService.Create` (`Hosting.cs:431`) blocks without any extra call. `AddStreamBusHosting` keeps at-most-once semantics but now only *overrides*.

**Tech Stack:** .NET 10, xUnit v3, Testcontainers.RabbitMq (integration lane; requires Docker).

**Spec:** User direction, Nov 10 2026; startup-order claim verified empirically by `tests/StreamBus.IntegrationTests/HostingStartupOrderTests.cs` (passed against a Docker broker).

## Global Constraints

- Public API break gets a changelog entry in `docs/src/content/docs/guides/changelog.mdx`; doc pages other than Getting Started (still under evaluation — untouched by this plan) that describe hosting must be updated: `docs/src/content/docs/guides/hosting.mdx`, `docs/src/content/docs/guides/configuration.mdx`.
- **Open naming decision pinned for user review:** the opt-out is `h.RunNonBlocking()`. Veto before Task 1 executes.
- Opt-out also disables the consumer-attach wait; last-call-wins between `RunNonBlocking()` and the `WaitUntil*` methods.
- Explicit timeout overload `WaitUntilStarted(TimeSpan?)` is unchanged (null = indefinite).

## Review Focus

- Broker down at startup + new default: startup blocks indefinitely without throwing — a unit test must show `StartAsync` never completes while a readiness task source is un-signalable, and that the startup `CancellationToken` unblocks it (host shutting down must not deadlock).
- `RunNonBlocking()` after `WaitUntilStarted(timeout)` wins (and vice versa).
- Consumers attach wait: default still blocks until typed consumers attach; a failing consumer attach fails startup under the default as it did under explicit `WaitUntilConsumersAttached`.
- Integration tests that never called `AddStreamBusHosting` and manually invoke `StreamBusHostedService.StartAsync` (e.g. `TypedStartupIntegrationTests`) must still pass — they await `WaitUntilReadyAsync` themselves and must not hang differently.
- The Timeout path: `WaitUntilStarted(1s)` against a stopped broker still throws `StreamBusTimeoutException` at startup.

---

### Task 1: Fallback defaults become blocking; `RunNonBlocking()` opt-out

**Files:**
- Modify: `src/StreamBus/Hosting.cs:210-284` (`AddStreamBusHosting`, `StreamBusHostingOptions`, `StreamBusHostingOptionsBuilder`)
- Test: `tests/StreamBus.UnitTests/HostingTests.cs`

**Interfaces:**
- Produces: `StreamBusHostingOptions { WaitUntilStarted = true, WaitUntilConsumersAttached = true, StartTimeout = null }` as the default everywhere (including when `AddStreamBusHosting` was never called).
- Produces: `StreamBusHostingOptionsBuilder.RunNonBlocking()` — sets `_block = false`; consumers' attach wait disabled with it.
- `AddStreamBusHosting`: unchanged signature, at-most-once, now documented as "override the defaults".

- [ ] **Step 1: Write the failing tests in `HostingTests.cs`:** (a) build a provider with `AddStreamBus` only (no hosting call), resolve the store the hosted service uses — assert blocking defaults with `StartTimeout == null`; (b) `h.RunNonBlocking()` asserts `WaitUntilStarted == false`; (c) `RunNonBlocking()`-then-`WaitUntilStarted(5s)` and `WaitUntilStarted(5s)`-then-`RunNonBlocking()` — last call wins; (d) keep `AddStreamBusHosting_second_call_throws` and null-argument tests.
- [ ] **Step 2: Run, verify failure.**
  Run: `dotnet test tests/StreamBus.UnitTests --filter "FullyQualifiedName~HostingTests"`
  Expected: FAIL — default options currently non-blocking.
- [ ] **Step 3: Implement:** flip `StreamBusHostingOptions` defaults; add `RunNonBlocking()`; update XML docs for `AddStreamBusHosting` ("startup blocks by default; this overrides") and the builder summary line (`Hosting.cs:250`).
- [ ] **Step 4: Run the unit suite.**
  Run: `dotnet test tests/StreamBus.UnitTests`
  Expected: PASS — tests asserting the old "no AddStreamBusHosting → non-blocking" contract flip here (a list exists in Review Focus; update those, nothing else).
- [ ] **Step 5: Commit** `feat(hosting)!: AddStreamBus blocks startup until ready by default`

### Task 2: Broker-backed verification of the default

**Files:**
- Modify: `tests/StreamBus.IntegrationTests/HostingStartupOrderTests.cs` (exists; currently calls `AddStreamBusHosting`)

**Interfaces:**
- Consumes: Task 1's defaults and `RunNonBlocking`.

- [ ] **Step 1: Update the passing test** to drop the `AddStreamBusHosting` call (default is blocking now); worker still publishes on entry with no `WaitUntilReadyAsync`.
- [ ] **Step 2: Add the `RunNonBlocking` test:** no broker listening; app with `AddStreamBus` + `AddStreamBusHosting(h => h.RunNonBlocking())` reaches host-started (e.g. a hosted-service probe after it records it ran) within a few seconds, publish fails or pends — assert only that startup completed.
- [ ] **Step 3: Run.**
  Run: `dotnet test tests/StreamBus.IntegrationTests --filter "FullyQualifiedName~HostingStartupOrderTests"`
  Expected: PASS (Docker daemon required).
- [ ] **Step 4: Commit** `test: prove default blocking startup and RunNonBlocking against a real broker`

### Task 3: Regression sweep of the integration lane and examples

**Files:**
- Modify: `tests/StreamBus.IntegrationTests/TypedStartupIntegrationTests.cs:181-194`, `TopologyIntegrationTests.cs:224,258`, `TlsIntegrationTests.cs:125`, `ErrorQueueIntegrationTests.cs:82` — only if any relied on non-blocking defaults.
- Modify: `examples/StreamBus.Examples.Producer/Program.cs:60-61`, `examples/StreamBus.Examples.Consumer/Program.cs:65-66` — drop the now-redundant `AddStreamBusHosting` lines, keep comments explaining the default.

**Interfaces:** none new.

- [ ] **Step 1: Run the full integration lane.**
  Run: `dotnet test tests/StreamBus.IntegrationTests`
  Expected: PASS; fix any test that charters the changed contract in this task, not later.
- [ ] **Step 2: Update the examples; run both manually against the Testcontainers broker handle ad hoc.** Expected: producer seeds, consumer receives.
- [ ] **Step 3: Doc pages describing hosting** (`hosting.mdx`, `configuration.mdx`) + changelog entry. Snippets shown to the user for approval before commit (Global Constraints of the pending rules).
- [ ] **Step 4: Commit** `feat(hosting)!: examples and docs adopt the default blocking startup`
