# StreamBus Host Options (MassTransit Parity) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace the `AddStreamBusHosting` fluent API with a MassTransit-parity `StreamBusHostOptions` class (Options pattern), with MassTransit defaults (non-blocking startup, infinite timeouts) and stop budgets (`StopTimeout`, `ConsumerStopTimeout`).

**Architecture:** Host configuration moves from a DI-carried `StreamBusHostingStore` to `Microsoft.Extensions.Options`: `AddShared` registers `services.AddOptions<StreamBusHostOptions>()`, and `StreamBusHostedService.Create` resolves `IOptions<StreamBusHostOptions>`. Start/stop behavior code paths stay; only what drives them changes. Attach-fail on typed consumers no longer fails blocking startup — the wait continues. `AddStreamBusHosting`, `StreamBusHostingOptionsBuilder`, `StreamBusHostingOptions`, `StreamBusHostingStore`, and `StreamBusStartupBlockedException` are deleted outright.

**Tech Stack:** .NET 10, xUnit v3, Testcontainers.RabbitMq (integration lane; requires Docker).

**Spec:** `docs/superpowers/specs/2026-11-11-host-options-design.md` (approved). Supersedes `docs/superpowers/plans/2026-10-13-blocking-hosting-default.md`.

## Global Constraints

- Public API break gets a loud changelog entry in `docs/src/content/docs/guides/changelog.mdx`; doc pages describing hosting must be updated: `docs/src/content/docs/guides/hosting.mdx`, `docs/src/content/docs/guides/configuration.mdx`. Getting Started stays untouched (still under evaluation).
- Defaults are MassTransit's exactly: `WaitUntilStarted = false`, `StartTimeout = null`, `StopTimeout = null`, `ConsumerStopTimeout = null`. `WaitUntilConsumersAttached = true` is the one StreamBus extension (only meaningful when `WaitUntilStarted = true`).
- Null timeout = truly infinite: no `CancelAfter` is armed; only the host's startup/shutdown `CancellationToken` ends the wait.
- Validation throws `StreamBusConfigurationException`: `StartTimeout`/`StopTimeout` positive when set; `ConsumerStopTimeout <= StopTimeout` (MassTransit's rule). `StartTimeout` validated in `StartAsync`; `StopTimeout`/`ConsumerStopTimeout` validated in `StopAsync`.
- No at-most-once guard anywhere; the Options pattern is last-wins by design.
- Breaking deletions: `AddStreamBusHosting`, `StreamBusHostingOptionsBuilder`, `StreamBusHostingOptions`, `StreamBusHostingStore`, `StreamBusStartupBlockedException`. No obsolete shims.

## Review Focus

- No options configured at all (empty service provider path): `StartAsync` must be non-blocking exactly as today — the `IOptions<StreamBusHostOptions>` default instance must never block.
- Host shutting down while a blocking start waits: the startup `CancellationToken` must unblock the wait and must NOT surface as `StreamBusTimeoutException` (existing disambiguation in `Hosting.cs:555-559, 588-592` — preserve it).
- A typed consumer stuck in `FailedRetrying` under `WaitUntilStarted = true` and no timeout: startup keeps waiting, never throws `StreamBusStartupBlockedException` (deleted) — only `StartTimeout` or shutdown ends it.
- `ConsumerStopTimeout` set but `StopTimeout` null (infinite stop): valid, per-consumer budget applies; validation must NOT require `StopTimeout` to be set.
- Options configured multiple times / via `IConfigureOptions` / bound from configuration: last-wins must hold with no at-most-once throws (a test must configure twice, unlike the old `AddStreamBusHosting_second_call_throws`).

---

### Task 1: `StreamBusHostOptions` class and validation

**Files:**
- Create: `src/StreamBus/HostOptions.cs`
- Test: `tests/StreamBus.UnitTests/HostOptionsTests.cs`

**Interfaces:**
- Produces: `public sealed class StreamBusHostOptions` with settable properties `bool WaitUntilStarted`, `TimeSpan? StartTimeout`, `bool WaitUntilConsumersAttached` (init `true`), `TimeSpan? StopTimeout`, `TimeSpan? ConsumerStopTimeout`; and `internal static void ValidateStart(StreamBusHostOptions o)` / `internal static void ValidateStop(StreamBusHostOptions o)` throwing `StreamBusConfigurationException`.

- [ ] **Step 1: Write the failing tests** in `HostOptionsTests.cs`:
  - `Defaults_match_MassTransit`: fresh `StreamBusHostOptions` — `WaitUntilStarted == false`, `StartTimeout == null`, `WaitUntilConsumersAttached == true`, `StopTimeout == null`, `ConsumerStopTimeout == null`.
  - `ValidateStart_rejects_non_positive_StartTimeout`: `StartTimeout = TimeSpan.Zero` and `-1s` throw `StreamBusConfigurationException`; `null` and `30s` do not.
  - `ValidateStop_rejects_non_positive_StopTimeout`: `StopTimeout = TimeSpan.Zero` throws; `null` does not.
  - `ValidateStop_rejects_ConsumerStopTimeout_greater_than_StopTimeout`: `(StopTimeout: 5s, ConsumerStopTimeout: 6s)` throws; `(5s, 5s)` allowed.
  - `ValidateStop_allows_ConsumerStopTimeout_without_StopTimeout`: `(StopTimeout: null, ConsumerStopTimeout: 1s)` does not throw.
- [ ] **Step 2: Run, verify failure.**
  Run: `dotnet test tests/StreamBus.UnitTests --filter "FullyQualifiedName~HostOptionsTests"`
  Expected: FAIL — type does not exist.
- [ ] **Step 3: Implement** `StreamBusHostOptions` in `src/StreamBus/HostOptions.cs` per the Interfaces block, with XML docs mirroring MassTransit's per-option descriptions (each doc line states the default explicitly), plus `ValidateStart`/`ValidateStop` as small static guards. Match the file's XML-doc style to the rest of `src/StreamBus` (see `Hosting.cs` records).
- [ ] **Step 4: Run.** Run: `dotnet test tests/StreamBus.UnitTests --filter "FullyQualifiedName~HostOptionsTests"` — PASS.
- [ ] **Step 5: Commit.**
```bash
git add src/StreamBus/HostOptions.cs tests/StreamBus.UnitTests/HostOptionsTests.cs
git commit -m "feat(hosting)!: add StreamBusHostOptions with MassTransit defaults"
```

### Task 2: Rewire the hosted service to options; delete the fluent API

**Files:**
- Modify: `src/StreamBus/Hosting.cs:110-143` (AddShared), `:210-284` (delete old API), `:380-431` (ctor + Create), `:540-635` (StartAsync wait paths)
- Modify: `src/StreamBus/Exceptions.cs:113-125` (delete `StreamBusStartupBlockedException`)
- Test: `tests/StreamBus.UnitTests/HostingTests.cs`, `tests/StreamBus.UnitTests/TypedConsumerTests.cs:176-184`, `tests/StreamBus.UnitTests/ErrorQueueShutdownTests.cs:84`

**Interfaces:**
- Consumes: `StreamBusHostOptions`, `ValidateStart` (Task 1).
- Produces: `internal StreamBusHostedService(..., StreamBusHostOptions options)` — the last ctor parameter is the options instance (replacing `StreamBusHostingStore`); `Create` resolves `services.GetRequiredService<IOptions<StreamBusHostOptions>>().Value`. `AddShared` calls `services.AddOptions<StreamBusHostOptions>()`. Task 3 adds `ValidateStop` consumption.

- [ ] **Step 1: Rewrite the affected unit tests first (deletion breaks compilation, so test rewrite and implementation land together in this task; the TDD red/green cycle for the new keep-waiting behavior is Step 3):**
  - `HostingTests.cs`: replace every `services.AddStreamBusHosting(h => h.WaitUntilStarted(t))` (and the `WaitUntilConsumersAttached` variants, lines 111, 121-131, 264-272, 340, 363, 408, 450, 503, 524, 843, 1041, 1075, 1106) with `services.AddOptions<StreamBusHostOptions>().Configure(o => { o.WaitUntilStarted = true; o.StartTimeout = t; })` (non-timeout call sites: only `o.WaitUntilStarted = true`). Keep assertions; the timeout/throw/infinite behaviors they pin (`StreamBusTimeoutException` at expiry, startup-cancellation rethrow) are unchanged contracts.
  - Delete `AddStreamBusHosting_second_call_throws`, `AddStreamBusHosting_null_arguments_throw`, and the builder-defaults test at lines 264-272. Add `Options_configured_twice_last_wins`: two `Configure` calls, second sets `WaitUntilStarted = false`; resolve `IOptions<StreamBusHostOptions>` — assert `Value.WaitUntilStarted == false`.
  - Add `No_options_registration_still_non_blocking` (Review Focus #1): provider with `AddStreamBus` only; `StreamBusHostedService.Create(sp)`; `StartAsync` completes without the fake connector ever signaling readiness (reuse the existing never-ready fake from the timeout tests).
  - `TypedConsumerTests.cs:176`: replace `new StreamBusHostingStore(new StreamBusHostingOptions())` ctor arg with a `StreamBusHostOptions` that has `WaitUntilStarted = false`. Delete lines 183-184 (builder timeout-validation tests — now covered by `HostOptionsTests`), replacing the direct ctor tests, per Step 1 of this task, with options-based ones.
  - `ErrorQueueShutdownTests.cs:84`: same replacement (options instance with `WaitUntilStarted = false`).
- [ ] **Step 2: Implement the rewire:**
  - `AddShared`: add `services.AddOptions<StreamBusHostOptions>();` next to the hosted-service registration (`Hosting.cs:135`).
  - Ctor: parameter type `StreamBusHostOptions options`; null-guard; assign field `_hosting` (field type changes to `StreamBusHostOptions`).
  - `Create`: `services.GetRequiredService<IOptions<StreamBusHostOptions>>().Value`.
  - `StartAsync`: call `StreamBusHostOptions.ValidateStart(_hosting)` at the top (before any supervisors start).
  - Attach-wait loop (`Hosting.cs:610-634`): delete the `FailedRetrying`/`Stopped` throw branch and its `StreamBusStartupBlockedException`; the loop now only exits when all rows are `Attached` (or the exit at `rows.Count == 0`). Keep the `StreamBusTimeoutException` on `StartTimeout` expiry (unchanged).
  - Delete `AddStreamBusHosting` (`Hosting.cs:210-220`), `StreamBusHostingOptions` (`:223-234`), `StreamBusHostingStore` (`:236-248`), `StreamBusHostingOptionsBuilder` (`:250-284`), and `StreamBusStartupBlockedException` (`Exceptions.cs:113-125`).
- [ ] **Step 3: Run, verify the failing consumer-attach test before fixing the loop** if sequencing allows: write `Attach_failure_keeps_waiting` first (using the existing consumer fakes from `HostingTests` — a typed consumer whose row parks in `FailedRetrying`), run, see FAIL (today it throws `StreamBusStartupBlockedException`), then apply the loop deletion. Assert: with `WaitUntilStarted = true`, no timeout, `StartAsync` task still incomplete after 500ms; with `StartTimeout = 200ms`, it throws `StreamBusTimeoutException`.
- [ ] **Step 4: Run the unit suite.**
  Run: `dotnet test tests/StreamBus.UnitTests`
  Expected: PASS.
- [ ] **Step 5: Commit.**
```bash
git add -A
git commit -m "feat(hosting)!: drive hosting from StreamBusHostOptions; delete AddStreamBusHosting"
```

### Task 3: Stop budgets — `StopTimeout` and `ConsumerStopTimeout`

**Files:**
- Modify: `src/StreamBus/Hosting.cs:817-850` (StopAsync)
- Test: `tests/StreamBus.UnitTests/ErrorQueueShutdownTests.cs` (stop-sequence harness lives here)

**Interfaces:**
- Consumes: `StreamBusHostOptions.StopTimeout`, `.ConsumerStopTimeout`, `ValidateStop` (Task 1); `StreamBusHostedService` ctor from Task 2.

- [ ] **Step 1: Write the failing tests**, constructing the hosted service directly with options (per Task 2's ctor) and the hanging-fake harnesses already in `ErrorQueueShutdownTests` (fakes whose `DisposeAsync` never completes, lines 98-114):
  - `StopTimeout_expires_and_proceeds`: `WaitUntilStarted = false`, `StopTimeout = 200ms`; a consumer whose `DisposeAsync` pends forever; `StopAsync` completes (with warnings) within a few seconds, then the supervisor/error-queue disposes still run.
  - `ConsumerStopTimeout_expires_and_proceeds`: `StopTimeout = null`, `ConsumerStopTimeout = 200ms`; same hanging consumer; `StopAsync` completes.
  - `Infinite_stop_waits_for_dispose`: `StopTimeout = null`; `DisposeAsync` completes after 300ms; `StopAsync` awaits it (does not abandon) — proves null = infinite, not zero.
  - `StopAsync_validates_before_anything`: `ConsumerStopTimeout = 6s, StopTimeout = 5s`; `StopAsync` throws `StreamBusConfigurationException` before any dispose is called (fake dispose counter must show zero calls).
- [ ] **Step 2: Run, verify failure.**
  Run: `dotnet test tests/StreamBus.UnitTests --filter "FullyQualifiedName~ErrorQueueShutdownTests"`
  Expected: FAIL — budgets not honored / validation not run.
- [ ] **Step 3: Implement in `StopAsync`:**
  - Call `StreamBusHostOptions.ValidateStop(_hosting)` first.
  - Build `stopCts` linked to `cancellationToken`, armed with `CancelAfter` only when `StopTimeout is { }`. Wrap every `await` in the sequence — per-consumer `DisposeAsync`, supervisor `DisposeAsync`, final `_registry.DisposeAsync()` — with `.WaitAsync(stopCts.Token)`; on `OperationCanceledException` from an expiring budget (not host cancellation), log one warning naming the abandoned step and continue to the next; host-token cancellation rethrows as today.
  - Per-consumer budget: a second linked CTS armed with `ConsumerStopTimeout` for each `consumer.DisposeAsync().WaitAsync(...)`, falling back to `stopCts.Token` when `ConsumerStopTimeout` is null; keep the existing per-failure warning wording.
  - Dispose `stopCts` at the end.
- [ ] **Step 4: Run the unit suite.** Run: `dotnet test tests/StreamBus.UnitTests` — PASS.
- [ ] **Step 5: Commit.**
```bash
git add src/StreamBus/Hosting.cs tests/StreamBus.UnitTests/ErrorQueueShutdownTests.cs
git commit -m "feat(hosting): add StopTimeout and ConsumerStopTimeout budgets"
```

### Task 4: Integration lane adopts options; broker-backed proof of parity

**Files:**
- Modify: `tests/StreamBus.IntegrationTests/ErrorQueueIntegrationTests.cs:82`, `TlsIntegrationTests.cs:125`, `TopologyIntegrationTests.cs:224,258`, `TypedStartupIntegrationTests.cs:195`, `HostingStartupOrderTests.cs`

**Interfaces:** none new — consumes Tasks 1-3.

- [ ] **Step 1: Mechanically rewrite the four call sites:** `AddStreamBusHosting(h => h.WaitUntilStarted(t))` → `services.AddOptions<StreamBusHostOptions>().Configure(o => { o.WaitUntilStarted = true; o.StartTimeout = t; })`. Keep timeouts identical to today.
- [ ] **Step 2: Update `HostingStartupOrderTests`:** the startup-order test that proved blocking keeps passing with options-based blocking (same wait, real broker). Add two cases:
  - `Default_starts_without_blocking`: `AddStreamBus` only, no broker listening — host reaches started (probe hosted service after it) within a few seconds; proves MassTransit's default against reality.
  - `WaitUntilStarted_infinite_never_completes_without_throw`: options with `WaitUntilStarted = true`, `StartTimeout = null`, no broker, startup CTS token — assert `StartAsync` pending after 1s, canceling token unblocks it.
- [ ] **Step 3: Run the integration lane.**
  Run: `dotnet test tests/StreamBus.IntegrationTests`
  Expected: PASS (Docker required). Fix any test charting the changed contract here, not later.
- [ ] **Step 4: Commit.**
```bash
git add tests/StreamBus.IntegrationTests
git commit -m "test: prove MassTransit-parity host options against a real broker"
```

### Task 5: Examples, docs, changelog

**Files:**
- Modify: `examples/StreamBus.Examples.Producer/Program.cs:60-61`, `examples/StreamBus.Examples.Consumer/Program.cs:65-66`
- Modify: `docs/src/content/docs/guides/hosting.mdx`, `configuration.mdx`, `changelog.mdx`

**Interfaces:** none new.

- [ ] **Step 1: Update both examples:** replace the `AddStreamBusHosting` line with `builder.Services.AddOptions<StreamBusHostOptions>().Configure(o => { o.WaitUntilStarted = true; o.StartTimeout = TimeSpan.FromSeconds(30); })` and a one-line comment noting the default is non-blocking.
- [ ] **Step 2: Run both manually** against the Testcontainers broker handle ad hoc (existing pattern from the Nov-10 plan's Task 3). Expected: producer seeds, consumer receives.
- [ ] **Step 3: Docs:** `hosting.mdx` and `configuration.mdx` rewritten around the options class (defaults table matching `HostOptions.cs` XML docs, the MassTransit comparison, validation rules); `changelog.mdx` breaking-entry listing every deleted symbol (`AddStreamBusHosting`, `StreamBusHostingOptionsBuilder`, `StreamBusHostingOptions`, `StreamBusHostingStore`, `StreamBusStartupBlockedException`) with before/after snippets. **Show the doc snippets to the user before committing** (per plan rules).
- [ ] **Step 4: Commit.**
```bash
git add examples docs
git commit -m "feat(hosting)!: docs and examples adopt StreamBusHostOptions"
```
