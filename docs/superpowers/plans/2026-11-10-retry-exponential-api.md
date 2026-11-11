# Retry `Exponential` API Change — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace the fluent `Exponential(retryCount, baseDelay, maxDelay, double multiplier)` with `Exponential(retryCount, minDelay, maxDelay)`, matching MassTransit and Polly conventions.

**Architecture:** Change confined to the fluent configurator surface. The internal `RetryPolicy` record keeps its `Multiplier` property (default `2.0`, `src/StreamBus/RetryPolicy.cs:29`), so low-level `ConsumeOptions.Retry` users are unaffected; the fluent method simply pins the growth factor at ×2, like Polly's `DelayBackoffType.Exponential`. Custom growth stays available through the existing `Intervals(params TimeSpan[])`.

**Tech Stack:** .NET 10, xUnit v3.

**Spec:** User review of comparable libraries, Nov 10 2026: MassTransit `Retry.Exponential(retryLimit, minInterval, maxInterval, intervalDelta)` has no growth-factor parameter; Polly hardcodes ×2 with `UseJitter`. Nobody exposes a raw `double multiplier`.

## Global Constraints

- Public API breaks get a changelog entry in `docs/src/content/docs/guides/changelog.mdx`.
- Doc prose uses no internal vocabulary; snippets shown to the user must be approved before any doc commit.
- `ConsumeOptions.Retry` (low-level policy) is out of scope and must not change.

## Review Focus

- Custom growth factors are still reachable: `Intervals(1.Seconds(), 4.Seconds(), 16.Seconds())` — no test changes needed, but the XML doc on `Exponential` must point there.
- Argument validation parity: negative `retryCount` throws `ArgumentOutOfRangeException`; non-positive `minDelay`/`maxDelay` throw `StreamBusConfigurationException` (message updated to the new parameter names).
- Capping: with `minDelay=1s, maxDelay=30s` and delays 1s,2s,4s,8s,16s, the 5th retry delay respects the cap when `retryCount` is larger (e.g. 10 retries → delays 1,2,4,8,16, then 30,30,...) — the existing `RetryPolicy` cap logic covers this; the unit test must pin it.
- No overload ambiguity: removing the 4-arg overload must not leave callers of the old arity compiling silently against `Intervals`.

---

### Task 1: New `Exponential` signature

**Files:**
- Modify: `src/StreamBus/Configuration/RetryConfiguration.cs:157-168` (interface declaration + XML docs), `:217-234` (implementation)
- Test: `tests/StreamBus.UnitTests/RetryConfigurationTests.cs:195-199`
- Modify: `docs/src/content/docs/guides/errors.mdx:43` (one line: the arity list in prose)

**Interfaces:**
- Produces: `void Exponential(int retryCount, TimeSpan minDelay, TimeSpan maxDelay)`. Behavior: delay before retry *i* is `minDelay × 2^(i−1)`, capped at `maxDelay`. Built policy: `Multiplier = 2.0, BaseDelay = minDelay, MaxDelay = maxDelay, MaxJitter = TimeSpan.Zero, MaxAttempts = retryCount + 1`.
- Produces: changelog entry (breaking change): "Rationale: matches MassTransit/Polly; use `Intervals` for custom growth."

- [ ] **Step 1: Update the test** `RetryConfigurationTests.cs:195-199`: call `Exponential(2, TimeSpan.FromMilliseconds(100), TimeSpan.FromSeconds(10))` and assert `Multiplier == 2.0`, `BaseDelay == 100ms`, `MaxDelay == 10s`, `MaxAttempts == 3`.
- [ ] **Step 2: Run, verify the old signature fails to compile.**
  Run: `dotnet test tests/StreamBus.UnitTests --filter "FullyQualifiedName~RetryConfigurationTests"`
  Expected: CS1503 at the new call.
- [ ] **Step 3: Implement** the new signature in interface + implementation, keep validation rules (non-negative count, positive delays — update exception text to `minDelay`/`maxDelay`), XML doc wording: "Delays double each attempt starting at `minDelay`, capped at `maxDelay`; use `Intervals` for custom growth factors."
- [ ] **Step 4: Run the cap-pin test** — extend the same test class: `Exponential(10, 1s, 30s)` produces a policy whose computed delays for attempts beyond attempt 5 are exactly `30s` (reuse the delay-calculation helper used by `RetryPolicyTests`, `src/StreamBus/RetryPolicy.cs:75`).
- [ ] **Step 5: Run the unit suite.**
  Run: `dotnet test tests/StreamBus.UnitTests`
  Expected: PASS (`RetryPolicyTests`, `TypedConsumeFilterTests`, `ActivityTests`, `PipelineSemanticsTests` use the `RetryPolicy` record directly and must not need changes — if one does, the constraint "low-level policy unchanged" was violated).
- [ ] **Step 6: Update `errors.mdx:43` arity mention and add the changelog entry.**
- [ ] **Step 7: Commit** `feat(retry)!: Exponential(retryCount, minDelay, maxDelay) — drop the multiplier parameter`
