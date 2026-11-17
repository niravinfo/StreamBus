# Handle / Ignore Exception Filtering Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add `Handle` / `Ignore` exception filters to the retry configurator so a schedule retries only selected exceptions and hands off everything else immediately.

**Architecture:** Filter rules are accumulated by `RetryConfigurator` into an internal `ExceptionFilterSet` carried on `RetryPolicy`. One decision method (`Decide`) runs at the existing retry/exhaust branch in `StreamPartitionConsumer.DispatchAsync`, so schedules, hand-off, and offset semantics stay untouched. Docs gain a rendered Mermaid flow diagram plus a worked Exponential example.

**Tech Stack:** C# / .NET 10, xunit v3, RabbitMQ Stream.Client, Astro Starlight 0.42 docs with `astro-mermaid`.

**Spec:** `docs/superpowers/specs/2026-11-17-exception-filtering-design.md`

## Global Constraints

- .NET 10, nullable enabled, warnings-as-errors, central package management.
- No new dependencies **in the shipped library**.
- **Naming:** no third-party library may be named in code comments, docs, tests, specs, or plans. Describe behavior generically ("the standard `Handle`/`Ignore` model", "established exception-filter semantics"). A repo-wide case-insensitive naming check must return no matches at the end of every task.
- Public API additions must be source-compatible; existing overloads unchanged.
- Filters are configured through the configurator callback. The raw `RetryPolicy` path keeps "retry all" behavior unless a filter set is attached internally.
- Matching semantics: type rules use `Type.IsInstanceOfType` and inspect the thrown exception, `GetBaseException()`, and — for `AggregateException` — each direct inner and its base exception. `Ignore` wins over `Handle`. No rules ⇒ `Retry`.
- `Ignore` / unmatched-with-`Handle` exceptions exhaust on the current attempt and go through the existing `ExhaustAsync` path.
- Filters + unbounded schedule (`MaxAttempts == null`) is a configuration error at both validation sites.
- Docs must contain the Mermaid diagram and the Exponential example from the spec verbatim; `Exponential(5, TimeSpan.FromSeconds(5), TimeSpan.FromMinutes(5))` = 6 attempts with waits 5s, 10s, 20s, 40s, 80s.
- Mermaid rendering requires `astro-mermaid` + `mermaid` in `docs/package.json` and `mermaid()` registered **before** `starlight()` in `docs/astro.config.mjs` (Starlight 0.42 has no built-in flag).

## Review Focus

- **Derived exception type** (`class SpecialSqlException : SqlException`) — one `Handle<SqlException>()` rule must still retry it: pinned by `Type_rule_matches_derived_exception` (Task 1).
- **`AggregateException` wrapping an ignored poison exception** — must be classified as ignored, not burn retries: pinned by `Type_rule_matches_aggregate_inner_exception` and `Ignore_wins_when_both_match` (Task 1).
- **Call order** — `Handle`/`Ignore` calls before or after the schedule method must all apply; last-schedule-wins must not drop filters: pinned by `Filter_rules_survive_schedule_overwrites` (Task 2).
- **Scope override** — endpoint `UseRetry` with no filters must replace, not inherit, bus-wide filters: pinned by `Endpoint_use_retry_replaces_global_filters` (Task 2).
- **Filters + unbounded schedule** — must fail at registration and never reach a hand-off with a null destination: pinned by `Filters_with_unbounded_schedule_throw` (Task 3) and the runtime defensive guard tested by `Ignored_exception_hands_off_after_one_attempt` plus the validation test (Tasks 3–4).

---

### Task 1: Exception filter model and matching

**Files:**
- Create: `src/StreamBus/ExceptionFilterSet.cs`
- Modify: `src/StreamBus/RetryPolicy.cs` (add `Filters` property beside `DelayCalculator`)
- Test: `tests/StreamBus.UnitTests/ExceptionFilterSetTests.cs` (new)

**Interfaces:**
- Consumes: nothing new.
- Produces:
  - `internal enum RetryDecision { Retry, Ignored, Unhandled }`
  - `internal sealed class ExceptionFilterSet` with `bool HasRules { get; }`, `void AddInclude(params Type[] types)`, `void AddInclude<T>(Func<T, bool> predicate) where T : Exception`, `void AddInclude(Func<Exception, bool> predicate)`, matching `AddExclude` overloads, and `RetryDecision Decide(Exception ex)`.
  - `internal ExceptionFilterSet? Filters { get; init; }` on `RetryPolicy` (null = retry everything).

- [ ] **Step 1: Write the failing tests** in `tests/StreamBus.UnitTests/ExceptionFilterSetTests.cs` (namespace `StreamBus.UnitTests`, which has internals access via `AssemblyInfo.cs`). Define local exception types (`DerivedInvalidOperationException : InvalidOperationException`, `OtherException : Exception`). Assert:
  - `Empty_set_decides_Retry`: new set, `Decide(new Exception()) == Retry`, `HasRules == false`.
  - `Type_rule_matches_derived_exception`: `AddInclude(typeof(InvalidOperationException))`; `Decide(new DerivedInvalidOperationException()) == Retry`.
  - `Type_rule_matches_base_exception_in_chain`: outer exception with `new InvalidOperationException("inner")` as `InnerException`; handling `InvalidOperationException` decides `Retry`.
  - `Type_rule_matches_aggregate_inner_exception`: `new AggregateException(new InvalidOperationException())`; handling `InvalidOperationException` decides `Retry`.
  - `Predicate_rule_matches_typed_exception`: `AddExclude<ArgumentException>(e => e.ParamName == "orderTotal")`; matching param ⇒ `Ignored`, different param ⇒ `Retry`.
  - `Handle_only_unmatched_decides_Unhandled`: `AddInclude(typeof(InvalidOperationException))`; `Decide(new Exception()) == Unhandled`.
  - `Ignore_rule_decides_Ignored`: `AddExclude(typeof(ArgumentException))`; `Decide(new ArgumentException()) == Ignored`.
  - `Ignore_wins_when_both_match`: include and exclude the same type ⇒ `Ignored`.
  - `Multiple_rules_accumulate`: two includes plus one exclude, each matched in turn.

- [ ] **Step 2: Run the tests to verify they fail**

Run: `dotnet test tests/StreamBus.UnitTests/StreamBus.UnitTests.csproj --filter "FullyQualifiedName~ExceptionFilterSetTests"`
Expected: FAIL — `ExceptionFilterSet` and `RetryDecision` do not exist.

- [ ] **Step 3: Implement the model**

Create `src/StreamBus/ExceptionFilterSet.cs` with the exact signatures above. Build the candidate sequence as the thrown exception, its `GetBaseException()`, and — when that base is `AggregateException` — each direct inner exception and each inner's `GetBaseException()`; de-duplicate if convenient. A type rule matches when any candidate `IsInstanceOfType` against any configured type; a typed predicate rule matches when any candidate is assignable to `T` and the predicate returns true. Decide in this order: any exclude match ⇒ `Ignored`; else any include rule configured and no include match ⇒ `Unhandled`; else `Retry`. Add the internal init-only `Filters` property to `RetryPolicy`.

- [ ] **Step 4: Run the tests to verify they pass**

Run: `dotnet test tests/StreamBus.UnitTests/StreamBus.UnitTests.csproj --filter "FullyQualifiedName~ExceptionFilterSetTests"`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add src/StreamBus/ExceptionFilterSet.cs src/StreamBus/RetryPolicy.cs tests/StreamBus.UnitTests/ExceptionFilterSetTests.cs
git commit -m "feat: add exception filter model and retry decision"
```

---

### Task 2: Configurator API

**Files:**
- Modify: `src/StreamBus/Configuration/RetryConfiguration.cs` (interfaces at ~136–174, implementation ~176–244)
- Modify: `tests/StreamBus.UnitTests/RetryApiSurfaceTests.cs`
- Test: `tests/StreamBus.UnitTests/ExceptionFilterConfigurationTests.cs` (new)

**Interfaces:**
- Consumes: `ExceptionFilterSet` and its `Add*` methods from Task 1.
- Produces:
  - `public interface IExceptionConfigurator` with exactly: `Handle(params Type[])`, `Handle<T>()`, `Handle<T1,T2>()`, `Handle<T1,T2,T3>()`, `Handle(Func<Exception,bool>)`, `Handle<T>(Func<T,bool>)`, plus the six matching `Ignore` overloads. All generic constraints are `where T : Exception`.
  - `public interface IRetryConfigurator : IExceptionConfigurator` — existing schedule methods unchanged.
  - `RetryConfigurator` implements all twelve methods; `IRetryConfigurator.BuildPolicy()` returns `_pending` with the accumulated filter set attached (`policy with { Filters = ... }`, null when no rules).

- [ ] **Step 1: Write the failing tests**

In `RetryApiSurfaceTests.cs`, assert by reflection that `IRetryConfigurator.GetMethods()` contains all twelve named overloads with the expected parameter shapes (including the generic arities). In the new test file, add:
  - `Filter_rules_survive_schedule_overwrites`: configure `Handle<InvalidOperationException>()`, then `Immediate(3)`, then `Ignore<ArgumentException>()`; `BuildPolicy().Filters.Decide(...)` returns `Retry` for the first type and `Ignored` for the second.
  - `Filters_called_before_schedule_are_kept`: call `Ignore<T>()` first, then `Immediate(1)`.
  - `Endpoint_use_retry_replaces_global_filters`: using the `EndpointOptions` helper pattern from `RetryConfigurationTests`, set a global `Handle<T>()` and an endpoint `Immediate(2)` with no filters; the resulting `ConsumeOptions.Retry.Filters` has no rules.
  - `Argument_validation`: null `params Type[]`, empty type array, a non-`Exception` type, a null element, and a null predicate each throw (`ArgumentNullException` or `StreamBusConfigurationException` per the spec).

- [ ] **Step 2: Run the tests to verify they fail**

Run: `dotnet test tests/StreamBus.UnitTests/StreamBus.UnitTests.csproj --filter "FullyQualifiedName~RetryApiSurfaceTests|FullyQualifiedName~ExceptionFilterConfigurationTests"`
Expected: FAIL — methods do not exist on `IRetryConfigurator`.

- [ ] **Step 3: Implement the configurator API**

Add `IExceptionConfigurator` and make `IRetryConfigurator` inherit it. In `RetryConfigurator`, keep `_includes` / `_excludes` as a single `ExceptionFilterSet` field independent of `_pending`, route all overloads into it (multi-generic overloads delegate to `params Type[]`), apply the spec's argument validation, and attach the set in `BuildPolicy()`.

- [ ] **Step 4: Run the tests to verify they pass**

Run: `dotnet test tests/StreamBus.UnitTests/StreamBus.UnitTests.csproj --filter "FullyQualifiedName~RetryApiSurfaceTests|FullyQualifiedName~ExceptionFilterConfigurationTests"`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add src/StreamBus/Configuration/RetryConfiguration.cs tests/StreamBus.UnitTests/RetryApiSurfaceTests.cs tests/StreamBus.UnitTests/ExceptionFilterConfigurationTests.cs
git commit -m "feat: expose Handle and Ignore on the retry configurator"
```

---

### Task 3: Bounded-schedule validation

**Files:**
- Modify: `src/StreamBus/Configuration/RetryConfiguration.cs` (add `RetryFilterValidation`, call from `RetryDerivation.Derive` ~263–290)
- Modify: `src/StreamBus/ConsumerRegistry.cs` (call from `ConsumeCoreAsync` after line 307)
- Test: `tests/StreamBus.UnitTests/RetryFilterValidationTests.cs` (new)

**Interfaces:**
- Consumes: `RetryPolicy.Filters`, `ExceptionFilterSet.HasRules`.
- Produces: `internal static class RetryFilterValidation { public static void Validate(RetryPolicy policy, string context); }` — throws `StreamBusConfigurationException` when `Filters?.HasRules == true && MaxAttempts is null`; message includes `context` and tells the caller to set a bounded schedule.

- [ ] **Step 1: Write the failing tests**

  - `Filters_with_unbounded_schedule_throw`: call `RetryFilterValidation.Validate(new RetryPolicy { Filters = rules }, "test context")` with one rule; assert `StreamBusConfigurationException` and that the message contains `"test context"` and a bounded-schedule hint.
  - `No_filters_with_unbounded_schedule_allowed` and `Filters_with_bounded_schedule_allowed`: no throw.
  - `Derive_rejects_filters_with_unbounded_schedule`: call `RetryDerivation.Derive(new RetryPolicy { Filters = rules }, null, null, false, "orders", "bus")` directly and assert `StreamBusConfigurationException` whose message contains the bus/endpoint context.

- [ ] **Step 2: Run the tests to verify they fail**

Run: `dotnet test tests/StreamBus.UnitTests/StreamBus.UnitTests.csproj --filter "FullyQualifiedName~RetryFilterValidationTests"`
Expected: FAIL — `RetryFilterValidation` does not exist.

- [ ] **Step 3: Implement and wire both validation sites**

Add `RetryFilterValidation` in `RetryConfiguration.cs`. Call it from `RetryDerivation.Derive` after `effective` is chosen (context `$"Bus {busName}: endpoint '{stream}'"`) and from `ConsumeCoreAsync` immediately after the existing `MaxAttempts < 1` check (context `"ConsumeOptions"`).

- [ ] **Step 4: Run the tests to verify they pass**

Run: `dotnet test tests/StreamBus.UnitTests/StreamBus.UnitTests.csproj --filter "FullyQualifiedName~RetryFilterValidationTests|FullyQualifiedName~RetryConfigurationTests"`
Expected: PASS, including the existing derivation tests.

- [ ] **Step 5: Commit**

```bash
git add src/StreamBus/Configuration/RetryConfiguration.cs src/StreamBus/ConsumerRegistry.cs tests/StreamBus.UnitTests/RetryFilterValidationTests.cs
git commit -m "feat: require a bounded schedule when exception filters are configured"
```

---

### Task 4: Runtime retry decision

**Files:**
- Modify: `src/StreamBus/StreamPartitionConsumer.cs` (retry loop ~634–663)
- Test: `tests/StreamBus.IntegrationTests/RetryIntegrationTests.cs`

**Interfaces:**
- Consumes: `RetryPolicy.Filters` and `ExceptionFilterSet.Decide` → `RetryDecision` from Tasks 1–3.
- Produces: no new public API; changed runtime behavior.

- [ ] **Step 1: Write the failing integration tests**

Add a helper `RunFilterCaseAsync(Action<ExceptionFilterSet> configureFilters, int maxAttempts, int expectedAttempts)` modeled on `RunSkipCaseAsync` (same stream/error-stream setup, publish five messages, throw `InvalidOperationException` for sequence 2, consume with an internal `RetryPolicy { MaxAttempts = maxAttempts, Filters = rules }` — internals are visible to this test assembly). Assert the poison message's attempt count, that the other four messages were consumed once, that exactly one error envelope exists, and that the offset is 4. Add four `[Fact]`s:
  - `Ignored_exception_hands_off_after_one_attempt` — exclude `InvalidOperationException`.
  - `Unhandled_exception_hands_off_after_one_attempt` — include a different exception type.
  - `Handled_exception_uses_configured_schedule` — include `InvalidOperationException` with `MaxAttempts = 3`; expect 3 attempts.
  - `Predicate_ignore_hands_off_after_one_attempt` — typed predicate on `ArgumentException.ParamName`.

- [ ] **Step 2: Run the tests to verify they fail**

Run: `dotnet test tests/StreamBus.IntegrationTests/StreamBus.IntegrationTests.csproj --filter "FullyQualifiedName~RetryIntegrationTests"` (requires the Testcontainers broker).
Expected: the four new tests fail because filters are not yet consulted.

- [ ] **Step 3: Implement the decision**

In `DispatchAsync`, compute `var decision = _options.Retry.Decide(ex)` and `exhausted = decision != RetryDecision.Retry || (!unbounded && attempts >= MaxAttempts.Value)`. Log `ignored`/`unhandled` with the decision reason for non-Retry cases, keep the existing warning/debug sampling for Retry cases. Before calling `ExhaustAsync`, guard the no-hand-off case exactly as the deserialization path does (`OnExhausted`/`DeadLetter`/`ErrorStream` all null ⇒ warning + `FailAttach`), so an invalid internal state can never NRE or advance the offset.

- [ ] **Step 4: Run the tests to verify they pass**

Run: `dotnet test tests/StreamBus.IntegrationTests/StreamBus.IntegrationTests.csproj --filter "FullyQualifiedName~RetryIntegrationTests"`
Expected: PASS, including existing retry tests (no filters ⇒ unchanged behavior).

- [ ] **Step 5: Commit**

```bash
git add src/StreamBus/StreamPartitionConsumer.cs tests/StreamBus.IntegrationTests/RetryIntegrationTests.cs
git commit -m "feat: apply exception filters to the retry decision"
```

---

### Task 5: Documentation and Mermaid rendering

**Files:**
- Modify: `docs/package.json`, `docs/astro.config.mjs`
- Modify: `docs/src/content/docs/concepts/exceptions.mdx`, `docs/src/content/docs/guides/handle-failures.mdx`, `docs/src/content/docs/configuration/retry-and-dead-letters.mdx`

**Interfaces:**
- Consumes: the public `Handle` / `Ignore` signatures from Task 2 and the semantics from Tasks 1–4.
- Produces: no code API; rendered documentation.

- [ ] **Step 1: Enable Mermaid rendering**

Add `astro-mermaid` and `mermaid` to `docs/package.json` dependencies; import and register `mermaid()` as the first entry of `integrations`, before `starlight()`, in `docs/astro.config.mjs`. Run `pnpm --dir docs build` and expect a clean build.

- [ ] **Step 2: Add the diagram and example**

In `concepts/exceptions.mdx`, replace the "Planned: exception filtering" note with the Mermaid diagram and the C# example copied **verbatim** from the spec's "Documentation content (required, verbatim)" section, plus prose covering the four outcomes and the 5s→10s→20s→40s→80s delays. In `handle-failures.mdx`, replace the roadmap note with a link to that diagram and the same example. In `retry-and-dead-letters.mdx`, add the API overload table, include/exclude precedence, derived/aggregate matching, and the bounded-schedule constraint.

- [ ] **Step 3: Verify**

Run: `pnpm --dir docs build`
Expected: clean build, and the diagram renders as an SVG (not a raw code fence) when previewed. Run the repo-wide case-insensitive naming check from Global Constraints; expect no matches.

- [ ] **Step 4: Commit**

```bash
git add docs/package.json docs/astro.config.mjs docs/src/content/docs/concepts/exceptions.mdx docs/src/content/docs/guides/handle-failures.mdx docs/src/content/docs/configuration/retry-and-dead-letters.mdx
git commit -m "docs: document exception filtering with a flow diagram"
```
