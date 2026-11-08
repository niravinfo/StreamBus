# Remove Effectively-Once Pipeline Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Delete the effectively-once (EO) read-process-write pipeline and every result-publish API from StreamBus; deduplication/at-exactly-once semantics become the application's responsibility, as of 0.2.0 (pre-release, no external customers).

**Architecture:** Straight removal, no replacement. The library keeps: normal/super publish (auto counter IDs, at-least-once), normal consume with retry/exhaustion, dead-letter error streams, and the **replay** path (replay republished messages still carry stable publishing IDs via the plain handle's retained `PublishReplayAsync` — that is part of error handling, not EO, and stays). Everything whose only purpose is "result of source offset N appears once in target" goes: the managed EO consume, manual result publish, the result-producer registry, and the `requiresContinuation` pipeline mode.

**Tech Stack:** .NET 10, xUnit, RabbitMQ stream client.

**Spec:** Product decision recorded in this plan (session 2026-11-07): "let's drop it all EO pipeline related code, there shouldn't be any code related to exactly once message publish or consume, its user responsibility to manage that as of current version. currently there are not customer using this library so we are okay to do any breaking changes."

## Global Constraints

- Unit suite (`dotnet test tests\StreamBus.UnitTests`) must be 100% green; solution builds with 0 warnings (`dotnet build StreamBus.slnx`); warnings are errors in Release builds.
- The brokered integration suite (`tests\StreamBus.IntegrationTests`) cannot run locally (no RabbitMQ broker); it must compile clean after every task.
- `streambus.replay` dead-letter machinery stays intact end to end: `IStreamPlainProducerHandle.PublishReplayAsync`, replay naming, and the internal `ConfirmationTracker.PublishWithIdAsync` are **in scope to keep**.
- CHANGELOG records every removed public type/member under `### Breaking` in release 0.2.0.
- Historical docs (`docs/superpowers/plans/*`, `docs/adr/*`, `docs/2026-10-*` acceptance/design records) are history and are NOT rewritten; user-facing guides (`docs/guide/*`, `docs/llms.txt`, README if it mentions EO) are updated.

## Review Focus

Failure modes the removal could break that no deletion step obviously covers:

1. **Replay accidentally broken** — the biggest risk. Any deletion that takes `PublishReplayAsync`/replay naming/`ConfirmationTracker.PublishWithIdAsync` with it breaks dead-letter replay. Pin: Task 4 keeps `ReplayAndEffectivelyOnceReattachTests`' replay-only tests green (or moved-then-green in Task 3) and `ConfirmationCloseTests` stays untouched in Task 3.
2. **Consume pipeline semantics silently changed** — after `requiresContinuation` is deleted, double-`next` on a (now only) normal consume pipeline must still throw (as `StreamBusTransientException`) and zero-`next` must still mean "skip delivery". Pin: `PipelineSemanticsTests` double-`next` and zero-`next` tests stay green after Task 4.
3. **Dangling DI wiring** — `StreamBusRegistry` constructs `StreamConsumerRegistry` with `ResultProducersFor(vhost)`; removing `ResultProducerRegistry` requires unwiring both or construction breaks. Pin: an existing registry-construction/hosted test (e.g. in `HostingTests` or `ConfiguratorTests`) stays green in Task 3.
4. **Dangling doc claims** — guides tell users to call deleted APIs. Pin: grep gate in Task 6 (`EffectivelyOnce|ConsumeEffectivelyOnce|PublishResultAsync|PublishResultOptions|effectively-once` over `src`, `examples`, user-facing `docs/guide`, `docs/llms.txt`, README returns zero hits outside CHANGELOG/plan/review files).
5. **Surface-test vacuity** — `ProducerHandleSurfaceTests` pins presence of members; if not flipped before deletion, the deleted-member assertions fail with compile errors instead of clean red. Pin: Task 1 flips the assertions first and watches them fail at runtime, not compile time.

---

### Task 1: Flip surface-pinning tests RED against the EO publish API

**Files:**
- Modify: `tests\StreamBus.UnitTests\ProducerHandleSurfaceTests.cs`
- Modify: `tests\StreamBus.UnitTests\BusSurfaceTests.cs` (or wherever `IStreamBus`/`IConsumer<TBus>` surface is pinned by reflection — locate with `grep -n "ConsumeEffectivelyOnceAsync" tests`; if no such surface test exists, add the absence assert to `ProducerHandleSurfaceTests` + a new `[Fact]` in `EffectivelyOnceTests` that asserts `typeof(IStreamBus).GetMethod("ConsumeEffectivelyOnceAsync") is null`)

**Interfaces:**
- Produces: nothing for later tasks; this task only proves the tests detect the current (wrong) state.

- [ ] **Step 1: Invert the assertions**

In `ProducerHandleSurfaceTests`, change the two `Assert.NotNull(...PublishWithIdAsync...)` / `(...PublishResultAsync...)` lines to assert absence:

```csharp
Assert.Null(Method(typeof(IStreamSuperProducerHandle), "PublishWithIdAsync"));
Assert.Null(Method(typeof(IStreamSuperProducerHandle), "PublishResultAsync"));
```

and remove them from the "keeps" comment/assert list. Same for the plain handle's `PublishResultAsync` if asserted. Add absence asserts for `IStreamBus.ConsumeEffectivelyOnceAsync` and `IStreamBus.PublishResultAsync` (or `IConsumer<TBus>` — use the interface that declares them at `Bus.cs:48`/`Bus.cs:34`; check which type declares and reflect over that).

- [ ] **Step 2: Watch them fail**

Run: `dotnet test tests\StreamBus.UnitTests --filter "FullyQualifiedName~ProducerHandleSurfaceTests" --nologo -v q`
Expected: FAIL on the absence asserts (members still exist).

- [ ] **Step 3: Commit**

`git commit -m "test: pin removal of effectively-once publish/consume surface"`

### Task 2: Remove the producer-handle result members

**Files:**
- Modify: `src\StreamBus\ProducerHandles.cs` — delete `PublishWithIdAsync` and `PublishResultAsync` from `IStreamSuperProducerHandle` (lines ~37–38) and `PublishResultAsync` from `IStreamPlainProducerHandle` (line ~38), plus the implementing methods on `SuperStreamProducerHandle` (~272–290) and the plain handle. **Keep** `PublishReplayAsync` on both, `SendEnvelopeAsync` on plain, and the internal `ConfirmationTracker.PublishWithIdAsync(IProducer, ...)` (replay uses it).
- Modify: `src\StreamBus\ResultProducerRegistry.cs` — it calls `super.PublishWithIdAsync` (line ~58) and `plain.PublishReplayAsync`; leave for Task 3 wholesale deletion; do not delete the file here.
- Modify test stubs that implement the removed members: `PublishTests.cs:40–42`, `PublishRoutingTests.cs:24–26`, `AcceptanceTests.cs:80–82`, `ActivityTests.cs:244–245`, `PublishPipelineTests.cs:1106–1109`, `EffectivelyOnceTests.cs:138–143`, `ResultPublisherTests.cs:16–17` (delete the interface stubs; `ResultPublisherTests` itself dies in Task 3 — here only keep it compiling by deleting the stub methods if the members are gone).

**Interfaces:**
- Consumes: Task 1's red asserts.
- Produces: `IStreamSuperProducerHandle` / `IStreamPlainProducerHandle` without the result-with-ID members.

- [ ] **Step 1: Delete the interface members and implementations** (leave `ConfirmationTracker` intact).
- [ ] **Step 2: Delete the same stubs from every test-double implementing the handles** (grep `PublishWithIdAsync|PublishResultAsync` under `tests` after the edit → only `ConfirmationTracker`-internal uses in `ConfirmationCloseTests`/`EffectivelyOnceTests:230/233` and `ConsumerRegistry` replay may remain at this point).
- [ ] **Step 3: Watch Task 1's tests go green + unit build**

Run: `dotnet test tests\StreamBus.UnitTests --filter "FullyQualifiedName~ProducerHandleSurfaceTests" --nologo -v q`
Expected: PASS; full `dotnet build tests\StreamBus.UnitTests` clean (ExpectivelyOnceTests may still reference `ResultPublisher` — it compiles until Task 3; if a stub deletion there breaks compilation, delete only the stub methods).
- [ ] **Step 4: Commit** `git commit -m "refactor: remove PublishWithIdAsync/PublishResultAsync from producer handles (EO removal 1/4)"`

### Task 3: Delete the result-publish machinery and EO bus API

**Files:**
- Delete: `src\StreamBus\EffectivelyOnce.cs` (all of it: `EffectivelyOnceOptions`, `PublishResultOptions`, `EffectivelyOnceConfig`, `ResultNaming`, `EffectivelyOnceValidation`).
- Delete: `src\StreamBus\ResultProducerRegistry.cs`, `src\StreamBus\ResultPublisher.cs`.
- Modify: `src\StreamBus\Bus.cs` — delete `ConsumeEffectivelyOnceAsync` and `PublishResultAsync` from the interface (lines ~26–48) and implementations (~246–249).
- Modify: `src\StreamBus\ConsumerRegistry.cs` — delete `ConsumeEffectivelyOnceAsync` (396–490), `PublishResultAsync` (673–700+), the `_results` field/ctor param (111, 126), and the `requiresContinuation: effectivelyOnce` argument site (line 174 keeps `new ConsumePipeline(filters)`).
- Modify: `src\StreamBus\ConsumeOptions.cs:54` — delete `internal EffectivelyOnceConfig? EffectivelyOnce`.
- Modify: `src\StreamBus\Registry.cs` — delete `_results` dict and `ResultProducersFor` (101–107), unwrap the `StreamConsumerRegistry` construction args.
- Modify: `src\StreamBus\ConsumerHealthNames.cs:16–17` — delete `EffectivelyOnce(...)`.
- Modify: `src\StreamBus\StreamBusNaming.cs:51–62` — delete `EffectivelyOnceProducerName` and `EffectivelyOnceConsumerReference`.
- Delete tests: `tests\StreamBus.UnitTests\ResultPublisherTests.cs`; EO-only tests inside `EffectivelyOnceTests.cs` (validation tests 58–103, registry tests ~213–233). **Move** the still-valid in-flight duplicate-ID test (`EffectivelyOnceTests.cs:~230`, `tracker.PublishWithIdAsync(…5…)` twice → `StreamBusPublishException`) into `ConfirmationCloseTests.cs` under the name `Duplicate_publishing_id_in_flight_is_rejected` — it pins the replay-path guard.
- Modify: `tests\StreamBus.UnitTests\HealthAggregatorTests.cs` (~209–215) — replace the EO consumer registration with a normal `ConsumeAsync`-style registration (mirror neighboring registrations in the same file).
- Modify: `tests\StreamBus.UnitTests\ReplayAndEffectivelyOnceReattachTests.cs` — rename file/class to `ReplayReattachTests` via `git mv`; delete the EO reattach scenarios; keep/re-point replay-only tests.
- Modify: `src\StreamBus\Exceptions.cs`, `src\StreamBus\ConsumeContext.cs:40`, `src\StreamBus\Bus.cs` XML docs that reference the EO path — scrub mentions.

**Interfaces:**
- Consumes: handle surface from Task 2.
- Produces: `StreamConsumerRegistry` ctor WITHOUT the `ResultProducerRegistry` parameter; `ConsumePipeline(filters)` default `requiresContinuation = false` (parameter removed in Task 4 — until then the default suffices).

TDD: this task is deletion-driven; the pinning tests are Tasks 1 and the moved duplicate-ID test. Watch the moved test pass in `ConfirmationCloseTests`.

- [ ] **Step 1: Add the moved test to `ConfirmationCloseTests.cs`** — copy the in-flight duplicate-ID test body verbatim; run it against unchanged `ConfirmationTracker`: PASS (it pins behavior that must survive).
- [ ] **Step 2: Delete `EffectivelyOnce.cs`, `ResultProducerRegistry.cs`, `ResultPublisher.cs`** and all the modify-site edits above.
- [ ] **Step 3: Fix every compile break the deletions expose** (grep `Effectively|ResultPublisher|ResultProducer|ResultNaming|PublishResultOptions` under `src` and `tests` — drive to zero except replay's `ReplayNaming` and `ErrorEnvelope`).
- [ ] **Step 4: Run unit suite** `dotnet test tests\StreamBus.UnitTests --nologo -v q` → green. `dotnet build StreamBus.slnx` → 0 warnings (integration project must compile).
- [ ] **Step 5: Commit** `git commit -m "refactor!: remove effectively-once consume/result-publish machinery (EO removal 2/4)"`

### Task 4: Strip the EO dispatch path and pipeline mode

**Files:**
- Modify: `src\StreamBus\StreamPartitionConsumer.cs` — delete `_eo` (145), `_results` (146), ctor params `results`/`effectivelyOnce` and both guard throws (182–207), the EO dispatch branch (555–561), `DispatchEffectivelyOnceAsync` (809–911), the `_eo is null &&` conditional at 346 (keep the interval logic unconditionally), and the `ResumeFor` EO arm (1166–1168 — collapse to just the stored-offset branch).
- Modify: `src\StreamBus\ConsumePipeline.cs` — delete `_requiresContinuation` (219, 223, 227, 270–272, 283–287, 295–297): double-`next` always throws `StreamBusTransientException` with the existing message; zero-`next` always `return`s (skip). Also delete the `requiresContinuation` ctor parameter.
- Modify: `src\StreamBus\Exceptions.cs` doc comment for `StreamBusFilterContractException` — now publish-side only.
- Modify: `src\StreamBus\Bus.cs` interface XML docs — remove the EO/publishResult remarks (~26–48).
- Modify tests: `PipelineSemanticsTests.cs` (~128) and `TypedConsumeFilterTests.cs` (~721) — delete `requiresContinuation: true` usages and the EO-contract tests built on them (e.g. zero-`next`-throws tests); ensure a plain-consume double-`next`-throws and zero-`next`-skips test remains green (they exist elsewhere in the file).
- Note: `StreamBusFilterContractException` stays public (used by `PublishPipeline`); only the consume-side throws collapse to `StreamBusTransientException`.

**Interfaces:**
- Consumes: `ConsumePipeline(filters)` single-arg ctor from Task 3.
- Produces: `StreamPartitionConsumer` ctor signature without `results`/`effectivelyOnce`; `ConsumePipeline` without the mode flag.

- [ ] **Step 1: Before touching `ConsumePipeline`, add/confirm a pinning test**: normal consume double-`next` → throws `StreamBusTransientException`; zero-`next` → handler skipped, no throw. (Present in `PipelineSemanticsTests`; if the double-`next` assert targets a `requiresContinuation: true` pipeline, re-point it at a default pipeline, run, watch it PASS — behavior unchanged on the default mode.)
- [ ] **Step 2: Apply the deletions above.**
- [ ] **Step 3: Unit suite green + solution clean build** (as Task 3 Step 4).
- [ ] **Step 4: Commit** `git commit -m "refactor!: remove EO dispatch path and requiresContinuation pipeline mode (EO removal 3/4)"`

### Task 5: Integration tests

**Files:**
- Delete: `tests\StreamBus.IntegrationTests\EffectivelyOnceIntegrationTests.cs`.
- Modify: `tests\StreamBus.IntegrationTests\SuperStreamRoutingIntegrationTests.cs:~110–135` — the test step consumes via `ConsumeEffectivelyOnceAsync`; re-point it at the file's existing normal-publish/routed-consume helpers (read the neighboring test steps; the test asserts routed output lands in the right partition — publish routed results with the ordinary `PublishAsync`/super publish instead of the EO step; keep the assertion intent). If the step cannot be expressed without EO, delete the whole test with a note in the commit message.
- Grep `tests\StreamBus.IntegrationTests` for `PublishResultAsync|EffectivelyOnceOptions|ConsumeEffectivelyOnceAsync` → zero.

**Interfaces:** none new.

- [ ] **Step 1: Apply edits; `dotnet build tests\StreamBus.IntegrationTests` clean.**
- [ ] **Step 2: Commit** `git commit -m "test: remove EO integration lane, re-point routed super-stream test (EO removal 4/4)"`

### Task 6: Docs, CHANGELOG, review-file notes

**Files:**
- Delete: `docs\guide\effectively-once.md`.
- Modify: `docs\guide\consume.md` — delete the "Effectively-once" section (~65–74) and scrub EO mentions (lines 45, 110–113 area: "an effectively-once pipeline must call next() exactly once" becomes just the normal zero/one rule; table line ~127 "effectively-once result" wording).
- Modify: `docs\guide\publish.md` — scrub any `PublishResultAsync`/manual-result-publish section if present.
- Modify: `docs\guide\observability.md` (~15, ~46), `docs\guide\hosting.cs`-adjacent `docs\guide\hosting.md` (~73), `docs\guide\errors.md` (~181 "effectively-once stream" row → remove): delete EO reference mentions.
- Modify: `docs\llms.txt` — remove EO statements; state plainly: "StreamBus is at-least-once; deduplication is the application's responsibility."
- Modify: `CHANGELOG.md` 0.2.0 `### Breaking` — one entry listing every removed public symbol: `ConsumeEffectivelyOnceAsync`, `EffectivelyOnceOptions`, `PublishResultAsync`, `PublishResultOptions`, `IStreamSuperProducerHandle.PublishWithIdAsync`/`PublishResultAsync`, `IStreamPlainProducerHandle.PublishResultAsync`, `StreamBusFilterContractException` consume-side use. One-sentence rationale: "Effectively-once semantics (publishing-id dedup read-process-write) are removed; at-least-once + user-side idempotency is the model."
- Modify: `README.md` only if it names EO.

**Interfaces:** none.

- [ ] **Step 1: Apply doc edits.**
- [ ] **Step 2: Grep gate** — zero hits for `EffectivelyOnce|ConsumeEffectivelyOnce|PublishResultAsync|PublishResultOptions|effectively-once` across `src`, `tests`, `examples`, `docs\guide`, `docs\llms.txt`, `README.md` (CHANGELOG, this plan, `docs\reviews`, and `docs\superpowers` are exempt).
- [ ] **Step 3: Full verification** — `dotnet test tests\StreamBus.UnitTests --nologo -v q` (all green), `dotnet build StreamBus.slnx` (0 warnings).
- [ ] **Step 4: Commit** `git commit -m "docs: remove effectively-once documentation; record breaking change (EO removal)"`

---

## Self-review notes

- Replay safety is pinned in Task 3 (moved duplicate-ID test) and Task 4 gr-keeping; task boundaries split compile-fixing between 2/3/4 — executor should expect compile breaks at each boundary carrying only forward (Task 2 leaves `ResultProducerRegistry` compile-broken only if interface stubs are gone — it's mitigated because its members stay implemented until Task 3 deletes the whole file; if compilation proves otherwise, fold Task 2's registry-adjacent edits into Task 3 during execution and note it in the commit).
- Skipping `PublishWithIdAsync` deletion order overlap is deliberate: `ResultProducerRegistry` (the only non-replay caller) is deleted in Task 3 — no dangling method remains after Task 3.
- With `requiresContinuation` gone, all consume pipelines use `StreamBusTransientException` for double-`next`; consistent with pre-EO-documented semantics.
- Effectively-once retry-behavior differences in `HostedService`/supervisor none found — `grep EffectivelyOnce src\StreamBus\Hosting.cs` returned no hits during surveying.
