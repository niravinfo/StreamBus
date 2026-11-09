# Remove error replay and requeue — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Delete every StreamBus API that reads an error stream/queue and republishes; the library's error contract becomes "dead-letter to stream or queue, nothing more."

**Architecture:** Straight removal, no replacement. Kept: normal publish (auto-id), consume with retry/exhaustion, the full dead-letter write path (`DeadLetterToStream`/`DeadLetterToQueue`, `ErrorPublisher`, `ErrorEnvelope` wire formats and headers), `SendEnvelopeAsync`, the `ConfirmationTracker` core used by all confirmed sends. Removed: `ReplayErrorStreamAsync`, `RequeueErrorsAsync`, `ConsumerRegistry.ReplayAsync`, `ErrorQueueTransport.DrainAsync`, handle-level `PublishReplayAsync`, `ConfirmationTracker.PublishWithIdAsync`, `ReplayNaming`, `ErrorEnvelope.ToReplayOptions`, `StreamBusNaming.Replay*`. Order: stream replay first, queue requeue second, shared primitives last (their only in-src callers die in the first two), then docs.

**Tech Stack:** .NET (C#), xUnit, RabbitMQ.Stream.Client; solution in `StreamBus.sln`-style layout with `src/StreamBus`, `tests/StreamBus.UnitTests`, `tests/StreamBus.IntegrationTests`.

**Spec:** `docs/superpowers/specs/2026-11-09-remove-error-replay-and-requeue-design.md`

## Global Constraints

- This is a deliberate breaking public API change; no compat shims, no `[Obsolete]` staging — consumers of the removed members must fail at compile time.
- The dead-letter **write** path must not change behavior: same envelope app-properties, same `streambus.error.*` headers, same "advance source offset only after destination confirm" semantics.
- `ConfirmationTracker` class itself, its confirmation-close behavior, `CancelAll`, and the `Confirm`-style path used by `SendAsync`/`SendEnvelopeAsync`/super sends stay untouched.
- Each task ends with a compiling solution and a green unit suite.
- Do not touch historical plans/specs under `docs/superpowers/` (they record past states).
- Changelog/release notes get a breaking-change entry naming all removed public members.

## Review Focus

1. **Dead-letter-to-stream writes break** (e.g. `ErrorEnvelope` fields or app-property mapping nicked while deleting `ToReplayOptions`) — a consumer that exhausts retries still lands the full envelope on the error stream. Pinned by existing `RetryIntegrationTests` non-replay cases; they must stay green in Task 1.
2. **Dead-letter-to-queue writes break** (e.g. `ErrorQueueTransport` control-channel/publish path nicked while deleting `DrainAsync`) — exhaustion still publishes headers-complete messages to the queue. Pinned by `ErrorQueueTests` + `ErrorQueueIntegrationTests` dead-letter case; stay green in Task 2.
3. **Confirmation-close semantics regress** (e.g. overly broad trim in `ConfirmationCloseTests` or `ProducerHandles.cs`) — a producer connection closed mid-confirm still surfaces "outcome unknown" for every kind of send. Pinned by the non-replay `ConfirmationCloseTests` cases; trim only replay cases there in Task 3.
4. **Orphaned machinery left behind** — dead naming helpers, guards, or fake methods survive. No test can pin an absence; Task 4's grep-to-zero steps are the check.
5. **Docs claim a capability the DLL no longer has** — `errors.md`/README/llms.txt still instruct users to call removed APIs. Task 4 greps include `docs/guide` and `README.md` (under `src`/`tests` excluded identifiers).

---

### Task 1: Remove stream replay end to end

**Files:**
- Modify: `src/StreamBus/ConsumerRegistry.cs` (delete `ReplayAsync`, ~lines 495–650, including the `DisposeProducersAsync` closure and inline callback, nothing around `RequireReadyAsync`)
- Modify: `src/StreamBus/Bus.cs` (delete `ReplayErrorStreamAsync`: interface lines ~123–130, `StreamBusCore` impl lines ~356–365; `StreamBus` forwards if separate)
- Modify: `src/StreamBus/ConsumeOptions.cs` (~line 77: reword the doc sentence that points at `ReplayErrorStreamAsync` to say error envelopes stay on the error stream until the user consumes them)
- Delete test cases: `tests/StreamBus.IntegrationTests/RetryIntegrationTests.cs` (`Replay_twice_stores_once`, ~lines 300–370, and any helper it alone uses)

**Interfaces:**
- Consumes: nothing new.
- Produces: `ConsumerRegistry` has no `ReplayAsync`; `IStreamBus`/`StreamBusCore` have no `ReplayErrorStreamAsync`. `ReplayNaming`, `ToReplayOptions`, `PublishReplayAsync` still exist at this point (requeue still uses them) — do **not** delete them here.

- [ ] **Step 1: Delete the test that pins replay semantics**

Delete `Replay_twice_stores_once` and replay-only helpers from `RetryIntegrationTests.cs`. Leave every other test in the file untouched.

- [ ] **Step 2: Delete the implementation**

Remove `ConsumerRegistry.ReplayAsync` (whole method incl. doc comment) and the `IStreamBus.ReplayErrorStreamAsync` declaration + `StreamBusCore`/`StreamBus` implementations from `Bus.cs`. Reword the `ConsumeOptions.cs` doc sentence.

- [ ] **Step 3: Build and run to verify nothing else referenced them**

Run: `dotnet build` then `dotnet test tests/StreamBus.UnitTests`
Expected: build succeeds (0 errors). Unit suite PASS — `ReplayReattachTests` still compiles because it tests the *handle* surface, not the bus API.

- [ ] **Step 4: Commit**

`git add -A && git commit -m "refactor: remove ReplayErrorStreamAsync stream-replay path"`

### Task 2: Remove queue requeue end to end

**Files:**
- Modify: `src/StreamBus/Bus.cs` (delete `RequeueErrorsAsync`: interface line ~98, impl ~lines 338–346)
- Modify: `src/StreamBus/ErrorQueueTransport.cs` (delete `DrainAsync`, ~lines 428–472, plus `DrainIdle`/`DrainPoll` constants and, if now unused, the `GetAsync`/`AckAsync` members on the control-channel interface/implementation — check with grep; **keep** `DeclarePassiveAsync`/`DeclareAsync`/publish-channel members: the dead-letter write path uses them)
- Delete test cases: `tests/StreamBus.UnitTests/ErrorQueueTransportTests.cs` (`DrainAsync` tests, ~line 150 context)
- Delete test cases: `tests/StreamBus.IntegrationTests/ErrorQueueIntegrationTests.cs` (the requeue case ending at `~line 183` `RequeueErrorsAsync` call — delete the case, not the file; the queue dead-letter write case stays)

**Interfaces:**
- Consumes: nothing from Task 1.
- Produces: `IStreamBus` has no `RequeueErrorsAsync`; `ErrorQueueTransport` exposes no drain surface. `ToReplayOptions` keeps existing until Task 3 (its last src caller dies here, prove with grep in Step 3).

- [ ] **Step 1: Delete the requeue tests** — remove the `DrainAsync` unit tests and the requeue integration case; keep the dead-letter-write integration case.

- [ ] **Step 2: Delete the implementation** — remove `RequeueErrorsAsync` (interface + impl) and `DrainAsync` + orphaned drain-only members as described above.

- [ ] **Step 3: Verify**

Run: `grep -rn "DrainAsync\|RequeueErrorsAsync" src tests` then `dotnet build` then `dotnet test tests/StreamBus.UnitTests`
Expected: grep returns only nothing (or Task-4-only doc matches under `docs`); build 0 errors; suite PASS.

- [ ] **Step 4: Commit**

`git commit -am "refactor: remove RequeueErrorsAsync queue-drain path"`

### Task 3: Remove the idempotent-publish primitive and naming machinery

**Files:**
- Modify: `src/StreamBus/ProducerHandles.cs` (delete `PublishReplayAsync` from `IStreamPlainProducerHandle` line ~29, `IStreamSuperProducerHandle` line ~42, and both implementing methods ~lines 239+ and ~292+; delete `ConfirmationTracker.PublishWithIdAsync` ~lines 146–175; keep `ConfirmationTracker` class, `Confirm`, `CancelAll`, close semantics)
- Modify: `src/StreamBus/ErrorEnvelope.cs` (delete `ToReplayOptions()` ~line 145 and `ReplayNaming` class ~lines 308–325)
- Modify: `src/StreamBus/StreamBusNaming.cs` (delete `ReplayProducerName` ~line 29, `ReplayReference` ~line 44)
- Delete files: `tests/StreamBus.UnitTests/ReplayNamingTests.cs`, `tests/StreamBus.UnitTests/ReplayReattachTests.cs`
- Modify: `tests/StreamBus.UnitTests/ErrorEnvelopeTests.cs` (delete `Replay_options_preserve_message_identity`, `Replay_options_preserve_user_headers_and_content_type`, `Replay_naming_is_stable_bounded_and_id_is_the_offset`)
- Modify: `tests/StreamBus.UnitTests/ConfirmationCloseTests.cs` (delete the `PublishReplayAsync` replay-case(s) ~lines 82–95 and the method's fake impls where the fakes exist only for replay; keep close-semantics cases)
- Modify: `tests/StreamBus.UnitTests/ProducerHandleSurfaceTests.cs` (delete the two `PublishReplayAsync` surface assertions, lines ~34, ~50)
- Modify: all test fakes/mocked handles — remove the `PublishReplayAsync` member from: `AcceptanceTests.cs:78`, `ActivityTests.cs:221`, `ConfirmationCloseTests.cs:40/63/82`, `PipelineSemanticsTests.cs:873`, `PublishPipelineTests.cs:1146`, `PublishTests.cs:38`, `PublishRoutingTests.cs:22`, `Fakes/ScriptedSystemHandle.cs:23`

**Interfaces:**
- Consumes: Tasks 1–2 removed the last src callers of `PublishReplayAsync` and `ToReplayOptions`.
- Produces: `IStreamPlainProducerHandle` = `SendAsync` + `SendEnvelopeAsync`; `IStreamSuperProducerHandle` = `SendAsync` (+ super-only sends). Nothing named `Replay*` remains in `src/`.

- [ ] **Step 1: Delete replay-only test files and cases** — remove the two files and the named cases; in `ConfirmationCloseTests` keep every non-replay case.

- [ ] **Step 2: Delete the production members** — interfaces, implementations, `PublishWithIdAsync`, `ToReplayOptions`, `ReplayNaming`, `StreamBusNaming.Replay*`.

- [ ] **Step 3: Strip `PublishReplayAsync` from every test fake** (list above); build.

Run: `dotnet build`
Expected: 0 errors — any remaining error names a file still referencing a deleted member; fix by deleting that member.

- [ ] **Step 4: Run the full unit suite**

Run: `dotnet test tests/StreamBus.UnitTests`
Expected: PASS.

- [ ] **Step 5: Commit**

`git add -A && git commit -m "refactor!: remove PublishReplayAsync, ReplayNaming and replay-options machinery"`

### Task 4: Docs, version note, grep-to-zero verification

**Files:**
- Modify: `docs/guide/errors.md` (delete the Replay and Requeue sections, ~lines 78, 127–165 context; replace with one paragraph stating the contract: StreamBus dead-letters to the configured destination; moving/consuming messages out of it is the user's job)
- Modify: `README.md` (drop the "Replay (streams) and requeue (queues)" feature bullet, line ~95)
- Modify: `docs/llms.txt` (remove `ReplayErrorStreamAsync`/`ReplayReference`-naming/`RequeueErrorsAsync` mentions, lines ~57, ~59, ~104)
- Modify: `docs/2026-10-13-streambus-acceptance.md` (FR-5.3 row line ~47 and queue-requeue row line ~276: do not delete the row — annotate "Removed 2026-11-09 by the error-recovery removal (see specs/2026-11-09...)" so traceability survives)
- Modify: `docs/2026-10-08-streambus-requirements.md` (FR-5.3 row line ~100: same annotation)
- Modify: changelog/release-notes file if the repo has one (grep `CHANGELOG` — if absent, note the breaking change in the acceptance doc annotation and move on).

**Interfaces:**
- Consumes: Tasks 1–3 landed all code removals.
- Produces: docs describing exactly the shipped surface.

- [ ] **Step 1: Update docs** as listed; do not edit anything under `docs/superpowers/`.

- [ ] **Step 2: Grep the identifier set to zero**

Run: `grep -rn "ReplayErrorStream\|RequeueErrors\|PublishReplayAsync\|ReplayNaming\|ToReplayOptions\|DrainAsync\|ReplayReference\|ReplayProducerName\|PublishWithIdAsync" src tests docs README.md`
Expected: only `docs/superpowers/` (historical, untouched) and the two dated "Removed" annotations; zero hits under `src/`, `tests/`, `docs/guide/`, `README.md`, `docs/llms.txt`.

- [ ] **Step 3: Full verification**

Run: `dotnet build` then `dotnet test tests/StreamBus.UnitTests` then, if the broker environment is available, `dotnet test tests/StreamBus.IntegrationTests`
Expected: build clean, unit suite PASS; dead-letter-write integration cases PASS (proof the write path survived).

- [ ] **Step 4: Commit**

`git add -A && git commit -m "docs: remove replay/requeue from user docs; mark FR-5.3 removed by breaking change"`
