# StreamBus Phase 10 (v1 Acceptance) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Close v1 with a committed acceptance record: a traceability matrix mapping every FR-1–FR-10 requirement to its pinning tests, recorded NFR verification evidence, a gate roll-up, plus the few missing gap tests.

**Architecture:** This phase writes almost no library code — it audits. Task 1 (subagent, very thorough) reads the requirements/design against every test body and commits an acceptance doc with per-FR verdicts; Task 2 adds the two confirmed gap tests (FR-8.2 two-streams-one-bus, FR-4.5 hook observe-only); Task 3 records NFR evidence (package list, source greps, doc pointers), rolls up the gates, and re-greens the full suite as the final v1 evidence. Pre-identified gaps come from the controller's own audit; Task 1 confirms or extends them.

**Tech Stack:** .NET 10 (`net10.0`), C# latest, `RabbitMQ.Stream.Client` 1.12.1 (same pins — no new packages), xUnit v3, `StreamBus.slnx`, Central Package Management.

**Spec:** `docs/2026-10-08-streambus-design.md` (all sections, §10 row 10, §13 metrics catalog) and `docs/2026-10-08-streambus-requirements.md` (FR-1–FR-10, NFR-1–NFR-7, §5 gates). v1 is done when all FR pass against the real broker and gates G-1–G-5 pass — with G-3 recorded open (no 3-node cluster).

## Global Constraints

- `TargetFramework` exactly `net10.0`; `Nullable`/`ImplicitUsings` enable; `TreatWarningsAsErrors` true; `ManagePackageVersionsCentrally` true; `IsTestingPlatformApplication` false (all in `Directory.Build.props`, unchanged).
- `RabbitMQ.Stream.Client` exactly `1.12.1`; `Microsoft.Extensions.Hosting` / `Diagnostics.HealthChecks` / `Logging.Abstractions` exactly `10.0.12`; `xunit.v3` 4.0.2, `xunit.runner.visualstudio` 4.0.1, `Microsoft.NET.Test.Sdk` 18.10.1. No new packages.
- Library root namespace `StreamBus`; never `RabbitMQ.Stream.*`.
- No library behavior changes in this phase unless a gap test exposes a real bug (then: rule first in the ledger, fix minimally, re-verify — the fix loop handles it; no silent scope growth).
- Gap tests reuse established harness idioms verbatim (`BusInstance`, `Unique`, `DisposeAsync` sweep, `WaitForAsync`, `TestContext.Current.CancellationToken`, no `.ConfigureAwait(false)` in `[Fact]`s).
- Integration tests require the local broker at `localhost:5552` with non-guest creds (`STREAMBUS_USER`/`STREAMBUS_PASSWORD`, defaulting to `app`/`password` in-test); never use `guest`.
- Every commit step requires explicit human approval before running; never push, never amend without permission.

## Review Focus

- A throwing `OnRetryExhausted` hook must never change the handoff outcome (advance-once + error publish still happen). Pinned by Task 2 hook test.
- One bus publishing to two streams must never cross routes or bodies. Pinned by Task 2 two-stream test (per-stream body asserts).
- NFR evidence must be recorded command outputs and pointers, never assertions from memory. Pinned by Task 3 (outputs pasted into the acceptance doc).
- Every FR marked ✅ must name the test pinning its acceptance criterion — not a same-named test covering half of it. Pinned by Task 1 matrix discipline (criterion column).
- G-3 must stay open and unclaimed (no cluster exists). Pinned by Task 1 + 3 gate-table wording.

---

## File Structure

New files:

- `docs/2026-10-13-streambus-acceptance.md` — the v1 acceptance record (Task 1 skeleton + matrix, Task 3 finalized with NFR evidence + gate roll-up + verdict). The durable deliverable of this phase.
- `tests/StreamBus.IntegrationTests/AcceptanceIntegrationTests.cs` — gap tests (Task 2).

Modified files: none expected (audit + tests only; library changes only if a gap test exposes a real bug, ruled first).

Explicitly out of Phase 10 (not stubs): new features, perf benchmarking beyond the documented hot-key ceiling (timing assertions are inherently flaky — ceiling stays documented + metrics-visible), 3-node cluster work (G-3 stays open), policy lifecycle (operator domain), any NFR re-design.

Recorded rulings (controller audit findings; bind all tasks): FR-8.2 (one bus, two streams) and FR-4.5 (hook observe-only) are the only functional gaps — everything else maps to existing tests (Task 1 confirms or extends with controller amendment before Task 2 dispatches); FR-5.2 effective retention and FR-3.4 hot-key ceiling are verified by declaration-args + documentation + metrics visibility, not by trimming/throughput assertions (both are timing/environment-dependent by design — see design §8.2); NFR evidence is recorded outputs, with no committed meta-tests (a test asserting its own repo's package list is brittle ceremony); the acceptance doc is a project record and commits like code.

---

### Task 1: Traceability audit + acceptance doc skeleton

**Files:**
- Create: `docs/2026-10-13-streambus-acceptance.md`

**Interfaces:**
- Consumes: `docs/2026-10-08-streambus-requirements.md` (FR + NFR + gates table), `docs/2026-10-08-streambus-design.md` (§13 metrics catalog, phase table), every test file under `tests/` (read bodies, not names).
- Produces: acceptance doc with per-FR rows + NFR placeholders + gate table for Task 3.

- [ ] **Step 1: Audit every requirement against test bodies (very thorough)**

Read the requirements doc end to end (FR-1–FR-10 with IDs and acceptance criteria, NFR-1–NFR-7, §5 gates), then read every test file body (not just names — verify each test actually asserts the criterion):

Unit: `ConnectionSupervisorTests`, `ConsumerMetricsTests`, `EffectivelyOnceTests`, `ErrorClassificationTests`, `ErrorEnvelopeTests`, `ErrorHandoffTests`, `HostingTests`, `OffsetCadenceTests`, `OffsetResolutionTests`, `PublishTests`, `RegistryTests`, `ResultPublisherTests`, `RetryPolicyTests`, `TopologyTests`, `TypedConsumerTests` (`tests/StreamBus.UnitTests/`).

Integration: `BusCoreIntegrationTests`, `ConsumerIntegrationTests`, `EffectivelyOnceIntegrationTests`, `PublishIntegrationTests`, `RetryIntegrationTests`, `TopologyIntegrationTests`, `TypedConsumerIntegrationTests`, `TypedStartupIntegrationTests` (`tests/StreamBus.IntegrationTests/`).

Rules for the matrix: a row is ✅ only if a test asserts the acceptance criterion (a same-named test covering half the criterion is ⚠️ with a note, never ✅). Expected confirmations (verify, do not assume): FR-8.2 has NO pin (gap → Task 2); FR-4.5 hook invocation/observation has NO pin (gap → Task 2); FR-10.9 IS pinned (`Failover_preserves_offsets_and_order` asserts per-partition stored == max received); FR-5.1 IS pinned (envelope field asserts + round-trip unit tests); FR-4.3 attempt numbering IS pinned (one-based calculator test); everything else per the bodies.

- [ ] **Step 2: Write `docs/2026-10-13-streambus-acceptance.md`**

```markdown
# StreamBus v1 Acceptance

Date: 2026-10-13. Broker: localhost:5552 (single node; version per `rabbitmqctl version` — record it here).
Branch: feature/phase10-acceptance. Head: <commit reviewed>.

## Functional matrix

| ID | Criterion (one line) | Pinning tests | Status |
...one row per FR-1.1 through FR-10.14...
- Status values: ✅ pinned (test names), ⚠️ partial (what is missing), ❌ gap (Task 2 covers it), N/A with reason (only where the acceptance itself is docs-side, e.g. FR-3.4/FR-5.2 — say so explicitly).

## Non-functional verification

One subsection per NFR-1–NFR-7. Each names its evidence type; Task 3 pastes command outputs here:
- NFR-1: ordering-first — evidence: ordering test roll-up (list the per-key-order tests) + Strict-by-default code pointer.
- NFR-2: no database/scheduler/MassTransit — evidence: `dotnet list` package output (Task 3).
- NFR-3: library owns offset-on-promotion; no implicit First/Next — evidence: `OffsetTypeFirst`/`OffsetTypeNext` site table with per-site justification (Task 3 grep).
- NFR-4: hot-key documented, not auto-solved — evidence: doc pointers (design §4.5, requirements §4) + metrics visibility test.
- NFR-5: automatic placement untouched — evidence: no placement-API grep hits + locator mapping test pointer.
- NFR-6: no TTL/DLX/priority assumptions — evidence: TTL/DLX grep (expect none) + priority-prose check.
- NFR-7: broker-owned offsets — evidence: FR-10 roll-up + no-local-resume code pointers.

## Gates

| ID | Gate | Evidence | Status |
- G-1: retry under load + failover — evidence: `Retry_preserves_order_under_load_and_failover` green.
- G-2: effectively-once end to end — evidence: `Full_cycle_super_to_super_with_flakes_and_kill_G2` green.
- G-3: 3-node placement — status OPEN (no cluster; skip-guarded test ready).
- G-4: SAC failover offsets + ordering — evidence: failover + super-split tests green.
- G-5: reconnect-during-retry + stale monotonicity — evidence: Strict kill-window test + guard/regression tests green.

## v1 verdict

(Completed by Task 3 after the final green run.)
```

- [ ] **Step 3: Verify**

```powershell
git status --short
```

Expected: only the new doc untracked (+ the plan file if uncommitted). No build impact (docs only), but run the unit suite as a sanity pulse:

```powershell
dotnet test tests/StreamBus.UnitTests/StreamBus.UnitTests.csproj -c Release --nologo -v q
```

- [ ] **Step 4: Commit (with human approval)**

```powershell
git add docs/2026-10-13-streambus-acceptance.md
git commit -m "docs: add v1 acceptance matrix skeleton"
```

---

### Task 2: Gap tests (FR-8.2 + FR-4.5 hook)

**Files:**
- Create: `tests/StreamBus.IntegrationTests/AcceptanceIntegrationTests.cs`

**Interfaces:**
- Consumes: `BusInstance`-style harness (copied verbatim below), `ErrorHandoff` fields, `RetryPolicy`/`ErrorStreamOptions` records.
- Produces: gap closure for Task 3's final matrix + verdict.

Controller note: Task 1 confirms this list. If the audit names further genuine gaps, the controller amends this task (ledgered ruling + exact test code) before dispatch — the two tests below always ship regardless.

- [ ] **Step 1: Implement `AcceptanceIntegrationTests.cs`**

```csharp
using System.Text;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using RabbitMQ.Stream.Client;

namespace StreamBus.IntegrationTests;

public sealed class AcceptanceIntegrationTests : IAsyncDisposable
{
    private static string User() => Environment.GetEnvironmentVariable("STREAMBUS_USER") ?? "app";
    private static string Password() => Environment.GetEnvironmentVariable("STREAMBUS_PASSWORD") ?? "password";
    private readonly List<string> _streams = new();

    private async Task<StreamSystem> ConnectAdminAsync()
        => await StreamSystem.Create(new StreamSystemConfig
        {
            UserName = User(),
            Password = Password(),
            VirtualHost = "/",
            Endpoints = new List<System.Net.EndPoint> { new System.Net.IPEndPoint(System.Net.IPAddress.Loopback, 5552) },
        });

    private static string Unique(string prefix) => $"{prefix}-{Guid.NewGuid():N}";

    public async ValueTask DisposeAsync()
    {
        var admin = await ConnectAdminAsync();
        try
        {
            foreach (var s in _streams) { try { await admin.DeleteStream(s); } catch { } }
        }
        finally { await admin.DisposeAsync(); }
    }

    private sealed class BusInstance : IAsyncDisposable
    {
        public ServiceProvider Provider { get; private set; } = null!;
        public StreamBusHostedService Hosted { get; private set; } = null!;
        public IStreamBus Bus { get; private set; } = null!;
        public static async Task<BusInstance> StartAsync(string vhost = "/")
        {
            var services = new ServiceCollection();
            services.AddLogging();
            services.AddStreamBus(vhost, o => o.Credentials(User(), Password()));
            var sp = services.BuildServiceProvider();
            var hosted = sp.GetServices<IHostedService>().OfType<StreamBusHostedService>().Single();
            await hosted.StartAsync(TestContext.Current.CancellationToken);
            var bus = sp.GetRequiredService<IStreamBus>();
            Assert.Equal(BusReadiness.Ready, await bus.WaitUntilReadyAsync(ReadinessPolicy.UpTo(TimeSpan.FromSeconds(30)), TestContext.Current.CancellationToken));
            return new BusInstance { Provider = sp, Hosted = hosted, Bus = bus };
        }
        public async ValueTask DisposeAsync()
        {
            try { await Hosted.StopAsync(TestContext.Current.CancellationToken); }
            finally { await Provider.DisposeAsync(); }
        }
    }

    private static ReadOnlyMemory<byte> Body(string s) => Encoding.UTF8.GetBytes(s);

    private static async Task WaitForAsync(Func<bool> done, TimeSpan timeout, string what)
    {
        var deadline = DateTime.UtcNow + timeout;
        while (!done())
        {
            if (DateTime.UtcNow >= deadline) Assert.Fail($"Timed out waiting for {what}.");
            await Task.Delay(100, TestContext.Current.CancellationToken);
        }
    }

    [Fact]
    public async Task One_bus_serves_two_streams()
    {
        // FR-8.2: one bus instance serves many streams — both confirmed, bodies uncrossed.
        var ct = TestContext.Current.CancellationToken;
        var first = Unique("acc-s1");
        var second = Unique("acc-s2");
        _streams.Add(first);
        _streams.Add(second);
        var admin = await ConnectAdminAsync();
        await using var _ = admin;
        await admin.CreateStream(new StreamSpec(first));
        await admin.CreateStream(new StreamSpec(second));
        await using var inst = await BusInstance.StartAsync();
        await inst.Bus.PublishAsync(first, Body("a:0"), new PublishOptions(), ct);
        await inst.Bus.PublishAsync(second, Body("b:0"), new PublishOptions(), ct);
        var gotFirst = new List<string>();
        var gotSecond = new List<string>();
        using var c1 = await inst.Bus.ConsumeAsync(first,
            new ConsumeOptions { Offset = ConsumeOffset.AlwaysFirst, Reference = Unique("read-1") },
            ctx => { lock (gotFirst) gotFirst.Add(Encoding.UTF8.GetString(ctx.Body.Span)); return Task.CompletedTask; }, ct);
        using var c2 = await inst.Bus.ConsumeAsync(second,
            new ConsumeOptions { Offset = ConsumeOffset.AlwaysFirst, Reference = Unique("read-2") },
            ctx => { lock (gotSecond) gotSecond.Add(Encoding.UTF8.GetString(ctx.Body.Span)); return Task.CompletedTask; }, ct);
        await WaitForAsync(() =>
        {
            lock (gotFirst) lock (gotSecond) return gotFirst.Count >= 1 && gotSecond.Count >= 1;
        }, TimeSpan.FromSeconds(30), "both streams delivered");
        await Task.Delay(500, ct);
        lock (gotFirst) lock (gotSecond)
        {
            Assert.Equal(new[] { "a:0" }, gotFirst);
            Assert.Equal(new[] { "b:0" }, gotSecond);
        }
    }
```

```csharp
    [Fact]
    public async Task OnRetryExhausted_observes_but_never_changes_outcome()
    {
        // FR-4.5 hook half: the hook observes the handoff (all fields) and even a
        // throwing hook cannot change the outcome (advance-once + error publish).
        var ct = TestContext.Current.CancellationToken;
        var stream = Unique("acc-src");
        var error = Unique("acc-src.error");
        _streams.Add(stream);
        _streams.Add(error);
        var admin = await ConnectAdminAsync();
        await using var _ = admin;
        await admin.CreateStream(new StreamSpec(stream));
        await admin.CreateStream(new StreamSpec(error) { MaxAge = TimeSpan.FromDays(7) });
        var handoffs = new List<ErrorHandoff>();
        var hookCalls = 0;
        var completed = new List<ulong>();
        await using var inst = await BusInstance.StartAsync();
        using var consumer = await inst.Bus.ConsumeAsync(stream,
            new ConsumeOptions
            {
                Reference = Unique("grp-acc"),
                Retry = new RetryPolicy { FailureMode = FailureMode.SkipAfterDeadline, MaxAttempts = 2, BaseDelay = TimeSpan.FromMilliseconds(10), MaxJitter = TimeSpan.Zero },
                ErrorStream = new ErrorStreamOptions { Stream = error },
                OnRetryExhausted = h =>
                {
                    Interlocked.Increment(ref hookCalls);
                    if (h.SourceOffset == 1) throw new InvalidOperationException("hook boom");
                    lock (handoffs) handoffs.Add(h);
                    return Task.CompletedTask;
                },
            },
            ctx =>
            {
                if (ctx.Offset < 2) throw new InvalidOperationException($"poison {ctx.Offset}");
                lock (completed) completed.Add(ctx.Offset);
                return Task.CompletedTask;
            },
            ct);
        await using var pub = await BusInstance.StartAsync();
        await pub.Bus.PublishAsync(stream, Body("poison-a"), new PublishOptions(), ct);
        await pub.Bus.PublishAsync(stream, Body("poison-b"), new PublishOptions(), ct);
        await pub.Bus.PublishAsync(stream, Body("good:2"), new PublishOptions(), ct);
        await pub.DisposeAsync();
        var errors = new List<byte[]>();
        using var reader = await inst.Bus.ConsumeAsync(error,
            new ConsumeOptions { Offset = ConsumeOffset.AlwaysFirst, Reference = Unique("read") },
            ctx => { lock (errors) errors.Add(ctx.Body.ToArray()); return Task.CompletedTask; }, ct);
        await WaitForAsync(() => { lock (errors) return errors.Count >= 2; }, TimeSpan.FromSeconds(60), "both handoffs");
        await WaitForAsync(() => Volatile.Read(ref hookCalls) >= 2, TimeSpan.FromSeconds(30), "both hook runs");
        await WaitForAsync(() => { lock (completed) return completed.Count >= 1; }, TimeSpan.FromSeconds(30), "good processed past both handoffs");
        ErrorHandoff handoff;
        lock (handoffs) handoff = Assert.Single(handoffs);
        Assert.Equal(error, handoff.ErrorStream);
        Assert.Equal(stream, handoff.SourceStream);
        Assert.Equal(0, handoff.SourcePartition);
        Assert.Equal(0UL, handoff.SourceOffset);
        Assert.Equal(2, handoff.Attempts);
        Assert.IsType<InvalidOperationException>(handoff.Exception);
        Assert.Contains("poison 0", handoff.Exception.Message);
        Assert.True(DateTimeOffset.UtcNow - handoff.FailedAt < TimeSpan.FromMinutes(1));
        Assert.True(handoff.FailedAt <= DateTimeOffset.UtcNow);
        var bodies = new List<string>();
        lock (errors) bodies.AddRange(errors.Select(Encoding.UTF8.GetString));
        Assert.Contains("poison-a", bodies);
        Assert.Contains("poison-b", bodies);
        lock (completed) Assert.Equal(new[] { 2UL }, completed);
    }
}
```

(`Reference = "grp-acc-hook"` — fixed (not Unique): the test asserts nothing about the stored value, and a fixed ref is stable across the single run; Unique would also work. Keep Unique for hygiene? A fixed ref risks cross-run broker state (offsets persist per reference!). If a previous failed run stored offsets, a rerun resumes past them → flake. Use Unique (fresh group every run — hermetic). The other test uses Unique refs ✓. So: `Reference = Unique("grp-acc")`.)

Wait — one flaw: hook for offset 0 records; hook for offset 1 throws. But attempts interleave? Partition is sequential (single partition, dispatch gate): offset 0 exhausts (2 attempts) → handoff+hook → offset 1 exhausts → handoff+hook(throws) → offset 2 succeeds. Deterministic order ✓. hookCalls==2 eventually ✓. handoffs has exactly A ✓.

`Assert.Single(handoffs)` — List<ErrorHandoff> single ✓.

`Encoding.UTF8.GetString` as method group in Select — `errors.Select(Encoding.UTF8.GetString)` — GetString(byte[]) overload exists ✓ (used in existing tests? They use `Encoding.UTF8.GetString(body.Span)`. Method group `Encoding.UTF8.GetString` with byte[] → resolves to GetString(byte[]) ✓ compiles.)

- [ ] **Step 2: Verify**

```powershell
dotnet test tests/StreamBus.IntegrationTests/StreamBus.IntegrationTests.csproj -c Release --filter "FullyQualifiedName~Acceptance"
```

Expected: both green against `localhost:5552` (+ any Task-1-confirmed extras, amended in).

- [ ] **Step 3: Commit (with human approval)**

```powershell
git add tests/StreamBus.IntegrationTests/AcceptanceIntegrationTests.cs
git commit -m "test: add v1 acceptance gap probes"
```

---

### Task 3: NFR evidence + gate roll-up + final green

**Files:**
- Modify: `docs/2026-10-13-streambus-acceptance.md` (paste outputs, set statuses, write verdict)

**Interfaces:**
- Consumes: Task 1 matrix, Task 2 green, existing suites.
- Produces: the v1 verdict.

- [ ] **Step 1: Record NFR evidence (paste command + output into the doc)**

Run each command from the repo root and paste verbatim output under the matching NFR section:

```powershell
dotnet list src/StreamBus/StreamBus.csproj package
```

Expected shape (NFR-2): `RabbitMQ.Stream.Client 1.12.1`, `Microsoft.Extensions.* 10.0.12`, test SDK/xUnit — and nothing else (no MassTransit, no database client, no scheduler). If any unexpected package appears, STOP and report BLOCKED (dependency drift).

```powershell
Select-String -Path 'src/StreamBus/*.cs' -Pattern 'ReliableProducer|ReliableConsumer|ReliableSuperStream|Reliable[A-Z]\w*'
Select-String -Path 'src/StreamBus/*.cs' -Pattern '\bTTL\b|\bDLX\b'
Select-String -Path 'src/StreamBus/*.cs' -Pattern 'MassTransit'
Select-String -Path 'src/StreamBus/*.cs' -Pattern 'DeleteStream|DeleteSuperStream'
Select-String -Path 'src/StreamBus/*.cs' -Pattern 'CreateStream|CreateSuperStream'
Select-String -Path 'src/StreamBus/*.cs' -Pattern 'OffsetTypeFirst|OffsetTypeNext'
Select-String -Path 'src/StreamBus/*.cs' -Pattern 'streambus_[a-z_]+' | Select-Object Filename, LineNumber, Line | Sort-Object Line
```

Expected shapes (record actuals either way; any surprise is a finding, not a failure):
- `Reliable*`: no matches (ordered paths never touch the swallowing layer — NFR-3 support).
- TTL/DLX: no matches (NFR-6 support).
- `MassTransit`: matches only in prose comments/docs (`MassTransit-style`, `HostOptions`-style) — never a using, dependency, or type reference (NFR-2 support).
- `DeleteStream`: no matches (topology never deletes).
- `CreateStream`: only `Connector.cs` pass-throughs + `TopologyRegistry.cs` via-handle calls (FR-7 sanctioned).
- `OffsetType*`: enumerate each site in the doc with its justification from this list (NFR-3/NFR-7 support — verify against the code, do not invent):
  - explicit `AlwaysFirst`/`AlwaysNext` operator escapes (resolution + options);
  - no-stored-offset fallback branches (`ResumeOrFirst`→First, `ResumeOrNext`→Next, each with warning + fallback metric);
  - promotion-degrade fallback (listener must never throw; degrades to replay-from-first, loud);
  - first-run EO resume (never-published → First).
  Any site outside these four shapes is a finding — report it, do not rationalize it.
- `streambus_*`: every name must appear in the design §13 catalog (`partition_lag`, `partition_inflight`, `partition_retry_blocked`, `offset_store_rejected_total`, `offset_regressed_total`, `offset_fallback_total`, `connection_state`) — record the match table (FR-9.1 support).

- [ ] **Step 2: Finalize the verdict + gate table**

Set every matrix row from Task 1 + Task 2 evidence (no row may stay ❌; ⚠️ rows keep their explicit reason). Fill the NFR sections with the Step 1 outputs. Set the gate table (G-1/G-2/G-4/G-5 PASS with test names; G-3 OPEN). Write the v1 verdict: v1 is done iff every FR row is ✅/accepted-⚠️ and G-1/G-2/G-4/G-5 are PASS — with G-3 open pending a cluster, stated verbatim.

- [ ] **Step 3: Full green run (the v1 evidence)**

```powershell
dotnet test tests/StreamBus.UnitTests/StreamBus.UnitTests.csproj -c Release --nologo -v q
dotnet test tests/StreamBus.IntegrationTests/StreamBus.IntegrationTests.csproj -c Release --nologo -v q
```

Expected: unit all-pass; integration all-pass + the 2 known env-skips (policy-401, G-3 single-node). Record the exact counts in the verdict section. Any other failure or skip is a finding — stop and report, do not explain it away.

- [ ] **Step 4: Commit (with human approval)**

```powershell
git add docs/2026-10-13-streambus-acceptance.md tests/StreamBus.IntegrationTests/AcceptanceIntegrationTests.cs
git commit -m "docs: finalize v1 acceptance with NFR evidence and gate roll-up"
```

(The tests file commits in Task 2's step; if already committed, that path is a no-op here and the commit carries only the doc. Either shape is fine — the command is idempotent.)

---

## Follow-ups beyond v1 (tracked, not in scope)

- G-3 (3-node cluster): the only open gate. Rerun the skip-guarded placement test on a cluster; close the gate only on green non-skipped evidence.
- FR-5.2/FR-3.4 timing-dependent halves stay docs-side by design (segment-kept retention, throughput ceilings) — revisit only with a dedicated perf harness, never with timing asserts in the suite.
- The transient-applied-then-conflict coincidence (create fails transiently after the broker applied it, then compare finds a matching stream) reports conflict and heals on retry — accepted order-dependence, same family as the Phase 8 residual.

---

## Self-Review

- Spec coverage: FR-1–FR-10 each have a Task 1 matrix row (audited, not assumed); the two gaps found (FR-8.2, FR-4.5-hook) get exact tests in Task 2; NFR-1–NFR-7 each get an evidence subsection in Task 3; G-1–G-5 each get a gate-table row (G-3 open, stated).
- Placeholder scan: no TBD/TODO; Task 2 ships complete test code; Task 3 commands are exact with expected shapes; Task 1's sources are named files, not vibes; the T2-extra mechanism is an explicit controller-amendment path, not an open brief.
- Type consistency: `ErrorHandoff` fields (ErrorStream/SourceStream/SourcePartition/SourceOffset/Attempts/FailedAt/Exception) match Phase 6 records; `RetryPolicy`/`ErrorStreamOptions`/`ConsumeOptions` shapes match existing code; `Unique`/`WaitForAsync`/`Body` mirror established harness helpers; `Assert.Single(handoffs)` on `List<ErrorHandoff>` uses the collection overload.
- Review Focus: each line pinned (T2 hook test, T2 two-stream test, T3 outputs-in-doc, T1 criterion column, gate table wording).
