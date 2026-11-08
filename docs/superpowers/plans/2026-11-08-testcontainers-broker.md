# Testcontainers Broker Support Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** The integration test suite spins up (and tears down) its own RabbitMQ 4.2 broker via Testcontainers for .NET, so `dotnet test` works anywhere Docker runs and nobody starts a container by hand — then fix the five failing integration tests.

**Architecture:** A shared `TestBroker` singleton in the integration test project lazily starts one `rabbitmq:4.2-management` container per test run (stream plugin enabled offline, advertised host forced to localhost) and exposes host/ports/credentials. All 17 integration test files and `StreamManagementApi` take broker coordinates from it instead of hardcoded `localhost`/5552/5672/15672 and `STREAMBUS_USER`/`STREAMBUS_PASSWORD` defaults. CI workflows are simplified to plain `dotnet test`; Testcontainers' Ryuk sidecar handles container cleanup at process exit.

**Tech Stack:** .NET 10, xunit.v3, `Testcontainers.RabbitMq` 4.16.0 (Central Package Management), `rabbitmq:4.2-management` Docker image.

**Spec:** `docs/superpowers/specs/2026-11-08-testcontainers-broker-design.md`

## Global Constraints

- Image pinned to `rabbitmq:4.2-management` (the 4.2 minor line) — never Testcontainers' default image, never unpinned.
- `Testcontainers.RabbitMq` version `4.16.0`, added to `Directory.Packages.props` (CPM is on).
- Container credentials: user `app`, password `password` (stream client rejects `guest`).
- Stream plugin enabled **after** server start via container exec `rabbitmq-plugins enable rabbitmq_stream` (same as CI does today); a readiness poll gates the handle. `RABBITMQ_SERVER_ADDITIONAL_ERL_ARGS` cannot enable plugins (README documents this), and `WithCommand` must not be used (it replaces the `rabbitmq-server` CMD).
- `-rabbitmq_stream advertised_host localhost` set via env `RABBITMQ_SERVER_ADDITIONAL_ERL_ARGS`, because the stream client connects to the host the broker advertises.
- `STREAMBUS_SKIP_INTEGRATION=1` must still skip every integration test AND must not start the container.
- No changes under `src/StreamBus`.
- Test names keep Given_When_Then underscores; `NoWarn` for `CA1707;CA1861` already set in the test csproj.

## Review Focus

- **Docker absent:** with no Docker daemon, tests should fail to start the container with a clear Testcontainers error (or be skipped via the env lane) — not hang forever building the container.
- **Escape hatch unused container:** `STREAMBUS_SKIP_INTEGRATION=1` must never trigger `TestBroker` startup; the skip check happens before any container reference.
- **Port drift:** any remaining literal `5552`/`5672`/`15672` or `localhost` in test code after migration means a file was missed; the whole-suite run in Task 4 catches it.
- **Parallel test classes sharing one container:** xunit runs test classes in parallel; the lazy singleton must be thread-safe and start exactly once.
- **Recon container collision:** a developer's manually-started broker bound to 5552/5672 must not break the suite — Testcontainers maps random host ports, so verify the suite passes with `streambus-recon` still running.

---

### Task 0: Create the feature branch

- [ ] **Step 1: Branch off origin/main**

Run: `git checkout -b testcontainers-integration-tests origin/main`
Expected: branch `testcontainers-integration-tests` created, working tree clean, based on `a32bf07`.

- [ ] **Step 2: Commit the design and plan docs**

Run: `git add docs/superpowers/ && git commit -m "docs: design and plan for Testcontainers broker support"`
Expected: commit created on the new branch.

### Task 1: `TestBroker` singleton

**Files:**
- Modify: `Directory.Packages.props` (add after line 8, `RabbitMQ.Client`)
- Modify: `tests/StreamBus.IntegrationTests/StreamBus.IntegrationTests.csproj` (add `<PackageReference Include="Testcontainers.RabbitMq" />` to the first ItemGroup)
- Create: `tests/StreamBus.IntegrationTests/TestBroker.cs`

**Interfaces:**
- Produces: `internal static class TestBroker` with:
  - `Task<BrokerHandle> StartAsync(CancellationToken ct = default)` — idempotent across concurrent callers; returns a cached task after the first call.
  - `sealed class BrokerHandle` (nested or file-level) exposing: `string Host`, `int StreamPort`, `int AmqpPort`, `int ManagementPort`, `string Username` ("app"), `string Password` ("password"). Constructed from the container's mapped ports.
- Consumes: nothing yet (existing tests unchanged in this task).

- [ ] **Step 1: Add the package and write the smoke test**

Add to the integration test file of your choice — create `tests/StreamBus.IntegrationTests/TestBrokerSmokeTests.cs`:

```csharp
namespace StreamBus.IntegrationTests;

public class TestBrokerSmokeTests()
{
    [Fact]
    public async Task Start_returns_running_broker_with_stream_listener()
    {
        IntegrationLane.RequireBroker();
        var broker = await TestBroker.StartAsync();
        Assert.Equal("app", broker.Username);
        Assert.True(broker.StreamPort > 0);
        using var system = await RabbitMQ.Stream.Client.StreamSystem.Create(new()
        {
            Endpoints = [new System.Net.IPEndPoint(System.Net.IPAddress.Loopback, broker.StreamPort)],
            UserName = broker.Username,
            Password = broker.Password,
        });
        Assert.True(BrokerProbe.StreamExists(system, "smoke-test-stream"));
        // create a throwaway stream via the real StreamSystem API, assert with
        // BrokerProbe.StreamExists, then delete it
    }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `dotnet test tests/StreamBus.IntegrationTests --filter TestBrokerSmokeTests`
Expected: compile error — `TestBroker` does not exist.

- [ ] **Step 3: Implement `TestBroker.cs`**

`RabbitMQBuilder` → `.WithImage("rabbitmq:4.2-management")` → `.WithUsername("app").WithPassword("password")` → `.WithEnvironment("RABBITMQ_SERVER_ADDITIONAL_ERL_ARGS", "-rabbitmq_stream advertised_host localhost")` → `.WithCleanUp(true)` → await `StartAsync(ct)` on the built container.

**Do not use `WithCommand` to enable the stream plugin — it overrides the image's `rabbitmq-server` CMD, so the container would run the plugin command and exit.** Instead, mirror what CI does today, inside the start sequence: after `container.StartAsync` succeeds, run `rabbitmq-plugins enable rabbitmq_stream` on the live node via container exec (Testcontainers exec API — `container.ExecScriptAsync("rabbitmq-plugins enable rabbitmq_stream")` or equivalent), then poll stream-listener readiness — attempt `StreamSystem.Create` at `StreamPort` plus a trivial probe (reuse `BrokerProbe`), retry up to 60 s — before returning the handle.

`TestBroker.StartAsync` holds a single static `Task<BrokerHandle>` created under a lock (or `Lazy<Task<T>>`): concurrent first calls all await the same in-flight task and receive the same handle instance. Cleanup relies on `WithCleanUp(true)` (the Ryuk sidecar reaps the container when the test process exits); fine for a test fixture.

- [ ] **Step 4: Run the smoke test to verify it passes**

Run: `dotnet test tests/StreamBus.IntegrationTests --filter TestBrokerSmokeTests`
Expected: PASS — exactly one container started (`docker ps` shows a `testcontainers` labeled `rabbitmq:4.2-management`).

- [ ] **Step 5: Verify the skip lane does not start a container**

Run: `STREAMBUS_SKIP_INTEGRATION=1 dotnet test tests/StreamBus.IntegrationTests --filter TestBrokerSmokeTests`
Expected: 1 skipped, 0 run; `docker ps` shows no new `testcontainers`-labeled RabbitMQ.

- [ ] **Step 6: Commit**

```bash
git add Directory.Packages.props tests/StreamBus.IntegrationTests/TestBroker.cs tests/StreamBus.IntegrationTests/TestBrokerSmokeTests.cs tests/StreamBus.IntegrationTests/StreamBus.IntegrationTests.csproj
git commit -m "test: Testcontainers RabbitMQ broker fixture for integration tests"
```

### Task 2: Migrate `StreamManagementApi` to the fixture

**Files:**
- Modify: `tests/StreamBus.IntegrationTests/StreamManagementApi.cs` (~lines 32-50, the credential/URL probing logic)

**Interfaces:**
- Consumes: `TestBroker.StartAsync()` → `BrokerHandle.ManagementPort`, `.Username`, `.Password`, `.Host`
- Produces: `StreamManagementApi.ConnectAsync(CancellationToken ct)` keeps its public signature; internally it no longer reads `STREAMBUS_MANAGEMENT_URL`, no longer probes credential candidates.

- [ ] **Step 1: Replace the connection logic**

Delete the `Candidates()` probing and the `STREAMBUS_MANAGEMENT_URL` read inside `ConnectAsync` (the `api/whoami` readiness poll concept can stay). New body: start the broker, build `http://{handle.Host}:{handle.ManagementPort}/`, Basic-auth header from `handle.Username`/`handle.Password`, poll `api/whoami` until success as today.
Keep the `User` property — tests reference it.

- [ ] **Step 2: Verify against the still-running suite**

Run: `dotnet test tests/StreamBus.IntegrationTests --filter "StreamManagementApi|BrokerRestart|ConsumerDeath"`
Expected: management-API-driven tests reach the Testcontainers broker (pass or fail for the same reasons as recon — the HTTP 400 failures are Task 5 territory; the point here is no connection errors).

- [ ] **Step 3: Commit**

```bash
git add tests/StreamBus.IntegrationTests/StreamManagementApi.cs
git commit -m "test: StreamManagementApi uses Testcontainers broker coordinates"
```

### Task 3: Migrate the 17 test files

**Files:**
- Modify: all 17 `tests/StreamBus.IntegrationTests/*IntegrationTests.cs` files (26 `cfg.Host(` literals, 14 `IPEndPoint(...5552)`, 7 `5672` literals), plus `AcceptanceIntegrationTests.cs`'s vhost-hosting variant and any remaining `User()`/`Password()` helpers reading env vars
- Modify: `tests/StreamBus.IntegrationTests/IntegrationLane.cs` only if a doc comment update is needed (no behavior change)

**Interfaces:**
- Consumes: `TestBroker.StartAsync()` → `BrokerHandle` (Task 1)
- Produces: no new interface — test files now call the fixture instead of literals.

- [ ] **Step 1: Replace hardcoded coordinates per file**

Per file: delete the private `User()`/`Password()` env-var helpers; call sites become pattern-style:
- `cfg.Host(handle.Host, vhost or "/", h => h.Credentials(handle.Username, handle.Password))`
- `StreamSystemConfig { Endpoints = [new IPEndPoint(IPAddress.Loopback, handle.StreamPort)], UserName = handle.Username, Password = handle.Password }`
- AMQP endpoints (5672) become `handle.AmqpPort`.
Since constructors are sync (`RequireBroker()`), each test obtains the handle the way its connection helpers already run (they are `async Task` methods) — pattern: each file's `ConnectAdminAsync()` starts with `var broker = await TestBroker.StartAsync();`. Files that need credentials in sync constructors keep a thin sync wrapper (`TestBroker.StartAsync().GetAwaiter().GetResult()` — acceptable only if a sync context truly requires it; prefer the async path).
Remove `STREAMBUS_USER`/`STREAMBUS_PASSWORD` reads everywhere.

- [ ] **Step 2: Grep for leftovers**

Run: `grep -rnE '"localhost"|, 5552|, 5672|15672|STREAMBUS_USER|STREAMBUS_PASSWORD' tests/StreamBus.IntegrationTests --include=*.cs`
Expected: only `TestBroker.cs` and `StreamManagementApi.cs` (base URL defaults and env reads removed) — plus `docs/` and workflow references, none in test code. Fix anything else found.

- [ ] **Step 3: Full suite against Testcontainers, with recon container still running**

Run: `dotnet test tests/StreamBus.IntegrationTests`
Expected: 55+/56 passed (previously passing tests must stay green portably); the 5 known failures may persist (fixed in Task 5); 0 failures beyond the known 5; 1 skip only with the env lane. Concurrent classes means startup happens once — verify via `docker ps` showing a single Testcontainers RabbitMQ during the run.

- [ ] **Step 4: Skip-lane full run**

Run: `STREAMBUS_SKIP_INTEGRATION=1 dotnet test tests/StreamBus.IntegrationTests`
Expected: 60 skipped, 0 failed, 0 containers started.

- [ ] **Step 5: Commit**

```bash
git add tests/StreamBus.IntegrationTests
git commit -m "test: integration tests take broker coordinates from TestBroker fixture"
```

### Task 4: Simplify CI workflows

**Files:**
- Modify: `.github/workflows/build.yml` (delete the `services.rabbitmq` block ~lines 93-111, the `env` with `STREAMBUS_USER`/`STREAMBUS_PASSWORD` ~117-119, the "Enable RabbitMQ stream plugin" step ~131-133, the "Wait for the stream listener" step ~135-145, and any "management API" wait step ~147-160)
- Modify: `.github/workflows/release.yml` (same shape: services block, plugin step, listener-wait steps — verify by reading the file first)
- Modify: `README.md` prerequisites section — replace "RabbitMQ 4.x with streams enabled" test-run guidance (only if README describes running the integration suite; adjust what's actually there; likely add one line: integration tests need only Docker, they start their own broker)

**Interfaces:**
- Consumes: Tasks 1-3 (the suite is self-sufficient).
- Produces: a CI setup where the integration job is `dotnet test` on `ubuntu-latest` with nothing broker-specific.

- [ ] **Step 1: Strip broker setup from build.yml**

Delete the blocks named above. The integration job's steps reduce to checkout, setup-dotnet, restore/test. Read the surrounding lines first — keep unrelated steps (lint, pack, artifact upload).

- [ ] **Step 2: Verify YAML and CI parity for release.yml**

Open `release.yml`, apply the same deletions (services rabbitmq, plugin enable, listener/management waits, related env).
Run: `python3 -c "import yaml,sys; yaml.safe_load(open('.github/workflows/build.yml')); yaml.safe_load(open('.github/workflows/release.yml'))"`
Expected: both parse.

- [ ] **Step 3: Run the whole suite once more locally**

Run: `dotnet test` (solution level)
Expected: unit tests pass (they never needed a broker); integration as in Task 3.

- [ ] **Step 4: Commit**

```bash
git add .github/workflows/build.yml .github/workflows/release.yml README.md
git commit -m "ci: integration suite brings its own Testcontainers broker"
```

### Task 5: Fix the five failing integration tests

**Files:**
- Modify: TBD by diagnosis — candidates: `BusCoreIntegrationTests.cs`, `ConsumePipelineIntegrationTests.cs`, `BrokerRestartIntegrationTests.cs`, `ConsumerDeathIntegrationTests.cs`, `tests/StreamBus.IntegrationTests/StreamManagementApi.cs`, possibly `src/StreamBus` internals (this task MAY break the "no src changes" constraint — the spec's non-goal was for the Testcontainers phase; a genuine library bug found here gets fixed, and that gets flagged to the human partner first)

**Interfaces:**
- Consumes: the green rest of the suite (Tasks 1-3) as a safety net.
- Produces: all 61 integration tests passing.

Known failures from recon (2026-11-08, rabbitmq:4.2-management):
1. `BusCoreIntegrationTests.Health_reports_per_vhost_bus_type_state` — health report contains only `bus:/|StreamBus.DefaultBus`, assertion expects more per-vhost entries.
2. `ConsumePipelineIntegrationTests.Zero_next_advances_the_offset_once_and_does_not_redeliver_after_dispose` — timeout waiting for three filtered offsets to be stored.
3. `BrokerRestartIntegrationTests.Connections_kill_does_not_skip_or_gap` — `GET api/stream/connections/%2F` HTTP 400.
4. `ConsumerDeathIntegrationTests.Connection_closed_while_idle_reattaches_within_bounds` — `GET api/stream/consumers/%2F` HTTP 400.
5. `ConsumerDeathIntegrationTests.Metadata_event_close_reattaches_and_promotes_correctly` — same API 400.

- [ ] **Step 1: Diagnose (mandatory skill: systematic-debugging)**

Work failures 3-5 first — they share management-API 400s, likely one root cause. Check the rabbitmq-management response body, RabbitMQ 4.2 stream plugin endpoints, and whether the API user's tags allow `api/stream/*`. Reproduce with curl against the running Testcontainers broker to isolate test-code vs broker behavior.
Then failures 1 and 2 individually: read the assertion and the tested StreamBus code path; decide test bug vs library bug. **If it's a library bug, stop and report to the human partner before changing `src/StreamBus`.**

- [ ] **Step 2: Fix + verify per failure**

For each: apply the fix, run

Run: `dotnet test tests/StreamBus.IntegrationTests --filter "<test name>"`
Expected: PASS, each fix run twice (portable-TCP-offset tests can be flaky — if flaky, note it and revisit).

- [ ] **Step 3: Full suite**

Run: `dotnet test tests/StreamBus.IntegrationTests`
Expected: `Passed: 60, Skipped: 1` (that skip is the intentional `Leader_placement_gate_G3`).

- [ ] **Step 4: Commit per fix**

```bash
git commit -m "test: <one line per root cause>"
```

- [ ] **Step 5: Final whole-branch review handoff**

REQUIRED SUB-SKILL: use `requesting-code-review` — dispatch a fresh reviewer over the whole branch before merging.
