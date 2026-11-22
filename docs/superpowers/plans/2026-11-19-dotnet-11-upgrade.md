# .NET 11 Upgrade (with .NET 10 support) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Ship one StreamBus NuGet package containing both a `net10.0` and a `net11.0` asset, built with the .NET 11 SDK, with runtime async enabled for the net11 asset only after verification.

**Architecture:** Multi-target the library and its test projects, keep examples on net10 as a source-level canary, add a package-level net10 smoke test, move CI to .NET 11 (with the .NET 10 SDK installed alongside), then enable runtime async on the net11 target behind a TFM condition. All work stays on the current branch (`nirav/feature/NET11Upgrade`); no new branches, no worktrees.

**Tech Stack:** .NET SDK 11.0.100 / 10.0.404, C# with `LangVersion=latest`, MSBuild central package management, xunit.v3, Testcontainers.RabbitMq, GitHub Actions, Astro Starlight docs.

**Spec:** `docs/superpowers/specs/2026-11-19-dotnet-11-multi-target-design.md`

## Global Constraints

- One `.nupkg` with `lib/net10.0/StreamBus.dll` and `lib/net11.0/StreamBus.dll`; one `.snupkg`; one version.
- The `net10.0` asset keeps today's behavior (no runtime async); examples remain `net10.0` with `ProjectReference`.
- Public API surfaces of both assets must be identical; no public member behind `#if`.
- `global.json` pins `11.0.100` with `rollForward: latestFeature`; CI installs `10.0.x` and `11.0.x`.
- `TreatWarningsAsErrors=true` stays on; new analyzer diagnostics get narrow, commented suppressions only.
- `Microsoft.Extensions.Hosting`/`.Logging.Abstractions`/`.Diagnostics.HealthChecks`: `10.0.15` for net10, `11.0.0` for net11 (conditional `PackageVersion`). `RabbitMQ.Stream.Client = 1.13.0`, `RabbitMQ.Client = 7.2.2`, `Testcontainers.RabbitMq = 4.17.0` for both TFMs.
- `VersionPrefix = 1.0.0` fixed in the library csproj for local builds; published versions always come from the release tag via `-p:Version` / `-p:PackageVersion`.
- Runtime async is enabled only in `src/StreamBus/StreamBus.csproj` via `Features` when `$(TargetFramework) == 'net11.0'`; rollback is removing that flag and rebuilding clean (`UseRuntimeAsync=false` is inert on SDK 11.0.100).
- No new branches, no git worktrees.

## Review Focus

1. **net10 consumer resolving the packed package** (not a ProjectReference) — Task 3's pack smoke test builds a net10 app from a local feed at the packed version.
2. **Public API drift between assets** — Task 2 runs the existing API-surface tests in both TFM legs; Task 6's `RuntimeAsyncCodegenTests` is `#if NET11_0` only.
3. **SDK 11 analyzer diagnostics becoming build errors** — Task 1's Release build with `TreatWarningsAsErrors=true` pins the fix.
4. **Runtime async actually changing codegen, and consumer opt-out not reaching our DLL** — Task 6's reflection test asserts no `AsyncStateMachineAttribute` on a known async method, plus a two-config local consumer probe.
5. **`IsTestingPlatformApplication=false` under SDK 11** — Task 1's classic `dotnet test` invocation must still pass; keep the flag if MTP targets require it.

---

### Task 1: Pin the SDK and prove the toolchain

**Files:**
- Create: `global.json`
- Modify: `Directory.Build.props` (only if SDK 11 surfaces new analyzer errors)

**Interfaces:**
- Produces: SDK floor used by every later task (`11.0.100`, `rollForward: latestFeature`).

- [ ] **Step 1: Add `global.json`**

```json
{
  "sdk": {
    "version": "11.0.100",
    "rollForward": "latestFeature"
  }
}
```

- [ ] **Step 2: Verify the pin resolves locally**

Run: `dotnet --version` (from repo root)
Expected: `11.0.100` (or a newer feature band under `latestFeature`).

- [ ] **Step 3: Build the current solution with SDK 11**

Run: `dotnet build StreamBus.slnx --configuration Release`
Expected: PASS. If new CA/SYSLIB diagnostics fail the build, add the specific diagnostic ID to the existing `NoWarn` line in `Directory.Build.props` with a one-line comment explaining why; never disable `TreatWarningsAsErrors`.

- [ ] **Step 4: Prove the existing unit tests run under SDK 11**

Run: `dotnet test tests/StreamBus.UnitTests/StreamBus.UnitTests.csproj --no-build --configuration Release`
Expected: PASS. If xunit.v3's MTP targets conflict, keep `IsTestingPlatformApplication=false` as-is; if the property is now inert, leave it and note it in the commit message.

- [ ] **Step 5: Commit**

```bash
git add global.json Directory.Build.props
git commit -m "build: pin .NET 11 SDK and verify toolchain"
```

---

### Task 2: Multi-target the library and test projects

**Files:**
- Modify: `Directory.Build.props`, `src/StreamBus/StreamBus.csproj`, `tests/StreamBus.UnitTests/StreamBus.UnitTests.csproj`, `tests/StreamBus.IntegrationTests/StreamBus.IntegrationTests.csproj`, `tests/StreamBus.DocsSnippets/StreamBus.DocsSnippets.csproj`, `examples/StreamBus.Examples.Producer/StreamBus.Examples.Producer.csproj`, `examples/StreamBus.Examples.Consumer/StreamBus.Examples.Consumer.csproj`

**Interfaces:**
- Produces: library, unit tests, integration tests, docs snippets target `net10.0;net11.0`; examples target `net10.0`.

- [ ] **Step 1: Make dual targeting the shared default in `Directory.Build.props`**

Replace `<TargetFramework>net10.0</TargetFramework>` with `<TargetFrameworks>net10.0;net11.0</TargetFrameworks>` (with a short comment). Leave every other property unchanged.

- [ ] **Step 2: Opt the examples out**

In both example projects add `<TargetFramework>net10.0</TargetFramework>` **and** `<TargetFrameworks></TargetFrameworks>` with a comment. Clearing `TargetFrameworks` is required: a non-empty value wins the outer build even when `TargetFramework` is set. Every other project inherits the shared default; do not change `Directory.Packages.props`.

- [ ] **Step 3: Build every TFM**

Run: `dotnet build StreamBus.slnx --configuration Release`
Expected: PASS for `net10.0` and `net11.0`; no `SYSLIB5007`, no analyzer errors. `StreamBus.DocsSnippets` is not in the solution, so also run `dotnet restore tests/StreamBus.DocsSnippets/StreamBus.DocsSnippets.csproj` then `dotnet build tests/StreamBus.DocsSnippets/StreamBus.DocsSnippets.csproj --configuration Release --no-restore`; expected: PASS for both TFMs.

- [ ] **Step 4: Run unit tests on both assets (public API parity included)**

Run: `dotnet test tests/StreamBus.UnitTests/StreamBus.UnitTests.csproj --no-build --configuration Release --framework net10.0` then the same with `--framework net11.0`
Expected: both PASS, including the existing API-surface tests.

- [ ] **Step 5: Confirm the net10 examples still build**

Run: `dotnet build examples/StreamBus.Examples.Producer/StreamBus.Examples.Producer.csproj --configuration Release` and the Consumer equivalent.
Expected: both PASS with `TargetFramework=net10.0` and their unchanged `ProjectReference`.

- [ ] **Step 6: Commit**

```bash
git add Directory.Build.props src tests examples
git commit -m "build: multi-target StreamBus for net10.0 and net11.0"
```

---

### Task 3: CI and package-level net10 smoke test

**Files:**
- Modify: `.github/workflows/build.yml`, `.github/workflows/release.yml`

**Interfaces:**
- Consumes: two assets from Task 2.
- Produces: CI that tests both TFMs and verifies a net10 consumer can restore the packed `.nupkg`.

- [ ] **Step 1: Install both SDKs in `build.yml`**

Replace each `dotnet-version: 10.0.x` with a block listing `10.0.x` and `11.0.x` (both the build job and the integration-tests job).

- [ ] **Step 2: Run unit and integration tests per TFM in `build.yml`**

In the build job run the unit-test step twice with `--framework net10.0` and `--framework net11.0`; in the integration job do the same for `tests/StreamBus.IntegrationTests/StreamBus.IntegrationTests.csproj`. Keep the TRX upload steps and artifact names distinct per TFM.

- [ ] **Step 3: Assert both assets in the pack guard**

Extend the existing pack step (after the one-nupkg/one-snupkg check) to assert the package contains both DLLs:

```bash
unzip -l ./artifacts/*.nupkg | grep -q 'lib/net10.0/StreamBus.dll'
unzip -l ./artifacts/*.nupkg | grep -q 'lib/net11.0/StreamBus.dll'
```

- [ ] **Step 4: Add the net10 package-consumption smoke step to `build.yml`**

After pack, derive the version from the produced filename (`StreamBus.<version>.nupkg`), create a throwaway `net10.0` console project outside the solution, add `PackageReference Include="StreamBus" Version="<version>"` with `--source` pointing at `./artifacts`, and run it with a program that constructs `new StreamBusHostOptions()` and prints `WaitUntilStarted`. Expected: restore resolves `lib/net10.0`, build and run PASS. Do not convert the examples to `PackageReference`.

- [ ] **Step 5: Update `release.yml`**

Install both SDKs as in Step 1; change the test step to `dotnet test StreamBus.slnx --no-build --configuration Release -p:Version=... -p:PackageVersion=...` so every test project runs both TFMs. Keep version resolution, OIDC publishing, the Production gate, and the packaged-version guard unchanged.

- [ ] **Step 6: Verify the local equivalents of the new CI steps**

Run `dotnet pack StreamBus.slnx --no-build --configuration Release --output ./artifacts`, then the Step 3 `unzip -l` checks and the Step 4 smoke project against the packed file. Expected: all PASS. Remove the throwaway smoke project afterwards.

- [ ] **Step 7: Commit**

```bash
git add .github/workflows/build.yml .github/workflows/release.yml
git commit -m "ci: test both TFMs and verify packed net10 asset"
```

---

### Task 4: Record the .NET 11 behavioral-change audit

**Files:**
- Modify: this plan file (append an "Audit results" section at the end)

**Interfaces:**
- Consumes: both TFM build/test legs from Tasks 2–3.

- [ ] **Step 1: Run the both-TFM checks that pin numeric/wire behavior**

Run `dotnet test tests/StreamBus.UnitTests/StreamBus.UnitTests.csproj --no-build --configuration Release --framework net11.0` and inspect that `WireEncodingTests` passes unchanged. Expected: PASS (no golden-value drift).

- [ ] **Step 2: Inspect the applicable audit areas**

Read `Hosting.cs` scope-disposal and fire-and-forget task paths and `Connector.cs`/`StreamWire.cs` for `double`/`decimal` or `DateOnly`/`TimeOnly` usage. Record one line per audit bullet from the spec's ".NET 11 behavioral-change audit" with PASS / N/A and evidence.

- [ ] **Step 3: Run integration tests on both TFMs where Docker is available**

Run the integration project with `--framework net10.0` and `--framework net11.0`. If Docker is unavailable locally, record that CI owns this check. Expected: PASS or explicitly deferred to CI with reason.

- [ ] **Step 4: Commit**

```bash
git add docs/superpowers/plans/2026-11-19-dotnet-11-upgrade.md
git commit -m "docs: record .NET 11 behavioral change audit"
```

---

### Task 5: README and docs support matrix

**Files:**
- Modify: `README.md`, `docs/astro.config.mjs`
- Create: `docs/src/content/docs/configuration/framework-support.mdx`

**Interfaces:**
- Consumes: final TFM layout from Task 2.

- [ ] **Step 1: Update README**

Change the badge to `.NET 10 | 11`, replace the hard-coded install version with `dotnet add package StreamBus` (resolves the latest stable release at install time and pins it in the consumer's project file), and add a link to the framework-support page in the Documentation list. Apply the same install-snippet change to `get-started.mdx`.

- [ ] **Step 2: Add the framework-support page**

Create `docs/src/content/docs/configuration/framework-support.mdx` with: a table mapping app TFM → resolved asset (`net10.0` → `lib/net10.0`, `net11.0` → `lib/net11.0`), the rule that one package serves both with no app changes, the runtime-async ownership rule (consumer opt-in affects only their project; the net11 StreamBus asset decides its own codegen), and the .NET 11 hardware baseline note.

- [ ] **Step 3: Add the sidebar entry**

In `docs/astro.config.mjs` add `{ slug: 'configuration/framework-support' }` to the Configuration items.

- [ ] **Step 4: Verify the docs build**

Run: `cd docs && pnpm install --frozen-lockfile && pnpm build` (or `pnpm build` if already installed).
Expected: PASS with the new page in the output.

- [ ] **Step 5: Commit**

```bash
git add README.md docs/astro.config.mjs docs/src/content/docs/configuration/framework-support.mdx
git commit -m "docs: document net10/net11 support matrix"
```

---

### Task 6: Enable runtime async for the net11 asset

**Files:**
- Modify: `src/StreamBus/StreamBus.csproj`
- Test: `tests/StreamBus.UnitTests/RuntimeAsyncCodegenTests.cs`

**Interfaces:**
- Consumes: multi-target setup from Task 2.
- Produces: net11 asset built with runtime async; net10 asset unchanged.

- [ ] **Step 1: Add the TFM-conditional flag**

In `src/StreamBus/StreamBus.csproj` add:

```xml
<Features Condition="'$(TargetFramework)' == 'net11.0'">$(Features);runtime-async=on</Features>
```

Do not add `EnablePreviewFeatures` and do not set `UseRuntimeAsync`.

- [ ] **Step 2: Write the failing codegen test**

Create `tests/StreamBus.UnitTests/RuntimeAsyncCodegenTests.cs` with a single `#if NET11_0`-guarded xunit fact named `Net11Asset_is_compiled_with_runtime_async` that reflects `typeof(Bus).GetMethod(nameof(Bus.PublishRawAsync))` and asserts its custom attributes contain no `AsyncStateMachineAttribute`. The test must not compile into the net10 test assembly.

- [ ] **Step 3: Run the test on net11 and net10**

Run: `dotnet test tests/StreamBus.UnitTests/StreamBus.UnitTests.csproj --configuration Release --framework net11.0 --filter Net11Asset_is_compiled_with_runtime_async`, then the same with `--framework net10.0`.
Expected: net11 PASS (async methods lack the attribute); net10 reports no matching test (the guard excluded it) and the rest of the suite passes.

- [ ] **Step 4: Verify the consumer configurations locally**

In a temp directory outside the repo, build two `net11.0` console projects that reference the packed package: one with `<Features>$(Features);runtime-async=on</Features>`, one with no flag. Each calls an async StreamBus API and exits. Expected: both build and run without exceptions; record the results in the commit message.

- [ ] **Step 5: Re-run the full both-TFM suites**

Run unit and integration tests per TFM as in Task 3 Step 2. Expected: all PASS. If any failure is attributable to runtime async, remove the `Features` line, rebuild clean, and record the rollback in the commit message instead of enabling it.

- [ ] **Step 6: Commit**

```bash
git add src/StreamBus/StreamBus.csproj tests/StreamBus.UnitTests/RuntimeAsyncCodegenTests.cs
git commit -m "feat: enable runtime async for the net11 asset"
```

---

## Audit results

Recorded 2026-11-19 (net11 unit suite and code inspection; integration tests deferred, see last line):

- Runtime async: enabled for the net11 asset only in Task 6; net10 asset unchanged. Single switch is `Features` (verified against SDK 11.0.100), rollback is removing the flag and rebuilding clean.
- `Microsoft.Extensions.*` in shared framework: central versions are now conditional — net10 resolves 10.0.15, net11 resolves 11.0.0 (verified in `project.assets.json`). Packed nuspec has per-TFM dependency groups (net10: 10.0.15 for all three; net11: 11.0.0 for Hosting and HealthChecks, with Logging.Abstractions correctly omitted because the .NET 11 shared framework provides it). No restore/build/test regression on either TFM.
- `VersionPrefix = 1.0.0` for local builds only; a net10 consumer restored `StreamBus.1.0.0` from a local feed and ran successfully. Published versions remain tag-driven via `release.yml`.
- DI scope disposal aggregates exceptions: PASS — scope disposal sites (`Bus.cs`, `StreamPartitionConsumer.cs`, `FilterRegistrationSupport.cs`) use `using`/`await using` with no catch matching a specific disposal exception type, so aggregation cannot change StreamBus behavior.
- `IHost.RunAsync`/`StopAsync` throwing on `BackgroundService` failure: N/A to StreamBus's `IHostedService`; fire-and-forget supervisor/topology tasks in `Hosting.cs` already attach `ContinueWith(OnlyOnFaulted)` handlers that log faults — behavior unchanged on net11 (551/551 unit tests pass).
- Numeric/precision changes: PASS — `WireEncodingTests` 10/10 pass on net11; production code contains no `decimal` casts (only `double` for metrics histograms and backoff math) and no `DateOnly`/`TimeOnly`.
- Minimum hardware baseline: .NET 11 raises the CPU baseline to `x86-64-v2` (and `LSE` on Arm64 Windows); .NET 10 apps are unaffected.
- Not applicable (with reason): ASP.NET Core/Kestrel/OpenAPI/response compression, HTTP semconv tags (no ASP.NET dependency); EF Core/SqlClient (no EF dependency); chained configuration (code-only config, no settings binder); SignalR/gRPC/Blazor/MAUI/union types/`Tensor<T>` (unused).
- AsyncLocal / stack traces (probed after final review, SDK 11.0.100, net11 consumer apps compiled with runtime async on and off): `AsyncLocal<string>` set inside an async method does not flow back to the caller after `await` in either mode, i.e. behavior is consistent. Live `StackTrace()` with the flag on shows the real method name (`<<Main>$>g__Chain`), with it off shows `MoveNext`/state-machine frames. Exception stack traces point to the same source line in both modes; the top frame's displayed method name differs (real method vs generated state-machine type).
- Integration tests on both TFMs: deferred to CI — Docker daemon is unavailable on this Windows host (`docker info` cannot connect to the Linux engine). CI's `integration-tests` job runs both TFM legs.
