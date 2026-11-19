# Design: .NET 11 upgrade with continued .NET 10 support

Date: 2026-11-19
Status: proposed — awaiting approval

## Intent and why

Issue [niravinfo/notes#34](https://github.com/niravinfo/notes/issues/34) asks to upgrade StreamBus to .NET 11, add a `global.json` with a minimum SDK version, and move the .NET pipelines to .NET 11. Issue [#33](https://github.com/niravinfo/notes/issues/33) is the wider roadmap; this document covers only the upgrade item. The constraint from the user is that **existing .NET 10 consumers must keep working** — StreamBus is a published NuGet library (`v0.5.1`) with users we do not control.

The resolution is to ship **one NuGet package with two assets**, `net10.0` and `net11.0`, built with the .NET 11 SDK. A .NET 10 app resolves the net10 asset and sees no change; a .NET 11 app resolves the net11 asset and can benefit from .NET 11 features. Runtime async is adopted in a later, isolated step because it changes observable behavior.

## Goals

1. Package contains `lib/net10.0/StreamBus.dll` and `lib/net11.0/StreamBus.dll`; both are built, tested, and packed from one version.
2. .NET 10 consumers continue to build and run against the net10 asset with zero code changes — including the examples, which stay on net10 as a living compatibility check.
3. Pin a minimum .NET 11 SDK via `global.json`; all .NET CI jobs install both SDK 10 and SDK 11.
4. Both assets have an **identical public API surface**; only implementation may differ per TFM.
5. Runtime async is enabled for the net11 asset only, as a separately verifiable phase after the multi-target build is green.
6. Docs and README state the support matrix and the runtime-async ownership rule.
7. Audit the applicable .NET 11 behavioral changes from [notes#35](https://github.com/niravinfo/notes/issues/35) and record the outcome.

## Non-goals

- No .NET 11-only public APIs and no public members behind `#if`.
- No dropping net10, no major version bump, no separate net10 maintenance branch — adding an asset is additive and non-breaking.
- No Docusaurus migration, no replay-code work, no scoped-filter refactor (separate roadmap items).
- No AOT/trimming support (already explicitly unsupported in `StreamBus.csproj`).
- No migration of docs CI (`docs.yml` is Node-only and stays as-is).

## Anti-goals (what would make this a failure even if it builds)

- A net10 consumer being forced to install a new SDK or change their TFM.
- The two assets exposing different public types/members.
- Turning runtime async on in the same change that adds the second TFM, making failures impossible to attribute.
- Blanket-suppressing new analyzer warnings just to get a green build under SDK 11.

## Constraints (current repository facts)

- `Directory.Build.props` sets a single global `<TargetFramework>net10.0</TargetFramework>`, `TreatWarningsAsErrors=true`, `AnalysisLevel=latest-recommended`, `IsTestingPlatformApplication=false` (a documented xunit.v3/.NET 10 SDK workaround), and per-project `NoWarn` rules.
- `Directory.Packages.props` uses central package management with transitive pinning; `Microsoft.Extensions.*` are `10.0.15`, `RabbitMQ.Stream.Client` is `1.13.0`, `RabbitMQ.Client` is `7.2.2`, `Testcontainers.RabbitMq` is `4.17.0`.
- Projects inheriting the TFM: `src/StreamBus`, `tests/StreamBus.UnitTests`, `tests/StreamBus.IntegrationTests` (Testcontainers), `tests/StreamBus.DocsSnippets` (compiles documentation samples), and the two `examples/` apps.
- `build.yml` (build/unit/pack), `build.yml` (integration), and `release.yml` install only `dotnet-version: 10.0.x`. Release version comes from the git tag via `release.yml`, not `VersionPrefix`.
- The development machine already has SDK `11.0.100` installed (alongside `10.0.404`), so the SDK pin can be validated locally without installing anything.
- `build.yml` asserts exactly one `.nupkg` and one `.snupkg` — multi-targeting still produces one of each.
- StreamBus is a library with no ASP.NET Core, EF Core, or configuration-binder dependency; it uses `IHostedService` (not `BackgroundService`), `IServiceScopeFactory`, `System.Diagnostics.Metrics`, and reflection-based `System.Text.Json`.

## Approaches considered

### A. Multi-target the package: `net10.0;net11.0`, one NuGet package (chosen)

One package, two compiled assets selected by the consumer's TFM through NuGet's nearest-framework rule. Runtime async can be conditioned on `net11.0`; the net10 asset remains byte-for-byte the current behavior.

Trade-offs: CI runs each test project per TFM, two SDKs must be installed, and new SDK 11 analyzers may require targeted suppressions because warnings are errors. In exchange, no consumer is broken and .NET 11 users get a real net11 asset. This is the only option that satisfies both the upgrade request and the compatibility constraint.

### B. Keep `net10.0`, upgrade only SDK/CI and add `global.json`

A net10 library is already consumable by net11 apps, so this is the cheapest, lowest-risk path and breaks nobody. But there is no net11 asset, so no runtime async or net11-specific improvements — it is a tooling upgrade rather than the library upgrade issue #34 asks for. Useful as the first phase of A, not as the endpoint.

### C. Move entirely to `net11.0`

Cleanest codebase and immediate access to net11 features, but it breaks every .NET 10 consumer, requires a major version bump and migration guide, and contradicts the constraint the user set.

### Decision

Approach A, delivered in three phases so the risky part is isolated:

- **Phase 1 — toolchain and multi-target.** SDK 11 + `global.json` + CI, add the `net11.0` asset with runtime async **off**, green build/tests/pack on both TFMs. This is option B plus the second asset.
- **Phase 2 — spike runtime async.** Enable it on the net11 asset only in a throwaway branch, confirm the codegen change via `AsyncStateMachineAttribute` reflection, run the full unit + integration suites, and verify consumer configurations (app with runtime async on and off), `AsyncLocal`/`ExecutionContext` flow, and stack traces. Measure allocation/throughput if practical.
- **Phase 3 — adopt runtime async** for the net11 asset if the spike is clean; rollback is removing the `Features` flag and rebuilding clean. Net10 asset unchanged.

## Detailed design

### Project TFM layout

Set `<TargetFrameworks>net10.0;net11.0</TargetFrameworks>` once in `Directory.Build.props` as the default for every project. The two example projects opt out by setting `<TargetFramework>net10.0</TargetFramework>` and clearing `<TargetFrameworks></TargetFrameworks>` (both are needed — a non-empty `TargetFrameworks` wins the outer build even when `TargetFramework` is set):

| Project | TFMs | Reason |
| --- | --- | --- |
| `src/StreamBus` | `net10.0;net11.0` | The shipped package. |
| `tests/StreamBus.UnitTests` | `net10.0;net11.0` | Exercises each asset; existing reflection-based API-surface tests catch public drift. |
| `tests/StreamBus.IntegrationTests` | `net10.0;net11.0` | Testcontainers suite validates real broker behavior for both assets. |
| `tests/StreamBus.DocsSnippets` | `net10.0;net11.0` | Documentation samples must compile against both assets. |
| `examples/StreamBus.Examples.Producer` and `Consumer` | `net10.0` only | Intentional canary proving an unchanged net10 consumer still builds. |

**Author's call on the canary:** keep the examples' existing `ProjectReference` to `src/StreamBus` — that is the correct developer experience and it exercises TFM selection. A `ProjectReference` does not exercise NuGet dependency-group/asset resolution, so phase 1 also adds a small **pack-time smoke test**: a minimal `net10.0` project restored from the local `./artifacts` feed at the packed version, which resolves the net10 asset and runs. Do not convert the examples to `PackageReference`; the smoke test is the package-level check.

### SDK pin and CI

- Add root `global.json` pinning the GA .NET 11 SDK baseline (e.g. `11.0.100`) with `rollForward: latestFeature`, so local builds and CI use at least .NET 11 but can take newer feature bands.
- `build.yml`: `actions/setup-dotnet` installs both `10.0.x` and `11.0.x`; restore/build/pack once, then run unit tests and integration tests once per TFM (`dotnet test -f net10.0`, `-f net11.0`). The pack guard keeps "exactly one nupkg and one snupkg" and additionally asserts both `lib/net10.0/StreamBus.dll` and `lib/net11.0/StreamBus.dll` are present in the package.
- `release.yml`: install both SDKs; build/test/pack with the tag-derived version as today; the single `.nupkg` now carries both assets. Keep version resolution, OIDC publishing, and the Production environment gate unchanged.
- `docs.yml`: unchanged.
- Re-validate `IsTestingPlatformApplication=false` under SDK 11: keep it if xunit.v3's MTP targets still require it, remove it with an explanatory comment if not.

### Dependencies and analyzer hygiene

- Keep `Microsoft.Extensions.*` at `10.0.15` for both TFMs in phase 1 — net11 can consume net10 packages, and moving to 11.0.x is not required to compile. If the SDK 11 resolution shows duplicate/shared-framework conflicts, introduce conditional `PackageVersion` items per TFM (a later, documented change, not part of phase 1).
- Keep RabbitMQ package versions unchanged; verify net11 restore succeeds with their existing assets.
- New diagnostics from SDK 11 analyzers under `TreatWarningsAsErrors` are handled with narrowly scoped, commented suppressions in `Directory.Build.props`, never by disabling `TreatWarningsAsErrors` or raising `NoWarn` globally.

### Public API parity

- No public type, member, or attribute may be wrapped in `#if NET11_0`. Conditional compilation is for internal implementation paths only.
- Existing API-surface tests run in both TFM test legs. A net10 consumer and a net11 consumer must see the same `StreamBus` surface.

### Runtime async (phases 2–3 only)

```xml
<Features Condition="'$(TargetFramework)' == 'net11.0'">$(Features);runtime-async=on</Features>
```

- The property lives in `src/StreamBus/StreamBus.csproj`, never in `Directory.Build.props`, so examples and net10 builds cannot pick it up. Append to `$(Features)` rather than replacing it. `EnablePreviewFeatures` is not needed on `net11.0`.
- **There is one switch, not two.** The .NET 11 docs mention `<UseRuntimeAsync>false</UseRuntimeAsync>` as an opt-out, but on the installed GA SDK 11.0.100 that property is not referenced anywhere in the SDK's props/targets; the compiler only reads `Features`. Verified locally by searching the SDK directory. Therefore:
  - Enable by adding `runtime-async=on` to `Features`.
  - Roll back by **removing that flag** (and rebuilding clean), not by setting `UseRuntimeAsync=false`.
  - Phase 2 must confirm the effective codegen by reflection on `AsyncStateMachineAttribute` (runtime-async methods lack it), not by trusting MSBuild properties. The flag may not invalidate MSBuild's up-to-date check, so the spike uses a separate output path or a clean rebuild.
- Consumers decide independently for their own projects; a consumer's setting does not recompile our DLL. Document this asymmetry explicitly: a consumer whose project opts out still gets runtime-async internals in the net11 StreamBus asset, which is why the change gets its own phase and release note.
- The net10 asset can never use runtime async (the feature requires a net11 target; forcing it fails with `SYSLIB5007` on the .NET 10 compiler), so no extra compatibility risk there.

### .NET 11 behavioral-change audit (from notes#35)

Verify and record results in the plan's verification section:

- Runtime async — phases 2–3 above.
- `Microsoft.Extensions.*` moving into the shared framework — watch package resolution/redundancy during the SDK 11 upgrade.
- DI scope disposal aggregates exceptions — StreamBus uses `IServiceScopeFactory`; check catch/logging around scope disposal.
- `IHost.RunAsync/StopAsync` throwing on `BackgroundService` failure — StreamBus's hosted service implements `IHostedService` directly, so the change does not directly apply; still audit the fire-and-forget supervisor/topology tasks in `Hosting.cs` (they already log faulted continuations).
- Numeric/precision changes (`double`→`decimal`, saturating float→int casts, `Math.Round`) — check wire encoding/serialization and golden values in `WireEncodingTests`.
- `DateOnly`/`TimeOnly` `TryParse` behavior — check whether any StreamBus code or docs sample uses it.
- Minimum hardware baseline (x86-64-v2 / Arm64 LSE) — add a short deployment note for users on older hosts.

Not applicable, with reason: ASP.NET Core/Kestrel/OpenAPI/response compression and HTTP semconv tags (no ASP.NET dependency); EF Core/SqlClient (no EF dependency); chained configuration empty strings (code-only configuration, no settings binder); SignalR/gRPC/Blazor/MAUI/union types/`Tensor<T>` (unused).

### Docs and release

- README: badge becomes `.NET 10 | 11`; install snippet's stale `0.4.0-beta.1` is updated to the current stable version; documentation list gains the new framework-support page.
- Add `docs/src/content/docs/configuration/framework-support.mdx` and a sidebar entry in `astro.config.mjs`: support matrix, asset selection rule, runtime-async ownership rule, and the hardware baseline note.
- Release: no major bump. Publish the next minor/patch tag; release notes state that the package now contains a net11 asset and that runtime async is enabled only if phase 3 completed.

## Risks and rollback

| Risk | Mitigation / rollback |
| --- | --- |
| SDK 11 analyzers break the build (warnings are errors) | Targeted, commented suppressions; never disable `TreatWarningsAsErrors`. |
| net11 restore conflict from `Microsoft.Extensions` 10.0.15 / shared framework | Defer to conditional per-TFM `PackageVersion`; phase 1 can still ship with 10.0.15 on both if it restores cleanly. |
| Runtime async changes behavior or breaks RabbitMQ client interop | Phases 2–3 are isolated; rollback is removing `runtime-async=on` from `Features` and rebuilding clean (`UseRuntimeAsync=false` is inert on SDK 11.0.100). |
| Public API drift between assets | `#if` rule plus API-surface tests in both TFM legs. |
| CI time doubles with two test legs | Keep one build/pack; parallelize the two test legs in the matrix. |

## Definition of done

- `global.json` present; clean checkout builds with SDK 11.
- `dotnet build`, both TFM unit legs, both TFM integration legs, docs snippets, and pack succeed in CI.
- Packed `.nupkg` contains both `lib/net10.0/StreamBus.dll` and `lib/net11.0/StreamBus.dll`, one `.snupkg`, and identical public API surfaces.
- Examples still target net10 with their existing `ProjectReference` and build in CI; the pack-time net10 smoke test restores the produced `.nupkg` from a local feed and runs.
- README/docs updated; audit results recorded.
- If phase 3 adopted: spike results (consumer on/off, `AsyncLocal`, stack traces) recorded in the PR.

## Left to the builder

- Exact SDK feature-band/patch value in `global.json` and the corresponding `setup-dotnet` input (choose the GA .NET 11 band available at implementation time).
- Mechanical details of the CI matrix and the `unzip -l` package-content assertion.
- Order of the phases into concrete PRs; each phase must be independently revertable.
