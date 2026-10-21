# StreamBus CI & NuGet Publishing Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add GitHub Actions that build, test (unit + containerized-broker integration), pack, and publish `StreamBus` to nuget.org, plus the package metadata and license that publishing requires.

**Architecture:** Two workflows. `build.yml` is the gate (PR / push to `main` / manual): build + unit tests + `dotnet pack` in one job, broker-backed integration tests in a second job that owns a `rabbitmq:4-management` service container. `release.yml` is the publisher: a published GitHub Release (or manual dispatch) resolves the version from the tag, packs with that version, and pushes to nuget.org over OIDC Trusted Publishing. Packaging metadata lives in `src/StreamBus/StreamBus.csproj`; every other project is excluded from packing by a repository-wide `IsPackable=false` default in `Directory.Build.props`.

**Tech Stack:** .NET 10 SDK, `StreamBus.slnx`, xUnit v3 (classic VSTest mode), GitHub Actions, RabbitMQ 4 (`rabbitmq:4-management` service container), NuGet Trusted Publishing (OIDC).

**Spec:** `docs/superpowers/specs/2026-10-21-streambus-ci-github-actions-design.md`

## Global Constraints

- Solution file is `StreamBus.slnx`; every `dotnet` command targets it or a project inside it.
- Classic test runner only: `Directory.Build.props` sets `IsTestingPlatformApplication=false`. Use `dotnet test <path>`, never `dotnet test --solution` (MTP syntax, not enabled here).
- `TreatWarningsAsErrors=true` repo-wide: the build must stay warning-free. Do not enable `GenerateDocumentationFile` (it would turn undocumented public members into CS1591 errors).
- Broker contract for integration tests: stream port `5552`, AMQP `5672`, management `15672`; non-`guest` user `app` / password `password` (`STREAMBUS_USER` / `STREAMBUS_PASSWORD`).
- The broker must advertise `localhost` as its host, or the stream client cannot connect from a runner.
- `PackageId` is `StreamBus`; the version comes from the release tag at publish time.
- Publishing uses OIDC Trusted Publishing only. No API key in repository secrets.
- Action versions pinned to the ones already proven in `niravinfo/NSLabs.EFCore.Extensions`: `actions/checkout@v7`, `actions/setup-dotnet@v6`, `actions/upload-artifact@v7`, `NuGet/login@v1`, `softprops/action-gh-release@v3`.

## Review Focus

- Broker advertised host: a containerized broker advertises its container hostname; without `-rabbitmq_stream advertised_host localhost` every integration test fails with a connection error that looks like a client bug.
- `guest` credentials: the stream client rejects `guest` outright; the container must create the `app` user.
- Stream plugin not enabled: the bundled `rabbitmq_stream` plugin is off by default in the official image, so the listener on `5552` never appears.
- Accidental publish: a green `main` build must never push to nuget.org; only a Release event or an explicit dispatch may publish.
- Version drift: the version in the tag and the version inside the `.nupkg` must be identical, or the published package is untraceable to its release.

---

### Task 1: Branch, LICENSE, and package metadata

**Files:**
- Create: `LICENSE`
- Modify: `src/StreamBus/StreamBus.csproj`
- Modify: `Directory.Build.props`

**Interfaces:**
- Consumes: nothing (first task).
- Produces: a packable `src/StreamBus` project producing `StreamBus.<Version>.nupkg` with the metadata listed in the spec; every other project excluded from packing.

- [ ] **Step 1: Create the branch from `origin/main`**

```bash
git fetch origin
git switch -c feature/ci-github-actions origin/main
```

Expected: `git branch --show-current` prints `feature/ci-github-actions`, and `git log --oneline -1` shows `5c7dbf2` (the current `origin/main` head).

- [ ] **Step 2: Add the MIT `LICENSE` file**

Create `LICENSE` at the repository root with the standard MIT text, copyright line `Copyright (c) 2026 Nirav Patel`.

- [ ] **Step 3: Exclude every project from packing by default**

Add to the `<PropertyGroup>` in `Directory.Build.props`:

```xml
<!-- Only src/StreamBus opts back in; examples and tests must never produce a package. -->
<IsPackable>false</IsPackable>
```

- [ ] **Step 4: Give `src/StreamBus` its package metadata**

Add to `src/StreamBus/StreamBus.csproj`:

```xml
<PropertyGroup>
  <RootNamespace>StreamBus</RootNamespace>
  <AssemblyName>StreamBus</AssemblyName>
  <IsPackable>true</IsPackable>
  <PackageId>StreamBus</PackageId>
  <Version>0.2.0</Version>
  <Authors>Nirav Patel</Authors>
  <Description>MassTransit-style client for RabbitMQ streams on .NET 10.</Description>
  <PackageLicenseExpression>MIT</PackageLicenseExpression>
  <PackageReadmeFile>README.md</PackageReadmeFile>
  <PackageTags>rabbitmq;rabbitmq-streams;streams;messaging;message-bus;dotnet</PackageTags>
  <RepositoryUrl>https://github.com/niravinfo/StreamBus</RepositoryUrl>
  <RepositoryType>git</RepositoryType>
  <PackageProjectUrl>https://github.com/niravinfo/StreamBus</PackageProjectUrl>
  <IncludeSymbols>true</IncludeSymbols>
  <SymbolPackageFormat>snupkg</SymbolPackageFormat>
  <PublishRepositoryUrl>true</PublishRepositoryUrl>
</PropertyGroup>
<ItemGroup>
  <None Include="..\..\README.md" Pack="true" PackagePath="\" />
</ItemGroup>
```

- [ ] **Step 5: Verify the build still passes and exactly one package is produced**

Run: `dotnet build StreamBus.slnx -c Release`
Expected: `Build succeeded. 0 Warning(s) 0 Error(s)`.

Run: `dotnet pack StreamBus.slnx -c Release --output ./artifacts`
Expected: exactly one `StreamBus.0.2.0.nupkg` and one `StreamBus.0.2.0.snupkg` in `./artifacts`; no package for the examples or the test projects.

- [ ] **Step 6: Verify the package metadata**

Run: `dotnet nuget` is not needed - unzip the nuspec instead:
`Expand-Archive ./artifacts/StreamBus.0.2.0.nupkg -DestinationPath ./artifacts/unpacked -Force` then read `./artifacts/unpacked/StreamBus.nuspec`.
Expected: `<id>StreamBus</id>`, `<version>0.2.0</version>`, `<license type="expression">MIT</license>`, `<readme>README.md</readme>`, `repository url="https://github.com/niravinfo/StreamBus"`, and `README.md` present at the package root.

- [ ] **Step 7: Commit**

```bash
git add LICENSE Directory.Build.props src/StreamBus/StreamBus.csproj
git commit -m "build: add MIT license and NuGet package metadata"
```

---

### Task 2: Prove the CI broker configuration against a real container

**Files:**
- None tracked (validation only). The exact container definition is copied into `.github/workflows/build.yml` in Task 3.

**Interfaces:**
- Consumes: the container definition in the spec ("Broker container" section).
- Produces: evidence that `rabbitmq:4.2-management` with the specified env/ports serves the stream protocol, and that the integration suite is green against it.

> Execution note (2026-10-21): the plan originally said to stop the local
> `RabbitMQ` Windows service to free ports `5552`/`5672`. The session has no
> administrator rights, and the service's data directory belongs to
> `LocalSystem`, so stopping it would have left the developer without a broker.
> The validation instead runs the test suite in a container that shares the
> broker container's network namespace
> (`docker run --network container:<broker>`), which exercises the same broker
> without touching the host ports.

- [ ] **Step 1: Start Docker Desktop and confirm the daemon is up**

Run: `Start-Process "C:\Program Files\Docker\Docker\Docker Desktop.exe"` then poll `docker version --format '{{.Server.Version}}'` until it prints a version.
Expected: a version string within ~2 minutes.

- [ ] **Step 2: Start the container with exactly the CI configuration**

```bash
docker run -d --name rb42 \
  -p 15672:15672 \
  -e RABBITMQ_DEFAULT_USER=app \
  -e RABBITMQ_DEFAULT_PASS=password \
  -e RABBITMQ_SERVER_ADDITIONAL_ERL_ARGS="-rabbitmq_stream advertised_host localhost" \
  rabbitmq:4.2-management
```

Expected: a container id; `docker logs rb42` shows the node booting.

- [ ] **Step 3: Enable the stream plugin and confirm the listener**

```bash
docker exec rb42 rabbitmq-plugins enable rabbitmq_stream
docker exec rb42 rabbitmq-diagnostics -q listeners | grep 5552
docker exec rb42 rabbitmqctl eval 'application:get_env(rabbitmq_stream, advertised_host).'
```
Expected: the `5552` stream listener is listed and the advertised host evaluates to `localhost`.

Note: `-rabbitmq_stream enable` in `RABBITMQ_SERVER_ADDITIONAL_ERL_ARGS` does **not** work on RabbitMQ 3.7+ (the flag reaches erl, the release handler ignores it), so the plugin is enabled on the running node.

- [ ] **Step 4: Run the integration suite against the container**

Run: `docker run --rm --network container:rb42 -v "<repo>:/src" -w /src mcr.microsoft.com/dotnet/sdk:10.0 bash -lc "dotnet test tests/StreamBus.IntegrationTests/StreamBus.IntegrationTests.csproj -c Release"`
Expected: all integration tests pass, and `TopologyIntegrationTests.Policy_applied_tuning_is_effective` **runs** rather than skips (the management API is reachable at `15672` with `app`/`password`).

- [ ] **Step 5: Tear down**

```bash
docker rm -f rb42
```
Expected: the container is gone; the developer's own broker was never touched.


---

### Task 3: The build workflow

**Files:**
- Create: `.github/workflows/build.yml`

**Interfaces:**
- Consumes: the solution path `StreamBus.slnx`; the test project paths; the container definition validated in Task 2.
- Produces: a `Build` workflow named `build` running on `pull_request`, `push` to `main`, and `workflow_dispatch`, with jobs `build` and `integration-test`.

- [ ] **Step 1: Write `.github/workflows/build.yml`**

The file must declare:

- `name: Build`; `on:` `pull_request`, `push: branches: [main]`, `workflow_dispatch`.
- `permissions: contents: read`.
- `concurrency: group: build-${{ github.ref }}`, `cancel-in-progress: true`.
- Job `build` on `ubuntu-latest`, `timeout-minutes: 20`: checkout (`actions/checkout@v7`), setup .NET (`actions/setup-dotnet@v6`, `dotnet-version: 10.0.x`), `dotnet restore StreamBus.slnx`, `dotnet build StreamBus.slnx --no-restore -c Release`, `dotnet test tests/StreamBus.UnitTests/StreamBus.UnitTests.csproj --no-build -c Release`, `dotnet pack StreamBus.slnx --no-build -c Release --output ./artifacts`, `ls -lh ./artifacts`, `actions/upload-artifact@v7` with `name: nuget-packages`, `path: ./artifacts/*.nupkg`, `if-no-files-found: error`.
- Job `integration-test` on `ubuntu-latest`, `timeout-minutes: 20`, `needs: build`, with the service container from Task 2 (`image: rabbitmq:4.2-management`, ports `5552:5552`, `5672:5672`, `15672:15672`, the three env vars, `options: --health-cmd "rabbitmq-diagnostics -q ping" --health-interval 10s --health-timeout 5s --health-retries 12`), then checkout, setup .NET, restore, build, a step that enables the stream plugin on the running node (`docker exec ${{ job.services.rabbitmq.id }} rabbitmq-plugins enable rabbitmq_stream`) and a readiness step that waits for the `5552` listener, `dotnet test tests/StreamBus.IntegrationTests/StreamBus.IntegrationTests.csproj --no-build -c Release` with `STREAMBUS_USER: app` / `STREAMBUS_PASSWORD: password` in `env:`, and `actions/upload-artifact@v7` uploading the TRX report (`path: ./artifacts/test-results/*.trx`, `if-no-files-found: warn`).

Use `--logger "trx;LogFileName=integration.trx"` on the integration `dotnet test` invocation and `--results-directory ./artifacts/test-results`.

- [ ] **Step 2: Validate the YAML parses and the structure is what was specified**

Run: `python -c "import yaml,sys; d=yaml.safe_load(open('.github/workflows/build.yml')); print(list(d['jobs']))"`
Expected: `['build', 'integration-test']`.

- [ ] **Step 3: Verify the commands the workflow runs actually work from a clean output**

Run, in order:
`dotnet restore StreamBus.slnx` -> `dotnet build StreamBus.slnx --no-restore -c Release` -> `dotnet test tests/StreamBus.UnitTests/StreamBus.UnitTests.csproj --no-build -c Release` -> `dotnet test tests/StreamBus.IntegrationTests/StreamBus.IntegrationTests.csproj --no-build -c Release --logger "trx;LogFileName=integration.trx" --results-directory ./artifacts/test-results` -> `dotnet pack StreamBus.slnx --no-build -c Release --output ./artifacts`
Expected: build succeeds; unit tests pass; integration tests pass (broker from Step 2 of Task 2, or the restored local broker); `artifacts/test-results/integration.trx` exists; `./artifacts` holds exactly one `.nupkg` and one `.snupkg`.

- [ ] **Step 4: Commit**

```bash
git add .github/workflows/build.yml
git commit -m "ci: add build workflow with unit and containerized integration tests"
```

---

### Task 4: The release workflow

**Files:**
- Create: `.github/workflows/release.yml`

**Interfaces:**
- Consumes: the GitHub Release `tag_name` event payload; the `NUGET_USER` secret for `NuGet/login@v1`; the package produced by `dotnet pack`.
- Produces: a `Release` workflow named `release` that publishes `StreamBus.<version>` to nuget.org and attaches the packages to the GitHub Release.

- [ ] **Step 1: Write `.github/workflows/release.yml`**

The file must declare:

- `name: Release`; `on: release: types: [published]` and `workflow_dispatch` with a `version` string input.
- `permissions: id-token: write` and `contents: write`.
- Job `publish` on `ubuntu-latest`, `timeout-minutes: 20`:
  - checkout, setup .NET (`10.0.x`).
  - A `Determine package version` step (`id: version`) that takes `${{ github.event.release.tag_name }}` for a Release event, `${{ inputs.version }}` for a dispatch, else `$GITHUB_REF_NAME`; strips a leading `v` and a `refs/tags/` prefix; fails when the result is empty; warns (does not fail) when it is not SemVer; exports `VERSION` to `$GITHUB_ENV` and `version` to `$GITHUB_OUTPUT`.
  - `dotnet restore StreamBus.slnx`.
  - `dotnet build StreamBus.slnx --no-restore -c Release -p:ContinuousIntegrationBuild=true -p:Version=${{ env.VERSION }} -p:PackageVersion=${{ env.VERSION }}`.
  - `dotnet test StreamBus.slnx --no-build -c Release -p:Version=${{ env.VERSION }}` (both suites).
  - `dotnet pack StreamBus.slnx --no-build -c Release --output ./artifacts -p:ContinuousIntegrationBuild=true -p:PackageVersion=${{ env.VERSION }} -p:Version=${{ env.VERSION }}`, then `ls -lh ./artifacts/`.
  - `NuGet/login@v1` with `user: ${{ secrets.NUGET_USER }}` (`id: login`).
  - `dotnet nuget push ./artifacts/*.nupkg --api-key ${{ steps.login.outputs.NUGET_API_KEY }} --source https://api.nuget.org/v3/index.json --skip-duplicate`.
  - `softprops/action-gh-release@v3` attaching `./artifacts/*.nupkg` and `./artifacts/*.snupkg`, `fail_on_unmatched_files: false`, only for the `release` event.
  - `actions/upload-artifact@v7` (`nuget-packages-${{ env.VERSION }}`) for `workflow_dispatch` runs.

- [ ] **Step 2: Validate the YAML parses**

Run: `python -c "import yaml; d=yaml.safe_load(open('.github/workflows/release.yml')); print(list(d['jobs']))"`
Expected: `['publish']`.

- [ ] **Step 3: Verify version resolution and packing with the release flags**

Run: `dotnet pack StreamBus.slnx -c Release --output ./artifacts-rel -p:ContinuousIntegrationBuild=true -p:Version=1.2.3 -p:PackageVersion=1.2.3`
Expected: `artifacts-rel/StreamBus.1.2.3.nupkg` and `StreamBus.1.2.3.snupkg`; the nuspec inside reports `<version>1.2.3</version>`.

- [ ] **Step 4: Commit**

```bash
git add .github/workflows/release.yml
git commit -m "ci: add release workflow publishing to NuGet via trusted publishing"
```

---

### Task 5: Badges and the release runbook

**Files:**
- Modify: `README.md`
- Create: `docs/guide/release.md`

**Interfaces:**
- Consumes: the workflow names `build.yml` and `release.yml` and the repository slug `niravinfo/StreamBus`.
- Produces: a visible CI status badge and a written procedure for the one-time nuget.org setup and every release.

- [ ] **Step 1: Add badges to the top of `README.md`**

Insert directly under the `# StreamBus` heading:

```markdown
[![Build](https://github.com/niravinfo/StreamBus/actions/workflows/build.yml/badge.svg)](https://github.com/niravinfo/StreamBus/actions/workflows/build.yml)
[![NuGet](https://img.shields.io/nuget/v/StreamBus.svg)](https://www.nuget.org/packages/StreamBus)
```

- [ ] **Step 2: Write `docs/guide/release.md`**

The document must cover: the one-time nuget.org setup (claim the `StreamBus` id, add the Trusted Publisher for `niravinfo/StreamBus` / `release.yml`, note that `NUGET_USER` must be a nuget.org account name that owns the id), the release procedure (create a GitHub Release with tag `vX.Y.Z`, wait for the `Release` workflow, verify on nuget.org), the manual-dispatch escape hatch (`workflow_dispatch` with a `version` input), and what the CI gate covers (build, unit tests, containerized integration tests, pack verification) and what it does not (no code coverage gate, no nightly run).

- [ ] **Step 3: Verify the docs render and the links resolve**

Run: `Select-String -Path README.md -Pattern "actions/workflows/build.yml"` returns the badge line.
Expected: the badge markdown is present above the first paragraph.

- [ ] **Step 4: Commit**

```bash
git add README.md docs/guide/release.md
git commit -m "docs: add CI badge and NuGet release runbook"
```

---

### Task 6: Push the branch and review the first CI run

**Files:**
- None.

**Interfaces:**
- Consumes: every commit from Tasks 1-5 on `feature/ci-github-actions`.
- Produces: a pushed branch whose `build.yml` run is green, and a PR-ready branch.

- [ ] **Step 1: Push the branch**

Run: `git push -u origin feature/ci-github-actions`
Expected: the remote branch is created.

- [ ] **Step 2: Watch the workflow**

Run: `gh run list --workflow build.yml --branch feature/ci-github-actions --limit 3`
Expected: one run per push, status `success` for the `build` job and the `integration-test` job.

- [ ] **Step 3: Report the outcome**

State plainly which jobs passed, how long the integration job took, and that `main` still needs the branch-protection rule (required checks: `build`, `integration-test`) plus the nuget.org Trusted Publisher before the first release.
