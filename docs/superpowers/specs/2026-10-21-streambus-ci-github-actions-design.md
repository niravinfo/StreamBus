# StreamBus CI & NuGet Publishing Design

Date: 2026-10-21
Status: approved (design decisions taken 2026-10-21)

## Intent and why

StreamBus has no CI. There is no `.github/` directory, no NuGet packaging
metadata, and no LICENSE file, so every change is verified by hand on one
developer's machine and the library cannot be published at all.

This design adds the missing release path: a push-triggered gate that builds,
unit-tests, and broker-backed integration-tests the code, and a tag-driven
release that publishes the package to nuget.org without storing a credential
in the repository.

## Goals

- A build workflow that restores, builds, unit-tests, integration-tests against
  a containerized RabbitMQ, and verifies `dotnet pack` on every pull request and
  every push to `main`.
- A release workflow: a published GitHub Release (or a manual dispatch with an
  explicit version) produces a package whose version comes from the tag, pushes
  it to nuget.org, and attaches the `.nupkg` to that Release.
- Package metadata that satisfies nuget.org's validation rules: license,
  readme, repository URL, tags, symbols.
- An MIT `LICENSE` file at the repository root.

## Non-goals

- No Azure DevOps pipeline. The V2 architecture spec allows the platform to be
  swapped later; GitHub Actions is chosen now because the repository is hosted
  on GitHub.
- No scheduled/nightly job, no code-coverage gate, no benchmark workflow (the
  repository has no benchmark project).
- No change to library or test code, and no change to the test-runner mode:
  `Directory.Build.props` pins `IsTestingPlatformApplication=false` so classic
  `dotnet test` keeps working, and that decision stays.
- No continuous publishing from `main`. Only an explicit Release event or a
  manual dispatch publishes.

## Anti-goals

- No NuGet API key stored in repository secrets. Publishing uses OIDC Trusted
  Publishing only.
- No accidental publish path. A green build on `main` must never push a
  package to nuget.org.

## Constraints (verified against this repository)

| Constraint | Source |
| --- | --- |
| Solution is `StreamBus.slnx` (.NET 10 SDK, SDK 10.0.401 installed) | repo root |
| Classic `dotnet test StreamBus.slnx -c Release --no-build` runs both suites (247 unit + 56 integration green, 2 skipped) | verified locally 2026-10-21 |
| Integration tests need RabbitMQ 4 with the stream plugin: stream `5552`, AMQP `5672`, management `15672` | `tests/StreamBus.IntegrationTests/*.cs` |
| Broker credentials are non-`guest`: `STREAMBUS_USER`/`STREAMBUS_PASSWORD`, defaulting to `app`/`password` | integration tests |
| The stream client connects to the host the **broker advertises**; a container advertises its container hostname, which the runner cannot resolve | `docs/2026-10-08-streambus-design.md` §1.1 |
| `guest` is rejected by the stream client | `docs/2026-10-08-streambus-design.md` §1.1 |
| Two topology tests `Assert.Skip` unless the management API answers at `127.0.0.1:15672` with the same credentials | `tests/StreamBus.IntegrationTests/TopologyIntegrationTests.cs` |
| CI platform is already decided: GitHub Actions with a `rabbitmq:4-management` service container, streams enabled, dedicated user | `docs/superpowers/specs/2026-10-18-streambus-v2-architecture-and-middleware-design.md` §9 (D1), §12.2 |
| `StreamBus` is an unused package id on nuget.org | nuget.org flat-container lookup 2026-10-21 |

## Design

### Branch

`feature/ci-github-actions`, cut from `origin/main`.

### Packaging

`Directory.Build.props` sets `<IsPackable>false</IsPackable>` as the
repository-wide default, so examples and test projects can never be packed by
accident. `src/StreamBus/StreamBus.csproj` opts in and carries the metadata:

| Property | Value |
| --- | --- |
| `PackageId` | `StreamBus` |
| `Version` | `0.2.0` (in-repo default; CI overrides it from the release tag) |
| `Authors` | `Nirav Patel` |
| `Description` | `MassTransit-style client for RabbitMQ streams on .NET 10.` |
| `PackageLicenseExpression` | `MIT` |
| `PackageReadmeFile` | `README.md` (packed at the package root) |
| `RepositoryUrl` / `RepositoryType` | `https://github.com/niravinfo/StreamBus` / `git` |
| `PackageTags` | `rabbitmq`, `rabbitmq-streams`, `streams`, `messaging`, `message-bus`, `dotnet` |
| `IncludeSymbols` / `SymbolPackageFormat` | `true` / `snupkg` |
| `PublishRepositoryUrl` | `true` |

`GenerateDocumentationFile` stays off: `TreatWarningsAsErrors` is on
repo-wide, and enabling it would turn every undocumented public member into
CS1591 build errors.

### `.github/workflows/build.yml`

Triggers: `pull_request` limited to `types: [opened, ready_for_review]`, plus
`workflow_dispatch`. There is no `push` trigger: a PR is built when it is
opened and when it leaves draft, later commits to the PR branch do not start
a run, and merging to `main` starts nothing. Everything else is a manual run.
Permissions: `contents: read`. Concurrency group per ref with
`cancel-in-progress: true`.

Two jobs on `ubuntu-latest`:

1. `build` - restore the solution, build Release, run the unit tests, pack,
   upload the `.nupkg` as an artifact for inspection.
2. `integration-tests` - `needs: build`; starts the RabbitMQ service container,
   waits for the stream listener, runs the integration suite, uploads the TRX.

### `.github/workflows/release.yml`

Triggers: `release: [published]`, plus `workflow_dispatch` with an optional
`version` input. Permissions: `id-token: write` (OIDC), `contents: write`
(attach assets to the Release).

Flow: resolve version from the Release tag (`v` prefix stripped) or the
dispatch input -> restore -> build Release with `-p:Version`/`-p:PackageVersion`
-> test -> pack -> `NuGet/login@v1` for a temporary API key -> `dotnet nuget
push --skip-duplicate` to `https://api.nuget.org/v3/index.json` -> attach the
`.nupkg`/`.snupkg` to the GitHub Release (dispatch runs upload an artifact
instead).

### Broker container

```yaml
image: rabbitmq:4.2-management
ports: 5552:5552, 5672:5672, 15672:15672
env:
  RABBITMQ_DEFAULT_USER: app
  RABBITMQ_DEFAULT_PASS: password
  RABBITMQ_SERVER_ADDITIONAL_ERL_ARGS: "-rabbitmq_stream advertised_host localhost"
```

- `-rabbitmq_stream advertised_host localhost` is mandatory: without it the
  broker advertises its container hostname and the stream client cannot
  connect from the runner. The three-token ER form sets the application
  environment (`application:get_env(rabbitmq_stream, advertised_host)` returns
  `localhost`, verified on the running container).
- The stream plugin is **not** enabled by the image and **cannot** be enabled
  through `RABBITMQ_SERVER_ADDITIONAL_ERL_ARGS`: RabbitMQ 3.7+ ignores the
  `-rabbitmq_stream enable` form (the flag reaches erl but the release handler
  no longer acts on it). The workflow therefore runs
  `docker exec <container> rabbitmq-plugins enable rabbitmq_stream` on the
  running node and then waits for the `5552` listener to appear.
- `RABBITMQ_DEFAULT_USER` creates a non-`guest` administrator, which is also
  what unlocks the two management-API topology tests instead of letting them
  skip.
- Health check: `rabbitmq-diagnostics -q ping`.
- The image is pinned to the `4.2` minor line (resolving to 4.2.9) because
  that is the broker line the library is developed against (4.2.6 locally).
  An unpinned `4` tag floats to the newest 4.x.

**Deviation from the V2 spec (§12.2), deliberate:** the spec says "streams
enabled via mounted config". A committed `rabbitmq.conf` cannot be
bind-mounted into a GitHub Actions service container, because service
containers are created before `actions/checkout` runs, so the file does not
exist at mount time. The ER_LAUNCH arguments plus a runtime plugin enable
produce the same result without the ordering problem.

### Findings from local validation (2026-10-21)

The container was exercised locally with the exact environment above, and the
integration suite was run against it:

- The stream plugin is bundled in the image but disabled; enabling it at
  runtime brings up the `5552` listener.
- The `Policy_applied_tuning_is_effective` integration test failed against the
  broker: it published a policy with `"max-age": 3600` (an integer), and
  RabbitMQ 4.x rejects a numeric `max-age` for streams - an integer crashes
  the broker's policy validation (`badarg` in `rabbit_amqqueue:check_max_age/1`,
  HTTP 500), and a numeric string is rejected as an invalid duration
  (HTTP 400). Stream retention is a duration string, so the payload is now
  `"max-age": "1h"`. The test had never run before: it skips wherever the
  management plugin is unreachable, which is the case on the development
  machine. It is now named
  `Policy_with_stream_retention_is_accepted_and_registered`, because RabbitMQ 4.x
  does not expose a stream's retention in the classic queue view
  (`effective_policy_definition` stays empty) - the observable is the policy
  resource, so the test proves the broker accepted and registered the policy,
  not that the retention was applied to the stream.
- The suite was flaky against the container: the stream client throws
  `StreamNotAvailable` instead of returning false while a stream is still being
  created, so the readiness poll in
  `TopologyIntegrationTests.Nonblocking_startup_declares_in_background` aborted
  on its first try (about one failure in three full runs). The poll now treats
  that response as "not yet" and also waits for the partition count the
  assertion checks. After the fix: six consecutive green runs against the
  containerized broker with `CI=true`.
- The management-API test must not skip in CI: with `CI=true` it now fails
  instead of skipping, so a missing management API cannot hide behind a green
  run. Both workflows wait for the management listener on `15672` before the
  suite starts.

### One-time nuget.org setup (human action, not code)

1. Create/claim the `StreamBus` package id on nuget.org and own it.
2. Add a **Trusted Publisher** for repository `niravinfo/StreamBus`, workflow
   `release.yml` (and the environment, if one is configured). Without this the
   OIDC exchange fails and the publish step cannot authenticate.
3. The `.snupkg` symbol package is pushed by the same `dotnet nuget push`
   command; nuget.org serves symbols from the same feed.

## Verification

- Local: `dotnet build`/`dotnet pack`/`dotnet test` on `StreamBus.slnx`; the
  packed `.nuspec` inspected for the required metadata; both workflow files
  parsed as YAML.
- Local broker proof: run the exact container configuration above and execute
  the integration suite against it, confirming the suite is green against a
  containerized broker and that the two management-API tests execute rather
  than skip.
- Remote: the first push of the branch runs `build.yml` end to end.

## Open items

None. Branch name (`feature/ci-github-actions`), authentication method (OIDC
Trusted Publishing), license (MIT + `LICENSE` file), and trigger scope (PR +
push to `main` + manual, integration tests on every PR) were decided on
2026-10-21.
