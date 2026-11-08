# Testcontainers broker support for the integration lane

## Intent and why

Today a developer (or CI) must start RabbitMQ by hand before running
`StreamBus.IntegrationTests` — CI does it with a GitHub Actions *service
container* plus `docker exec` plugin enabling, and locally you're on your own.
We want the integration suite to own its broker: a RabbitMQ container spun up
by **Testcontainers for .NET** (`Testcontainers.RabbitMq`, latest = 4.16.0)
that starts on demand, is configured for streams, and is cleaned up afterwards.
Local runs become `dotnet test` with Docker running; CI drops the service
container entirely.

## Goals

- One shared RabbitMQ container per test run (not per test class — startup and
  plugin enabling are multi-second).
- Stream plugin enabled, advertised host forced to `localhost`, ports mapped
  dynamically; tests read host/ports/credentials from the fixture instead of
  hardcoded values.
- Update all 17 integration test files to take broker coordinates from the
  shared fixture.
- Update `build.yml` (and `release.yml` if it runs the same suite) to remove
  the service container and related setup steps.

## Non-goals

- Fixing the failing integration tests — separate phase, driven by the
  currently-running recon test run.
- Changing the library source (`src/StreamBus`) at all.
- Testcontainers Desktop / cloud features.

## Constraints

- Tests use xunit.v3 + `Microsoft.NET.Test.Sdk`; the suite currently has no
  fixtures — brokers coordinates are duplicated per file.
- `STREAMBUS_SKIP_INTEGRATION=1` skip lane must keep working (hosts without
  Docker).
- Central Package Management (`Directory.Packages.props`) — the package
  version goes there.
- Stream client rejects `guest`, so the container uses a dedicated user
  (`app`/`password`, matching existing defaults).
- Image is pinned to the **4.2 minor line** (`rabbitmq:4.2-management`), matching
  the broker the library is developed against and CI's current service
  container. Testcontainers' built-in default image is not used, so it cannot
  drift to a newer major/minor; 4.2.x also keeps compatibility with older
  RabbitMQ deployments the library targets.
- Stream plugin cannot be enabled via `RABBITMQ_SERVER_ADDITIONAL_ERL_ARGS`; CI's
  proven approach is used instead — the container starts, then
  `rabbitmq-plugins enable rabbitmq_stream` runs on the live node via container
  exec, and a readiness poll gates the fixture. `WithCommand` is avoided: it
  overrides the image's `rabbitmq-server` CMD and the container would exit
  instead of starting the broker. `-rabbitmq_stream advertised_host localhost`
  is still required via env.

## Approach (picked with the human partner)

**Chosen: Testcontainers module + assembly-wide singleton fixture.**
Use ` RabbitMQBuilder` from `Testcontainers.RabbitMq` 4.16.0. One container,
started lazily once per test assembly and disposed at the end. A `TestBroker`
static class (replacing the per-file `User()`/`Password()` duplication)
exposes `Host`, `StreamPort` (5552 mapped), `AmqpPort` (5672 mapped),
`Username`, `Password`. Tests keep calling `IntegrationLane.RequireBroker()` at
construction; when skipped, the container is never started.

Rejected: docker-compose file (manual step remains, CI keeps diverging) and
container-per-test-class (reliable but multiplies broker startup cost ~17x).

## What's left to the builder (settled by plan)

- Exact singleton mechanism for xunit.v3 (static `Lazy<Task>` + module
  initializer vs. xunit v3 assembly fixture) — plan decides.
- Whether `StreamManagementApi.cs` helpers need port parameterization too.
- How far to simplify `build.yml` plugin/listener wait steps (they all go).

## Deliberate call made by the agent

Keep `STREAMBUS_USER`/`STREAMBUS_PASSWORD` env overrides out of the new
fixture — Testcontainers owns credentials now, and the env vars exist only to
point tests at the CI service container, which is being deleted.
