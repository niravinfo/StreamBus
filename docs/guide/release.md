# Releasing StreamBus

The package is published to nuget.org by the `Release` workflow. Publishing is
deliberately **not** wired to pushes: a green build on `main` never publishes.

## One-time setup on nuget.org

1. **Own the package id.** The `StreamBus` id is currently unused on nuget.org.
   The first push claims it, so the account that pushes must be allowed to
   reserve new ids (this is the default for accounts with a verified email).
2. **Add a Trusted Publisher.** On the nuget.org package page, go to
   *Trusted Publishing* and add:
   - Repository: `niravinfo/StreamBus`
   - Workflow: `release.yml`
   - Environment: leave empty unless a protected environment is configured
     (if you use one, add it in the workflow's `environment:` key as well).

   Without a Trusted Publisher the OIDC exchange in `NuGet/login@v1` fails and
   the publish step cannot authenticate. No API key is stored in the repository.
3. **Set the `NUGET_USER` secret.** Add the repository secret `NUGET_USER` with
   the nuget.org account name that owns the `StreamBus` id. The workflow
   exchanges an OIDC token for a short-lived API key for that account.

## Releasing

1. Make sure `main` is green: the `Build` workflow must have passed for the
   commit you are about to tag.
2. Create a GitHub Release with the tag `vX.Y.Z` (for example `v0.2.0`).
3. Publishing the Release starts the `Release` workflow. It resolves the
   version from the tag (`v` prefix stripped), rebuilds with that version,
   runs the whole test suite against a containerized RabbitMQ, packs, pushes
   the `.nupkg` and the `.snupkg` to nuget.org, and attaches both packages to
   the GitHub Release.
4. Check https://www.nuget.org/packages/StreamBus - nuget.org indexes a
   freshly pushed version within a few minutes.

## Manual dispatch (escape hatch)

The `Release` workflow can also be started by hand from the Actions tab with
an optional `version` input (for example `1.2.3`). The run packs that version
and uploads the packages as a workflow artifact instead of attaching them to a
GitHub Release. Use it to re-publish a version that failed mid-push
(`dotnet nuget push --skip-duplicate` makes the push idempotent).

## What the CI gate covers

The `Build` workflow runs on every pull request and on every push to `main`:

| Job | What it does |
| --- | --- |
| `build` | restore, Release build, unit tests, `dotnet pack`, uploads the `.nupkg` for inspection |
| `integration-test` | starts a RabbitMQ 4.2 service container (streams plugin enabled, dedicated `app` user), runs the integration suite against it, uploads the TRX report |

The broker is a GitHub Actions service container, so Docker is the only
prerequisite on the runner. The integration tests need a broker with the
stream plugin on port `5552`, AMQP on `5672`, and the management API on
`15672`; the container publishes all three.

Not covered: code-coverage thresholds, nightly runs, and multi-node broker
scenarios (the leader-placement test skips on a single-node broker by design).

## Branch protection

Recommended on `main`: require the `build` and `integration-test` checks to
pass before merging, so nothing reaches `main` unverified.
