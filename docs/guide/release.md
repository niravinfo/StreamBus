# Releasing StreamBus

The package is published to nuget.org by the `Release` workflow. Publishing is
deliberately **not** wired to pushes: a green build on `main` never publishes.

## One-time setup on nuget.org

1. **Add a Trusted Publishing policy.** Sign in to nuget.org, click your
   username, choose **Trusted Publishing**, and add a policy:
   - Repository Owner: `niravinfo`
   - Repository: `StreamBus`
   - Workflow File: `release.yml` (the file name only - not the
     `.github/workflows/` path)
   - Environment: leave empty unless the workflow uses a GitHub Actions
     environment
   - **Scope:** the policy must allow publishing a **new package id**, not only
     new versions of existing packages. A scope limited to existing packages
     rejects the first push of `StreamBus`.

   The policy applies to every package the selected owner owns. Without a
   matching policy the OIDC exchange in `NuGet/login@v1` fails and the publish
   step cannot authenticate - no API key is stored in the repository.

   If the Trusted Publishing option is missing from your nuget.org account
   menu, the feature is not enabled for that account yet: fall back to an API
   key with the `StreamBus` glob for the first publish.

2. **Set the `NUGET_USER` secret.** Add the repository secret `NUGET_USER`
   with the nuget.org account name (the profile name, not the email address)
   that owns the `StreamBus` id. The workflow exchanges an OIDC token for a
   short-lived API key for that account.

   The `StreamBus` package id is not taken yet, so the first push claims it.
   Any nuget.org account can claim an unused id.

## Releasing

1. Make sure `main` is green: the `Build` workflow must have passed for the
   commit you are about to tag.
2. Create a GitHub Release with the tag `vX.Y.Z` (for example `v0.1.0`).
3. Publishing the Release starts the `Release` workflow. It resolves the
   version from the tag (`v` prefix stripped), rebuilds with that version,
   runs the whole test suite against a containerized RabbitMQ, packs, pushes
   the `.nupkg` and the `.snupkg` to nuget.org, and attaches both packages to
   the GitHub Release.
4. Check https://www.nuget.org/packages/StreamBus - nuget.org indexes a
   freshly pushed version within a few minutes.

The tag must be a strict SemVer version (`v1.2.3`, `v1.2.3-preview.1`). The
workflow refuses anything else: NuGet normalizes `v1.2` to `1.2.0` and drops
build metadata, and a published version can never be changed afterwards.

Version `0.2.0` is reserved for the breaking V2 release (ADR-6 in the V2
architecture spec) and is expected to ship with a `CHANGELOG.md` break list.

## Manual dispatch (escape hatch)

The `Release` workflow can also be started by hand from the Actions tab with
the `version` input (for example `1.2.3`) - the input is required, and the
workflow fails rather than guessing a version. The run packs that version and
uploads the packages as a workflow artifact (named with the source commit)
instead of attaching them to a GitHub Release. Use it to re-publish a version
that failed mid-push (`dotnet nuget push --skip-duplicate` makes the push
idempotent).

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
