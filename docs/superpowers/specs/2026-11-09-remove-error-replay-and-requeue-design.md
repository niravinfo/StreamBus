# Remove error replay and requeue — design

Date: 2026-11-09
Status: awaiting approval

## Intent

StreamBus's contract for errors becomes: **write error envelopes to the
configured dead-letter destination (stream or queue) — nothing more.** Reading
those destinations back, and republishing their content, is the consumer's
own job (manual moves or consuming directly). All library-side recovery code
goes away.

## Why

No users rely on the recovery APIs. Keeping dead (in-repo-uncalled) public
surface around the replay publishing method costs maintenance and invites confusion
about where responsibility for error recovery lies. The write path
(dead-lettering) is untouched and remains the product.

## Goals

- Remove every public API whose purpose is reading from an error
  destination and republishing: `ReplayErrorStreamAsync`,
  `RequeueErrorsAsync`, and the handle-level replay publishing method.
- Remove all internal machinery that exists only to serve them.
- Keep tests, docs, and traceability rows consistent with the new surface
  (deleted, not silently stale).

## Non-goals

- No changes to dead-letter writing: `DeadLetterToStream` /
  `DeadLetterToQueue`, `ErrorPublisher`, `ErrorEnvelope` wire formats,
  header round-trip, offset-commit-once semantics.
- No changes to retry/exhaustion pipeline, producers, consumers, offset
  APIs.

## Anti-goals

- Breaking the dead-letter write path in any way.
- Leaving orphaned naming helpers, guards, or test fakes behind.

## Approaches

1. **Full removal (chosen).** Delete recovery APIs and all private
   supporting machinery, including the handle-level replay publishing method.
   Trades off a breaking change — accepted; no existing users.
2. **Keep the replay publishing method as a general id-publish primitive.**
   Rejected: it exists only for replay idempotency; manual recoverers
   republish as fresh messages via normal publish.

This reversal supersedes the "keep replay machinery" pin in a prior plan (that
plan already executed; this change removes what it deliberately preserved).

## Scope of removal

### Public API (`src/StreamBus`)

- `Bus.cs`: `IStreamBus.ReplayErrorStreamAsync` (interface + impl in
  `StreamBusCore`), `IStreamBus.RequeueErrorsAsync` (interface + impl), and
  the `ReplayErrorStreamAsync` forwarding in `StreamBus`.
- `ProducerHandles.cs`: the replay publishing method from
  `IStreamPlainProducerHandle` and `IStreamSuperProducerHandle`, the two
  implementing methods, and its `ConfirmationTracker` counterpart (replay is
  its only caller; `ConfirmationTracker` itself and its confirmation-close
  behavior stay — all other sends use them).
- `ConsumerRegistry.cs`: `ReplayAsync` and its helper surface
  (idle-termination loop, malformed/vanished-partition skip handling,
  reference-scoped replay producer acquisition).
- `ErrorQueueTransport.cs`: `DrainAsync` (the BasicGet looper) and requeue
  logging it carries.
- `ErrorEnvelope.cs`: `ToReplayOptions()` and the `ReplayNaming` class.
- The shared naming helper: `ReplayProducerName`, `ReplayReference`.
- `ConsumeOptions.cs`: doc reference to `ReplayErrorStreamAsync` (line ~77)
  reworded.

### Tests

- Delete: `ReplayNamingTests`, `ReplayReattachTests`,
  `Replay_twice_stores_once` (RetryIntegrationTests), the requeue
  integration test (`ErrorQueueIntegrationTests` requeue case), replay
  cases in `ConfirmationCloseTests`, `Replay_options_*` and
  `Replay_naming_*` in `ErrorEnvelopeTests`, replay publishing method surface
  assertions in `ProducerHandleSurfaceTests`, the `DrainAsync` tests in
  `ErrorQueueTransportTests`, and the replay publishing method members in
  every test fake/mocked handle (AcceptanceTests, ActivityTests,
  ConfirmationCloseTests, PipelineSemanticsTests, PublishPipelineTests,
  PublishTests, PublishRoutingTests, Fakes/ScriptedSystemHandle).
- Where a test exists only to pin replay semantics, delete it; where a
  test covers more, trim the replay/requeue parts.

### Docs

- `docs/guide/errors.md`: remove Replay and Requeue sections; state the
  new contract (user moves/consumes manually).
- `README.md` feature list: drop "Replay (streams) and requeue (queues)".
- `docs/llms.txt`: drop replay/requeue references.
- `docs/2026-10-13-streambus-acceptance.md`: mark FR-5.3 and the
  queue-requeue row as removed by this change (date + pointer), not
  silently deleted — keep traceability.
- `docs/2026-10-08-streambus-requirements.md`: FR-5.3 annotated likewise.

Historical plans/specs under `docs/superpowers/` are left as-is (they
describe past states).

## Constraints

- Library targets .NET with packable public API; removal means compile
  breaks for external code — accepted, warrants a version bump in the
  changelog/release notes as breaking.
- `SendEnvelopeAsync` on the plain handle stays: the dead-letter write
  path uses it.

## Verification

- `dotnet build` clean across solution.
- Full unit suite green; integration suite green where a broker env is
  available (remaining dead-letter write tests must still pass — proof the
  write path survived).
- Greps drive to zero for every removed symbol name — the replay entry
  points, requeue, replay naming, `ToReplayOptions`, `DrainAsync`,
  `ReplayReference`, `ReplayProducerName`, and the replay publishing method —
  under `src/` and `tests/`.

## Left to the builder

- Exact splitting of multi-purpose test files (trim vs delete).
- Whether `ErrorQueueTransport` retains AMQP *publish* path only (yes —
  dead-letter-to-queue writes still need it).
- Changelog wording for the breaking-change entry.
