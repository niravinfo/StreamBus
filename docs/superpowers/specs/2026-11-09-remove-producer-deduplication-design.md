# Remove producer deduplication and all legacy effectively-once surfaces — design

Date: 2026-11-09
Status: awaiting approval

## Intent

Make the library safe by default when many identical service instances run
concurrently:

- StreamBus never assigns a **producer name** (`Reference`) to any producer, on
  any publish path. RabbitMQ Streams broker-side deduplication is therefore never
  enabled.
- Delivery stays **at-least-once**. Consuming applications own duplicate handling
  and idempotency. The library adds no consumer-side deduplication.
- Every code and documentation artifact belonging to the old deduplication and
  effectively-once (EO) feature is removed from the current tree, as if the
  feature never existed.

## Why

1. The current producer name is `streambus-pub-{stream}-{process-local counter}`.
   The counter restarts at 1 in every process, so two instances — or one instance
   after a restart — reuse `streambus-pub-orders-1`, `-2`, … The broker's dedup
   limit for that name lives in the stream and survives producer disconnect and
   application restart.
2. The second instance's publishing IDs start at 1 again. Because they are ≤ the
   stored limit, the broker **filters the messages out but confirms them `Ok`**.
   The publisher sees success and the messages are silently lost. This directly
   breaks the library's primary goal: ordered consumption with load balancing
   across N instances.
3. Deduplication existed only to support the EO read-process-write pipeline and
   error replay. Both are already removed. No current code path needs a producer
   name.
4. The library is pre-release and has no users; breaking changes are allowed.

## Verified facts (documentation and source reading, no POC)

1. **Dedup is opt-in through the producer name, not the publishing ID.** RabbitMQ
   Streams docs (4.2 and 4.4, "Deduplication of Published Messages"): dedup is
   activated per producer; the RabbitMQ-maintained clients do not activate it by
   default; the producer name must be unique for a given stream.
2. **The broker keeps the highest publishing ID per (stream, producer name).** An
   outbound message with an ID ≤ that limit is filtered out, but a confirmation is
   still sent. The limit is stream state, not connection state.
3. **Name uniqueness is enforced per connection only.** In
   `rabbitmq_stream_reader.erl` (`declare_publisher`), uniqueness is checked
   against `publisher_to_ids` on that connection. Two connections can both declare
   the same name; their publishing ID sequences interleave and messages get
   filtered.
4. **No name means no dedup.** The .NET client writes a null/empty `Reference` as
   an empty string (`WireFormatting.WriteString`). The broker maps an empty writer
   reference to `undefined`, and `osiris_writer:is_duplicate(undefined, …)`
   returns `false`. Publishing IDs are still required by the protocol and are used
   to correlate publisher confirms; without a name they cause no dedup.
5. **Publishing IDs cannot be removed.** `IProducer.Send(ulong publishingId,
   Message)` is the only send shape in client 1.12.1. The per-handle counter stays
   and remains the confirmation-correlation key.
6. **Current StreamBus already has the collision bug** because of fact 1 plus the
   process-local name construction in `Connector.cs`.
7. **Consumers are unaffected.** The consumer group reference is cursor identity,
   not dedup identity. Super streams distribute partitions across instances in one
   group; a plain stream with SAC (the default) has one active instance and the
   others as standby. Duplicates are an at-least-once property and stay the
   application's responsibility.

## Goals

- Anonymous producers on every path: typed publish, raw publish, super publish,
  and error-envelope / dead-letter sends (they share the producer registry).
- Delete the dedup-only public surface from `IStreamSystemHandle`:
  `CreatePlainProducerWithReferenceAsync`,
  `CreateSuperStreamProducerWithReferenceAsync`, `QuerySequenceAsync`.
- Delete `StreamBusNaming` and the process-local producer sequence.
- Remove every EO and dedup code reference from tests, samples, docs, and
  generated artifacts.
- Add regression pins: producer configs carry `Reference == null`; the removed
  handle members do not exist.
- Document the real scaling contract for publish and consume.

## Non-goals

- No change to consumer-group semantics, offset tracking, retry, exhaustion,
  dead-letter writing, topology declaration, or the serializers.
- No change to AMQP message properties (`MessageId`, `CorrelationId`,
  `SentTime`) — these are identity properties, not dedup.
- No broker or client-library changes; no POC.
- No git history rewrite. Old commits and release tags remain; the working tree
  is what changes.

## Anti-goals

- Any producer reference left in `src/`.
- A new opt-in dedup API hidden behind configuration.
- Consumer-side dedup added to the library.
- Changing publish confirm, timeout, or producer-invalidation behavior.
- Leaving generated XML or stale docs that still describe removed APIs.

## Approaches considered

1. **Anonymous producers + full removal (chosen).** Remove the name from every
   producer config and delete the dedup-only APIs and naming helpers. Keep the
   per-handle publishing ID counter for confirms. Only this option actually turns
   broker dedup off, makes identical instances safe by construction, and matches
   the product decision that consumers own duplicates.
2. **Keep named producers behind an opt-in API.** Rejected: preserves the
   collision/loss footgun, adds surface, and violates "no changes needed for
   scaling".
3. **Keep names but make them GUID-unique.** Rejected: avoids today's collision
   but leaves dedup state and the concept alive, adds pointless broker tracking,
   and is not a removal.
4. **Disable dedup on a named producer with a client flag.** Rejected: no such
   flag exists; the name itself is the switch.

## Scope of removal

### Source

- `src/StreamBus/Connector.cs`
  - Delete `CreatePlainProducerWithReferenceAsync`,
    `CreateSuperStreamProducerWithReferenceAsync`, and `QuerySequenceAsync` from
    `IStreamSystemHandle` and `StreamSystemHandle`.
  - Delete the static `_producerSeq` and `ProducerReference`.
  - Remove `Reference = …` from both `RawProducerConfig` and
    `RawSuperStreamProducerConfig`.
  - Keep one plain-producer creation core; keep the confirm and
    connection-closed handlers unchanged.
  - Reword `TryQueryOffsetAsync` / `StoreSystemOffsetAsync` docs to say
    "consumer reference".
- `src/StreamBus/StreamBusNaming.cs`
  - Delete the file. After the above, `PublishReference`, `Enforce`, and
    `MaxName` have no callers. Consumer-name validation uses
    `StreamBusLimits.MaxReferenceBytes` directly.
- `src/StreamBus/ProducerHandles.cs`
  - Comment-only: publishing IDs are sequential per handle and exist to correlate
    confirms; there is no broker dedup. No behavior change.
- No logic changes in `Bus.cs`, `PublishPipeline.cs`, `ErrorPublisher.cs`,
  `ProducerRegistry.cs`, `ConsumerRegistry.cs`, or `StreamPartitionConsumer.cs`.

### Tests

- Remove the three removed members from every `IStreamSystemHandle` fake and
  subclass. Known sites: `Fakes/ScriptedSystemHandle.cs` (base and strict),
  `AcceptanceTests`, `HostingTests`, `PipelineSemanticsTests`,
  `PublishRoutingTests` (two classes), `TopologyTests`.
- Delete `ScriptedSystemHandle.SequenceQueries` and `SequenceResult`; no test
  reads them after the API is gone.
- `ProducerHandleSurfaceTests`: delete the EO absence assertions and the
  `Bus_surface_has_no_effectively_once_members` test; keep the
  `SendEnvelopeAsync` and plain/super split pins.
- Add `SystemHandleSurfaceTests`: reflect that
  `IStreamSystemHandle` has no `CreatePlainProducerWithReferenceAsync`,
  `CreateSuperStreamProducerWithReferenceAsync`, or `QuerySequenceAsync`.
- Add `ProducerConfigTests` against an internal config factory: plain and super
  `RawProducerConfig`/`RawSuperStreamProducerConfig` have `Reference == null`,
  and their confirm/routing/connection-closed handlers are still wired.
- Rename `Super_stream_splits_across_instances_exactly_once_ordered` to a
  duplicate-free partition-assignment name (the assertion is partition
  distribution, not an EO guarantee).
- Add an integration guarantee: two independent publisher instances publish
  concurrently to one stream; the consumer receives every message. Note in the
  test that the regression guard for the collision is the config pin, because
  two in-process publishers shared the old static counter and would not reproduce
  a cross-process collision.

### Documentation (current, user-facing)

- `README.md`: add a scale-out bullet: publishers are anonymous, any number of
  identical instances can publish, delivery is at-least-once, consumers own
  idempotency.
- `docs/guide/publish.md`: new "Publishing from many instances" section with the
  same contract; state that the library never sets a producer name.
- `docs/guide/consume.md`: correct the scale-out statement. Super stream + one
  group = partitions distributed across instances (SAC per partition). Plain
  stream + SAC = one active instance with standby failover, not throughput
  scale-out. Different groups read independently. Consumers must be idempotent.
- `docs/llms.txt`: replace the dedup sentence with: producers are anonymous, the
  library never enables broker deduplication, any number of identical instances
  may publish, delivery is at-least-once, applications own duplicate handling and
  idempotency.
- `docs/src/content/docs/guides/publishing.mdx`: same publish contract.
- `docs/src/content/docs/guides/consuming.mdx`: fix the incorrect "distribute
  messages across instances automatically" claim for plain streams; state the
  super-stream vs plain-stream model.
- `docs/guide/errors.md`: reword "exactly-once decision" to "one-time decision"
  (cosmetic; unrelated to delivery semantics).
- Fix every other current-doc location that repeats the old plain-stream
  scale-out claim: `README.md` (the concepts and endpoints bullets),
  `docs/llms.txt` (the endpoints/super-stream bullet),
  `docs/src/content/docs/index.mdx`, and `examples/README.md`. The file list
  above is illustrative, not exhaustive: any current doc that says a plain
  stream distributes work across instances in one group is corrected to the
  super-stream vs SAC model.

### Historical records

**Decision: scrub the whole tree (literal zero hits).**

- Delete EO-only artifacts:
  `docs/superpowers/plans/2026-10-13-streambus-phase7-read-process-write.md`,
  `docs/superpowers/plans/2026-11-07-remove-effectively-once.md`, and the
  untracked `.superpowers/sdd/2026-10-31-streambus-message-context/` scratch
  directory.
- Scrub EO/dedup material from every other tracked file that mentions it:
  - `docs/2026-10-08-streambus-design.md`
  - `docs/2026-10-08-streambus-requirements.md`
  - `docs/2026-10-13-streambus-acceptance.md`
  - `docs/superpowers/plans/2026-10-11-streambus-phase4-producer.md`
  - `docs/superpowers/plans/2026-10-11-streambus-phase5-consumer.md`
  - `docs/superpowers/plans/2026-10-13-streambus-phase6-retry.md`
  - `docs/superpowers/plans/2026-10-13-streambus-phase9-typed-consumer.md`
  - `docs/superpowers/plans/2026-10-13-streambus-phase10-acceptance.md`
  - `docs/superpowers/plans/2026-10-13-streambus-v2-user-centric-api.md`
  - `docs/superpowers/plans/2026-10-17-streambus-registration-owned-stream-spec.md`
  - `docs/superpowers/plans/2026-10-19-streambus-v2-architecture-and-middleware.md`
  - `docs/superpowers/plans/2026-10-25-streambus-typed-filters.md`
  - `docs/superpowers/plans/2026-10-27-streambus-single-filter-context.md`
  - `docs/superpowers/plans/2026-10-31-streambus-message-context.md`
  - `docs/superpowers/plans/2026-11-09-remove-error-replay-and-requeue.md`
  - `docs/superpowers/specs/2026-10-18-streambus-v2-architecture-and-middleware-design.md`
  - `docs/superpowers/specs/2026-10-25-streambus-typed-filter-design.md`
  - `docs/superpowers/specs/2026-10-25-streambus-wire-metadata-design.md`
  - `docs/superpowers/specs/2026-10-27-streambus-single-filter-context-design.md`
  - `docs/superpowers/specs/2026-10-31-streambus-message-context-design.md`
  - `docs/superpowers/specs/2026-11-09-remove-error-replay-and-requeue-design.md`
- When a step, section, or requirement exists only for EO/dedup, delete it. When
  a sentence is incidental, rewrite it without the concept. Do not renumber
  unrelated IDs unless the file would otherwise be incoherent.
- Delete this change's own design and plan files in the final cleanup task, after
  verification. Git history still contains them; the working tree does not.

### Build artifacts

- Delete `bin/` and `obj/` before the final grep gate. The stale generated
  `StreamBus.xml` in example build output still documents removed APIs.
- Delete stale untracked test output (`artifacts/test-results/`) before the gate;
  old `.trx` files still contain removed test names.

## Scaling contract after this change

- **Publish:** N identical instances may share configuration and target the same
  stream/super stream. No producer name is set, so the broker has no dedup
  namespace to collide. Each connection gets its own confirms. Duplicates can
  only come from at-least-once retries, never from broker filtering.
- **Consume:** a super stream with one group distributes partitions across
  instances, with SAC per partition; a plain stream with SAC keeps one active
  consumer and standbys; different groups each receive every message. Consumers
  must tolerate duplicates.
- **Dead-letter:** uses the same anonymous producers. A retried hand-off may
  duplicate; that is already the at-least-once contract.

## Constraints

- .NET 10; `RabbitMQ.Stream.Client` 1.12.1.
- `IStreamSystemHandle` is public; deleting members is a breaking change. Accepted
  (pre-release, no users).
- Build with 0 warnings; unit suite green; integration project compiles even
  without a broker.
- A null/empty producer `Reference` must serialize as an empty name, which the
  pinned client version does.

## Verification

- `dotnet build StreamBus.slnx` — 0 warnings.
- `dotnet test tests/StreamBus.UnitTests` — all green.
- `dotnet build tests/StreamBus.IntegrationTests` — clean; run the new
  multi-instance test when Docker/RabbitMQ is available.
- Surface pins: removed members absent; producer configs have `Reference == null`.
- Behavior pins: `ConfirmationCloseTests` and publish/retry suites stay green
  (confirms, timeouts, invalidation unchanged).
- Grep gate after cleaning build output. The gate is **symbol-based**, not
  word-based, because the new contract text legitimately says the library does
  not enable broker deduplication:
  - Zero hits for removed identifiers across `src/`, `tests/`, `examples/`,
    current docs, and historical docs (per the scope decision):
    `EffectivelyOnce`, `effectively-once`, `ConsumeEffectivelyOnceAsync`,
    `PublishResultAsync`, `PublishResultOptions`, `ResultPublisher`,
    `ResultProducer`, `PublishWithIdAsync`, `PublishReplayAsync`,
    `requiresContinuation`, `DeduplicationName`, `StreamBusNaming`,
    `PublishReference`, `CreatePlainProducerWithReferenceAsync`,
    `CreateSuperStreamProducerWithReferenceAsync`, `QuerySequenceAsync`,
    `SequenceQueries`, `SequenceResult`.
  - The words "deduplication"/"dedup" may remain only in the explicit negative
    contract statements ("the library never assigns a producer name, so broker
    deduplication is never enabled; applications own duplicate handling"), never
    as a described capability.
  - Exclude `.git/`, `bin/`, `obj/`, `node_modules/`, and `artifacts/`.
- `Reference` in `src/` appears only for consumer groups.
- No `Reference =` assignment in either producer config.

## Decisions

1. **Historical docs: scrub the whole tree.** Literal zero EO/dedup hits across
   the tracked tree: scrub the mixed historical plans/specs and the dated
   design/requirements/acceptance records, delete EO-only plans, and delete this
   change's design/plan after execution.

## Calls made by the author (check while reading)

- **Changelog:** no EO/dedup mention, and no changelog edit for this change — the
  concepts are treated as never shipped. The release version comes from the git
  tag, not the csproj.
- **Grep gate:** symbol-based, not word-based, because the required new contract
  text names the disabled capability. Exact rules under Verification.
- **Scale-out correction:** apply it to every current doc that repeats the old
  plain-stream claim, not only the enumerated files.
- **Stale artifacts:** delete `bin/`, `obj/`, and `artifacts/test-results/`
  before the gate rather than special-casing them.

## Left to the builder

- The exact internal seam for the config pin (static factory on
  `StreamSystemHandle` vs a small internal `ProducerConfigs` type).
- Whether to keep `CreatePlainProducerCoreAsync` as a private helper or inline it.
- How to split shared test files when deleting overrides.
- Exact wording of the publish/consume scaling sections.
