# Remove Producer Deduplication Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make every StreamBus producer anonymous so any number of identical instances can publish safely, and remove every producer-deduplication and legacy effectively-once (EO) artifact from the tree.

**Architecture:** `StreamSystemHandle` builds both raw producer configs through one internal `ProducerConfigs` factory. The factory never sets `Reference`, so the broker's named-producer deduplication path is never activated. The dedup-only handle members, `StreamBusNaming`, and the process-local producer counter are deleted. Per-handle publishing IDs stay because the client protocol requires them to correlate confirms. Delivery remains at-least-once; consumers own duplicate handling.

**Tech Stack:** .NET 10, xUnit v3, `RabbitMQ.Stream.Client` 1.12.1, RabbitMQ Streams 4.2+.

**Spec:** `docs/superpowers/specs/2026-11-09-remove-producer-deduplication-design.md`

## Global Constraints

- Work only on branch `nirav/feature/RemoveEO`. Do not create branches, worktrees, or stashes.
- Do not push, force-push, or open a pull request unless the human asks.
- .NET 10 and `RabbitMQ.Stream.Client` 1.12.1 are pinned; do not change package versions.
- `TreatWarningsAsErrors` is on: every build must finish with 0 warnings.
- No producer config may assign `Reference` on any path (normal, super, error-envelope, dead-letter).
- Keep sequential per-handle publishing IDs; they correlate confirms. Only the producer name/reference is removed.
- Do not add consumer-side deduplication, a new opt-in dedup API, or a changelog entry.
- Do not rewrite git history and do not edit `docs/src/content/docs/guides/changelog.mdx`.
- Commit after each task. Stage only the files that task names.
- The final grep gate is symbol-based and excludes `.git/`, `bin/`, `obj/`, `node_modules/`, and `artifacts/`.

## Review Focus

1. **Dead-letter/error-envelope sends must use the same anonymous producer path.** A leftover `Reference` there would silently drop hand-offs. Pinned by `ProducerConfigTests` (`Reference is null` for both configs) and the `Reference =` source scan in Task 8.
2. **Consumer group identity also uses the name `Reference`.** Removing producer references must not remove consumer offset/cursor APIs. Pinned by `SystemHandleSurfaceTests.Offset_members_remain_for_consumer_groups` and the existing consume/offset suites.
3. **Super-stream routing must work without a producer name.** Pinned by `ProducerConfigTests.Super_producer_config_keeps_routing_wired` and `PublishIntegrationTests.Super_stream_keys_land_on_stable_partitions`.
4. **Confirms still depend on sequential per-handle publishing IDs.** Pinned by `ConfirmationCloseTests` and `PublishTests` (confirm, timeout, invalidation).
5. **Two identical publishers to one stream must not lose messages.** The in-process integration test is a contract check; the real collision regression guard is the config pin because the old counter was process-wide. Pinned by `ConsumerIntegrationTests.Multiple_publisher_instances_to_one_stream_lose_nothing`.

---

### Task 1: Extract the internal producer-config factory (characterization)

**Files:**
- Create: `src/StreamBus/ProducerConfigs.cs`
- Modify: `src/StreamBus/Connector.cs:118-225`
- Create: `tests/StreamBus.UnitTests/ProducerConfigTests.cs`

**Interfaces:**
- Produces:
  - `internal static class ProducerConfigs`
  - `public static RawProducerConfig Plain(string stream, string reference, ConfirmationTracker confirmations)`
  - `public static RawSuperStreamProducerConfig Super(string stream, string reference, ConfirmationTracker confirmations, SuperStreamRouting routing)`
- Consumes: `ConfirmationTracker`, `SuperStreamRouting` (both internal, visible to tests via `InternalsVisibleTo`).

- [ ] **Step 1: Write the failing characterization test**

Create `tests/StreamBus.UnitTests/ProducerConfigTests.cs`:

```csharp
using RabbitMQ.Stream.Client;

namespace StreamBus.UnitTests;

public sealed class ProducerConfigTests
{
    [Fact]
    public void Plain_config_keeps_the_supplied_reference_and_handlers()
    {
        var confirmations = new ConfirmationTracker();

        var config = ProducerConfigs.Plain("orders", "stable-ref", confirmations);

        Assert.Equal("orders", config.Stream);
        Assert.Equal("stable-ref", config.Reference);
        Assert.Equal(100, config.MessagesBufferSize);
        Assert.NotNull(config.ConfirmHandler);
        Assert.NotNull(config.ConnectionClosedHandler);
    }

    [Fact]
    public void Super_config_keeps_the_supplied_reference_and_routing()
    {
        var confirmations = new ConfirmationTracker();
        var routing = new SuperStreamRouting();

        var config = ProducerConfigs.Super("orders-super", "stable-ref", confirmations, routing);

        Assert.Equal("orders-super", config.SuperStream);
        Assert.Equal("stable-ref", config.Reference);
        Assert.Equal(100, config.MessagesBufferSize);
        Assert.NotNull(config.ConfirmHandler);
        Assert.NotNull(config.ConnectionClosedHandler);
        Assert.NotNull(config.Routing);

        var message = new Message(new byte[] { 1 });
        routing.Stamp(message, "k0");
        Assert.Equal("k0", config.Routing(message));
    }
}
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `dotnet test tests/StreamBus.UnitTests/StreamBus.UnitTests.csproj --filter "FullyQualifiedName~ProducerConfigTests"`

Expected: compile failure, `ProducerConfigs` does not exist.

- [ ] **Step 3: Add the factory and move both config literals into it**

Create `src/StreamBus/ProducerConfigs.cs`:

```csharp
using RabbitMQ.Stream.Client;

namespace StreamBus;

/// <summary>
/// Builds the raw producer configs StreamBus uses for every publish path.
/// Publishing ids still correlate confirms; no producer name is assigned.
/// </summary>
internal static class ProducerConfigs
{
    public static RawProducerConfig Plain(string stream, string reference, ConfirmationTracker confirmations)
        => new(stream)
        {
            MessagesBufferSize = 100,
            Reference = reference,
            ConfirmHandler = confirmations.Complete,
            ConnectionClosedHandler = _ =>
            {
                confirmations.OnConnectionLost();
                return Task.CompletedTask;
            },
        };

    public static RawSuperStreamProducerConfig Super(
        string stream,
        string reference,
        ConfirmationTracker confirmations,
        SuperStreamRouting routing)
        => new(stream)
        {
            MessagesBufferSize = 100,
            Reference = reference,
            Routing = routing.Route,
            ConfirmHandler = confirmation => confirmations.Complete(confirmation.Item2),
            ConnectionClosedHandler = (_, _) =>
            {
                confirmations.OnConnectionLost();
                return Task.CompletedTask;
            },
        };
}
```

In `src/StreamBus/Connector.cs`:

1. Replace the `new RawProducerConfig(stream) { ... }` literal in `CreatePlainProducerCoreAsync` with `ProducerConfigs.Plain(stream, reference, confirmations)`.
2. Replace the `new RawSuperStreamProducerConfig(stream) { ... }` literal in `CreateSuperStreamProducerAsync` with `ProducerConfigs.Super(stream, ProducerReference(stream), confirmations, routing)`.
3. Replace the same literal in `CreateSuperStreamProducerWithReferenceAsync` with `ProducerConfigs.Super(stream, reference, confirmations, routing)`.
4. Delete the now-obsolete comment blocks above both literals (the dedup rationale paragraphs).

- [ ] **Step 4: Run the tests to verify they pass**

Run: `dotnet test tests/StreamBus.UnitTests/StreamBus.UnitTests.csproj --filter "FullyQualifiedName~ProducerConfigTests"`

Expected: PASS (2 tests).

Then run the full unit suite: `dotnet test tests/StreamBus.UnitTests/StreamBus.UnitTests.csproj`

Expected: all green.

- [ ] **Step 5: Commit**

```bash
git add src/StreamBus/ProducerConfigs.cs src/StreamBus/Connector.cs tests/StreamBus.UnitTests/ProducerConfigTests.cs
git commit -m "refactor: extract internal producer config factory"
```

---

### Task 2: Make producers anonymous and delete the dedup-only APIs

**Files:**
- Modify: `src/StreamBus/ProducerConfigs.cs`
- Modify: `src/StreamBus/Connector.cs`
- Delete: `src/StreamBus/StreamBusNaming.cs`
- Modify: `tests/StreamBus.UnitTests/ProducerConfigTests.cs`
- Create: `tests/StreamBus.UnitTests/SystemHandleSurfaceTests.cs`
- Modify: `tests/StreamBus.UnitTests/Fakes/ScriptedSystemHandle.cs`
- Modify: `tests/StreamBus.UnitTests/AcceptanceTests.cs:81-94`
- Modify: `tests/StreamBus.UnitTests/HostingTests.cs:20-45`
- Modify: `tests/StreamBus.UnitTests/PipelineSemanticsTests.cs:840-850`
- Modify: `tests/StreamBus.UnitTests/PublishRoutingTests.cs:30-48` and `:205-222`
- Modify: `tests/StreamBus.UnitTests/TopologyTests.cs:101-105`

**Interfaces:**
- Produces:
  - `public static RawProducerConfig Plain(string stream, ConfirmationTracker confirmations)`
  - `public static RawSuperStreamProducerConfig Super(string stream, ConfirmationTracker confirmations, SuperStreamRouting routing)`
- Removes from `IStreamSystemHandle` and `StreamSystemHandle`:
  - `CreatePlainProducerWithReferenceAsync`
  - `CreateSuperStreamProducerWithReferenceAsync`
  - `QuerySequenceAsync`
- Keeps: `CreatePlainProducerAsync`, `CreateSuperStreamProducerAsync`, `TryQueryOffsetAsync`, `StoreSystemOffsetAsync`.

- [ ] **Step 1: Write the failing anonymous-producer and surface tests**

Replace the body of `tests/StreamBus.UnitTests/ProducerConfigTests.cs` with:

```csharp
using RabbitMQ.Stream.Client;

namespace StreamBus.UnitTests;

public sealed class ProducerConfigTests
{
    [Fact]
    public void Plain_config_is_anonymous_and_wired()
    {
        var config = ProducerConfigs.Plain("orders", new ConfirmationTracker());

        Assert.Equal("orders", config.Stream);
        Assert.Null(config.Reference);
        Assert.Equal(100, config.MessagesBufferSize);
        Assert.NotNull(config.ConfirmHandler);
        Assert.NotNull(config.ConnectionClosedHandler);
    }

    [Fact]
    public void Super_config_is_anonymous_and_keeps_routing_wired()
    {
        var routing = new SuperStreamRouting();
        var config = ProducerConfigs.Super("orders-super", new ConfirmationTracker(), routing);

        Assert.Equal("orders-super", config.SuperStream);
        Assert.Null(config.Reference);
        Assert.Equal(100, config.MessagesBufferSize);
        Assert.NotNull(config.ConfirmHandler);
        Assert.NotNull(config.ConnectionClosedHandler);
        Assert.NotNull(config.Routing);

        var message = new Message(new byte[] { 1 });
        routing.Stamp(message, "k0");
        Assert.Equal("k0", config.Routing(message));
    }
}
```

Create `tests/StreamBus.UnitTests/SystemHandleSurfaceTests.cs`:

```csharp
using System.Reflection;

namespace StreamBus.UnitTests;

public sealed class SystemHandleSurfaceTests
{
    [Theory]
    [InlineData("CreatePlainProducerWithReferenceAsync")]
    [InlineData("CreateSuperStreamProducerWithReferenceAsync")]
    [InlineData("QuerySequenceAsync")]
    public void Removed_producer_reference_members_are_absent(string name)
        => Assert.Null(typeof(IStreamSystemHandle).GetMethod(
            name,
            BindingFlags.Public | BindingFlags.Instance | BindingFlags.DeclaredOnly));

    [Fact]
    public void Offset_members_remain_for_consumer_groups()
    {
        Assert.NotNull(typeof(IStreamSystemHandle).GetMethod(
            nameof(IStreamSystemHandle.TryQueryOffsetAsync),
            BindingFlags.Public | BindingFlags.Instance));
        Assert.NotNull(typeof(IStreamSystemHandle).GetMethod(
            nameof(IStreamSystemHandle.StoreSystemOffsetAsync),
            BindingFlags.Public | BindingFlags.Instance));
    }
}
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `dotnet test tests/StreamBus.UnitTests/StreamBus.UnitTests.csproj --filter "FullyQualifiedName~ProducerConfigTests|FullyQualifiedName~SystemHandleSurfaceTests"`

Expected: `ProducerConfigTests` fail because `Reference` is not null (and the new factory signature does not compile yet); `SystemHandleSurfaceTests` fail because the removed members still exist.

- [ ] **Step 3: Remove the reference from the factory and the handle**

In `src/StreamBus/ProducerConfigs.cs`:

1. Change `Plain(string stream, string reference, ConfirmationTracker confirmations)` to `Plain(string stream, ConfirmationTracker confirmations)`.
2. Change `Super(string stream, string reference, ConfirmationTracker confirmations, SuperStreamRouting routing)` to `Super(string stream, ConfirmationTracker confirmations, SuperStreamRouting routing)`.
3. Delete both `Reference = reference,` lines.

In `src/StreamBus/Connector.cs`:

1. Delete `_producerSeq` and `ProducerReference`.
2. In `IStreamSystemHandle`, delete `CreatePlainProducerWithReferenceAsync`, `CreateSuperStreamProducerWithReferenceAsync`, and `QuerySequenceAsync`, including their XML docs.
3. Reword the remaining producer XML docs to remove "generated unique reference". Use: `Creates a plain-stream producer.` / `Creates a super-stream producer; every send must carry a routing key.`
4. In `StreamSystemHandle`, delete the three implementations.
5. Replace `CreatePlainProducerCoreAsync` and `CreatePlainProducerAsync` with one method:

```csharp
public async Task<IStreamPlainProducerHandle> CreatePlainProducerAsync(string stream, CancellationToken cancellationToken)
{
    cancellationToken.ThrowIfCancellationRequested();
    var confirmations = new ConfirmationTracker();
    var producer = await _system.CreateRawProducer(
        ProducerConfigs.Plain(stream, confirmations),
        _factory.CreateLogger<RawProducer>()).ConfigureAwait(false);
    return new StreamProducerHandle(producer, stream, confirmations, _factory.CreateLogger<StreamProducerHandle>());
}
```

6. Update `CreateSuperStreamProducerAsync` to call `ProducerConfigs.Super(stream, confirmations, routing)`.
7. Change the `TryQueryOffsetAsync` and `StoreSystemOffsetAsync` XML docs from "Consumer/producer reference" to "Consumer reference".
8. Delete `using System.Text;` (its only remaining use was the removed reference-length validation).

Delete `src/StreamBus/StreamBusNaming.cs`.

Verify no other source caller exists:

Run: `grep -rn "WithReference\|QuerySequenceAsync\|StreamBusNaming" src --include='*.cs'`

Expected: no output.

- [ ] **Step 4: Remove the removed members from every fake and test override**

In `tests/StreamBus.UnitTests/Fakes/ScriptedSystemHandle.cs`:

1. Delete the `SequenceQueries` property and the `SequenceResult` property.
2. Delete the virtual `CreatePlainProducerWithReferenceAsync` and `CreateSuperStreamProducerWithReferenceAsync` methods.
3. Delete the virtual `QuerySequenceAsync` method.
4. Delete the matching `StrictScriptedSystemHandle` overrides for all three.
5. Remove "(and its with-reference default)" from the `PlainStreams` and `SuperStreams` doc comments.
6. Remove "and sequence queries throw" from the `NeverReadyConsumerSystem` doc comment.

In the test files, delete only the listed overrides; leave every other member untouched:

| File | Delete |
|---|---|
| `AcceptanceTests.cs` | `CreatePlainProducerWithReferenceAsync`, `CreateSuperStreamProducerWithReferenceAsync` |
| `HostingTests.cs` | `CreatePlainProducerWithReferenceAsync`, `CreateSuperStreamProducerWithReferenceAsync`, `QuerySequenceAsync` |
| `PipelineSemanticsTests.cs` | `CreateSuperStreamProducerWithReferenceAsync` |
| `PublishRoutingTests.cs` (`CapturingSystem`) | `CreatePlainProducerWithReferenceAsync`, `CreateSuperStreamProducerWithReferenceAsync`, `QuerySequenceAsync` |
| `PublishRoutingTests.cs` (`NotFoundSystem`) | `CreatePlainProducerWithReferenceAsync`, `CreateSuperStreamProducerWithReferenceAsync`, `QuerySequenceAsync` |
| `TopologyTests.cs` (`ScriptSystem`) | `CreatePlainProducerWithReferenceAsync`, `CreateSuperStreamProducerWithReferenceAsync` |

- [ ] **Step 5: Run the build and tests**

Run: `dotnet build StreamBus.slnx`

Expected: 0 warnings, 0 errors.

Run: `dotnet test tests/StreamBus.UnitTests/StreamBus.UnitTests.csproj`

Expected: all green, including `ProducerConfigTests` and `SystemHandleSurfaceTests`.

- [ ] **Step 6: Commit**

```bash
git add src/StreamBus/ProducerConfigs.cs src/StreamBus/Connector.cs src/StreamBus/StreamBusNaming.cs tests/StreamBus.UnitTests/ProducerConfigTests.cs tests/StreamBus.UnitTests/SystemHandleSurfaceTests.cs tests/StreamBus.UnitTests/Fakes/ScriptedSystemHandle.cs tests/StreamBus.UnitTests/AcceptanceTests.cs tests/StreamBus.UnitTests/HostingTests.cs tests/StreamBus.UnitTests/PipelineSemanticsTests.cs tests/StreamBus.UnitTests/PublishRoutingTests.cs tests/StreamBus.UnitTests/TopologyTests.cs
git commit -m "refactor!: make producers anonymous and remove dedup-only APIs"
```

---

### Task 3: Remove EO remnants from code and tests; cover multi-publisher delivery

**Files:**
- Modify: `src/StreamBus/ProducerHandles.cs:58-65`
- Modify: `tests/StreamBus.UnitTests/ProducerHandleSurfaceTests.cs`
- Modify: `tests/StreamBus.IntegrationTests/ConsumerIntegrationTests.cs:96`
- Modify: `tests/StreamBus.IntegrationTests/ConsumerIntegrationTests.cs` (add the new test after the renamed one)

**Interfaces:**
- No new production interfaces. Uses the existing `BusInstance`, `Body`, `Parse`, `Count`, `WaitForAsync` helpers inside `ConsumerIntegrationTests`.

- [ ] **Step 1: Delete the EO assertions and rewrite the publishing-id comment**

In `tests/StreamBus.UnitTests/ProducerHandleSurfaceTests.cs`:

1. In `Super_handle_keeps_its_routed_members`, delete the `PublishWithIdAsync` and `PublishResultAsync` assertions and the `// EO removal` comment. Keep the `SendAsync` assertion.
2. In `Plain_handle_keeps_envelope_sends`, delete the `PublishResultAsync` assertion and the `// EO removal` comment. Keep the `SendEnvelopeAsync` assertion.
3. Delete the entire `Bus_surface_has_no_effectively_once_members` test.

In `src/StreamBus/ProducerHandles.cs`, replace:

```csharp
// keyed by publishing id. Publishing ids are unique per producer instance
// (Interlocked counter; no dedup in this phase).
```

with:

```csharp
// keyed by publishing id. Publishing ids are sequential per producer handle;
// the client requires one per send and echoes it in the confirmation.
```

- [ ] **Step 2: Rename the integration test to describe what it asserts**

In `tests/StreamBus.IntegrationTests/ConsumerIntegrationTests.cs`, rename:

`Super_stream_splits_across_instances_exactly_once_ordered`

to:

`Super_stream_partitions_distribute_across_instances_without_duplicates`

Do not change its body. Update any reference to the old name in the same file.

- [ ] **Step 3: Add the multi-publisher integration test**

Add this test immediately after the renamed test in `ConsumerIntegrationTests`:

```csharp
[Fact]
public async Task Multiple_publisher_instances_to_one_stream_lose_nothing()
{
    // The real collision regression guard is ProducerConfigTests: every producer
    // config has Reference == null. Two in-process instances share one process,
    // so they cannot reproduce a cross-process producer-name collision. This test
    // pins the anonymous multi-publisher contract end to end.
    var ct = TestContext.Current.CancellationToken;
    var stream = Unique("con-many-pub");
    var admin = await ConnectAdminAsync();
    await using var _ = admin;
    await admin.CreateStream(new StreamSpec(stream));
    _streams.Add(stream);

    await using var first = await BusInstance.StartAsync("pub-a");
    await using var second = await BusInstance.StartAsync("pub-b");

    const int perPublisher = 20;
    await Task.WhenAll(
        PublishLoopAsync(first, stream, perPublisher, ct),
        PublishLoopAsync(second, stream, perPublisher, ct));

    var received = new List<(string Key, int Seq)>();
    await using var consumer = await first.Bus.ConsumeAsync(
        stream,
        new ConsumeOptions { Offset = ConsumeOffset.AlwaysFirst, Reference = Unique("many-read") },
        ctx =>
        {
            var parsed = Parse(ctx.Body);
            lock (received) received.Add(parsed);
            return Task.CompletedTask;
        },
        ct);

    await WaitForAsync(() => Count(received) >= 2 * perPublisher, TimeSpan.FromSeconds(30), "all published messages");
    await Task.Delay(500, ct);

    lock (received)
    {
        Assert.Equal(2 * perPublisher, received.Count);
        Assert.Equal(2 * perPublisher, received.Distinct().Count());
    }

    static async Task PublishLoopAsync(BusInstance instance, string stream, int count, CancellationToken ct)
    {
        for (var i = 0; i < count; i++)
            await instance.Bus.PublishRawAsync(
                stream,
                Body($"{instance.Name}:{i}"),
                new PublishOptions(),
                ct);
    }
}
```

- [ ] **Step 4: Run the builds and tests**

Run: `dotnet test tests/StreamBus.UnitTests/StreamBus.UnitTests.csproj`

Expected: all green.

Run: `dotnet build tests/StreamBus.IntegrationTests/StreamBus.IntegrationTests.csproj`

Expected: 0 warnings, 0 errors.

If Docker is available, run the new test:

`dotnet test tests/StreamBus.IntegrationTests/StreamBus.IntegrationTests.csproj --filter "FullyQualifiedName~Multiple_publisher_instances_to_one_stream_lose_nothing"`

Expected: PASS. If Docker is unavailable, record that the project compiled and leave the run for the final gate.

- [ ] **Step 5: Commit**

```bash
git add src/StreamBus/ProducerHandles.cs tests/StreamBus.UnitTests/ProducerHandleSurfaceTests.cs tests/StreamBus.IntegrationTests/ConsumerIntegrationTests.cs
git commit -m "test: rename partition coverage and add multi-publisher delivery coverage"
```

---

### Task 4: Correct current user-facing documentation

**Files:**
- Modify: `README.md:90,114`
- Modify: `docs/guide/publish.md`
- Modify: `docs/guide/consume.md:15-27`
- Modify: `docs/guide/errors.md:72-74`
- Modify: `docs/llms.txt:24-30` and `:92`
- Modify: `docs/src/content/docs/guides/publishing.mdx`
- Modify: `docs/src/content/docs/guides/consuming.mdx:6-8,27-38`
- Modify: `docs/src/content/docs/index.mdx:24`
- Modify: `examples/README.md:40-44`

**Interfaces:**
- No code interfaces. The publish contract text must state: producers are anonymous; broker published-message deduplication is never enabled; any number of identical instances may publish with unchanged configuration; delivery is at-least-once; applications own duplicate handling and idempotency.
- The consume contract text must state: a super stream with one group distributes partitions across instances with SAC per partition; a plain stream with SAC has one active consumer and standby failover, not throughput scale-out; different groups are independent cursors; consumers must tolerate duplicates.

- [ ] **Step 1: Update the README and the guides**

`README.md`:

- Line 90: change `Endpoints: required groups, scale-out, SAC/offset/store knobs` to `Endpoints: required groups, super-stream partition scale-out, SAC/offset/store knobs`.
- Line 114: replace `the consumer group is required and verbatim - same group scales out, different groups are independent.` with:

```markdown
  the consumer group is required and verbatim. On a super stream, one group
  distributes partitions across instances; on a plain stream, SAC keeps one
  active consumer with standby failover. Different groups are independent
  cursors, and delivery is at-least-once, so consumers must tolerate duplicates.
```

- Add this bullet to the **Features** list, after the typed-publish bullet:

```markdown
- Publishing from many instances: producers are anonymous, so any number of
  identical instances publish safely; the library never enables broker
  published-message deduplication (`docs/guide/publish.md`)
```

`docs/guide/publish.md`: add a new section after the "Plain streams" section:

```markdown
## Publishing from many instances

StreamBus never assigns a producer name. RabbitMQ Streams only deduplicates
published messages for a named producer, so with no name the broker filters
nothing: any number of identical application instances can share this
configuration and publish to the same stream or super stream. Delivery stays
at-least-once; consumers own duplicate handling and idempotency.
```

`docs/guide/consume.md`: replace lines 19-20 and the cross-process caveat with:

```markdown
- A super stream with one group distributes its partitions across instances
  (`SingleActiveConsumer` applies per partition), so adding instances adds
  parallel partition readers.
- A plain stream with the default `SingleActiveConsumer = true` has one active
  consumer per group; other instances stand by for failover and do not add
  throughput. Use a super stream when you need horizontal scale.
- Different groups are independent cursors: each receives every message.
- Delivery is at-least-once. Consumers must tolerate duplicates and own their
  idempotency.

### Cross-process caveat

Same reference, `SingleActiveConsumer = false`, two processes: both receive
deliveries (at-least-once duplicates, never skips). Stores interleave; a stale
process may regress the broker-side offset - never run two writers on one
reference outside SAC.
```

`docs/guide/errors.md`: change `exactly-once decision` to `one-time decision`.

`docs/llms.txt`:

- Replace the consumer-identity scale-out sentence with the super-stream vs plain-stream model from `consume.md`.
- Add after the typed-publish bullet:

```text
- Producers are anonymous: StreamBus never assigns a producer name, so the
  broker's published-message deduplication is never enabled. Any number of
  identical instances may publish to the same stream with unchanged
  configuration. Delivery is at-least-once; applications own duplicate handling
  and idempotency.
```

- Keep the existing delivery-semantics bullet (`Applications own deduplication and idempotency everywhere`).

`docs/src/content/docs/guides/publishing.mdx`: add the same "Many instances" section as `publish.md`.

`docs/src/content/docs/guides/consuming.mdx`:

- Change the intro to: `The consumer group is required and verbatim. On a super stream, one group distributes partitions across instances; different groups consume independently.`
- Replace the "Scale out" section with:

```markdown
## Scale out

Super streams distribute partitions, not individual messages. With one group,
instances take ownership of different partitions (`SingleActiveConsumer`
applies per partition), so adding instances reads more partitions in parallel.

Plain streams are different: with the default `SingleActiveConsumer = true`,
one instance owns the group's cursor and the others stand by for failover.
They do not add throughput. Use a super stream when you need horizontal scale.

A second endpoint with a different group reads the stream from its own offset.
Delivery is at-least-once, so consumers must tolerate duplicates.
```

`docs/src/content/docs/index.mdx`: change the "Consumer Groups" card to:

```markdown
`ReceiveEndpoint`s with first-class, required consumer-group identity — a super
stream's partitions scale across instances; different groups are independent.
```

`examples/README.md`: replace the consumer-group note with:

```markdown
- Consumer groups are declared verbatim. On the super stream, two consumer
  instances with the same group split partitions; on the plain `orders`
  stream, one instance is active and the second stands by for failover. A
  different group gets an independent cursor that receives every message again.
```

- [ ] **Step 2: Verify the current docs describe the new contract and no longer claim plain-stream scale-out**

Run:

```bash
grep -rniE "distribut(e|es) (messages|work) across instances|same group scales out|shared cursor" README.md docs/guide docs/llms.txt docs/src/content/docs examples/README.md
```

Expected: no output. No current doc claims that a plain stream distributes messages or work across instances.

Run:

```bash
grep -rniE "producer name|named producer|deduplication" README.md docs/guide docs/llms.txt docs/src/content/docs
```

Expected: only the explicit negative contract statements ("never assigns a producer name", "never enables ... deduplication") and application-owned duplicate handling.

- [ ] **Step 3: Commit**

```bash
git add README.md docs/guide/publish.md docs/guide/consume.md docs/guide/errors.md docs/llms.txt docs/src/content/docs/guides/publishing.mdx docs/src/content/docs/guides/consuming.mdx docs/src/content/docs/index.mdx examples/README.md
git commit -m "docs: document anonymous publishers and the consumer scale-out model"
```

---

### Task 5: Scrub the dated design, requirements, and acceptance records

**Files:**
- Modify: `docs/2026-10-08-streambus-design.md`
- Modify: `docs/2026-10-08-streambus-requirements.md`
- Modify: `docs/2026-10-13-streambus-acceptance.md`

**Interfaces:**
- No code interfaces. Scrub rule below.

**Scrub rule (applies to Tasks 5-7):**

- Delete material that exists only for producer deduplication, stable producer names, EO read-process-write/result publishing, `EffectivelyOnce*`, `ConsumeEffectivelyOnceAsync`, `PublishResult*`, `ResultPublisher`, `ResultProducer`, `ResultNaming`, `requiresContinuation`, `DeduplicationName`, `StreamBusNaming`, `PublishReference`, and the three removed handle members.
- `PublishWithIdAsync` and `PublishReplayAsync` belong to the already-removed replay feature, not to EO. Keep the surrounding replay narrative in the remove-replay records, but replace those symbols with prose (for example, "the replay publishing method"). The symbol gate still requires zero hits.
- When a whole section, task, requirement, gate row, or test list entry exists only for that material, delete it.
- Rewrite incidental sentences without the concept. Do not renumber unrelated IDs unless the file becomes incoherent.
- Keep generic "exactly once" statements about a single invocation or a single offset advance (`Host` called once, `next()` called once, offset advances once). Rewrite delivery claims ("consumed exactly once", "effectively-once end to end") to the at-least-once/no-loss contract.
- Rename references to the old integration test name to the new name from Task 3.

- [ ] **Step 1: Scrub `docs/2026-10-08-streambus-design.md`**

Delete the entire `## 7. Effectively-once read-process-write` section and every reference to §7, FR-6, and G-2 (including the phase table row and the gate summary sentence). In the API sketch, delete the `DeduplicationName` field and both `PublishResultAsync` lines. Rewrite the surrounding sentences so the remaining producer/consumer contract is coherent and at-least-once.

- [ ] **Step 2: Scrub `docs/2026-10-08-streambus-requirements.md`**

Delete the `FR-6 Effectively-once read-process-write` section and the summary bullet. Delete the `G-2` row. In FR-10.9, remove the `PublishResultAsync` result-path clause. Delete FR-10.14. Rewrite FR-2.3's "consumed exactly once" to "consumed without loss". Keep generic "offset advances exactly once" rows.

- [ ] **Step 3: Scrub `docs/2026-10-13-streambus-acceptance.md`**

Delete the FR-6.1, FR-6.3, FR-6.8, and G-2 rows. Remove `EffectivelyOnce*`, `ResultPublisher*`, `PublishResultAsync`, `QuerySequence`, and `ResultProducerRegistry` mentions from the test lists and prose. Rename `Super_stream_splits_across_instances_exactly_once_ordered` to `Super_stream_partitions_distribute_across_instances_without_duplicates`. Remove "effectively-once" from the runtime-rows sentence. Rewrite "exactly once" delivery claims to "without loss".

- [ ] **Step 4: Verify the dated records**

Run:

```bash
grep -rniE "EffectivelyOnce|effectively-once|ConsumeEffectivelyOnceAsync|PublishResult|ResultPublisher|ResultProducer|PublishWithIdAsync|requiresContinuation|DeduplicationName|StreamBusNaming|PublishReference|QuerySequenceAsync" docs/2026-10-08-streambus-design.md docs/2026-10-08-streambus-requirements.md docs/2026-10-13-streambus-acceptance.md
```

Expected: no output.

- [ ] **Step 5: Commit**

```bash
git add docs/2026-10-08-streambus-design.md docs/2026-10-08-streambus-requirements.md docs/2026-10-13-streambus-acceptance.md
git commit -m "docs: align dated records with at-least-once delivery"
```

---

### Task 6: Delete EO-only plans and scrub the remaining historical plans

**Files:**
- Delete: `docs/superpowers/plans/2026-10-13-streambus-phase7-read-process-write.md`
- Delete: `docs/superpowers/plans/2026-11-07-remove-effectively-once.md`
- Modify: `docs/superpowers/plans/2026-10-11-streambus-phase4-producer.md`
- Modify: `docs/superpowers/plans/2026-10-11-streambus-phase5-consumer.md`
- Modify: `docs/superpowers/plans/2026-10-13-streambus-phase6-retry.md`
- Modify: `docs/superpowers/plans/2026-10-13-streambus-phase9-typed-consumer.md`
- Modify: `docs/superpowers/plans/2026-10-13-streambus-phase10-acceptance.md`
- Modify: `docs/superpowers/plans/2026-10-13-streambus-v2-user-centric-api.md`
- Modify: `docs/superpowers/plans/2026-10-17-streambus-registration-owned-stream-spec.md`
- Modify: `docs/superpowers/plans/2026-10-19-streambus-v2-architecture-and-middleware.md`
- Modify: `docs/superpowers/plans/2026-10-25-streambus-typed-filters.md`
- Modify: `docs/superpowers/plans/2026-10-27-streambus-single-filter-context.md`
- Modify: `docs/superpowers/plans/2026-10-31-streambus-message-context.md`
- Modify: `docs/superpowers/plans/2026-11-09-remove-error-replay-and-requeue.md`

- [ ] **Step 1: Delete the two EO-only plans**

```bash
git rm docs/superpowers/plans/2026-10-13-streambus-phase7-read-process-write.md docs/superpowers/plans/2026-11-07-remove-effectively-once.md
```

- [ ] **Step 2: Scrub the remaining plans with the Task 5 rule**

Apply the rule to every hit. Specific guidance:

- **phase4-producer:** delete the deferred `DeduplicationName`/`PublishResultAsync` clauses (lines ~20, 52, 56, 994, 998). Keep the confirm-tracker facts, but remove the "Phase 7 note" EO sentences.
- **phase5-consumer:** rename the test name and code-block occurrence to the Task 3 name; change "exactly-once" in the coverage description to "no-loss"; delete the read-process-write note at line ~1724.
- **phase6-retry:** keep the envelope handoff content. Delete the `PublishWithIdAsync`/replay-bearing lines (~48, 222, 345, 387, 833, 881) and the stable FR-6 result-path naming clause. Keep generic "advance-store exactly once" comments.
- **phase9-typed-consumer:** delete the EO typed-path clauses (~19, 48).
- **phase10-acceptance:** remove `EffectivelyOnceTests`, `ResultPublisherTests`, `EffectivelyOnceIntegrationTests`, and other EO names from the test lists; delete the G-2 evidence row; delete the `PublishResultAsync` stub; remove the `ResultProducerRegistry` constructor argument from the sample. Keep the sample coherent.
- **v2-user-centric-api:** delete section A.8 in full; remove EO and `ResultNaming` clauses from the other hits (~276, 320, 355).
- **registration-owned-stream-spec:** change "exactly-once destination rule/tier" to "one-time destination rule/tier"; remove `docs/guide/effectively-once.md` from the docs list; change "Q5 - exactly-once dead-letter destination" to "one-time".
- **v2-architecture-and-middleware:** remove the EO item from the preserve list, remove the EO producer-surface members from the interface proposal, and delete the EO naming-utility task/section (`EffectivelyOnceProducerName`, `PublishReference`, `ReplayNaming`). Remove the EO filter-skip rejection and the `EffectivelyOnce.cs`/`ResultProducerRegistry` modifications. Rewrite "no filtering, reordering, deduplication, or cap" without "deduplication".
- **typed-filters:** remove "effectively-once rejection of `CanSkip`" and "raw and effectively-once" clauses. The remaining text should say raw pipelines only.
- **single-filter-context:** delete `requiresContinuation` from the proposed constructor and every reference; delete the three EO tests; delete the EO call-site step; delete `PublishResultOptions` retry-knob material; remove `EffectivelyOnce.cs`, `ResultPublisher.cs`, `EffectivelyOnceTests.cs`, and `ResultPublisherTests.cs` from file lists; delete EO checklist items. Keep the universal and typed filter-context content.
- **message-context:** remove `EffectivelyOnceTests`, `EffectivelyOnceIntegrationTests`, `ReplayAndEffectivelyOnceReattachTests`, and `ResultProducerRegistry` from test/file lists; remove `requiresContinuation` and the `effectivelyOnce` parameter; remove `DispatchEffectivelyOnceAsync`; delete the EO half of the routing-key test; remove "effectively-once result" from the headers row.
- **remove-error-replay-and-requeue:** replace the reference to the EO plan with "a prior plan"; replace `PublishWithIdAsync` and `PublishReplayAsync` with prose ("the replay publishing method"); remove `StreamBusNaming` from the removed-symbol list and from the final grep command. Keep the replay/requeue content.

- [ ] **Step 3: Verify the historical plans**

Run:

```bash
grep -rniE "EffectivelyOnce|effectively-once|ConsumeEffectivelyOnceAsync|PublishResult|ResultPublisher|ResultProducer|ResultNaming|PublishWithIdAsync|PublishReplayAsync|requiresContinuation|DeduplicationName|StreamBusNaming|PublishReference|CreatePlainProducerWithReferenceAsync|CreateSuperStreamProducerWithReferenceAsync|QuerySequenceAsync|SequenceQueries|SequenceResult" docs/superpowers/plans --exclude='2026-11-09-remove-producer-deduplication*'
```

Expected: no output. (Generic "exactly once" may remain; this change's own plan is excluded until Task 8 deletes it.)

- [ ] **Step 4: Commit**

Use `git add -u` so this change's own untracked plan is not staged:

```bash
git add -u docs/superpowers/plans
git commit -m "docs: remove obsolete read-process-write plans and align historical plans"
```

---

### Task 7: Scrub the historical specs

**Files:**
- Modify: `docs/superpowers/specs/2026-10-18-streambus-v2-architecture-and-middleware-design.md`
- Modify: `docs/superpowers/specs/2026-10-25-streambus-typed-filter-design.md`
- Modify: `docs/superpowers/specs/2026-10-25-streambus-wire-metadata-design.md`
- Modify: `docs/superpowers/specs/2026-10-27-streambus-single-filter-context-design.md`
- Modify: `docs/superpowers/specs/2026-10-31-streambus-message-context-design.md`
- Modify: `docs/superpowers/specs/2026-11-09-remove-error-replay-and-requeue-design.md`

- [ ] **Step 1: Scrub each spec with the Task 5 rule**

Specific guidance:

- **v2-architecture design:** remove the EO preserve item, the EO resume-query paragraph, the `PublishResultAsync` confirm-timeout paragraph, the EO health registration row, the producer-naming utility paragraph, the EO members from the producer-surface split, the EO result-publish exception, the effectively-once outer-delegate clause, the EO skip-rejection rules, the C5 section, and the EO part of M6. Keep the plain/super surface split itself.
- **typed-filter design:** remove "effectively-once rejection" from the preserved list; remove `ConsumeEffectivelyOnceAsync` from the non-goal; change "raw consume / effectively-once" to "raw consume"; keep the filter-composition deduplication sentence only if it does not describe broker dedup (otherwise delete the word).
- **wire-metadata design:** delete the two EO result-publish clauses; keep the error-envelope metadata contract.
- **single-filter-context design:** delete the EO filter-contract section, the `requiresContinuation` proposal and all references, the EO zero-`next` rules, the `PublishResultOptions` knob table row, and the EO doc/checklist items. Keep universal and typed filter context content.
- **message-context design:** delete the EO result/envelope clauses. Keep the normal publish and consume context identity.
- **remove-error-replay design:** replace the EO-plan reference and `PublishWithIdAsync`/`StreamBusNaming` mentions the same way as Task 6. Keep replay/requeue content.

- [ ] **Step 2: Verify the historical specs**

Run:

```bash
grep -rniE "EffectivelyOnce|effectively-once|ConsumeEffectivelyOnceAsync|PublishResult|ResultPublisher|ResultProducer|ResultNaming|PublishWithIdAsync|PublishReplayAsync|requiresContinuation|DeduplicationName|StreamBusNaming|PublishReference|CreatePlainProducerWithReferenceAsync|CreateSuperStreamProducerWithReferenceAsync|QuerySequenceAsync|SequenceQueries|SequenceResult" docs/superpowers/specs --exclude='2026-11-09-remove-producer-deduplication*'
```

Expected: no output. (This change's own design is excluded until Task 8 deletes it.)

- [ ] **Step 3: Commit**

Use `git add -u` so this change's own untracked design is not staged:

```bash
git add -u docs/superpowers/specs
git commit -m "docs: align historical specs with the current producer model"
```

---

### Task 8: Final cleanup, full verification, and remove this change's planning files

**Files:**
- Delete: `.superpowers/sdd/2026-10-31-streambus-message-context/` (untracked)
- Delete: `bin/`, `obj/`, `artifacts/test-results/` (generated/untracked)
- Delete: `docs/superpowers/specs/2026-11-09-remove-producer-deduplication-design.md`
- Delete: `docs/superpowers/plans/2026-11-09-remove-producer-deduplication.md`

- [ ] **Step 1: Remove the scratch and generated artifacts**

```bash
rm -rf .superpowers/sdd/2026-10-31-streambus-message-context
rm -rf artifacts/test-results
find . -type d \( -name bin -o -name obj \) -not -path './.git/*' -prune -exec rm -rf {} +
```

- [ ] **Step 2: Run the full verification**

```bash
dotnet build StreamBus.slnx
dotnet test tests/StreamBus.UnitTests/StreamBus.UnitTests.csproj
dotnet build tests/StreamBus.IntegrationTests/StreamBus.IntegrationTests.csproj
```

Expected: build 0 warnings/0 errors; all unit tests green; integration project builds clean. If Docker is available, also run:

```bash
dotnet test tests/StreamBus.IntegrationTests/StreamBus.IntegrationTests.csproj
```

Expected: all green.

- [ ] **Step 3: Delete this change's design and plan**

Verification (Step 2) is complete, so the planning artifacts can go:

```bash
rm docs/superpowers/specs/2026-11-09-remove-producer-deduplication-design.md
rm docs/superpowers/plans/2026-11-09-remove-producer-deduplication.md
```

- [ ] **Step 4: Delete the regenerated build output before the gate**

```bash
find . -type d \( -name bin -o -name obj \) -not -path './.git/*' -prune -exec rm -rf {} +
```

- [ ] **Step 5: Run the symbol gate and source scans**

```bash
grep -rniE "EffectivelyOnce|effectively-once|ConsumeEffectivelyOnceAsync|PublishResult|ResultPublisher|ResultProducer|ResultNaming|PublishWithIdAsync|PublishReplayAsync|requiresContinuation|DeduplicationName|StreamBusNaming|PublishReference|CreatePlainProducerWithReferenceAsync|CreateSuperStreamProducerWithReferenceAsync|QuerySequenceAsync|SequenceQueries|SequenceResult" \
  --exclude-dir=.git --exclude-dir=bin --exclude-dir=obj --exclude-dir=node_modules --exclude-dir=artifacts .
```

Expected: no output.

Run:

```bash
grep -rn "Reference =" src/StreamBus --include='*.cs'
```

Expected: only consumer/descriptor assignments (`StreamBusConfigurator`, `ConsumerRegistry`, `Hosting`, `StreamPartitionConsumer`). No hit inside `ProducerConfigs.cs` and no producer config assignment in `Connector.cs`.

Run:

```bash
grep -rniE "deduplicat|dedup" src tests --include='*.cs'
```

Expected: no output.

- [ ] **Step 6: Review the diff and commit**

```bash
git status --short
git diff --stat
git diff
```

Confirm:

- No `bin/`, `obj/`, `artifacts/`, or `.superpowers/` files are staged.
- No producer `Reference` assignment remains.
- The only tracked deletions are the two EO-only plans.
- The unit suite and build results from Step 2 are recorded in the task notes.

```bash
git add -A
git commit -m "chore: clean build artifacts and remove change planning docs"
```

---

## Self-Review Notes

- **Spec coverage:** anonymous producers (Tasks 1-2), removed handle members and `StreamBusNaming` (Task 2), EO code/test remnants (Task 3), current docs (Task 4), dated records (Task 5), historical plans (Task 6), historical specs (Task 7), final gate and artifacts (Task 8).
- **Type consistency:** `ProducerConfigs.Plain` and `ProducerConfigs.Super` use the same names and parameter order in Tasks 1, 2, and the tests. `RawProducerConfig.Stream`, `RawSuperStreamProducerConfig.SuperStream`, `MessagesBufferSize`, `Reference`, `Routing`, `ConfirmHandler`, and `ConnectionClosedHandler` are public members of client 1.12.1 (verified by reflection).
- **Review Focus mapping:** lines 1-5 map to Task 2 tests, Task 2 surface tests, Task 2 routing test, existing confirm suites re-run in Task 2/8, and the Task 3 integration test.
