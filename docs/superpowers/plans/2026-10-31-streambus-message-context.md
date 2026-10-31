# StreamBus Message Context Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use `subagent-driven-development` (recommended) or `executing-plans` to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace every publish and consume context with one flat, MassTransit-shaped surface backed by AMQP properties, and make super-stream routing keys visible to consumers.

**Architecture:** Two context families, one instance per direction: `ConsumeContext` / `ConsumeContext<T>` and `PublishContext` / `PublishContext<T>`. Ids and the send time live in AMQP `message-id`, `correlation-id`, and `creation-time`; content type in `content-type`; everything else, including `streambus.routing-key`, in application properties. Wire encoding is confined to `StreamMessageProperties.ApplyTo` and `StreamWire`. Public types are `Guid` / `Guid?` and `DateTime` / `DateTime?`.

**Tech Stack:** C# / .NET 10, RabbitMQ.Stream.Client 1.12.1, xUnit.

**Spec:** `docs/superpowers/specs/2026-10-31-streambus-message-context-design.md`

## Global Constraints

- `dotnet build StreamBus.slnx` must report **0 warnings** before any task is considered done.
- Unit suite: `dotnet test tests/StreamBus.UnitTests/StreamBus.UnitTests.csproj`.
- Integration suite: `dotnet test tests/StreamBus.IntegrationTests/StreamBus.IntegrationTests.csproj` (needs a broker; when absent, the existing lane skips).
- Ids cross the wire as `Guid.ToString("D")` strings. No `Guid` is ever handed to `WriteAny`.
- `SentTime` is truncated to whole milliseconds at generation so publish and consume values are identical.
- Public surface must not contain `Raw`, `Metadata`, `MessageMetadata`, `OutputHeaders`, `MessageType`, `RuntimeMessageType`, `Bus`, `IStreamHeaderWriter`, `StreamConsumeFilterContext`, or `StreamPublishFilterContext` after Task 4.
- Contexts are immutable except headers: every member except the header bags is `init`-only.
- Reserved header keys, rejected on every write path: `streambus.message-id`, `streambus.correlation-id`, `streambus.sent-time`, `streambus.routing-key`.
- One context instance per delivery and per publish: filters and the handler/terminal receive the same object.
- Framework values are written after user headers so a user header can never override them.
- Changelog entry goes under a new `## 0.3.0` section.

## Review Focus

- A foreign producer writes a native AMQP UUID (16 bytes) or a non-Guid string as `message-id`; both must decode to `Guid?` without throwing.
- A message arrives with no `creation-time`; the consume context must expose `SentTime == null`, never `DateTime.MinValue`.
- A user passes a reserved header through `PublishOptions.Properties.Headers`, bypassing filters; it must be rejected exactly like `StreamSendHeaders.Set`.
- A typed consumer's body fails to deserialize while a typed filter is attached; the failure must follow retry/exhaustion, not recycle the attach.
- A filter sets `SendHeaders` and then throws; the failed attempt's headers must not leak into the next attempt, so the error envelope carries only the final attempt's headers.

---

### Task 1: Header surface

**Files:**
- Create: `src/StreamBus/Headers.cs`
- Test: `tests/StreamBus.UnitTests/HeadersTests.cs`

**Interfaces:**
- Consumes: `SystemTextJsonStreamSerializer` (`src/StreamBus/Serialization.cs`), `StreamMessageProperties.ValidateValue` (`src/StreamBus/MessageProperties.cs`).
- Produces:
  - `public interface IStreamHeaders { IReadOnlyDictionary<string, object?> All { get; } bool TryGet(string key, out object? value); string? GetString(string key); int? GetInt(string key); long? GetLong(string key); bool? GetBool(string key); T? GetJson<T>(string key); }`
  - `public interface IStreamSendHeaders : IStreamHeaders { void Set(string key, object? value); bool Remove(string key); }`
  - `internal static class StreamHeaderKeys { const string MessageId/CorrelationId/SentTime/RoutingKey; static void EnsureNotReserved(string key); }` with values `streambus.message-id`, `streambus.correlation-id`, `streambus.sent-time`, `streambus.routing-key`.
  - `internal abstract class StreamHeaderBag : IStreamHeaders` (accessor logic, protected `IStreamSerializer Serializer`, abstract `IReadOnlyDictionary<string, object?> Values`).
  - `internal sealed class StreamHeaders : StreamHeaderBag` — ctor `(IReadOnlyDictionary<string, object?> values, IStreamSerializer serializer)`; read-only.
  - `internal sealed class StreamSendHeaders : StreamHeaderBag, IStreamSendHeaders` — ctor `(IReadOnlyDictionary<string, object?>? initial, IStreamSerializer serializer)`; `Set` rejects null/whitespace keys, reserved keys, and unsupported value types; `internal void Reset()`, `internal IReadOnlyDictionary<string, object?> Snapshot()`, `internal StreamMessageProperties Materialize(StreamMessageProperties? callLevel)`.

- [ ] **Step 1: Write the failing tests**

Create `tests/StreamBus.UnitTests/HeadersTests.cs` with `HeaderPayload`-shaped assertions. Include these facts, using `SystemTextJsonStreamSerializer.Default` unless stated:

```csharp
[Fact]
public void Read_accessors_return_the_legal_value_set()
{
    var headers = new StreamHeaders(new Dictionary<string, object?>
    {
        ["text"] = "value",
        ["int"] = 42,
        ["long"] = 9_000_000_000L,
        ["bool"] = true,
        ["json"] = "{\"Id\":7}",
        ["not-a-number"] = "42",
    }, SystemTextJsonStreamSerializer.Default);

    Assert.Equal("value", headers.GetString("text"));
    Assert.Equal(42, headers.GetInt("int"));
    Assert.Null(headers.GetInt("long"));
    Assert.Equal(42L, headers.GetLong("int"));
    Assert.Equal(9_000_000_000L, headers.GetLong("long"));
    Assert.True(headers.GetBool("bool"));
    Assert.Null(headers.GetBool("text"));
    Assert.Null(headers.GetInt("not-a-number"));
    Assert.Null(headers.GetString("missing"));
    Assert.Equal(7, headers.GetJson<HeaderPayload>("json")!.Id);
}

[Fact]
public void GetJson_uses_the_configured_serializer()
{
    var camelCase = new SystemTextJsonStreamSerializer(new JsonSerializerOptions
    {
        PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
    });
    var headers = new StreamHeaders(new Dictionary<string, object?>
    {
        ["payload"] = "{\"id\":7}",
    }, camelCase);

    Assert.Equal(7, headers.GetJson<HeaderPayload>("payload")!.Id);
}

[Fact]
public void GetJson_invalid_json_names_the_key_and_keeps_the_json_exception()
{
    var headers = new StreamHeaders(new Dictionary<string, object?>
    {
        ["payload"] = "{not-json",
    }, SystemTextJsonStreamSerializer.Default);

    var ex = Assert.Throws<ArgumentException>(() => headers.GetJson<HeaderPayload>("payload"));
    Assert.Contains("payload", ex.Message, StringComparison.Ordinal);
    Assert.IsType<JsonException>(ex.InnerException);
}

[Theory]
[InlineData("streambus.message-id")]
[InlineData("streambus.correlation-id")]
[InlineData("streambus.sent-time")]
[InlineData("streambus.routing-key")]
public void Send_headers_reject_framework_keys(string key)
{
    var headers = new StreamSendHeaders(null, SystemTextJsonStreamSerializer.Default);
    var ex = Assert.Throws<ArgumentException>(() => headers.Set(key, "spoofed"));
    Assert.Contains(key, ex.Message, StringComparison.Ordinal);
}

[Fact]
public void Send_headers_reject_illegal_value_types()
{
    var headers = new StreamSendHeaders(null, SystemTextJsonStreamSerializer.Default);
    var ex = Assert.Throws<ArgumentException>(() => headers.Set("tenant", 1.5));
    Assert.Contains("tenant", ex.Message, StringComparison.Ordinal);
    Assert.Contains("Double", ex.Message, StringComparison.Ordinal);
}

[Fact]
public void Send_headers_support_set_get_remove()
{
    var headers = new StreamSendHeaders(new Dictionary<string, object?> { ["seed"] = 1 },
        SystemTextJsonStreamSerializer.Default);
    headers.Set("tenant", "acme");
    Assert.True(headers.TryGet("tenant", out var value));
    Assert.Equal("acme", value);
    Assert.True(headers.Remove("tenant"));
    Assert.False(headers.TryGet("tenant", out _));
    headers.Reset();
    Assert.Empty(headers.All);
}

[Fact]
public void Materialize_lets_call_level_headers_win()
{
    var callLevel = new StreamMessageProperties
    {
        Headers = new Dictionary<string, object?> { ["tenant"] = "call-level", ["call-only"] = true },
    };
    var headers = new StreamSendHeaders(callLevel.Headers, SystemTextJsonStreamSerializer.Default);
    headers.Set("tenant", "filter-level");
    headers.Set("filter-only", 7);

    var merged = headers.Materialize(callLevel)!;
    Assert.Equal("call-level", merged.Headers["tenant"]);
    Assert.Equal(true, merged.Headers["call-only"]);
    Assert.Equal(7, merged.Headers["filter-only"]);
}

[Fact]
public void All_includes_framework_and_user_keys()
{
    var headers = new StreamHeaders(new Dictionary<string, object?>
    {
        ["streambus.routing-key"] = "key-a",
        ["x-tenant"] = "acme",
    }, SystemTextJsonStreamSerializer.Default);

    Assert.Equal("acme", headers.All["x-tenant"]);
    Assert.Equal("key-a", headers.All["streambus.routing-key"]);
}
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `dotnet test tests/StreamBus.UnitTests/StreamBus.UnitTests.csproj --filter FullyQualifiedName~HeadersTests`
Expected: FAIL to compile — `IStreamHeaders`, `StreamHeaders`, and `StreamSendHeaders` do not exist.

- [ ] **Step 3: Implement `src/StreamBus/Headers.cs`**

Declare the two interfaces and the key constants exactly as in the Interfaces block. `StreamHeaderBag` holds the accessor logic copied from `MessageMetadata` (`GetString`/`GetInt`/`GetLong`/`GetBool`/`GetJson`), reading `Values` and `Serializer`. `StreamSendHeaders.Set` calls, in order: `ArgumentException.ThrowIfNullOrWhiteSpace(key)`, `StreamHeaderKeys.EnsureNotReserved(key)`, `StreamMessageProperties.ValidateValue(key, value)`, then writes to its `Dictionary<string, object?>(StringComparer.Ordinal)`. `Materialize` copies the filter headers, overlays `callLevel.Headers` on top, and returns `(callLevel ?? new StreamMessageProperties()) with { Headers = merged }`.

`EnsureNotReserved` throws `ArgumentException` with message `Header '{key}' is reserved by StreamBus and cannot be set by user code.`

- [ ] **Step 4: Run the tests to verify they pass**

Run: `dotnet test tests/StreamBus.UnitTests/StreamBus.UnitTests.csproj --filter FullyQualifiedName~HeadersTests`
Expected: PASS (8 tests).

- [ ] **Step 5: Build the solution**

Run: `dotnet build StreamBus.slnx`
Expected: 0 warnings, 0 errors.

- [ ] **Step 6: Commit**

```bash
git add src/StreamBus/Headers.cs tests/StreamBus.UnitTests/HeadersTests.cs
git commit -m "feat: add read and send header interfaces with reserved-key guards"
```

---

### Task 2: AMQP property encoding

**Files:**
- Modify: `src/StreamBus/MessageProperties.cs`
- Modify: `src/StreamBus/Serialization.cs`
- Modify: `src/StreamBus/ProducerHandles.cs:200-235, 318-338`
- Modify: `src/StreamBus/Bus.cs:150-245`
- Create: `src/StreamBus/StreamWire.cs`
- Test: `tests/StreamBus.UnitTests/WireEncodingTests.cs`
- Modify: `tests/StreamBus.UnitTests/HeaderCarrierTests.cs`
- Modify: `tests/StreamBus.UnitTests/PublishPipelineTests.cs:276-297`

**Interfaces:**
- Consumes: `StreamHeaderKeys`, `IStreamSendHeaders` (Task 1).
- Produces:
  - `StreamMessageProperties` becomes `{ Guid? MessageId; Guid? CorrelationId; DateTime? SentTime; internal string? ContentType; IReadOnlyDictionary<string, object?> Headers; }`.
  - `internal static void StreamMessageProperties.ApplyTo(RabbitMQ.Stream.Client.Message message, StreamMessageProperties? properties, string? routingKey = null)`
  - `internal static void StreamMessageProperties.ValidateHeaders(IReadOnlyDictionary<string, object?>? headers)`
  - `internal static class StreamWire { Guid? ReadGuid(object? value); DateTime? ReadSentTime(RabbitMQ.Stream.Client.AMQP.Properties? properties); DateTime NormalizeSentTime(DateTime value); }`
  - `IStreamSerializer.ContentType` (`string`, never null or whitespace).

- [ ] **Step 1: Write the failing tests**

Create `tests/StreamBus.UnitTests/WireEncodingTests.cs`:

```csharp
using RabbitMQ.Stream.Client;
using RabbitMQ.Stream.Client.AMQP;

[Fact]
public void Publish_writes_ids_sent_time_and_content_type_to_amqp_properties()
{
    var messageId = Guid.Parse("0f8fad5b-d9cb-469f-a165-70867728950e");
    var correlationId = Guid.Parse("6b1f5c1a-0d9e-4b6f-9a4e-1f2d3c4b5a69");
    var sentTime = new DateTime(2026, 10, 31, 9, 12, 3, 284, DateTimeKind.Utc);
    var message = new Message(new byte[] { 1 });

    StreamMessageProperties.ApplyTo(message, new StreamMessageProperties
    {
        MessageId = messageId,
        CorrelationId = correlationId,
        SentTime = sentTime,
        ContentType = "application/json",
        Headers = new Dictionary<string, object?> { ["x-tenant"] = "acme" },
    });

    Assert.Equal("0f8fad5b-d9cb-469f-a165-70867728950e", message.Properties!.MessageId);
    Assert.Equal("6b1f5c1a-0d9e-4b6f-9a4e-1f2d3c4b5a69", message.Properties.CorrelationId);
    Assert.Equal(sentTime, message.Properties.CreationTime);
    Assert.Equal("application/json", message.Properties.ContentType);
    Assert.Equal("acme", message.ApplicationProperties!["x-tenant"]);
    Assert.False(message.ApplicationProperties.ContainsKey("streambus.message-id"));
}

[Fact]
public void Publish_generates_no_properties_section_when_nothing_to_write()
{
    var message = new Message(new byte[] { 1 });
    StreamMessageProperties.ApplyTo(message, new StreamMessageProperties());
    Assert.Null(message.Properties);
    Assert.Null(message.ApplicationProperties);
}

[Fact]
public void Sent_time_survives_the_wire_round_trip()
{
    var sentTime = StreamWire.NormalizeSentTime(DateTime.UtcNow);
    var message = new Message(new byte[] { 1 });
    StreamMessageProperties.ApplyTo(message, new StreamMessageProperties { SentTime = sentTime });
    Assert.Equal(sentTime, StreamWire.ReadSentTime(message.Properties));
    Assert.Equal(DateTimeKind.Utc, StreamWire.ReadSentTime(message.Properties)!.Value.Kind);
}

[Fact]
public void Absent_creation_time_reads_as_null()
{
    Assert.Null(StreamWire.ReadSentTime(new Properties()));
    Assert.Null(StreamWire.ReadSentTime(null));
}

[Fact]
public void Message_ids_parse_from_strings_and_native_uuids()
{
    var expected = Guid.Parse("0f8fad5b-d9cb-469f-a165-70867728950e");
    Assert.Equal(expected, StreamWire.ReadGuid(expected.ToString("D")));

    // AMQP 1.0 UUID is network byte order; .NET Guid is little-endian in its
    // first three groups, so the native form must be converted, not copied.
    var amqpUuid = new byte[]
    {
        0x0f, 0x8f, 0xad, 0x5b, 0xd9, 0xcb, 0x46, 0x9f,
        0xa1, 0x65, 0x70, 0x86, 0x77, 0x28, 0x95, 0x0e,
    };
    Assert.Equal(expected, StreamWire.ReadGuid(amqpUuid));
}

[Fact]
public void Non_guid_message_ids_read_as_null()
{
    Assert.Null(StreamWire.ReadGuid("m-1"));
    Assert.Null(StreamWire.ReadGuid(new byte[] { 1, 2, 3 }));
    Assert.Null(StreamWire.ReadGuid(42));
    Assert.Null(StreamWire.ReadGuid(null));
}

[Fact]
public void Routing_key_is_written_as_an_application_property()
{
    var message = new Message(new byte[] { 1 });
    StreamMessageProperties.ApplyTo(message, new StreamMessageProperties(), routingKey: "key-a");
    Assert.Equal("key-a", message.ApplicationProperties!["streambus.routing-key"]);
}

[Fact]
public void Reserved_user_headers_are_rejected_before_the_send()
{
    var headers = new Dictionary<string, object?> { ["streambus.routing-key"] = "spoofed" };
    var ex = Assert.Throws<ArgumentException>(() => StreamMessageProperties.ValidateHeaders(headers));
    Assert.Contains("streambus.routing-key", ex.Message, StringComparison.Ordinal);
    StreamMessageProperties.ValidateHeaders(null); // must not throw
}

[Fact]
public void Sent_time_is_truncated_to_milliseconds()
{
    var unrounded = new DateTime(2026, 10, 31, 9, 12, 3, 284, DateTimeKind.Utc).AddTicks(6197);
    var normalized = StreamWire.NormalizeSentTime(unrounded);
    Assert.Equal(TimeSpan.Zero, TimeSpan.FromTicks(normalized.Ticks % TimeSpan.TicksPerMillisecond));
    Assert.Equal(DateTimeKind.Utc, normalized.Kind);
}

[Fact]
public void Json_serializer_reports_application_json()
    => Assert.Equal("application/json", SystemTextJsonStreamSerializer.Default.ContentType);
```

Then update `HeaderCarrierTests.cs`: change `MessageId = "m-1"` / `"m-2"` to `Guid.Parse(...)` values, assert `message.Properties!.MessageId` equals `id.ToString("D")`, and add `Assert.Equal("application/json", message.Properties.ContentType)`. Add `public string ContentType => "application/json";` to `RecordingSerializer`.

Rewrite `PublishPipelineTests.Header_writer_merges_call_level_properties_and_call_level_wins` to use `StreamSendHeaders` instead of `HeaderWriter`, keep the header assertions, and assert `merged.MessageId` equals the `Guid` supplied on `callLevel` instead of `"m-1"`.

- [ ] **Step 2: Run the tests to verify they fail**

Run: `dotnet test tests/StreamBus.UnitTests/StreamBus.UnitTests.csproj --filter "FullyQualifiedName~WireEncodingTests|FullyQualifiedName~HeaderCarrierTests"`
Expected: FAIL to compile — `SentTime`, `ContentType`, `StreamWire`, and `IStreamSerializer.ContentType` do not exist.

- [ ] **Step 3: Implement `StreamMessageProperties` and `StreamWire`**

`ValidateHeaders(null)` returns quietly; otherwise it runs `ValidateValue` on every pair and `StreamHeaderKeys.EnsureNotReserved(key)` on every key.

`ApplyTo` performs the following, in this order, and leaves `message.Properties` null when it writes no AMQP property:

1. Lazily create `RabbitMQ.Stream.Client.AMQP.Properties` only when at least one of `MessageId`, `CorrelationId`, `SentTime`, `ContentType` is non-null.
2. `MessageId` / `CorrelationId` → `value.ToString("D")`; `CreationTime` → `StreamWire.NormalizeSentTime(value)`; `ContentType` → the string as-is.
3. Create or reuse `ApplicationProperties`; write every header pair.
4. When `routingKey` is non-null, write `applicationProperties[StreamHeaderKeys.RoutingKey] = routingKey` **after** the user headers so it wins.
5. Assign `message.ApplicationProperties` whenever the map was created.

`StreamWire.ReadGuid` accepts `string` parsed with `Guid.TryParse`, or `byte[]{ Length: 16 }` converted from AMQP network order:

```csharp
static Guid FromAmqpUuid(byte[] b) => new(
    (b[0] << 24) | (b[1] << 16) | (b[2] << 8) | b[3],
    (short)((b[4] << 8) | b[5]),
    (short)((b[6] << 8) | b[7]),
    b[8], b[9], b[10], b[11], b[12], b[13], b[14], b[15]);
```

Everything else returns null. `ReadSentTime` returns null for a null `Properties` or `default(CreationTime)`, otherwise `Utc` kind (convert `Local` through `ToUniversalTime`, treat `Unspecified` as UTC). `NormalizeSentTime` returns `DateTimeOffset.FromUnixTimeMilliseconds(new DateTimeOffset(value.ToUniversalTime()).ToUnixTimeMilliseconds()).UtcDateTime`.

- [ ] **Step 4: Wire the producer and publish entry points**

In `ProducerHandles.cs`, every `StreamMessageProperties.ApplyTo(message, options.Properties)` becomes `ApplyTo(message, options.Properties, options.RoutingKey)` (three call sites: `StreamProducerHandle.SendAsync`, `StreamProducerHandle.PublishReplayAsync`, `SuperStreamProducerHandle.SendMessageAsync`).

In `Bus.cs`:

- `PublishThroughPipelineAsync` and `PublishRawAsync` both start with `StreamMessageProperties.ValidateHeaders(effective.Properties?.Headers)` (raw: `options?.Properties?.Headers`).
- `SendTypedAsync` replaces the outgoing properties with `(effective.Properties ?? new StreamMessageProperties()) with { ContentType = _serializer.ContentType }` before calling `_producers.PublishAsync`.

In `Serialization.cs`, add `string ContentType { get; }` to `IStreamSerializer` and `public string ContentType => "application/json";` to `SystemTextJsonStreamSerializer`.

- [ ] **Step 5: Run the tests to verify they pass**

Run: `dotnet test tests/StreamBus.UnitTests/StreamBus.UnitTests.csproj`
Expected: PASS, with only the pre-existing failures (none expected; every reference has been migrated in this task).

- [ ] **Step 6: Build the solution**

Run: `dotnet build StreamBus.slnx`
Expected: 0 warnings.

- [ ] **Step 7: Commit**

```bash
git add src/StreamBus/MessageProperties.cs src/StreamBus/StreamWire.cs src/StreamBus/Serialization.cs src/StreamBus/ProducerHandles.cs src/StreamBus/Bus.cs tests/StreamBus.UnitTests/WireEncodingTests.cs tests/StreamBus.UnitTests/HeaderCarrierTests.cs tests/StreamBus.UnitTests/PublishPipelineTests.cs
git commit -m "feat: encode message ids, sent time, content type, and routing key on the wire"
```

---

### Task 3: Consume context and delivery path

**Files:**
- Modify: `src/StreamBus/ConsumeContext.cs` (rewrite)
- Modify: `src/StreamBus/Consumer.cs` (interface only)
- Modify: `src/StreamBus/ConsumePipeline.cs` (rewrite the public filter surface and the pipeline)
- Modify: `src/StreamBus/FilterAdapters.cs:59-77`
- Modify: `src/StreamBus/StreamPartitionConsumer.cs:503-653, 659-745, 809-842, 933-971`
- Modify: `src/StreamBus/Hosting.cs:268-319, 729`
- Modify: `src/StreamBus/Configuration/RetryConfiguration.cs:57-67`
- Modify: `src/StreamBus/ErrorEnvelope.cs`
- Modify: `src/StreamBus/ErrorPublisher.cs:20-54`
- Modify: `src/StreamBus/ProducerHandles.cs:18-35, 209-226, 272-295`
- Modify: `src/StreamBus/ProducerRegistry.cs:66-90`
- Modify: `src/StreamBus/ConsumerRegistry.cs:597`
- Tests: `ActivityTests.cs`, `PipelineSemanticsTests.cs`, `PipelineMetricsTests.cs`, `TypedConsumeFilterTests.cs`, `TypedFilterContractTests.cs`, `TypedFilterRegistrationTests.cs`, `HostingTests.cs`, `TypedConsumerTests.cs`, `RetryConfigurationTests.cs`, `ErrorEnvelopeTests.cs`, `ErrorHandoffTests.cs`, `ErrorQueueTests.cs`, `ErrorQueueTransportTests.cs`, `AmqpPublishChannelTests.cs`, `ReplayAndEffectivelyOnceReattachTests.cs`, `PublishTests.cs` (stub signature), `ConsumerIntegrationTests.cs`, `ConsumePipelineIntegrationTests.cs`, `TypedFilterIntegrationTests.cs`, `RetryIntegrationTests.cs`, `EffectivelyOnceIntegrationTests.cs`, `TypedConsumerIntegrationTests.cs`, `TypedStartupIntegrationTests.cs`.

This task is one compile unit: the public rename breaks every reference until Steps 1-4 land, so there is no red/green step inside it. The behavioral proofs are the migrated assertions plus the two new tests added in Step 5.

**Interfaces:**
- Consumes: `StreamHeaders` / `StreamSendHeaders` (Task 1), `StreamWire` and new `StreamMessageProperties` (Task 2).- Produces:
  - `public class ConsumeContext` with `required string Stream`, `required int Partition`, `required ulong Offset`, `required string? RoutingValue`, `required ReadOnlyMemory<byte> Body`, `Guid? MessageId`, `Guid? CorrelationId`, `DateTime? SentTime`, `required IStreamHeaders Headers`, `required IStreamSendHeaders SendHeaders`, `required IServiceProvider Services`, `required CancellationToken CancellationToken`, plus `T? GetPayload<T>()`, `bool TryGetPayload<T>(out T? payload)`, `void SetPayload<T>(T)`, and `internal void ResetPayloads()`.
  - `public sealed class ConsumeContext<T> : ConsumeContext { public required T Message { get; init; } }`
  - `public interface IConsumer<T> { Task Consume(ConsumeContext<T> context); }`
  - `public interface IStreamConsumeFilter { Task OnConsume(ConsumeContext context, ConsumeDelegate next); }`
  - `public interface IStreamConsumeFilter<T> { Task OnConsume(ConsumeContext<T> context, ConsumeDelegate next); }`
  - `internal static class TypedConsumeContext { public static ConsumeContext Create(Type messageType, ConsumeContext delivery, object message); }`
  - `internal sealed class ConsumePipeline { ConsumePipeline(IReadOnlyList<IStreamConsumeFilter> filters, bool requiresContinuation = false); int Count { get; } Task RunAsync(ConsumeContext context, ConsumeDelegate terminal); }`
  - `ExhaustedContext` gains `Guid? MessageId`, `Guid? CorrelationId`, `DateTime? SentTime`, `required IStreamHeaders Headers`; `Metadata` is deleted.
  - `ErrorEnvelope` gains `Guid? MessageId/CorrelationId` (still `required`), `DateTime? SentTime` (optional), and renames `OutputHeaders` to `SendHeaders`.
  - `IStreamPlainProducerHandle.SendEnvelopeAsync(ReadOnlyMemory<byte> body, StreamMessageProperties properties, CancellationToken ct)`; `StreamProducerRegistry.PublishEnvelopeAsync(string stream, ReadOnlyMemory<byte> body, StreamMessageProperties properties, TimeSpan confirmTimeout, CancellationToken ct = default)`.

- [ ] **Step 1: Rewrite the consume context types**

`ConsumeContext.cs` contains both classes; delete `MessageMetadata` and its `System.Text`/`System.Text.Json` usings. `Consumer.cs` keeps only `IConsumer<T>` with the new single-parameter signature.

The payload bag is `private readonly Dictionary<Type, object?> _payloads = new();` with `GetPayload<T>()` returning the value when it is a `T`, `TryGetPayload<T>(out T? payload)` returning `false` when unset or of another type, `SetPayload<T>(T)` storing by `typeof(T)`, and `ResetPayloads()` clearing.

- [ ] **Step 2: Rewrite the consume filter surface and pipeline**

In `ConsumePipeline.cs` delete `StreamConsumeFilterContext`, `StreamConsumeFilterContext<T>`, `ContextFactories`, `CreateContext`, `CreateContextFactory`, `CreateTypedContext`, `_endpointMessageType`, and `RequiresTypedMessage`. `RunAsync` and `InvokeAsync` drop the `CancellationToken` parameter. Add `TypedConsumeContext` (reflection factory keyed by message type, `CreateDelegate(typeof(Func<ConsumeContext, object, ConsumeContext>))`, generic `CreateTyped<T>` returning `new ConsumeContext<T> { ...copied delivery members..., Message = (T)message }`).

`ConsumerRegistry.BuildPipeline` keeps `messageType` for typed-filter selection but its `new ConsumePipeline(...)` call becomes `new ConsumePipeline(filters, requiresContinuation: effectivelyOnce)`. In `FilterAdapters.cs`, `TypedConsumeFilterAdapter<T>.OnConsume` casts `context as ConsumeContext<T>` and throws `StreamBusConfigurationException` when the cast fails.

- [ ] **Step 3: Rebuild the delivery path**

In `StreamPartitionConsumer`:

- `BuildContext(ulong offset, Message message, CancellationToken ct, IServiceProvider services, StreamSendHeaders sendHeaders)` builds one `ConsumeContext`: `Headers = new StreamHeaders(appPropertiesCopy, _serializer)`, `SendHeaders = sendHeaders`, `Services = services`, `MessageId = StreamWire.ReadGuid(message.Properties?.MessageId)`, `CorrelationId = StreamWire.ReadGuid(message.Properties?.CorrelationId)`, `SentTime = StreamWire.ReadSentTime(message.Properties)`, `RoutingValue` from `appProperties[StreamHeaderKeys.RoutingKey]?.ToString()`. Delete `MapMetadata`.
- Dispatch creates the scope when `_pipeline.Count > 0 || _options.MessageType is not null`, creates `var sendHeaders = new StreamSendHeaders(_serializer)` **before** `BuildContext` (so the context can carry it), and passes it in.
- At the start of every attempt (normal and effectively-once): `sendHeaders.Reset(); consumeContext.ResetPayloads();`.
- `InvokeHandlerPipelineAsync(ConsumeContext consumeContext)`: when `_options.MessageType` is non-null, deserialize with `TypedMessageDeserializer`, then `consumeContext = TypedConsumeContext.Create(messageType, consumeContext, message)`; if `_pipeline.Count == 0` call `_handler(consumeContext)`; else `_pipeline.RunAsync(consumeContext, () => _handler(consumeContext))`.
- `DispatchEffectivelyOnceAsync` takes the `StreamSendHeaders` explicitly (`DispatchEffectivelyOnceAsync(offset, consumeContext, sendHeaders, attach, activity)`), uses it for filter output headers, and snapshots it for the result publish.
- `ExhaustAsync` passes `consumeContext.MessageId`, `consumeContext.CorrelationId`, `consumeContext.SentTime`, and an `ExhaustedContext` with `Headers = consumeContext.Headers`.
- Envelopes set `MessageId`/`CorrelationId`/`SentTime` from the context and `SendHeaders = outputHeaders`.

- [ ] **Step 4: Rewire the typed consumer and the envelope path**

`Hosting.cs`: `TypedConsumerHandler.Create(Type messageType, Type consumerType)`; `CreateTyped<T, TConsumer>(IServiceScopeFactory scopes)` becomes `CreateTyped<T, TConsumer>()`; the body requires `context is ConsumeContext<T> typed` (otherwise `StreamBusConfigurationException` naming `typeof(T)`), resolves `TConsumer` from `context.Services`, and calls `consumer.Consume(typed)`. Delete the owned-scope logic. The call site becomes `TypedConsumerHandler.Create(reg.MessageType, reg.ConsumerType)` — both the `_scopes` and `serializer` arguments go away.

`ErrorEnvelope.cs`: parse ids with `Guid.TryParse`, parse `sent-time` with `DateTime.TryParse(..., DateTimeStyles.RoundtripKind)` then `SpecifyKind(Utc)`, write them back as `ToString("D")` / `"o"`. Rename `OutputHeaders` to `SendHeaders` and update `ErrorPublisher`.

`ProducerHandles.cs` / `ProducerRegistry.cs`: envelope publishing now takes a `StreamMessageProperties`. `ErrorPublisher.PublishHandoffAsync` builds:

```csharp
new StreamMessageProperties
{
    MessageId = envelope.MessageId,
    CorrelationId = envelope.CorrelationId,
    SentTime = envelope.SentTime,
    Headers = envelope.ToApplicationProperties(),
}
```

`SendEnvelopeAsync` bodies become `new Message(body.ToArray())`, `StreamMessageProperties.ApplyTo(message, properties)`, then send.

`ConsumerRegistry.cs:597`: `ctx.Metadata.ApplicationProperties` → `ctx.Headers.All`.

- [ ] **Step 5: Migrate the tests**

Apply this mapping to every listed test file, keeping existing assertions otherwise intact:

| Before | After |
|---|---|
| `OnConsume(StreamConsumeFilterContext ctx, ConsumeDelegate next, CancellationToken ct)` | `OnConsume(ConsumeContext ctx, ConsumeDelegate next)` |
| `OnConsume(StreamConsumeFilterContext<T> ctx, ...)` | `OnConsume(ConsumeContext<T> ctx, ...)` |
| `context.Raw.Metadata.GetString(k)` | `context.Headers.GetString(k)` |
| `context.Raw.Metadata.ApplicationProperties` | `context.Headers.All` |
| `context.Raw.Stream` / `.Offset` / `.Body` | `context.Stream` / `.Offset` / `.Body` |
| `context.OutputHeaders` | `context.SendHeaders` |
| `context.Metadata.GetX(...)` (typed consumer) | `context.Headers.GetX(...)` |
| `new MessageMetadata { ApplicationProperties = ... }` | `new StreamHeaders(..., SystemTextJsonStreamSerializer.Default)` |
| `new HeaderWriter()` | `new StreamSendHeaders(null, SystemTextJsonStreamSerializer.Default)` |
| `new StreamConsumeFilterContext { Raw = ..., OutputHeaders = ... }` | `new ConsumeContext { ..., SendHeaders = ... }` |
| `new StreamConsumeFilterContext<T> { Raw = ..., Message = ... }` | `new ConsumeContext<T> { ..., Message = ... }` |
| `Metadata = consumeContext.Metadata` | `Headers = consumeContext.Headers`, plus `MessageId`/`CorrelationId`/`SentTime` |
| `consumer.Consume(context, ct)` | `consumer.Consume(context)` |
| `envelope.OutputHeaders` | `envelope.SendHeaders` |
| envelope `MessageId = "m-1"` | `Guid.Parse("...")` |
| `context.Metadata.MessageId == "m-props"` (integration) | `context.MessageId == Guid.Parse("...")` and publish with the same `Guid`; also assert `context.CorrelationId` and `context.SentTime` equal the supplied values, and that a properties-only raw publish with no supplied `SentTime` observes `context.SentTime == null` |

`PipelineMetricsTests` and `TypedFilterContractTests` construct contexts directly; build `ConsumeContext`/`ConsumeContext<T>` with `Headers = new StreamHeaders(new Dictionary<string, object?>(), ...)`, `SendHeaders = new StreamSendHeaders(null, ...)`, and `Services`. In `TypedFilterContractTests`, the consume helpers take no `MessageType`, `context.OutputHeaders` reads become `context.SendHeaders`, and `Typed_contexts_share_headers_payloads_services_and_raw_context` is rewritten as `Typed_and_universal_consume_filters_share_one_instance`: pass the typed instance to a `ConsumeContext` parameter, assert `Assert.Same(typed, universal)`, `Assert.Same(typed.Headers, universal.Headers)`, `Assert.Same(typed.SendHeaders, universal.SendHeaders)`, `Assert.Same(typed.Services, universal.Services)`, `Assert.Same(typed.CancellationToken, universal.CancellationToken)`, and `Assert.Equal("payload", typed.GetPayload<string>())`. Delete the consume `MessageType` assertion. Leave the publish half of the file for Task 4.

`PublishTests.StubProducerHandle.SendEnvelopeAsync` and any other stub adapt to the new signature.

Two harness changes are needed because typed-context creation is now driven by `ConsumeOptions.MessageType` rather than by the presence of typed filters:

- `PipelineSemanticsTests.DispatchOptions` gains `Type? messageType = null` and sets `MessageType = messageType`. Every typed-endpoint call site passes it: `Per_message_scope_is_shared_by_the_filter_and_the_typed_consumer` becomes `DispatchOptions(endpointFilters: ..., typedEndpoint: true, messageType: typeof(TenantMessage))`.
- `TypedConsumerHandler.Create(typeof(TenantMessage), typeof(TenantProbeConsumer), harness.ScopeFactory, SystemTextJsonStreamSerializer.Default)` becomes `TypedConsumerHandler.Create(typeof(TenantMessage), typeof(TenantProbeConsumer))`.

Add these two tests to `PipelineSemanticsTests.cs`; they are the named proofs for the Review Focus cases that no existing test covers.

```csharp
[Fact]
public async Task Typed_message_that_fails_to_deserialize_is_retried_and_exhausted()
{
    await using var harness = await Harness.CreateAsync(configureServices: services =>
        services.AddTransient<DeserializationProbeConsumer>());
    var handler = TypedConsumerHandler.Create(
        typeof(TenantMessage), typeof(DeserializationProbeConsumer));
    var options = DispatchOptions(
        maxAttempts: 2,
        errorStream: "errors",
        messageType: typeof(TenantMessage));
    harness.Consumer = await harness.Registry.ConsumeAsync(
        typeof(DefaultBus), "orders", options, handler, TestContext.Current.CancellationToken);

    await harness.DeliverAsync(12, "this-is-not-json");

    var envelope = Assert.Single(harness.System.Envelopes);
    Assert.Equal("2", envelope.Properties["streambus.error.attempts"]?.ToString());
    Assert.Contains(
        nameof(StreamBusDeserializationException),
        envelope.Properties["streambus.error.exception-type"]?.ToString(),
        StringComparison.Ordinal);
    Assert.Equal(new ulong[] { 12 }, harness.Factory.Current.StoredOffsets);
}

[Fact]
public async Task Failed_attempt_headers_do_not_leak_into_the_next_attempt()
{
    await using var harness = await Harness.CreateAsync();
    var options = DispatchOptions(
        endpointFilters: new[] { ConsumeFilterRegistration.ForInstance(new SetOnceThenThrowFilter()) },
        maxAttempts: 2,
        errorStream: "errors");
    harness.Consumer = await harness.Registry.ConsumeAsync(
        typeof(DefaultBus), "orders", options, _ => Task.CompletedTask,
        TestContext.Current.CancellationToken);

    await harness.DeliverAsync(11, "hello");

    var envelope = Assert.Single(harness.System.Envelopes);
    Assert.Equal("2", envelope.Properties["streambus.error.attempts"]?.ToString());
    Assert.False(envelope.Properties.ContainsKey("tenant"));
}
```

with the two helpers:

```csharp
internal sealed class DeserializationProbeConsumer : IConsumer<TenantMessage>
{
    public Task Consume(ConsumeContext<TenantMessage> context) => Task.CompletedTask;
}

private sealed class SetOnceThenThrowFilter : IStreamConsumeFilter
{
    public int Calls { get; private set; }

    public Task OnConsume(ConsumeContext context, ConsumeDelegate next)
    {
        Calls++;
        if (Calls == 1)
            context.SendHeaders.Set("tenant", "first-attempt");
        throw new InvalidOperationException("boom");
    }
}
```

`SetOnceThenThrowFilter` proves the per-attempt reset: attempt 1 writes `tenant`, attempt 2 does not, and the envelope must carry only attempt 2's (empty) send headers.

- [ ] **Step 6: Build and run the unit suite**

Run: `dotnet build StreamBus.slnx` then `dotnet test tests/StreamBus.UnitTests/StreamBus.UnitTests.csproj`
Expected: 0 warnings; all unit tests pass.

- [ ] **Step 7: Run the brokered integration suite**

Run: `dotnet test tests/StreamBus.IntegrationTests/StreamBus.IntegrationTests.csproj`
Expected: PASS when a broker is available; the lane skips cleanly otherwise.

- [ ] **Step 8: Commit**

```bash
git add src tests
git commit -m "refactor!: replace consume contexts, metadata, and envelope ids with one flat Guid surface"
```

---

### Task 4: Publish context and publish path

**Files:**
- Modify: `src/StreamBus/PublishPipeline.cs` (rewrite)
- Modify: `src/StreamBus/Bus.cs` (ctor, `PublishThroughPipelineAsync`, `CreatePublishContext` deletion, `SendTypedAsync`, `PublishRawAsync`)
- Modify: `src/StreamBus/FilterAdapters.cs:38-57`
- Modify: `src/StreamBus/Registry.cs:191-206`
- Modify: `src/StreamBus/ResultProducerRegistry.cs:78-91`
- Tests: `PublishPipelineTests.cs`, `TypedFilterContractTests.cs`, `TypedFilterRegistrationTests.cs`, `PipelineMetricsTests.cs`, `ActivityTests.cs`, `PublishIntegrationTests.cs`, `PublishPipelineIntegrationTests.cs`, `TypedFilterIntegrationTests.cs`, `ResultPublisherTests.cs`, `EffectivelyOnceTests.cs`, `ReplayAndEffectivelyOnceReattachTests.cs`, `HostingTests.cs`.

This task is one compile unit for the same reason as Task 3; the red/green work is the new tests in Step 1, which are written before the types exist.

**Interfaces:**
- Consumes: `StreamSendHeaders` (Task 1), `StreamWire.NormalizeSentTime` (Task 2), `ConsumeContext` (Task 3).
- Produces:
  - `public interface IStreamPublishFilter { Task OnPublish(PublishContext context, PublishDelegate next); }`
  - `public interface IStreamPublishFilter<T> { Task OnPublish(PublishContext<T> context, PublishDelegate next); }`
  - `public delegate Task PublishDelegate();`
  - `public class PublishContext { required string Stream; required string? RoutingKey; required object Payload; required Guid MessageId; Guid? CorrelationId; required DateTime SentTime; required IStreamSendHeaders Headers; required IServiceProvider Services; required CancellationToken CancellationToken; }`
  - `public sealed class PublishContext<T> : PublishContext { public T Message => (T)Payload; }`
  - `internal sealed class PublishPipeline { PublishPipeline(IReadOnlyList<IStreamPublishFilter> filters); int Count { get; } Task RunAsync(PublishContext context, PublishDelegate terminal); }`

- [ ] **Step 1: Write the failing tests**

Add these probes next to the existing `SharedPublishContextProbe` in `PublishPipelineTests.cs`:

```csharp
public sealed class SentContextProbe
{
    public PublishContext? Context { get; private set; }
    public void Record(PublishContext context) => Context = context;
}

public sealed class SentProbeFilter(SentContextProbe probe) : IStreamPublishFilter
{
    public Task OnPublish(PublishContext context, PublishDelegate next)
    {
        probe.Record(context);
        return next();
    }
}
```

Then add three facts, using the file's existing `TestBus.StartAsync(cfg, ct, configureServices)` helper and its `CapturingSystem.Publishes` list:

```csharp
[Fact]
public async Task Publish_context_carries_generated_identity_and_sent_time()
{
    var probe = new SentContextProbe();
    await using var bus = await TestBus.StartAsync(cfg =>
    {
        cfg.Publish<PipelineOrder>("orders");
        cfg.UsePublishFilter<SentProbeFilter>();
    }, TestContext.Current.CancellationToken, s => s.AddSingleton(probe));

    await bus.Bus.PublishAsync(new PipelineOrder("p-1"), TestContext.Current.CancellationToken);

    var context = Assert.IsType<PublishContext<PipelineOrder>>(probe.Context);
    Assert.NotEqual(Guid.Empty, context.MessageId);
    Assert.Null(context.CorrelationId);
    Assert.Equal(DateTimeKind.Utc, context.SentTime.Kind);
    Assert.Equal(0, context.SentTime.Ticks % TimeSpan.TicksPerMillisecond);

    var sent = Assert.Single(bus.System.Publishes);
    Assert.Equal(context.MessageId, sent.Options.Properties!.MessageId);
    Assert.Equal(context.SentTime, sent.Options.Properties.SentTime);
    Assert.Equal("application/json", sent.Options.Properties.ContentType);
}

[Fact]
public async Task Supplied_message_id_and_sent_time_are_preserved()
{
    var probe = new SentContextProbe();
    await using var bus = await TestBus.StartAsync(cfg =>
    {
        cfg.Publish<PipelineOrder>("orders");
        cfg.UsePublishFilter<SentProbeFilter>();
    }, TestContext.Current.CancellationToken, s => s.AddSingleton(probe));

    var messageId = Guid.NewGuid();
    var correlationId = Guid.NewGuid();
    var sentTime = new DateTime(2026, 10, 31, 9, 12, 3, 284, DateTimeKind.Utc);
    await bus.Bus.PublishAsync(
        new PipelineOrder("p-1"),
        new PublishOptions
        {
            Properties = new StreamMessageProperties
            {
                MessageId = messageId,
                CorrelationId = correlationId,
                SentTime = sentTime,
            },
        },
        TestContext.Current.CancellationToken);

    var context = Assert.IsType<PublishContext<PipelineOrder>>(probe.Context);
    Assert.Equal(messageId, context.MessageId);
    Assert.Equal(correlationId, context.CorrelationId);
    Assert.Equal(sentTime, context.SentTime);

    var sent = Assert.Single(bus.System.Publishes);
    Assert.Equal(messageId, sent.Options.Properties!.MessageId);
    Assert.Equal(sentTime, sent.Options.Properties.SentTime);
}

[Fact]
public async Task Publish_context_rejects_reserved_headers_from_options()
{
    await using var bus = await TestBus.StartAsync(
        cfg => cfg.Publish<PipelineOrder>("orders"), TestContext.Current.CancellationToken);

    var options = new PublishOptions
    {
        Properties = new StreamMessageProperties
        {
            Headers = new Dictionary<string, object?> { ["streambus.message-id"] = "spoofed" },
        },
    };

    var ex = await Assert.ThrowsAsync<ArgumentException>(() =>
        bus.Bus.PublishAsync(new PipelineOrder("p-1"), options, TestContext.Current.CancellationToken));
    Assert.Contains("streambus.message-id", ex.Message, StringComparison.Ordinal);
    Assert.Empty(bus.System.Publishes);
}
```

Keep the existing `Universal_and_typed_publish_filters_share_one_context_instance` fact: it already asserts `Assert.Same(probe.UniversalContext, probe.TypedContext)`. Retarget `SharedPublishContextProbe` to `PublishContext?` / `PublishContext<PipelineOrder>?`, delete its `MessageType`/`RuntimeMessageType` writes, and delete the `Assert.Equal(typeof(PipelineOrder), probe.UniversalContext.MessageType)` assertion.

- [ ] **Step 2: Run the tests to verify they fail**

Run: `dotnet test tests/StreamBus.UnitTests/StreamBus.UnitTests.csproj --filter FullyQualifiedName~PublishPipelineTests`
Expected: FAIL to compile — `PublishContext` does not exist.

- [ ] **Step 3: Implement the publish context**

Rewrite `PublishPipeline.cs` with the types from the Interfaces block. Delete `StreamPublishFilterContext`, `StreamPublishFilterContext<T>`, `IStreamHeaderWriter`, and `HeaderWriter`. The pipeline's `Next` closure stays identical; `RunAsync` no longer takes a token and reads nothing from the filter besides the continuation.

- [ ] **Step 4: Rebuild the publish path**

In `Bus.cs` `PublishThroughPipelineAsync`:

1. `StreamMessageProperties.ValidateHeaders(effective.Properties?.Headers)`.
2. Seed identity: `var seed = effective.Properties ?? new StreamMessageProperties(); var messageId = seed.MessageId ?? Guid.NewGuid(); var sentTime = StreamWire.NormalizeSentTime(seed.SentTime ?? DateTime.UtcNow);` then `effective = effective with { Properties = seed with { MessageId = messageId, SentTime = sentTime } };`.
3. No-filter path: call `SendTypedAsync` with the seeded `effective`.
4. Filter path: `var headers = new StreamSendHeaders(effective.Properties?.Headers, _serializer);` build `PublishContext<T>` with `MessageId`, `CorrelationId = seed.CorrelationId`, `SentTime`, `Headers`, `Services = publishScope.ServiceProvider`, `CancellationToken = ct`, and run the pipeline with `() => SendTypedAsync(stream, message, effective with { Properties = headers.Materialize(effective.Properties) }, ct, activity)`.

Delete `CreatePublishContext`, the `_busView` field, and the `busView` constructor parameter from `StreamBusCore`/`StreamBus`/`StreamBus<TBus>`; update `Registry.cs` to stop building and passing `busView`. Keep `StreamBusRegistrationView` itself — it is still used by the configuration store.

Scope note: `PublishRawAsync` does not run publish filters today (it calls `_producers.PublishAsync` directly) and this plan keeps that behaviour. The design's "`PublishContext` … raw `PublishRawAsync`" row means raw publishes get the same reserved-key validation and wire encoding, not a filter pipeline; do not invent a `PublishContext` for raw byte publishes.

- [ ] **Step 5: Give effectively-once results identity**

In `ResultProducerRegistry.ResultOptions`, always build properties:

```csharp
new PublishOptions
{
    IsSuperStream = targetSuper,
    RoutingKey = targetSuper ? routingKey : null,
    Properties = new StreamMessageProperties
    {
        MessageId = Guid.NewGuid(),
        SentTime = StreamWire.NormalizeSentTime(DateTime.UtcNow),
        Headers = outputHeaders ?? new Dictionary<string, object?>(),
    },
}
```

`outputHeaders` no longer changes whether properties are present.

- [ ] **Step 6: Migrate the tests**

Apply the publish mapping:

| Before | After |
|---|---|
| `OnPublish(StreamPublishFilterContext ctx, PublishDelegate next, CancellationToken ct)` | `OnPublish(PublishContext ctx, PublishDelegate next)` |
| `OnPublish(StreamPublishFilterContext<T> ctx, ...)` | `OnPublish(PublishContext<T> ctx, ...)` |
| `context.MessageType` / `context.RuntimeMessageType` | deleted; delete the assertions |
| `context.Bus` | deleted; the test holds its own view |
| `new StreamPublishFilterContext { Bus = ..., Payload = ... }` | `new PublishContext { ..., MessageId = Guid.NewGuid(), SentTime = ... }` |
| `new StreamPublishFilterContext<T> { ... }` | `new PublishContext<T> { ... }` |
| `new HeaderWriter()` in publish tests | `new StreamSendHeaders(null, SystemTextJsonStreamSerializer.Default)` |

Delete `PipelineMetricsTests`' `StreamBusRegistrationView` construction and the `Bus` assertion. In `TypedFilterContractTests`, delete the `RuntimeMessageType` assertion and the `MessageType` assertions. In `PublishPipelineTests`, `PublishContextProbe.Record` takes `PublishContext` and drops its `MessageType`/`RuntimeMessageType` properties, `SharedPublishContextProbe` takes the new types, and `Header_writer_merges_call_level_properties_and_call_level_wins` uses `StreamSendHeaders`.

- [ ] **Step 7: Build and run the unit suite**

Run: `dotnet build StreamBus.slnx` then `dotnet test tests/StreamBus.UnitTests/StreamBus.UnitTests.csproj`
Expected: 0 warnings; all unit tests pass.

- [ ] **Step 8: Run the brokered integration suite**

Run: `dotnet test tests/StreamBus.IntegrationTests/StreamBus.IntegrationTests.csproj`
Expected: PASS when a broker is available.

- [ ] **Step 9: Commit**

```bash
git add src tests
git commit -m "refactor!: replace publish filter contexts with one flat PublishContext and generate identity"
```

---

### Task 5: Super-stream routing end to end

**Files:**
- Create: `tests/StreamBus.IntegrationTests/SuperStreamRoutingIntegrationTests.cs`
- Modify (only if the assertion belongs there): `tests/StreamBus.IntegrationTests/ConsumerIntegrationTests.cs`

**Interfaces:**
- Consumes: `StreamMessageProperties.ApplyTo(..., routingKey)` (Task 2), `ConsumeContext.RoutingValue` / `Headers` (Task 3), `PublishSuperAsync` (unchanged).
- Produces: no new public API. The guarantee is that a super-stream publish writes `streambus.routing-key` and every partition consumer observes it.

- [ ] **Step 1: Write the failing test**

Create `SuperStreamRoutingIntegrationTests` in the style of `ConsumerIntegrationTests` (same fixtures: `ConnectAdminAsync`, `BusInstance.StartAsync`, `WaitForAsync`):

```csharp
[Fact]
public async Task Super_publish_exposes_its_routing_key_on_every_delivery()
{
    var super = Unique("routing-super");
    var admin = await ConnectAdminAsync();
    await using var _ = admin;
    await admin.CreateSuperStream(new PartitionsSuperStreamSpec(super, 3));
    _superStreams.Add(super);

    await using var publisher = await BusInstance.StartAsync("routing-pub");
    foreach (var key in new[] { "key-a", "key-b", "key-a" })
        await publisher.Bus.PublishSuperAsync(super, new RoutingEvent(key), key, TestContext.Current.CancellationToken);

    var seen = new List<ConsumeContext>();
    await using var consumer = await BusInstance.StartAsync("routing-con");
    await using var subscription = await consumer.Bus.ConsumeAsync(
        super,
        new ConsumeOptions { IsSuperStream = true, Reference = "routing-grp", Offset = ConsumeOffset.AlwaysFirst },
        ctx => { lock (seen) seen.Add(ctx); return Task.CompletedTask; },
        TestContext.Current.CancellationToken);

    await WaitForAsync(() => Count(seen) == 3, TimeSpan.FromSeconds(30), "routed deliveries");
    Assert.All(seen, ctx => Assert.False(string.IsNullOrWhiteSpace(ctx.RoutingValue)));
    Assert.All(seen, ctx => Assert.Equal(ctx.RoutingValue, ctx.Headers.GetString("streambus.routing-key")));
    Assert.Contains(seen, ctx => ctx.RoutingValue == "key-a");
    Assert.All(seen, ctx => Assert.NotEqual(Guid.Empty, ctx.MessageId));
}
```

`Append` `RoutingEvent` as a `public sealed record RoutingEvent(string RoutingKey);` at file scope in the test file.

Add the effectively-once half, which proves `streambus.routing-key` is written for results too and that the default `TargetRouting` (`ctx => ctx.RoutingValue`) now works:

```csharp
[Fact]
public async Task Super_target_result_carries_the_source_routing_key()
{
    var source = Unique("routing-eo-src");
    var target = Unique("routing-eo-tgt");
    var admin = await ConnectAdminAsync();
    await using var _ = admin;
    await admin.CreateSuperStream(new PartitionsSuperStreamSpec(source, 2));
    _superStreams.Add(source);
    await admin.CreateSuperStream(new PartitionsSuperStreamSpec(target, 2));
    _superStreams.Add(target);

    await using var publisher = await BusInstance.StartAsync("routing-eo-pub");
    await publisher.Bus.PublishSuperAsync(source, new RoutingEvent("key-a"), "key-a",
        TestContext.Current.CancellationToken);

    await using var eo = await BusInstance.StartAsync("routing-eo");
    await using var step = await eo.Bus.ConsumeEffectivelyOnceAsync(
        source, target,
        new EffectivelyOnceOptions
        {
            SourceIsSuperStream = true,
            TargetIsSuperStream = true,
            Retry = new RetryPolicy { BaseDelay = TimeSpan.FromMilliseconds(10), MaxJitter = TimeSpan.Zero },
        },
        ctx => Task.FromResult(ctx.Body),
        TestContext.Current.CancellationToken);

    var seen = new List<ConsumeContext>();
    await using var reader = await BusInstance.StartAsync("routing-eo-read");
    await using var subscription = await reader.Bus.ConsumeAsync(
        target,
        new ConsumeOptions { IsSuperStream = true, Reference = "routing-eo-grp", Offset = ConsumeOffset.AlwaysFirst },
        ctx => { lock (seen) seen.Add(ctx); return Task.CompletedTask; },
        TestContext.Current.CancellationToken);

    await WaitForAsync(() => Count(seen) == 1, TimeSpan.FromSeconds(30), "routed result");
    var result = Assert.Single(seen);
    Assert.Equal("key-a", result.RoutingValue);
    Assert.Equal("key-a", result.Headers.GetString("streambus.routing-key"));
    Assert.NotEqual(Guid.Empty, result.MessageId);
}
```

- [ ] **Step 2: Run the tests to verify they fail (or are skipped without a broker)**

Run: `dotnet test tests/StreamBus.IntegrationTests/StreamBus.IntegrationTests.csproj --filter FullyQualifiedName~SuperStreamRoutingIntegrationTests`
Expected before the write side is complete: FAIL with `RoutingValue` null. With Task 2 applied both pass; if they already pass, confirm by reverting the `routingKey` argument in `SuperStreamProducerHandle.SendMessageAsync` locally, observing the failure, and restoring it.

- [ ] **Step 3: Confirm the write path**

Verify `SuperStreamProducerHandle.SendMessageAsync` calls `StreamMessageProperties.ApplyTo(message, options.Properties, options.RoutingKey)` before `_routing.Stamp(...)`, and that `PublishResultAsync` flows its `routingKey` into `PublishOptions.RoutingKey` (it does). No further production change is expected; this task exists to prove the half-finished path now works.

- [ ] **Step 4: Run the tests to verify they pass**

Run: `dotnet test tests/StreamBus.IntegrationTests/StreamBus.IntegrationTests.csproj --filter FullyQualifiedName~SuperStreamRoutingIntegrationTests`
Expected: both tests PASS with a broker.

- [ ] **Step 5: Commit**

```bash
git add tests/StreamBus.IntegrationTests/SuperStreamRoutingIntegrationTests.cs
git commit -m "test: prove super-stream routing keys reach consumers"
```

---

### Task 6: Docs, examples, changelog

**Files:**
- Modify: `docs/guide/consume.md:88-150`
- Modify: `docs/guide/publish.md:70-140`
- Modify: `docs/guide/errors.md:95-100`
- Modify: `docs/guide/observability.md:56-62`
- Modify: `docs/guide/multi-bus.md:55`
- Modify: `examples/StreamBus.Examples.Consumer/TenantFilter.cs`
- Modify: `examples/StreamBus.Examples.Consumer/OrderEventFilter.cs`
- Modify: `examples/StreamBus.Examples.Consumer/Program.cs:78-95`
- Modify: `examples/StreamBus.Examples.Producer/TenantFilter.cs`
- Modify: `examples/StreamBus.Examples.Producer/CorrelationIdFilter.cs`
- Modify: `examples/StreamBus.Examples.Producer/OrderPublishFilter.cs`
- Modify: `CHANGELOG.md`

**Interfaces:**
- Consumes: the public surface from Tasks 1-5.
- Produces: no code.

- [ ] **Step 1: Update the consume guide**

Replace the filter samples with `OnConsume(ConsumeContext context, ConsumeDelegate next)` and `context.Headers.GetString("x-tenant")`. Add a context member table:

| Member | Type | Meaning |
|---|---|---|
| `MessageId` | `Guid?` | AMQP `message-id`; null when absent or not a Guid |
| `CorrelationId` | `Guid?` | AMQP `correlation-id`; never invented |
| `SentTime` | `DateTime?` | AMQP `creation-time`, UTC, millisecond precision |
| `Headers` | `IStreamHeaders` | application properties as received, framework keys included |
| `SendHeaders` | `IStreamSendHeaders` | headers carried on the error envelope or effectively-once result |
| `Stream` / `Partition` / `Offset` / `RoutingValue` / `Body` / `Services` / `CancellationToken` | unchanged semantics |

State that `stream`, partition, offset, ids, sent time, services, and the token are fixed for the delivery, and that headers (and `SendHeaders`) are the only mutable surface. Replace the `context.OutputHeaders` sentence with `context.SendHeaders`. Delete the paragraph that says a typed context exposes `context.Raw` — `ConsumeContext<T>` is now the context, not a wrapper; the typed consumer and every filter share it. `multi-bus.md:55` changes `Consume(ConsumeContext<OrderEvent> ctx, CancellationToken ct)` to `Consume(ConsumeContext<OrderEvent> ctx)`.

- [ ] **Step 2: Update the publish guide**

Replace both filter samples with the two-parameter signature, delete the `context.MessageType` / `context.RuntimeMessageType` paragraph (replacing it with one sentence: "Filters attach by declared type; there is no runtime-type dispatch"), and document that `MessageId` and `SentTime` are generated when not supplied through `PublishOptions.Properties`, that `CorrelationId` is never generated, and that the four framework keys are rejected.

- [ ] **Step 3: Update the remaining guides**

`errors.md`: replace "metadata" in the `ExhaustedContext` description with "message id, correlation id, sent time, and headers". `observability.md`: replace the `MessageMetadata.CorrelationId` traceparent paragraph with "Consume spans carry an `ActivityLink` when the AMQP `correlation-id` holds a valid W3C `traceparent`; ids produced by StreamBus are Guids, so the link applies to foreign producers only."

- [ ] **Step 4: Update the examples**

All four filters and both `Program.cs` consumers adopt the new signatures. `TenantFilter` reads `context.Headers.GetString("x-tenant")`; `OrderEventFilter` uses `ConsumeContext<OrderEvent>` and `context.Stream` / `context.Offset`; consumers drop the `CancellationToken` parameter and use `context.CancellationToken` if needed. `CorrelationIdFilter` keeps writing `x-correlation-id` (it is a user header, not the AMQP property) and gains a comment saying the AMQP correlation id is `PublishOptions.Properties.CorrelationId`.

- [ ] **Step 5: Add the changelog entry**

Under a new `## 0.3.0` → `### Breaking` section, list: contexts replaced (`StreamConsumeFilterContext*` → `ConsumeContext*`, `StreamPublishFilterContext*` → `PublishContext*`), `Raw`/`Metadata`/`OutputHeaders`/`MessageType`/`RuntimeMessageType`/`Bus` removed, `MessageMetadata` removed, `IConsumer<T>` and filter signatures drop the token, `IStreamHeaderWriter` replaced by `IStreamSendHeaders`, ids are `Guid`/`Guid?`, `SentTime` added, content type and sent time written to AMQP properties, `ErrorEnvelope` ids are `Guid?`, super-stream routing keys are now written as `streambus.routing-key`. Under `### Added`, note the reserved-key guard and that `IStreamSerializer` now exposes `ContentType`.

- [ ] **Step 6: Verify the docs build and the examples compile**

Run: `dotnet build StreamBus.slnx`
Expected: 0 warnings (examples are part of the solution).

- [ ] **Step 7: Grep for stragglers**

Run: `Get-ChildItem -Recurse -Include *.cs,*.md | Where-Object { $_.FullName -notmatch '\\(obj|bin)\\' } | Select-String -Pattern 'StreamConsumeFilterContext|StreamPublishFilterContext|IStreamHeaderWriter|MessageMetadata|RuntimeMessageType|OutputHeaders|context\.Raw'`
Expected: matches only under `docs/superpowers/specs/2026-10-2*` and `docs/superpowers/plans/2026-10-*` (historical records). Any hit under `src`, `tests`, `examples`, `docs/guide`, or `CHANGELOG.md` is a miss.

- [ ] **Step 8: Commit**

```bash
git add docs examples CHANGELOG.md
git commit -m "docs: document the shared message contexts and migrate examples"
```

---

## Final Verification

- [ ] `dotnet build StreamBus.slnx` — 0 warnings.
- [ ] `dotnet test tests/StreamBus.UnitTests/StreamBus.UnitTests.csproj` — all pass.
- [ ] `dotnet test tests/StreamBus.IntegrationTests/StreamBus.IntegrationTests.csproj` — pass or skip without a broker.
- [ ] `git grep -n "StreamPublishFilterContext\|StreamConsumeFilterContext\|MessageMetadata\|IStreamHeaderWriter\|RuntimeMessageType"` returns only historical docs.
- [ ] Confirm a published `MessageId`/`CorrelationId`/`SentTime` arrives unchanged on the consumer, and a properties-only raw publish without `SentTime` yields `null`.
- [ ] Confirm the reserved-key guard rejects all four keys through both `PublishOptions.Properties.Headers` and `IStreamSendHeaders.Set`.
- [ ] Confirm `context.RoutingValue` is non-null for a super-stream delivery and equals `context.Headers.GetString("streambus.routing-key")`.
