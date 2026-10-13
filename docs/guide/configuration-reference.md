# Configuration Reference

> Every StreamBus setting: what it does, its default, valid values, what breaks
> if you get it wrong, and how to feed it from `appsettings.json`.
> Source: `src/StreamBus/Options.cs`, `src/StreamBus/Hosting.cs:143-348`.

## 1. Mental model

```
appsettings.json / env vars
        │  (YOU bind: IConfiguration -> POCO -> builder lambda)
        ▼
services.AddStreamBus(vhost, o => o.Credentials(...).Endpoint(...).Connection(...)
                                     .Hosting(...).Topology(...).Serializer(...))
        │  builds StreamBusOptions (Options.cs:27-38)
        ▼
StreamBusConfigStore  +  StreamBusConsumerStore   (build-time, on IServiceCollection)
        │  (validated at Add* time + at hosted-service StartAsync)
        ▼
IStreamBusRegistry -> IStreamBus / IStreamBus<TBus>  +  StreamBusHostedService
```

Key facts:

- **Code-first, no `IConfiguration` overload.** The vhost is the first argument
  to `AddStreamBus`; everything else comes from the builder lambda. This is
  deliberate: message/consumer types are code, so their binding stays in code.
  Section 6 gives you the recommended `appsettings.json` ↔ builder bridge.
- `StreamBusOptions` (`Options.cs:27-38`) is an immutable `record`. The builder
  (`StreamBusOptionsBuilder`, `Hosting.cs:143-201`) starts from defaults and
  `Build()`s it. Invalid combinations throw `StreamBusConfigurationException`
  (never silently ignored).
- Registration rules are enforced in `StreamBusConfigStore.Add`
  (`Hosting.cs:19-29`): the **non-generic overload at most once** per service
  collection, **each marker `TBus` at most once**, and registrations sharing a
  **vhost must agree on connection identity** (user, password, host, port,
  connection pool) — first registration wins, conflicts throw.

## 2. Bus identity: vhost + credentials + endpoint

| Builder call | Backing option | Default | Required? | What it does |
|---|---|---|---|---|
| `new StreamBusOptionsBuilder(vhost)` / `AddStreamBus(vhost, ...)` | `Vhost` | — | **yes** (first arg) | RabbitMQ vhost, e.g. `"/"`. One supervised `StreamSystem` connection per distinct vhost (`Registry.cs:58-83`). Buses on the same vhost share it. |
| `.Credentials(user, password)` (`Hosting.cs:156`) | `UserName`, `Password` | `""` (must override) | **yes** | Broker login. Validated non-empty/non-whitespace; `guest` is **forbidden** (`CredentialValidation.cs:3-17`) because the broker rejects `guest` on non-loopback with `AuthenticationFailureLoopback`. Create a dedicated app user. |
| `.Endpoint(host, port)` (`Hosting.cs:157`) | `Host`, `Port` | `"127.0.0.1"`, `5552` | no | Stream endpoint (NOT AMQP 5672). Host may be IP (parsed to `IPEndPoint`) or DNS name (`Connector.cs:197-226`). Single locator endpoint; client follows broker metadata afterwards. |

Example:

```csharp
services.AddStreamBus("/", o => o
    .Credentials("app", "s3cret")
    .Endpoint("rabbitmq.internal", 5552));
```

## 3. Connection pooling

```csharp
o.Connection(c => c
    .ProducersPerConnection(3)
    .ConsumersPerConnection(5));
```

| Option (`StreamBusConnectionOptions`, `Options.cs:3-19`) | Default | Valid | Effect |
|---|---|---|---|
| `ProducersPerConnection` | `1` | `1..255` (`byte.MaxValue`, client pool limit) | Max producer handles multiplexed per connection. Raise when many concurrent publishers contend. Validation failure → config exception listing the bad field (`Validate()`). |
| `ConsumersPerConnection` | `1` | `1..255` | Same for consumer handles. Super-stream consumers fan out per partition behind this. |

Same-vhost registrations must specify **identical** pool settings (part of
connection identity, `Hosting.cs:31-37`).

## 4. Hosting: startup blocking

```csharp
o.Hosting(h => h.BlockStartupUntilReady(TimeSpan.FromSeconds(30)));
// or non-blocking (default): o.Hosting(h => { });  // BlockStartupUntilReady = false
```

| Option (`StreamBusHostOptions`, `Options.cs:21-25`) | Default | Meaning |
|---|---|---|
| `BlockStartupUntilReady` | `false` | `false`: `StartAsync` kicks off connect + declare + consumer-start in the background and returns; failures surface as `LogCritical` background logs (`Hosting.cs:464-494`). `true`: `StartAsync` awaits connection ready + all startup declarations + all typed-consumer attaches; on failure it throws (`StreamBusFatalException` / `StreamBusTimeoutException` / config exceptions via `ExceptionDispatchInfo`, `Hosting.cs:506-560`). |
| `StartupTimeout` (`TimeSpan?`) | `null` (= wait indefinitely when blocking) | Only meaningful with blocking. Non-positive → `StreamBusConfigurationException` at configure time (`Hosting.cs:218-219`). On expiry → `StreamBusTimeoutException`. |

Recommendation for Workers: **block with a timeout** (e.g. 30s) so a bad
password, missing vhost, or conflicting topology crashes startup instead of
running deaf.

## 5. Topology declarations (startup)

```csharp
o.Topology(t => t
    .DeclareStream("orders", knobs => knobs
        .MaxAge(TimeSpan.FromDays(7))
        .MaxLengthBytes(10UL * 1024 * 1024 * 1024)
        .MaxSegmentSizeBytes(500 * 1024 * 1024)
        .LeaderLocator(StreamLeaderLocator.LeastLeaders))
    .DeclareSuperStream("orders-super", partitions: 3));
```

Builder: `StreamTopologyDeclarationsBuilder` (`Hosting.cs:225-255`), knobs
`StreamDeclarationKnobsBuilder` (`Hosting.cs:257-280`).

| Declaration | Required | Optional knobs (all `null` = broker default, except locator) | Validation (`Topology.cs:75-104`) |
|---|---|---|---|
| `DeclareStream(name, knobs?)` → `StreamDeclaration` (`Topology.cs:10-17`) | `Name` non-empty | `MaxAge` (`TimeSpan?`, must be `> Zero` if set — typically retention), `MaxLengthBytes` (`ulong?`, `>= 1`), `MaxSegmentSizeBytes` (`int?`, `>= 1`), `LeaderLocator` (default `LeastLeaders`) | Throws `StreamBusConfigurationException` on empty name, non-positive age, zero lengths, undefined locator. |
| `DeclareSuperStream(name, partitions, knobs?)` → `SuperStreamDeclaration` (`Topology.cs:19-27`) | `Name` non-empty, `Partitions >= 1` | Same knobs | Plus `Partitions must be >= 1`. |

Semantics (see [Effectively-Once & Topology](effectively-once-topology.md)):

- Declarations run at startup with **infinite retry** (`DeclareUntilReadyAsync`,
  backoff 200ms→30s, `Hosting.cs:563-597`).
- They are **idempotent**: same spec twice = no-op (in-process fingerprint
  cache + tolerate already-exists cross-process, `TopologyRegistry.cs:36-38`).
- **Streams are immutable**: if the broker has a conflicting spec (plain vs
  super, partition count, or differing args on create race) you get
  `StreamConfigurationConflictException` — delete/recreate or align the
  declaration. The broker allows a plain stream and super stream to share a
  name; StreamBus treats that as a conflict (Ruling R6).
- Error streams for retry-exhausted messages are **ordinary streams** — declare
  them here too (with `MaxAge` retention!).

`StreamLeaderLocator` (`Topology.cs:3-8`): `LeastLeaders` (default),
`ClientLocal`, `Random` — mapped 1:1 to the client enum (`Topology.cs:66-72`).

## 6. Serializer

```csharp
o.Serializer(new SystemTextJsonStreamSerializer(new JsonSerializerOptions
{
    PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
}));
// shorthand:
o.Serializer(json => json.PropertyNamingPolicy = JsonNamingPolicy.CamelCase);
```

| Option | Default | Contract (`Serialization.cs:5-24`) |
|---|---|---|
| `Serializer` (`IStreamSerializer`) | `SystemTextJsonStreamSerializer.Default` | `byte[] Serialize<T>(T)`, `T Deserialize<T>(ReadOnlyMemory<byte>)`. Only built-in is STJ (`SerializeToUtf8Bytes` / `Deserialize(span)`; null payload → `JsonException`). |

The **typed consumer path deserializes with this serializer**
(`Hosting.cs:371-399`); a mismatch (or null JSON) throws
`StreamBusDeserializationException` naming stream/partition/offset/message-type.
Publishers must serialize with compatible options — share one
`JsonSerializerOptions` instance between your publisher code and `o.Serializer(...)`.

Custom serializer (e.g. protobuf): implement `IStreamSerializer` (both methods),
pass with `o.Serializer(custom)`. Keep it deterministic and side-effect free —
it runs per message on the dispatch path.

## 7. Recommended `appsettings.json` bridge

Copy-paste recipe. StreamBus stays code-first; this POCO covers everything
bindable, and consumer *bindings* (type ↔ stream) stay in code where the types live.

**`appsettings.json`:**

```json
{
  "StreamBus": {
    "Vhost": "/",
    "UserName": "app",
    "Password": "s3cret",
    "Host": "127.0.0.1",
    "Port": 5552,
    "Connection": {
      "ProducersPerConnection": 1,
      "ConsumersPerConnection": 1
    },
    "Hosting": {
      "BlockStartupUntilReady": true,
      "StartupTimeoutSeconds": 30
    },
    "Topology": {
      "Streams": [
        { "Name": "orders", "MaxAgeDays": 7 },
        { "Name": "orders-errors", "MaxAgeDays": 14 }
      ],
      "SuperStreams": [
        { "Name": "orders-super", "Partitions": 3 }
      ]
    }
  }
}
```

**Settings POCO (put in your app, not the library):**

```csharp
public sealed class StreamBusSettings
{
    public string Vhost { get; set; } = "/";
    public string UserName { get; set; } = "";
    public string Password { get; set; } = "";
    public string Host { get; set; } = "127.0.0.1";
    public int Port { get; set; } = 5552;
    public ConnectionSettings Connection { get; set; } = new();
    public HostingSettings Hosting { get; set; } = new();
    public TopologySettings Topology { get; set; } = new();

    public sealed class ConnectionSettings
    {
        public int ProducersPerConnection { get; set; } = 1;
        public int ConsumersPerConnection { get; set; } = 1;
    }
    public sealed class HostingSettings
    {
        public bool BlockStartupUntilReady { get; set; }
        public int? StartupTimeoutSeconds { get; set; }
    }
    public sealed class TopologySettings
    {
        public List<StreamSetting> Streams { get; set; } = [];
        public List<SuperStreamSetting> SuperStreams { get; set; } = [];
        public sealed class StreamSetting
        {
            public string Name { get; set; } = "";
            public int? MaxAgeDays { get; set; }
            public ulong? MaxLengthBytes { get; set; }
            public int? MaxSegmentSizeBytes { get; set; }
            public string? LeaderLocator { get; set; } // LeastLeaders|ClientLocal|Random
        }
        public sealed class SuperStreamSetting : StreamSetting
        {
            public int Partitions { get; set; } = 1;
        }
    }
}
```

**`Program.cs` wiring:**

```csharp
var sb = builder.Configuration.GetSection("StreamBus").Get<StreamBusSettings>()
    ?? throw new InvalidOperationException("Missing 'StreamBus' configuration section.");

builder.Services.AddStreamBus(sb.Vhost, o => o
    .Credentials(sb.UserName, sb.Password)
    .Endpoint(sb.Host, sb.Port)
    .Connection(c => c
        .ProducersPerConnection(sb.Connection.ProducersPerConnection)
        .ConsumersPerConnection(sb.Connection.ConsumersPerConnection))
    .Hosting(h =>
    {
        if (sb.Hosting.BlockStartupUntilReady)
            h.BlockStartupUntilReady(sb.Hosting.StartupTimeoutSeconds is { } s
                ? TimeSpan.FromSeconds(s)
                : null);
    })
    .Topology(t =>
    {
        foreach (var s in sb.Topology.Streams)
            t.DeclareStream(s.Name, k =>
            {
                if (s.MaxAgeDays is { } d) k.MaxAge(TimeSpan.FromDays(d));
                if (s.MaxLengthBytes is { } l) k.MaxLengthBytes(l);
                if (s.MaxSegmentSizeBytes is { } sg) k.MaxSegmentSizeBytes(sg);
                if (s.LeaderLocator is { } loc) k.LeaderLocator(Enum.Parse<StreamLeaderLocator>(loc));
            });
        foreach (var s in sb.Topology.SuperStreams)
            t.DeclareSuperStream(s.Name, s.Partitions, k =>
            {
                if (s.MaxAgeDays is { } d) k.MaxAge(TimeSpan.FromDays(d));
                // ... same knobs
            });
    })
    .Serializer(json => json.PropertyNamingPolicy = JsonNamingPolicy.CamelCase));
```

Secrets: prefer env vars / Key Vault references over plaintext passwords:

```bash
StreamBus__Password=s3cret dotnet run
# or User Secrets in development:
dotnet user-secrets set "StreamBus:Password" "s3cret"
```

Multi-bus: repeat the section per bus (`StreamBus:Orders`, `StreamBus:Billing`)
— see [Multiple Buses](multiple-buses.md).

## 8. Consumer options at a glance (typed registration)

`AddStreamBusConsumer<TMessage, TConsumer>(stream, configure)` builds
`ConsumeOptions` via `ConsumeOptionsBuilder` (`Hosting.cs:282-319`). Full table
in [Consuming](consuming.md); defaults:

```
SuperStream(false)  Reference(null → derived)  SingleActiveConsumer(true)
Ordering(Strict)    Offset(ResumeOrFirst)      OffsetStore(EveryMessages: 1, no interval)
Retry(Strict, unbounded)  ErrorStream(null)    OnRetryExhausted(null)
Readiness(FailFast)
```

## 9. Validation cheat-sheet (all throw `StreamBusConfigurationException`)

- Empty vhost/user/password, or user `guest`.
- Pool values outside `1..255`.
- Non-positive `StartupTimeout`; non-positive `MaxAge`; zero `MaxLengthBytes` /
  `MaxSegmentSizeBytes`; undefined `LeaderLocator`; super `Partitions < 1`.
- Duplicate default-bus registration; duplicate marker `TBus`; same-vhost
  connection mismatch; duplicate typed consumer (same marker+message+consumer+stream).
- Publish super stream without `RoutingKey`; `Timeout <= 0`.
- `Strict` retry combined with `MaxAttempts` or `ErrorStream`; `SkipAfterDeadline`
  without both; super-stream error target; retry knob out of range.
- Reference / producer name > 255 UTF-8 bytes.
- Effectively-once with non-`Strict`/bounded retry; result-target super without routing key.
