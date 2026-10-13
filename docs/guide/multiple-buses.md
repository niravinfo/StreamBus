# Multiple Buses

> Run one or many buses (one per vhost, optionally several logical buses on one
> vhost) in a single Worker. Marker types, resolution, sharing rules, per-bus config.
> Source: `Hosting.cs:49-140`, `Bus.cs`, `Registry.cs`.

## 1. When you need more than one bus

- **One bus** (default): single vhost, e.g. `"/"`. Inject `IStreamBus`.
- **Many buses**: multiple vhosts (e.g. `"/orders"`, `"/billing"`), or logical
  isolation on one vhost (orders pipeline vs audit pipeline with separate
  consumer groups, health, and topology). Each bus gets a **marker type** and is
  injected as `IStreamBus<TMarker>`.

Marker types are empty classes you own:

```csharp
public sealed class OrdersBus;
public sealed class BillingBus;
```

## 2. Register + resolve

```csharp
services.AddStreamBus<OrdersBus>("/orders", o => o
    .Credentials("app", "s3cret")
    .Topology(t => t.DeclareStream("orders")));

services.AddStreamBus<BillingBus>("/billing", o => o
    .Credentials("app", "s3cret")
    .Topology(t => t.DeclareStream("invoices")));

// typed consumers bind to a bus explicitly:
services.AddStreamBusConsumer<OrdersBus, OrderEvent, OrderConsumer>("orders");
services.AddStreamBusConsumer<BillingBus, InvoiceEvent, InvoiceConsumer>("invoices");

// resolve anywhere:
public sealed class CheckoutWorker(IStreamBus<OrdersBus> orders, IStreamBus<BillingBus> billing)
    : BackgroundService
{
    protected override async Task ExecuteAsync(CancellationToken ct)
    {
        await orders.PublishAsync("orders", body, new PublishOptions(), ct);
        await billing.PublishAsync("invoices", body2, new PublishOptions(), ct);
    }
}
```

The non-generic overload registers `DefaultBus` + `IStreamBus`; each generic
overload registers its marker + `IStreamBus<TBus>` (`Hosting.cs:51-75`).
Default and markers coexist (`IStreamBus` and `IStreamBus<OrdersBus>` are
distinct singletons).

## 3. Rules (enforced, not advisory)

1. **Non-generic overload at most once** per `IServiceCollection`;
   **each marker at most once** (`StreamBusConfigStore.Add`, `Hosting.cs:19-29`).
   Duplicate → `StreamBusConfigurationException`.
2. **One marker = one vhost.** A marker cannot span vhosts (second registration
   with the same marker throws as duplicate).
3. **Same vhost must agree on connection identity**: user, password, host, port,
   pool settings. First registration wins; conflicts throw. Two markers on the
   same vhost with identical connection **share** one supervised connection,
   producer/consumer registries, and topology handle (`Registry.cs:58-83`) but
   keep distinct facades — and typed-consumer references differ per marker, so
   cursors stay isolated.
4. A typed consumer's marker **must have a bus**: registering
   `AddStreamBusConsumer<OrdersBus, ...>` without `AddStreamBus<OrdersBus>`
   throws at hosted-service start (`Hosting.cs:429-435`).

## 4. Per-bus configuration (`appsettings.json`)

```json
{
  "StreamBus": {
    "Orders":  { "Vhost": "/orders",  "UserName": "app", "Password": "s3cret", "Host": "rmq", "Port": 5552 },
    "Billing": { "Vhost": "/billing", "UserName": "app", "Password": "s3cret", "Host": "rmq", "Port": 5552 }
  }
}
```

```csharp
var ordersCfg = builder.Configuration.GetSection("StreamBus:Orders").Get<StreamBusSettings>()!;
var billingCfg = builder.Configuration.GetSection("StreamBus:Billing").Get<StreamBusSettings>()!;

builder.Services.AddStreamBus<OrdersBus>(ordersCfg.Vhost, o => o
    .Credentials(ordersCfg.UserName, ordersCfg.Password)
    .Endpoint(ordersCfg.Host, ordersCfg.Port)
    .Topology(t => t.DeclareStream("orders")));

builder.Services.AddStreamBus<BillingBus>(billingCfg.Vhost, o => o
    .Credentials(billingCfg.UserName, billingCfg.Password)
    .Endpoint(billingCfg.Host, billingCfg.Port)
    .Topology(t => t.DeclareStream("invoices")));
```

See [Configuration Reference](configuration-reference.md#7-recommended-appsettingsjson-bridge)
for the `StreamBusSettings` POCO.

## 5. Lifetimes + health

- `IStreamBus`, `IStreamBus<TBus>`, `IStreamBusRegistry`, connector, hosted
  service, health check: **singletons** (`TryAddSingleton`, `Hosting.cs:77-96`).
- `TConsumer` classes: **transient**, one scope per message.
- Health check (`"streambus"`) aggregates **all** buses:
  `Healthy` only if every bus is `Ready`; any `Fatal` → `Unhealthy`; otherwise
  `Degraded` (includes the zero-buses case). Data maps `{vhost|busType: State}`.
  `IStreamBusRegistry.DescribeStates()` gives the same per-bus view for dashboards.

## 6. Common mistakes

| Mistake | What happens |
|---|---|
| Two `AddStreamBus` (non-generic) calls | `StreamBusConfigurationException`: duplicate |
| Same marker, two vhosts | Duplicate-marker exception (a marker is one bus) |
| Same vhost, different passwords per marker | Conflicting-connection exception |
| Consumer marker without matching bus | `StartAsync` throws: no registration for marker |
| Expecting cross-bus transactions | None — each publish/consume is per-bus; coordinate idempotently in your code |
