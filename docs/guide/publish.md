# Publish

Typed publish routes by message type. Register once, publish without
stream names or manual serialization.

## Plain streams

```csharp
services.AddStreamBus((context, cfg) =>
{
    cfg.Host("localhost", "/", h => h.Credentials("app", "s3cret"));
    cfg.Publish<OrderEvent>("orders");
});

await bus.PublishAsync(new OrderEvent("o-1", 1), ct);
```

The stream and the bus serializer come from the registration. Registering
one message type on two streams of one bus fails fast instead of picking
a random destination.

## Super streams

```csharp
cfg.PublishSuper<OrderEvent>("orders-super");

await bus.PublishSuperAsync(new OrderEvent("o-1", 1), "o-1", ct);
```

The routing key is a required `PublishSuperAsync` parameter — forgetting
it is a compile error, not a runtime surprise. Registration kind must
agree with the declaration: `Publish` for a super stream (or `PublishSuper`
for a plain one) fails at startup naming the type and the stream, as does
a `PublishSuper` registration with no super-stream declaration anywhere.
Calling the wrong method for a registration (`PublishAsync` on a
`PublishSuper` registration and vice versa) throws
`StreamBusConfigurationException` telling you which method to use.

Consumers never route — an endpoint on a super stream reads all
partitions. Declare the partitions on the endpoint and auto-declare works:

```csharp
cfg.ReceiveEndpoint("orders-super", e =>
{
    e.Consumer<SuperOrderConsumer>("super-processors");
    e.SetSuperStream(partitions: 3);
});
```

## Overrides and escape hatches

```csharp
await bus.PublishAsync("orders-manual", new OrderEvent("o-1", 1), ct);
await bus.PublishSuperAsync("manual-super", new OrderEvent("o-1", 1), "o-1", ct);
await bus.PublishRawAsync("orders", rawBytes, new PublishOptions(), ct);
```

Explicit-stream targets keep the method-implied kind (plain vs super). An
undeclared target surfaces `StreamBusStreamNotFoundException` — the call
site never auto-creates. `PublishRawAsync` sends pre-serialized bytes.

## Serializer

```csharp
cfg.Serializer(json => json.PropertyNamingPolicy = JsonNamingPolicy.CamelCase);
```
