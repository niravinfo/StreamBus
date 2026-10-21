// Sync rule: README snippets MUST be reproduced here verbatim. If this project
// stops compiling, the README drifted from the real API; fix the README and
// copy the corrected example back into this file.

using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using StreamBus;

var builder = Host.CreateApplicationBuilder(args);

builder.Services.AddStreamBus((_, cfg) =>
{
    cfg.Host("localhost", "/", h =>
    {
        h.Credentials("guest", "guest");
        h.StreamPort(5552);
        h.AmqpPort(5672);
        h.Heartbeat(TimeSpan.FromSeconds(60));
        h.MonitorInterval(TimeSpan.FromSeconds(5));
    });

    cfg.Publish<OrderPlaced>("orders");

    cfg.ReceiveEndpoint("orders", e =>
    {
        e.Consumer<OrderConsumer>("billing");
        e.UseMessageRetry(r => r.Immediate(3));
    });
});

builder.Services.AddStreamBusHosting(h => h.BlockStartupUntilReady(TimeSpan.FromSeconds(30)));

await builder.Build().RunAsync();

public sealed record OrderPlaced(string OrderId);

public sealed class OrderConsumer(ILogger<OrderConsumer> log) : IConsumer<OrderPlaced>
{
    public Task Consume(ConsumeContext<OrderPlaced> context, CancellationToken cancellationToken)
    {
        log.LogInformation("Order {OrderId} at offset {Offset}", context.Message.OrderId, context.Offset);
        return Task.CompletedTask;
    }
}
