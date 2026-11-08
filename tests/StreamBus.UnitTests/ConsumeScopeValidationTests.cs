using Microsoft.Extensions.DependencyInjection;

namespace StreamBus.UnitTests;

// Consume filters require a service scope to run in (the publish path
// already rejects filters without an IServiceScopeFactory, Bus.cs). A consumer
// with filters and no scope factory previously handed every filter a provider
// that silently resolved every service to null — so a missing DI setup looked
// like missing services instead of a configuration error. Endpoints without
// filters and without a message type keep the empty provider: nothing on
// those paths legitimately resolves services.
public sealed class ConsumeScopeValidationTests
{
    private const string Stream = "orders";
    private const int Partition = 0;

    private sealed class ScriptedSystem : NeverReadyConsumerSystem;

    private sealed class NeverReadyConnector : IStreamSystemConnector
    {
        public Task<IStreamSystemHandle> ConnectAsync(StreamBusOptions options, CancellationToken ct)
            => new TaskCompletionSource<IStreamSystemHandle>().Task;
    }

    private sealed class NoopFilter : IStreamConsumeFilter
    {
        public Task OnConsume(ConsumeContext context, ConsumeNext next) => next();
    }

    private static StreamPartitionConsumer CreateConsumer(
        IServiceScopeFactory? scopes,
        ConsumePipeline? pipeline = null)
    {
        var system = new ScriptedSystem();
        var supervisor = new StreamBusConnectionSupervisor(
            new StreamBusOptions { Vhost = "/", UserName = "app", Password = "secret" },
            new NeverReadyConnector());
        var producers = new StreamProducerRegistry(supervisor, "/");
        var consumer = new StreamPartitionConsumer(
            system, Stream, Stream, Partition, "grp", new ConsumeOptions
            {
                Reference = "grp",
                SingleActiveConsumer = false,
                Offset = ConsumeOffset.AlwaysFirst,
            },
            _ => Task.CompletedTask,
            new MonotonicOffsetGuard(),
            new PartitionState(),
            StreamBusMetrics.Shared,
            "/", "bus", new ErrorPublisher(producers),
            logger: null,
            scopeFactory: scopes,
            pipeline: pipeline);
        return consumer;
    }

    [Fact]
    public void Consume_filters_without_a_scope_factory_are_rejected_at_construction()
    {
        var pipeline = new ConsumePipeline(new IStreamConsumeFilter[] { new NoopFilter() });

        var ex = Record.Exception(() => CreateConsumer(scopes: null, pipeline: pipeline));

        var configuration = Assert.IsType<StreamBusConfigurationException>(ex);
        Assert.Contains("Consume filters are configured", configuration.Message);
    }

    [Fact]
    public async Task Consume_filters_with_a_scope_factory_are_allowed()
    {
        using var services = new ServiceCollection().BuildServiceProvider();
        var scopes = services.GetRequiredService<IServiceScopeFactory>();
        var pipeline = new ConsumePipeline(new IStreamConsumeFilter[] { new NoopFilter() });

        await using var consumer = CreateConsumer(scopes: scopes, pipeline: pipeline);
    }

    [Fact]
    public async Task Filterless_endpoint_without_a_scope_factory_is_allowed()
    {
        await using var consumer = CreateConsumer(scopes: null, pipeline: null);
    }
}
