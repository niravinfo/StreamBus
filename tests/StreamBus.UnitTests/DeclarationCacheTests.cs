namespace StreamBus.UnitTests;

public sealed class DeclarationCacheTests
{
    private sealed class NeverConnector : IStreamSystemConnector
    {
        public Task<IStreamSystemHandle> ConnectAsync(StreamBusOptions options, CancellationToken ct)
            => new TaskCompletionSource<IStreamSystemHandle>().Task;
    }

    [Fact]
    public async Task Declaration_lookup_is_cached_at_bus_creation()
    {
        var calls = 0;
        var registry = new StreamBusRegistry(
            vhost => new StreamBusOptions { Vhost = vhost, UserName = "app", Password = "secret" },
            options => new StreamBusConnectionSupervisor(options, new NeverConnector()),
            declarationResolver: _ =>
            {
                Interlocked.Increment(ref calls);
                return Array.Empty<DeclaredStream>();
            });

        var bus = registry.GetOrCreateBus("/");
        Assert.Equal(1, calls);

        for (var i = 0; i < 3; i++)
            await Assert.ThrowsAsync<StreamBusNotReadyException>(() =>
                bus.PublishAsync("orders", new object(), ct: TestContext.Current.CancellationToken));

        Assert.Equal(1, calls);
    }
}
