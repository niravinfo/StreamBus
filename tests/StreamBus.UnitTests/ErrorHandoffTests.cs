namespace StreamBus.UnitTests;

public sealed class ErrorHandoffTests
{
    private static ErrorEnvelope Sample() => new()
    {
        ErrorStream = "e",
        SourceStream = "s",
        SourcePartition = 0,
        SourcePartitionStream = "s",
        SourceOffset = 7,
        Body = new byte[] { 1 },
        ExceptionType = "X",
        ExceptionMessage = "m",
        ExceptionStack = "st",
        Attempts = 3,
        FailedAt = DateTimeOffset.UtcNow,
        MessageId = null,
        CorrelationId = null,
        RoutingValue = null,
        ErrorRoutingValue = "s",
    };

    private sealed class NeverReadyConnector : IStreamSystemConnector
    {
        public Task<IStreamSystemHandle> ConnectAsync(StreamBusOptions options, CancellationToken ct)
            => new TaskCompletionSource<IStreamSystemHandle>().Task;
    }

    private static ErrorPublisher Publisher()
    {
        var options = new StreamBusOptions { Vhost = "/", UserName = "app", Password = "secret" };
        var supervisor = new StreamBusConnectionSupervisor(options, new NeverReadyConnector());
        return new ErrorPublisher(new StreamProducerRegistry(supervisor, "/"));
    }

    [Fact]
    public async Task Publish_failure_retries_then_commits_once_after_confirmation()
    {
        var publisher = Publisher();
        var publishes = 0;
        var commits = 0;
        var order = new List<string>();
        await publisher.HandOffAsync(Sample(),
            (_, _) =>
            {
                publishes++;
                order.Add($"publish#{publishes}");
                return publishes < 3
                    ? Task.FromException(new TimeoutException("blip"))
                    : Task.CompletedTask;
            },
            () => { commits++; order.Add("commit"); return Task.CompletedTask; },
            (_, _) => TimeSpan.Zero,
            TestContext.Current.CancellationToken);
        Assert.Equal(3, publishes);
        Assert.Equal(1, commits);
        Assert.Equal(new[] { "publish#1", "publish#2", "publish#3", "commit" }, order);
    }

    [Fact]
    public async Task Cancel_mid_handoff_commits_nothing()
    {
        var publisher = Publisher();
        using var cts = new CancellationTokenSource();
        cts.Cancel();
        var commits = 0;
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() =>
            publisher.HandOffAsync(Sample(),
                (_, _) => Task.FromException(new TimeoutException("blip")),
                () => { commits++; return Task.CompletedTask; },
                (_, _) => TimeSpan.FromMilliseconds(50),
                cts.Token));
        Assert.Equal(0, commits);
    }
}
