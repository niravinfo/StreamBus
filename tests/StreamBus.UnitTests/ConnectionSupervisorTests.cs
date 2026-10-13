namespace StreamBus.UnitTests;

using RabbitMQ.Stream.Client;

file sealed class StubHandle : IStreamSystemHandle
{
    public bool IsClosed => false;
    public ValueTask DisposeAsync() => ValueTask.CompletedTask;
    public Task<IStreamProducerHandle> CreatePlainProducerAsync(string stream, CancellationToken ct)
        => throw new NotImplementedException("ConnectionSupervisorTests never publishes.");
    public Task<IStreamProducerHandle> CreatePlainProducerWithReferenceAsync(string stream, string reference, CancellationToken ct)
        => throw new NotImplementedException("ConnectionSupervisorTests never publishes.");
    public Task<IStreamProducerHandle> CreateSuperStreamProducerAsync(string stream, CancellationToken ct)
        => throw new NotImplementedException("ConnectionSupervisorTests never publishes.");
    public Task<ulong?> TryQueryOffsetAsync(string reference, string stream, CancellationToken ct)
        => throw new NotImplementedException("ConnectionSupervisorTests never queries offsets.");
    public Task StoreSystemOffsetAsync(string reference, string stream, ulong offset, CancellationToken ct)
        => throw new NotImplementedException("ConnectionSupervisorTests never queries offsets.");
    public Task<bool> StreamExistsAsync(string stream, CancellationToken ct)
        => throw new NotImplementedException("ConnectionSupervisorTests never queries offsets.");
    public Task<bool> SuperStreamExistsAsync(string superStream, CancellationToken ct)
        => throw new NotImplementedException("ConnectionSupervisorTests never queries offsets.");
    public Task<string[]> QueryPartitionAsync(string superStream, CancellationToken ct)
        => throw new NotImplementedException("ConnectionSupervisorTests never queries offsets.");
    public Task<RawConsumer> CreateRawConsumerAsync(RawConsumerConfig config, CancellationToken ct)
        => throw new NotImplementedException("ConnectionSupervisorTests never consumes.");
}

file sealed class ScriptedConnector : IStreamSystemConnector
{
    private readonly Queue<Func<Task<IStreamSystemHandle>>> _steps = new();
    private readonly object _gate = new();
    public void FailWith(Exception ex)
    {
        lock (_gate) _steps.Enqueue(() => Task.FromException<IStreamSystemHandle>(ex));
    }
    public void Succeed()
    {
        lock (_gate) _steps.Enqueue(() => Task.FromResult<IStreamSystemHandle>(new StubHandle()));
    }
    public async Task<IStreamSystemHandle> ConnectAsync(StreamBusOptions options, CancellationToken ct)
    {
        while (true)
        {
            Func<Task<IStreamSystemHandle>>? step = null;
            lock (_gate) { if (_steps.Count > 0) step = _steps.Dequeue(); }
            if (step is not null) return await step().ConfigureAwait(false);
            await Task.Delay(50, ct).ConfigureAwait(false);
        }
    }
}

public sealed class ConnectionSupervisorTests
{
    private static StreamBusOptions Valid(string user = "app") => new()
    {
        Vhost = "/",
        UserName = user,
        Password = "secret",
    };

    [Fact]
    public void Guest_credentials_fail_fast_with_actionable_message()
    {
        var ex = Assert.Throws<StreamBusConfigurationException>(() =>
            new StreamBusConnectionSupervisor(Valid("guest"), new ScriptedConnector()));
        Assert.Contains("guest", ex.Message, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("loopback", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public async Task Transient_failures_retry_then_ready()
    {
        var connector = new ScriptedConnector();
        connector.FailWith(new System.IO.IOException("reset"));
        connector.FailWith(new TimeoutException("t"));
        connector.Succeed();
        await using var sup = new StreamBusConnectionSupervisor(Valid(), connector);
        var states = new List<BusConnectionState>();
        sup.StateChanged += states.Add;
        await sup.StartAsync(CancellationToken.None);
        var readiness = await sup.WaitUntilReadyAsync(ReadinessPolicy.Indefinitely, new CancellationTokenSource(TimeSpan.FromSeconds(10)).Token);
        Assert.Equal(BusReadiness.Ready, readiness);
        Assert.Contains(BusConnectionState.TransientFailure, states);
        Assert.Equal(BusConnectionState.Ready, sup.State);
    }

    [Fact]
    public async Task Fatal_error_becomes_fatal_without_retry()
    {
        var connector = new ScriptedConnector();
        connector.FailWith(new GenericProtocolException(ResponseCode.InternalError, "p"));
        await using var sup = new StreamBusConnectionSupervisor(Valid(), connector);
        await sup.StartAsync(CancellationToken.None);
        var readiness = await sup.WaitUntilReadyAsync(ReadinessPolicy.Indefinitely, new CancellationTokenSource(TimeSpan.FromSeconds(5)).Token);
        Assert.Equal(BusReadiness.Fatal, readiness);
        Assert.Equal(BusConnectionState.Fatal, sup.State);
    }

    [Fact]
    public async Task WaitUpTo_times_out_but_loop_keeps_retrying()
    {
        var connector = new ScriptedConnector();
        connector.FailWith(new System.IO.IOException("down"));
        await using var sup = new StreamBusConnectionSupervisor(Valid(), connector);
        await sup.StartAsync(CancellationToken.None);
        var readiness = await sup.WaitUntilReadyAsync(ReadinessPolicy.UpTo(TimeSpan.FromMilliseconds(200)), TestContext.Current.CancellationToken);
        Assert.Equal(BusReadiness.TimedOut, readiness);
        Assert.NotEqual(BusConnectionState.Ready, sup.State);
        connector.Succeed();
        var later = await sup.WaitUntilReadyAsync(ReadinessPolicy.Indefinitely, new CancellationTokenSource(TimeSpan.FromSeconds(35)).Token);
        Assert.Equal(BusReadiness.Ready, later);
    }

    [Fact]
    public async Task Backoff_is_capped()
    {
        await using var sup = new StreamBusConnectionSupervisor(Valid(), new ScriptedConnector());
        foreach (var attempt in new[] { 1, 5, 10, 50, 100 })
            Assert.True(sup.ComputeDelay(attempt) <= TimeSpan.FromSeconds(31));
    }
}

public sealed class StreamSystemConnectorTests
{
    private static StreamBusOptions WithPool(int producers, int consumers) => new()
    {
        Vhost = "/",
        UserName = "app",
        Password = "secret",
        Connection = new StreamBusConnectionOptions
        {
            ProducersPerConnection = producers,
            ConsumersPerConnection = consumers,
        },
    };

    [Theory]
    [InlineData(256, 1)]
    [InlineData(300, 1)]
    [InlineData(1, 256)]
    [InlineData(1, 1000)]
    public async Task Out_of_range_pool_values_fail_fast_without_connecting(int producers, int consumers)
    {
        var connector = new StreamSystemConnector();
        var ex = await Assert.ThrowsAsync<StreamBusConfigurationException>(() =>
            connector.ConnectAsync(WithPool(producers, consumers), TestContext.Current.CancellationToken));
        Assert.Contains("255", ex.Message, StringComparison.Ordinal);
        Assert.Contains(producers > byte.MaxValue ? "ProducersPerConnection" : "ConsumersPerConnection", ex.Message, StringComparison.Ordinal);
    }
}
