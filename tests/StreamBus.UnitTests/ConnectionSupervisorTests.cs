namespace StreamBus.UnitTests;

using RabbitMQ.Stream.Client;

file sealed class StubHandle : IStreamSystemHandle
{
    public bool IsClosed => false;
    public ValueTask DisposeAsync() => ValueTask.CompletedTask;
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
