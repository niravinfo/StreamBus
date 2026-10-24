namespace StreamBus;

public sealed class StreamBusConnectionSupervisor : IAsyncDisposable
{
    private readonly StreamBusOptions _options;
    private readonly IStreamSystemConnector _connector;
    private readonly TimeSpan _baseDelay = TimeSpan.FromMilliseconds(200);
    private readonly TimeSpan _maxDelay = TimeSpan.FromSeconds(30);
    private readonly object _gate = new();
    private IStreamSystemHandle? _handle;
    private Task? _loop;
    private CancellationTokenSource? _cts;

    public BusConnectionState State { get; private set; } = BusConnectionState.Starting;
    public event Action<BusConnectionState>? StateChanged;

    internal IStreamSystemHandle? CurrentHandle
    {
        get { lock (_gate) return State == BusConnectionState.Ready ? _handle : null; }
    }

    public StreamBusConnectionSupervisor(StreamBusOptions options, IStreamSystemConnector connector)
    {
        ArgumentNullException.ThrowIfNull(options);
        ArgumentNullException.ThrowIfNull(connector);
        _options = options;
        _connector = connector;
        foreach (var error in options.Validate())
            throw new StreamBusConfigurationException(error);
    }

    public Task StartAsync(CancellationToken ct)
    {
        lock (_gate)
        {
            _cts ??= CancellationTokenSource.CreateLinkedTokenSource(ct);
            _loop ??= RunLoopAsync(_cts.Token);
        }
        return Task.CompletedTask;
    }

    public async Task<BusReadiness> WaitUntilReadyAsync(ReadinessPolicy policy, CancellationToken ct = default)
    {
        ArgumentNullException.ThrowIfNull(policy);
        if (State == BusConnectionState.Ready) return BusReadiness.Ready;
        if (State == BusConnectionState.Fatal) return BusReadiness.Fatal;
        return policy switch
        {
            ReadinessPolicy.FailFast => BusReadiness.TimedOut,
            ReadinessPolicy.WaitIndefinitely => await WaitForStateAsync(null, ct).ConfigureAwait(false),
            ReadinessPolicy.WaitUpTo w => await WaitForStateAsync(w.Timeout, ct).ConfigureAwait(false),
            _ => throw new StreamBusConfigurationException($"Unknown readiness policy {policy.GetType().Name}."),
        };
    }

    private async Task<BusReadiness> WaitForStateAsync(TimeSpan? timeout, CancellationToken ct)
    {
        var tcs = new TaskCompletionSource<BusReadiness>(TaskCreationOptions.RunContinuationsAsynchronously);
        void Handler(BusConnectionState s)
        {
            if (s == BusConnectionState.Ready) tcs.TrySetResult(BusReadiness.Ready);
            else if (s == BusConnectionState.Fatal) tcs.TrySetResult(BusReadiness.Fatal);
        }
        StateChanged += Handler;
        try
        {
            if (State == BusConnectionState.Ready) return BusReadiness.Ready;
            if (State == BusConnectionState.Fatal) return BusReadiness.Fatal;
            if (timeout is null) return await tcs.Task.WaitAsync(ct).ConfigureAwait(false);
            if (timeout.Value <= TimeSpan.Zero) return BusReadiness.TimedOut;
            using var cts = CancellationTokenSource.CreateLinkedTokenSource(ct);
            cts.CancelAfter(timeout.Value);
            try { return await tcs.Task.WaitAsync(cts.Token).ConfigureAwait(false); }
            catch (OperationCanceledException) when (!ct.IsCancellationRequested) { return BusReadiness.TimedOut; }
        }
        finally { StateChanged -= Handler; }
    }

    private async Task RunLoopAsync(CancellationToken ct)
    {
        var attempt = 0;
        while (!ct.IsCancellationRequested)
        {
            try
            {
                var handle = await _connector.ConnectAsync(_options, ct).ConfigureAwait(false);
                SwapHandle(handle);
                attempt = 0; // a successful connect clears the backoff streak
                SetState(BusConnectionState.Ready);
                await MonitorAsync(handle, ct).ConfigureAwait(false);
                // MonitorAsync returns only when the handle closed or we are stopping.
                if (ct.IsCancellationRequested) return;
                await ReleaseHandleAsync(handle).ConfigureAwait(false);
                SetState(BusConnectionState.TransientFailure);
            }
            catch (OperationCanceledException) { return; }
            catch (Exception ex) when (StreamBusErrorClassification.IsFatal(ex))
            {
                SetState(BusConnectionState.Fatal);
                return;
            }
            catch (Exception)
            {
                attempt++;
                SetState(BusConnectionState.TransientFailure);
                await Task.Delay(ComputeDelay(attempt), ct).ConfigureAwait(false);
            }
        }
    }

    private async Task MonitorAsync(IStreamSystemHandle handle, CancellationToken ct)
    {
        var interval = _options.Connection.MonitorInterval;
        while (!ct.IsCancellationRequested)
        {
            await Task.Delay(interval, ct).ConfigureAwait(false);
            if (handle.IsClosed) return;
        }
    }

    private void SwapHandle(IStreamSystemHandle handle)
    {
        lock (_gate) _handle = handle;
    }

    private async Task ReleaseHandleAsync(IStreamSystemHandle handle)
    {
        lock (_gate)
        {
            if (ReferenceEquals(_handle, handle)) _handle = null;
        }
        try
        {
            await handle.DisposeAsync().ConfigureAwait(false);
        }
        catch
        {
            // The broker connection is already gone; disposal must not abort the reconnect loop.
        }
    }

    public TimeSpan ComputeDelay(int attempt)
        => StreamBusDelays.ExponentialCapped(attempt, _baseDelay, _maxDelay);

    private void SetState(BusConnectionState next)
    {
        State = next;
        StateChanged?.Invoke(next);
    }

    public async ValueTask DisposeAsync()
    {
        if (_cts is not null) await _cts.CancelAsync().ConfigureAwait(false);
        if (_loop is not null) { try { await _loop.ConfigureAwait(false); } catch (OperationCanceledException) { } }
        IStreamSystemHandle? handle;
        lock (_gate) { handle = _handle; _handle = null; }
        if (handle is not null) await handle.DisposeAsync().ConfigureAwait(false);
        _cts?.Dispose();
    }
}
