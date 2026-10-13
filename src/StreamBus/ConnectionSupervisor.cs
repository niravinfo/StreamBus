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
    private Exception? _fatal;

    public BusConnectionState State { get; private set; } = BusConnectionState.Starting;
    public event Action<BusConnectionState>? StateChanged;

    public StreamBusConnectionSupervisor(StreamBusOptions options, IStreamSystemConnector connector)
    {
        _options = options;
        _connector = connector;
        foreach (var error in options.Connection.Validate())
            throw new StreamBusConfigurationException(error);
        StreamBusCredentialValidator.Validate(options);
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
            ReadinessPolicy.FailFast => State == BusConnectionState.Ready ? BusReadiness.Ready : BusReadiness.TimedOut,
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
                _handle = handle;
                SetState(BusConnectionState.Ready);
                return;
            }
            catch (OperationCanceledException) { return; }
            catch (Exception ex) when (StreamBusErrorClassification.IsFatal(ex))
            {
                _fatal = ex;
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

    public TimeSpan ComputeDelay(int attempt)
    {
        var grown = TimeSpan.FromMilliseconds(_baseDelay.TotalMilliseconds * Math.Pow(2, Math.Min(attempt - 1, 8)));
        if (grown > _maxDelay) grown = _maxDelay;
        var jitter = TimeSpan.FromMilliseconds(Random.Shared.Next(0, (int)Math.Min(1000, grown.TotalMilliseconds / 2 + 1)));
        return grown + jitter;
    }

    private void SetState(BusConnectionState next)
    {
        State = next;
        StateChanged?.Invoke(next);
    }

    public async ValueTask DisposeAsync()
    {
        if (_cts is not null) await _cts.CancelAsync().ConfigureAwait(false);
        if (_loop is not null) { try { await _loop.ConfigureAwait(false); } catch (OperationCanceledException) { } }
        if (_handle is not null) await _handle.DisposeAsync().ConfigureAwait(false);
        _cts?.Dispose();
    }
}
