namespace StreamBus;

internal sealed class PartitionLagPoller : IAsyncDisposable
{
    private readonly PartitionState _state;
    private readonly Func<Task<ulong>> _getEnd;
    private readonly Func<ulong> _getStored;
    private readonly PeriodicTimer _timer;

    public PartitionLagPoller(PartitionState state, Func<Task<ulong>> getEnd, Func<ulong> getStored, TimeSpan interval)
    {
        _state = state;
        _getEnd = getEnd;
        _getStored = getStored;
        _timer = new PeriodicTimer(interval);
    }

    public async Task RunAsync(CancellationToken ct)
    {
        try
        {
            while (await _timer.WaitForNextTickAsync(ct).ConfigureAwait(false))
            {
                try
                {
                    var end = await _getEnd().ConfigureAwait(false);
                    var stored = _getStored();
                    var lag = end > stored ? (long)(end - stored) : 0;
                    Interlocked.Exchange(ref _state.Lag, lag);
                }
                catch (OperationCanceledException) { return; }
                catch (Exception) { /* poller never faults the loop; next tick retries */ }
            }
        }
        catch (OperationCanceledException) { }
    }

    public ValueTask DisposeAsync()
    {
        _timer.Dispose();
        return ValueTask.CompletedTask;
    }
}
