namespace StreamBus.UnitTests;

// OffsetStorer is mutated on the dispatch thread and read by the timer
// thread. The timer must observe a consistent snapshot (single success offset and
// its stored-ness) rather than three unsynchronized field reads that can tear or
// interleave with a concurrent dispatch-path mutation.
public sealed class OffsetStorerTests
{
    private static OffsetStorer Create(int every = 1000, Action<ulong>? onRejected = null)
        => new(every, new MonotonicOffsetGuard(), "water", onRejected ?? (_ => { }));

    [Fact]
    public void Cadence_store_requested_only_on_every_Nth_success()
    {
        var storer = Create(every: 3);
        Assert.False(storer.ShouldStoreOnSuccess(1));
        Assert.False(storer.ShouldStoreOnSuccess(2));
        Assert.True(storer.ShouldStoreOnSuccess(3));
        // The cadence counter resets only once the store commits; while the
        // triggered store is pending, the next success keeps requesting it.
        storer.Stored(3);
        Assert.False(storer.ShouldStoreOnSuccess(4));
    }

    [Fact]
    public void TryGetPendingStore_before_any_success_returns_false()
    {
        var storer = Create();
        Assert.False(storer.TryGetPendingStore(out _));
    }

    [Fact]
    public void TryGetPendingStore_returns_unstored_latest_success()
    {
        var storer = Create(every: 3);
        Assert.False(storer.ShouldStoreOnSuccess(1));
        Assert.False(storer.ShouldStoreOnSuccess(2));
        Assert.True(storer.TryGetPendingStore(out var pending));
        Assert.Equal(2UL, pending);
    }

    [Fact]
    public void TryGetPendingStore_returns_false_when_latest_success_is_stored()
    {
        var storer = Create(every: 3);
        storer.ShouldStoreOnSuccess(1);
        storer.ShouldStoreOnSuccess(2);
        storer.Stored(2);
        Assert.False(storer.TryGetPendingStore(out _));
    }

    [Fact]
    public async Task Concurrent_writers_and_reader_observe_consistent_snapshots()
    {
        var storer = Create(every: 64);
        // Not a multiple of the cadence so the latest success stays pending.
        var writes = 40_001UL;
        var writer = Task.Run(() =>
        {
            for (var offset = 1UL; offset <= writes; offset++)
            {
                if (storer.ShouldStoreOnSuccess(offset))
                {
                    storer.Stored(offset);
                }
            }
        }, TestContext.Current.CancellationToken);
        var readerStop = new CancellationTokenSource();
        var reader = Task.Run(() =>
        {
            while (!readerStop.Token.IsCancellationRequested)
            {
                if (storer.TryGetPendingStore(out var pending))
                {
                    // The reported pending offset is always a real success that was
                    // observed atomically: it must be within the current write range.
                    Assert.InRange(pending, 1UL, writes);
                }
            }
        }, TestContext.Current.CancellationToken);
        await writer;
        readerStop.Cancel();
        await reader;
        // Final state: cadence counter and pending snapshot agree.
        Assert.True(storer.TryGetPendingStore(out var last));
        Assert.Equal(writes, last);
    }
}
