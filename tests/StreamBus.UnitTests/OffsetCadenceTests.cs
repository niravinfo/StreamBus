namespace StreamBus.UnitTests;

public sealed class OffsetCadenceTests
{
    [Fact]
    public async Task Every3_stores_at_3_and_redelivers_at_most_2()
    {
        // Simulate: successes 0..4 with Every=3 -> stores {2}; "crash" (no more stores);
        // resume replays from last stored + 1 = 3 -> redelivery set {3,4} (<= N-1 = 2).
        var guard = new MonotonicOffsetGuard();
        var rejected = new List<ulong>();
        var storer = new OffsetStorer(3, guard, "k", rejected.Add);
        var stored = new List<ulong>();
        for (ulong o = 0; o < 5; o++)
        {
            if (storer.ShouldStoreOnSuccess(o))
            {
                Assert.True(storer.TryAdmit(o));
                stored.Add(o);
                storer.Stored(o);
            }
        }
        Assert.Equal(new[] { 2UL }, stored);
        Assert.Equal(3UL, stored[^1] + 1); // resume point
        Assert.Empty(rejected);
        await Task.CompletedTask;
    }

    [Fact]
    public async Task MaxInterval_timer_stores_last_success()
    {
        var guard = new MonotonicOffsetGuard();
        var storer = new OffsetStorer(1000, guard, "k", _ => { });
        var stored = new List<ulong>();
        using var timer = new PeriodicTimer(TimeSpan.FromMilliseconds(50));
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(5));
        // one success, no cadence store; timer path stores LastSuccess:
        Assert.False(storer.ShouldStoreOnSuccess(0)); // 1 < 1000
        // emulate the loop's timer tick body:
        while (await timer.WaitForNextTickAsync(cts.Token))
        {
            if (storer.HasSuccess && storer.TryAdmit(storer.LastSuccess))
            {
                stored.Add(storer.LastSuccess);
                storer.Stored(storer.LastSuccess);
                break;
            }
        }
        Assert.Equal(new[] { 0UL }, stored);
    }
}
