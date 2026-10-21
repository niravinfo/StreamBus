namespace StreamBus.UnitTests;

public sealed class RetryPolicyTests
{
    private static StreamConsumerRegistry UnstartedRegistry()
    {
        var options = new StreamBusOptions { Vhost = "/", UserName = "app", Password = "secret" };
        var supervisor = new StreamBusConnectionSupervisor(options, new NeverReadyConnector());
        return new StreamConsumerRegistry(supervisor, "/", new StreamProducerRegistry(supervisor, "/"), null, new ResultProducerRegistry(supervisor, "/"));
    }

    private sealed class NeverReadyConnector : IStreamSystemConnector
    {
        public Task<IStreamSystemHandle> ConnectAsync(StreamBusOptions options, CancellationToken ct)
            => new TaskCompletionSource<IStreamSystemHandle>().Task;
    }

    private static Task<IStreamConsumer> Consume(ConsumeOptions options)
    {
        var registry = UnstartedRegistry();
        return registry.ConsumeAsync(typeof(DefaultBus), "s", options, _ => Task.CompletedTask, TestContext.Current.CancellationToken);
    }

    [Fact]
    public async Task Strict_with_max_attempts_throws()
    {
        var ex = await Assert.ThrowsAsync<StreamBusConfigurationException>(() =>
            Consume(new ConsumeOptions { Retry = new RetryPolicy { MaxAttempts = 3 } }));
        Assert.Contains("Strict", ex.Message);
    }

    [Fact]
    public async Task Strict_with_error_stream_throws()
    {
        var ex = await Assert.ThrowsAsync<StreamBusConfigurationException>(() =>
            Consume(new ConsumeOptions { ErrorStream = new ErrorStreamOptions { Stream = "e" } }));
        Assert.Equal(
            "Retry policy Strict never exhausts; a hand-off destination (ErrorStream/DeadLetter) cannot fire. Remove the destination or use a bounded retry schedule.",
            ex.Message);
    }

    [Fact]
    public async Task Strict_with_dead_letter_throws()
    {
        var ex = await Assert.ThrowsAsync<StreamBusConfigurationException>(() =>
            Consume(new ConsumeOptions
            {
                DeadLetter = new DeadLetterOptions { Name = "e", QueueType = DeadLetterQueueType.ClassicQueue },
            }));
        Assert.Equal(
            "Retry policy Strict never exhausts; a hand-off destination (ErrorStream/DeadLetter) cannot fire. Remove the destination or use a bounded retry schedule.",
            ex.Message);
    }

    [Fact]
    public async Task Skip_without_max_attempts_throws()
    {
        var ex = await Assert.ThrowsAsync<StreamBusConfigurationException>(() =>
            Consume(new ConsumeOptions
            {
                Retry = new RetryPolicy { FailureMode = FailureMode.SkipAfterDeadline },
                ErrorStream = new ErrorStreamOptions { Stream = "e" },
            }));
        Assert.Contains("MaxAttempts", ex.Message);
    }

    [Fact]
    public async Task Skip_without_error_stream_throws()
    {
        var ex = await Assert.ThrowsAsync<StreamBusConfigurationException>(() =>
            Consume(new ConsumeOptions
            {
                Retry = new RetryPolicy { FailureMode = FailureMode.SkipAfterDeadline, MaxAttempts = 3 },
            }));
        Assert.Contains("ErrorStream", ex.Message);
    }

    [Theory]
    [InlineData(0)]
    [InlineData(-2)]
    public async Task Non_positive_max_attempts_throws(int max)
    {
        await Assert.ThrowsAsync<StreamBusConfigurationException>(() =>
            Consume(new ConsumeOptions
            {
                Retry = new RetryPolicy { FailureMode = FailureMode.SkipAfterDeadline, MaxAttempts = max },
                ErrorStream = new ErrorStreamOptions { Stream = "e" },
            }));
    }

    [Theory]
    [InlineData(0)]
    [InlineData(-1)]
    public async Task Non_positive_base_delay_throws(int ms)
    {
        await Assert.ThrowsAsync<StreamBusConfigurationException>(() =>
            Consume(new ConsumeOptions { Retry = new RetryPolicy { BaseDelay = TimeSpan.FromMilliseconds(ms) } }));
    }

    [Theory]
    [InlineData(-1)]
    public async Task Negative_max_jitter_throws(int ms)
    {
        await Assert.ThrowsAsync<StreamBusConfigurationException>(() =>
            Consume(new ConsumeOptions { Retry = new RetryPolicy { MaxJitter = TimeSpan.FromMilliseconds(ms) } }));
    }

    [Fact]
    public async Task Sub_one_multiplier_throws()
    {
        await Assert.ThrowsAsync<StreamBusConfigurationException>(() =>
            Consume(new ConsumeOptions { Retry = new RetryPolicy { Multiplier = 0.5 } }));
    }

    [Theory]
    [InlineData(double.NaN)]
    [InlineData(double.PositiveInfinity)]
    [InlineData(double.NegativeInfinity)]
    public async Task Non_finite_multiplier_throws(double multiplier)
    {
        var ex = await Assert.ThrowsAsync<StreamBusConfigurationException>(() =>
            Consume(new ConsumeOptions { Retry = new RetryPolicy { Multiplier = multiplier } }));
        Assert.Contains("finite", ex.Message);
    }

    [Fact]
    public void Astronomical_base_delay_clamps_to_max_without_throwing()
    {
        var log = new ListLogger();
        var policy = new RetryPolicy { BaseDelay = TimeSpan.MaxValue, Multiplier = 2.0, MaxDelay = TimeSpan.FromSeconds(30), MaxJitter = TimeSpan.Zero };
        Assert.Equal(TimeSpan.FromSeconds(30), RetryDelays.ResolveDelay(policy, 100, new InvalidOperationException(), log));
    }

    [Fact]
    public void Attempt_1_resolves_to_base_delay_without_jitter()
    {
        var log = new ListLogger();
        var policy = new RetryPolicy { BaseDelay = TimeSpan.FromMilliseconds(200), Multiplier = 2.0, MaxDelay = TimeSpan.FromSeconds(30), MaxJitter = TimeSpan.Zero };
        Assert.Equal(TimeSpan.FromMilliseconds(200), RetryDelays.ResolveDelay(policy, 1, new InvalidOperationException(), log));
        Assert.Equal(TimeSpan.FromMilliseconds(400), RetryDelays.ResolveDelay(policy, 2, new InvalidOperationException(), log));
        Assert.Equal(TimeSpan.FromMilliseconds(800), RetryDelays.ResolveDelay(policy, 3, new InvalidOperationException(), log));
    }

    [Fact]
    public void Delay_caps_at_max_delay_and_jitter_stays_bounded()
    {
        var log = new ListLogger();
        var policy = new RetryPolicy { BaseDelay = TimeSpan.FromSeconds(1), Multiplier = 10.0, MaxDelay = TimeSpan.FromSeconds(5), MaxJitter = TimeSpan.FromSeconds(1) };
        // Attempts 1-2 resolve below the cap by design (1s, 5s+jitter); from attempt 3
        // growth saturates: grown capped to 5s plus at most 1s jitter.
        for (var attempt = 3; attempt <= 6; attempt++)
        {
            var delay = RetryDelays.ResolveDelay(policy, attempt, new InvalidOperationException(), log);
            Assert.True(delay >= TimeSpan.FromSeconds(5) && delay <= TimeSpan.FromSeconds(6));
        }
    }

    [Fact]
    public void Delay_calculator_overrides_knobs_with_one_based_attempts()
    {
        var log = new ListLogger();
        var seen = new List<int>();
        var policy = new RetryPolicy { DelayCalculator = (attempt, _) => { seen.Add(attempt); return TimeSpan.FromMilliseconds(attempt * 10); } };
        Assert.Equal(TimeSpan.FromMilliseconds(10), RetryDelays.ResolveDelay(policy, 1, new InvalidOperationException(), log));
        Assert.Equal(TimeSpan.FromMilliseconds(20), RetryDelays.ResolveDelay(policy, 2, new InvalidOperationException(), log));
        Assert.Equal(new[] { 1, 2 }, seen);
    }

    [Fact]
    public void Throwing_calculator_falls_back_and_warns()
    {
        var log = new ListLogger();
        var policy = new RetryPolicy
        {
            BaseDelay = TimeSpan.FromMilliseconds(200),
            Multiplier = 2.0,
            MaxDelay = TimeSpan.FromSeconds(30),
            MaxJitter = TimeSpan.Zero,
            DelayCalculator = (_, _) => throw new InvalidOperationException("boom"),
        };
        Assert.Equal(TimeSpan.FromMilliseconds(200), RetryDelays.ResolveDelay(policy, 1, new InvalidOperationException(), log));
        Assert.Contains(log.Lines, l => l.Contains("DelayCalculator"));
    }

    [Fact]
    public void Negative_calculator_result_clamps_to_zero()
    {
        var log = new ListLogger();
        var policy = new RetryPolicy { DelayCalculator = (_, _) => TimeSpan.FromMilliseconds(-5) };
        Assert.Equal(TimeSpan.Zero, RetryDelays.ResolveDelay(policy, 1, new InvalidOperationException(), log));
    }

    [Theory]
    [InlineData(1, true)]
    [InlineData(5, true)]
    [InlineData(6, false)]
    [InlineData(99, false)]
    [InlineData(100, true)]
    [InlineData(200, true)]
    public void Log_sampling_warns_early_then_every_hundredth(int attempt, bool warn)
    {
        Assert.Equal(warn, RetryDelays.ShouldLogRetryWarning(attempt));
    }
}
