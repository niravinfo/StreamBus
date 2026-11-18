namespace StreamBus.UnitTests;

public sealed class RetryDecisionEvaluatorTests
{
    private static RetryPolicy Policy(Action<ExceptionFilterSet> configure, int maxAttempts)
    {
        var filters = new ExceptionFilterSet();
        configure(filters);
        return new RetryPolicy { MaxAttempts = maxAttempts, Filters = filters };
    }

    [Fact]
    public void Ignored_exception_exhausts_on_first_attempt()
    {
        var policy = Policy(f => f.AddExclude(typeof(InvalidOperationException)), maxAttempts: 5);

        Assert.True(RetryDecisionEvaluator.ShouldExhaust(policy, new InvalidOperationException(), 1));
    }

    [Fact]
    public void Unhandled_exception_exhausts_on_first_attempt()
    {
        var policy = Policy(f => f.AddInclude(typeof(ArgumentException)), maxAttempts: 5);

        Assert.True(RetryDecisionEvaluator.ShouldExhaust(policy, new InvalidOperationException(), 1));
    }

    [Fact]
    public void Handled_exception_respects_attempt_limit()
    {
        var policy = Policy(f => f.AddInclude(typeof(InvalidOperationException)), maxAttempts: 3);

        Assert.False(RetryDecisionEvaluator.ShouldExhaust(policy, new InvalidOperationException(), 1));
        Assert.False(RetryDecisionEvaluator.ShouldExhaust(policy, new InvalidOperationException(), 2));
        Assert.True(RetryDecisionEvaluator.ShouldExhaust(policy, new InvalidOperationException(), 3));
    }

    [Fact]
    public void No_filters_keep_existing_attempt_limit_behavior()
    {
        var policy = new RetryPolicy { MaxAttempts = 2 };

        Assert.False(RetryDecisionEvaluator.ShouldExhaust(policy, new InvalidOperationException(), 1));
        Assert.True(RetryDecisionEvaluator.ShouldExhaust(policy, new InvalidOperationException(), 2));
    }

    [Fact]
    public void Unbounded_without_filters_never_exhausts()
    {
        Assert.False(RetryDecisionEvaluator.ShouldExhaust(new RetryPolicy(), new InvalidOperationException(), 100));
    }
}
