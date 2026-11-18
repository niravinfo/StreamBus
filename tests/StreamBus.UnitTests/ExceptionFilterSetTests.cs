namespace StreamBus.UnitTests;

public sealed class ExceptionFilterSetTests
{
    private sealed class DerivedInvalidOperationException : InvalidOperationException { }

    private sealed class OtherException : Exception
    {
        public OtherException() { }
        public OtherException(string message, Exception inner) : base(message, inner) { }
    }

    [Fact]
    public void Empty_set_decides_Retry()
    {
        var set = new ExceptionFilterSet();

        Assert.False(set.HasRules);
        Assert.Equal(RetryDecision.Retry, set.Decide(new OtherException()));
    }

    [Fact]
    public void Type_rule_matches_derived_exception()
    {
        var set = new ExceptionFilterSet();
        set.AddInclude(typeof(InvalidOperationException));

        Assert.Equal(RetryDecision.Retry, set.Decide(new DerivedInvalidOperationException()));
    }

    [Fact]
    public void Type_rule_matches_base_exception_in_chain()
    {
        var set = new ExceptionFilterSet();
        set.AddInclude(typeof(InvalidOperationException));

        var thrown = new OtherException("outer", new InvalidOperationException("inner"));
        Assert.Equal(RetryDecision.Retry, set.Decide(thrown));
    }

    [Fact]
    public void Type_rule_matches_aggregate_inner_exception()
    {
        var set = new ExceptionFilterSet();
        set.AddInclude(typeof(InvalidOperationException));

        Assert.Equal(RetryDecision.Retry, set.Decide(new AggregateException(new InvalidOperationException("inner"))));
    }

    [Fact]
    public void Predicate_rule_matches_typed_exception()
    {
        var set = new ExceptionFilterSet();
        set.AddExclude<ArgumentException>(e => e.ParamName == "orderTotal");

        Assert.Equal(RetryDecision.Ignored, set.Decide(new ArgumentException("bad", "orderTotal")));
        Assert.Equal(RetryDecision.Retry, set.Decide(new ArgumentException("bad", "other")));
    }

    [Fact]
    public void Handle_only_unmatched_decides_Unhandled()
    {
        var set = new ExceptionFilterSet();
        set.AddInclude(typeof(InvalidOperationException));

        Assert.Equal(RetryDecision.Unhandled, set.Decide(new OtherException()));
    }

    [Fact]
    public void Ignore_rule_decides_Ignored()
    {
        var set = new ExceptionFilterSet();
        set.AddExclude(typeof(ArgumentException));

        Assert.Equal(RetryDecision.Ignored, set.Decide(new ArgumentException("bad")));
    }

    [Fact]
    public void Ignore_wins_when_both_match()
    {
        var set = new ExceptionFilterSet();
        set.AddInclude(typeof(InvalidOperationException));
        set.AddExclude(typeof(InvalidOperationException));

        Assert.Equal(RetryDecision.Ignored, set.Decide(new InvalidOperationException("bad")));
    }

    [Fact]
    public void Multiple_rules_accumulate()
    {
        var set = new ExceptionFilterSet();
        set.AddInclude(typeof(InvalidOperationException));
        set.AddInclude(typeof(ArgumentException));
        set.AddExclude(typeof(ArgumentException));

        Assert.Equal(RetryDecision.Retry, set.Decide(new InvalidOperationException()));
        Assert.Equal(RetryDecision.Ignored, set.Decide(new ArgumentException()));
        Assert.Equal(RetryDecision.Unhandled, set.Decide(new OtherException()));
    }
}
