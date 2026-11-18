namespace StreamBus.UnitTests;

public sealed class RetryFilterValidationTests
{
    private static ExceptionFilterSet Rules()
    {
        var set = new ExceptionFilterSet();
        set.AddInclude(typeof(InvalidOperationException));
        return set;
    }

    [Fact]
    public void Filters_with_unbounded_schedule_throw()
    {
        var ex = Assert.Throws<StreamBusConfigurationException>(
            () => RetryFilterValidation.Validate(new RetryPolicy { Filters = Rules() }, "test context"));

        Assert.Contains("test context", ex.Message, StringComparison.Ordinal);
        Assert.Contains("bounded", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void No_filters_with_unbounded_schedule_allowed()
        => RetryFilterValidation.Validate(new RetryPolicy(), "test context");

    [Fact]
    public void Filters_with_bounded_schedule_allowed()
        => RetryFilterValidation.Validate(
            new RetryPolicy { MaxAttempts = 3, Filters = Rules() }, "test context");

    [Fact]
    public void Derive_rejects_filters_with_unbounded_schedule()
    {
        var ex = Assert.Throws<StreamBusConfigurationException>(
            () => RetryDerivation.Derive(
                new RetryPolicy { Filters = Rules() }, null, null, false, "orders", "bus"));

        Assert.Contains("bus", ex.Message, StringComparison.Ordinal);
        Assert.Contains("orders", ex.Message, StringComparison.Ordinal);
    }
}
