namespace StreamBus.UnitTests;

public sealed class ExceptionHierarchyTests
{
    [Fact]
    public void All_sealed_exceptions_share_the_streambus_base()
    {
        var assembly = typeof(StreamBusFatalException).Assembly;
        var baseType = assembly.GetType("StreamBus.StreamBusException");

        Assert.NotNull(baseType);
        Assert.True(baseType!.IsAbstract);

        var exceptions = assembly.GetTypes()
            .Where(t => t.IsSealed && typeof(Exception).IsAssignableFrom(t))
            .ToList();

        Assert.NotEmpty(exceptions);
        Assert.All(exceptions, t =>
            Assert.True(baseType.IsAssignableFrom(t), $"{t.FullName} does not derive from {baseType.FullName}."));
    }
}
